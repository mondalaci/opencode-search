/**
 * opencode-search — a small local service that adds FTS5/BM25 content search
 * over opencode v2 conversation databases.
 *
 * Each configured source database is opened READ-ONLY (WAL allows many
 * concurrent readers) and indexed into a private FTS5 database. One process
 * serves every source over a single loopback port, and each result carries the
 * source it came from.
 *
 * Sync strategy (per source):
 *   - initial backfill of all searchable parts
 *   - `max(part.time_updated)` on the source db as a cheap cross-process change
 *     detector; when it advances, index parts newer than the stored watermark
 *
 * Endpoints:
 *   GET /health
 *   GET /search?q=<query>&source=<name>&limit=<n>
 *   GET /sessions?source=<name>&limit=<n>&offset=<n>
 *   GET /debug
 */

import { Database } from "bun:sqlite"
import { homedir } from "node:os"
import { join } from "node:path"
import { mkdirSync } from "node:fs"

const SEARCH_DB = process.env.SEARCH_DB ?? join(import.meta.dir, "..", "data", "search.db")
const PORT = Number(process.env.SEARCH_PORT ?? 27184)
const HOST = process.env.SEARCH_HOST ?? "127.0.0.1"
// Extra allowed origins. Loopback origins are always allowed (see
// `allowedOrigin`), so this defaults to none rather than `*` — a wildcard would
// let any website read the user's conversations.
const ORIGINS = (process.env.SEARCH_ORIGINS ?? "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean)
const POLL_MS = Number(process.env.SEARCH_POLL_MS ?? 1000)

/** A database to index: `name=path`, comma separated in `SEARCH_SOURCES`. */
interface Source {
  name: string
  path: string
}

const expandHome = (value: string): string => (value.startsWith("~") ? join(homedir(), value.slice(1)) : value)

function parseSources(): Source[] {
  const raw = process.env.SEARCH_SOURCES ?? `opencode=${join(homedir(), ".local", "share", "opencode", "opencode.db")}`
  return raw
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const eq = entry.indexOf("=")
      const name = (eq === -1 ? entry : entry.slice(0, eq)).trim()
      const path = expandHome((eq === -1 ? "" : entry.slice(eq + 1)).trim())
      return { name, path }
    })
    .filter((source) => source.name && source.path)
}

const SOURCES = parseSources()
const sourceByName = new Map(SOURCES.map((source) => [source.name, source]))

/** Message text, file references and tool errors — mirrors the server's recall search. */
const TEXT_SQL = `CASE
    WHEN json_extract(p.data, '$.type') = 'text' THEN coalesce(json_extract(p.data, '$.text'), '')
    WHEN json_extract(p.data, '$.type') = 'file' THEN trim(
      coalesce(json_extract(p.data, '$.filename'), '') || ' ' ||
      CASE WHEN coalesce(json_extract(p.data, '$.url'), '') NOT LIKE 'data:%'
        THEN coalesce(json_extract(p.data, '$.url'), '') ELSE '' END || ' ' ||
      coalesce(json_extract(p.data, '$.source.path'), '') || ' ' ||
      coalesce(json_extract(p.data, '$.source.name'), '') || ' ' ||
      CASE WHEN coalesce(json_extract(p.data, '$.source.uri'), '') NOT LIKE 'data:%'
        THEN coalesce(json_extract(p.data, '$.source.uri'), '') ELSE '' END || ' ' ||
      coalesce(json_extract(p.data, '$.source.clientName'), '')
    )
    ELSE coalesce(json_extract(p.data, '$.state.error'), '')
  END`

const PART_FILTER_SQL = `json_valid(p.data) AND (
    (json_extract(p.data, '$.type') = 'text'
      AND coalesce(json_extract(p.data, '$.synthetic'), 0) = 0
      AND coalesce(json_extract(p.data, '$.ignored'), 0) = 0)
    OR json_extract(p.data, '$.type') = 'file'
    OR (json_extract(p.data, '$.type') = 'tool'
      AND json_extract(p.data, '$.state.status') = 'error')
  )`

mkdirSync(join(SEARCH_DB, ".."), { recursive: true })

const index = new Database(SEARCH_DB, { create: true })

index.run("PRAGMA journal_mode = WAL")
index.run("PRAGMA synchronous = NORMAL")
index.exec(`
  CREATE TABLE IF NOT EXISTS parts (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    part_id TEXT NOT NULL,
    session_id TEXT,
    updated INTEGER,
    text TEXT,
    UNIQUE(source, part_id)
  );
  CREATE VIRTUAL TABLE IF NOT EXISTS part_fts USING fts5(
    text, content='parts', content_rowid='id', tokenize='unicode61 remove_diacritics 2'
  );
  CREATE TRIGGER IF NOT EXISTS parts_ai AFTER INSERT ON parts BEGIN
    INSERT INTO part_fts(rowid, text) VALUES (new.id, new.text);
  END;
  CREATE TRIGGER IF NOT EXISTS parts_ad AFTER DELETE ON parts BEGIN
    INSERT INTO part_fts(part_fts, rowid, text) VALUES ('delete', old.id, old.text);
  END;
  CREATE TRIGGER IF NOT EXISTS parts_au AFTER UPDATE ON parts BEGIN
    INSERT INTO part_fts(part_fts, rowid, text) VALUES ('delete', old.id, old.text);
    INSERT INTO part_fts(rowid, text) VALUES (new.id, new.text);
  END;
  CREATE TABLE IF NOT EXISTS source_state (
    name TEXT PRIMARY KEY,
    db_path TEXT,
    watermark INTEGER
  );
`)

/** A source database is opened fresh per use: a long-lived read-only WAL
 *  connection can pin to an old snapshot, hiding newly committed parts. */
function withSource<T>(source: Source, fn: (db: Database) => T): T {
  const db = new Database(source.path, { readonly: true })
  try {
    return fn(db)
  } finally {
    db.close()
  }
}

const getWatermark = (source: string): number => {
  const row = index
    .query<{ watermark: number | null }, [string]>("SELECT watermark FROM source_state WHERE name = ?")
    .get(source)
  return row?.watermark ?? 0
}

const setWatermark = (source: string, dbPath: string, value: number) =>
  index.run(
    `INSERT INTO source_state(name, db_path, watermark) VALUES(?, ?, ?)
     ON CONFLICT(name) DO UPDATE SET db_path = excluded.db_path, watermark = excluded.watermark`,
    [source, dbPath, value],
  )

const upsert = index.prepare(
  `INSERT INTO parts(source, part_id, session_id, updated, text) VALUES (?, ?, ?, ?, ?)
   ON CONFLICT(source, part_id) DO UPDATE SET
     session_id = excluded.session_id, updated = excluded.updated, text = excluded.text`,
)

/** Index all parts with time_updated > since (0 = full backfill), returning the new watermark. */
function indexSince(db: Database, source: string, since: number): { indexed: number; watermark: number } {
  let watermark = since
  let indexed = 0
  const page = db.query<
    { part_id: string; session_id: string; updated: number; text: string },
    [number]
  >(
    `SELECT p.id AS part_id, p.session_id AS session_id, p.time_updated AS updated, ${TEXT_SQL} AS text
     FROM part p
     WHERE ${PART_FILTER_SQL} AND p.time_updated > ? AND length(${TEXT_SQL}) > 0
     ORDER BY p.time_updated, p.id
     LIMIT 2000`,
  )

  try {
    for (;;) {
      const rows = page.all(since)
      if (rows.length === 0) break
      const tx = index.transaction((items: typeof rows) => {
        for (const row of items) {
          upsert.run(source, row.part_id, row.session_id, row.updated, row.text)
          if (row.updated > watermark) watermark = row.updated
        }
      })
      tx(rows)
      indexed += rows.length
      since = rows[rows.length - 1].updated
      if (rows.length < 2000) break
    }
  } finally {
    page.finalize()
  }
  return { indexed, watermark }
}

/** Newest part timestamp in a source db — the sync change signal. */
function newestPartUpdated(db: Database): number {
  const row = db.query<{ v: number | null }, []>("SELECT max(time_updated) AS v FROM part").get()
  return row?.v ?? 0
}

let polls = 0
let lastError: string | null = null
let lastSync: Record<string, unknown> = {}

function syncSource(source: Source, force = false) {
  const watermark = getWatermark(source.name)
  withSource(source, (db) => {
    const newest = newestPartUpdated(db)
    if (!force && newest <= watermark) return
    const { indexed, watermark: next } = indexSince(db, source.name, watermark)
    // Advance past non-indexable parts too (reasoning, step markers, completed
    // tools); parts written during the pass are > newest and caught next poll.
    setWatermark(source.name, source.path, Math.max(next, newest))
    lastSync = { ...lastSync, [source.name]: { watermark, newest, indexed, next } }
    if (indexed > 0) console.log(`[opencode-search] ${source.name}: indexed ${indexed} parts (watermark ${next})`)
  })
}

// ---- session metadata cache (per source) ------------------------------------

type SessionMeta = { title: string; directory: string; updated: number }
const sessionCaches = new Map<string, { at: number; map: Map<string, SessionMeta> }>()

function sessions(source: Source): Map<string, SessionMeta> {
  const cached = sessionCaches.get(source.name)
  if (cached && Date.now() - cached.at < 1000) return cached.map
  const rows = withSource(source, (db) =>
    db
      .query<{ id: string; title: string; directory: string; time_updated: number }, []>(
        "SELECT id, title, directory, time_updated FROM session",
      )
      .all(),
  )
  const map = new Map(rows.map((row) => [row.id, { title: row.title, directory: row.directory, updated: row.time_updated }]))
  sessionCaches.set(source.name, { at: Date.now(), map })
  return map
}

/** Resolve the `source` query parameter: default to the only source, if there
 *  is one. A stale name is tolerated in single-source setups. */
function resolveSource(name: string | null): Source | null {
  if (name) {
    const found = sourceByName.get(name)
    if (found) return found
    return SOURCES.length === 1 ? SOURCES[0] : null
  }
  return SOURCES.length === 1 ? SOURCES[0] : null
}

// ---- query ------------------------------------------------------------------

type PartHit = { source: string; session_id: string; part_id: string; score: number; snippet: string }

const searchStmt = index.query<PartHit, [string, string, number]>(
  `SELECT p.source AS source, p.session_id AS session_id, p.part_id AS part_id,
          bm25(part_fts) AS score,
          snippet(part_fts, 0, '', '', ' … ', 14) AS snippet
   FROM part_fts JOIN parts p ON p.id = part_fts.rowid
   WHERE part_fts MATCH ? AND p.source = ?
   ORDER BY score
   LIMIT ?`,
)

/** Build a safe FTS5 MATCH expression: quoted terms joined by `op`. */
function matchExpr(query: string, op: "AND" | "OR"): string | null {
  const terms = query
    .toLowerCase()
    .split(/[^\p{L}\p{N}_]+/u)
    .filter(Boolean)
    .slice(0, 16)
  if (terms.length === 0) return null
  return terms.map((term) => `"${term.replaceAll('"', '""')}"`).join(` ${op} `)
}

export function search(query: string, source: Source, limit: number, directory?: string) {
  const trimmed = query.trim()
  if (!trimmed) return []

  // A directory scope can push a project's matches past a small candidate
  // window, so widen it when filtering.
  const candidates = directory ? 5000 : 2000

  // Prefer sessions matching every term; fall back to any term.
  let hits = searchStmt.all(matchExpr(trimmed, "AND") ?? "", source.name, candidates)
  if (hits.length === 0) hits = searchStmt.all(matchExpr(trimmed, "OR") ?? "", source.name, candidates)

  const meta = sessions(source)
  const best = new Map<string, { score: number; snippet: string; partID: string }>()
  for (const hit of hits) {
    const info = meta.get(hit.session_id)
    if (!info) continue // deleted session
    if (directory && info.directory !== directory) continue
    const current = best.get(hit.session_id)
    if (!current || hit.score < current.score)
      best.set(hit.session_id, { score: hit.score, snippet: hit.snippet, partID: hit.part_id })
  }

  return [...best.entries()]
    .sort((a, b) => a[1].score - b[1].score)
    .slice(0, limit)
    .map(([sessionID, hit]) => {
      const info = meta.get(sessionID)!
      return {
        source: source.name,
        sessionID,
        title: info.title,
        directory: info.directory,
        updated: info.updated,
        score: hit.score,
        snippet: hit.snippet,
      }
    })
}

// ---- http -------------------------------------------------------------------

const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "::1", "[::1]", "0.0.0.0"])

/** Allow an origin if it is explicitly listed, if `*` is configured, or if it
 *  is a loopback origin (`127.0.0.1:*`, `localhost:*`, `*.localhost:*`). The
 *  loopback rule keeps dev servers on arbitrary ports working without config. */
function allowedOrigin(origin: string | null): string | null {
  if (ORIGINS.includes("*")) return origin ?? "*"
  if (!origin) return null
  if (ORIGINS.includes(origin)) return origin
  try {
    const host = new URL(origin).hostname.toLowerCase()
    if (LOOPBACK_HOSTS.has(host) || host.endsWith(".localhost")) return origin
  } catch {
    /* not a URL */
  }
  return null
}

function corsHeaders(req: Request): Record<string, string> {
  const origin = allowedOrigin(req.headers.get("Origin"))
  if (!origin) return {}
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    Vary: "Origin",
  }
}

const json = (body: unknown, req: Request, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders(req) },
  })

const server = Bun.serve({
  port: PORT,
  hostname: HOST,
  fetch(req) {
    const url = new URL(req.url)
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders(req) })

    if (url.pathname === "/health") {
      return json(
        {
          ok: true,
          sources: SOURCES.map((source) => ({
            name: source.name,
            db: source.path,
            parts: index.query<{ n: number }, [string]>("SELECT count(*) AS n FROM parts WHERE source = ?").get(source.name)?.n ?? 0,
            sessions: sessions(source).size,
            watermark: getWatermark(source.name),
          })),
        },
        req,
      )
    }

    if (url.pathname === "/sessions") {
      const source = resolveSource(url.searchParams.get("source"))
      if (!source) return json({ error: "unknown or ambiguous source" }, req, 400)
      const directory = url.searchParams.get("directory") || undefined
      const limit = Math.min(50000, Math.max(1, Number(url.searchParams.get("limit") ?? 2000) || 2000))
      const offset = Math.max(0, Number(url.searchParams.get("offset") ?? 0) || 0)
      const all = [...sessions(source).entries()]
        .filter(([, meta]) => !directory || meta.directory === directory)
        .sort((a, b) => b[1].updated - a[1].updated)
      return json(
        {
          source: source.name,
          total: all.length,
          results: all.slice(offset, offset + limit).map(([sessionID, meta]) => ({
            source: source.name,
            sessionID,
            title: meta.title,
            directory: meta.directory,
            updated: meta.updated,
          })),
        },
        req,
      )
    }

    if (url.pathname === "/debug") {
      return json(
        {
          polls,
          lastError,
          lastSync,
          sources: SOURCES.map((source) => ({
            name: source.name,
            db: source.path,
            watermark: getWatermark(source.name),
            newest: withSource(source, (db) => newestPartUpdated(db)),
          })),
        },
        req,
      )
    }

    if (url.pathname === "/search") {
      const source = resolveSource(url.searchParams.get("source"))
      if (!source) return json({ error: "unknown or ambiguous source" }, req, 400)
      const q = url.searchParams.get("q") ?? ""
      const directory = url.searchParams.get("directory") || undefined
      const limit = Math.min(50, Math.max(1, Number(url.searchParams.get("limit") ?? 20) || 20))
      return json({ results: search(q, source, limit, directory) }, req)
    }

    return json({ error: "not found" }, req, 404)
  },
})

// initial backfill (blocking) then start watching for changes
for (const source of SOURCES) {
  try {
    syncSource(source, true)
  } catch (error) {
    lastError = String(error)
    console.error(`[opencode-search] initial sync failed for ${source.name}:`, error)
  }
}
setInterval(() => {
  polls++
  for (const source of SOURCES) {
    try {
      syncSource(source)
    } catch (error) {
      lastError = String(error)
      console.error(`[opencode-search] sync failed for ${source.name}:`, error)
    }
  }
}, POLL_MS)

console.log(
  `[opencode-search] listening on http://${HOST}:${server.port} ` +
    `(sources: ${SOURCES.map((s) => `${s.name}=${s.path}`).join(", ")})`,
)
