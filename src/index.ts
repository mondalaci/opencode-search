/**
 * opencode-search — a small local service that adds FTS5/BM25 content search
 * over opencode v2 conversations.
 *
 * Each configured source database is opened READ-ONLY (WAL allows many
 * concurrent readers) and indexed into a private FTS5 database. One process
 * serves every source over a single loopback port, and each result carries the
 * source it came from.
 *
 * opencode v2 stores conversations in `session_v2` and `session_message` (the
 * `session`/`message`/`part` tables are the legacy v1 shape and are ignored).
 * A message's `data` JSON is the unit of indexing: user text for user
 * messages, and the text and tool errors inside `content[]` for assistant
 * messages.
 *
 * Sync strategy (per source):
 *   - initial backfill of all searchable messages
 *   - `max(session_message.time_updated)` as a cheap cross-process change
 *     detector; when it advances, index messages newer than the stored watermark
 *
 * Endpoints:
 *   GET /health
 *   GET /search?q=<query>&source=<name>&limit=<n>&directory=<path>
 *   GET /sessions?source=<name>&limit=<n>&offset=<n>&directory=<path>
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

/** Bump when the index schema changes; the index is rebuilt on mismatch. */
const SCHEMA_VERSION = 2

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

/**
 * The text to index for one `session_message` row. User messages carry `text`;
 * assistant messages carry `content[]` parts, of which the text parts and any
 * tool error messages are searchable. Reasoning is deliberately excluded.
 */
const TEXT_SQL = `CASE m.type
    WHEN 'user' THEN coalesce(json_extract(m.data, '$.text'), '')
    WHEN 'assistant' THEN trim(
      coalesce((SELECT group_concat(json_extract(j.value, '$.text'), ' ')
                FROM json_each(m.data, '$.content') j
                WHERE json_extract(j.value, '$.type') = 'text'), '') || ' ' ||
      coalesce((SELECT group_concat(
                  coalesce(json_extract(j.value, '$.state.error.message'), json_extract(j.value, '$.state.error')), ' ')
                FROM json_each(m.data, '$.content') j
                WHERE json_extract(j.value, '$.type') = 'tool'
                  AND json_extract(j.value, '$.state.status') = 'error'), '')
    )
    ELSE ''
  END`

mkdirSync(join(SEARCH_DB, ".."), { recursive: true })

const index = new Database(SEARCH_DB, { create: true })

index.run("PRAGMA journal_mode = WAL")
index.run("PRAGMA synchronous = NORMAL")

// The index is derived data: on a schema change, drop it and re-backfill.
index.exec(`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);`)
const storedSchema = Number(
  index.query<{ value: string }, []>("SELECT value FROM meta WHERE key = 'schema'").get()?.value ?? 0,
)
if (storedSchema !== SCHEMA_VERSION) {
  index.exec(`
    DROP TABLE IF EXISTS message_fts;
    DROP TABLE IF EXISTS messages;
    DROP TABLE IF EXISTS source_state;
  `)
  index.run("INSERT INTO meta(key, value) VALUES('schema', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", [
    String(SCHEMA_VERSION),
  ])
}

index.exec(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    message_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    updated INTEGER,
    text TEXT,
    UNIQUE(source, message_id)
  );
  CREATE VIRTUAL TABLE IF NOT EXISTS message_fts USING fts5(
    text, content='messages', content_rowid='id', tokenize='unicode61 remove_diacritics 2'
  );
  CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
    INSERT INTO message_fts(rowid, text) VALUES (new.id, new.text);
  END;
  CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
    INSERT INTO message_fts(message_fts, rowid, text) VALUES ('delete', old.id, old.text);
  END;
  CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
    INSERT INTO message_fts(message_fts, rowid, text) VALUES ('delete', old.id, old.text);
    INSERT INTO message_fts(rowid, text) VALUES (new.id, new.text);
  END;
  CREATE TABLE IF NOT EXISTS source_state (
    name TEXT PRIMARY KEY,
    db_path TEXT,
    watermark INTEGER
  );
`)

/** A source database is opened fresh per use: a long-lived read-only WAL
 *  connection can pin to an old snapshot, hiding newly committed rows. */
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
  `INSERT INTO messages(source, message_id, session_id, updated, text) VALUES (?, ?, ?, ?, ?)
   ON CONFLICT(source, message_id) DO UPDATE SET
     session_id = excluded.session_id, updated = excluded.updated, text = excluded.text`,
)

/** Index all messages with time_updated > since (0 = full backfill), returning the new watermark. */
function indexSince(db: Database, source: string, since: number): { indexed: number; watermark: number } {
  let watermark = since
  let indexed = 0
  const page = db.query<
    { message_id: string; session_id: string; updated: number; text: string },
    [number]
  >(
    `SELECT m.id AS message_id, m.session_id AS session_id, m.time_updated AS updated, ${TEXT_SQL} AS text
     FROM session_message m
     WHERE m.type IN ('user', 'assistant') AND m.time_updated > ? AND length(${TEXT_SQL}) > 0
     ORDER BY m.time_updated, m.id
     LIMIT 2000`,
  )

  try {
    for (;;) {
      const rows = page.all(since)
      if (rows.length === 0) break
      const tx = index.transaction((items: typeof rows) => {
        for (const row of items) {
          upsert.run(source, row.message_id, row.session_id, row.updated, row.text)
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

/** Newest message timestamp in a source db — the sync change signal. */
function newestMessageUpdated(db: Database): number {
  const row = db.query<{ v: number | null }, []>("SELECT max(time_updated) AS v FROM session_message").get()
  return row?.v ?? 0
}

let polls = 0
let lastError: string | null = null
let lastSync: Record<string, unknown> = {}

function syncSource(source: Source, force = false) {
  const watermark = getWatermark(source.name)
  withSource(source, (db) => {
    const newest = newestMessageUpdated(db)
    if (!force && newest <= watermark) return
    const { indexed, watermark: next } = indexSince(db, source.name, watermark)
    // Advance past non-indexable messages too (system, idle, switches); rows
    // written during the pass are > newest and caught next poll.
    setWatermark(source.name, source.path, Math.max(next, newest))
    lastSync = { ...lastSync, [source.name]: { watermark, newest, indexed, next } }
    if (indexed > 0) console.log(`[opencode-search] ${source.name}: indexed ${indexed} messages (watermark ${next})`)
  })
}

// ---- session metadata cache (per source) ------------------------------------

type SessionMeta = { title: string; directory: string; updated: number }
const sessionCaches = new Map<string, { at: number; map: Map<string, SessionMeta> }>()

/** Root conversations (no subagents) of a source, keyed by session id. */
function sessions(source: Source): Map<string, SessionMeta> {
  const cached = sessionCaches.get(source.name)
  if (cached && Date.now() - cached.at < 1000) return cached.map
  const rows = withSource(source, (db) =>
    db
      .query<{ id: string; title: string | null; directory: string; time_updated: number }, []>(
        "SELECT id, title, directory, time_updated FROM session_v2 WHERE parent_id IS NULL",
      )
      .all(),
  )
  const map = new Map(
    rows.map((row) => [
      row.id,
      { title: row.title ?? "", directory: row.directory, updated: row.time_updated },
    ]),
  )
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

type MessageHit = { source: string; session_id: string; message_id: string; score: number; snippet: string }

const searchStmt = index.query<MessageHit, [string, string, number]>(
  `SELECT m.source AS source, m.session_id AS session_id, m.message_id AS message_id,
          bm25(message_fts) AS score,
          snippet(message_fts, 0, '', '', ' … ', 14) AS snippet
   FROM message_fts JOIN messages m ON m.id = message_fts.rowid
   WHERE message_fts MATCH ? AND m.source = ?
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
  const best = new Map<string, { score: number; snippet: string; messageID: string }>()
  for (const hit of hits) {
    const info = meta.get(hit.session_id)
    if (!info) continue // deleted session or subagent
    if (directory && info.directory !== directory) continue
    const current = best.get(hit.session_id)
    if (!current || hit.score < current.score)
      best.set(hit.session_id, { score: hit.score, snippet: hit.snippet, messageID: hit.message_id })
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
            parts: index.query<{ n: number }, [string]>("SELECT count(*) AS n FROM messages WHERE source = ?").get(source.name)?.n ?? 0,
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
            newest: withSource(source, (db) => newestMessageUpdated(db)),
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
