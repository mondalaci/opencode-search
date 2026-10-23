# opencode-search

A small local service that adds **content search (FTS5 / BM25)** over opencode
v2 conversations — message text, file references and tool errors — without
modifying the opencode server.

It opens each source SQLite database **read-only** (WAL allows many concurrent
readers) and keeps its own FTS5 index in a separate database file. One process
can index several sources and serves them all over a single loopback port; every
result is tagged with the source it came from.

> **Scope.** The service speaks the opencode v2 database shape: conversations
> live in `session_v2` and `session_message`. The legacy `session` / `message` /
> `part` tables are ignored.

## Run

```bash
bun run start           # or: bun src/index.ts
```

Environment overrides:

| Variable | Default | Purpose |
|---|---|---|
| `SEARCH_SOURCES` | `opencode=~/.local/share/opencode/opencode.db` | Comma-separated `name=path` source databases (read-only) |
| `SEARCH_DB` | `data/search.db` | Own FTS index |
| `SEARCH_PORT` | `27184` | HTTP port |
| `SEARCH_HOST` | `127.0.0.1` | Bind address |
| `SEARCH_ORIGINS` | *(none)* | Extra `Access-Control-Allow-Origin` values (comma separated) |
| `SEARCH_POLL_MS` | `1000` | Sync poll interval |

### CORS

Loopback origins (`http://127.0.0.1:*`, `http://localhost:*`,
`http://*.localhost:*`) are always allowed, so dev servers on arbitrary ports
work without configuration. Anything else must be listed in `SEARCH_ORIGINS`.
`*` is accepted but discouraged: it lets any website read the indexed
conversations.

## API

```
GET /health
  -> { ok, sources: [{ name, db, parts, sessions, watermark }] }

GET /search?q=<terms>&source=<name>&limit=<1..50>&directory=<path>
  -> { results: [{ source, sessionID, title, directory, updated, score, snippet }] }

GET /sessions?source=<name>&limit=<n>&offset=<n>&directory=<path>
  -> { source, total, results: [{ source, sessionID, title, directory, updated }] }

GET /debug
```

`source` may be omitted only when exactly one source is configured.

`directory` optionally scopes `/search` and `/sessions` to one project directory
(the session's `directory`), so a client can restrict results to the project the
user is looking at. Without it, all directories are searched.

`score` is FTS5 `bm25` — **more negative is better**. Results are aggregated to
one row per session (its best-matching part) and only sessions that still exist
and are not archived are returned.

## Client

`client/index.ts` is a framework-agnostic client (needs only `fetch` and
`localStorage`). Each app creates one with its own storage key and source:

```ts
import { createSearchClient } from "opencode-search/client"

export const search = createSearchClient({
  storageKey: "my-app.search-url",
  source: "opencode",
})
```

## How sync works

- On start (and whenever a source db's `max(session_message.time_updated)`
  advances) messages with `time_updated` greater than the stored watermark are
  (re)indexed.
- The watermark advances even past non-indexable messages (`system`, `idle`,
  model/agent switches) so the scan window never grows.
- Deletions are not tracked; a deleted session simply disappears from results
  because session metadata is read live from the source db.

The searchable text is a message's user text, or — for assistant messages — the
text parts and tool error messages inside `content[]`. Reasoning is excluded.
Only root conversations (no subagents) are listed.
