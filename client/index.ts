/**
 * Client for the local opencode-search service.
 *
 * Framework-agnostic: it needs only `fetch` and `localStorage` (the latter for
 * a user-overridden service URL). Each application creates one client with its
 * own storage key and source name.
 */

export interface ContentHit {
  /** Source the hit came from, when the service reports it. */
  source?: string
  sessionID: string
  title: string
  directory: string
  updated: number
  score?: number
  snippet?: string
}

export interface SearchSourceHealth {
  name: string
  db: string
  parts: number
  sessions: number
  watermark: number
}

export interface SearchHealth {
  ok: boolean
  /** Indexed parts for the configured source. */
  parts: number
  /** Conversations known for the configured source. */
  sessions: number
  sources: SearchSourceHealth[]
}

export interface SessionPage {
  /** Total conversations the service knows about for the source. */
  total: number
  /** The slice of conversations for this page (newest first). */
  results: ContentHit[]
}

export interface SearchClientOptions {
  /** localStorage key holding a user-overridden service URL. */
  storageKey: string
  /** Service URL used when nothing is stored. */
  defaultURL?: string
  /** Which source to query. Omit to let the service pick (single-source setups). */
  source?: string
}

const DEFAULT_URL = "http://127.0.0.1:27184"

export function createSearchClient(options: SearchClientOptions) {
  const { storageKey, source } = options
  const defaultURL = options.defaultURL ?? DEFAULT_URL

  function searchURL(): string {
    return localStorage.getItem(storageKey)?.trim() || defaultURL
  }

  function setSearchURL(url: string) {
    const value = url.trim()
    if (value) localStorage.setItem(storageKey, value)
    else localStorage.removeItem(storageKey)
  }

  function endpoint(path: string, params: Record<string, string | number | undefined>): URL {
    const url = new URL(path, searchURL())
    if (source) url.searchParams.set("source", source)
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) url.searchParams.set(key, String(value))
    }
    return url
  }

  async function request<T>(
    path: string,
    params: Record<string, string | number | undefined>,
    timeout: number,
  ): Promise<T> {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeout)
    try {
      const res = await fetch(endpoint(path, params), { signal: controller.signal })
      if (!res.ok) throw new Error(`${path} failed: ${res.status}`)
      return (await res.json()) as T
    } finally {
      clearTimeout(timer)
    }
  }

  /** Query content search; throws if the service is unreachable or errors. */
  async function searchContent(query: string, limit = 30): Promise<ContentHit[]> {
    const data = await request<{ results?: ContentHit[] }>("/search", { q: query, limit }, 3000)
    return data.results ?? []
  }

  /** Health of the service, narrowed to the configured source. */
  async function searchHealth(timeout = 1500): Promise<SearchHealth | null> {
    try {
      const data = await request<{ ok: boolean; sources?: SearchSourceHealth[] }>("/health", {}, timeout)
      const sources = data.sources ?? []
      const mine = source ? sources.find((entry) => entry.name === source) : sources[0]
      return { ok: data.ok, parts: mine?.parts ?? 0, sessions: mine?.sessions ?? 0, sources }
    } catch {
      return null
    }
  }

  /** Whether the content-search service is reachable. */
  async function searchReachable(timeout = 1500): Promise<boolean> {
    return (await searchHealth(timeout)) != null
  }

  /** One page of conversations (newest first) from the service. */
  async function listSessionsPage(offset = 0, limit = 2000, timeout = 5000): Promise<SessionPage> {
    const data = await request<{ total?: number; results?: ContentHit[] }>("/sessions", { limit, offset }, timeout)
    const results = data.results ?? []
    return { total: data.total ?? results.length, results }
  }

  /** List every conversation (newest first) by paging through the service. */
  async function listAllSessions(pageSize = 2000): Promise<SessionPage> {
    const first = await listSessionsPage(0, pageSize)
    const results = [...first.results]
    while (results.length < first.total && first.results.length > 0) {
      let page: SessionPage
      try {
        page = await listSessionsPage(results.length, pageSize)
      } catch {
        break // Service hiccup mid-way: keep what we already have.
      }
      if (page.results.length === 0) break
      results.push(...page.results)
    }
    return { total: first.total, results }
  }

  return {
    searchURL,
    setSearchURL,
    searchContent,
    searchHealth,
    searchReachable,
    listSessionsPage,
    listAllSessions,
  }
}

export type SearchClient = ReturnType<typeof createSearchClient>
