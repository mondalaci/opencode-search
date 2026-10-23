// PM2 process definition for the opencode-search service.
//
//   pm2 start ecosystem.config.cjs
//   pm2 save
//
// A small Bun service that provides FTS5/BM25 content search over opencode v2
// conversation databases. It opens each source database read-only and keeps its
// own index under data/, so the servers are never modified.

const path = require("node:path")

const HOME = process.env.HOME || "/root"

module.exports = {
  apps: [
    {
      name: "opencode-search",
      cwd: __dirname,
      script: "src/index.ts",
      // bun:sqlite and Bun.serve require the Bun runtime.
      interpreter: path.join(HOME, ".bun", "bin", "bun"),
      env: {
        // One process, several sources. Names are what clients pass as `source`.
        // Add more with `name=path` pairs, e.g. a Kilo database.
        SEARCH_SOURCES: `opencode=${path.join(HOME, ".local", "share", "opencode", "opencode.db")}`,
        SEARCH_PORT: "27184",
        // Loopback origins are always allowed; list any non-loopback ones here.
        SEARCH_ORIGINS: "http://chat.localhost",
      },
      autorestart: true,
      max_restarts: 20,
      restart_delay: 2000,
    },
  ],
}
