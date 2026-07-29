import type { Database } from 'bun:sqlite';

// SQLite's busy_timeout does NOT cover `PRAGMA journal_mode = WAL` on a
// connection's first-ever switch from the default rollback-journal mode:
// that switch needs a brief exclusive lock, and unlike ordinary reads/writes,
// SQLite fails it immediately with SQLITE_BUSY instead of honoring the busy
// handler - confirmed empirically (busy_timeout set first still throws
// instantly against a lock held by another process). Re-applying
// journal_mode=WAL on a file already in WAL mode is a safe no-op regardless
// of contention, so this only matters the first time a given file is ever
// opened. Retry manually at the application level instead.
export function setWalModeWithRetry(db: Database, timeoutMs = 10000): void {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      db.run('PRAGMA journal_mode = WAL');
      return;
    } catch (err) {
      if (Date.now() >= deadline) throw err;
      Bun.sleepSync(20);
    }
  }
}
