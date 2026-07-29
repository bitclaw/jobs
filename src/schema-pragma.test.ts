import { Database } from 'bun:sqlite';
import { describe, expect, test } from 'bun:test';
import { applyPragmas } from './schema';

describe('applyPragmas - concurrent open under lock contention', () => {
  // Regression test for a production crash (SQLiteError: database is
  // locked, thrown from JobQueue's constructor during a zero-downtime
  // deploy). Two bugs stacked:
  //
  // 1. journal_mode=WAL was being set before busy_timeout (fixed: order
  //    swapped).
  // 2. Separately, and more subtly: SQLite's busy_timeout does NOT cover
  //    `PRAGMA journal_mode = WAL` itself on a connection's FIRST-EVER
  //    switch from the default rollback-journal mode - that switch needs a
  //    brief exclusive lock and fails instantly with SQLITE_BUSY regardless
  //    of busy_timeout, confirmed empirically against a real second OS
  //    process. Fixed via setWalModeWithRetry's manual retry loop
  //    (wal-mode.ts).
  //
  // Spawns a real child process to hold the write lock: the retry loop uses
  // Bun.sleepSync (blocks the thread), so a same-process "holder" scheduled
  // via setTimeout would never get a chance to run its release while the
  // retry loop blocks the event loop.
  test('given a fresh jobs db exclusively locked by another process, when applyPragmas runs, then it waits for the lock instead of throwing', async () => {
    const tmpPath = `/tmp/jobs-schema-contention-test-${Date.now()}.db`;
    const holderScript = `
      import { Database } from 'bun:sqlite';
      const db = new Database(${JSON.stringify(tmpPath)}, { create: true });
      db.run('CREATE TABLE t (id INTEGER PRIMARY KEY)');
      db.run('BEGIN IMMEDIATE');
      db.run('INSERT INTO t (id) VALUES (1)');
      await Bun.sleep(300);
      db.run('COMMIT');
    `;
    const holder = Bun.spawn(['bun', '-e', holderScript], {
      stdout: 'inherit',
      stderr: 'inherit'
    });

    await Bun.sleep(80);

    const db = new Database(tmpPath, { create: true });
    const t0 = Date.now();
    expect(() => applyPragmas(db)).not.toThrow();
    expect(Date.now() - t0).toBeGreaterThan(100);

    await holder.exited;
    db.close();

    try {
      require('node:fs').unlinkSync(tmpPath);
      require('node:fs').unlinkSync(`${tmpPath}-wal`);
      require('node:fs').unlinkSync(`${tmpPath}-shm`);
    } catch {
      // ignore
    }
  });
});
