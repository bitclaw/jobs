import { Database } from 'bun:sqlite';
import { describe, expect, test } from 'bun:test';
import { existsSync, statSync, unlinkSync } from 'node:fs';
import { JobQueue } from './queue';
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

describe('applyPragmas - WAL checkpointing', () => {
  // Regression test for the WAL file growing unbounded: wal_autocheckpoint
  // used to be forced to 0 here on the assumption an external Litestream
  // process would checkpoint the WAL instead - nothing ever configured
  // Litestream to do that, so the WAL grew unchecked between clean process
  // restarts. Fixed by leaving wal_autocheckpoint at SQLite's own default.
  test('given a fresh db, when applyPragmas runs, then wal_autocheckpoint is left at a real (non-zero) value', () => {
    const tmpPath = `/tmp/jobs-schema-autockpt-test-${Date.now()}.db`;
    const db = new Database(tmpPath, { create: true });
    applyPragmas(db);

    const row = db.query('PRAGMA wal_autocheckpoint').get() as {
      wal_autocheckpoint: number;
    };
    expect(row.wal_autocheckpoint).toBeGreaterThan(0);

    db.close();
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        unlinkSync(`${tmpPath}${suffix}`);
      } catch {
        // ignore
      }
    }
  });
});

describe('JobQueue - periodic WAL checkpoint backstop', () => {
  // bun:test has no fake-timer mechanism that advances a real setInterval
  // early (setSystemTime/useFakeTimers only mock Date, not scheduling), so
  // this can't wait for the real 60s interval to fire without an actual
  // 60s sleep. Instead, this proves the mechanism JobQueue's interval
  // relies on - PRAGMA wal_checkpoint(PASSIVE) - actually works, by
  // calling it directly the same way the interval callback does.
  //
  // Note: a PASSIVE checkpoint does NOT shrink the -wal file's size on
  // disk (only a TRUNCATE checkpoint does, and TRUNCATE can block
  // concurrent readers/writers, which is exactly why PASSIVE is the right
  // choice for a background backstop). What PASSIVE actually does - and
  // what bounds long-term growth - is flush all outstanding WAL frames
  // back into the main db file so subsequent writes can reuse that space
  // instead of the file growing further. wal_checkpoint's own return row
  // reports (busy, log frames, checkpointed frames) - asserting
  // log === checkpointed is the real proof the flush completed.
  test('given a WAL grown by real writes, when PRAGMA wal_checkpoint(PASSIVE) runs, then all WAL frames get flushed back to the main db file', () => {
    const tmpPath = `/tmp/jobs-queue-checkpoint-test-${Date.now()}.db`;
    const queue = new JobQueue<{ noop: { n: number } }>(tmpPath);

    for (let i = 0; i < 500; i++) {
      queue.add('noop', { n: i });
    }

    const walPath = `${tmpPath}-wal`;
    expect(existsSync(walPath)).toBe(true);
    expect(statSync(walPath).size).toBeGreaterThan(0);

    const result = queue.db.query('PRAGMA wal_checkpoint(PASSIVE)').get() as {
      busy: number;
      log: number;
      checkpointed: number;
    };
    expect(result.log).toBeGreaterThan(0);
    expect(result.checkpointed).toBe(result.log);

    queue.close();
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        unlinkSync(`${tmpPath}${suffix}`);
      } catch {
        // ignore
      }
    }
  });
});
