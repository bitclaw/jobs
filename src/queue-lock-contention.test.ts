import { describe, expect, test } from 'bun:test';
import { unlinkSync } from 'node:fs';
import { JobQueue } from './queue';

// Regression test for a production crash (SQLiteError: database is locked,
// thrown from markJobFailed -> stmts.markFailed.run). Root cause:
// markJobDone/markJobDead/markJobFailed each ran `this.stmts.selectJob.get()`
// (a read) followed by a write, inside a *deferred* transaction
// (`this.db.transaction(fn)()`). A deferred transaction only acquires the
// write lock lazily, at its first write statement - if another connection
// commits a write in between this transaction's read and its write attempt,
// SQLite can return SQLITE_BUSY / SQLITE_BUSY_SNAPSHOT immediately,
// bypassing busy_timeout's retry loop entirely (that loop only covers
// waiting for a lock already held at BEGIN time, not a stale read snapshot
// discovered later). pollAndClaim's `claimTx.immediate()` already avoided
// this by acquiring the write lock at BEGIN. markJobDone/markJobDead/
// markJobFailed now do the same.
//
// Spawns a real child process to hold the write lock, same shape as
// schema-pragma.test.ts's own regression test for the analogous
// WAL-mode-switch bug.
describe('markJobFailed/markJobDone/markJobDead - lock contention', () => {
  test('given another connection holds the write lock, markJobFailed waits for it via busy_timeout instead of throwing "database is locked"', async () => {
    const tmpPath = `/tmp/jobs-lock-contention-test-${Date.now()}.db`;
    const queue = new JobQueue(tmpPath);
    const jobId = queue.add('test:job', { foo: 'bar' });

    const holderScript = `
      import { Database } from 'bun:sqlite';
      const db = new Database(${JSON.stringify(tmpPath)}, { create: true });
      db.run('PRAGMA busy_timeout = 10000');
      db.run('BEGIN IMMEDIATE');
      await Bun.sleep(300);
      db.run('COMMIT');
    `;
    const holder = Bun.spawn(['bun', '-e', holderScript], {
      stdout: 'inherit',
      stderr: 'inherit'
    });

    await Bun.sleep(80);

    const t0 = Date.now();
    expect(() => queue.markJobFailed(jobId, 'boom')).not.toThrow();
    expect(Date.now() - t0).toBeGreaterThan(100);

    await holder.exited;
    queue.close();

    try {
      unlinkSync(tmpPath);
      unlinkSync(`${tmpPath}-wal`);
      unlinkSync(`${tmpPath}-shm`);
    } catch {
      // ignore
    }
  });

  test('given another connection holds the write lock, markJobDone waits for it instead of throwing', async () => {
    const tmpPath = `/tmp/jobs-lock-contention-test-${Date.now()}-b.db`;
    const queue = new JobQueue(tmpPath);
    const jobId = queue.add('test:job', { foo: 'bar' });

    const holderScript = `
      import { Database } from 'bun:sqlite';
      const db = new Database(${JSON.stringify(tmpPath)}, { create: true });
      db.run('PRAGMA busy_timeout = 10000');
      db.run('BEGIN IMMEDIATE');
      await Bun.sleep(300);
      db.run('COMMIT');
    `;
    const holder = Bun.spawn(['bun', '-e', holderScript], {
      stdout: 'inherit',
      stderr: 'inherit'
    });

    await Bun.sleep(80);

    const t0 = Date.now();
    expect(() => queue.markJobDone(jobId, { ok: true })).not.toThrow();
    expect(Date.now() - t0).toBeGreaterThan(100);

    await holder.exited;
    queue.close();

    try {
      unlinkSync(tmpPath);
      unlinkSync(`${tmpPath}-wal`);
      unlinkSync(`${tmpPath}-shm`);
    } catch {
      // ignore
    }
  });
});
