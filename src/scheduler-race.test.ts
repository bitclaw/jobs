import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JobQueue } from './queue';
import { Scheduler } from './scheduler';
import type { ScheduleRow } from './types';

type TestJobs = { 'data:cleanup': Record<string, never> };

const PAST = '2020-01-01T00:00:00.000Z';

// Regression 2026-10-16: two tickers (a stray second cron line on
// runmist.com) both enqueued the same due schedule when they read it before
// either advanced next_run_at. tick() now claims each schedule with a
// compare-and-swap inside the insert's transaction.
describe('Scheduler.tick() with concurrent tickers', () => {
  let dir: string;
  let dbPath: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'scheduler-race-'));
    dbPath = join(dir, 'jobs.db');
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  const countJobs = (queue: JobQueue<TestJobs>) =>
    (
      queue.db
        .query("SELECT COUNT(*) AS n FROM jobs WHERE type = 'data:cleanup'")
        .get() as { n: number }
    ).n;

  test('a ticker holding a stale read of a due schedule does not enqueue it again', () => {
    const a = new JobQueue<TestJobs>(dbPath);
    const b = new JobQueue<TestJobs>(dbPath);
    const schedA = new Scheduler(a);
    const schedB = new Scheduler(b);
    schedA.register('cleanup', 'data:cleanup', '* * * * *', { overlap: true });
    a.db.query('UPDATE schedules SET next_run_at = $p').run({ $p: PAST });

    // B reads the due row, then A ticks first: B's read is now stale.
    const staleRows = b.db
      .query(
        'SELECT * FROM schedules WHERE enabled = 1 AND next_run_at <= $now'
      )
      .all({ $now: new Date().toISOString() }) as ScheduleRow[];
    expect(staleRows).toHaveLength(1);

    expect(schedA.tick()).toBe(1);
    const runDue = (
      schedB as unknown as { runDue: (r: ScheduleRow, now: string) => boolean }
    ).runDue.bind(schedB);
    expect(runDue(staleRows[0] as ScheduleRow, new Date().toISOString())).toBe(
      false
    );

    expect(countJobs(a)).toBe(1);
    schedA.stop();
    schedB.stop();
    a.close();
    b.close();
  });

  test('ten processes ticking the same due schedule at once enqueue it once', async () => {
    const setup = new JobQueue<TestJobs>(dbPath);
    new Scheduler(setup).register('cleanup', 'data:cleanup', '* * * * *', {
      overlap: true
    });
    setup.db.query('UPDATE schedules SET next_run_at = $p').run({ $p: PAST });

    const script = `
      import { JobQueue } from ${JSON.stringify(join(import.meta.dir, 'queue.ts'))};
      import { Scheduler } from ${JSON.stringify(join(import.meta.dir, 'scheduler.ts'))};
      const q = new JobQueue(${JSON.stringify(dbPath)});
      const s = new Scheduler(q);
      // Start together: wait for the shared go-time, then tick.
      while (Date.now() < Number(process.env.GO_AT)) {}
      s.tick();
      q.close();
    `;
    const goAt = String(Date.now() + 1500);
    const procs = Array.from({ length: 10 }, () =>
      Bun.spawn([process.execPath, '-e', script], {
        env: { ...process.env, GO_AT: goAt },
        stdout: 'inherit',
        stderr: 'inherit'
      })
    );
    const codes = await Promise.all(procs.map(p => p.exited));
    expect(codes.every(c => c === 0)).toBe(true);

    expect(countJobs(setup)).toBe(1);
    setup.close();
  }, 20_000);
});
