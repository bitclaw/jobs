import { afterEach, beforeEach, describe, expect, test, vi } from 'bun:test';
import { JobQueue } from './queue';

type TestJobs = {
  'email:send': { to: string; subject: string };
  'deploy:provision': { serverId: string };
  'test:simple': { value: string };
};

describe('JobQueue', () => {
  let queue: JobQueue<TestJobs>;

  beforeEach(() => {
    queue = new JobQueue<TestJobs>(':memory:');
  });

  afterEach(() => {
    queue.close();
  });

  describe('reconcileStaleJobs', () => {
    test('resets processing jobs stuck past threshold back to pending', () => {
      const id = queue.add('email:send', { to: 'a@test.com', subject: 'Hi' });
      queue.pollAndClaim('email:send'); // moves to processing

      // Back-date updated_at so it exceeds threshold
      queue.db
        .query(
          "UPDATE jobs SET updated_at = '2000-01-01T00:00:00.000Z' WHERE id = $id"
        )
        .run({ $id: id });

      const count = queue.reconcileStaleJobs(300_000);
      expect(count).toBe(1);

      const job = queue.getJob(id)!;
      expect(job.status).toBe('pending');
      expect(job.error).toContain('stale');
    });

    test('leaves processing jobs within threshold untouched', () => {
      queue.add('email:send', { to: 'a@test.com', subject: 'Hi' });
      queue.pollAndClaim('email:send');

      const count = queue.reconcileStaleJobs(300_000);
      expect(count).toBe(0);
    });

    test('does not touch pending or done jobs', () => {
      const id1 = queue.add('email:send', { to: 'a@test.com', subject: 'Hi' });
      const id2 = queue.add('email:send', { to: 'b@test.com', subject: 'Hi' });
      queue.pollAndClaim('email:send');
      queue.markJobDone(id1);

      queue.db
        .query("UPDATE jobs SET updated_at = '2000-01-01T00:00:00.000Z'")
        .run();

      const count = queue.reconcileStaleJobs(300_000);
      expect(count).toBe(0); // id2 is pending, id1 is done — neither resets

      expect(queue.getJob(id2)!.status).toBe('pending');
    });
  });

  describe('backoff', () => {
    test('fixed backoff delays retry by delayMs', () => {
      const before = Date.now();
      const id = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { maxRetries: 5, backoff: { type: 'fixed', delayMs: 5_000 } }
      );
      queue.pollAndClaim('email:send');
      queue.markJobFailed(id, 'transient error');

      const job = queue.getJob(id)!;
      expect(job.status).toBe('pending');
      const runAt = new Date(job.runAt).getTime();
      // run_at should be ~5000ms in the future
      expect(runAt).toBeGreaterThan(before + 4_000);
      expect(runAt).toBeLessThan(before + 10_000);
    });

    test('exponential backoff doubles delay each retry', () => {
      const before = Date.now();
      const id = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { maxRetries: 5, backoff: { type: 'exponential', delayMs: 1_000 } }
      );

      // First failure: retry_count=0 → delay = 1000 * 2^0 = 1000ms
      queue.pollAndClaim('email:send');
      queue.markJobFailed(id, 'err');
      const job1 = queue.getJob(id)!;
      expect(new Date(job1.runAt).getTime()).toBeGreaterThan(before + 500);
      expect(new Date(job1.runAt).getTime()).toBeLessThan(before + 3_000);

      // Second failure: retry_count=1 → delay = 1000 * 2^1 = 2000ms
      // Back-date run_at so pollAndClaim picks it up
      queue.db
        .query(
          "UPDATE jobs SET run_at = '2000-01-01T00:00:00.000Z' WHERE id = $id"
        )
        .run({ $id: id });
      const before2 = Date.now();
      queue.pollAndClaim('email:send');
      queue.markJobFailed(id, 'err');
      const job2 = queue.getJob(id)!;
      expect(new Date(job2.runAt).getTime()).toBeGreaterThan(before2 + 1_000);
      expect(new Date(job2.runAt).getTime()).toBeLessThan(before2 + 5_000);
    });

    test('no backoff: retry is immediate (run_at ~ now)', () => {
      const before = Date.now();
      const id = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { maxRetries: 5 }
      );
      queue.pollAndClaim('email:send');
      queue.markJobFailed(id, 'err');

      const job = queue.getJob(id)!;
      const runAt = new Date(job.runAt).getTime();
      // Should be at or after job processing time, not delayed
      expect(runAt).toBeGreaterThanOrEqual(before);
      expect(runAt).toBeLessThan(before + 2_000);
    });

    test('exponential backoff caps at 1 hour', () => {
      const before = Date.now();
      const id = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { maxRetries: 99, backoff: { type: 'exponential', delayMs: 3_600_000 } }
      );
      // Simulate many retries by setting retry_count high
      queue.db
        .query('UPDATE jobs SET retry_count = 20 WHERE id = $id')
        .run({ $id: id });
      queue.pollAndClaim('email:send');
      queue.markJobFailed(id, 'err');

      const job = queue.getJob(id)!;
      const runAt = new Date(job.runAt).getTime();
      // Cap at 1h = 3_600_000ms
      expect(runAt).toBeLessThan(before + 3_600_001 + 1_000);
    });
  });

  describe('multi-process lease safety', () => {
    test('pollAndClaim sets claimed_until', () => {
      queue.add('test:simple', { value: 'a' });
      const job = queue.pollAndClaim('test:simple', 60_000);
      expect(job).not.toBeNull();
      // job is now processing with claimed_until ~60s from now
      const stats = queue.getStats();
      expect(stats.processing).toBe(1);
    });

    test('reclaims expired lease', async () => {
      queue.add('test:simple', { value: 'a' });
      // claim with 1ms lease (expires immediately)
      const job1 = queue.pollAndClaim('test:simple', 1);
      expect(job1).not.toBeNull();
      await new Promise(r => setTimeout(r, 10));
      // second poll reclaims expired lease
      const job2 = queue.pollAndClaim('test:simple', 60_000);
      expect(job2).not.toBeNull();
      expect(job2!.id).toBe(job1!.id);
    });

    test('does not reclaim active lease', () => {
      queue.add('test:simple', { value: 'a' });
      const job1 = queue.pollAndClaim('test:simple', 60_000);
      expect(job1).not.toBeNull();
      const job2 = queue.pollAndClaim('test:simple', 60_000);
      expect(job2).toBeNull();
    });

    test('renewLease extends claimed_until', async () => {
      queue.add('test:simple', { value: 'a' });
      const job = queue.pollAndClaim('test:simple', 1);
      expect(job).not.toBeNull();
      // renew with 60s lease
      queue.renewLease(job!.id, 60_000);
      await new Promise(r => setTimeout(r, 10));
      // should NOT be reclaimable now
      const job2 = queue.pollAndClaim('test:simple', 60_000);
      expect(job2).toBeNull();
    });
  });

  describe('getJobByUniqueKey', () => {
    test('returns pending job by type + uniqueKey', () => {
      const id = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { uniqueKey: 'welcome:u1' }
      );
      const job = queue.getJobByUniqueKey('email:send', 'welcome:u1');
      expect(job).not.toBeNull();
      expect(job!.id).toBe(id);
      expect(job!.status).toBe('pending');
    });

    test('returns processing job', () => {
      const id = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { uniqueKey: 'welcome:u2' }
      );
      queue.pollAndClaim('email:send');
      const job = queue.getJobByUniqueKey('email:send', 'welcome:u2');
      expect(job).not.toBeNull();
      expect(job!.id).toBe(id);
      expect(job!.status).toBe('processing');
    });

    test('returns null after job completes', () => {
      const id = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { uniqueKey: 'welcome:u3' }
      );
      queue.pollAndClaim('email:send');
      queue.markJobDone(id);
      expect(queue.getJobByUniqueKey('email:send', 'welcome:u3')).toBeNull();
    });

    test('returns null for wrong type', () => {
      queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { uniqueKey: 'key:shared' }
      );
      expect(
        queue.getJobByUniqueKey('deploy:provision', 'key:shared')
      ).toBeNull();
    });

    test('returns null when no uniqueKey match', () => {
      expect(queue.getJobByUniqueKey('email:send', 'nonexistent')).toBeNull();
    });
  });

  describe('typed event emitter', () => {
    test('job:done fires after markJobDone', () => {
      const handler = vi.fn();
      queue.on('job:done', handler);
      const id = queue.add('test:simple', { value: 'a' });
      queue.pollAndClaim('test:simple');
      queue.markJobDone(id);
      expect(handler).toHaveBeenCalledTimes(1);
      expect(handler.mock.calls[0]![0].id).toBe(id);
    });

    test('job:dead fires after markJobDead', () => {
      const handler = vi.fn();
      queue.on('job:dead', handler);
      const id = queue.add('test:simple', { value: 'a' });
      queue.pollAndClaim('test:simple');
      queue.markJobDead(id, 'fatal');
      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'test:simple' }),
        'fatal'
      );
    });

    test('on() returns unsubscribe fn', () => {
      const handler = vi.fn();
      const unsub = queue.on('job:done', handler);
      unsub();
      const id = queue.add('test:simple', { value: 'a' });
      queue.pollAndClaim('test:simple');
      queue.markJobDone(id);
      expect(handler).not.toHaveBeenCalled();
    });

    test('once() fires only one time', () => {
      const handler = vi.fn();
      queue.once('job:done', handler);
      for (let i = 0; i < 3; i++) {
        const id = queue.add('test:simple', { value: `j${i}` });
        queue.pollAndClaim('test:simple');
        queue.markJobDone(id);
      }
      expect(handler).toHaveBeenCalledTimes(1);
    });

    test('job:stale fires with count from reconcileStaleJobs', () => {
      const handler = vi.fn();
      queue.on('job:stale', handler);
      // Manually insert a stale processing job
      queue.db.run(
        "INSERT INTO jobs (type, data, status, priority, max_retries, run_at, updated_at) VALUES ('test:simple', '{}', 'processing', 0, 3, datetime('now'), datetime('now', '-1 hour'))"
      );
      const count = queue.reconcileStaleJobs(1); // 1ms threshold
      expect(count).toBe(1);
      expect(handler).toHaveBeenCalledWith(1);
    });
  });

  describe('middleware', () => {
    test('use() registers middleware', () => {
      queue.use(async (_job, next) => next());
      expect(queue.middlewares).toHaveLength(1);
    });

    test('multiple use() calls stack', () => {
      queue.use(async (_job, next) => next());
      queue.use(async (_job, next) => next());
      expect(queue.middlewares).toHaveLength(2);
    });
  });
});
