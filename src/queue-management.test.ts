import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
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

  describe('listJobs', () => {
    test('returns paginated results with total', () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'a' });
      queue.add('email:send', { to: 'b@c.com', subject: 'b' });
      queue.add('deploy:provision', { serverId: 'srv-1' });

      const result = queue.listJobs({ limit: 2 });
      expect(result.items).toHaveLength(2);
      expect(result.total).toBe(3);
    });

    test('filters by status', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'a' });
      queue.add('email:send', { to: 'b@c.com', subject: 'b' });
      queue.pollAndClaim('email:send');
      queue.markJobDone(id);

      const done = queue.listJobs({ status: 'done' });
      expect(done.items).toHaveLength(1);
      expect(done.total).toBe(1);
      expect(done.items[0]!.status).toBe('done');

      const pending = queue.listJobs({ status: 'pending' });
      expect(pending.items).toHaveLength(1);
      expect(pending.total).toBe(1);
    });

    test('filters by type', () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'a' });
      queue.add('deploy:provision', { serverId: 'srv-1' });

      const result = queue.listJobs({ type: 'deploy:provision' });
      expect(result.items).toHaveLength(1);
      expect(result.total).toBe(1);
      expect(result.items[0]!.type).toBe('deploy:provision');
    });

    test('combined filters', () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'a' });
      const id = queue.add('email:send', { to: 'b@c.com', subject: 'b' });
      queue.add('deploy:provision', { serverId: 'srv-1' });
      queue.pollAndClaim('email:send');
      queue.markJobDone(id);

      const result = queue.listJobs({ status: 'done', type: 'email:send' });
      expect(result.items).toHaveLength(1);
      expect(result.total).toBe(1);
    });

    test('pagination with offset', () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'a' });
      queue.add('email:send', { to: 'b@c.com', subject: 'b' });
      queue.add('email:send', { to: 'c@d.com', subject: 'c' });

      const page1 = queue.listJobs({ limit: 2, offset: 0 });
      expect(page1.items).toHaveLength(2);
      expect(page1.total).toBe(3);

      const page2 = queue.listJobs({ limit: 2, offset: 2 });
      expect(page2.items).toHaveLength(1);
      expect(page2.total).toBe(3);
    });
  });

  describe('cancelJob', () => {
    test('cancels a pending job', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'cancel' });
      expect(queue.cancelJob(id)).toBe(true);
      expect(queue.getJob(id)!.status).toBe('cancelled');
    });

    test('cancels a blocked job', () => {
      const dep = queue.add('email:send', { to: 'a@b.com', subject: 'dep' });
      const id = queue.add(
        'email:send',
        { to: 'b@c.com', subject: 'blocked' },
        { dependsOn: [dep] }
      );
      expect(queue.getJob(id)!.status).toBe('blocked');
      expect(queue.cancelJob(id)).toBe(true);
      expect(queue.getJob(id)!.status).toBe('cancelled');
    });

    test('returns false for processing job', () => {
      const id = queue.add('email:send', {
        to: 'a@b.com',
        subject: 'processing'
      });
      queue.pollAndClaim('email:send');
      expect(queue.cancelJob(id)).toBe(false);
    });

    test('returns false for done job', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'done' });
      queue.pollAndClaim('email:send');
      queue.markJobDone(id);
      expect(queue.cancelJob(id)).toBe(false);
    });

    test('returns false for non-existent job', () => {
      expect(queue.cancelJob(9999)).toBe(false);
    });
  });

  describe('getJobTypes', () => {
    test('returns distinct sorted types', () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'a' });
      queue.add('deploy:provision', { serverId: 'srv-1' });
      queue.add('email:send', { to: 'b@c.com', subject: 'b' });

      const types = queue.getJobTypes();
      expect(types).toEqual(['deploy:provision', 'email:send']);
    });

    test('returns empty array when no jobs', () => {
      expect(queue.getJobTypes()).toEqual([]);
    });
  });

  describe('getFailedJobs (paginated)', () => {
    test('returns paginated result with total', () => {
      // Create 3 failed jobs
      for (let i = 0; i < 3; i++) {
        const id = queue.add(
          'email:send',
          { to: `user${i}@test.com`, subject: `die-${i}` },
          { maxRetries: 1 }
        );
        queue.pollAndClaim('email:send');
        queue.markJobFailed(id, `error-${i}`);
      }

      const page1 = queue.getFailedJobs({ limit: 2 });
      expect(page1.items).toHaveLength(2);
      expect(page1.total).toBe(3);

      const page2 = queue.getFailedJobs({ limit: 2, offset: 2 });
      expect(page2.items).toHaveLength(1);
      expect(page2.total).toBe(3);
    });

    test('filters by type with correct total', () => {
      const id1 = queue.add(
        'email:send',
        { to: 'a@b.com', subject: 'die' },
        { maxRetries: 1 }
      );
      queue.pollAndClaim('email:send');
      queue.markJobFailed(id1, 'error-1');

      const id2 = queue.add(
        'deploy:provision',
        { serverId: 'srv-1' },
        { maxRetries: 1 }
      );
      queue.pollAndClaim('deploy:provision');
      queue.markJobFailed(id2, 'error-2');

      const result = queue.getFailedJobs({ type: 'email:send' });
      expect(result.items).toHaveLength(1);
      expect(result.total).toBe(1);
    });

    test('round-trips requestLog and responseLog', () => {
      const id = queue.add(
        'email:send',
        { to: 'a@b.com', subject: 'log' },
        { maxRetries: 1 }
      );
      queue.pollAndClaim('email:send');
      queue.setJobHttpLog(id, 'req-body', 'res-body');
      queue.markJobFailed(id, 'error');

      const result = queue.getFailedJobs();
      expect(result.items[0]!.requestLog).toBe('req-body');
      expect(result.items[0]!.responseLog).toBe('res-body');
    });
  });

  describe('failed job management', () => {
    test('retryFailedJob creates a new pending job', () => {
      const id = queue.add(
        'email:send',
        { to: 'a@b.com', subject: 'retry-dead' },
        { maxRetries: 1 }
      );
      queue.pollAndClaim('email:send');
      queue.markJobFailed(id, 'fatal');

      const failed = queue.getFailedJobs();
      expect(failed.items).toHaveLength(1);

      const newId = queue.retryFailedJob(failed.items[0]!.id);
      expect(newId).toBeGreaterThan(0);

      const newJob = queue.getJob(newId)!;
      expect(newJob.status).toBe('pending');
      expect(newJob.type).toBe('email:send');

      // Failed job removed
      expect(queue.getFailedJobs().items).toHaveLength(0);
    });

    test('retryFailedJob throws for non-existent failed job', () => {
      expect(() => queue.retryFailedJob(9999)).toThrow(
        'Failed job 9999 not found'
      );
    });
  });

  describe('retryFailedJobsByType', () => {
    test('re-enqueues all failed jobs of given type', () => {
      const id1 = queue.add('test:simple', { value: 'a' }, { maxRetries: 1 });
      const id2 = queue.add('test:simple', { value: 'b' }, { maxRetries: 1 });
      queue.pollAndClaim('test:simple');
      queue.markJobFailed(id1, 'err1');
      queue.pollAndClaim('test:simple');
      queue.markJobFailed(id2, 'err2');

      expect(queue.getStats().dead).toBe(2);

      const count = queue.retryFailedJobsByType('test:simple');
      expect(count).toBe(2);
      expect(queue.getStats().dead).toBe(0);
      expect(queue.getStats().pending).toBe(2);
    });

    test('returns 0 for type with no failed jobs', () => {
      expect(queue.retryFailedJobsByType('test:simple')).toBe(0);
    });
  });

  describe('cancelByUniqueKey', () => {
    test('cancels pending job by unique key', () => {
      queue.add('test:simple', { value: 'a' }, { uniqueKey: 'k1' });
      const cancelled = queue.cancelByUniqueKey('test:simple', 'k1');
      expect(cancelled).toBe(true);
      const stats = queue.getStats();
      expect(stats.cancelled).toBe(1);
    });

    test('returns false if no matching job', () => {
      const cancelled = queue.cancelByUniqueKey('test:simple', 'nonexistent');
      expect(cancelled).toBe(false);
    });

    test('does not cancel processing job', () => {
      queue.add('test:simple', { value: 'a' }, { uniqueKey: 'k2' });
      queue.pollAndClaim('test:simple');
      const cancelled = queue.cancelByUniqueKey('test:simple', 'k2');
      expect(cancelled).toBe(false);
    });
  });

  describe('purge', () => {
    test('deletes done jobs older than threshold', async () => {
      const id = queue.add('email:send', {
        to: 'a@b.com',
        subject: 'purge'
      });
      queue.pollAndClaim('email:send');
      queue.markJobDone(id);

      // Wait so updated_at is in the past relative to cutoff
      await new Promise(resolve => setTimeout(resolve, 10));

      const count = queue.purge({ status: 'done', olderThanMs: 0 });
      expect(count).toBe(1);
      expect(queue.getJob(id)).toBeNull();
    });

    test('does not delete recent jobs', () => {
      const id = queue.add('email:send', {
        to: 'a@b.com',
        subject: 'keep'
      });
      queue.pollAndClaim('email:send');
      queue.markJobDone(id);

      // Purge with huge threshold
      const count = queue.purge({
        status: 'done',
        olderThanMs: 999_999_999
      });
      expect(count).toBe(0);
    });
  });

  describe('per-job result', () => {
    test('getJobResult returns null for no result', () => {
      const id = queue.add('test:simple', { value: 'a' });
      expect(queue.getJobResult(id)).toBeNull();
    });

    test('markJobDone stores result', () => {
      const id = queue.add('test:simple', { value: 'a' });
      queue.pollAndClaim('test:simple');
      queue.markJobDone(id, { ok: true, count: 42 });
      expect(queue.getJobResult<{ ok: boolean; count: number }>(id)).toEqual({
        ok: true,
        count: 42
      });
    });

    test('getJobResult type param works', () => {
      const id = queue.add('test:simple', { value: 'a' });
      queue.pollAndClaim('test:simple');
      queue.markJobDone(id, 'hello');
      const r = queue.getJobResult<string>(id);
      expect(r).toBe('hello');
    });
  });

  describe('job TTL', () => {
    test('expired job not claimed', async () => {
      const past = new Date(Date.now() - 1000);
      queue.add('test:simple', { value: 'a' }, { expireAt: past });
      const job = queue.pollAndClaim('test:simple');
      expect(job).toBeNull();
    });

    test('non-expired job still claimed', () => {
      const future = new Date(Date.now() + 60_000);
      queue.add('test:simple', { value: 'a' }, { expireAt: future });
      const job = queue.pollAndClaim('test:simple');
      expect(job).not.toBeNull();
    });

    test('purgeExpiredJobs removes expired pending', () => {
      const past = new Date(Date.now() - 1000);
      queue.add('test:simple', { value: 'a' }, { expireAt: past });
      const count = queue.purgeExpiredJobs();
      expect(count).toBe(1);
      expect(queue.getStats().pending).toBe(0);
    });
  });
});
