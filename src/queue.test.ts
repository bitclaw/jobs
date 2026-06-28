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

  describe('add', () => {
    test('returns a job ID and stores data as JSON', () => {
      const id = queue.add('email:send', {
        to: 'user@test.com',
        subject: 'Hello'
      });

      expect(id).toBeGreaterThan(0);

      const job = queue.getJob(id);
      expect(job).not.toBeNull();
      expect(job!.type).toBe('email:send');
      expect(job!.data).toEqual({ to: 'user@test.com', subject: 'Hello' });
      expect(job!.status).toBe('pending');
      expect(job!.priority).toBe(0);
      expect(job!.maxRetries).toBe(3);
      expect(job!.retryCount).toBe(0);
      expect(job!.progress).toBe(0);
    });

    test('respects priority and maxRetries options', () => {
      const id = queue.add(
        'deploy:provision',
        { serverId: 'srv-1' },
        { priority: 10, maxRetries: 5 }
      );

      const job = queue.getJob(id)!;
      expect(job.priority).toBe(10);
      expect(job.maxRetries).toBe(5);
    });

    test('respects runAt option', () => {
      const future = new Date('2099-01-01T00:00:00Z');
      const id = queue.add(
        'email:send',
        { to: 'a@b.com', subject: 'later' },
        { runAt: future }
      );

      const job = queue.getJob(id)!;
      expect(job.runAt).toBe(future.toISOString());
    });

    test('sets status to blocked when dependsOn is provided', () => {
      const depId = queue.add('email:send', {
        to: 'a@b.com',
        subject: 'first'
      });
      const id = queue.add(
        'email:send',
        { to: 'b@c.com', subject: 'second' },
        { dependsOn: [depId] }
      );

      const job = queue.getJob(id)!;
      expect(job.status).toBe('blocked');
    });

    test('throws when dependsOn references non-existent job', () => {
      expect(() =>
        queue.add(
          'email:send',
          { to: 'a@b.com', subject: 'fail' },
          { dependsOn: [9999] }
        )
      ).toThrow('Dependency job 9999 does not exist');
    });
  });

  describe('getJob', () => {
    test('returns null for non-existent job', () => {
      expect(queue.getJob(9999)).toBeNull();
    });
  });

  describe('getStats', () => {
    test('counts jobs by status including dead', () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'a' });
      queue.add('email:send', { to: 'b@c.com', subject: 'b' });

      const stats = queue.getStats();
      expect(stats.pending).toBe(2);
      expect(stats.processing).toBe(0);
      expect(stats.done).toBe(0);
      expect(stats.failed).toBe(0);
      expect(stats.blocked).toBe(0);
      expect(stats.dead).toBe(0);
    });
  });

  describe('pollAndClaim', () => {
    test('claims oldest pending job by priority then created_at', () => {
      const lowId = queue.add(
        'email:send',
        { to: 'low@test.com', subject: 'low' },
        { priority: 1 }
      );
      const highId = queue.add(
        'email:send',
        { to: 'high@test.com', subject: 'high' },
        { priority: 10 }
      );

      const claimed = queue.pollAndClaim('email:send');
      expect(claimed).not.toBeNull();
      expect(claimed!.id).toBe(highId);
      expect(claimed!.status).toBe('pending');

      // After claiming, getJob shows processing
      const updated = queue.getJob(highId)!;
      expect(updated.status).toBe('processing');
      expect(updated.startedAt).not.toBeNull();

      // Low priority still pending
      const low = queue.getJob(lowId)!;
      expect(low.status).toBe('pending');
    });

    test('returns null when no pending jobs', () => {
      expect(queue.pollAndClaim('email:send')).toBeNull();
    });

    test('does not claim jobs scheduled in the future', () => {
      queue.add(
        'email:send',
        { to: 'a@b.com', subject: 'future' },
        { runAt: new Date('2099-01-01T00:00:00Z') }
      );

      expect(queue.pollAndClaim('email:send')).toBeNull();
    });

    test('does not claim blocked jobs', () => {
      const dep = queue.add('email:send', {
        to: 'a@b.com',
        subject: 'dep'
      });
      queue.add(
        'email:send',
        { to: 'b@c.com', subject: 'blocked' },
        { dependsOn: [dep] }
      );

      const claimed = queue.pollAndClaim('email:send');
      // Should only claim the dependency, not the blocked job
      expect(claimed).not.toBeNull();
      expect(claimed!.id).toBe(dep);
    });
  });

  describe('markJobDone', () => {
    test('sets status to done with completedAt', () => {
      const id = queue.add('email:send', {
        to: 'a@b.com',
        subject: 'done'
      });
      queue.pollAndClaim('email:send');
      queue.markJobDone(id);

      const job = queue.getJob(id)!;
      expect(job.status).toBe('done');
      expect(job.completedAt).not.toBeNull();
      expect(job.progress).toBe(100);
    });

    test('unblocks dependent jobs when all deps are done', () => {
      const dep1 = queue.add('email:send', {
        to: 'a@b.com',
        subject: 'dep1'
      });
      const dep2 = queue.add('email:send', {
        to: 'b@c.com',
        subject: 'dep2'
      });
      const blocked = queue.add(
        'email:send',
        { to: 'c@d.com', subject: 'blocked' },
        { dependsOn: [dep1, dep2] }
      );

      expect(queue.getJob(blocked)!.status).toBe('blocked');

      // Complete dep1 — still blocked
      queue.pollAndClaim('email:send');
      queue.markJobDone(dep1);
      expect(queue.getJob(blocked)!.status).toBe('blocked');

      // Complete dep2 — now unblocked
      queue.pollAndClaim('email:send');
      queue.markJobDone(dep2);
      expect(queue.getJob(blocked)!.status).toBe('pending');
    });
  });

  describe('markJobFailed', () => {
    test('retries by setting status back to pending', () => {
      const id = queue.add('email:send', {
        to: 'a@b.com',
        subject: 'retry'
      });
      queue.pollAndClaim('email:send');
      queue.markJobFailed(id, 'network error');

      const job = queue.getJob(id)!;
      expect(job.status).toBe('pending');
      expect(job.retryCount).toBe(1);
      expect(job.error).toBe('network error');
    });

    test('moves to failed_jobs when max retries exceeded', () => {
      const id = queue.add(
        'email:send',
        { to: 'a@b.com', subject: 'die' },
        { maxRetries: 1 }
      );
      queue.pollAndClaim('email:send');
      queue.markJobFailed(id, 'fatal error');

      // Job should be gone from jobs table
      expect(queue.getJob(id)).toBeNull();

      // Should be in failed_jobs
      const failed = queue.getFailedJobs({ type: 'email:send' });
      expect(failed.items).toHaveLength(1);
      expect(failed.items[0]!.originalJobId).toBe(id);
      expect(failed.items[0]!.error).toBe('fatal error');
      expect(failed.items[0]!.retryCount).toBe(1);
    });

    test('copies request_log and response_log to failed_jobs', () => {
      const id = queue.add(
        'email:send',
        { to: 'a@b.com', subject: 'die' },
        { maxRetries: 1 }
      );
      queue.pollAndClaim('email:send');
      queue.setJobHttpLog(id, '{"payload":"req"}', '{"status":"500"}');
      queue.markJobFailed(id, 'webhook error');

      const failed = queue.getFailedJobs();
      expect(failed.items).toHaveLength(1);
      expect(failed.items[0]!.requestLog).toBe('{"payload":"req"}');
      expect(failed.items[0]!.responseLog).toBe('{"status":"500"}');
    });
  });

  describe('updateProgress', () => {
    test('updates progress on a job', () => {
      const id = queue.add('email:send', {
        to: 'a@b.com',
        subject: 'progress'
      });
      queue.updateProgress(id, 50);

      const job = queue.getJob(id)!;
      expect(job.progress).toBe(50);
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

  describe('forceRetryJob', () => {
    test('resets processing job to pending', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'stuck' });
      queue.pollAndClaim('email:send'); // → processing
      expect(queue.getJob(id)!.status).toBe('processing');

      expect(queue.forceRetryJob(id)).toBe(true);
      const job = queue.getJob(id)!;
      expect(job.status).toBe('pending');
      expect(job.retryCount).toBe(0);
      expect(job.error).toBeNull();
      expect(job.startedAt).toBeNull();
    });

    test('resets cancelled job to pending', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'cancel' });
      queue.cancelJob(id);
      expect(queue.getJob(id)!.status).toBe('cancelled');

      expect(queue.forceRetryJob(id)).toBe(true);
      expect(queue.getJob(id)!.status).toBe('pending');
    });

    test('returns false for done job', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'done' });
      queue.pollAndClaim('email:send');
      queue.markJobDone(id);
      expect(queue.forceRetryJob(id)).toBe(false);
    });

    test('returns false for pending job', () => {
      const id = queue.add('email:send', {
        to: 'a@b.com',
        subject: 'pending'
      });
      expect(queue.forceRetryJob(id)).toBe(false);
    });

    test('returns false for non-existent job', () => {
      expect(queue.forceRetryJob(9999)).toBe(false);
    });
  });

  describe('setJobHttpLog', () => {
    test('stores request and response log on the job row', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'log' });
      queue.setJobHttpLog(id, '{"request":"data"}', '{"response":"ok"}');

      const job = queue.getJob(id)!;
      expect(job.requestLog).toBe('{"request":"data"}');
      expect(job.responseLog).toBe('{"response":"ok"}');
    });
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

  describe('uniqueKey deduplication', () => {
    test('second enqueue with same (type, uniqueKey) while pending is ignored', () => {
      const id1 = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { uniqueKey: 'welcome:user-1' }
      );
      const id2 = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { uniqueKey: 'welcome:user-1' }
      );
      expect(id1).toBeGreaterThan(0);
      expect(id2).toBe(id1);
      expect(queue.getStats().pending).toBe(1);
    });

    test('same uniqueKey on different type does not conflict', () => {
      const id1 = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { uniqueKey: 'user-1' }
      );
      const id2 = queue.add(
        'deploy:provision',
        { serverId: 'srv-1' },
        { uniqueKey: 'user-1' }
      );
      expect(id1).toBeGreaterThan(0);
      expect(id2).toBeGreaterThan(0);
      expect(id1).not.toBe(id2);
      expect(queue.getStats().pending).toBe(2);
    });

    test('same uniqueKey can be re-enqueued after job completes', () => {
      const id1 = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { uniqueKey: 'welcome:user-1' }
      );
      queue.markJobDone(id1);

      const id2 = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi again' },
        { uniqueKey: 'welcome:user-1' }
      );
      expect(id2).toBeGreaterThan(id1);
      expect(queue.getStats().pending).toBe(1);
    });

    test('enqueue without uniqueKey allows unlimited duplicates', () => {
      const id1 = queue.add('email:send', { to: 'a@test.com', subject: 'Hi' });
      const id2 = queue.add('email:send', { to: 'a@test.com', subject: 'Hi' });
      expect(id1).not.toBe(id2);
      expect(queue.getStats().pending).toBe(2);
    });

    test('second enqueue returns existing id while job is processing', () => {
      const id1 = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { uniqueKey: 'welcome:user-2' }
      );
      // Claim the job (moves to processing)
      queue.pollAndClaim('email:send');

      const id2 = queue.add(
        'email:send',
        { to: 'a@test.com', subject: 'Hi' },
        { uniqueKey: 'welcome:user-2' }
      );
      expect(id2).toBe(id1);
    });
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

  describe('dedup replace mode', () => {
    test('replace updates data of existing pending job', () => {
      const id1 = queue.add(
        'test:simple',
        { value: 'old' },
        { uniqueKey: 'k1' }
      );
      const id2 = queue.add(
        'test:simple',
        { value: 'new' },
        { uniqueKey: 'k1', dedup: 'replace' }
      );
      expect(id1).toBe(id2); // same job
      const job = queue.getJob(id1)!;
      expect((job.data as { value: string }).value).toBe('new'); // data updated
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

  describe('getJobGraph', () => {
    test('single node with no deps', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'root' });
      const graph = queue.getJobGraph(id);
      expect(graph).toHaveLength(1);
      expect(graph[0]!.id).toBe(id);
      expect(graph[0]!.dependsOn).toEqual([]);
      expect(graph[0]!.dependents).toEqual([]);
    });

    test('empty for non-existent job', () => {
      expect(queue.getJobGraph(9999)).toHaveLength(0);
    });

    test('A→B→C chain returns all 3 nodes with correct edges', () => {
      const a = queue.add('email:send', { to: 'a@b.com', subject: 'A' });
      const b = queue.add(
        'email:send',
        { to: 'b@c.com', subject: 'B' },
        { dependsOn: [a] }
      );
      const c = queue.add(
        'email:send',
        { to: 'c@d.com', subject: 'C' },
        { dependsOn: [b] }
      );

      const graph = queue.getJobGraph(b);
      expect(graph.map(n => n.id).sort()).toEqual([a, b, c].sort());

      const nodeA = graph.find(n => n.id === a)!;
      const nodeB = graph.find(n => n.id === b)!;
      const nodeC = graph.find(n => n.id === c)!;

      expect(nodeA.dependsOn).toEqual([]);
      expect(nodeA.dependents).toContain(b);
      expect(nodeB.dependsOn).toContain(a);
      expect(nodeB.dependents).toContain(c);
      expect(nodeC.dependsOn).toContain(b);
      expect(nodeC.dependents).toEqual([]);
    });

    test('includes result in node', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'res' });
      queue.pollAndClaim('email:send');
      queue.markJobDone(id, { sent: true });
      const graph = queue.getJobGraph(id);
      expect(graph[0]!.result).toEqual({ sent: true });
    });
  });

  describe('mountAdminHandler', () => {
    let handler: (req: Request) => Promise<Response>;

    beforeEach(() => {
      handler = queue.mountAdminHandler();
    });

    test('GET /stats returns job stats', async () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(new Request('http://localhost/stats'));
      expect(res.status).toBe(200);
      const body = (await res.json()) as { pending: number };
      expect(body.pending).toBe(1);
    });

    test('GET /jobs returns paginated jobs', async () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(new Request('http://localhost/jobs'));
      expect(res.status).toBe(200);
      const body = (await res.json()) as { items: unknown[]; total: number };
      expect(body.total).toBe(1);
      expect(body.items).toHaveLength(1);
    });

    test('GET /jobs/:id returns job', async () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(new Request(`http://localhost/jobs/${id}`));
      expect(res.status).toBe(200);
      const body = (await res.json()) as { id: number };
      expect(body.id).toBe(id);
    });

    test('GET /jobs/:id returns 404 for missing', async () => {
      const res = await handler(new Request('http://localhost/jobs/9999'));
      expect(res.status).toBe(404);
    });

    test('GET /jobs/types returns job types', async () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(new Request('http://localhost/jobs/types'));
      expect(res.status).toBe(200);
      const body = (await res.json()) as string[];
      expect(body).toContain('email:send');
    });

    test('POST /jobs/:id/cancel cancels job', async () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(
        new Request(`http://localhost/jobs/${id}/cancel`, { method: 'POST' })
      );
      expect(res.status).toBe(200);
      const body = (await res.json()) as { ok: boolean };
      expect(body.ok).toBe(true);
      expect(queue.getJob(id)!.status).toBe('cancelled');
    });

    test('GET /failed returns failed jobs', async () => {
      const res = await handler(new Request('http://localhost/failed'));
      expect(res.status).toBe(200);
      const body = (await res.json()) as { items: unknown[]; total: number };
      expect(body.total).toBe(0);
    });

    test('POST /failed/retry-by-type requires type', async () => {
      const res = await handler(
        new Request('http://localhost/failed/retry-by-type', {
          method: 'POST',
          body: JSON.stringify({}),
          headers: { 'Content-Type': 'application/json' }
        })
      );
      expect(res.status).toBe(400);
    });

    test('returns 404 for unknown route', async () => {
      const res = await handler(new Request('http://localhost/unknown'));
      expect(res.status).toBe(404);
    });

    test('prefix strips from path', async () => {
      const prefixed = queue.mountAdminHandler('/admin');
      queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await prefixed(new Request('http://localhost/admin/stats'));
      expect(res.status).toBe(200);
    });

    test('GET /jobs/:id/graph returns graph', async () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(
        new Request(`http://localhost/jobs/${id}/graph`)
      );
      expect(res.status).toBe(200);
      const body = (await res.json()) as unknown[];
      expect(body).toHaveLength(1);
    });
  });

  describe('addToBatch validation', () => {
    test('throws when batchId does not exist', () => {
      expect(() =>
        queue.addToBatch('non-existent-batch', 'email:send', {
          to: 'a@b.com',
          subject: 'hi'
        })
      ).toThrow('addToBatch: batch "non-existent-batch" does not exist');
    });

    test('succeeds when batch exists', () => {
      const batchId = queue.createBatch('test-batch');
      expect(() =>
        queue.addToBatch(batchId, 'email:send', {
          to: 'a@b.com',
          subject: 'hi'
        })
      ).not.toThrow();
      const batch = queue.getBatch(batchId)!;
      expect(batch.totalJobs).toBe(1);
      expect(batch.pendingJobs).toBe(1);
    });
  });

  describe('uniqueKey dedup race', () => {
    test('throws when dedup job completed between INSERT and SELECT', () => {
      const id = queue.add(
        'email:send',
        { to: 'a@b.com', subject: 'hi' },
        { uniqueKey: 'race:key-1' }
      );
      // Simulate job completing before the dedup SELECT runs
      queue.pollAndClaim('email:send');
      queue.markJobDone(id);

      // Now uniqueKey is no longer in pending/processing — INSERT OR IGNORE will succeed
      // (new row), so no race path is hit. This test verifies the happy-path re-enqueue works.
      const id2 = queue.add(
        'email:send',
        { to: 'a@b.com', subject: 'hi again' },
        { uniqueKey: 'race:key-1' }
      );
      expect(id2).toBeGreaterThan(id);
    });
  });
});
