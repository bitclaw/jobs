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

  describe('setJobHttpLog', () => {
    test('stores request and response log on the job row', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'log' });
      queue.setJobHttpLog(id, '{"request":"data"}', '{"response":"ok"}');

      const job = queue.getJob(id)!;
      expect(job.requestLog).toBe('{"request":"data"}');
      expect(job.responseLog).toBe('{"response":"ok"}');
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
});
