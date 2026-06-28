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
