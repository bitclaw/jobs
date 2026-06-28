import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { JobQueue } from './queue';

type TestJobs = {
  'email:send': { to: string };
  'batch:then': { result: string };
  'batch:finally': { cleanup: boolean };
};

describe('Job Batches', () => {
  let queue: JobQueue<TestJobs>;

  beforeEach(() => {
    queue = new JobQueue<TestJobs>(':memory:');
  });

  afterEach(() => {
    queue.close();
  });

  test('createBatch returns a UUID and stores the batch', () => {
    const id = queue.createBatch('test-batch');
    expect(id).toBeTruthy();

    const batch = queue.getBatch(id);
    expect(batch).not.toBeNull();
    expect(batch!.name).toBe('test-batch');
    expect(batch!.totalJobs).toBe(0);
    expect(batch!.pendingJobs).toBe(0);
    expect(batch!.failedJobs).toBe(0);
    expect(batch!.failedJobIds).toEqual([]);
    expect(batch!.finishedAt).toBeNull();
  });

  test('addToBatch increments counters and links job', () => {
    const batchId = queue.createBatch('send-emails');
    const jobId = queue.addToBatch(batchId, 'email:send', { to: 'a@b.com' });

    expect(jobId).toBeGreaterThan(0);

    const job = queue.getJob(jobId);
    expect(job).not.toBeNull();
    expect(job!.batchId).toBe(batchId);

    const batch = queue.getBatch(batchId)!;
    expect(batch.totalJobs).toBe(1);
    expect(batch.pendingJobs).toBe(1);
  });

  test('batch finishes when all jobs complete successfully', () => {
    const batchId = queue.createBatch('send-emails');
    const id1 = queue.addToBatch(batchId, 'email:send', { to: 'a@b.com' });
    const id2 = queue.addToBatch(batchId, 'email:send', { to: 'c@d.com' });

    // Process job 1
    queue.pollAndClaim('email:send');
    queue.markJobDone(id1);

    let batch = queue.getBatch(batchId)!;
    expect(batch.pendingJobs).toBe(1);
    expect(batch.finishedAt).toBeNull();

    // Process job 2
    queue.pollAndClaim('email:send');
    queue.markJobDone(id2);

    batch = queue.getBatch(batchId)!;
    expect(batch.pendingJobs).toBe(0);
    expect(batch.finishedAt).not.toBeNull();
  });

  test('then callback fires on success (zero failures)', () => {
    const batchId = queue.createBatch('send-emails', {
      thenType: 'batch:then',
      thenData: { result: 'ok' }
    });
    const jobId = queue.addToBatch(batchId, 'email:send', { to: 'a@b.com' });

    queue.pollAndClaim('email:send');
    queue.markJobDone(jobId);

    // The "then" job should be enqueued
    const thenJob = queue.pollAndClaim('batch:then');
    expect(thenJob).not.toBeNull();
    expect(thenJob!.data).toEqual({ result: 'ok' });
  });

  test('then callback does NOT fire when there are failures', () => {
    const batchId = queue.createBatch('send-emails', {
      thenType: 'batch:then',
      thenData: { result: 'ok' }
    });
    queue.addToBatch(batchId, 'email:send', {
      to: 'a@b.com'
    });

    // Make the job fail permanently (maxRetries=1, so first failure = dead)
    const jobId = (
      queue.db
        .query(
          "SELECT id FROM jobs WHERE type = 'email:send' AND batch_id = $batchId"
        )
        .get({ $batchId: batchId }) as { id: number }
    ).id;

    // Override max_retries to 1 so it dies on first failure
    queue.db
      .query('UPDATE jobs SET max_retries = 1 WHERE id = $id')
      .run({ $id: jobId });

    queue.pollAndClaim('email:send');
    queue.markJobFailed(jobId, 'boom');

    // "then" should NOT be enqueued
    const thenJob = queue.pollAndClaim('batch:then');
    expect(thenJob).toBeNull();
  });

  test('finally callback fires regardless of failures', () => {
    const batchId = queue.createBatch('send-emails', {
      finallyType: 'batch:finally',
      finallyData: { cleanup: true }
    });
    queue.addToBatch(batchId, 'email:send', { to: 'a@b.com' });

    // Make the job fail permanently
    const jobId = (
      queue.db
        .query(
          "SELECT id FROM jobs WHERE type = 'email:send' AND batch_id = $batchId"
        )
        .get({ $batchId: batchId }) as { id: number }
    ).id;
    queue.db
      .query('UPDATE jobs SET max_retries = 1 WHERE id = $id')
      .run({ $id: jobId });

    queue.pollAndClaim('email:send');
    queue.markJobFailed(jobId, 'boom');

    // "finally" should be enqueued
    const finallyJob = queue.pollAndClaim('batch:finally');
    expect(finallyJob).not.toBeNull();
    expect(finallyJob!.data).toEqual({ cleanup: true });
  });

  test('failure tracking increments failed_jobs and records IDs', () => {
    const batchId = queue.createBatch('send-emails');
    queue.addToBatch(batchId, 'email:send', { to: 'a@b.com' });
    queue.addToBatch(batchId, 'email:send', { to: 'c@d.com' });

    // Get the first job's ID
    const firstJobId = (
      queue.db
        .query(
          "SELECT id FROM jobs WHERE type = 'email:send' AND batch_id = $batchId ORDER BY id LIMIT 1"
        )
        .get({ $batchId: batchId }) as { id: number }
    ).id;

    // Kill first job permanently
    queue.db
      .query('UPDATE jobs SET max_retries = 1 WHERE id = $id')
      .run({ $id: firstJobId });
    queue.pollAndClaim('email:send');
    queue.markJobFailed(firstJobId, 'failed');

    const batch = queue.getBatch(batchId)!;
    expect(batch.failedJobs).toBe(1);
    expect(batch.failedJobIds).toContain(firstJobId);
  });

  test('retry does not decrement pending_jobs (only permanent failure does)', () => {
    const batchId = queue.createBatch('send-emails');
    queue.addToBatch(batchId, 'email:send', { to: 'a@b.com' });

    const jobId = (
      queue.db
        .query(
          "SELECT id FROM jobs WHERE type = 'email:send' AND batch_id = $batchId"
        )
        .get({ $batchId: batchId }) as { id: number }
    ).id;

    // Default maxRetries=3, so first failure is a retry not permanent death
    queue.pollAndClaim('email:send');
    queue.markJobFailed(jobId, 'transient error');

    const batch = queue.getBatch(batchId)!;
    // Still pending , job is retrying, not dead
    expect(batch.pendingJobs).toBe(1);
    expect(batch.failedJobs).toBe(0);
  });

  test('cancelBatch cancels pending jobs and sets cancelled_at', () => {
    const batchId = queue.createBatch('send-emails');
    const id1 = queue.addToBatch(batchId, 'email:send', { to: 'a@b.com' });
    const id2 = queue.addToBatch(batchId, 'email:send', { to: 'c@d.com' });

    queue.cancelBatch(batchId);

    const batch = queue.getBatch(batchId)!;
    expect(batch.cancelledAt).not.toBeNull();

    const job1 = queue.getJob(id1)!;
    const job2 = queue.getJob(id2)!;
    expect(job1.status).toBe('cancelled');
    expect(job2.status).toBe('cancelled');
  });

  test('getBatch returns null for non-existent batch', () => {
    expect(queue.getBatch('nonexistent')).toBeNull();
  });

  test('batch with both then and finally callbacks', () => {
    const batchId = queue.createBatch('full-batch', {
      thenType: 'batch:then',
      thenData: { result: 'done' },
      finallyType: 'batch:finally',
      finallyData: { cleanup: true }
    });
    queue.addToBatch(batchId, 'email:send', { to: 'a@b.com' });

    const jobId = (
      queue.db
        .query(
          "SELECT id FROM jobs WHERE type = 'email:send' AND batch_id = $batchId"
        )
        .get({ $batchId: batchId }) as { id: number }
    ).id;

    queue.pollAndClaim('email:send');
    queue.markJobDone(jobId);

    // Both should be enqueued
    const thenJob = queue.pollAndClaim('batch:then');
    const finallyJob = queue.pollAndClaim('batch:finally');
    expect(thenJob).not.toBeNull();
    expect(finallyJob).not.toBeNull();
  });
});
