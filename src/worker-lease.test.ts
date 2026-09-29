import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { JobQueue } from './queue';
import type { Job } from './types';

// Lease heartbeat, claim fencing, and timeout abort - the three ways a job
// used to execute twice while its first run was still alive.

type TestJobs = { 'test:work': { value: string } };

const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

const expireLease = (queue: JobQueue<TestJobs>, id: number) =>
  queue.db.run(
    "UPDATE jobs SET claimed_until = '2000-01-01T00:00:00.000Z' WHERE id = ?",
    [id]
  );

describe('worker lease heartbeat', () => {
  let queue: JobQueue<TestJobs>;
  beforeEach(() => {
    queue = new JobQueue<TestJobs>(':memory:');
  });
  afterEach(() => queue.close());

  test('given a handler running past leaseMs, the heartbeat keeps it claimed so no other poll can take it', async () => {
    let runs = 0;
    const id = queue.add('test:work', { value: 'long' });
    const worker = queue.createWorker({
      type: 'test:work',
      leaseMs: 60,
      heartbeatMs: 15,
      pollIntervalMs: 5,
      handler: async () => {
        runs++;
        await sleep(250);
      }
    });

    worker.start();
    await sleep(150); // well past the 60ms lease
    expect(queue.pollAndClaim('test:work', 60)).toBeNull();
    await sleep(200);
    await worker.stop();

    expect(runs).toBe(1);
    expect(queue.getJob(id)?.status).toBe('done');
  });

  test('given heartbeatMs 0 and no manual renew, an overrunning job is reclaimable (documents why the default is on)', async () => {
    let started = false;
    const id = queue.add('test:work', { value: 'long' });
    const worker = queue.createWorker({
      type: 'test:work',
      leaseMs: 30,
      heartbeatMs: 0,
      pollIntervalMs: 5,
      handler: async () => {
        started = true;
        await sleep(150);
      }
    });

    worker.start();
    await sleep(80);
    expect(started).toBe(true);
    expect(queue.pollAndClaim('test:work', 30)?.id).toBe(id);
    await worker.stop();
  });
});

describe('claim fencing', () => {
  let queue: JobQueue<TestJobs>;
  beforeEach(() => {
    queue = new JobQueue<TestJobs>(':memory:');
  });
  afterEach(() => queue.close());

  test('given a job reclaimed after its lease expired, the stale claim cannot finish, fail, or renew it', () => {
    const id = queue.add('test:work', { value: 'x' });
    const first = queue.pollAndClaim('test:work')!;
    expireLease(queue, id);
    const second = queue.pollAndClaim('test:work')!;

    expect(second.claimId).not.toBe(first.claimId);
    expect(queue.renewLease(id, 60_000, first.claimId!)).toBe(false);
    expect(queue.markJobDone(id, 'stale', first.claimId!)).toBe(false);
    expect(queue.markJobFailed(id, 'stale', first.claimId!)).toBe(false);
    expect(queue.markJobDead(id, 'stale', first.claimId!)).toBe(false);
    expect(queue.getJob(id)?.status).toBe('processing');

    expect(queue.renewLease(id, 60_000, second.claimId!)).toBe(true);
    expect(queue.markJobDone(id, 'ok', second.claimId!)).toBe(true);
    expect(queue.getJob(id)?.status).toBe('done');
  });

  test('given no claimId (admin/manual callers), mark methods behave as before', () => {
    const id = queue.add('test:work', { value: 'x' });
    queue.pollAndClaim('test:work');
    expect(queue.markJobDone(id)).toBe(true);
    expect(queue.getJob(id)?.status).toBe('done');
  });

  test('given a worker whose lease is taken over mid-run, its signal aborts and job:leaseLost fires instead of marking done', async () => {
    const lost: Job[] = [];
    queue.on('job:leaseLost', job => lost.push(job));
    let aborted = false;
    const id = queue.add('test:work', { value: 'x' });
    const worker = queue.createWorker({
      type: 'test:work',
      leaseMs: 60_000,
      heartbeatMs: 10,
      pollIntervalMs: 5,
      handler: async (_job, ctx) => {
        await new Promise<void>(r => {
          ctx.signal.addEventListener('abort', () => {
            aborted = true;
            r();
          });
        });
      }
    });

    worker.start();
    await sleep(30);
    expireLease(queue, id);
    queue.pollAndClaim('test:work'); // another worker takes over
    await sleep(40);
    await worker.stop();

    expect(aborted).toBe(true);
    expect(lost.map(j => j.id)).toEqual([id]);
    expect(queue.getJob(id)?.status).toBe('processing'); // new owner's run
  });
});

describe('timeout', () => {
  let queue: JobQueue<TestJobs>;
  beforeEach(() => {
    queue = new JobQueue<TestJobs>(':memory:');
  });
  afterEach(() => queue.close());

  test('given a handler exceeding timeoutMs, its signal is aborted and the job is failed for retry', async () => {
    let abortReason: unknown;
    const failures: Array<{ id: number; error: string }> = [];
    queue.on('job:failed', (job, error) =>
      failures.push({ id: job.id, error })
    );
    const id = queue.add('test:work', { value: 'slow' }, { maxRetries: 3 });
    const worker = queue.createWorker({
      type: 'test:work',
      timeoutMs: 20,
      pollIntervalMs: 5,
      handler: async (_job, ctx) => {
        await new Promise<void>(r => {
          ctx.signal.addEventListener('abort', () => {
            abortReason ??= ctx.signal.reason;
            r();
          });
        });
      }
    });

    worker.start();
    await sleep(40);
    worker.pause();
    await worker.stop();

    expect(String(abortReason)).toContain('timed out');
    expect(failures).toEqual([{ id, error: 'Job timed out after 20ms' }]);
  });
});
