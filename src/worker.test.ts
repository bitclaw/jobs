import { afterEach, beforeEach, describe, expect, test, vi } from 'bun:test';
import { JobQueue } from './queue';
import type { Job } from './types';

type TestJobs = {
  'test:work': { value: string };
};

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe('JobWorker', () => {
  let queue: JobQueue<TestJobs>;

  beforeEach(() => {
    queue = new JobQueue<TestJobs>(':memory:');
  });

  afterEach(() => {
    queue.close();
  });

  test('processes a pending job and marks it done', async () => {
    const handled = vi.fn<(j: Job<{ value: string }>) => Promise<void>>();
    handled.mockResolvedValue(undefined);

    const id = queue.add('test:work', { value: 'hello' });
    const worker = queue.createWorker({
      type: 'test:work',
      handler: async (received, _ctx) => {
        handled(received);
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(50);
    await worker.stop();

    expect(handled).toHaveBeenCalledTimes(1);
    const calledJob = handled.mock.calls[0]![0]!;
    expect(calledJob.data.value).toBe('hello');

    const job = queue.getJob(id)!;
    expect(job.status).toBe('done');
    expect(job.progress).toBe(100);
  });

  test('provides JobContext with progress reporting', async () => {
    const id = queue.add('test:work', { value: 'progress' });
    const worker = queue.createWorker({
      type: 'test:work',
      handler: async (_job, ctx) => {
        ctx.reportProgress(50);
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(50);
    await worker.stop();

    // After done, progress is 100 (markJobDone sets it)
    const job = queue.getJob(id)!;
    expect(job.status).toBe('done');
    expect(job.progress).toBe(100);
  });

  test('provides abort signal via JobContext', async () => {
    let receivedSignal: AbortSignal | null = null;

    queue.add('test:work', { value: 'signal' });
    const worker = queue.createWorker({
      type: 'test:work',
      handler: async (_job, ctx) => {
        receivedSignal = ctx.signal;
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(50);
    await worker.stop();

    expect(receivedSignal).not.toBeNull();
    expect(receivedSignal!.aborted).toBe(true); // aborted after stop
  });

  test('retries on handler error', async () => {
    let callCount = 0;
    const id = queue.add('test:work', { value: 'retry' });

    const worker = queue.createWorker({
      type: 'test:work',
      handler: async () => {
        callCount++;
        if (callCount < 3) {
          throw new Error('transient');
        }
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(200);
    await worker.stop();

    expect(callCount).toBe(3);
    const job = queue.getJob(id)!;
    expect(job.status).toBe('done');
  });

  test('moves to failed_jobs on max retries exceeded', async () => {
    const id = queue.add('test:work', { value: 'die' }, { maxRetries: 1 });

    const onError = vi.fn();
    const worker = queue.createWorker({
      type: 'test:work',
      handler: async () => {
        throw new Error('permanent failure');
      },
      pollIntervalMs: 10,
      onError
    });

    worker.start();
    await sleep(100);
    await worker.stop();

    // Job removed from jobs table
    expect(queue.getJob(id)).toBeNull();

    // Exists in failed_jobs
    const failed = queue.getFailedJobs();
    expect(failed.items).toHaveLength(1);
    expect(failed.items[0]!.error).toBe('permanent failure');

    expect(onError).toHaveBeenCalled();
  });

  test('respects priority ordering', async () => {
    const order: string[] = [];

    queue.add('test:work', { value: 'low' }, { priority: 1 });
    queue.add('test:work', { value: 'high' }, { priority: 10 });
    queue.add('test:work', { value: 'medium' }, { priority: 5 });

    const worker = queue.createWorker({
      type: 'test:work',
      handler: async job => {
        order.push(job.data.value);
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(200);
    await worker.stop();

    expect(order).toEqual(['high', 'medium', 'low']);
  });

  test('stop() finishes current job then stops', async () => {
    let handlerFinished = false;

    queue.add('test:work', { value: 'slow' });
    const worker = queue.createWorker({
      type: 'test:work',
      handler: async () => {
        await sleep(50);
        handlerFinished = true;
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(30); // Let it start processing
    await worker.stop(); // Should wait for handler to finish

    expect(handlerFinished).toBe(true);
    expect(worker.isRunning).toBe(false);
  });

  test('does not claim jobs when rate limited', async () => {
    let processedCount = 0;

    // Add 5 jobs but limit to 2 per 200ms
    for (let i = 0; i < 5; i++) {
      queue.add('test:work', { value: `job-${i}` });
    }

    const worker = queue.createWorker({
      type: 'test:work',
      handler: async () => {
        processedCount++;
      },
      pollIntervalMs: 10,
      maxRate: { count: 2, windowMs: 200 }
    });

    worker.start();
    await sleep(100); // Not enough time for window to expire
    await worker.stop();

    expect(processedCount).toBeLessThanOrEqual(2);
  });
});
