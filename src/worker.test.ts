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

  test('concurrency: runs N jobs simultaneously', async () => {
    let concurrentPeak = 0;
    let current = 0;
    let completed = 0;

    for (let i = 0; i < 4; i++) {
      queue.add('test:work', { value: `job-${i}` });
    }

    const worker = queue.createWorker({
      type: 'test:work',
      concurrency: 3,
      handler: async () => {
        current++;
        if (current > concurrentPeak) concurrentPeak = current;
        await sleep(30);
        completed++;
        current--;
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(300);
    await worker.stop();

    expect(completed).toBe(4);
    expect(concurrentPeak).toBeGreaterThanOrEqual(2);
  });

  test('concurrency=1 default: no concurrent execution', async () => {
    let peak = 0;
    let current = 0;

    for (let i = 0; i < 3; i++) {
      queue.add('test:work', { value: `job-${i}` });
    }

    const worker = queue.createWorker({
      type: 'test:work',
      handler: async () => {
        current++;
        if (current > peak) peak = current;
        await sleep(20);
        current--;
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(300);
    await worker.stop();

    expect(peak).toBe(1);
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

  describe('retryIf predicate', () => {
    test('retryIf returning false → markJobDead skip retries', async () => {
      const id = queue.add('test:work', { value: 'die' }, { maxRetries: 5 });

      const worker = queue.createWorker({
        type: 'test:work',
        handler: async () => {
          throw new Error('bad input');
        },
        retryIf: () => false,
        pollIntervalMs: 10
      });

      worker.start();
      await sleep(100);
      await worker.stop();

      expect(queue.getJob(id)).toBeNull();
      const failed = queue.getFailedJobs();
      expect(failed.items).toHaveLength(1);
    });

    test('retryIf returning true → normal retry', async () => {
      let calls = 0;
      const id = queue.add('test:work', { value: 'retry' }, { maxRetries: 3 });

      const worker = queue.createWorker({
        type: 'test:work',
        handler: async () => {
          calls++;
          if (calls < 3) throw new Error('transient');
        },
        retryIf: () => true,
        pollIntervalMs: 10
      });

      worker.start();
      await sleep(200);
      await worker.stop();

      expect(calls).toBe(3);
      expect(queue.getJob(id)!.status).toBe('done');
    });
  });

  describe('pause/resume', () => {
    test('pause stops claiming new jobs', async () => {
      let processed = 0;
      for (let i = 0; i < 3; i++) {
        queue.add('test:work', { value: `j${i}` });
      }

      const worker = queue.createWorker({
        type: 'test:work',
        handler: async () => {
          processed++;
        },
        pollIntervalMs: 10
      });

      worker.start();
      worker.pause();
      await sleep(100);
      await worker.stop();

      expect(processed).toBe(0);
      expect(worker.isPaused).toBe(true);
    });

    test('resume restarts processing', async () => {
      let processed = 0;
      for (let i = 0; i < 3; i++) {
        queue.add('test:work', { value: `j${i}` });
      }

      const worker = queue.createWorker({
        type: 'test:work',
        handler: async () => {
          processed++;
        },
        pollIntervalMs: 50
      });

      worker.start();
      worker.pause();
      await sleep(80);
      expect(processed).toBe(0);

      worker.resume();
      await sleep(300);
      await worker.stop();

      expect(processed).toBe(3);
    });
  });

  describe('handler result captured', () => {
    test('handler return value stored in job result', async () => {
      const id = queue.add('test:work', { value: 'compute' });
      const worker = queue.createWorker({
        type: 'test:work',
        handler: async job => {
          return { processed: job.data.value, count: 42 };
        },
        pollIntervalMs: 10
      });

      worker.start();
      await sleep(100);
      await worker.stop();

      const result = queue.getJobResult<{ processed: string; count: number }>(
        id
      );
      expect(result).toEqual({ processed: 'compute', count: 42 });
    });
  });

  describe('middleware integration', () => {
    test('middleware wraps handler in order', async () => {
      const order: string[] = [];

      queue.use(async (_job, next) => {
        order.push('before');
        const r = await next();
        order.push('after');
        return r;
      });

      queue.add('test:work', { value: 'x' });
      const worker = queue.createWorker({
        type: 'test:work',
        handler: async () => {
          order.push('handler');
        },
        pollIntervalMs: 10
      });

      worker.start();
      await sleep(100);
      await worker.stop();

      expect(order).toEqual(['before', 'handler', 'after']);
    });

    test('middleware can short-circuit handler', async () => {
      let handlerCalled = false;

      queue.use(async () => 'skipped');

      queue.add('test:work', { value: 'x' });
      const worker = queue.createWorker({
        type: 'test:work',
        handler: async () => {
          handlerCalled = true;
        },
        pollIntervalMs: 10
      });

      worker.start();
      await sleep(100);
      await worker.stop();

      expect(handlerCalled).toBe(false);
    });

    test('middleware error propagates to onError', async () => {
      const onError = vi.fn();

      queue.use(async () => {
        throw new Error('middleware failure');
      });

      queue.add('test:work', { value: 'x' });
      const worker = queue.createWorker({
        type: 'test:work',
        handler: async () => {},
        onError,
        pollIntervalMs: 10
      });

      worker.start();
      await sleep(100);
      await worker.stop();

      expect(onError).toHaveBeenCalled();
    });

    test('multiple middlewares run in correct onion order', async () => {
      const order: string[] = [];

      queue.use(async (_job, next) => {
        order.push('mw1-in');
        await next();
        order.push('mw1-out');
      });
      queue.use(async (_job, next) => {
        order.push('mw2-in');
        await next();
        order.push('mw2-out');
      });

      queue.add('test:work', { value: 'x' });
      const worker = queue.createWorker({
        type: 'test:work',
        handler: async () => {
          order.push('handler');
        },
        pollIntervalMs: 10
      });

      worker.start();
      await sleep(100);
      await worker.stop();

      expect(order).toEqual([
        'mw1-in',
        'mw2-in',
        'handler',
        'mw2-out',
        'mw1-out'
      ]);
    });
  });

  describe('priority aging', () => {
    test('aging boosts priority of pending jobs', async () => {
      // Schedule far in future so job stays pending (can't be claimed)
      // but aging SQL still matches it (checks created_at, not run_at)
      queue.add(
        'test:work',
        { value: 'age-me' },
        {
          priority: 0,
          runAt: new Date(Date.now() + 60_000)
        }
      );

      const before = queue.listJobs({ status: 'pending', type: 'test:work' })
        .items[0]!.priority;

      const worker = queue.createWorker({
        type: 'test:work',
        handler: async () => {},
        pollIntervalMs: 10,
        aging: { boostPerMinute: 6000, maxBoost: 100 }
      });

      worker.start();
      await sleep(150);
      await worker.stop();

      const after = queue.listJobs({ status: 'pending', type: 'test:work' })
        .items[0]!.priority;

      expect(after).toBeGreaterThan(before);
    });
  });
});
