import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { JobQueue } from './queue';

type AppJobs = {
  'email:send': { to: string; body: string };
  'deploy:provision': { serverId: string };
};

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe('Integration', () => {
  let queue: JobQueue<AppJobs>;

  beforeEach(() => {
    queue = new JobQueue<AppJobs>(':memory:');
  });

  afterEach(() => {
    queue.close();
  });

  test('full enqueue-to-done workflow', async () => {
    const id = queue.add('email:send', {
      to: 'user@test.com',
      body: 'Welcome!'
    });

    const worker = queue.createWorker({
      type: 'email:send',
      handler: async (received, ctx) => {
        ctx.reportProgress(50);
        expect(received.data.to).toBe('user@test.com');
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(100);
    await worker.stop();

    const job = queue.getJob(id)!;
    expect(job.status).toBe('done');
    expect(job.completedAt).not.toBeNull();

    const stats = queue.getStats();
    expect(stats.done).toBe(1);
    expect(stats.pending).toBe(0);
  });

  test('multi-type workers process independently', async () => {
    const emailResults: string[] = [];
    const deployResults: string[] = [];

    queue.add('email:send', { to: 'a@b.com', body: 'email1' });
    queue.add('deploy:provision', { serverId: 'srv-1' });
    queue.add('email:send', { to: 'c@d.com', body: 'email2' });

    const emailWorker = queue.createWorker({
      type: 'email:send',
      handler: async job => {
        emailResults.push(job.data.to);
      },
      pollIntervalMs: 10
    });

    const deployWorker = queue.createWorker({
      type: 'deploy:provision',
      handler: async job => {
        deployResults.push(job.data.serverId);
      },
      pollIntervalMs: 10
    });

    emailWorker.start();
    deployWorker.start();
    await sleep(200);
    await emailWorker.stop();
    await deployWorker.stop();

    expect(emailResults).toEqual(['a@b.com', 'c@d.com']);
    expect(deployResults).toEqual(['srv-1']);
  });

  test('dependency chain A->B->C processes in order', async () => {
    const order: string[] = [];

    const a = queue.add('email:send', { to: 'A', body: 'first' });
    const b = queue.add(
      'email:send',
      { to: 'B', body: 'second' },
      { dependsOn: [a] }
    );
    queue.add('email:send', { to: 'C', body: 'third' }, { dependsOn: [b] });

    const worker = queue.createWorker({
      type: 'email:send',
      handler: async job => {
        order.push(job.data.to);
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(300);
    await worker.stop();

    expect(order).toEqual(['A', 'B', 'C']);
  });

  test('retry flow: fail twice, succeed third', async () => {
    let attempts = 0;
    const id = queue.add('email:send', { to: 'retry@test.com', body: 'x' });

    const worker = queue.createWorker({
      type: 'email:send',
      handler: async () => {
        attempts++;
        if (attempts < 3) {
          throw new Error(`attempt ${attempts}`);
        }
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(300);
    await worker.stop();

    expect(attempts).toBe(3);
    const job = queue.getJob(id)!;
    expect(job.status).toBe('done');
  });

  test('moves to failed_jobs on max retries exceeded', async () => {
    const id = queue.add(
      'email:send',
      { to: 'dead@test.com', body: 'gone' },
      { maxRetries: 2 }
    );

    let attempts = 0;
    const worker = queue.createWorker({
      type: 'email:send',
      handler: async () => {
        attempts++;
        throw new Error('always fails');
      },
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(200);
    await worker.stop();

    expect(attempts).toBe(2);
    expect(queue.getJob(id)).toBeNull();

    const failed = queue.getFailedJobs();
    expect(failed.items).toHaveLength(1);
    expect(failed.items[0]!.originalJobId).toBe(id);
    expect(failed.items[0]!.error).toBe('always fails');

    const stats = queue.getStats();
    expect(stats.dead).toBe(1);
  });

  test('scheduled job skipped until run_at arrives', async () => {
    let processed = false;
    // Schedule 200ms in the future
    const runAt = new Date(Date.now() + 200);
    queue.add(
      'email:send',
      { to: 'later@test.com', body: 'scheduled' },
      { runAt }
    );

    const worker = queue.createWorker({
      type: 'email:send',
      handler: async () => {
        processed = true;
      },
      pollIntervalMs: 10
    });

    worker.start();

    // Should not be processed yet
    await sleep(100);
    expect(processed).toBe(false);

    // Wait for run_at to pass
    await sleep(200);
    expect(processed).toBe(true);

    await worker.stop();
  });

  test('rate limiting restricts throughput', async () => {
    let processedCount = 0;

    for (let i = 0; i < 10; i++) {
      queue.add('email:send', { to: `user${i}@test.com`, body: 'bulk' });
    }

    const worker = queue.createWorker({
      type: 'email:send',
      handler: async () => {
        processedCount++;
      },
      pollIntervalMs: 10,
      maxRate: { count: 3, windowMs: 500 }
    });

    worker.start();
    await sleep(200);
    await worker.stop();

    // Should be throttled to ~3 in 200ms with 500ms window
    expect(processedCount).toBeLessThanOrEqual(3);
    expect(processedCount).toBeGreaterThanOrEqual(1);
  });
});
