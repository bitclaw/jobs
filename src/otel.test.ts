import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import type { OtelSpan, OtelTracer } from './otel';
import { createOtelMiddleware } from './otel';
import { JobQueue } from './queue';

type TestJobs = { 'test:work': { value: string } };

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function makeTracer(): {
  tracer: OtelTracer;
  spans: Array<{
    name: string;
    attrs: Record<string, string | number | boolean>;
    status: { code: number; message?: string } | null;
    error: unknown;
    ended: boolean;
  }>;
} {
  const spans: ReturnType<typeof makeTracer>['spans'] = [];

  const tracer: OtelTracer = {
    startActiveSpan(name, fn) {
      const entry: (typeof spans)[number] = {
        name,
        attrs: {},
        status: null,
        error: null,
        ended: false
      };
      spans.push(entry);

      const span: OtelSpan = {
        setAttribute(k, v) {
          entry.attrs[k] = v;
        },
        setStatus(s) {
          entry.status = s;
        },
        recordException(e) {
          entry.error = e;
        },
        end() {
          entry.ended = true;
        }
      };

      return fn(span);
    }
  };

  return { tracer, spans };
}

describe('createOtelMiddleware', () => {
  let queue: JobQueue<TestJobs>;

  beforeEach(() => {
    queue = new JobQueue<TestJobs>(':memory:');
  });

  afterEach(() => {
    queue.close();
  });

  test('creates span named job.<type> on success', async () => {
    const { tracer, spans } = makeTracer();
    queue.use(createOtelMiddleware(tracer));

    queue.add('test:work', { value: 'hello' });
    const worker = queue.createWorker({
      type: 'test:work',
      handler: async () => {},
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(100);
    await worker.stop();

    expect(spans).toHaveLength(1);
    expect(spans[0]!.name).toBe('job.test:work');
    expect(spans[0]!.ended).toBe(true);
    expect(spans[0]!.status?.code).toBe(1); // STATUS_OK
  });

  test('sets job.id, job.type, job.priority, job.retry attributes', async () => {
    const { tracer, spans } = makeTracer();
    queue.use(createOtelMiddleware(tracer));

    const id = queue.add('test:work', { value: 'attrs' }, { priority: 5 });
    const worker = queue.createWorker({
      type: 'test:work',
      handler: async () => {},
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(100);
    await worker.stop();

    const attrs = spans[0]!.attrs;
    expect(attrs['job.id']).toBe(id);
    expect(attrs['job.type']).toBe('test:work');
    expect(attrs['job.priority']).toBe(5);
    expect(attrs['job.retry']).toBe(0);
  });

  test('records error and sets ERROR status on failure', async () => {
    const { tracer, spans } = makeTracer();
    queue.use(createOtelMiddleware(tracer));

    queue.add('test:work', { value: 'fail' });
    const worker = queue.createWorker({
      type: 'test:work',
      handler: async () => {
        throw new Error('bad thing');
      },
      retryIf: () => false,
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(100);
    await worker.stop();

    expect(spans[0]!.status?.code).toBe(2); // STATUS_ERROR
    expect(spans[0]!.attrs['job.error']).toBe('bad thing');
    expect(spans[0]!.error).toBeInstanceOf(Error);
    expect(spans[0]!.ended).toBe(true);
  });

  test('span ends even when middleware re-throws', async () => {
    const { tracer, spans } = makeTracer();
    queue.use(createOtelMiddleware(tracer));

    queue.add('test:work', { value: 'throw' });
    const worker = queue.createWorker({
      type: 'test:work',
      handler: async () => {
        throw new Error('boom');
      },
      retryIf: () => false,
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(100);
    await worker.stop();

    expect(spans[0]!.ended).toBe(true);
  });

  test('one span per job execution', async () => {
    const { tracer, spans } = makeTracer();
    queue.use(createOtelMiddleware(tracer));

    queue.add('test:work', { value: 'a' });
    queue.add('test:work', { value: 'b' });
    queue.add('test:work', { value: 'c' });

    const worker = queue.createWorker({
      type: 'test:work',
      handler: async () => {},
      pollIntervalMs: 10
    });

    worker.start();
    await sleep(200);
    await worker.stop();

    expect(spans).toHaveLength(3);
    expect(spans.every(s => s.ended)).toBe(true);
  });
});
