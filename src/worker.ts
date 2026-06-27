// packages/jobs/src/worker.ts
// JobWorker — setTimeout-based poll loop with graceful shutdown
import type { JobQueue } from './queue';
import { SlidingWindowRateLimiter } from './rate-limiter';
import type {
  Job,
  JobContext,
  JobMap,
  MiddlewareFn,
  WorkerOptions
} from './types';
import { NonRetryableError } from './types';
import { nowISO } from './utils';

export class JobWorker<
  TMap extends JobMap = Record<string, unknown>,
  K extends string & keyof TMap = string & keyof TMap
> {
  private queue: JobQueue<TMap>;
  private options: WorkerOptions<TMap[K]> & { type: K };
  private rateLimiter: SlidingWindowRateLimiter | null;
  private abortController: AbortController | null = null;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private running = false;
  private paused = false;
  private activeCount = 0;
  private stopResolve: (() => void) | null = null;

  constructor(
    queue: JobQueue<TMap>,
    options: WorkerOptions<TMap[K]> & { type: K }
  ) {
    this.queue = queue;
    this.options = options;
    this.rateLimiter = options.maxRate
      ? new SlidingWindowRateLimiter(
          options.maxRate.count,
          options.maxRate.windowMs
        )
      : null;
  }

  get isRunning(): boolean {
    return this.running;
  }

  get isPaused(): boolean {
    return this.paused;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.abortController = new AbortController();
    this.scheduleNext();
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;

    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    this.abortController?.abort();

    if (this.activeCount > 0) {
      return new Promise<void>(resolve => {
        this.stopResolve = resolve;
      });
    }
  }

  pause(): void {
    this.paused = true;
  }

  resume(): void {
    this.paused = false;
    if (this.running) {
      // Cancel existing timer and poll immediately
      if (this.timer) {
        clearTimeout(this.timer);
        this.timer = null;
      }
      void this.poll();
    }
  }

  private scheduleNext(): void {
    if (!this.running) return;
    const interval = this.options.pollIntervalMs ?? 1000;
    this.timer = setTimeout(() => this.poll(), interval);
  }

  private async poll(): Promise<void> {
    if (!this.running) return;
    if (this.paused) {
      this.scheduleNext();
      return;
    }

    const concurrency = this.options.concurrency ?? 1;
    const leaseMs = this.options.leaseMs ?? 300_000;

    // Drain available jobs up to available capacity in one poll tick
    while (this.activeCount < concurrency) {
      if (this.rateLimiter && !this.rateLimiter.canProceed()) break;

      const job = this.queue.pollAndClaim(this.options.type, leaseMs);
      if (!job) break;

      this.rateLimiter?.record();
      this.activeCount++;
      void this.runJob(job as Job<TMap[K]>);
    }

    // Apply priority aging if configured
    if (this.options.aging) {
      const { boostPerMinute, maxBoost } = this.options.aging;
      const pollIntervalMs = this.options.pollIntervalMs ?? 1000;
      const boostPerTick = (boostPerMinute * pollIntervalMs) / 60_000;
      if (boostPerTick > 0) {
        const cutoff = new Date(Date.now() - pollIntervalMs).toISOString();
        this.queue.db.run(
          `UPDATE jobs
           SET priority = MIN(priority + ?, ?),
               updated_at = ?
           WHERE status = 'pending'
             AND type = ?
             AND created_at < ?
             AND priority < ?`,
          [
            boostPerTick,
            maxBoost,
            nowISO(),
            this.options.type,
            cutoff,
            maxBoost
          ]
        );
      }
    }

    this.scheduleNext();
  }

  private async runJob(job: Job<TMap[K]>): Promise<void> {
    try {
      const ctx: JobContext = {
        reportProgress: (percent: number) => {
          this.queue.updateProgress(job.id, percent);
        },
        renewLease: () => {
          this.queue.renewLease(job.id, this.options.leaseMs ?? 300_000);
        },
        signal: this.abortController!.signal
      };

      const handler = this.options.handler;
      const middlewares = this.queue.middlewares as MiddlewareFn<TMap[K]>[];
      const chain: Array<
        (j: Job<TMap[K]>, next: () => Promise<unknown>) => Promise<unknown>
      > = [...middlewares, (j: Job<TMap[K]>) => handler(j, ctx)];
      const execute = (): Promise<unknown> => {
        let i = 0;
        const run = (): Promise<unknown> => {
          const mw = chain[i++];
          if (!mw) return Promise.resolve(undefined);
          return mw(job as Job<TMap[K]>, run);
        };
        return run();
      };

      let handlerResult: unknown;
      if (this.options.timeoutMs) {
        const timeoutMs = this.options.timeoutMs;
        handlerResult = await Promise.race([
          execute(),
          new Promise<never>((_, reject) =>
            setTimeout(
              () => reject(new Error(`Job timed out after ${timeoutMs}ms`)),
              timeoutMs
            )
          )
        ]);
      } else {
        handlerResult = await execute();
      }

      this.queue.markJobDone(job.id, handlerResult);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      // Check both instanceof (same bundle) and the isNonRetryable property
      // (duck-type fallback in case module deduplication fails across Vite chunks)
      const isNonRetryable =
        error instanceof NonRetryableError ||
        (typeof error === 'object' &&
          error !== null &&
          (error as { isNonRetryable?: unknown }).isNonRetryable === true);

      const shouldRetry = this.options.retryIf
        ? this.options.retryIf(error, job as Job<TMap[K]>)
        : true;

      if (isNonRetryable || !shouldRetry) {
        this.queue.markJobDead(job.id, message);
      } else {
        this.queue.markJobFailed(job.id, message);
      }
      this.options.onError?.(job as Job<TMap[K]>, error);
    } finally {
      this.activeCount--;
      if (!this.running && this.activeCount === 0 && this.stopResolve) {
        this.stopResolve();
        this.stopResolve = null;
      }
    }
  }
}
