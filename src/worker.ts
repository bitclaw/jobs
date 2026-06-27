// packages/jobs/src/worker.ts
// JobWorker — setTimeout-based poll loop with graceful shutdown
import type { JobQueue } from './queue';
import { SlidingWindowRateLimiter } from './rate-limiter';
import type { Job, JobContext, JobMap, WorkerOptions } from './types';
import { NonRetryableError } from './types';

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

  private scheduleNext(): void {
    if (!this.running) return;
    const interval = this.options.pollIntervalMs ?? 1000;
    this.timer = setTimeout(() => this.poll(), interval);
  }

  private async poll(): Promise<void> {
    if (!this.running) return;

    const concurrency = this.options.concurrency ?? 1;

    // Drain available jobs up to available capacity in one poll tick
    while (this.activeCount < concurrency) {
      if (this.rateLimiter && !this.rateLimiter.canProceed()) break;

      const job = this.queue.pollAndClaim(this.options.type);
      if (!job) break;

      this.rateLimiter?.record();
      this.activeCount++;
      void this.runJob(job as Job<TMap[K]>);
    }

    this.scheduleNext();
  }

  private async runJob(job: Job<TMap[K]>): Promise<void> {
    try {
      const ctx: JobContext = {
        reportProgress: (percent: number) => {
          this.queue.updateProgress(job.id, percent);
        },
        signal: this.abortController!.signal
      };

      const handlerPromise = this.options.handler(job as Job<TMap[K]>, ctx);
      if (this.options.timeoutMs) {
        const timeoutMs = this.options.timeoutMs;
        await Promise.race([
          handlerPromise,
          new Promise<never>((_, reject) =>
            setTimeout(
              () => reject(new Error(`Job timed out after ${timeoutMs}ms`)),
              timeoutMs
            )
          )
        ]);
      } else {
        await handlerPromise;
      }

      this.queue.markJobDone(job.id);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      // Check both instanceof (same bundle) and the isNonRetryable property
      // (duck-type fallback in case module deduplication fails across Vite chunks)
      const isNonRetryable =
        error instanceof NonRetryableError ||
        (typeof error === 'object' &&
          error !== null &&
          (error as { isNonRetryable?: unknown }).isNonRetryable === true);
      if (isNonRetryable) {
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
