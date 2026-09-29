// packages/jobs/src/worker.ts
// JobWorker , setTimeout-based poll loop with graceful shutdown
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
    // Jitter desynchronizes concurrent worker processes so their poll ticks
    // don't bunch up and contend for the write lock in lockstep.
    const jitter = interval * 0.1 * Math.random();
    this.timer = setTimeout(() => this.poll(), interval + jitter);
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
    const leaseMs = this.options.leaseMs ?? 300_000;
    const heartbeatMs = this.options.heartbeatMs ?? Math.floor(leaseMs / 4);
    const claimId = job.claimId ?? undefined;

    // Per-job signal: aborted on worker stop, on timeout, and when the lease
    // is lost to another worker - so a handler that honors ctx.signal stops
    // instead of running on alongside the job's retry.
    const jobAbort = new AbortController();
    const workerSignal = this.abortController?.signal;
    const onWorkerAbort = () => jobAbort.abort(workerSignal?.reason);
    if (workerSignal?.aborted) jobAbort.abort(workerSignal.reason);
    else workerSignal?.addEventListener('abort', onWorkerAbort, { once: true });

    let leaseLost = false;
    const renewLease = (): boolean => {
      const held = this.queue.renewLease(job.id, leaseMs, claimId);
      if (!held && !leaseLost) {
        leaseLost = true;
        jobAbort.abort(new Error('Job lease lost'));
      }
      return held;
    };

    // Built-in heartbeat: without it, any handler running past leaseMs was
    // reclaimed by pollAndClaim while still alive and executed twice.
    const heartbeat =
      heartbeatMs > 0
        ? setInterval(() => {
            try {
              renewLease();
            } catch {
              // transient write failure (e.g. SQLITE_BUSY) - next tick retries
            }
          }, heartbeatMs)
        : null;
    let timeoutTimer: ReturnType<typeof setTimeout> | null = null;

    const reportLeaseLost = () => {
      this.queue.emit('job:leaseLost', job as Job);
    };

    try {
      const ctx: JobContext = {
        reportProgress: (percent: number) => {
          this.queue.updateProgress(job.id, percent);
        },
        renewLease,
        signal: jobAbort.signal
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
          new Promise<never>((_, reject) => {
            timeoutTimer = setTimeout(() => {
              const error = new Error(`Job timed out after ${timeoutMs}ms`);
              jobAbort.abort(error);
              reject(error);
            }, timeoutMs);
          })
        ]);
      } else {
        handlerResult = await execute();
      }

      try {
        if (!this.queue.markJobDone(job.id, handlerResult, claimId)) {
          reportLeaseLost();
        }
      } catch (markError: unknown) {
        // Handler already succeeded - don't let a write failure here (e.g.
        // SQLITE_BUSY under contention) fall through to the catch block below
        // and get this job wrongly reclassified as failed/dead.
        this.queue.emit('job:markDoneFailed', job, markError);
      }
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

      try {
        const held =
          isNonRetryable || !shouldRetry
            ? this.queue.markJobDead(job.id, message, claimId)
            : this.queue.markJobFailed(job.id, message, claimId);
        if (!held) reportLeaseLost();
      } catch (markError: unknown) {
        this.queue.emit('job:markFailedError', job, markError);
      }
      this.options.onError?.(job as Job<TMap[K]>, error);
    } finally {
      if (heartbeat) clearInterval(heartbeat);
      if (timeoutTimer) clearTimeout(timeoutTimer);
      workerSignal?.removeEventListener('abort', onWorkerAbort);
      this.activeCount--;
      if (!this.running && this.activeCount === 0 && this.stopResolve) {
        this.stopResolve();
        this.stopResolve = null;
      }
    }
  }
}
