import { SlidingWindowRateLimiter } from './rate-limiter';
import { NonRetryableError } from './types';
export class JobWorker {
    queue;
    options;
    rateLimiter;
    abortController = null;
    timer = null;
    running = false;
    paused = false;
    activeCount = 0;
    stopResolve = null;
    constructor(queue, options) {
        this.queue = queue;
        this.options = options;
        this.rateLimiter = options.maxRate
            ? new SlidingWindowRateLimiter(options.maxRate.count, options.maxRate.windowMs)
            : null;
    }
    get isRunning() {
        return this.running;
    }
    get isPaused() {
        return this.paused;
    }
    start() {
        if (this.running)
            return;
        this.running = true;
        this.abortController = new AbortController();
        this.scheduleNext();
    }
    async stop() {
        if (!this.running)
            return;
        this.running = false;
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        this.abortController?.abort();
        if (this.activeCount > 0) {
            return new Promise(resolve => {
                this.stopResolve = resolve;
            });
        }
    }
    pause() {
        this.paused = true;
    }
    resume() {
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
    scheduleNext() {
        if (!this.running)
            return;
        const interval = this.options.pollIntervalMs ?? 1000;
        this.timer = setTimeout(() => this.poll(), interval);
    }
    async poll() {
        if (!this.running)
            return;
        if (this.paused) {
            this.scheduleNext();
            return;
        }
        const concurrency = this.options.concurrency ?? 1;
        const leaseMs = this.options.leaseMs ?? 300_000;
        // Drain available jobs up to available capacity in one poll tick
        while (this.activeCount < concurrency) {
            if (this.rateLimiter && !this.rateLimiter.canProceed())
                break;
            const job = this.queue.pollAndClaim(this.options.type, leaseMs);
            if (!job)
                break;
            this.rateLimiter?.record();
            this.activeCount++;
            void this.runJob(job);
        }
        this.scheduleNext();
    }
    async runJob(job) {
        try {
            const ctx = {
                reportProgress: (percent) => {
                    this.queue.updateProgress(job.id, percent);
                },
                renewLease: () => {
                    this.queue.renewLease(job.id, this.options.leaseMs ?? 300_000);
                },
                signal: this.abortController.signal
            };
            const handlerPromise = this.options.handler(job, ctx);
            let handlerResult;
            if (this.options.timeoutMs) {
                const timeoutMs = this.options.timeoutMs;
                handlerResult = await Promise.race([
                    handlerPromise,
                    new Promise((_, reject) => setTimeout(() => reject(new Error(`Job timed out after ${timeoutMs}ms`)), timeoutMs))
                ]);
            }
            else {
                handlerResult = await handlerPromise;
            }
            this.queue.markJobDone(job.id, handlerResult);
        }
        catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            // Check both instanceof (same bundle) and the isNonRetryable property
            // (duck-type fallback in case module deduplication fails across Vite chunks)
            const isNonRetryable = error instanceof NonRetryableError ||
                (typeof error === 'object' &&
                    error !== null &&
                    error.isNonRetryable === true);
            const shouldRetry = this.options.retryIf
                ? this.options.retryIf(error, job)
                : true;
            if (isNonRetryable || !shouldRetry) {
                this.queue.markJobDead(job.id, message);
            }
            else {
                this.queue.markJobFailed(job.id, message);
            }
            this.options.onError?.(job, error);
        }
        finally {
            this.activeCount--;
            if (!this.running && this.activeCount === 0 && this.stopResolve) {
                this.stopResolve();
                this.stopResolve = null;
            }
        }
    }
}
