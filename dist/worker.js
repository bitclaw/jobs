import { SlidingWindowRateLimiter } from './rate-limiter';
import { NonRetryableError } from './types';
export class JobWorker {
    queue;
    options;
    rateLimiter;
    abortController = null;
    timer = null;
    running = false;
    processing = false;
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
        if (this.processing) {
            return new Promise(resolve => {
                this.stopResolve = resolve;
            });
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
        if (this.rateLimiter && !this.rateLimiter.canProceed()) {
            this.scheduleNext();
            return;
        }
        const job = this.queue.pollAndClaim(this.options.type);
        if (!job) {
            this.scheduleNext();
            return;
        }
        this.processing = true;
        try {
            const ctx = {
                reportProgress: (percent) => {
                    this.queue.updateProgress(job.id, percent);
                },
                signal: this.abortController.signal
            };
            this.rateLimiter?.record();
            const handlerPromise = this.options.handler(job, ctx);
            if (this.options.timeoutMs) {
                const timeoutMs = this.options.timeoutMs;
                await Promise.race([
                    handlerPromise,
                    new Promise((_, reject) => setTimeout(() => reject(new Error(`Job timed out after ${timeoutMs}ms`)), timeoutMs))
                ]);
            }
            else {
                await handlerPromise;
            }
            this.queue.markJobDone(job.id);
        }
        catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            // Check both instanceof (same bundle) and the isNonRetryable property
            // (duck-type fallback in case module deduplication fails across Vite chunks)
            const isNonRetryable = error instanceof NonRetryableError ||
                (typeof error === 'object' &&
                    error !== null &&
                    error.isNonRetryable === true);
            if (isNonRetryable) {
                this.queue.markJobDead(job.id, message);
            }
            else {
                this.queue.markJobFailed(job.id, message);
            }
            this.options.onError?.(job, error);
        }
        finally {
            this.processing = false;
            if (this.stopResolve) {
                this.stopResolve();
                this.stopResolve = null;
            }
            else {
                this.scheduleNext();
            }
        }
    }
}
