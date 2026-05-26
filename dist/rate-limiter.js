// packages/jobs/src/rate-limiter.ts
// In-memory sliding window rate limiter for per-type job throttling
export class SlidingWindowRateLimiter {
    timestamps = [];
    maxCount;
    windowMs;
    constructor(maxCount, windowMs) {
        this.maxCount = maxCount;
        this.windowMs = windowMs;
    }
    canProceed() {
        this.prune();
        return this.timestamps.length < this.maxCount;
    }
    record() {
        this.timestamps.push(Date.now());
    }
    reset() {
        this.timestamps = [];
    }
    prune() {
        const cutoff = Date.now() - this.windowMs;
        while (this.timestamps.length > 0 && this.timestamps[0] < cutoff) {
            this.timestamps.shift();
        }
    }
}
