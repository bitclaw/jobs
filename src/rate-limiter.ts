// packages/jobs/src/rate-limiter.ts
// In-memory sliding window rate limiter for per-type job throttling

export class SlidingWindowRateLimiter {
  private timestamps: number[] = [];
  private maxCount: number;
  private windowMs: number;

  constructor(maxCount: number, windowMs: number) {
    this.maxCount = maxCount;
    this.windowMs = windowMs;
  }

  canProceed(): boolean {
    this.prune();
    return this.timestamps.length < this.maxCount;
  }

  record(): void {
    this.timestamps.push(Date.now());
  }

  reset(): void {
    this.timestamps = [];
  }

  private prune(): void {
    const cutoff = Date.now() - this.windowMs;
    while (this.timestamps.length > 0 && this.timestamps[0]! < cutoff) {
      this.timestamps.shift();
    }
  }
}
