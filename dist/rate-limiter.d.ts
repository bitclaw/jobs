export declare class SlidingWindowRateLimiter {
    private timestamps;
    private maxCount;
    private windowMs;
    constructor(maxCount: number, windowMs: number);
    canProceed(): boolean;
    record(): void;
    reset(): void;
    private prune;
}
//# sourceMappingURL=rate-limiter.d.ts.map