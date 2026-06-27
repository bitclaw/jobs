/**
 * Throw this from a job handler to skip all retries and move immediately to
 * the dead-letter (failed_jobs) table. Use for permanent configuration errors
 * like missing SSH keys, bad tokens, or invalid input that will never succeed
 * no matter how many times the job runs.
 */
export declare class NonRetryableError extends Error {
    readonly isNonRetryable = true;
    constructor(message: string);
}
export type JobStatus = 'pending' | 'processing' | 'done' | 'failed' | 'blocked' | 'cancelled';
export type Job<T = unknown> = {
    readonly id: number;
    readonly type: string;
    readonly data: T;
    readonly status: JobStatus;
    readonly priority: number;
    readonly progress: number;
    readonly maxRetries: number;
    readonly retryCount: number;
    readonly runAt: string;
    readonly createdAt: string;
    readonly updatedAt: string;
    readonly startedAt: string | null;
    readonly completedAt: string | null;
    readonly error: string | null;
    readonly batchId: string | null;
    readonly requestLog: string | null;
    readonly responseLog: string | null;
    readonly uniqueKey: string | null;
    readonly claimedUntil: string | null;
    readonly result: unknown | null;
    readonly expireAt: string | null;
};
export type FailedJob = {
    readonly id: number;
    readonly originalJobId: number;
    readonly type: string;
    readonly data: unknown;
    readonly error: string | null;
    readonly retryCount: number;
    readonly maxRetries: number;
    readonly createdAt: string;
    readonly failedAt: string;
    readonly requestLog: string | null;
    readonly responseLog: string | null;
};
export type BackoffConfig = {
    type: 'exponential' | 'fixed' | 'jitter' | 'fibonacci';
    /** Base delay in ms. Exponential: delayMs * 2^retryCount. Fixed: always delayMs. Max 1h. */
    delayMs: number;
};
export type AddJobOptions = {
    priority?: number;
    runAt?: Date;
    maxRetries?: number;
    dependsOn?: number[];
    /**
     * Deduplication key scoped to job type. If a job with the same (type, uniqueKey)
     * is already pending or processing, the new enqueue is silently ignored and the
     * existing job id is returned. Once the job completes, the same key can be re-used.
     */
    uniqueKey?: string;
    /**
     * Backoff strategy for retries. Exponential: delayMs * 2^retryCount, capped at 1h.
     * Fixed: always delayMs between retries. Default: retry immediately.
     */
    backoff?: BackoffConfig;
    dedup?: 'ignore' | 'replace';
    expireAt?: Date;
    onComplete?: {
        url: string;
        method?: string;
        headers?: Record<string, string>;
    };
};
export type JobContext = {
    reportProgress: (percent: number) => void;
    signal: AbortSignal;
    renewLease(): void;
};
export type RateLimit = {
    count: number;
    windowMs: number;
};
export type MiddlewareFn<T = unknown> = (job: Job<T>, next: () => Promise<unknown>) => Promise<unknown>;
export type JobGraphNode = {
    readonly id: number;
    readonly type: string;
    readonly status: JobStatus;
    readonly result: unknown | null;
    readonly dependsOn: number[];
    readonly dependents: number[];
};
export type WorkerOptions<T = unknown> = {
    type: string;
    handler: (job: Job<T>, ctx: JobContext) => Promise<unknown>;
    pollIntervalMs?: number;
    maxRate?: RateLimit;
    onError?: (job: Job<T>, error: unknown) => void;
    /** Hard wall-clock limit per job execution in ms. Job is marked failed on timeout. */
    timeoutMs?: number;
    /** Max concurrent jobs this worker runs simultaneously. Default: 1. */
    concurrency?: number;
    leaseMs?: number;
    retryIf?: (error: unknown, job: Job<T>) => boolean;
    aging?: {
        boostPerMinute: number;
        maxBoost: number;
    };
};
export type JobStats = {
    pending: number;
    blocked: number;
    processing: number;
    done: number;
    failed: number;
    cancelled: number;
    dead: number;
};
export type PurgeOptions = {
    status: 'done' | 'failed';
    olderThanMs: number;
};
export type ListJobsOptions = {
    status?: JobStatus;
    type?: string;
    limit?: number;
    offset?: number;
};
export type PaginatedResult<T> = {
    items: T[];
    total: number;
};
export type JobMap = Record<string, unknown>;
export type JobRow = {
    id: number;
    type: string;
    data: string;
    status: string;
    priority: number;
    progress: number;
    max_retries: number;
    retry_count: number;
    run_at: string;
    created_at: string;
    updated_at: string;
    started_at: string | null;
    completed_at: string | null;
    error: string | null;
    batch_id: string | null;
    request_log: string | null;
    response_log: string | null;
    unique_key: string | null;
    backoff_config: string | null;
    claimed_until: string | null;
    result: string | null;
    expire_at: string | null;
    webhook_config: string | null;
};
export type FailedJobRow = {
    id: number;
    original_job_id: number;
    type: string;
    data: string;
    error: string | null;
    retry_count: number;
    max_retries: number;
    created_at: string;
    failed_at: string;
    request_log: string | null;
    response_log: string | null;
};
export type StatsRow = {
    status: string;
    count: number;
};
export type BatchOptions = {
    thenType?: string;
    thenData?: unknown;
    finallyType?: string;
    finallyData?: unknown;
};
export type JobBatch = {
    readonly id: string;
    readonly name: string;
    readonly totalJobs: number;
    readonly pendingJobs: number;
    readonly failedJobs: number;
    readonly failedJobIds: number[];
    readonly options: BatchOptions | null;
    readonly cancelledAt: string | null;
    readonly createdAt: string;
    readonly finishedAt: string | null;
};
export type JobBatchRow = {
    id: string;
    name: string;
    total_jobs: number;
    pending_jobs: number;
    failed_jobs: number;
    failed_job_ids: string;
    options: string | null;
    cancelled_at: string | null;
    created_at: string;
    finished_at: string | null;
};
export type Schedule = {
    readonly id: number;
    readonly name: string;
    readonly type: string;
    readonly data: unknown;
    readonly cron: string;
    readonly timezone: string;
    readonly enabled: boolean;
    readonly overlap: boolean;
    readonly maxRetries: number;
    readonly lastRunAt: string | null;
    readonly nextRunAt: string | null;
    readonly createdAt: string;
    readonly updatedAt: string;
};
export type ScheduleRow = {
    id: number;
    name: string;
    type: string;
    data: string;
    cron: string;
    timezone: string;
    enabled: number;
    overlap: number;
    max_retries: number;
    last_run_at: string | null;
    next_run_at: string | null;
    created_at: string;
    updated_at: string;
};
export type AddScheduleOptions = {
    data?: unknown;
    timezone?: string;
    overlap?: boolean;
    maxRetries?: number;
};
export type WorkflowExecutionStatus = 'running' | 'completed' | 'compensating' | 'failed';
export type WorkflowExecution = {
    readonly id: string;
    readonly name: string;
    readonly status: WorkflowExecutionStatus;
    readonly createdAt: string;
    readonly completedAt: string | null;
};
export type WorkflowRunResult<TStepNames extends string> = {
    instanceId: string;
    jobIds: Record<TStepNames, number>;
};
export type WorkflowReconcileResult = {
    completed: number;
    compensated: number;
    failed: number;
};
//# sourceMappingURL=types.d.ts.map