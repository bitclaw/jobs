import { Database } from 'bun:sqlite';
import { JobQueueEmitter } from './events';
import type { AddJobOptions, BatchOptions, FailedJob, Job, JobBatch, JobGraphNode, JobMap, JobStats, ListJobsOptions, MiddlewareFn, PaginatedResult, PurgeOptions, WorkerOptions } from './types';
import { JobWorker } from './worker';
export declare class JobQueue<TMap extends JobMap = Record<string, unknown>> extends JobQueueEmitter {
    readonly db: Database;
    readonly middlewares: MiddlewareFn[];
    private readonly stmts;
    constructor(dbPath: string);
    add<K extends string & keyof TMap>(type: K, data: TMap[K], options?: AddJobOptions): number;
    getJob(id: number): Job | null;
    getStats(): JobStats;
    getFailedJobs(options?: {
        type?: string;
        limit?: number;
        offset?: number;
    }): PaginatedResult<FailedJob>;
    listJobs(options?: ListJobsOptions): PaginatedResult<Job>;
    cancelJob(id: number): boolean;
    forceRetryJob(id: number): boolean;
    setJobHttpLog(id: number, requestLog: string, responseLog: string): void;
    getJobTypes(): string[];
    retryFailedJob(failedJobId: number): number;
    purgeFailedJobs(olderThanMs: number): number;
    purge(options: PurgeOptions): number;
    pollAndClaim(type: string, leaseMs?: number): Job | null;
    renewLease(id: number, leaseMs: number): void;
    markJobDone(id: number, result?: unknown): void;
    markJobDead(id: number, error: string): void;
    markJobFailed(id: number, error: string): void;
    private fib;
    updateProgress(id: number, progress: number): void;
    createBatch(name: string, options?: BatchOptions): string;
    addToBatch<K extends string & keyof TMap>(batchId: string, type: K, data: TMap[K], options?: AddJobOptions): number;
    getBatch(batchId: string): JobBatch | null;
    cancelBatch(batchId: string): void;
    createWorker<K extends string & keyof TMap>(options: WorkerOptions<TMap[K]> & {
        type: K;
    }): JobWorker<TMap, K>;
    private insertJob;
    /**
     * Reset stuck `processing` jobs back to `pending`. Call at startup to recover
     * from server crashes that left jobs claimed but never completed.
     * @param thresholdMs Jobs processing longer than this (ms) are reset. Default 5min.
     * @returns Number of jobs reset.
     */
    reconcileStaleJobs(thresholdMs?: number): number;
    /**
     * Look up a pending or processing job by its uniqueKey.
     * Returns null if no such job exists (completed, dead-lettered, or never queued).
     */
    getJobByUniqueKey(type: string, uniqueKey: string): Job | null;
    getJobResult<T>(id: number): T | null;
    cancelByUniqueKey(type: string, uniqueKey: string): boolean;
    retryFailedJobsByType(type: string): number;
    purgeExpiredJobs(): number;
    use(fn: MiddlewareFn): void;
    getJobGraph(rootId: number): JobGraphNode[];
    mountAdminHandler(prefix?: string): (req: Request) => Promise<Response>;
    close(): void;
    private unblockDependents;
    private handleBatchJobComplete;
}
//# sourceMappingURL=queue.d.ts.map