import type { JobQueue } from './queue';
import type { JobMap, WorkerOptions } from './types';
export declare class JobWorker<TMap extends JobMap = Record<string, unknown>, K extends string & keyof TMap = string & keyof TMap> {
    private queue;
    private options;
    private rateLimiter;
    private abortController;
    private timer;
    private running;
    private processing;
    private stopResolve;
    constructor(queue: JobQueue<TMap>, options: WorkerOptions<TMap[K]> & {
        type: K;
    });
    get isRunning(): boolean;
    start(): void;
    stop(): Promise<void>;
    private scheduleNext;
    private poll;
}
//# sourceMappingURL=worker.d.ts.map