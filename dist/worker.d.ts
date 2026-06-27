import type { JobQueue } from './queue';
import type { JobMap, WorkerOptions } from './types';
export declare class JobWorker<TMap extends JobMap = Record<string, unknown>, K extends string & keyof TMap = string & keyof TMap> {
    private queue;
    private options;
    private rateLimiter;
    private abortController;
    private timer;
    private running;
    private paused;
    private activeCount;
    private stopResolve;
    constructor(queue: JobQueue<TMap>, options: WorkerOptions<TMap[K]> & {
        type: K;
    });
    get isRunning(): boolean;
    get isPaused(): boolean;
    start(): void;
    stop(): Promise<void>;
    pause(): void;
    resume(): void;
    private scheduleNext;
    private poll;
    private runJob;
}
//# sourceMappingURL=worker.d.ts.map