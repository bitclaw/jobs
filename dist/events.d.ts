import type { Job, JobBatch } from './types';
export type JobQueueEventMap = {
    'job:done': (job: Job) => void;
    'job:failed': (job: Job, error: string) => void;
    'job:dead': (job: Job, error: string) => void;
    'job:progress': (job: Job, progress: number) => void;
    'job:stale': (count: number) => void;
    'batch:complete': (batch: JobBatch) => void;
    'batch:failed': (batch: JobBatch) => void;
};
type Handler<K extends keyof JobQueueEventMap> = JobQueueEventMap[K];
export declare class JobQueueEmitter {
    private readonly listeners;
    on<K extends keyof JobQueueEventMap>(event: K, handler: Handler<K>): () => void;
    off<K extends keyof JobQueueEventMap>(event: K, handler: Handler<K>): void;
    once<K extends keyof JobQueueEventMap>(event: K, handler: Handler<K>): void;
    emit<K extends keyof JobQueueEventMap>(event: K, ...args: Parameters<JobQueueEventMap[K]>): void;
}
export {};
//# sourceMappingURL=events.d.ts.map