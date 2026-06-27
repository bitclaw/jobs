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

export class JobQueueEmitter {
  private readonly listeners = new Map<
    keyof JobQueueEventMap,
    Set<(...args: unknown[]) => void>
  >();

  on<K extends keyof JobQueueEventMap>(
    event: K,
    handler: Handler<K>
  ): () => void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(handler as (...args: unknown[]) => void);
    return () => this.off(event, handler);
  }

  off<K extends keyof JobQueueEventMap>(event: K, handler: Handler<K>): void {
    this.listeners.get(event)?.delete(handler as (...args: unknown[]) => void);
  }

  once<K extends keyof JobQueueEventMap>(event: K, handler: Handler<K>): void {
    const wrapper = ((...args: unknown[]) => {
      this.off(event, wrapper as unknown as Handler<K>);
      (handler as (...args: unknown[]) => void)(...args);
    }) as Handler<K>;
    this.on(event, wrapper);
  }

  emit<K extends keyof JobQueueEventMap>(
    event: K,
    ...args: Parameters<JobQueueEventMap[K]>
  ): void {
    const handlers = this.listeners.get(event);
    if (!handlers) return;
    for (const handler of handlers) {
      try {
        handler(...(args as unknown[]));
      } catch {
        // silently swallow listener errors
      }
    }
  }
}
