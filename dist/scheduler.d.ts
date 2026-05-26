import type { JobQueue } from './queue';
import type { AddScheduleOptions, JobMap, Schedule } from './types';
export declare class Scheduler<TMap extends JobMap = Record<string, unknown>> {
    private readonly queue;
    private timer;
    private readonly upsertStmt;
    private readonly selectByNameStmt;
    private readonly selectAllStmt;
    private readonly selectDueStmt;
    private readonly updateLastRunStmt;
    private readonly pauseStmt;
    private readonly resumeStmt;
    private readonly removeStmt;
    constructor(queue: JobQueue<TMap>);
    register(name: string, type: string, cron: string, options?: AddScheduleOptions): Schedule;
    cleanup(registeredNames: string[]): number;
    pauseSchedule(name: string): void;
    resumeSchedule(name: string): void;
    getSchedules(): Schedule[];
    getSchedule(name: string): Schedule | null;
    removeSchedule(name: string): boolean;
    tick(): number;
    start(intervalMs?: number): void;
    stop(): void;
}
//# sourceMappingURL=scheduler.d.ts.map