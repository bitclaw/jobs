import type { JobQueue } from './queue';
export declare function createAdminHandler<T extends Record<string, unknown>>(queue: JobQueue<T>, prefix?: string): (req: Request) => Promise<Response>;
//# sourceMappingURL=admin.d.ts.map