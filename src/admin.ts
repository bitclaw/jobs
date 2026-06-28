import type { JobQueue } from './queue';
import type { JobStatus } from './types';

export function createAdminHandler<T extends Record<string, unknown>>(
  queue: JobQueue<T>,
  prefix = ''
): (req: Request) => Promise<Response> {
  const base = prefix.replace(/\/$/, '');
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const path = url.pathname.slice(base.length).replace(/^\//, '');
    const parts = path.split('/').filter(Boolean);
    const method = req.method.toUpperCase();

    try {
      if (method === 'GET' && parts[0] === 'stats' && parts.length === 1) {
        return Response.json(queue.getStats());
      }

      if (method === 'GET' && parts[0] === 'jobs' && parts[1] === 'types') {
        return Response.json(queue.getJobTypes());
      }

      if (method === 'GET' && parts[0] === 'jobs' && parts.length === 1) {
        const status = url.searchParams.get('status') as JobStatus | null;
        const type = url.searchParams.get('type') ?? undefined;
        const limit = Number(url.searchParams.get('limit') ?? 50);
        const offset = Number(url.searchParams.get('offset') ?? 0);
        return Response.json(
          queue.listJobs({ status: status ?? undefined, type, limit, offset })
        );
      }

      if (method === 'GET' && parts[0] === 'jobs' && parts.length === 2) {
        const id = Number(parts[1]);
        const job = queue.getJob(id);
        if (!job) return Response.json({ error: 'Not found' }, { status: 404 });
        return Response.json(job);
      }

      if (method === 'GET' && parts[0] === 'jobs' && parts[2] === 'graph') {
        return Response.json(queue.getJobGraph(Number(parts[1])));
      }

      if (method === 'POST' && parts[0] === 'jobs' && parts[2] === 'cancel') {
        return Response.json({ ok: queue.cancelJob(Number(parts[1])) });
      }

      if (
        method === 'POST' &&
        parts[0] === 'jobs' &&
        parts[2] === 'force-retry'
      ) {
        return Response.json({ ok: queue.forceRetryJob(Number(parts[1])) });
      }

      if (method === 'GET' && parts[0] === 'failed' && parts.length === 1) {
        const type = url.searchParams.get('type') ?? undefined;
        const limit = Number(url.searchParams.get('limit') ?? 50);
        const offset = Number(url.searchParams.get('offset') ?? 0);
        return Response.json(queue.getFailedJobs({ type, limit, offset }));
      }

      if (
        method === 'POST' &&
        parts[0] === 'failed' &&
        parts[1] === 'retry-by-type'
      ) {
        const body = (await req.json()) as { type?: string };
        if (!body.type)
          return Response.json({ error: 'type required' }, { status: 400 });
        return Response.json({
          count: queue.retryFailedJobsByType(body.type)
        });
      }

      if (method === 'POST' && parts[0] === 'failed' && parts[2] === 'retry') {
        return Response.json({ id: queue.retryFailedJob(Number(parts[1])) });
      }

      return Response.json({ error: 'Not found' }, { status: 404 });
    } catch (err) {
      return Response.json(
        { error: err instanceof Error ? err.message : 'Internal error' },
        { status: 500 }
      );
    }
  };
}
