import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { JobQueue } from './queue';

type TestJobs = {
  'email:send': { to: string; subject: string };
  'deploy:provision': { serverId: string };
  'test:simple': { value: string };
};

describe('JobQueue', () => {
  let queue: JobQueue<TestJobs>;

  beforeEach(() => {
    queue = new JobQueue<TestJobs>(':memory:');
  });

  afterEach(() => {
    queue.close();
  });

  describe('getJobGraph', () => {
    test('single node with no deps', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'root' });
      const graph = queue.getJobGraph(id);
      expect(graph).toHaveLength(1);
      expect(graph[0]!.id).toBe(id);
      expect(graph[0]!.dependsOn).toEqual([]);
      expect(graph[0]!.dependents).toEqual([]);
    });

    test('empty for non-existent job', () => {
      expect(queue.getJobGraph(9999)).toHaveLength(0);
    });

    test('A→B→C chain returns all 3 nodes with correct edges', () => {
      const a = queue.add('email:send', { to: 'a@b.com', subject: 'A' });
      const b = queue.add(
        'email:send',
        { to: 'b@c.com', subject: 'B' },
        { dependsOn: [a] }
      );
      const c = queue.add(
        'email:send',
        { to: 'c@d.com', subject: 'C' },
        { dependsOn: [b] }
      );

      const graph = queue.getJobGraph(b);
      expect(graph.map(n => n.id).sort()).toEqual([a, b, c].sort());

      const nodeA = graph.find(n => n.id === a)!;
      const nodeB = graph.find(n => n.id === b)!;
      const nodeC = graph.find(n => n.id === c)!;

      expect(nodeA.dependsOn).toEqual([]);
      expect(nodeA.dependents).toContain(b);
      expect(nodeB.dependsOn).toContain(a);
      expect(nodeB.dependents).toContain(c);
      expect(nodeC.dependsOn).toContain(b);
      expect(nodeC.dependents).toEqual([]);
    });

    test('includes result in node', () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'res' });
      queue.pollAndClaim('email:send');
      queue.markJobDone(id, { sent: true });
      const graph = queue.getJobGraph(id);
      expect(graph[0]!.result).toEqual({ sent: true });
    });
  });

  describe('mountAdminHandler', () => {
    let handler: (req: Request) => Promise<Response>;

    beforeEach(() => {
      handler = queue.mountAdminHandler();
    });

    test('GET /stats returns job stats', async () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(new Request('http://localhost/stats'));
      expect(res.status).toBe(200);
      const body = (await res.json()) as { pending: number };
      expect(body.pending).toBe(1);
    });

    test('GET /jobs returns paginated jobs', async () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(new Request('http://localhost/jobs'));
      expect(res.status).toBe(200);
      const body = (await res.json()) as { items: unknown[]; total: number };
      expect(body.total).toBe(1);
      expect(body.items).toHaveLength(1);
    });

    test('GET /jobs/:id returns job', async () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(new Request(`http://localhost/jobs/${id}`));
      expect(res.status).toBe(200);
      const body = (await res.json()) as { id: number };
      expect(body.id).toBe(id);
    });

    test('GET /jobs/:id returns 404 for missing', async () => {
      const res = await handler(new Request('http://localhost/jobs/9999'));
      expect(res.status).toBe(404);
    });

    test('GET /jobs/types returns job types', async () => {
      queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(new Request('http://localhost/jobs/types'));
      expect(res.status).toBe(200);
      const body = (await res.json()) as string[];
      expect(body).toContain('email:send');
    });

    test('POST /jobs/:id/cancel cancels job', async () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(
        new Request(`http://localhost/jobs/${id}/cancel`, { method: 'POST' })
      );
      expect(res.status).toBe(200);
      const body = (await res.json()) as { ok: boolean };
      expect(body.ok).toBe(true);
      expect(queue.getJob(id)!.status).toBe('cancelled');
    });

    test('GET /failed returns failed jobs', async () => {
      const res = await handler(new Request('http://localhost/failed'));
      expect(res.status).toBe(200);
      const body = (await res.json()) as { items: unknown[]; total: number };
      expect(body.total).toBe(0);
    });

    test('POST /failed/retry-by-type requires type', async () => {
      const res = await handler(
        new Request('http://localhost/failed/retry-by-type', {
          method: 'POST',
          body: JSON.stringify({}),
          headers: { 'Content-Type': 'application/json' }
        })
      );
      expect(res.status).toBe(400);
    });

    test('returns 404 for unknown route', async () => {
      const res = await handler(new Request('http://localhost/unknown'));
      expect(res.status).toBe(404);
    });

    test('prefix strips from path', async () => {
      const prefixed = queue.mountAdminHandler('/admin');
      queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await prefixed(new Request('http://localhost/admin/stats'));
      expect(res.status).toBe(200);
    });

    test('GET /jobs/:id/graph returns graph', async () => {
      const id = queue.add('email:send', { to: 'a@b.com', subject: 'x' });
      const res = await handler(
        new Request(`http://localhost/jobs/${id}/graph`)
      );
      expect(res.status).toBe(200);
      const body = (await res.json()) as unknown[];
      expect(body).toHaveLength(1);
    });
  });
});
