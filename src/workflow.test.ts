import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { JobQueue } from './queue';
import { WorkflowEngine } from './workflow';

type TestJobs = {
  'payment:charge': { amount: number };
  'server:provision': { serverId: string };
  'email:welcome': { userId: string };
  'payment:refund': { amount: number; reason: string };
  'cleanup:rollback': { serverId: string };
};

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe('WorkflowEngine', () => {
  let queue: JobQueue<TestJobs>;
  let engine: WorkflowEngine<TestJobs>;

  beforeEach(() => {
    queue = new JobQueue<TestJobs>(':memory:');
    engine = new WorkflowEngine(queue);
  });

  afterEach(() => {
    queue.close();
  });

  describe('WorkflowBuilder.run()', () => {
    test('creates execution record and enqueues step jobs', () => {
      const { instanceId, jobIds } = engine
        .workflow('order-flow')
        .step('charge', 'payment:charge', { amount: 100 })
        .step(
          'provision',
          'server:provision',
          { serverId: 'srv-1' },
          {
            dependsOn: ['charge']
          }
        )
        .run();

      expect(instanceId).toBeTruthy();
      expect(typeof jobIds.charge).toBe('number');
      expect(typeof jobIds.provision).toBe('number');
      expect(jobIds.charge).toBeGreaterThan(0);
      expect(jobIds.provision).toBeGreaterThan(0);

      const exec = engine.getExecution(instanceId);
      expect(exec).not.toBeNull();
      expect(exec!.name).toBe('order-flow');
      expect(exec!.status).toBe('running');
      expect(exec!.completedAt).toBeNull();
    });

    test('step with dependsOn is enqueued as blocked', () => {
      const { jobIds } = engine
        .workflow('dep-flow')
        .step('charge', 'payment:charge', { amount: 50 })
        .step(
          'notify',
          'email:welcome',
          { userId: 'u1' },
          {
            dependsOn: ['charge']
          }
        )
        .run();

      const chargeJob = queue.getJob(jobIds.charge)!;
      const notifyJob = queue.getJob(jobIds.notify)!;

      expect(chargeJob.status).toBe('pending');
      expect(notifyJob.status).toBe('blocked');
    });

    test('step without dependsOn is enqueued as pending', () => {
      const { jobIds } = engine
        .workflow('simple-flow')
        .step('charge', 'payment:charge', { amount: 10 })
        .run();

      expect(queue.getJob(jobIds.charge)!.status).toBe('pending');
    });

    test('accepts custom instanceId', () => {
      const { instanceId } = engine
        .workflow('my-flow')
        .step('charge', 'payment:charge', { amount: 1 })
        .run('my-custom-id');

      expect(instanceId).toBe('my-custom-id');
      expect(engine.getExecution('my-custom-id')!.id).toBe('my-custom-id');
    });

    test('three-step linear chain — correct dep structure', () => {
      const { jobIds } = engine
        .workflow('chain-flow')
        .step('charge', 'payment:charge', { amount: 200 })
        .step(
          'provision',
          'server:provision',
          { serverId: 'srv-2' },
          {
            dependsOn: ['charge']
          }
        )
        .step(
          'notify',
          'email:welcome',
          { userId: 'u2' },
          {
            dependsOn: ['provision']
          }
        )
        .run();

      expect(queue.getJob(jobIds.charge)!.status).toBe('pending');
      expect(queue.getJob(jobIds.provision)!.status).toBe('blocked');
      expect(queue.getJob(jobIds.notify)!.status).toBe('blocked');
    });

    test('registers compensation config via onFail', () => {
      const { instanceId, jobIds } = engine
        .workflow('saga-flow')
        .step('charge', 'payment:charge', { amount: 300 })
        .step(
          'provision',
          'server:provision',
          { serverId: 'srv-x' },
          {
            dependsOn: ['charge']
          }
        )
        .onFail('charge', {
          compensate: 'payment:refund',
          compensateData: { amount: 300, reason: 'step-failed' }
        })
        .run();

      // Compensation not yet enqueued — no step has failed yet
      expect(queue.getJob(jobIds.charge)!.status).toBe('pending');
      expect(engine.getExecution(instanceId)!.status).toBe('running');
    });
  });

  describe('getExecution', () => {
    test('returns null for unknown instanceId', () => {
      expect(engine.getExecution('non-existent')).toBeNull();
    });

    test('returns execution after run()', () => {
      const { instanceId } = engine
        .workflow('x')
        .step('charge', 'payment:charge', { amount: 1 })
        .run();
      const exec = engine.getExecution(instanceId);
      expect(exec!.status).toBe('running');
    });
  });

  describe('getStepResult', () => {
    test('returns null when step job not yet done', () => {
      const { instanceId } = engine
        .workflow('x')
        .step('charge', 'payment:charge', { amount: 1 })
        .run();
      expect(engine.getStepResult(instanceId, 'charge')).toBeNull();
    });

    test('returns null for unknown step name', () => {
      const { instanceId } = engine
        .workflow('x')
        .step('charge', 'payment:charge', { amount: 1 })
        .run();
      expect(engine.getStepResult(instanceId, 'unknown')).toBeNull();
    });

    test('returns handler result after job completes', async () => {
      const { instanceId, jobIds } = engine
        .workflow('result-flow')
        .step('charge', 'payment:charge', { amount: 99 })
        .run();

      const worker = queue.createWorker({
        type: 'payment:charge',
        handler: async job => ({ charged: job.data.amount, txId: 'tx-123' }),
        pollIntervalMs: 10
      });

      worker.start();
      await sleep(100);
      await worker.stop();

      expect(queue.getJob(jobIds.charge)!.status).toBe('done');

      const result = engine.getStepResult<{ charged: number; txId: string }>(
        instanceId,
        'charge'
      );
      expect(result).toEqual({ charged: 99, txId: 'tx-123' });
    });
  });

  describe('listExecutions', () => {
    test('returns all executions when no filter', () => {
      engine
        .workflow('a')
        .step('charge', 'payment:charge', { amount: 1 })
        .run();
      engine
        .workflow('b')
        .step('charge', 'payment:charge', { amount: 2 })
        .run();

      const list = engine.listExecutions();
      expect(list).toHaveLength(2);
    });

    test('filters by name', () => {
      engine
        .workflow('order')
        .step('charge', 'payment:charge', { amount: 1 })
        .run();
      engine
        .workflow('signup')
        .step('charge', 'payment:charge', { amount: 2 })
        .run();

      expect(engine.listExecutions({ name: 'order' })).toHaveLength(1);
      expect(engine.listExecutions({ name: 'signup' })).toHaveLength(1);
    });

    test('filters by status', () => {
      engine
        .workflow('a')
        .step('charge', 'payment:charge', { amount: 1 })
        .run('id-a');

      expect(engine.listExecutions({ status: 'running' })).toHaveLength(1);
      expect(engine.listExecutions({ status: 'completed' })).toHaveLength(0);
    });

    test('respects limit/offset', () => {
      for (let i = 0; i < 5; i++) {
        engine
          .workflow('x')
          .step('charge', 'payment:charge', { amount: i })
          .run();
      }
      expect(engine.listExecutions({ limit: 3 })).toHaveLength(3);
      expect(engine.listExecutions({ limit: 3, offset: 3 })).toHaveLength(2);
    });
  });

  describe('reconcile — completion', () => {
    test('marks execution completed when all step jobs done', async () => {
      const { instanceId, jobIds } = engine
        .workflow('complete-flow')
        .step('charge', 'payment:charge', { amount: 10 })
        .run();

      const worker = queue.createWorker({
        type: 'payment:charge',
        handler: async () => {},
        pollIntervalMs: 10
      });

      worker.start();
      await sleep(100);
      await worker.stop();

      expect(queue.getJob(jobIds.charge)!.status).toBe('done');

      const result = engine.reconcile();
      expect(result.completed).toBe(1);
      expect(result.failed).toBe(0);
      expect(result.compensated).toBe(0);

      expect(engine.getExecution(instanceId)!.status).toBe('completed');
      expect(engine.getExecution(instanceId)!.completedAt).not.toBeNull();
    });

    test('does not touch running execution with incomplete steps', () => {
      const { instanceId } = engine
        .workflow('pending-flow')
        .step('charge', 'payment:charge', { amount: 10 })
        .run();

      engine.reconcile();
      expect(engine.getExecution(instanceId)!.status).toBe('running');
    });

    test('multi-step: marks completed after all steps done', async () => {
      const { instanceId, jobIds } = engine
        .workflow('chain-complete')
        .step('charge', 'payment:charge', { amount: 50 })
        .step(
          'notify',
          'email:welcome',
          { userId: 'u1' },
          {
            dependsOn: ['charge']
          }
        )
        .run();

      const chargeWorker = queue.createWorker({
        type: 'payment:charge',
        handler: async () => {},
        pollIntervalMs: 10
      });
      const notifyWorker = queue.createWorker({
        type: 'email:welcome',
        handler: async () => {},
        pollIntervalMs: 10
      });

      chargeWorker.start();
      notifyWorker.start();
      await sleep(200);
      await chargeWorker.stop();
      await notifyWorker.stop();

      expect(queue.getJob(jobIds.charge)!.status).toBe('done');
      expect(queue.getJob(jobIds.notify)!.status).toBe('done');

      engine.reconcile();
      expect(engine.getExecution(instanceId)!.status).toBe('completed');
    });
  });

  describe('reconcile — saga compensation', () => {
    // Saga semantics: compensation runs for COMPLETED steps when a LATER step fails.
    // If charge succeeds and provision (which depends on charge) fails,
    // then the refund (compensation for charge) is triggered.

    test('enqueues compensation for completed steps when later step fails', async () => {
      const { instanceId } = engine
        .workflow('saga')
        .step('charge', 'payment:charge', { amount: 500 })
        .step(
          'provision',
          'server:provision',
          { serverId: 'srv-fail' },
          {
            dependsOn: ['charge']
          }
        )
        .onFail('charge', {
          compensate: 'payment:refund',
          compensateData: { amount: 500, reason: 'provision-failed' }
        })
        .run();

      // charge succeeds
      const chargeW = queue.createWorker({
        type: 'payment:charge',
        handler: async () => {},
        pollIntervalMs: 10
      });
      chargeW.start();
      await sleep(100);
      await chargeW.stop();

      // provision fails permanently (unblocked after charge done)
      const provisionW = queue.createWorker({
        type: 'server:provision',
        handler: async () => {
          throw new Error('infra down');
        },
        retryIf: () => false,
        pollIntervalMs: 10
      });
      provisionW.start();
      await sleep(100);
      await provisionW.stop();

      expect(queue.getFailedJobs().items).toHaveLength(1);

      const result = engine.reconcile();
      expect(result.compensated).toBe(1);
      expect(engine.getExecution(instanceId)!.status).toBe('compensating');

      // Refund job enqueued
      expect(queue.getStats().pending).toBeGreaterThanOrEqual(1);
    });

    test('marks failed when no compensation registered and step dies', async () => {
      const { instanceId } = engine
        .workflow('no-comp-saga')
        .step('charge', 'payment:charge', { amount: 100 })
        .run();

      const w = queue.createWorker({
        type: 'payment:charge',
        handler: async () => {
          throw new Error('boom');
        },
        retryIf: () => false,
        pollIntervalMs: 10
      });

      w.start();
      await sleep(100);
      await w.stop();

      const result = engine.reconcile();
      expect(result.failed).toBe(1);
      expect(result.compensated).toBe(0);
      expect(engine.getExecution(instanceId)!.status).toBe('failed');
    });

    test('marks failed after all compensation jobs complete', async () => {
      // charge succeeds → provision fails → refund enqueued → refund completes → workflow=failed
      const { instanceId } = engine
        .workflow('full-saga')
        .step('charge', 'payment:charge', { amount: 200 })
        .step(
          'provision',
          'server:provision',
          { serverId: 'srv-bad' },
          {
            dependsOn: ['charge']
          }
        )
        .onFail('charge', {
          compensate: 'payment:refund',
          compensateData: { amount: 200, reason: 'rollback' }
        })
        .run();

      // charge succeeds
      const chargeW = queue.createWorker({
        type: 'payment:charge',
        handler: async () => {},
        pollIntervalMs: 10
      });
      chargeW.start();
      await sleep(100);
      await chargeW.stop();

      // provision fails permanently
      const provisionW = queue.createWorker({
        type: 'server:provision',
        handler: async () => {
          throw new Error('bad infra');
        },
        retryIf: () => false,
        pollIntervalMs: 10
      });
      provisionW.start();
      await sleep(100);
      await provisionW.stop();

      // First reconcile: transition to compensating, enqueue refund
      engine.reconcile();
      expect(engine.getExecution(instanceId)!.status).toBe('compensating');

      // Run the compensation (refund) job
      const refundW = queue.createWorker({
        type: 'payment:refund',
        handler: async () => {},
        pollIntervalMs: 10
      });
      refundW.start();
      await sleep(100);
      await refundW.stop();

      // Second reconcile: compensation done → mark failed
      const result = engine.reconcile();
      expect(result.failed).toBe(1);
      expect(engine.getExecution(instanceId)!.status).toBe('failed');
      expect(engine.getExecution(instanceId)!.completedAt).not.toBeNull();
    });

    test('marks failed when compensation job itself dead-letters', async () => {
      // charge succeeds → provision fails → refund enqueued → refund exhausts retries → dead-letter
      // Workflow should still transition to 'failed', not stay stuck in 'compensating'
      const { instanceId } = engine
        .workflow('comp-dead-saga')
        .step('charge', 'payment:charge', { amount: 100 })
        .step(
          'provision',
          'server:provision',
          { serverId: 'srv-dead' },
          { dependsOn: ['charge'] }
        )
        .onFail('charge', {
          compensate: 'payment:refund',
          compensateData: { amount: 100, reason: 'rollback' }
        })
        .run();

      // charge succeeds
      const chargeW = queue.createWorker({
        type: 'payment:charge',
        handler: async () => {},
        pollIntervalMs: 10
      });
      chargeW.start();
      await sleep(100);
      await chargeW.stop();

      // provision fails permanently → triggers compensation
      const provisionW = queue.createWorker({
        type: 'server:provision',
        handler: async () => {
          throw new Error('infra down');
        },
        retryIf: () => false,
        pollIntervalMs: 10
      });
      provisionW.start();
      await sleep(100);
      await provisionW.stop();

      // First reconcile: enqueue refund compensation job
      engine.reconcile();
      expect(engine.getExecution(instanceId)!.status).toBe('compensating');

      // Refund itself fails permanently (dead-letters)
      const refundW = queue.createWorker({
        type: 'payment:refund',
        handler: async () => {
          throw new Error('payment gateway down');
        },
        retryIf: () => false,
        pollIntervalMs: 10
      });
      refundW.start();
      await sleep(100);
      await refundW.stop();

      // Refund is now in failed_jobs (dead-lettered)
      const failedJobs = queue.getFailedJobs({ type: 'payment:refund' });
      expect(failedJobs.items).toHaveLength(1);

      // Second reconcile: compensation job dead-lettered → execution must transition to 'failed'
      const result = engine.reconcile();
      expect(result.failed).toBe(1);
      expect(engine.getExecution(instanceId)!.status).toBe('failed');
      expect(engine.getExecution(instanceId)!.completedAt).not.toBeNull();
    });
  });

  describe('topological ordering', () => {
    test('steps defined out of order are enqueued in dep order', () => {
      // 'provision' depends on 'charge' but is defined first
      const { jobIds } = engine
        .workflow('order-test')
        .step(
          'provision',
          'server:provision',
          { serverId: 's1' },
          {
            dependsOn: ['charge']
          }
        )
        .step('charge', 'payment:charge', { amount: 1 })
        .run();

      // charge has no deps → pending; provision depends on charge → blocked
      expect(queue.getJob(jobIds.charge)!.status).toBe('pending');
      expect(queue.getJob(jobIds.provision)!.status).toBe('blocked');
    });
  });
});
