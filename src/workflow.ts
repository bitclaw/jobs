import type { JobQueue } from './queue';
import type {
  AddJobOptions,
  BackoffConfig,
  JobMap,
  WorkflowExecution,
  WorkflowExecutionStatus,
  WorkflowReconcileResult,
  WorkflowRunResult
} from './types';
import { nowISO } from './utils';

export type {
  WorkflowExecution,
  WorkflowExecutionStatus,
  WorkflowReconcileResult,
  WorkflowRunResult
};

type StepStepOptions = Omit<AddJobOptions, 'dependsOn'> & {
  dependsOn?: string[];
  backoff?: BackoffConfig;
};

type StepSpec = {
  stepName: string;
  jobType: string;
  data: unknown;
  dependsOn: string[];
  options: StepStepOptions;
  compensation: { type: string; data: unknown } | null;
};

type WorkflowExecutionRow = {
  id: string;
  name: string;
  status: string;
  created_at: string;
  completed_at: string | null;
};

type WorkflowStepRow = {
  execution_id: string;
  step_name: string;
  job_id: number;
  compensate_type: string | null;
  compensate_data: string | null;
  compensate_job_id: number | null;
  step_order: number;
};

// ─── Helpers ────────────────────────────────────────────────────────────────

function topologicalSort(specs: StepSpec[]): StepSpec[] {
  const nameToSpec = new Map(specs.map(s => [s.stepName, s]));
  const visited = new Set<string>();
  const result: StepSpec[] = [];

  const visit = (name: string): void => {
    if (visited.has(name)) return;
    visited.add(name);
    const spec = nameToSpec.get(name);
    if (!spec) throw new Error(`Unknown step '${name}' in dependsOn`);
    for (const dep of spec.dependsOn) visit(dep);
    result.push(spec);
  };

  for (const spec of specs) visit(spec.stepName);
  return result;
}

function toWorkflowExecution(row: WorkflowExecutionRow): WorkflowExecution {
  return {
    id: row.id,
    name: row.name,
    status: row.status as WorkflowExecutionStatus,
    createdAt: row.created_at,
    completedAt: row.completed_at
  };
}

// ─── WorkflowBuilder ────────────────────────────────────────────────────────

export class WorkflowBuilder<
  TMap extends JobMap,
  TStepNames extends string = never
> {
  private readonly specs: StepSpec[] = [];
  private readonly compensations = new Map<
    string,
    { type: string; data: unknown }
  >();

  constructor(
    private readonly queue: JobQueue<TMap>,
    private readonly name: string
  ) {}

  step<SName extends string, K extends string & keyof TMap>(
    stepName: SName,
    jobType: K,
    data: TMap[K],
    options?: StepStepOptions
  ): WorkflowBuilder<TMap, TStepNames | SName> {
    this.specs.push({
      stepName,
      jobType: jobType as string,
      data,
      dependsOn: (options?.dependsOn ?? []) as string[],
      options: options ?? {},
      compensation: null
    });
    return this as unknown as WorkflowBuilder<TMap, TStepNames | SName>;
  }

  onFail<K extends string & keyof TMap>(
    stepName: TStepNames,
    compensation: { compensate: K; compensateData: TMap[K] }
  ): this {
    this.compensations.set(stepName, {
      type: compensation.compensate as string,
      data: compensation.compensateData
    });
    return this;
  }

  run(instanceId?: string): WorkflowRunResult<TStepNames> {
    const id = instanceId ?? crypto.randomUUID();
    const jobIds: Record<string, number> = {};

    this.queue.db.transaction(() => {
      this.queue.db.run(
        `INSERT INTO workflow_executions (id, name, status, created_at)
         VALUES (?, ?, 'running', ?)`,
        [id, this.name, nowISO()]
      );

      const sorted = topologicalSort(this.specs);

      for (let order = 0; order < sorted.length; order++) {
        const spec = sorted[order]!;
        const depJobIds = spec.dependsOn.map(depName => {
          const depId = jobIds[depName];
          if (depId === undefined)
            throw new Error(
              `Step '${depName}' not enqueued yet — check dependsOn`
            );
          return depId;
        });

        const addOpts: AddJobOptions = {
          priority: spec.options.priority,
          maxRetries: spec.options.maxRetries,
          backoff: spec.options.backoff,
          runAt: spec.options.runAt,
          uniqueKey: spec.options.uniqueKey,
          ...(depJobIds.length > 0 ? { dependsOn: depJobIds } : {})
        };

        const jobId = this.queue.add(
          spec.jobType as string & keyof TMap,
          spec.data as TMap[string & keyof TMap],
          addOpts
        );

        jobIds[spec.stepName] = jobId;
        const comp = this.compensations.get(spec.stepName) ?? null;

        this.queue.db.run(
          `INSERT INTO workflow_steps
             (execution_id, step_name, job_id, compensate_type, compensate_data, step_order)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [
            id,
            spec.stepName,
            jobId,
            comp?.type ?? null,
            comp ? JSON.stringify(comp.data) : null,
            order
          ]
        );
      }
    })();

    return { instanceId: id, jobIds: jobIds as Record<TStepNames, number> };
  }
}

// ─── WorkflowEngine ─────────────────────────────────────────────────────────

export class WorkflowEngine<TMap extends JobMap> {
  constructor(private readonly queue: JobQueue<TMap>) {}

  workflow<TStepNames extends string = never>(
    name: string
  ): WorkflowBuilder<TMap, TStepNames> {
    return new WorkflowBuilder(this.queue, name);
  }

  getExecution(instanceId: string): WorkflowExecution | null {
    const row = this.queue.db
      .query('SELECT * FROM workflow_executions WHERE id = ?')
      .get(instanceId) as WorkflowExecutionRow | null;
    return row ? toWorkflowExecution(row) : null;
  }

  getStepResult<T>(instanceId: string, stepName: string): T | null {
    const step = this.queue.db
      .query(
        'SELECT job_id FROM workflow_steps WHERE execution_id = ? AND step_name = ?'
      )
      .get(instanceId, stepName) as { job_id: number } | null;
    if (!step) return null;
    return this.queue.getJobResult<T>(step.job_id);
  }

  listExecutions(
    opts: {
      status?: WorkflowExecutionStatus;
      name?: string;
      limit?: number;
      offset?: number;
    } = {}
  ): WorkflowExecution[] {
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (opts.status) {
      conditions.push('status = ?');
      params.push(opts.status);
    }
    if (opts.name) {
      conditions.push('name = ?');
      params.push(opts.name);
    }

    const where =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const limit = opts.limit ?? 50;
    const offset = opts.offset ?? 0;

    const rows = this.queue.db
      .query(
        `SELECT * FROM workflow_executions ${where}
         ORDER BY created_at DESC LIMIT ? OFFSET ?`
      )
      .all(
        ...(params as Parameters<typeof this.queue.db.query>),
        limit,
        offset
      ) as WorkflowExecutionRow[];

    return rows.map(toWorkflowExecution);
  }

  reconcile(): WorkflowReconcileResult {
    const stats: WorkflowReconcileResult = {
      completed: 0,
      compensated: 0,
      failed: 0
    };

    const running = this.queue.db
      .query(
        "SELECT * FROM workflow_executions WHERE status IN ('running', 'compensating')"
      )
      .all() as WorkflowExecutionRow[];

    for (const exec of running) {
      this.reconcileExecution(exec, stats);
    }

    return stats;
  }

  private reconcileExecution(
    exec: WorkflowExecutionRow,
    stats: WorkflowReconcileResult
  ): void {
    const steps = this.queue.db
      .query(
        'SELECT * FROM workflow_steps WHERE execution_id = ? ORDER BY step_order ASC'
      )
      .all(exec.id) as WorkflowStepRow[];

    if (exec.status === 'compensating') {
      // If all comp jobs done (or no comp jobs), mark failed
      const pendingComp = steps.filter(s => {
        if (!s.compensate_job_id) return false;
        const job = this.queue.getJob(s.compensate_job_id);
        if (job !== null) return job.status !== 'done';
        // job removed from jobs table — check if it dead-lettered (permanent failure)
        const dead = this.queue.db
          .query('SELECT id FROM failed_jobs WHERE original_job_id = ? LIMIT 1')
          .get(s.compensate_job_id) as { id: number } | null;
        if (dead !== null) {
          console.warn(
            '[workflow] compensation job dead-lettered: exec=%s step=%s compensate_job_id=%d',
            exec.id,
            s.step_name,
            s.compensate_job_id
          );
        }
        return false; // removed from jobs table (done or dead) → not pending
      });
      if (pendingComp.length === 0) {
        this.queue.db.run(
          "UPDATE workflow_executions SET status = 'failed', completed_at = ? WHERE id = ?",
          [nowISO(), exec.id]
        );
        stats.failed++;
      }
      return;
    }

    // Build step status map
    const stepStatuses = steps.map(step => {
      const job = this.queue.getJob(step.job_id);
      if (!job) {
        // Job deleted from jobs table — check failed_jobs
        const dead = this.queue.db
          .query('SELECT id FROM failed_jobs WHERE original_job_id = ? LIMIT 1')
          .get(step.job_id) as { id: number } | null;
        return { step, status: dead ? ('dead' as const) : ('done' as const) };
      }
      return { step, status: job.status };
    });

    // Any step dead → start compensation
    const deadStep = stepStatuses.find(s => s.status === 'dead');
    if (deadStep) {
      const stepsWithComp = stepStatuses
        .filter(s => s.status === 'done' && s.step.compensate_type)
        .reverse();

      this.queue.db.transaction(() => {
        if (stepsWithComp.length > 0) {
          this.queue.db.run(
            "UPDATE workflow_executions SET status = 'compensating' WHERE id = ?",
            [exec.id]
          );

          for (const { step } of stepsWithComp) {
            const compData = step.compensate_data
              ? (JSON.parse(step.compensate_data) as TMap[string & keyof TMap])
              : ({} as TMap[string & keyof TMap]);

            const compJobId = this.queue.add(
              step.compensate_type! as string & keyof TMap,
              compData,
              {}
            );
            this.queue.db.run(
              'UPDATE workflow_steps SET compensate_job_id = ? WHERE execution_id = ? AND step_name = ?',
              [compJobId, exec.id, step.step_name]
            );
          }
          stats.compensated++;
        } else {
          // No compensation needed — immediately fail
          this.queue.db.run(
            "UPDATE workflow_executions SET status = 'failed', completed_at = ? WHERE id = ?",
            [nowISO(), exec.id]
          );
          stats.failed++;
        }
      })();
      return;
    }

    // All done → mark completed
    if (stepStatuses.every(s => s.status === 'done')) {
      this.queue.db.run(
        "UPDATE workflow_executions SET status = 'completed', completed_at = ? WHERE id = ?",
        [nowISO(), exec.id]
      );
      stats.completed++;
    }
  }
}
