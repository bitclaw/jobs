import { nowISO } from './utils';
// ─── Helpers ────────────────────────────────────────────────────────────────
function topologicalSort(specs) {
    const nameToSpec = new Map(specs.map(s => [s.stepName, s]));
    const visited = new Set();
    const result = [];
    const visit = (name) => {
        if (visited.has(name))
            return;
        visited.add(name);
        const spec = nameToSpec.get(name);
        if (!spec)
            throw new Error(`Unknown step '${name}' in dependsOn`);
        for (const dep of spec.dependsOn)
            visit(dep);
        result.push(spec);
    };
    for (const spec of specs)
        visit(spec.stepName);
    return result;
}
function toWorkflowExecution(row) {
    return {
        id: row.id,
        name: row.name,
        status: row.status,
        createdAt: row.created_at,
        completedAt: row.completed_at
    };
}
// ─── WorkflowBuilder ────────────────────────────────────────────────────────
export class WorkflowBuilder {
    queue;
    name;
    specs = [];
    compensations = new Map();
    constructor(queue, name) {
        this.queue = queue;
        this.name = name;
    }
    step(stepName, jobType, data, options) {
        this.specs.push({
            stepName,
            jobType: jobType,
            data,
            dependsOn: (options?.dependsOn ?? []),
            options: options ?? {},
            compensation: null
        });
        return this;
    }
    onFail(stepName, compensation) {
        this.compensations.set(stepName, {
            type: compensation.compensate,
            data: compensation.compensateData
        });
        return this;
    }
    run(instanceId) {
        const id = instanceId ?? crypto.randomUUID();
        const jobIds = {};
        this.queue.db.transaction(() => {
            this.queue.db.run(`INSERT INTO workflow_executions (id, name, status, created_at)
         VALUES (?, ?, 'running', ?)`, [id, this.name, nowISO()]);
            const sorted = topologicalSort(this.specs);
            for (let order = 0; order < sorted.length; order++) {
                const spec = sorted[order];
                const depJobIds = spec.dependsOn.map(depName => {
                    const depId = jobIds[depName];
                    if (depId === undefined)
                        throw new Error(`Step '${depName}' not enqueued yet — check dependsOn`);
                    return depId;
                });
                const addOpts = {
                    priority: spec.options.priority,
                    maxRetries: spec.options.maxRetries,
                    backoff: spec.options.backoff,
                    runAt: spec.options.runAt,
                    uniqueKey: spec.options.uniqueKey,
                    ...(depJobIds.length > 0 ? { dependsOn: depJobIds } : {})
                };
                const jobId = this.queue.add(spec.jobType, spec.data, addOpts);
                jobIds[spec.stepName] = jobId;
                const comp = this.compensations.get(spec.stepName) ?? null;
                this.queue.db.run(`INSERT INTO workflow_steps
             (execution_id, step_name, job_id, compensate_type, compensate_data, step_order)
           VALUES (?, ?, ?, ?, ?, ?)`, [
                    id,
                    spec.stepName,
                    jobId,
                    comp?.type ?? null,
                    comp ? JSON.stringify(comp.data) : null,
                    order
                ]);
            }
        })();
        return { instanceId: id, jobIds: jobIds };
    }
}
// ─── WorkflowEngine ─────────────────────────────────────────────────────────
export class WorkflowEngine {
    queue;
    constructor(queue) {
        this.queue = queue;
    }
    workflow(name) {
        return new WorkflowBuilder(this.queue, name);
    }
    getExecution(instanceId) {
        const row = this.queue.db
            .query('SELECT * FROM workflow_executions WHERE id = ?')
            .get(instanceId);
        return row ? toWorkflowExecution(row) : null;
    }
    getStepResult(instanceId, stepName) {
        const step = this.queue.db
            .query('SELECT job_id FROM workflow_steps WHERE execution_id = ? AND step_name = ?')
            .get(instanceId, stepName);
        if (!step)
            return null;
        return this.queue.getJobResult(step.job_id);
    }
    listExecutions(opts = {}) {
        const conditions = [];
        const params = [];
        if (opts.status) {
            conditions.push('status = ?');
            params.push(opts.status);
        }
        if (opts.name) {
            conditions.push('name = ?');
            params.push(opts.name);
        }
        const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
        const limit = opts.limit ?? 50;
        const offset = opts.offset ?? 0;
        const rows = this.queue.db
            .query(`SELECT * FROM workflow_executions ${where}
         ORDER BY created_at DESC LIMIT ? OFFSET ?`)
            .all(...params, limit, offset);
        return rows.map(toWorkflowExecution);
    }
    reconcile() {
        const stats = {
            completed: 0,
            compensated: 0,
            failed: 0
        };
        const running = this.queue.db
            .query("SELECT * FROM workflow_executions WHERE status IN ('running', 'compensating')")
            .all();
        for (const exec of running) {
            this.reconcileExecution(exec, stats);
        }
        return stats;
    }
    reconcileExecution(exec, stats) {
        const steps = this.queue.db
            .query('SELECT * FROM workflow_steps WHERE execution_id = ? ORDER BY step_order ASC')
            .all(exec.id);
        if (exec.status === 'compensating') {
            // If all comp jobs done (or no comp jobs), mark failed
            const pendingComp = steps.filter(s => {
                if (!s.compensate_job_id)
                    return false;
                const job = this.queue.getJob(s.compensate_job_id);
                return job !== null && job.status !== 'done';
            });
            if (pendingComp.length === 0) {
                this.queue.db.run("UPDATE workflow_executions SET status = 'failed', completed_at = ? WHERE id = ?", [nowISO(), exec.id]);
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
                    .get(step.job_id);
                return { step, status: dead ? 'dead' : 'done' };
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
                    this.queue.db.run("UPDATE workflow_executions SET status = 'compensating' WHERE id = ?", [exec.id]);
                    for (const { step } of stepsWithComp) {
                        const compData = step.compensate_data
                            ? JSON.parse(step.compensate_data)
                            : {};
                        const compJobId = this.queue.add(step.compensate_type, compData, {});
                        this.queue.db.run('UPDATE workflow_steps SET compensate_job_id = ? WHERE execution_id = ? AND step_name = ?', [compJobId, exec.id, step.step_name]);
                    }
                    stats.compensated++;
                }
                else {
                    // No compensation needed — immediately fail
                    this.queue.db.run("UPDATE workflow_executions SET status = 'failed', completed_at = ? WHERE id = ?", [nowISO(), exec.id]);
                    stats.failed++;
                }
            })();
            return;
        }
        // All done → mark completed
        if (stepStatuses.every(s => s.status === 'done')) {
            this.queue.db.run("UPDATE workflow_executions SET status = 'completed', completed_at = ? WHERE id = ?", [nowISO(), exec.id]);
            stats.completed++;
        }
    }
}
