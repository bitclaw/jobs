// packages/jobs/src/queue.ts
// JobQueue , SQLite-backed job queue with typed payloads and dependency support
import { Database } from 'bun:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { createAdminHandler } from './admin';
import { JobQueueEmitter } from './events';
import { applyPragmas, initializeSchema } from './schema';
import { createStatements } from './statements';
import { nowISO } from './utils';
import { JobWorker } from './worker';
function toJob(row) {
    return {
        id: row.id,
        type: row.type,
        data: JSON.parse(row.data),
        status: row.status,
        priority: row.priority,
        progress: row.progress,
        maxRetries: row.max_retries,
        retryCount: row.retry_count,
        runAt: row.run_at,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        startedAt: row.started_at,
        completedAt: row.completed_at,
        error: row.error,
        batchId: row.batch_id,
        requestLog: row.request_log,
        responseLog: row.response_log,
        uniqueKey: row.unique_key,
        claimedUntil: row.claimed_until,
        result: row.result ? JSON.parse(row.result) : null,
        expireAt: row.expire_at
    };
}
function toBatch(row) {
    return {
        id: row.id,
        name: row.name,
        totalJobs: row.total_jobs,
        pendingJobs: row.pending_jobs,
        failedJobs: row.failed_jobs,
        failedJobIds: JSON.parse(row.failed_job_ids),
        options: row.options ? JSON.parse(row.options) : null,
        cancelledAt: row.cancelled_at,
        createdAt: row.created_at,
        finishedAt: row.finished_at
    };
}
function toFailedJob(row) {
    return {
        id: row.id,
        originalJobId: row.original_job_id,
        type: row.type,
        data: JSON.parse(row.data),
        error: row.error,
        retryCount: row.retry_count,
        maxRetries: row.max_retries,
        createdAt: row.created_at,
        failedAt: row.failed_at,
        requestLog: row.request_log,
        responseLog: row.response_log
    };
}
export class JobQueue extends JobQueueEmitter {
    db;
    middlewares = [];
    stmts;
    // Backstop for wal_autocheckpoint being left at SQLite's default rather
    // than disabled - see schema.ts's applyPragmas. Bounds the WAL to
    // roughly "writes accumulated per interval" under normal conditions;
    // a PASSIVE checkpoint yields to any active reader/writer rather than
    // blocking, so this isn't a hard guarantee under sustained contention,
    // just a real backstop between SQLite's own auto-checkpoint triggers.
    checkpointIntervalId;
    constructor(dbPath) {
        super();
        mkdirSync(dirname(dbPath), { recursive: true });
        this.db = new Database(dbPath, { create: true });
        applyPragmas(this.db);
        initializeSchema(this.db);
        this.stmts = createStatements(this.db);
        this.checkpointIntervalId =
            this.db.filename !== ':memory:' && this.db.filename !== ''
                ? setInterval(() => {
                    try {
                        this.db.run('PRAGMA wal_checkpoint(PASSIVE)');
                    }
                    catch {
                        // Non-critical - next tick or close() will retry/finalize.
                    }
                }, 60_000)
                : null;
        if (this.checkpointIntervalId?.unref) {
            this.checkpointIntervalId.unref();
        }
    }
    add(type, data, options) {
        return this.db
            .transaction(() => this.insertJob(type, data, null, options))
            .immediate();
    }
    getJob(id) {
        const row = this.stmts.selectJob.get({ $id: id });
        return row ? toJob(row) : null;
    }
    getStats() {
        const rows = this.stmts.selectStats.all();
        const deadRow = this.stmts.countFailed.get();
        const stats = {
            pending: 0,
            blocked: 0,
            processing: 0,
            done: 0,
            failed: 0,
            cancelled: 0,
            dead: deadRow.count
        };
        for (const row of rows) {
            const key = row.status;
            if (key in stats) {
                stats[key] = row.count;
            }
        }
        return stats;
    }
    getFailedJobs(options) {
        const limit = options?.limit ?? 100;
        const offset = options?.offset ?? 0;
        let rows;
        let total;
        if (options?.type) {
            rows = this.db
                .query('SELECT * FROM failed_jobs WHERE type = $type ORDER BY failed_at DESC LIMIT $limit OFFSET $offset')
                .all({
                $type: options.type,
                $limit: limit,
                $offset: offset
            });
            total = this.db
                .query('SELECT COUNT(*) as count FROM failed_jobs WHERE type = $type')
                .get({ $type: options.type }).count;
        }
        else {
            rows = this.db
                .query('SELECT * FROM failed_jobs ORDER BY failed_at DESC LIMIT $limit OFFSET $offset')
                .all({ $limit: limit, $offset: offset });
            total = this.stmts.countFailed.get().count;
        }
        return { items: rows.map(toFailedJob), total };
    }
    listJobs(options) {
        const limit = options?.limit ?? 50;
        const offset = options?.offset ?? 0;
        const conditions = [];
        const filterParams = {};
        if (options?.status) {
            conditions.push('status = $status');
            filterParams.$status = options.status;
        }
        if (options?.type) {
            conditions.push('type = $type');
            filterParams.$type = options.type;
        }
        const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
        const rows = this.db
            .query(`SELECT * FROM jobs ${where} ORDER BY created_at DESC LIMIT $limit OFFSET $offset`)
            .all({ ...filterParams, $limit: limit, $offset: offset });
        const total = this.db
            .query(`SELECT COUNT(*) as count FROM jobs ${where}`)
            .get(filterParams).count;
        return { items: rows.map(toJob), total };
    }
    cancelJob(id) {
        const result = this.db
            .query("UPDATE jobs SET status = 'cancelled', updated_at = $now WHERE id = $id AND status IN ('pending', 'blocked')")
            .run({ $id: id, $now: nowISO() });
        const cancelled = result.changes > 0;
        // A dependent blocked on this job must not stay blocked forever just
        // because the dependency was cancelled instead of completing - same
        // "wake on any terminal outcome" reasoning as markJobDone's own call.
        // Row isn't deleted on cancel, so order relative to the status update
        // above doesn't matter here (unlike markJobDead/markJobFailed).
        if (cancelled)
            this.tryUnblockDependents(this.getDependents(id));
        return cancelled;
    }
    forceRetryJob(id) {
        const result = this.db
            .query("UPDATE jobs SET status = 'pending', retry_count = 0, error = NULL, run_at = $now, started_at = NULL, updated_at = $now WHERE id = $id AND status IN ('processing', 'cancelled')")
            .run({ $id: id, $now: nowISO() });
        return result.changes > 0;
    }
    setJobHttpLog(id, requestLog, responseLog) {
        this.db
            .query('UPDATE jobs SET request_log = $req, response_log = $res, updated_at = $now WHERE id = $id')
            .run({ $id: id, $req: requestLog, $res: responseLog, $now: nowISO() });
    }
    getJobTypes() {
        const rows = this.db
            .query('SELECT DISTINCT type FROM jobs ORDER BY type')
            .all();
        return rows.map(r => r.type);
    }
    retryFailedJob(failedJobId) {
        return this.db
            .transaction(() => {
            const row = this.db
                .query('SELECT * FROM failed_jobs WHERE id = $id')
                .get({ $id: failedJobId });
            if (!row) {
                throw new Error(`Failed job ${failedJobId} not found`);
            }
            const now = nowISO();
            this.stmts.insertJob.run({
                $type: row.type,
                $data: row.data,
                $status: 'pending',
                $priority: 0,
                $maxRetries: row.max_retries,
                $runAt: now,
                $batchId: null,
                $uniqueKey: null,
                $backoffConfig: null,
                $expireAt: null,
                $webhookConfig: null
            });
            const newJobId = this.stmts.lastInsertRowId.get();
            this.db
                .query('DELETE FROM failed_jobs WHERE id = $id')
                .run({ $id: failedJobId });
            return newJobId.id;
        })
            .immediate();
    }
    purgeFailedJobs(olderThanMs) {
        const cutoff = new Date(Date.now() - olderThanMs).toISOString();
        const result = this.db
            .query('DELETE FROM failed_jobs WHERE failed_at < $cutoff')
            .run({ $cutoff: cutoff });
        return result.changes;
    }
    purge(options) {
        const cutoff = new Date(Date.now() - options.olderThanMs).toISOString();
        const result = this.db
            .query('DELETE FROM jobs WHERE status = $status AND updated_at < $cutoff')
            .run({ $status: options.status, $cutoff: cutoff });
        return result.changes;
    }
    pollAndClaim(type, leaseMs = 300_000) {
        const now = nowISO();
        const claimedUntil = new Date(Date.now() + leaseMs).toISOString();
        const claimTx = this.db.transaction(() => {
            const row = this.stmts.selectPending.get({
                $type: type,
                $now: now
            });
            if (!row)
                return null;
            this.stmts.markProcessing.run({
                $id: row.id,
                $now: now,
                $claimedUntil: claimedUntil
            });
            return row;
        });
        const row = claimTx.immediate();
        return row ? toJob(row) : null;
    }
    renewLease(id, leaseMs) {
        const claimedUntil = new Date(Date.now() + leaseMs).toISOString();
        this.stmts.renewLease.run({
            $id: id,
            $claimedUntil: claimedUntil,
            $now: nowISO()
        });
    }
    markJobDone(id, result) {
        let doneJob = null;
        // Use container object so TypeScript tracks mutation across the closure
        const wh = { config: null };
        this.db
            .transaction(() => {
            const now = nowISO();
            const row = this.stmts.selectJob.get({ $id: id });
            if (row?.webhook_config) {
                try {
                    wh.config = JSON.parse(row.webhook_config);
                }
                catch {
                    // ignore malformed config
                }
            }
            this.stmts.markDone.run({
                $id: id,
                $now: now,
                $result: result !== undefined ? JSON.stringify(result) : null
            });
            this.tryUnblockDependents(this.getDependents(id));
            if (row?.batch_id) {
                this.handleBatchJobComplete(row.batch_id);
            }
            const updatedRow = this.stmts.selectJob.get({
                $id: id
            });
            if (updatedRow)
                doneJob = toJob(updatedRow);
        })
            .immediate();
        if (doneJob)
            this.emit('job:done', doneJob);
        if (wh.config && doneJob) {
            const cfg = wh.config;
            const payload = doneJob;
            void fetch(cfg.url, {
                method: cfg.method ?? 'POST',
                headers: { 'Content-Type': 'application/json', ...(cfg.headers ?? {}) },
                body: JSON.stringify({ job: payload, result })
            }).catch(() => { });
        }
    }
    markJobDead(id, error) {
        let deadJob = null;
        this.db
            .transaction(() => {
            const row = this.stmts.selectJob.get({ $id: id });
            if (!row)
                return;
            // capture before delete
            deadJob = toJob(row);
            this.stmts.insertFailedJob.run({
                $originalJobId: row.id,
                $type: row.type,
                $data: row.data,
                $error: error,
                $retryCount: row.retry_count,
                $maxRetries: row.max_retries,
                $createdAt: row.created_at,
                $requestLog: row.request_log,
                $responseLog: row.response_log
            });
            // Captured *before* deleteJob, not after - job_dependencies has
            // ON DELETE CASCADE on depends_on_id (schema.ts), so deleting this
            // row wipes the dependency edges as a side effect; capturing first is
            // what lets a dependent still get found and unblocked below.
            const dependents = this.getDependents(id);
            this.stmts.deleteJob.run({ $id: id });
            // A dependent blocked on this job must not stay blocked forever just
            // because the dependency died instead of completing - same "wake on
            // any terminal outcome" reasoning as markJobDone's own call.
            this.tryUnblockDependents(dependents);
            if (row.batch_id) {
                this.stmts.incrementBatchFailed.run({
                    $id: row.batch_id,
                    $jobId: row.id
                });
                this.handleBatchJobComplete(row.batch_id);
            }
        })
            .immediate();
        if (deadJob)
            this.emit('job:dead', deadJob, error);
    }
    markJobFailed(id, error) {
        let failedJob = null;
        this.db
            .transaction(() => {
            const now = nowISO();
            const row = this.stmts.selectJob.get({ $id: id });
            if (!row)
                return;
            if (row.retry_count + 1 >= row.max_retries) {
                this.stmts.insertFailedJob.run({
                    $originalJobId: row.id,
                    $type: row.type,
                    $data: row.data,
                    $error: error,
                    $retryCount: row.retry_count + 1,
                    $maxRetries: row.max_retries,
                    $createdAt: row.created_at,
                    $requestLog: row.request_log,
                    $responseLog: row.response_log
                });
                // Captured *before* deleteJob, not after - job_dependencies has
                // ON DELETE CASCADE on depends_on_id (schema.ts), so deleting this
                // row wipes the dependency edges as a side effect; capturing first
                // is what lets a dependent still get found and unblocked below.
                const dependents = this.getDependents(id);
                this.stmts.deleteJob.run({ $id: id });
                // A dependent blocked on this job must not stay blocked forever
                // just because the dependency dead-lettered instead of completing -
                // same "wake on any terminal outcome" reasoning as markJobDone's
                // own call.
                this.tryUnblockDependents(dependents);
                // Job is permanently dead , decrement batch counter and track failure
                if (row.batch_id) {
                    this.stmts.incrementBatchFailed.run({
                        $id: row.batch_id,
                        $jobId: row.id
                    });
                    this.handleBatchJobComplete(row.batch_id);
                }
            }
            else {
                const backoff = row.backoff_config
                    ? JSON.parse(row.backoff_config)
                    : null;
                let retryRunAt = now;
                if (backoff) {
                    let delayMs;
                    switch (backoff.type) {
                        case 'exponential':
                            delayMs = Math.min(backoff.delayMs * 2 ** row.retry_count, 3_600_000);
                            break;
                        case 'jitter':
                            delayMs = Math.min(backoff.delayMs *
                                2 ** row.retry_count *
                                (0.5 + Math.random()), 3_600_000);
                            break;
                        case 'fibonacci':
                            delayMs = Math.min(backoff.delayMs * this.fib(row.retry_count), 3_600_000);
                            break;
                        default: // 'fixed'
                            delayMs = backoff.delayMs;
                    }
                    retryRunAt = new Date(Date.now() + delayMs).toISOString();
                }
                this.stmts.markFailed.run({
                    $id: id,
                    $error: error,
                    $runAt: retryRunAt,
                    $now: now
                });
                // capture updated job for post-tx emit
                const updatedRow = this.stmts.selectJob.get({
                    $id: id
                });
                if (updatedRow)
                    failedJob = toJob(updatedRow);
            }
        })
            .immediate();
        if (failedJob)
            this.emit('job:failed', failedJob, error);
    }
    fib(n) {
        if (n <= 1)
            return 1;
        let a = 1, b = 1;
        for (let i = 2; i <= n; i++) {
            [a, b] = [b, a + b];
        }
        return b;
    }
    updateProgress(id, progress) {
        this.stmts.updateProgress.run({
            $id: id,
            $progress: progress,
            $now: nowISO()
        });
        const row = this.stmts.selectJob.get({ $id: id });
        if (row)
            this.emit('job:progress', toJob(row), progress);
    }
    // --- Batch API ---
    createBatch(name, options) {
        const id = crypto.randomUUID();
        this.stmts.insertBatch.run({
            $id: id,
            $name: name,
            $options: options ? JSON.stringify(options) : null,
            $createdAt: nowISO()
        });
        return id;
    }
    addToBatch(batchId, type, data, options) {
        return this.db
            .transaction(() => {
            const batchExists = this.db
                .query('SELECT id FROM job_batches WHERE id = ? LIMIT 1')
                .get(batchId);
            if (!batchExists)
                throw new Error(`addToBatch: batch "${batchId}" does not exist`);
            const jobId = this.insertJob(type, data, batchId, options);
            this.db
                .query('UPDATE job_batches SET total_jobs = total_jobs + 1, pending_jobs = pending_jobs + 1 WHERE id = $id')
                .run({ $id: batchId });
            return jobId;
        })
            .immediate();
    }
    getBatch(batchId) {
        const row = this.stmts.selectBatch.get({
            $id: batchId
        });
        return row ? toBatch(row) : null;
    }
    cancelBatch(batchId) {
        const now = nowISO();
        this.db
            .transaction(() => {
            this.stmts.cancelBatch.run({ $id: batchId, $now: now });
            this.stmts.cancelBatchJobs.run({ $batchId: batchId, $now: now });
        })
            .immediate();
    }
    createWorker(options) {
        return new JobWorker(this, options);
    }
    insertJob(type, data, batchId, options) {
        const now = nowISO();
        const runAt = options?.runAt ? options.runAt.toISOString() : now;
        // Resolved *before* deciding status/inserting the row, not after - a
        // dependency that already reached a terminal state (done/cancelled/
        // dead-lettered) by the time this runs must not block the new job at
        // all, since unblockDependents only ever fires once, at the moment a
        // dependency itself completes; inserting 'blocked' against an
        // already-resolved dependency would never get unblocked. Also
        // distinguishes "dependency id was legitimately dead-lettered" (not an
        // error - already resolved) from "dependency id never existed" (a real
        // caller bug, must still throw).
        const liveDepIds = [];
        if (options?.dependsOn) {
            for (const depId of options.dependsOn) {
                const dep = this.stmts.selectJob.get({ $id: depId });
                if (dep) {
                    if (dep.status === 'pending' ||
                        dep.status === 'processing' ||
                        dep.status === 'blocked') {
                        liveDepIds.push(depId);
                    }
                    // else: dep exists but already 'done'/'cancelled' - resolved,
                    // doesn't block, no edge needed.
                    continue;
                }
                const deadLettered = this.stmts.selectFailedJobByOriginalId.get({
                    $originalJobId: depId
                });
                if (!deadLettered) {
                    throw new Error(`Dependency job ${depId} does not exist`);
                }
                // else: dead-lettered - resolved, doesn't block, no edge needed.
            }
        }
        const hasDeps = liveDepIds.length > 0;
        const status = hasDeps ? 'blocked' : 'pending';
        const expireAt = options?.expireAt ? options.expireAt.toISOString() : null;
        const webhookConfig = options?.onComplete
            ? JSON.stringify(options.onComplete)
            : null;
        // dedup='replace': update existing pending job's data + run_at
        if (options?.dedup === 'replace' && options.uniqueKey) {
            const existing = this.stmts.selectDedupedJob.get({
                $type: type,
                $uniqueKey: options.uniqueKey
            });
            if (existing) {
                this.db
                    .query('UPDATE jobs SET data = $data, run_at = $runAt, updated_at = $now WHERE id = $id')
                    .run({
                    $id: existing.id,
                    $data: JSON.stringify(data),
                    $runAt: runAt,
                    $now: now
                });
                return existing.id;
            }
        }
        const result = this.stmts.insertJob.run({
            $type: type,
            $data: JSON.stringify(data),
            $status: status,
            $priority: options?.priority ?? 0,
            $maxRetries: options?.maxRetries ?? 3,
            $runAt: runAt,
            $batchId: batchId,
            $uniqueKey: options?.uniqueKey ?? null,
            $backoffConfig: options?.backoff ? JSON.stringify(options.backoff) : null,
            $expireAt: expireAt,
            $webhookConfig: webhookConfig
        });
        // INSERT OR IGNORE: if a pending/processing job with same (type, uniqueKey)
        // already exists, the insert is a no-op. Return the existing job id.
        if (result.changes === 0 && options?.uniqueKey) {
            const existing = this.stmts.selectDedupedJob.get({
                $type: type,
                $uniqueKey: options.uniqueKey
            });
            if (!existing) {
                throw new Error(`uniqueKey dedup race: job type="${type}" key="${options.uniqueKey}" completed between INSERT and SELECT`);
            }
            return existing.id;
        }
        const jobId = this.stmts.lastInsertRowId.get();
        for (const depId of liveDepIds) {
            this.stmts.insertDep.run({
                $jobId: jobId.id,
                $depsOnId: depId
            });
        }
        return jobId.id;
    }
    /**
     * Reset stuck `processing` jobs back to `pending`. Call at startup to recover
     * from server crashes that left jobs claimed but never completed.
     * @param thresholdMs Jobs processing longer than this (ms) are reset. Default 5min.
     * @returns Number of jobs reset.
     */
    reconcileStaleJobs(thresholdMs = 300_000) {
        const cutoff = new Date(Date.now() - thresholdMs).toISOString();
        const result = this.db
            .query(`UPDATE jobs
         SET status = 'pending',
             error = 'stale: worker crash or restart , reset for retry',
             updated_at = $now
         WHERE status = 'processing' AND updated_at < $cutoff`)
            .run({ $now: nowISO(), $cutoff: cutoff });
        const count = result.changes;
        if (count > 0)
            this.emit('job:stale', count);
        return count;
    }
    /**
     * Look up a pending or processing job by its uniqueKey.
     * Returns null if no such job exists (completed, dead-lettered, or never queued).
     */
    getJobByUniqueKey(type, uniqueKey) {
        const row = this.db
            .query(`SELECT * FROM jobs
         WHERE type = $type AND unique_key = $uniqueKey
           AND status IN ('pending', 'processing')
         LIMIT 1`)
            .get({ $type: type, $uniqueKey: uniqueKey });
        return row ? toJob(row) : null;
    }
    getJobResult(id) {
        const row = this.stmts.selectJob.get({ $id: id });
        if (!row?.result)
            return null;
        return JSON.parse(row.result);
    }
    cancelByUniqueKey(type, uniqueKey) {
        const result = this.db
            .query("UPDATE jobs SET status = 'cancelled', updated_at = $now WHERE type = $type AND unique_key = $uniqueKey AND status IN ('pending', 'blocked')")
            .run({ $type: type, $uniqueKey: uniqueKey, $now: nowISO() });
        return result.changes > 0;
    }
    retryFailedJobsByType(type) {
        return this.db
            .transaction(() => {
            const rows = this.db
                .query('SELECT * FROM failed_jobs WHERE type = $type')
                .all({ $type: type });
            if (rows.length === 0)
                return 0;
            const now = nowISO();
            for (const row of rows) {
                this.stmts.insertJob.run({
                    $type: row.type,
                    $data: row.data,
                    $status: 'pending',
                    $priority: 0,
                    $maxRetries: row.max_retries,
                    $runAt: now,
                    $batchId: null,
                    $uniqueKey: null,
                    $backoffConfig: null,
                    $expireAt: null,
                    $webhookConfig: null
                });
                this.db
                    .query('DELETE FROM failed_jobs WHERE id = $id')
                    .run({ $id: row.id });
            }
            return rows.length;
        })
            .immediate();
    }
    purgeExpiredJobs() {
        const result = this.db
            .query("DELETE FROM jobs WHERE expire_at IS NOT NULL AND expire_at <= $now AND status = 'pending'")
            .run({ $now: nowISO() });
        return result.changes;
    }
    use(fn) {
        this.middlewares.push(fn);
    }
    getJobGraph(rootId) {
        const relatedRows = this.db
            .query(`WITH RECURSIVE
          ancestors(id) AS (
            SELECT depends_on_id FROM job_dependencies WHERE job_id = $root
            UNION
            SELECT jd.depends_on_id FROM job_dependencies jd JOIN ancestors a ON jd.job_id = a.id
          ),
          descendants(id) AS (
            SELECT job_id FROM job_dependencies WHERE depends_on_id = $root
            UNION
            SELECT jd.job_id FROM job_dependencies jd JOIN descendants d ON jd.depends_on_id = d.id
          )
        SELECT * FROM jobs
        WHERE id IN (SELECT id FROM ancestors UNION SELECT $root UNION SELECT id FROM descendants)`)
            .all({ $root: rootId });
        if (relatedRows.length === 0)
            return [];
        const ids = relatedRows.map(r => r.id);
        const namedParams = {};
        for (let i = 0; i < ids.length; i++)
            namedParams[`$id${i}`] = ids[i];
        const ph = ids.map((_, i) => `$id${i}`).join(',');
        const edges = this.db
            .query(`SELECT job_id, depends_on_id FROM job_dependencies
         WHERE job_id IN (${ph}) OR depends_on_id IN (${ph})`)
            .all(namedParams);
        return relatedRows.map(row => ({
            id: row.id,
            type: row.type,
            status: row.status,
            result: row.result ? JSON.parse(row.result) : null,
            dependsOn: edges
                .filter(e => e.job_id === row.id)
                .map(e => e.depends_on_id),
            dependents: edges
                .filter(e => e.depends_on_id === row.id)
                .map(e => e.job_id)
        }));
    }
    mountAdminHandler(prefix = '') {
        return createAdminHandler(this, prefix);
    }
    close() {
        if (this.checkpointIntervalId)
            clearInterval(this.checkpointIntervalId);
        try {
            if (this.db.filename !== ':memory:' && this.db.filename !== '') {
                this.db.run('PRAGMA wal_checkpoint(PASSIVE)');
            }
        }
        catch {
            // ignore checkpoint errors on close
        }
        this.db.close();
    }
    // Split from tryUnblockDependents (below) because job_dependencies has
    // ON DELETE CASCADE on depends_on_id (schema.ts) - deleteJob (used by
    // markJobDead/markJobFailed's dead-letter branch) wipes the dependency
    // edges as a side effect, before there'd be anything left to find. Callers
    // that delete the job row must capture this list *before* deleting, then
    // call tryUnblockDependents with it *after* - see markJobDead/markJobFailed
    // for the shape. Callers that only update status (markJobDone, cancelJob)
    // can call both back-to-back in either order, since the row survives.
    getDependents(jobId) {
        return this.stmts.selectDependents.all({
            $depsOnId: jobId
        });
    }
    tryUnblockDependents(dependents) {
        const now = nowISO();
        for (const dep of dependents) {
            const unmetCount = this.stmts.countUnmetDeps.get({
                $jobId: dep.job_id
            });
            if (unmetCount.count === 0) {
                this.stmts.unblockJob.run({ $id: dep.job_id, $now: now });
            }
        }
    }
    handleBatchJobComplete(batchId) {
        this.stmts.decrementBatchPending.run({ $id: batchId });
        const batch = this.stmts.selectBatch.get({
            $id: batchId
        });
        if (!batch || batch.pending_jobs > 0)
            return;
        // Batch is complete
        const now = nowISO();
        this.stmts.finishBatch.run({ $id: batchId, $now: now });
        const options = batch.options
            ? JSON.parse(batch.options)
            : null;
        if (options) {
            // Enqueue "then" callback job only if zero failures
            if (batch.failed_jobs === 0 && options.thenType) {
                this.stmts.insertJob.run({
                    $type: options.thenType,
                    $data: JSON.stringify(options.thenData ?? {}),
                    $status: 'pending',
                    $priority: 0,
                    $maxRetries: 3,
                    $runAt: now,
                    $batchId: null,
                    $uniqueKey: null,
                    $backoffConfig: null,
                    $expireAt: null,
                    $webhookConfig: null
                });
            }
            // Enqueue "finally" callback job regardless of failures
            if (options.finallyType) {
                this.stmts.insertJob.run({
                    $type: options.finallyType,
                    $data: JSON.stringify(options.finallyData ?? {}),
                    $status: 'pending',
                    $priority: 0,
                    $maxRetries: 3,
                    $runAt: now,
                    $batchId: null,
                    $uniqueKey: null,
                    $backoffConfig: null,
                    $expireAt: null,
                    $webhookConfig: null
                });
            }
        }
        const finishedBatch = this.stmts.selectBatch.get({
            $id: batchId
        });
        if (finishedBatch) {
            const b = toBatch(finishedBatch);
            if (b.failedJobs === 0) {
                this.emit('batch:complete', b);
            }
            else {
                this.emit('batch:failed', b);
            }
        }
    }
}
