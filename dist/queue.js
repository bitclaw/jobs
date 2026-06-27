// packages/jobs/src/queue.ts
// JobQueue — SQLite-backed job queue with typed payloads and dependency support
import { Database } from 'bun:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { applyPragmas, initializeSchema } from './schema';
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
        responseLog: row.response_log
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
export class JobQueue {
    db;
    insertJobStmt;
    selectDedupedJobStmt;
    insertDepStmt;
    selectJobStmt;
    selectPendingStmt;
    markProcessingStmt;
    markDoneStmt;
    markFailedStmt;
    updateProgressStmt;
    selectStatsStmt;
    countFailedStmt;
    insertFailedJobStmt;
    deleteJobStmt;
    selectDependentsStmt;
    countUnmetDepsStmt;
    unblockJobStmt;
    lastInsertRowIdStmt;
    // Batch statements
    insertBatchStmt;
    selectBatchStmt;
    decrementBatchPendingStmt;
    incrementBatchFailedStmt;
    finishBatchStmt;
    cancelBatchStmt;
    cancelBatchJobsStmt;
    constructor(dbPath) {
        mkdirSync(dirname(dbPath), { recursive: true });
        this.db = new Database(dbPath, { create: true });
        applyPragmas(this.db);
        initializeSchema(this.db);
        this.insertJobStmt = this.db.query(`
      INSERT OR IGNORE INTO jobs (type, data, status, priority, max_retries, run_at, batch_id, unique_key)
      VALUES ($type, $data, $status, $priority, $maxRetries, $runAt, $batchId, $uniqueKey)
    `);
        this.selectDedupedJobStmt = this.db.query(`
      SELECT id FROM jobs
      WHERE type = $type AND unique_key = $uniqueKey
        AND status IN ('pending', 'processing')
      LIMIT 1
    `);
        this.insertDepStmt = this.db.query(`
      INSERT INTO job_dependencies (job_id, depends_on_id) VALUES ($jobId, $depsOnId)
    `);
        this.selectJobStmt = this.db.query('SELECT * FROM jobs WHERE id = $id');
        this.selectPendingStmt = this.db.query(`
      SELECT * FROM jobs
      WHERE status = 'pending' AND type = $type AND run_at <= $now
      ORDER BY priority DESC, created_at ASC
      LIMIT 1
    `);
        this.markProcessingStmt = this.db.query(`
      UPDATE jobs SET status = 'processing', started_at = $now, updated_at = $now
      WHERE id = $id
    `);
        this.markDoneStmt = this.db.query(`
      UPDATE jobs SET status = 'done', completed_at = $now, updated_at = $now, progress = 100
      WHERE id = $id
    `);
        this.markFailedStmt = this.db.query(`
      UPDATE jobs
      SET status = 'pending',
          retry_count = retry_count + 1,
          error = $error,
          updated_at = $now
      WHERE id = $id
    `);
        this.updateProgressStmt = this.db.query(`
      UPDATE jobs SET progress = $progress, updated_at = $now WHERE id = $id
    `);
        this.selectStatsStmt = this.db.query('SELECT status, COUNT(*) as count FROM jobs GROUP BY status');
        this.countFailedStmt = this.db.query('SELECT COUNT(*) as count FROM failed_jobs');
        this.insertFailedJobStmt = this.db.query(`
      INSERT INTO failed_jobs (original_job_id, type, data, error, retry_count, max_retries, created_at, request_log, response_log)
      VALUES ($originalJobId, $type, $data, $error, $retryCount, $maxRetries, $createdAt, $requestLog, $responseLog)
    `);
        this.deleteJobStmt = this.db.query('DELETE FROM jobs WHERE id = $id');
        this.selectDependentsStmt = this.db.query(`
      SELECT job_id FROM job_dependencies WHERE depends_on_id = $depsOnId
    `);
        this.countUnmetDepsStmt = this.db.query(`
      SELECT COUNT(*) as count FROM job_dependencies jd
      JOIN jobs j ON jd.depends_on_id = j.id
      WHERE jd.job_id = $jobId AND j.status != 'done'
    `);
        this.unblockJobStmt = this.db.query(`
      UPDATE jobs SET status = 'pending', updated_at = $now
      WHERE id = $id AND status = 'blocked'
    `);
        this.lastInsertRowIdStmt = this.db.query('SELECT last_insert_rowid() as id');
        // Batch statements
        this.insertBatchStmt = this.db.query(`
      INSERT INTO job_batches (id, name, options, created_at)
      VALUES ($id, $name, $options, $createdAt)
    `);
        this.selectBatchStmt = this.db.query('SELECT * FROM job_batches WHERE id = $id');
        this.decrementBatchPendingStmt = this.db.query(`
      UPDATE job_batches SET pending_jobs = pending_jobs - 1
      WHERE id = $id
    `);
        this.incrementBatchFailedStmt = this.db.query(`
      UPDATE job_batches
      SET failed_jobs = failed_jobs + 1,
          failed_job_ids = json_insert(failed_job_ids, '$[#]', $jobId)
      WHERE id = $id
    `);
        this.finishBatchStmt = this.db.query(`
      UPDATE job_batches SET finished_at = $now WHERE id = $id
    `);
        this.cancelBatchStmt = this.db.query(`
      UPDATE job_batches SET cancelled_at = $now WHERE id = $id
    `);
        this.cancelBatchJobsStmt = this.db.query(`
      UPDATE jobs SET status = 'cancelled', updated_at = $now
      WHERE batch_id = $batchId AND status IN ('pending', 'blocked')
    `);
    }
    add(type, data, options) {
        return this.insertJob(type, data, null, options);
    }
    getJob(id) {
        const row = this.selectJobStmt.get({ $id: id });
        return row ? toJob(row) : null;
    }
    getStats() {
        const rows = this.selectStatsStmt.all();
        const deadRow = this.countFailedStmt.get();
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
            total = this.countFailedStmt.get().count;
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
        return result.changes > 0;
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
        const row = this.db
            .query('SELECT * FROM failed_jobs WHERE id = $id')
            .get({ $id: failedJobId });
        if (!row) {
            throw new Error(`Failed job ${failedJobId} not found`);
        }
        const now = nowISO();
        this.insertJobStmt.run({
            $type: row.type,
            $data: row.data,
            $status: 'pending',
            $priority: 0,
            $maxRetries: row.max_retries,
            $runAt: now,
            $batchId: null,
            $uniqueKey: null
        });
        const newJobId = this.lastInsertRowIdStmt.get();
        this.db
            .query('DELETE FROM failed_jobs WHERE id = $id')
            .run({ $id: failedJobId });
        return newJobId.id;
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
    pollAndClaim(type) {
        const now = nowISO();
        const claimTx = this.db.transaction(() => {
            const row = this.selectPendingStmt.get({
                $type: type,
                $now: now
            });
            if (!row)
                return null;
            this.markProcessingStmt.run({ $id: row.id, $now: now });
            return row;
        });
        const row = claimTx.immediate();
        return row ? toJob(row) : null;
    }
    markJobDone(id) {
        this.db.transaction(() => {
            const now = nowISO();
            const row = this.selectJobStmt.get({ $id: id });
            this.markDoneStmt.run({ $id: id, $now: now });
            this.unblockDependents(id);
            if (row?.batch_id) {
                this.handleBatchJobComplete(row.batch_id);
            }
        })();
    }
    markJobDead(id, error) {
        this.db.transaction(() => {
            const row = this.selectJobStmt.get({ $id: id });
            if (!row)
                return;
            this.insertFailedJobStmt.run({
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
            this.deleteJobStmt.run({ $id: id });
            if (row.batch_id) {
                this.incrementBatchFailedStmt.run({
                    $id: row.batch_id,
                    $jobId: row.id
                });
                this.handleBatchJobComplete(row.batch_id);
            }
        })();
    }
    markJobFailed(id, error) {
        this.db.transaction(() => {
            const now = nowISO();
            const row = this.selectJobStmt.get({ $id: id });
            if (!row)
                return;
            if (row.retry_count + 1 >= row.max_retries) {
                this.insertFailedJobStmt.run({
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
                this.deleteJobStmt.run({ $id: id });
                // Job is permanently dead — decrement batch counter and track failure
                if (row.batch_id) {
                    this.incrementBatchFailedStmt.run({
                        $id: row.batch_id,
                        $jobId: row.id
                    });
                    this.handleBatchJobComplete(row.batch_id);
                }
            }
            else {
                this.markFailedStmt.run({ $id: id, $error: error, $now: now });
            }
        })();
    }
    updateProgress(id, progress) {
        this.updateProgressStmt.run({
            $id: id,
            $progress: progress,
            $now: nowISO()
        });
    }
    // --- Batch API ---
    createBatch(name, options) {
        const id = crypto.randomUUID();
        this.insertBatchStmt.run({
            $id: id,
            $name: name,
            $options: options ? JSON.stringify(options) : null,
            $createdAt: nowISO()
        });
        return id;
    }
    addToBatch(batchId, type, data, options) {
        const jobId = this.insertJob(type, data, batchId, options);
        this.db
            .query('UPDATE job_batches SET total_jobs = total_jobs + 1, pending_jobs = pending_jobs + 1 WHERE id = $id')
            .run({ $id: batchId });
        return jobId;
    }
    getBatch(batchId) {
        const row = this.selectBatchStmt.get({
            $id: batchId
        });
        return row ? toBatch(row) : null;
    }
    cancelBatch(batchId) {
        const now = nowISO();
        this.cancelBatchStmt.run({ $id: batchId, $now: now });
        this.cancelBatchJobsStmt.run({ $batchId: batchId, $now: now });
    }
    createWorker(options) {
        return new JobWorker(this, options);
    }
    insertJob(type, data, batchId, options) {
        const now = nowISO();
        const runAt = options?.runAt ? options.runAt.toISOString() : now;
        const hasDeps = options?.dependsOn && options.dependsOn.length > 0;
        const status = hasDeps ? 'blocked' : 'pending';
        const result = this.insertJobStmt.run({
            $type: type,
            $data: JSON.stringify(data),
            $status: status,
            $priority: options?.priority ?? 0,
            $maxRetries: options?.maxRetries ?? 3,
            $runAt: runAt,
            $batchId: batchId,
            $uniqueKey: options?.uniqueKey ?? null
        });
        // INSERT OR IGNORE: if a pending/processing job with same (type, uniqueKey)
        // already exists, the insert is a no-op. Return the existing job id.
        if (result.changes === 0 && options?.uniqueKey) {
            const existing = this.selectDedupedJobStmt.get({
                $type: type,
                $uniqueKey: options.uniqueKey
            });
            return existing?.id ?? 0;
        }
        const jobId = this.lastInsertRowIdStmt.get();
        if (hasDeps) {
            for (const depId of options.dependsOn) {
                const dep = this.selectJobStmt.get({ $id: depId });
                if (!dep) {
                    throw new Error(`Dependency job ${depId} does not exist`);
                }
                this.insertDepStmt.run({
                    $jobId: jobId.id,
                    $depsOnId: depId
                });
            }
        }
        return jobId.id;
    }
    close() {
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
    unblockDependents(completedJobId) {
        const now = nowISO();
        const dependents = this.selectDependentsStmt.all({
            $depsOnId: completedJobId
        });
        for (const dep of dependents) {
            const unmetCount = this.countUnmetDepsStmt.get({
                $jobId: dep.job_id
            });
            if (unmetCount.count === 0) {
                this.unblockJobStmt.run({ $id: dep.job_id, $now: now });
            }
        }
    }
    handleBatchJobComplete(batchId) {
        this.decrementBatchPendingStmt.run({ $id: batchId });
        const batch = this.selectBatchStmt.get({
            $id: batchId
        });
        if (!batch || batch.pending_jobs > 0)
            return;
        // Batch is complete
        const now = nowISO();
        this.finishBatchStmt.run({ $id: batchId, $now: now });
        const options = batch.options
            ? JSON.parse(batch.options)
            : null;
        if (!options)
            return;
        // Enqueue "then" callback job only if zero failures
        if (batch.failed_jobs === 0 && options.thenType) {
            this.insertJobStmt.run({
                $type: options.thenType,
                $data: JSON.stringify(options.thenData ?? {}),
                $status: 'pending',
                $priority: 0,
                $maxRetries: 3,
                $runAt: now,
                $batchId: null,
                $uniqueKey: null
            });
        }
        // Enqueue "finally" callback job regardless of failures
        if (options.finallyType) {
            this.insertJobStmt.run({
                $type: options.finallyType,
                $data: JSON.stringify(options.finallyData ?? {}),
                $status: 'pending',
                $priority: 0,
                $maxRetries: 3,
                $runAt: now,
                $batchId: null,
                $uniqueKey: null
            });
        }
    }
}
