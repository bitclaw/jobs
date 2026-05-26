// packages/jobs/src/queue.ts
// JobQueue — SQLite-backed job queue with typed payloads and dependency support
import { Database } from 'bun:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { applyPragmas, initializeSchema } from './schema';
import type {
  AddJobOptions,
  BatchOptions,
  FailedJob,
  FailedJobRow,
  Job,
  JobBatch,
  JobBatchRow,
  JobMap,
  JobRow,
  JobStats,
  ListJobsOptions,
  PaginatedResult,
  PurgeOptions,
  StatsRow,
  WorkerOptions
} from './types';
import { nowISO } from './utils';
import { JobWorker } from './worker';

function toJob<T = unknown>(row: JobRow): Job<T> {
  return {
    id: row.id,
    type: row.type,
    data: JSON.parse(row.data) as T,
    status: row.status as Job['status'],
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

function toBatch(row: JobBatchRow): JobBatch {
  return {
    id: row.id,
    name: row.name,
    totalJobs: row.total_jobs,
    pendingJobs: row.pending_jobs,
    failedJobs: row.failed_jobs,
    failedJobIds: JSON.parse(row.failed_job_ids) as number[],
    options: row.options ? (JSON.parse(row.options) as BatchOptions) : null,
    cancelledAt: row.cancelled_at,
    createdAt: row.created_at,
    finishedAt: row.finished_at
  };
}

function toFailedJob(row: FailedJobRow): FailedJob {
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

export class JobQueue<TMap extends JobMap = Record<string, unknown>> {
  readonly db: Database;

  private readonly insertJobStmt;
  private readonly insertDepStmt;
  private readonly selectJobStmt;
  private readonly selectPendingStmt;
  private readonly markProcessingStmt;
  private readonly markDoneStmt;
  private readonly markFailedStmt;
  private readonly updateProgressStmt;
  private readonly selectStatsStmt;
  private readonly countFailedStmt;
  private readonly insertFailedJobStmt;
  private readonly deleteJobStmt;
  private readonly selectDependentsStmt;
  private readonly countUnmetDepsStmt;
  private readonly unblockJobStmt;
  private readonly lastInsertRowIdStmt;

  // Batch statements
  private readonly insertBatchStmt;
  private readonly selectBatchStmt;
  private readonly decrementBatchPendingStmt;
  private readonly incrementBatchFailedStmt;
  private readonly finishBatchStmt;
  private readonly cancelBatchStmt;
  private readonly cancelBatchJobsStmt;

  constructor(dbPath: string) {
    mkdirSync(dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath, { create: true });
    applyPragmas(this.db);
    initializeSchema(this.db);

    this.insertJobStmt = this.db.query(`
      INSERT INTO jobs (type, data, status, priority, max_retries, run_at, batch_id)
      VALUES ($type, $data, $status, $priority, $maxRetries, $runAt, $batchId)
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
    this.selectStatsStmt = this.db.query(
      'SELECT status, COUNT(*) as count FROM jobs GROUP BY status'
    );
    this.countFailedStmt = this.db.query(
      'SELECT COUNT(*) as count FROM failed_jobs'
    );
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
    this.lastInsertRowIdStmt = this.db.query(
      'SELECT last_insert_rowid() as id'
    );

    // Batch statements
    this.insertBatchStmt = this.db.query(`
      INSERT INTO job_batches (id, name, options, created_at)
      VALUES ($id, $name, $options, $createdAt)
    `);
    this.selectBatchStmt = this.db.query(
      'SELECT * FROM job_batches WHERE id = $id'
    );
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

  add<K extends string & keyof TMap>(
    type: K,
    data: TMap[K],
    options?: AddJobOptions
  ): number {
    return this.insertJob(type, data, null, options);
  }

  getJob(id: number): Job | null {
    const row = this.selectJobStmt.get({ $id: id }) as JobRow | null;
    return row ? toJob(row) : null;
  }

  getStats(): JobStats {
    const rows = this.selectStatsStmt.all() as StatsRow[];
    const deadRow = this.countFailedStmt.get() as { count: number };

    const stats: JobStats = {
      pending: 0,
      blocked: 0,
      processing: 0,
      done: 0,
      failed: 0,
      cancelled: 0,
      dead: deadRow.count
    };

    for (const row of rows) {
      const key = row.status as keyof Omit<JobStats, 'dead'>;
      if (key in stats) {
        stats[key] = row.count;
      }
    }

    return stats;
  }

  getFailedJobs(options?: {
    type?: string;
    limit?: number;
    offset?: number;
  }): PaginatedResult<FailedJob> {
    const limit = options?.limit ?? 100;
    const offset = options?.offset ?? 0;

    let rows: FailedJobRow[];
    let total: number;

    if (options?.type) {
      rows = this.db
        .query(
          'SELECT * FROM failed_jobs WHERE type = $type ORDER BY failed_at DESC LIMIT $limit OFFSET $offset'
        )
        .all({
          $type: options.type,
          $limit: limit,
          $offset: offset
        }) as FailedJobRow[];
      total = (
        this.db
          .query('SELECT COUNT(*) as count FROM failed_jobs WHERE type = $type')
          .get({ $type: options.type }) as { count: number }
      ).count;
    } else {
      rows = this.db
        .query(
          'SELECT * FROM failed_jobs ORDER BY failed_at DESC LIMIT $limit OFFSET $offset'
        )
        .all({ $limit: limit, $offset: offset }) as FailedJobRow[];
      total = (this.countFailedStmt.get() as { count: number }).count;
    }

    return { items: rows.map(toFailedJob), total };
  }

  listJobs(options?: ListJobsOptions): PaginatedResult<Job> {
    const limit = options?.limit ?? 50;
    const offset = options?.offset ?? 0;

    const conditions: string[] = [];
    const filterParams: Record<string, string | number> = {};

    if (options?.status) {
      conditions.push('status = $status');
      filterParams.$status = options.status;
    }
    if (options?.type) {
      conditions.push('type = $type');
      filterParams.$type = options.type;
    }

    const where =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    const rows = this.db
      .query(
        `SELECT * FROM jobs ${where} ORDER BY created_at DESC LIMIT $limit OFFSET $offset`
      )
      .all({ ...filterParams, $limit: limit, $offset: offset }) as JobRow[];

    const total = (
      this.db
        .query(`SELECT COUNT(*) as count FROM jobs ${where}`)
        .get(filterParams) as { count: number }
    ).count;

    return { items: rows.map(toJob), total };
  }

  cancelJob(id: number): boolean {
    const result = this.db
      .query(
        "UPDATE jobs SET status = 'cancelled', updated_at = $now WHERE id = $id AND status IN ('pending', 'blocked')"
      )
      .run({ $id: id, $now: nowISO() });
    return result.changes > 0;
  }

  forceRetryJob(id: number): boolean {
    const result = this.db
      .query(
        "UPDATE jobs SET status = 'pending', retry_count = 0, error = NULL, run_at = $now, started_at = NULL, updated_at = $now WHERE id = $id AND status IN ('processing', 'cancelled')"
      )
      .run({ $id: id, $now: nowISO() });
    return result.changes > 0;
  }

  setJobHttpLog(id: number, requestLog: string, responseLog: string): void {
    this.db
      .query(
        'UPDATE jobs SET request_log = $req, response_log = $res, updated_at = $now WHERE id = $id'
      )
      .run({ $id: id, $req: requestLog, $res: responseLog, $now: nowISO() });
  }

  getJobTypes(): string[] {
    const rows = this.db
      .query('SELECT DISTINCT type FROM jobs ORDER BY type')
      .all() as Array<{ type: string }>;
    return rows.map(r => r.type);
  }

  retryFailedJob(failedJobId: number): number {
    const row = this.db
      .query('SELECT * FROM failed_jobs WHERE id = $id')
      .get({ $id: failedJobId }) as FailedJobRow | null;

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
      $batchId: null
    });

    const newJobId = this.lastInsertRowIdStmt.get() as { id: number };

    this.db
      .query('DELETE FROM failed_jobs WHERE id = $id')
      .run({ $id: failedJobId });

    return newJobId.id;
  }

  purgeFailedJobs(olderThanMs: number): number {
    const cutoff = new Date(Date.now() - olderThanMs).toISOString();
    const result = this.db
      .query('DELETE FROM failed_jobs WHERE failed_at < $cutoff')
      .run({ $cutoff: cutoff });
    return result.changes;
  }

  purge(options: PurgeOptions): number {
    const cutoff = new Date(Date.now() - options.olderThanMs).toISOString();
    const result = this.db
      .query('DELETE FROM jobs WHERE status = $status AND updated_at < $cutoff')
      .run({ $status: options.status, $cutoff: cutoff });
    return result.changes;
  }

  pollAndClaim(type: string): Job | null {
    const now = nowISO();
    const claimTx = this.db.transaction(() => {
      const row = this.selectPendingStmt.get({
        $type: type,
        $now: now
      }) as JobRow | null;
      if (!row) return null;
      this.markProcessingStmt.run({ $id: row.id, $now: now });
      return row;
    });

    const row = claimTx.immediate();
    return row ? toJob(row) : null;
  }

  markJobDone(id: number): void {
    this.db.transaction(() => {
      const now = nowISO();
      const row = this.selectJobStmt.get({ $id: id }) as JobRow | null;
      this.markDoneStmt.run({ $id: id, $now: now });
      this.unblockDependents(id);

      if (row?.batch_id) {
        this.handleBatchJobComplete(row.batch_id);
      }
    })();
  }

  markJobDead(id: number, error: string): void {
    this.db.transaction(() => {
      const row = this.selectJobStmt.get({ $id: id }) as JobRow | null;
      if (!row) return;

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

  markJobFailed(id: number, error: string): void {
    this.db.transaction(() => {
      const now = nowISO();
      const row = this.selectJobStmt.get({ $id: id }) as JobRow | null;
      if (!row) return;

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
      } else {
        this.markFailedStmt.run({ $id: id, $error: error, $now: now });
      }
    })();
  }

  updateProgress(id: number, progress: number): void {
    this.updateProgressStmt.run({
      $id: id,
      $progress: progress,
      $now: nowISO()
    });
  }

  // --- Batch API ---

  createBatch(name: string, options?: BatchOptions): string {
    const id = crypto.randomUUID();
    this.insertBatchStmt.run({
      $id: id,
      $name: name,
      $options: options ? JSON.stringify(options) : null,
      $createdAt: nowISO()
    });
    return id;
  }

  addToBatch<K extends string & keyof TMap>(
    batchId: string,
    type: K,
    data: TMap[K],
    options?: AddJobOptions
  ): number {
    const jobId = this.insertJob(type, data, batchId, options);

    this.db
      .query(
        'UPDATE job_batches SET total_jobs = total_jobs + 1, pending_jobs = pending_jobs + 1 WHERE id = $id'
      )
      .run({ $id: batchId });

    return jobId;
  }

  getBatch(batchId: string): JobBatch | null {
    const row = this.selectBatchStmt.get({
      $id: batchId
    }) as JobBatchRow | null;
    return row ? toBatch(row) : null;
  }

  cancelBatch(batchId: string): void {
    const now = nowISO();
    this.cancelBatchStmt.run({ $id: batchId, $now: now });
    this.cancelBatchJobsStmt.run({ $batchId: batchId, $now: now });
  }

  createWorker<K extends string & keyof TMap>(
    options: WorkerOptions<TMap[K]> & { type: K }
  ): JobWorker<TMap, K> {
    return new JobWorker(this, options);
  }

  private insertJob<K extends string & keyof TMap>(
    type: K,
    data: TMap[K],
    batchId: string | null,
    options?: AddJobOptions
  ): number {
    const now = nowISO();
    const runAt = options?.runAt ? options.runAt.toISOString() : now;
    const hasDeps = options?.dependsOn && options.dependsOn.length > 0;
    const status = hasDeps ? 'blocked' : 'pending';

    this.insertJobStmt.run({
      $type: type,
      $data: JSON.stringify(data),
      $status: status,
      $priority: options?.priority ?? 0,
      $maxRetries: options?.maxRetries ?? 3,
      $runAt: runAt,
      $batchId: batchId
    });

    const jobId = this.lastInsertRowIdStmt.get() as { id: number };

    if (hasDeps) {
      for (const depId of options!.dependsOn!) {
        const dep = this.selectJobStmt.get({ $id: depId }) as JobRow | null;
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

  close(): void {
    try {
      if (this.db.filename !== ':memory:' && this.db.filename !== '') {
        this.db.run('PRAGMA wal_checkpoint(PASSIVE)');
      }
    } catch {
      // ignore checkpoint errors on close
    }
    this.db.close();
  }

  private unblockDependents(completedJobId: number): void {
    const now = nowISO();
    const dependents = this.selectDependentsStmt.all({
      $depsOnId: completedJobId
    }) as Array<{ job_id: number }>;

    for (const dep of dependents) {
      const unmetCount = this.countUnmetDepsStmt.get({
        $jobId: dep.job_id
      }) as { count: number };

      if (unmetCount.count === 0) {
        this.unblockJobStmt.run({ $id: dep.job_id, $now: now });
      }
    }
  }

  private handleBatchJobComplete(batchId: string): void {
    this.decrementBatchPendingStmt.run({ $id: batchId });

    const batch = this.selectBatchStmt.get({
      $id: batchId
    }) as JobBatchRow | null;
    if (!batch || batch.pending_jobs > 0) return;

    // Batch is complete
    const now = nowISO();
    this.finishBatchStmt.run({ $id: batchId, $now: now });

    const options = batch.options
      ? (JSON.parse(batch.options) as BatchOptions)
      : null;

    if (!options) return;

    // Enqueue "then" callback job only if zero failures
    if (batch.failed_jobs === 0 && options.thenType) {
      this.insertJobStmt.run({
        $type: options.thenType,
        $data: JSON.stringify(options.thenData ?? {}),
        $status: 'pending',
        $priority: 0,
        $maxRetries: 3,
        $runAt: now,
        $batchId: null
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
        $batchId: null
      });
    }
  }
}
