import type { Database } from 'bun:sqlite';

export function createStatements(db: Database) {
  return {
    insertJob: db.query(`
      INSERT OR IGNORE INTO jobs (type, data, status, priority, max_retries, run_at, batch_id, unique_key, backoff_config, expire_at, webhook_config)
      VALUES ($type, $data, $status, $priority, $maxRetries, $runAt, $batchId, $uniqueKey, $backoffConfig, $expireAt, $webhookConfig)
    `),
    selectDedupedJob: db.query(`
      SELECT id FROM jobs
      WHERE type = $type AND unique_key = $uniqueKey
        AND status IN ('pending', 'processing')
      LIMIT 1
    `),
    insertDep: db.query(`
      INSERT INTO job_dependencies (job_id, depends_on_id) VALUES ($jobId, $depsOnId)
    `),
    selectJob: db.query('SELECT * FROM jobs WHERE id = $id'),
    selectPending: db.query(`
      SELECT * FROM jobs
      WHERE type = $type AND run_at <= $now
        AND (expire_at IS NULL OR expire_at > $now)
        AND (
          (status = 'pending')
          OR (status = 'processing' AND claimed_until IS NOT NULL AND claimed_until < $now)
        )
      ORDER BY priority DESC, created_at ASC
      LIMIT 1
    `),
    markProcessing: db.query(`
      UPDATE jobs SET status = 'processing', started_at = $now, updated_at = $now, claimed_until = $claimedUntil
      WHERE id = $id
    `),
    markDone: db.query(`
      UPDATE jobs SET status = 'done', completed_at = $now, updated_at = $now, progress = 100, result = $result
      WHERE id = $id
    `),
    markFailed: db.query(`
      UPDATE jobs
      SET status = 'pending',
          retry_count = retry_count + 1,
          error = $error,
          run_at = $runAt,
          updated_at = $now
      WHERE id = $id
    `),
    updateProgress: db.query(`
      UPDATE jobs SET progress = $progress, updated_at = $now WHERE id = $id
    `),
    selectStats: db.query(
      'SELECT status, COUNT(*) as count FROM jobs GROUP BY status'
    ),
    countFailed: db.query('SELECT COUNT(*) as count FROM failed_jobs'),
    insertFailedJob: db.query(`
      INSERT INTO failed_jobs (original_job_id, type, data, error, retry_count, max_retries, created_at, request_log, response_log)
      VALUES ($originalJobId, $type, $data, $error, $retryCount, $maxRetries, $createdAt, $requestLog, $responseLog)
    `),
    deleteJob: db.query('DELETE FROM jobs WHERE id = $id'),
    selectDependents: db.query(`
      SELECT job_id FROM job_dependencies WHERE depends_on_id = $depsOnId
    `),
    countUnmetDeps: db.query(`
      SELECT COUNT(*) as count FROM job_dependencies jd
      JOIN jobs j ON jd.depends_on_id = j.id
      WHERE jd.job_id = $jobId AND j.status != 'done'
    `),
    unblockJob: db.query(`
      UPDATE jobs SET status = 'pending', updated_at = $now
      WHERE id = $id AND status = 'blocked'
    `),
    lastInsertRowId: db.query('SELECT last_insert_rowid() as id'),
    renewLease: db.query(`
      UPDATE jobs SET claimed_until = $claimedUntil, updated_at = $now
      WHERE id = $id AND status = 'processing'
    `),
    // Batch
    insertBatch: db.query(`
      INSERT INTO job_batches (id, name, options, created_at)
      VALUES ($id, $name, $options, $createdAt)
    `),
    selectBatch: db.query('SELECT * FROM job_batches WHERE id = $id'),
    decrementBatchPending: db.query(`
      UPDATE job_batches SET pending_jobs = pending_jobs - 1
      WHERE id = $id
    `),
    incrementBatchFailed: db.query(`
      UPDATE job_batches
      SET failed_jobs = failed_jobs + 1,
          failed_job_ids = json_insert(failed_job_ids, '$[#]', $jobId)
      WHERE id = $id
    `),
    finishBatch: db.query(`
      UPDATE job_batches SET finished_at = $now WHERE id = $id
    `),
    cancelBatch: db.query(`
      UPDATE job_batches SET cancelled_at = $now WHERE id = $id
    `),
    cancelBatchJobs: db.query(`
      UPDATE jobs SET status = 'cancelled', updated_at = $now
      WHERE batch_id = $batchId AND status IN ('pending', 'blocked')
    `)
  };
}

export type JobQueueStatements = ReturnType<typeof createStatements>;
