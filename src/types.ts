// packages/jobs/src/types.ts
// All type definitions for the SQLite background job queue

/**
 * Throw this from a job handler to skip all retries and move immediately to
 * the dead-letter (failed_jobs) table. Use for permanent configuration errors
 * like missing SSH keys, bad tokens, or invalid input that will never succeed
 * no matter how many times the job runs.
 */
export class NonRetryableError extends Error {
  readonly isNonRetryable = true;

  constructor(message: string) {
    super(message);
    this.name = 'NonRetryableError';
  }
}

export type JobStatus =
  | 'pending'
  | 'processing'
  | 'done'
  | 'failed'
  | 'blocked'
  | 'cancelled';

export type Job<T = unknown> = {
  readonly id: number;
  readonly type: string;
  readonly data: T;
  readonly status: JobStatus;
  readonly priority: number;
  readonly progress: number;
  readonly maxRetries: number;
  readonly retryCount: number;
  readonly runAt: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly startedAt: string | null;
  readonly completedAt: string | null;
  readonly error: string | null;
  readonly batchId: string | null;
  readonly requestLog: string | null;
  readonly responseLog: string | null;
};

export type FailedJob = {
  readonly id: number;
  readonly originalJobId: number;
  readonly type: string;
  readonly data: unknown;
  readonly error: string | null;
  readonly retryCount: number;
  readonly maxRetries: number;
  readonly createdAt: string;
  readonly failedAt: string;
  readonly requestLog: string | null;
  readonly responseLog: string | null;
};

export type AddJobOptions = {
  priority?: number;
  runAt?: Date;
  maxRetries?: number;
  dependsOn?: number[];
};

export type JobContext = {
  reportProgress: (percent: number) => void;
  signal: AbortSignal;
};

export type RateLimit = {
  count: number;
  windowMs: number;
};

export type WorkerOptions<T = unknown> = {
  type: string;
  handler: (job: Job<T>, ctx: JobContext) => Promise<void>;
  pollIntervalMs?: number;
  maxRate?: RateLimit;
  onError?: (job: Job<T>, error: unknown) => void;
  /** Hard wall-clock limit per job execution in ms. Job is marked failed on timeout. */
  timeoutMs?: number;
};

export type JobStats = {
  pending: number;
  blocked: number;
  processing: number;
  done: number;
  failed: number;
  cancelled: number;
  dead: number;
};

export type PurgeOptions = {
  status: 'done' | 'failed';
  olderThanMs: number;
};

export type ListJobsOptions = {
  status?: JobStatus;
  type?: string;
  limit?: number;
  offset?: number;
};

export type PaginatedResult<T> = {
  items: T[];
  total: number;
};

export type JobMap = Record<string, unknown>;

// Raw row shape from SQLite (snake_case)
export type JobRow = {
  id: number;
  type: string;
  data: string;
  status: string;
  priority: number;
  progress: number;
  max_retries: number;
  retry_count: number;
  run_at: string;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  batch_id: string | null;
  request_log: string | null;
  response_log: string | null;
};

export type FailedJobRow = {
  id: number;
  original_job_id: number;
  type: string;
  data: string;
  error: string | null;
  retry_count: number;
  max_retries: number;
  created_at: string;
  failed_at: string;
  request_log: string | null;
  response_log: string | null;
};

export type StatsRow = {
  status: string;
  count: number;
};

// --- Batch types ---

export type BatchOptions = {
  thenType?: string;
  thenData?: unknown;
  finallyType?: string;
  finallyData?: unknown;
};

export type JobBatch = {
  readonly id: string;
  readonly name: string;
  readonly totalJobs: number;
  readonly pendingJobs: number;
  readonly failedJobs: number;
  readonly failedJobIds: number[];
  readonly options: BatchOptions | null;
  readonly cancelledAt: string | null;
  readonly createdAt: string;
  readonly finishedAt: string | null;
};

export type JobBatchRow = {
  id: string;
  name: string;
  total_jobs: number;
  pending_jobs: number;
  failed_jobs: number;
  failed_job_ids: string;
  options: string | null;
  cancelled_at: string | null;
  created_at: string;
  finished_at: string | null;
};

// --- Schedule types ---

export type Schedule = {
  readonly id: number;
  readonly name: string;
  readonly type: string;
  readonly data: unknown;
  readonly cron: string;
  readonly timezone: string;
  readonly enabled: boolean;
  readonly overlap: boolean;
  readonly maxRetries: number;
  readonly lastRunAt: string | null;
  readonly nextRunAt: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
};

export type ScheduleRow = {
  id: number;
  name: string;
  type: string;
  data: string;
  cron: string;
  timezone: string;
  enabled: number;
  overlap: number;
  max_retries: number;
  last_run_at: string | null;
  next_run_at: string | null;
  created_at: string;
  updated_at: string;
};

export type AddScheduleOptions = {
  data?: unknown;
  timezone?: string;
  overlap?: boolean;
  maxRetries?: number;
};
