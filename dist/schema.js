const JOBS_TABLE = `
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,
  data TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'pending',
  priority INTEGER NOT NULL DEFAULT 0,
  progress INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 3,
  retry_count INTEGER NOT NULL DEFAULT 0,
  run_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  started_at TEXT,
  completed_at TEXT,
  error TEXT,
  request_log TEXT,
  response_log TEXT,
  batch_id TEXT REFERENCES job_batches(id),
  unique_key TEXT,
  backoff_config TEXT,
  claimed_until TEXT,
  result TEXT,
  expire_at TEXT,
  webhook_config TEXT
)`;
// State-aware dedup: same (type, unique_key) cannot be both pending/processing at once.
// Once the job completes, the same key can be re-enqueued.
const JOBS_UNIQUE_KEY_INDEX = `
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_unique_key
  ON jobs (type, unique_key)
  WHERE unique_key IS NOT NULL AND status IN ('pending', 'processing')`;
const JOBS_POLL_INDEX = `
CREATE INDEX IF NOT EXISTS idx_jobs_poll
  ON jobs (status, type, run_at, priority DESC, created_at ASC)`;
const JOBS_STATUS_INDEX = `
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status)`;
const JOB_DEPENDENCIES_TABLE = `
CREATE TABLE IF NOT EXISTS job_dependencies (
  job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  depends_on_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  PRIMARY KEY (job_id, depends_on_id)
)`;
const JOB_DEPS_INDEX = `
CREATE INDEX IF NOT EXISTS idx_job_deps_depends_on
  ON job_dependencies (depends_on_id)`;
const FAILED_JOBS_TABLE = `
CREATE TABLE IF NOT EXISTS failed_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  original_job_id INTEGER NOT NULL,
  type TEXT NOT NULL,
  data TEXT NOT NULL,
  error TEXT,
  retry_count INTEGER NOT NULL,
  max_retries INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  failed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  request_log TEXT,
  response_log TEXT
)`;
const JOB_BATCHES_TABLE = `
CREATE TABLE IF NOT EXISTS job_batches (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  total_jobs INTEGER NOT NULL DEFAULT 0,
  pending_jobs INTEGER NOT NULL DEFAULT 0,
  failed_jobs INTEGER NOT NULL DEFAULT 0,
  failed_job_ids TEXT NOT NULL DEFAULT '[]',
  options TEXT,
  cancelled_at TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  finished_at TEXT
)`;
const SCHEDULES_TABLE = `
CREATE TABLE IF NOT EXISTS schedules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL,
  data TEXT NOT NULL DEFAULT '{}',
  cron TEXT NOT NULL,
  timezone TEXT NOT NULL DEFAULT 'UTC',
  enabled INTEGER NOT NULL DEFAULT 1,
  overlap INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 3,
  last_run_at TEXT,
  next_run_at TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
)`;
const SCHEDULES_NEXT_RUN_INDEX = `
CREATE INDEX IF NOT EXISTS idx_schedules_next_run
  ON schedules (enabled, next_run_at)`;
export function applyPragmas(db) {
    if (db.filename !== ':memory:' && db.filename !== '') {
        db.run('PRAGMA journal_mode = WAL');
        db.run('PRAGMA busy_timeout = 5000');
    }
    db.run('PRAGMA foreign_keys = ON');
    db.run('PRAGMA synchronous = NORMAL');
    db.run('PRAGMA cache_size = -4000');
    db.run('PRAGMA temp_store = MEMORY');
    db.run('PRAGMA mmap_size = 16777216'); // 16MB — jobs rows are small, no blobs
    db.run('PRAGMA wal_autocheckpoint = 0'); // Litestream controls checkpointing
}
export function initializeSchema(db) {
    db.run(JOB_BATCHES_TABLE);
    db.run(JOBS_TABLE);
    db.run(JOBS_POLL_INDEX);
    db.run(JOBS_STATUS_INDEX);
    db.run(JOB_DEPENDENCIES_TABLE);
    db.run(JOB_DEPS_INDEX);
    db.run(FAILED_JOBS_TABLE);
    db.run(SCHEDULES_TABLE);
    db.run(SCHEDULES_NEXT_RUN_INDEX);
    // Migrations for columns added after initial schema creation
    const cols = db.prepare('PRAGMA table_info(jobs)').all();
    if (!cols.some(c => c.name === 'unique_key')) {
        db.run('ALTER TABLE jobs ADD COLUMN unique_key TEXT');
    }
    if (!cols.some(c => c.name === 'backoff_config')) {
        db.run('ALTER TABLE jobs ADD COLUMN backoff_config TEXT');
    }
    if (!cols.some(c => c.name === 'claimed_until')) {
        db.run('ALTER TABLE jobs ADD COLUMN claimed_until TEXT');
    }
    if (!cols.some(c => c.name === 'result')) {
        db.run('ALTER TABLE jobs ADD COLUMN result TEXT');
    }
    if (!cols.some(c => c.name === 'expire_at')) {
        db.run('ALTER TABLE jobs ADD COLUMN expire_at TEXT');
    }
    if (!cols.some(c => c.name === 'webhook_config')) {
        db.run('ALTER TABLE jobs ADD COLUMN webhook_config TEXT');
    }
    db.run(JOBS_UNIQUE_KEY_INDEX);
}
