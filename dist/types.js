// packages/jobs/src/types.ts
// All type definitions for the SQLite background job queue
/**
 * Throw this from a job handler to skip all retries and move immediately to
 * the dead-letter (failed_jobs) table. Use for permanent configuration errors
 * like missing SSH keys, bad tokens, or invalid input that will never succeed
 * no matter how many times the job runs.
 */
export class NonRetryableError extends Error {
    isNonRetryable = true;
    constructor(message) {
        super(message);
        this.name = 'NonRetryableError';
    }
}
