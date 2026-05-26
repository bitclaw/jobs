# @bitclaw/jobs

SQLite-backed background job queue for Bun. Features priority ordering, retries with backoff, cron scheduling, job dependencies, batch processing, rate limiting, and a dead-letter table.

## Features

- **Typed** Generic `JobQueue<TMap>` with per-type payload validation
- **Priority** Jobs ordered by priority DESC then created_at ASC
- **Retries** Configurable `maxRetries` with automatic dead-letter after exhaustion
- **Dependencies** Blocked jobs auto-unblock when all dependencies complete
- **Batches** Group jobs, track progress, fire `then`/`finally` callbacks on completion
- **Cron** 5-field cron parser with `nextCronOccurrence` and overlap control
- **Scheduler** Persistent `schedules` table with upsert semantics and cleanup
- **Rate Limiter** In-memory sliding window per-worker throttling
- **Worker** `setTimeout`-based poll loop with graceful shutdown and per-job timeout

## Installation

```bash
bun add @bitclaw/jobs
```

## Quick Start

```typescript
import { JobQueue } from '@bitclaw/jobs'

type AppJobs = {
  'email:send': { to: string; subject: string }
}

const queue = new JobQueue<AppJobs>('./jobs.db')
queue.add('email:send', { to: 'user@test.com', subject: 'Hello' })
```

## Worker

```typescript
const worker = queue.createWorker({
  type: 'email:send',
  handler: async (job, ctx) => {
    ctx.reportProgress(50)
    await sendEmail(job.data.to, job.data.subject)
  },
  pollIntervalMs: 1000,
  maxRate: { count: 10, windowMs: 1000 }
})

worker.start()
// ... later
await worker.stop()
```

## Cron Scheduler

```typescript
import { Scheduler } from '@bitclaw/jobs'

const scheduler = new Scheduler(queue)
scheduler.register('daily-report', 'report:generate', '0 2 * * *', {
  data: { type: 'daily' }
})
scheduler.start() // ticks every 60s by default
```

## Subpath Exports

```typescript
import { JobQueue }       from '@bitclaw/jobs'
import { JobWorker }      from '@bitclaw/jobs/worker'
import { JobQueue }       from '@bitclaw/jobs/queue'
import { Scheduler }      from '@bitclaw/jobs/scheduler'
import { parseCron }      from '@bitclaw/jobs/cron'
import { initializeSchema } from '@bitclaw/jobs/schema'
import { SlidingWindowRateLimiter } from '@bitclaw/jobs/rate-limiter'
```

## Testing

```bash
bun test
```

118 tests across 7 files.
