# @bitclaw/jobs

SQLite-backed background job queue for Bun. Typed generics, multi-process lease safety, priority aging, middleware pipeline, workflow engine with saga compensation, and zero runtime dependencies.

## Installation

```bash
bun add @bitclaw/jobs
```

Requires Bun ≥ 1.3.0. Uses `bun:sqlite` , no native build step, no extra packages.

## Feature Overview

| Feature | Details |
|---|---|
| **Typed payloads** | `JobQueue<TMap>` , per-type payload inference, no `any` |
| **Priority + aging** | Jobs ordered by priority DESC; workers can boost old jobs per minute |
| **Retries + backoff** | `exponential`, `fixed`, `jitter`, `fibonacci`; `retryIf` predicate to skip retries |
| **Dead-letter** | Exhausted jobs moved to `failed_jobs`, retryable via `retryFailedJob` |
| **Dependencies** | Blocked jobs auto-unblock when all deps complete |
| **Multi-process safety** | Lease column (`claimed_until`) prevents double-claim across processes |
| **Lease renewal** | Long-running handlers call `ctx.renewLease()` to extend their claim |
| **Batches** | Group jobs, track progress, fire `then`/`finally` callbacks on completion |
| **Cron scheduler** | 5-field cron parser, persistent `schedules` table, overlap control |
| **Rate limiter** | Per-worker sliding-window throttle (`maxRate`) |
| **Middleware** | Onion-style `queue.use(fn)` wraps all executions (logging, OTel, timing) |
| **Job graph API** | `getJobGraph(id)` traverses dependency DAG via recursive CTE |
| **Dedup** | `uniqueKey` + `dedup: 'ignore' | 'replace'` , state-aware, key reusable after completion |
| **TTL** | `expireAt` , expired jobs silently skipped and purgeable |
| **Result storage** | Handler return value persisted; `getJobResult<T>(id)` to read it |
| **Webhook on completion** | `onComplete: { url }` fires a detached POST after job finishes |
| **Typed events** | `queue.on('job:done' | 'job:failed' | 'job:dead' | ...)` |
| **Pause / resume** | `worker.pause()` / `worker.resume()` , in-flight job finishes, no new claims |
| **Admin handler** | `queue.mountAdminHandler()` returns a zero-dep `Request → Response` handler |
| **Workflow engine** | `WorkflowEngine` , typed DAG of steps, saga compensation, restart-safe `reconcile()` |
| **OpenTelemetry** | `createOtelMiddleware(tracer)` , zero-dep, structural tracer interface |

---

## Quick Start

```typescript
import { JobQueue } from '@bitclaw/jobs'

type AppJobs = {
  'email:send': { to: string; subject: string }
  'report:generate': { type: string }
}

const queue = new JobQueue<AppJobs>('./jobs.db')

// Enqueue
queue.add('email:send', { to: 'user@example.com', subject: 'Welcome' })

// Worker
const worker = queue.createWorker({
  type: 'email:send',
  handler: async (job, ctx) => {
    ctx.reportProgress(50)
    await sendEmail(job.data)
    return { sent: true }          // stored in job.result
  },
  pollIntervalMs: 1000,
  maxRate: { count: 20, windowMs: 1000 }
})

worker.start()
```

---

## Worker Configuration

All options for `queue.createWorker(options)`:

| Option | Type | Default | Description |
|---|---|---|---|
| `type` | `keyof TMap` | required | Job type to process |
| `handler` | `(job, ctx) => Promise<any>` | required | Job handler |
| `concurrency` | `number` | `1` | Max parallel jobs per poll tick |
| `pollIntervalMs` | `number` | `1000` | Poll interval in ms |
| `leaseMs` | `number` | `300000` | Job lease duration in ms |
| `timeoutMs` | `number` | , | Per-job timeout; throws if exceeded |
| `maxRate` | `{ count, windowMs }` | , | Sliding-window rate limit |
| `aging` | `{ boostPerMinute, maxBoost }` | , | Priority aging config |
| `retryIf` | `(err, job) => boolean` | , | Return `false` to skip retry |
| `onError` | `(job, err) => void` | , | Called on every failure (retries + dead) |

---

## Retries and Backoff

```typescript
queue.add('email:send', data, {
  maxRetries: 5,
  backoff: { type: 'exponential', delayMs: 1000 }
  // types: 'exponential' | 'fixed' | 'jitter' | 'fibonacci'
})

// Skip retries for permanent errors
queue.createWorker({
  type: 'email:send',
  handler: async job => { /* ... */ },
  retryIf: (err, job) => !(err instanceof ValidationError)
})

// Or throw NonRetryableError inside the handler
import { NonRetryableError } from '@bitclaw/jobs'

queue.createWorker({
  type: 'email:send',
  handler: async job => {
    if (!job.data.to) throw new NonRetryableError('Missing recipient')
    await sendEmail(job.data)
  }
})
// NonRetryableError bypasses maxRetries and goes directly to failed_jobs
```

---

## Dependencies

```typescript
const depA = queue.add('data:fetch', { source: 'api' })
const depB = queue.add('data:fetch', { source: 'db' })

// Blocked until both depA and depB complete
queue.add('report:generate', { type: 'combined' }, {
  dependsOn: [depA, depB]
})
```

---

## Multi-Process Lease Safety

Two workers against the same DB file cannot claim the same job. `pollAndClaim` atomically sets `claimed_until`. If a worker crashes mid-job, any other worker reclaims after lease expiry.

```typescript
// Default lease: 5 minutes. Override per worker:
queue.createWorker({
  type: 'server:provision',
  handler: async (job, ctx) => {
    await longStep()
    ctx.renewLease()   // extend before expiry
    await anotherStep()
  },
  leaseMs: 600_000    // 10 min
})
```

---

## Startup Recovery

Call at startup to reclaim jobs that were mid-processing when the previous instance crashed:

```typescript
queue.reconcileStaleJobs()          // default: resets jobs claimed > 5 min ago
queue.reconcileStaleJobs(60_000)    // custom threshold in ms
```

Pairs with `engine.reconcile()` for workflow executions , both should run on boot.

---

## Middleware

```typescript
// Logging
queue.use(async (job, next) => {
  console.info(`[job] ${job.type} #${job.id} start`)
  const result = await next()
  console.info(`[job] ${job.type} #${job.id} done`)
  return result
})

// Multiple middlewares run in registration order (onion)
queue.use(timingMiddleware)
queue.use(loggingMiddleware)
// execution: timing → logging → handler → logging → timing
```

---

## OpenTelemetry

No peer dependency. Pass any OTel-compatible tracer , the interface is structural.

```typescript
import { trace } from '@opentelemetry/api'
import { createOtelMiddleware } from '@bitclaw/jobs/otel'

const tracer = trace.getTracer('my-app', '1.0.0')
queue.use(createOtelMiddleware(tracer))
```

Each job execution becomes a span named `job.<type>` with attributes:

| Attribute | Value |
|---|---|
| `job.id` | numeric job ID |
| `job.type` | type string |
| `job.priority` | priority |
| `job.retry` | retry count at execution time |
| `job.error` | error message (failure only) |

---

## Priority Aging

Prevents starvation of low-priority jobs under sustained high-priority load.

```typescript
queue.createWorker({
  type: 'email:send',
  handler: async job => { /* ... */ },
  aging: {
    boostPerMinute: 10,  // +10 priority per minute of wait
    maxBoost: 100        // cap
  }
})
```

---

## Pause / Resume

```typescript
worker.pause()      // stops claiming new jobs; in-flight job finishes
worker.resume()     // resumes polling
worker.isPaused     // boolean
```

Useful for graceful shutdown, maintenance windows, or circuit-breaker patterns.

---

## Job Graph API

```typescript
const nodes = queue.getJobGraph(rootJobId)
// nodes: Array<{ id, type, status, result, dependsOn: number[], dependents: number[] }>
```

Traverses the full dependency DAG (ancestors + descendants) via SQLite recursive CTE.

---

## Deduplication

```typescript
// Ignore: silently re-uses existing pending job
queue.add('report:generate', data, { uniqueKey: 'daily-2026-06-27', dedup: 'ignore' })

// Replace: updates data on existing pending job
queue.add('report:generate', freshData, { uniqueKey: 'daily-2026-06-27', dedup: 'replace' })

// Cancel pending job by key
queue.cancelByUniqueKey('report:generate', 'daily-2026-06-27')
```

---

## Result Storage

```typescript
const id = queue.add('data:process', payload)

// In handler , return value is stored automatically
queue.createWorker({
  type: 'data:process',
  handler: async job => {
    return { count: 42, processed: true }
  }
})

// Read later
const result = queue.getJobResult<{ count: number; processed: boolean }>(id)
```

---

## Webhook on Completion

Fire a POST when a job finishes:

```typescript
queue.add('report:generate', data, {
  onComplete: {
    url: 'https://example.com/hooks/job-done',
    headers: { 'Authorization': 'Bearer token' }   // optional
  }
})
```

Fire-and-forget (detached). Payload: `{ job: <job object>, result: <handler return value> }`.

---

## Typed Events

```typescript
queue.on('job:done',     job => console.info('done', job.id))
queue.on('job:failed',   (job, err) => console.warn('retry', err))
queue.on('job:dead',     (job, err) => alert.send(err))
queue.on('job:progress', (job, pct) => ws.send({ id: job.id, pct }))
queue.on('batch:complete', batch => notify(batch.id))

const unsub = queue.on('job:done', cb)
unsub()   // remove listener
```

---

## Workflow Engine

Typed DAG of steps where each step is a real job. Dependencies handled by the existing `job_dependencies` mechanism , no separate scheduler. Saga compensation runs in reverse topological order if any step fails permanently.

```typescript
import { WorkflowEngine } from '@bitclaw/jobs'

const engine = new WorkflowEngine(queue)

const { instanceId, jobIds } = engine
  .workflow('order-flow')
  .step('charge',    'payment:charge',   { amount: 100 })
  .step('provision', 'server:provision', { serverId: 'srv-1' }, { dependsOn: ['charge'] })
  .step('notify',    'email:welcome',    { userId: 'u1' },      { dependsOn: ['provision'] })
  .onFail('charge', {
    compensate: 'payment:refund',
    compensateData: { amount: 100, reason: 'provision-failed' }
  })
  .run()

// Reconcile on startup , resumes any interrupted workflows
engine.reconcile()

// Read a step's result from within a dependent handler
const chargeResult = engine.getStepResult<ChargeResult>(instanceId, 'charge')

// Query executions
engine.listExecutions({ status: 'running', name: 'order-flow' })
engine.getExecution(instanceId)
```

**Saga semantics:** `onFail(stepName, { compensate, compensateData })` registers a compensation job for a step that completed successfully. If a later step fails permanently, `reconcile()` enqueues compensation jobs (in reverse topological order) for all completed steps that registered one, then transitions the execution to `compensating`. When all compensation jobs finish, the execution is marked `failed`.

**Restart safety:** Call `engine.reconcile()` on boot. It finds all `running`/`compensating` executions and advances their state without re-running completed steps.

**Compensation failure:** If a compensation job exhausts its retry attempts, it moves to the `failed_jobs` table. The execution transitions to `'failed'` , it is never silently marked complete. To observe: query `job_executions WHERE status = 'failed'` to find affected executions, then `failed_jobs WHERE original_job_id = ?` with each step's `compensate_job_id` to inspect the dead-lettered compensation job.

---

## Cron Scheduler

```typescript
import { Scheduler } from '@bitclaw/jobs'

const scheduler = new Scheduler(queue)

scheduler.register('daily-report', 'report:generate', '0 2 * * *', {
  data: { type: 'daily' },
  timezone: 'America/New_York',
  overlap: false   // skip if previous run still processing
})

scheduler.start()   // ticks every 60s by default
await scheduler.stop()
```

---

## Admin Handler

Zero-dependency `Request → Response` handler. Mount in any framework.

```typescript
// TanStack Start / Hono / bare Bun.serve
const adminHandler = queue.mountAdminHandler('/admin/jobs')

// Routes:
// GET  /admin/jobs/stats
// GET  /admin/jobs/jobs
// GET  /admin/jobs/jobs/:id
// GET  /admin/jobs/jobs/:id/graph
// POST /admin/jobs/jobs/:id/cancel
// POST /admin/jobs/jobs/:id/force-retry
// GET  /admin/jobs/failed
// POST /admin/jobs/failed/:id/retry
// POST /admin/jobs/failed/retry-by-type
// GET  /admin/jobs/jobs/types
```

---

## Batch Processing

```typescript
const { batchId, jobIds } = queue.addBatch('nightly-sync', [
  { type: 'user:sync', data: { userId: 'u1' } },
  { type: 'user:sync', data: { userId: 'u2' } }
], {
  thenType: 'report:generate',
  thenData: { trigger: 'batch-done' }
})

const batch = queue.getBatch(batchId)
// { totalJobs: 2, pendingJobs: 1, failedJobs: 0, ... }
```

---

## Subpath Exports

```typescript
import { JobQueue, WorkflowEngine }  from '@bitclaw/jobs'
import { createOtelMiddleware }       from '@bitclaw/jobs/otel'
import { WorkflowBuilder }           from '@bitclaw/jobs/workflow'
import { Scheduler }                 from '@bitclaw/jobs/scheduler'
import { parseCron }                 from '@bitclaw/jobs/cron'
import { initializeSchema }          from '@bitclaw/jobs/schema'
import { SlidingWindowRateLimiter }  from '@bitclaw/jobs/rate-limiter'
import { JobWorker }                 from '@bitclaw/jobs/worker'
```

---

## Competitor Comparison

Analysis against every actively-maintained SQLite job queue as of 2026-06.

| Feature | **@bitclaw/jobs** | bunqueue | plainjob | workmatic | liteq (Go) | apalis-sqlite (Rust) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Typed generics | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| Priority ordering | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Priority aging (starvation prevention) | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Exponential / jitter / fibonacci backoff | ✅ | ✅ (exp only) | ✅ (exp only) | ❌ | ✅ (fixed) | ✅ (exp only) |
| `retryIf` predicate | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Dead-letter table | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ |
| Job dependencies (DAG) | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Job graph API (recursive CTE) | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Multi-process lease safety | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| Lease renewal (`ctx.renewLease`) | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Dedup (ignore + replace) | ✅ | ✅ (ignore) | ❌ | ✅ (ignore) | ❌ | ❌ |
| Job TTL / `expireAt` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Result storage | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |
| Typed events | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Middleware pipeline | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| OpenTelemetry helper | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Pause / resume per worker | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Webhook on completion | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Batch processing | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ |
| Cron scheduler | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| Admin HTTP handler | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |
| Workflow engine | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Saga compensation | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Restart-safe reconcile | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Concurrency per worker | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Rate limiting per worker | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Zero runtime dependencies | ✅ | ✅ | ✅ | ❌ | n/a | n/a |

**Key differentiators:**

- **Only queue with a workflow engine.** `WorkflowEngine` composes real jobs into typed DAGs with saga compensation and restart-safe reconciliation. Competitors require external orchestrators (Temporal, Inngest) for this.
- **Only queue with middleware.** `queue.use(fn)` enables logging, tracing, and auth-checking without modifying job handlers. Every other queue buries these concerns inside worker configuration.
- **Only queue with lease renewal.** Long-running jobs (provisioning, ML inference) can extend their claim without crashing. Competitors force you to size `leaseMs` conservatively.
- **Only queue with priority aging.** High-throughput workloads can starve low-priority jobs indefinitely. Aging prevents it.
- **Only queue with `retryIf`.** Permanent errors (bad config, invalid input) should not consume retry budget. All others retry blindly.
- **Job graph API.** `getJobGraph(id)` returns the full dependency graph via recursive CTE , useful for admin UIs and debugging complex pipelines. No competitor exposes this.
- **OTel helper.** `createOtelMiddleware(tracer)` instruments all job executions with zero library coupling. No peer dependency , the tracer interface is structural.

---

## Testing

```bash
bun test
```

208 tests across 13 files.
