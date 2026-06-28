/** SpanStatusCode.OK = 1, SpanStatusCode.ERROR = 2 (OTel spec) */
const STATUS_OK = 1;
const STATUS_ERROR = 2;
/**
 * Returns a middleware that wraps every job execution in an OTel span.
 *
 * Usage:
 * ```ts
 * import { trace } from '@opentelemetry/api'
 * import { createOtelMiddleware } from '@bitclaw/jobs/otel'
 *
 * const tracer = trace.getTracer('my-app')
 * queue.use(createOtelMiddleware(tracer))
 * ```
 *
 * Each span is named `job.<type>` and carries these attributes:
 * - `job.id`       — numeric job ID
 * - `job.type`     — job type string
 * - `job.priority` — job priority
 * - `job.retry`    — current retry count
 * - `job.error`    — error message (only on failure)
 */
export function createOtelMiddleware(tracer) {
    return (job, next) => tracer.startActiveSpan(`job.${job.type}`, async (span) => {
        span.setAttribute('job.id', job.id);
        span.setAttribute('job.type', job.type);
        span.setAttribute('job.priority', job.priority);
        span.setAttribute('job.retry', job.retryCount);
        try {
            const result = await next();
            span.setStatus({ code: STATUS_OK });
            return result;
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            span.setAttribute('job.error', message);
            span.recordException(err);
            span.setStatus({ code: STATUS_ERROR, message });
            throw err;
        }
        finally {
            span.end();
        }
    });
}
