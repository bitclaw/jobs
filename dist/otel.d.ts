import type { MiddlewareFn } from './types';
/**
 * Minimal interface matching @opentelemetry/api Tracer.
 * Typed structurally so @bitclaw/jobs stays dependency-free —
 * pass any OTel-compatible tracer without adding it as a peer dep.
 */
export type OtelTracer = {
    startActiveSpan<T>(name: string, fn: (span: OtelSpan) => T): T;
};
export type OtelSpan = {
    setAttribute(key: string, value: string | number | boolean): void;
    setStatus(status: {
        code: number;
        message?: string;
    }): void;
    recordException(error: unknown): void;
    end(): void;
};
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
export declare function createOtelMiddleware(tracer: OtelTracer): MiddlewareFn;
//# sourceMappingURL=otel.d.ts.map