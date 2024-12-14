import { trace, context, SpanStatusCode } from '@opentelemetry/api'

/**
 * @template {unknown[]} A
 * @template {unknown} T
 * @template {Error} X
 * @template {import('../api.js').Result<T, X>} Result
 * @param {string} spanName
 * @param {(...args: A) => Promise<Result>} fn
 */
export const withResultSpan = (spanName, fn) =>
  /**
   * @param {A} args
  */
  async (...args) => {
    const tracer = trace.getTracer('blob-fetcher')
    const span = tracer.startSpan(spanName)
    const ctx = trace.setSpan(context.active(), span)

    const result = await context.with(ctx, fn, null, ...args)
    if (result.ok) {
      span.setStatus({ code: SpanStatusCode.OK })
    } else {
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: result.error?.message
      })
    }
    span.end()
    return result
  }

/**
 * @template {unknown[]} A
 * @template {*} T
 * @template {*} This
 * @param {string} spanName
 * @param {(this: This, ...args: A) => Promise<T>} fn
 * @param {This} [thisParam]
 */
export const withSimpleSpan = (spanName, fn, thisParam) =>
  /**
   * @param {A} args
  */
  async (...args) => {
    const tracer = trace.getTracer('blob-fetcher')
    const span = tracer.startSpan(spanName)
    const ctx = trace.setSpan(context.active(), span)

    try {
      const result = await context.with(ctx, fn, thisParam, ...args)
      span.setStatus({ code: SpanStatusCode.OK })
      span.end()
      return result
    } catch (err) {
      if (err instanceof Error) {
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: err.message
        })
      } else {
        span.setStatus({
          code: SpanStatusCode.ERROR
        })
      }
      span.end()
      throw err
    }
  }
