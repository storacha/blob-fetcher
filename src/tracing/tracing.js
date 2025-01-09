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
 * @template {unknown} O
 * @template {Error} X
 * @template {unknown} T
 * @template {import('../api.js').Result<O, X>} Result
 * @param {import('@opentelemetry/api').Span} span
 * @param {AsyncGenerator<T, Result>} gen
 */
async function * recordAsyncGeneratorSpan (span, gen) {
  try {
    for (;;) {
      const { value: result, done } = await gen.next()
      if (done) {
        if (result.ok) {
          span.setStatus({ code: SpanStatusCode.OK })
        } else {
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: result.error?.message
          })
        }
        return result
      }
      yield (result)
    }
  } finally {
    span.end()
  }
}

/**
 * @template {unknown[]} A
 * @template {unknown} O
 * @template {Error} X
 * @template {unknown} T
 * @template {import('../api.js').Result<O, X>} Result
 * @param {string} spanName
 * @param {(...args: A) => AsyncGenerator<T, Result>} fn
 */
export function withAsyncGeneratorSpan (spanName, fn) {
  /**
   * @param {A} args
  */
  return function (...args) {
    const tracer = trace.getTracer('blob-fetcher')
    const span = tracer.startSpan(spanName)
    const ctx = trace.setSpan(context.active(), span)
    const gen = context.with(ctx, fn, null, ...args)
    return recordAsyncGeneratorSpan(span, bindAsyncGenerator(ctx, gen))
  }
}

/**
 * bindAsyncGenerator binds an async generator to a context
 * see https://github.com/open-telemetry/opentelemetry-js/issues/2951
 * @template {unknown} T
 * @template {any} TReturn
 * @template {unknown} TNext
 * @param {import('@opentelemetry/api').Context} ctx
 * @param {AsyncGenerator<T, TReturn, TNext>} generator
 * @returns {AsyncGenerator<T, TReturn, TNext>}
 */
function bindAsyncGenerator (ctx, generator) {
  return {
    next: context.bind(ctx, generator.next.bind(generator)),
    return: context.bind(ctx, generator.return.bind(generator)),
    throw: context.bind(ctx, generator.throw.bind(generator)),

    [Symbol.asyncIterator] () {
      return bindAsyncGenerator(ctx, generator[Symbol.asyncIterator]())
    },
    [Symbol.asyncDispose]: context.bind(ctx, generator[Symbol.asyncDispose]?.bind(generator))
  }
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
