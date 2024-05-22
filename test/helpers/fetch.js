import Queue from 'p-queue'

/**
 * @param {object} config
 * @param {number} config.concurrency Maximum number of requests to.
 * @param {number} config.lag Simulate network lag (in ms).
 */
export const patchFetch = ({ concurrency, lag }) => {
  const q = new Queue({ concurrency })
  const fetch = global.fetch.bind(global)
  global.fetch = (...args) => q.add(async () => {
    await new Promise(resolve => setTimeout(resolve, lag))
    return fetch(...args)
  })
}
