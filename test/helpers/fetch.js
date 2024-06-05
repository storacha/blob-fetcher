import Queue from 'p-queue'

let total = 0

export const count = () => total
export const resetCount = () => { total = 0 }

/**
 * @param {object} config
 * @param {number} config.concurrency Maximum number of requests to.
 * @param {number} config.lag Simulate network lag (in ms).
 */
export const patch = ({ concurrency, lag }) => {
  const q = new Queue({ concurrency })
  const fetch = global.fetch.bind(global)
  global.fetch = (...args) => q.add(async () => {
    total++
    await new Promise(resolve => setTimeout(resolve, lag))
    return fetch(...args)
  })
}
