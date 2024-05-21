import Queue from 'p-queue'

/** @param {number} concurrency */
export const patchFetch = (concurrency) => {
  const q = new Queue({ concurrency })
  const fetch = global.fetch.bind(global)
  global.fetch = (...args) => q.add(() => fetch(...args))
}
