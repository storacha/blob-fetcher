// eslint-disable-next-line
import defer from 'p-defer'

/** @template Work */
export class Batcher {
  /** @type {Work} */
  #work

  #scheduled = false

  /** @type {Promise<void>|null} */
  #processing = null

  #processBatch
  #initializeWork
  /**
   * @param {(work: Work) => Promise<void>} processBatch
   * @param {() => Work} initializeWork
   */
  constructor (processBatch, initializeWork) {
    this.#processBatch = processBatch
    this.#initializeWork = initializeWork
    this.#work = this.#initializeWork()
  }

  #scheduleBatchProcessing () {
    if (this.#scheduled) return
    this.#scheduled = true

    const startProcessing = async () => {
      this.#scheduled = false
      const { promise, resolve } = defer()
      this.#processing = promise
      try {
        const work = this.#work
        this.#work = this.#initializeWork()
        await this.#processBatch(work)
      } finally {
        this.#processing = null
        resolve()
      }
    }

    // If already running, then start when finished
    if (this.#processing) {
      return this.#processing.then(startProcessing)
    }

    // If not running, then start on the next tick
    setTimeout(startProcessing)
  }

  /**
   * @template Pending
   * @param {(work: Work) => Pending} addNewWork
   */
  schedule (addNewWork) {
    const p = addNewWork(this.#work)
    this.#scheduleBatchProcessing()
    return p
  }
}
