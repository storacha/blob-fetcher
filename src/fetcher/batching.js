// eslint-disable-next-line
import * as API from '../api.js'
import { DigestMap } from '@web3-storage/blob-index'
import { meros } from 'meros/browser'
import defer from 'p-defer'
import { NetworkError, NotFoundError } from '../lib.js'
import { fetchBlob } from './simple.js'
// import { base58btc } from 'multiformats/bases/base58'
// import { sha256 } from 'multiformats/hashes/sha2'
// import { equals } from 'multiformats/bytes'

const MAX_BATCH_SIZE = 6

/** @implements {API.Fetcher} */
class BatchingFetcher {
  #locator

  /** @type {Map<API.MultihashDigest, Array<import('p-defer').DeferredPromise<API.Result<API.Blob, API.NotFound|API.Aborted|API.NetworkError>>>>} */
  #pendingReqs = new DigestMap()

  /** @type {API.Location[]} */
  #queue = []

  #scheduled = false

  /** @type {Promise<void>|null} */
  #processing = null

  /** @param {API.Locator} locator */
  constructor (locator) {
    this.#locator = locator
  }

  #scheduleBatchProcessing () {
    if (this.#scheduled) return
    this.#scheduled = true

    const startProcessing = async () => {
      this.#scheduled = false
      const { promise, resolve } = defer()
      this.#processing = promise
      try {
        await this.#processBatch()
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

  async #processBatch () {
    // console.log('processing batch')
    const queue = this.#queue
    this.#queue = []
    const pendingReqs = this.#pendingReqs
    this.#pendingReqs = new DigestMap()

    while (true) {
      const first = queue.shift()
      if (!first) break

      const siteURL = first.site[0].location[0]
      const locs = [first]
      while (true) {
        const next = queue.shift()
        if (!next) break

        const site = next.site.find(s => s.location.some(l => l.toString() === siteURL.toString()))
        if (!site) break

        locs.push(next)
        if (locs.length >= MAX_BATCH_SIZE) break
      }

      const res = await fetchBlobs(siteURL, locs)
      if (res.error) break
      for (const blob of res.ok) {
        const reqs = pendingReqs.get(blob.digest)
        reqs?.forEach(r => r.resolve({ ok: blob }))
        pendingReqs.delete(blob.digest)
      }
    }

    // resolve `undefined` for any remaining blocks
    for (const [digest, reqs] of pendingReqs) {
      reqs.forEach(r => r.resolve({ error: new NotFoundError(digest) }))
    }
  }

  /**
   * @param {API.MultihashDigest} digest
   * @param {API.GetOptions} [options]
   */
  async fetch (digest, options) {
    // console.log('fetch', base58btc.encode(digest.bytes))
    const locResult = await this.#locator.locate(digest, options)
    if (locResult.error) return locResult

    let reqs = this.#pendingReqs.get(locResult.ok.digest)
    if (!reqs) {
      reqs = []
      this.#pendingReqs.set(locResult.ok.digest, reqs)
      this.#queue.push(locResult.ok)
    }
    /** @type {import('p-defer').DeferredPromise<API.Result<API.Blob, API.NotFound|API.Aborted|API.NetworkError>>} */
    const deferred = defer()
    reqs.push(deferred)
    this.#scheduleBatchProcessing()
    return deferred.promise
  }
}

/**
 * Create a new batching blob fetcher.
 * @param {API.Locator} locator
 * @returns {API.Fetcher}
 */
export const create = (locator) => new BatchingFetcher(locator)

/**
 * Fetch blobs from the passed locations. The locations MUST share a common
 * site to fetch from.
 *
 * @param {URL} url Desired URL to fetch blobs from.
 * @param {API.Location[]} locations
 * @returns {Promise<API.Result<API.Blob[], API.NotFound|API.Aborted|API.NetworkError>>}
 */
export const fetchBlobs = async (url, locations) => {
  if (locations.length === 1) {
    const res = await fetchBlob(locations[0])
    if (res.error) return res
    return { ok: [res.ok] }
  }

  const ranges = []
  for (const loc of locations) {
    for (const s of loc.site) {
      let found = false
      for (const l of s.location) {
        if (l.toString() === url.toString()) {
          ranges.push(s.range)
          found = true
          break
        }
      }
      if (found) break
    }
  }
  if (ranges.length !== locations.length) {
    throw new Error('no common site')
  }

  const headers = { Range: `bytes=${ranges.map(r => `${r.offset}-${r.offset + r.length - 1}`).join(',')}` }
  try {
    // console.log(url.toString(), headers)
    const res = await fetch(url, { headers })
    if (!res.ok) {
      return { error: new NetworkError(url, { cause: new Error(`unexpected HTTP status: ${res.status}`) }) }
    }

    let i = 0
    const parts = /** @type {AsyncGenerator<import('meros').Part<string, string>>} */ (await meros(res))
    const blobs = []
    for await (const part of parts) {
      // FIXME: this does not work
      const bytes = new TextEncoder().encode(part.body)
      blobs.push({ digest: locations[i].digest, bytes })
      i++
    }
    return { ok: blobs }
  } catch (err) {
    return { error: new NetworkError(url, { cause: err }) }
  }
}
