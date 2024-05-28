// eslint-disable-next-line
import * as API from '../api.js'
import { DigestMap } from '@web3-storage/blob-index'
import defer from 'p-defer'
import { MultipartByteRangeDecoder, getBoundary } from 'multipart-byte-range/decoder'
import { NetworkError, NotFoundError } from '../lib.js'
import { fetchBlob } from './simple.js'
import { resolveRange } from './lib.js'

/**
 * @typedef {'*'|`${number},${number}`|`${number}`} RangeKey
 * @typedef {import('p-defer').DeferredPromise<API.Result<API.Blob, API.NotFound|API.Aborted|API.NetworkError>>} PendingBlobRequest
 * @typedef {Map<RangeKey, PendingBlobRequest[]>} RangedRequests
 */

const MAX_BATCH_SIZE = 12

/** @implements {API.Fetcher} */
class BatchingFetcher {
  #locator

  /** @type {Map<API.MultihashDigest, RangedRequests>} */
  #pendingReqs = new DigestMap()

  /** @type {Array<{ location: API.Location, range?: API.Range }>} */
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
    const queue = this.#queue
    this.#queue = []
    const pendingReqs = this.#pendingReqs
    this.#pendingReqs = new DigestMap()

    while (true) {
      const first = queue.shift()
      if (!first) break

      const siteURL = first.location.site[0].location[0]
      const locs = [first]
      while (true) {
        const next = queue[0]
        if (!next) break

        const site = next.location.site.find(s => s.location.some(l => l.toString() === siteURL.toString()))
        if (!site) break

        queue.shift()
        locs.push(next)
        if (locs.length >= MAX_BATCH_SIZE) break
      }

      const res = await fetchBlobs(siteURL, locs)
      if (res.error) break
      for (const [i, blob] of res.ok.entries()) {
        const rangeReqs = pendingReqs.get(blob.digest)
        const key = rangeKey(locs[i].range)
        const reqs = rangeReqs?.get(key)
        reqs?.[0].resolve({ ok: blob })
        reqs?.slice(1).forEach(r => r.resolve({ ok: blob.clone() }))
        rangeReqs?.delete(key)
      }
    }

    // resolve `undefined` for any remaining requests
    for (const [digest, rangeReqs] of pendingReqs) {
      for (const [, reqs] of rangeReqs) {
        reqs.forEach(r => r.resolve({ error: new NotFoundError(digest) }))
      }
    }
  }

  /**
   * @param {API.MultihashDigest} digest
   * @param {API.FetchOptions} [options]
   */
  async fetch (digest, options) {
    const locResult = await this.#locator.locate(digest, options)
    if (locResult.error) return locResult

    let rangeReqs = this.#pendingReqs.get(locResult.ok.digest)
    if (!rangeReqs) {
      rangeReqs = new Map()
      this.#pendingReqs.set(locResult.ok.digest, rangeReqs)
    }
    const key = rangeKey(options?.range)
    let reqs = rangeReqs.get(key)
    if (!reqs) {
      reqs = []
      rangeReqs.set(key, reqs)
      this.#queue.push({ location: locResult.ok, range: options?.range })
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
 * @param {Array<{ location: API.Location, range?: API.Range }>} locations
 * @returns {Promise<API.Result<API.Blob[], API.NotFound|API.Aborted|API.NetworkError>>}
 */
export const fetchBlobs = async (url, locations) => {
  if (locations.length === 1) {
    const res = await fetchBlob(locations[0].location, locations[0].range)
    if (res.error) return res
    return { ok: [res.ok] }
  }

  const ranges = []
  for (const { location, range } of locations) {
    for (const s of location.site) {
      let found = false
      for (const l of s.location) {
        if (l.toString() === url.toString()) {
          /** @type {import('multipart-byte-range').AbsoluteRange} */
          let resolvedRange = [s.range.offset, s.range.offset + s.range.length - 1]
          if (range) {
            const relRange = resolveRange(range, s.range.length)
            resolvedRange = [s.range.offset + relRange[0], s.range.offset + relRange[1]]
          }
          ranges.push(resolvedRange)
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

  const headers = { Range: `bytes=${ranges.map(r => `${r[0]}-${r[1]}`).join(',')}` }
  try {
    const res = await fetch(url, { headers })
    if (!res.ok) {
      return { error: new NetworkError(url, { cause: new Error(`unexpected HTTP status: ${res.status}`) }) }
    }

    if (!res.body) {
      return { error: new NetworkError(url, { cause: new Error('missing repsonse body') }) }
    }

    const boundary = getBoundary(res.headers)
    if (!boundary) {
      return { error: new NetworkError(url, { cause: new Error('missing multipart boundary') }) }
    }

    /** @type {API.Blob[]} */
    const blobs = []
    let i = 0
    await res.body
      .pipeThrough(new MultipartByteRangeDecoder(boundary))
      .pipeTo(new WritableStream({
        write (part) {
          blobs.push(new Blob(locations[i].location.digest, part.content))
          i++
        }
      }))

    return { ok: blobs }
  } catch (err) {
    return { error: new NetworkError(url, { cause: err }) }
  }
}

/** @implements {API.Blob} */
class Blob {
  #digest
  #bytes

  /**
   * @param {API.MultihashDigest} digest
   * @param {Uint8Array} bytes
   */
  constructor (digest, bytes) {
    this.#digest = digest
    this.#bytes = bytes
  }

  get digest () {
    return this.#digest
  }

  async bytes () {
    return this.#bytes
  }

  stream () {
    return new ReadableStream({
      pull: (controller) => {
        controller.enqueue(this.#bytes)
        controller.close()
      }
    })
  }

  clone () {
    return new Blob(this.#digest, this.#bytes)
  }
}

/** @param {API.Range} [range] */
const rangeKey = (range) => /** @type {RangeKey} */ (range ? range.toString() : '*')
