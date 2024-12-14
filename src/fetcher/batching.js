// eslint-disable-next-line
import * as API from '../api.js'
import { DigestMap } from '@web3-storage/blob-index'
import defer from 'p-defer'
import { MultipartByteRangeDecoder, getBoundary } from 'multipart-byte-range/decoder'
import { NetworkError, NotFoundError } from '../lib.js'
import { fetchBlob } from './simple.js'
import { resolveRange } from './lib.js'
import { withResultSpan } from '../tracing/tracing.js'
import { Uint8ArrayList } from 'uint8arraylist'

/**
 * @typedef {'*'|`${number},${number}`|`${number}`} RangeKey
 * @typedef {import('p-defer').DeferredPromise<API.Result<API.Blob, API.NotFound|API.Aborted|API.NetworkError>>} PendingBlobRequest
 * @typedef {Map<RangeKey, PendingBlobRequest[]>} RangedRequests
 */

const MAX_BATCH_SIZE = 16

/** @implements {API.Fetcher} */
class BatchingFetcher {
  #locator

  /** @type {DigestMap<API.MultihashDigest, RangedRequests>} */
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

    const responses = []
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

      responses.push(fetchBlobs(siteURL, locs, pendingReqs))
    }

    await Promise.all(responses)

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

/** @typedef {{range: import('multipart-byte-range').AbsoluteRange, digest: API.MultihashDigest, orig: API.Range | undefined}} ResolvedRange */

/**
 * Fetch blobs from the passed locations. The locations MUST share a common
 * site to fetch from.
 */
export const fetchBlobs = withResultSpan('fetchBlobs',
/**
 * @param {URL} url Desired URL to fetch blobs from.
 * @param {Array<{ location: API.Location, range?: API.Range }>} locations
 * @param {DigestMap<API.MultihashDigest, RangedRequests>} pendingReqs
 * @returns {Promise<API.Result<true, API.NotFound|API.Aborted|API.NetworkError>>}
 */
  async (url, locations, pendingReqs) => {
    if (locations.length === 1) {
      const res = await fetchBlob(locations[0].location, locations[0].range)
      if (res.error) return res
      resolveBlob(res.ok, locations[0].range, pendingReqs)
      return { ok: true }
    }

    /** @type {ResolvedRange[]>} */
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
            ranges.push({
              digest: location.digest,
              range: resolvedRange,
              orig: range
            })
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

    ranges.sort((a, b) => a.range[0] - b.range[0])
    const aggregateRangeEnd = ranges.reduce((aggregateEnd, r) => r.range[1] > aggregateEnd ? r.range[1] : aggregateEnd, 0)
    const headers = { Range: `bytes=${ranges[0].range[0]}-${aggregateRangeEnd}` }
    try {
      const res = await fetch(url, { headers })
      if (!res.ok) {
        return { error: new NetworkError(url, { cause: new Error(`unexpected HTTP status: ${res.status}`) }) }
      }
      return await consumeMultipartResponse(url, ranges, res, pendingReqs)
    } catch (err) {
      return { error: new NetworkError(url, { cause: err }) }
    }
  })

/**
 * Consumes a multipart range request to create multiple blobs
 */
const consumeMultipartResponse = withResultSpan('consumeMultipartResponse',
/**
 * @param {URL} url
 * @param {ResolvedRange[]} sortedRanges
 * @param {Response} res
 * @param {DigestMap<API.MultihashDigest, RangedRequests>} pendingReqs
 * @returns {Promise<API.Result<true, API.NotFound|API.Aborted|API.NetworkError>>}
 */
  async (url, sortedRanges, res, pendingReqs) => {
    if (!res.body) {
      return { error: new NetworkError(url, { cause: new Error('missing repsonse body') }) }
    }
    const parts = new Uint8ArrayList()
    let farthestRead = sortedRanges[0].range[0]
    let farthestConsumed = sortedRanges[0].range[0]
    let currentRange = 0
    try {
      for await (const chunk of res.body) {
        // append the chunk to our buffer
        parts.append(chunk)
        // update the absolute range of what we've read
        farthestRead += chunk.byteLength
        // read and push any blobs in the current buffer
        // note that as long as ranges are sorted ascending by start
        // this should be resilient to overlapping ranges
        while (farthestRead >= sortedRanges[currentRange].range[1] + 1) {
          const blob = new Blob(sortedRanges[currentRange].digest,
            parts.subarray(sortedRanges[currentRange].range[0] - farthestConsumed, sortedRanges[currentRange].range[1] + 1 - farthestConsumed))
          resolveBlob(blob, sortedRanges[currentRange].orig, pendingReqs)
          currentRange++
          if (currentRange >= sortedRanges.length) {
            return { ok: true }
          }
          let toConsume = sortedRanges[currentRange].range[0] - farthestConsumed
          if (toConsume > parts.byteLength) { toConsume = parts.byteLength }
          parts.consume(toConsume)
          farthestConsumed += toConsume
        }
      }
      return { error: new NetworkError(url, { cause: new Error('did not resolve all chunks') }) }
    } catch (err) {
      console.log(err)
      return { error: new NetworkError(url, { cause: err }) }
    }
  })

/**
 *
 * @param {API.Blob} blob
 * @param {API.Range | undefined} range
 * @param {DigestMap<API.MultihashDigest, RangedRequests>} pendingReqs
 */
const resolveBlob = (blob, range, pendingReqs) => {
  const rangeReqs = pendingReqs.get(blob.digest)
  const key = rangeKey(range)
  const reqs = rangeReqs?.get(key)
  reqs?.[0].resolve({ ok: blob })
  reqs?.slice(1).forEach(r => r.resolve({ ok: blob.clone() }))
  rangeReqs?.delete(key)
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
