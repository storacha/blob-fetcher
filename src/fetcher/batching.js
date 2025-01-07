// eslint-disable-next-line
import * as API from '../api.js'
import { DigestMap } from '@web3-storage/blob-index'
import defer from 'p-defer'
import { NetworkError, NotFoundError } from '../lib.js'
import { fetchBlob } from './simple.js'
import { resolveRange } from './lib.js'
import { withAsyncGeneratorSpan, withResultSpan } from '../tracing/tracing.js'
import { MultipartByteRangeDecoder, getBoundary } from 'multipart-byte-range'

/**
 * @typedef {'*'|`${number},${number}`|`${number}`} RangeKey
 * @typedef {import('p-defer').DeferredPromise<API.Result<API.Blob, API.NotFound|API.Aborted|API.NetworkError>>} PendingBlobRequest
 * @typedef {Map<RangeKey, PendingBlobRequest[]>} RangedRequests
 */

const MAX_BATCH_SIZE = 16

/** @implements {API.Fetcher} */
class BatchingFetcher {
  #locator
  #fetch

  /** @type {DigestMap<API.MultihashDigest, RangedRequests>} */
  #pendingReqs = new DigestMap()

  /** @type {Array<{ location: API.Location, range?: API.Range }>} */
  #queue = []

  #scheduled = false

  /** @type {Promise<void>|null} */
  #processing = null

  /**
   * @param {API.Locator} locator
   * @param {typeof globalThis.fetch} [fetch]
   */
  constructor (locator, fetch = globalThis.fetch.bind(globalThis)) {
    this.#locator = locator
    this.#fetch = fetch
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

    // Basic algorithm
    // 1. assemble each http request
    // 2. fire off request
    // 3. once first byte received, begin processing the response async in background
    // 4. immediately go to next http request, but after first iteration, wait so that we're never processing the body
    // of more than one response at a time
    /** @type {Promise<API.Result<true, API.NotFound|API.Aborted|API.NetworkError>> | undefined } */
    let lastResolveBlobs
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

      const fetchRes = await fetchBlobs(siteURL, locs, this.#fetch)
      // if we have an error, stop
      if (fetchRes.error) {
        break
      }
      // if we are still processing the previous response, we should wait before we process this response
      if (lastResolveBlobs !== undefined) {
        const resolveRes = await lastResolveBlobs
        lastResolveBlobs = undefined
        if (resolveRes.error) {
          break
        }
      }
      lastResolveBlobs = resolveRequests(fetchRes.ok, pendingReqs)
    }

    // await the last call to resolve blobs
    if (lastResolveBlobs !== undefined) {
      await lastResolveBlobs
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
 * @param {typeof globalThis.fetch} [fetch]
 * @returns {API.Fetcher}
 */
export const create = (locator, fetch = globalThis.fetch.bind(globalThis)) => new BatchingFetcher(locator, fetch)

/** @typedef {{range: API.AbsoluteRange, digest: API.MultihashDigest, orig: API.Range | undefined}} ResolvedBlobs */

/**
 * Fetch blobs from the passed locations. The locations MUST share a common
 * site to fetch from.
 */
export const fetchBlobs = withResultSpan('fetchBlobs', _fetchBlobs)

/**
 * @param {URL} url Desired URL to fetch blobs from.
 * @param {Array<{ location: API.Location, range?: API.Range }>} locations
 * @param {typeof globalThis.fetch} [fetch]
 * @returns {Promise<API.Result<AsyncGenerator<BlobResult, API.Result<true, API.NotFound|API.Aborted|API.NetworkError>>, API.NotFound|API.Aborted|API.NetworkError>>}
 */
async function _fetchBlobs (url, locations, fetch = globalThis.fetch.bind(globalThis)) {
  if (locations.length === 1) {
    const res = await fetchBlob(locations[0].location, locations[0].range, fetch)
    if (res.error) return res
    return {
      ok: (async function * () {
        yield { blob: res.ok, range: locations[0].range }
        return { ok: true }
      }())
    }
  }

  // resolve ranges for blobs

  /** @type {ResolvedBlobs[]} */
  const resolvedBlobs = []
  for (const { location, range } of locations) {
    for (const s of location.site) {
      let found = false
      for (const l of s.location) {
        if (l.toString() === url.toString()) {
          /** @type {API.AbsoluteRange} */
          let resolvedRange = [s.range.offset, s.range.offset + s.range.length - 1]
          if (range) {
            const relRange = resolveRange(range, s.range.length)
            resolvedRange = [s.range.offset + relRange[0], s.range.offset + relRange[1]]
          }
          resolvedBlobs.push({
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
  if (resolvedBlobs.length !== locations.length) {
    throw new Error('no common site')
  }

  const headers = { Range: `bytes=${resolvedBlobs.map(r => `${r.range[0]}-${r.range[1]}`).join(',')}` }
  try {
    const res = await fetch(url, { headers })
    if (!res.ok) {
      return { error: new NetworkError(url, { cause: new Error(`unexpected HTTP status: ${res.status}`) }) }
    }
    return { ok: consumeBatchResponse(url, resolvedBlobs, res) }
  } catch (err) {
    return { error: new NetworkError(url, { cause: err }) }
  }
}

/** @typedef {{blob: API.Blob, range: API.Range | undefined}} BlobResult */

/**
 * Consumes a batch request to create multiple blobs. Will break up
 * a byte range going from first byte byte of first blob to last byte of last blob
 * into appropriate ranges for each blob
 */
const consumeBatchResponse = withAsyncGeneratorSpan('consumeBatchResponse', _consumeBatchResponse)

/**
 * @param {URL} url
 * @param {ResolvedBlobs[]} resolvedBlobs
 * @param {Response} res
 * @returns {AsyncGenerator<BlobResult, API.Result<true, API.NotFound|API.Aborted|API.NetworkError>>}
 */
async function * _consumeBatchResponse (url, resolvedBlobs, res) {
  if (!res.body) {
    return { error: new NetworkError(url, { cause: new Error('missing repsonse body') }) }
  }

  const boundary = getBoundary(res.headers)
  if (!boundary) {
    return { error: new NetworkError(url, { cause: new Error('missing multipart boundary') }) }
  }

  let i = 0

  try {
    for await (const chunk of res.body.pipeThrough(new MultipartByteRangeDecoder(boundary))) {
      // generate blob out of the current buffer
      const blob = new Blob(resolvedBlobs[i].digest, chunk.content)
      yield ({ blob, range: resolvedBlobs[i].orig })
      i++
    }
    return { ok: true }
  } catch (err) {
    return { error: new NetworkError(url, { cause: err }) }
  }
}

/**
 * Resolve pending requests from blobs generated out of the last fetch
 *
 * @param {AsyncGenerator<BlobResult, API.Result<true, API.NotFound|API.Aborted|API.NetworkError>>} blobResults
 * @param {DigestMap<API.MultihashDigest, RangedRequests>} pendingReqs
 * @returns {Promise<API.Result<true, API.NotFound|API.Aborted|API.NetworkError>>}
 */
const resolveRequests = async (blobResults, pendingReqs) => {
  for (;;) {
    const { value: result, done } = await blobResults.next()
    if (done) {
      return result
    }
    const { blob, range } = result
    const rangeReqs = pendingReqs.get(blob.digest)
    const key = rangeKey(range)
    const reqs = rangeReqs?.get(key)
    reqs?.[0].resolve({ ok: blob })
    reqs?.slice(1).forEach(r => r.resolve({ ok: blob.clone() }))
    rangeReqs?.delete(key)
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
