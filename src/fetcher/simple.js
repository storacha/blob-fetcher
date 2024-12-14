// eslint-disable-next-line
import * as API from '../api.js'
import { resolveRange } from './lib.js'
import { NetworkError, NotFoundError } from '../lib.js'
import { withResultSpan } from '../tracing/tracing.js'

/** @implements {API.Fetcher} */
class SimpleFetcher {
  #locator

  /** @param {API.Locator} locator */
  constructor (locator) {
    this.#locator = locator
  }

  /**
   * @param {API.MultihashDigest} digest
   * @param {API.FetchOptions} [options]
   */
  async fetch (digest, options) {
    const locResult = await this.#locator.locate(digest, options)
    if (locResult.error) return locResult
    return fetchBlob(locResult.ok, options?.range)
  }
}

/**
 * Create a new blob fetcher.
 * @param {API.Locator} locator
 * @returns {API.Fetcher}
 */
export const create = (locator) => new SimpleFetcher(locator)

export const fetchBlob = withResultSpan('fetchBlob',
  /**
 * Fetch a blob from the passed location.
 * @param {API.Location} location
 * @param {API.Range} [range]
 */
  async (location, range) => {
    let networkError

    for (const site of location.site) {
      for (const url of site.location) {
        let resolvedRange = [site.range.offset, site.range.offset + site.range.length - 1]
        if (range) {
          const relRange = resolveRange(range, site.range.length)
          resolvedRange = [site.range.offset + relRange[0], site.range.offset + relRange[1]]
        }
        const headers = { Range: `bytes=${resolvedRange[0]}-${resolvedRange[1]}` }
        try {
          const res = await fetch(url, { headers })
          if (!res.ok || !res.body) {
            console.warn(`failed to fetch ${url}: ${res.status} ${await res.text()}`)
            continue
          }
          return { ok: new Blob(location.digest, res) }
        } catch (err) {
          networkError = new NetworkError(url, { cause: err })
        }
      }
    }

    return { error: networkError || new NotFoundError(location.digest) }
  }
)

/** @implements {API.Blob} */
class Blob {
  #digest
  #response

  /**
   * @param {API.MultihashDigest} digest
   * @param {Response} response
   */
  constructor (digest, response) {
    this.#digest = digest
    this.#response = response
  }

  get digest () {
    return this.#digest
  }

  async bytes () {
    return new Uint8Array(await this.#response.arrayBuffer())
  }

  stream () {
    if (!this.#response.body) throw new Error('missing response body')
    return this.#response.body
  }

  clone () {
    return new Blob(this.#digest, this.#response.clone())
  }
}
