// eslint-disable-next-line
import * as API from '../api.js'
import { NetworkError, NotFoundError } from '../lib.js'

/** @implements {API.Fetcher} */
class SimpleFetcher {
  #locator

  /** @param {API.Locator} locator */
  constructor (locator) {
    this.#locator = locator
  }

  /**
   * @param {API.MultihashDigest} digest
   * @param {API.GetOptions} [options]
   */
  async fetch (digest, options) {
    const locResult = await this.#locator.locate(digest, options)
    if (locResult.error) return locResult
    return fetchBlob(locResult.ok)
  }
}

/**
 * Create a new blob fetcher.
 * @param {API.Locator} locator
 * @returns {API.Fetcher}
 */
export const create = (locator) => new SimpleFetcher(locator)

/**
 * Fetch a blob from the passed location.
 * @param {API.Location} location
 */
export const fetchBlob = async (location) => {
  let networkError
  for (const site of location.site) {
    for (const url of site.location) {
      const headers = { Range: `bytes=${site.range.offset}-${site.range.offset + site.range.length - 1}` }
      try {
        const res = await fetch(url, { headers })
        if (!res.ok) {
          console.warn(`failed to fetch ${url}: ${res.status} ${await res.text()}`)
          continue
        }
        return { ok: { digest: location.digest, bytes: new Uint8Array(await res.arrayBuffer()) } }
      } catch (err) {
        networkError = new NetworkError(url, { cause: err })
      }
    }
  }

  return { error: networkError || new NotFoundError(location.digest) }
}
