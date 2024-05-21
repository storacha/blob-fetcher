// eslint-disable-next-line
import * as API from './api.js'
import { base58btc } from 'multiformats/bases/base58'

/** @implements {API.NotFound} */
export class NotFoundError extends Error {
  static name = /** @type {const} */ ('NotFound')

  /** @param {API.MultihashDigest} digest */
  constructor (digest) {
    super(`not found: ${base58btc.encode(digest.bytes)}`)
    this.name = NotFoundError.name
    this.digest = digest.bytes
  }
}

/** @implements {API.Aborted} */
export class AbortError extends Error {
  static name = /** @type {const} */ ('Aborted')

  /** @param {API.MultihashDigest} digest */
  constructor (digest) {
    super(`aborted: ${base58btc.encode(digest.bytes)}`)
    this.name = AbortError.name
    this.digest = digest.bytes
  }
}

/** @implements {API.NetworkError} */
export class NetworkError extends Error {
  static name = /** @type {const} */ ('NetworkError')

  /**
   * @param {URL} url
   * @param {ErrorOptions} [options]
   */
  constructor (url, options) {
    super(`failed to fetch: ${url}`, options)
    this.name = NetworkError.name
    this.url = /** @type {API.URI} */ (url.toString())
  }
}
