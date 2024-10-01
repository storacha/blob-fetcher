// eslint-disable-next-line
import * as API from '../api.js'
import * as Claims from '@web3-storage/content-claims/client'
import { DigestMap, ShardedDAGIndex } from '@web3-storage/blob-index'
import { fetchBlob } from '../fetcher/simple.js'
import { NotFoundError } from '../lib.js'

/**
 * @typedef {import('multiformats').UnknownLink} UnknownLink
 */

/** @implements {API.Locator} */
export class ContentClaimsLocator {
  /**
   * Cached content claims.
   * @type {Map<API.MultihashDigest, import('@web3-storage/content-claims/client/api').Claim[]>}
   */
  claims
  /**
   * Cached location entries.
   * @type {Map<API.MultihashDigest, API.Location>}
   */
  #cache
  /**
   * Multihash digests for which we have already fetched claims.
   *
   * Note: _only_ the digests which have been explicitly queried, for which we
   * have made a content claim request. Not using `this.#cache` because reading
   * a claim may cause us to add other digests to the cache that we haven't
   * read claims for.
   *
   * Note: implemented as a Map not a Set so that we take advantage of the
   * key cache that `DigestMap` provides, so we don't duplicate base58 encoded
   * multihash keys.
   * @type {Map<API.MultihashDigest, true>}
   */
  #claimFetched
  /**
   * @type {URL|undefined}
   */
  #serviceURL

  /**
   * @param {{ serviceURL?: URL }} [options]
   */
  constructor (options) {
    this.claims = new DigestMap()
    this.#cache = new DigestMap()
    this.#claimFetched = new DigestMap()
    this.#serviceURL = options?.serviceURL
  }

  /** @param {API.MultihashDigest} digest */
  async locate (digest) {
    // get the index data for this CID (CAR CID & offset)
    let location = this.#cache.get(digest)
    if (!location) {
      // we not found the index data!
      await this.#readClaims(digest)
      // seeing as we just read the index for this CID we _should_ have some
      // index information for it now.
      location = this.#cache.get(digest)
      // if not then, well, it's not found!
      if (!location) return { error: new NotFoundError(digest) }
    }
    return { ok: location }
  }

  /**
   * Read claims for the passed CID and populate the cache.
   * @param {API.MultihashDigest} digest
   */
  async #readClaims (digest) {
    if (this.#claimFetched.has(digest)) return

    const claims = await Claims.read(digest, { serviceURL: this.#serviceURL })
    for (const claim of claims) {
      if (claim.type === 'assert/location' && claim.range?.length != null) {
        const location = this.#cache.get(digest)
        if (location) {
          location.site.push({
            location: claim.location.map(l => new URL(l)),
            range: { offset: claim.range.offset, length: claim.range.length }
          })
        } else {
          this.#cache.set(digest, {
            digest,
            site: [{
              location: claim.location.map(l => new URL(l)),
              range: { offset: claim.range.offset, length: claim.range.length }
            }]
          })
        }
      }

      if (claim.type === 'assert/index') {
        await this.#readClaims(claim.index.multihash)
        const location = this.#cache.get(claim.index.multihash)
        if (!location) continue

        const fetchRes = await fetchBlob(location)
        if (fetchRes.error) {
          console.warn('failed to fetch index', fetchRes.error)
          continue
        }

        const indexBytes = await fetchRes.ok.bytes()
        const decodeRes = ShardedDAGIndex.extract(indexBytes)
        if (decodeRes.error) {
          console.warn('failed to decode index', decodeRes.error)
          continue
        }

        const index = decodeRes.ok
        await Promise.all([...index.shards].map(async ([shard, slices]) => {
          await this.#readClaims(shard)
          const location = this.#cache.get(shard)
          if (!location) return

          for (const [slice, pos] of slices) {
            this.#cache.set(slice, {
              digest: slice,
              site: location.site.map(s => ({
                location: s.location,
                range: {
                  offset: s.range.offset + pos[0],
                  length: s.range.offset + pos[1]
                }
              }))
            })
          }
        }))
      }
    }
    this.claims.set(digest, claims)
    this.#claimFetched.set(digest, true)
  }
}

/**
 * Create a new content claims blob locator.
 * @param {{ serviceURL?: URL }} [options]
 * @returns {API.Locator}
 */
export const create = (options) => new ContentClaimsLocator(options)
