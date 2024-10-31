// eslint-disable-next-line
import * as API from '../api.js'
import * as Claims from '@web3-storage/content-claims/client'
import { DigestMap, ShardedDAGIndex } from '@web3-storage/blob-index'
import { fetchBlob } from '../fetcher/simple.js'
import { NotFoundError } from '../lib.js'
import { base58btc } from 'multiformats/bases/base58'

/**
 * @typedef {import('multiformats').UnknownLink} UnknownLink
 */

/** @implements {API.Locator} */
export class ContentClaimsLocator {
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
   * @type {import('@cloudflare/workers-types').R2Bucket|undefined}
   */
  #carpark
  /**
   * @type {URL | Undefined}
   */
  #carparkPublicBucketURL
  /**
   * @param {{ serviceURL?: URL, carpark?: import('@cloudflare/workers-types').R2Bucket, carparkPublicBucketURL?: URL}} [options]
   */
  constructor (options) {
    this.#cache = new DigestMap()
    this.#claimFetched = new DigestMap()
    this.#serviceURL = options?.serviceURL
    this.#carpark = options?.carpark
    this.#carparkPublicBucketURL = options?.carparkPublicBucketURL
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
        /** @type {Uint8Array} */
        let indexBytes
        if (!location) {
          if (this.#carpark === undefined) {
            continue
          }
          const obj = await this.#carpark.get(toBlobKey(claim.index.multihash))
          if (!obj) {
            continue
          }
          indexBytes = new Uint8Array(await obj.arrayBuffer())
        } else {
          const fetchRes = await fetchBlob(location)
          if (fetchRes.error) {
            console.warn('failed to fetch index', fetchRes.error)
            continue
          }
          indexBytes = await fetchRes.ok.bytes()
        }

        const decodeRes = ShardedDAGIndex.extract(indexBytes)
        if (decodeRes.error) {
          console.warn('failed to decode index', decodeRes.error)
          continue
        }

        const index = decodeRes.ok
        await Promise.all([...index.shards].map(async ([shard, slices]) => {
          await this.#readClaims(shard)
          let location = this.#cache.get(shard)
          if (!location) {
            if (this.#carpark === undefined || this.#carparkPublicBucketURL === undefined) {
              return
            }
            const obj = await this.#carpark.head(toBlobKey(claim.index.multihash))
            if (!obj) {
              return
            }
            location = {
              digest: claim.index.multihash,
              site: [{
                location: [new URL(toBlobKey(claim.index.multihash), this.#carparkPublicBucketURL)],
                range: { offset: 0, length: obj.size }
              }]
            }
          }

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
    this.#claimFetched.set(digest, true)
  }
}

/**
 * Create a new content claims blob locator.
 * @param {{ serviceURL?: URL }} [options]
 * @returns {API.Locator}
 */
export const create = (options) => new ContentClaimsLocator(options)

/** @param {import('multiformats').MultihashDigest} digest */
const toBlobKey = digest => {
  const mhStr = base58btc.encode(digest.bytes)
  return `${mhStr}/${mhStr}.blob`
}
