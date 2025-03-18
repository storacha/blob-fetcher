import { DigestMap } from '@web3-storage/blob-index'
import { NotFoundError } from '../lib.js'
import { withSimpleSpan } from '../tracing/tracing.js'
import { contentMultihash } from '@web3-storage/content-claims/client'

/**
 * @import * as API from '../api.js'
 * @import {Kind, IndexingServiceClient as ServiceClient} from '@storacha/indexing-service-client/api'
 * @import { DID } from '@ucanto/interface'
 * @import { ShardDigest, Position } from '@web3-storage/blob-index/types'
 */

/**
 * @typedef {Object} LocatorOptions
 * @property {ServiceClient} client An Indexing Service client instance.
 * @property {DID[]} [spaces] The Spaces to search for the content. If
 * missing, the locator will search all Spaces.
 * @property {API.AsyncDigestMap<API.MultihashDigest, API.Location>} [cache]
 */

export class IndexingServiceLocator {
  #client
  #spaces

  /**
   * Cached location entries.
   * @type {API.AsyncDigestMap<API.MultihashDigest, API.Location>}
   */
  #cache

  /** @type {DigestMap<API.MultihashDigest, { shardDigest: ShardDigest; position: Position; }>} */
  #knownSlices

  /**
   * Known Shards are locations claims we have a URL for but no length. They can be combined with known
   * slices to make a location entry, but can't be used for blob fetching on their own
  * @type {API.AsyncDigestMap<API.MultihashDigest, API.ShardLocation>}
   *
   */
  #knownShards

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
   * @type {Record<Kind, DigestMap<API.MultihashDigest, Promise<void>>>}
   */
  #claimFetched

  /**
   *
   * @param {LocatorOptions} options
   */
  constructor ({ client, spaces, cache = new SimpleAsyncDigestMap() }) {
    this.#client = client
    this.#spaces = spaces ?? []
    this.#cache = cache
    this.#claimFetched = {
      index_or_location: new DigestMap(),
      location: new DigestMap(),
      standard: new DigestMap()
    }
    this.#knownSlices = new DigestMap()
    this.#knownShards = new SimpleAsyncDigestMap()
  }

  /** @param {API.MultihashDigest} digest */
  async locate (digest) {
    // get the cached data for this CID (CAR CID & offset)
    let location = await this.getCache(digest)
    if (!location) {
      // no full cached data -- but perhaps we have the shard already?
      const knownSlice = this.#knownSlices.get(digest)
      if (knownSlice) {
        // read the shard
        await this.#readShard(digest, knownSlice.shardDigest, knownSlice.position)
      } else {
        // nope we don't know anything really here, better read for the digest
        await this.#readClaims(digest, 'standard')
        // if we now have and index, read the shard
        const knownSlice = this.#knownSlices.get(digest)
        if (knownSlice) {
          // read the shard
          await this.#readShard(digest, knownSlice.shardDigest, knownSlice.position)
        }
      }
      // seeing as we just read the index for this CID we _should_ have some
      // index information for it now.
      location = await this.getCache(digest)
      // if not then, well, it's not found!
      if (!location) return { error: new NotFoundError(digest) }
    }
    return { ok: location }
  }

  /**
   *
   * @param {API.MultihashDigest} digest
   * @param {ShardDigest} shard
   * @param {Position} pos
   * @returns
   */
  async #readShard (digest, shard, pos) {
    let location = await this.#getShard(shard)
    if (!location) {
      await this.#readClaims(shard, 'location')
      location = await this.#getShard(shard)
      // if not then, well, it's not found!
      if (!location) return
    }
    await this.#cache.set(digest, {
      digest,
      site: location.site.map(s => ({
        location: s.location,
        range: {
          offset: (s.range?.offset || 0) + pos[0],
          length: pos[1]
        },
        space: s.space
      }))
    })
  }

  /**
   *
   * @param {API.MultihashDigest} shardKey
   * @returns
   */
  async #getShard (shardKey) {
    const knownShard = await this.#knownShards.get(shardKey)
    if (knownShard) {
      return knownShard
    }
    return await this.getCache(shardKey)
  }

  /**
   *
   * @param {API.MultihashDigest} cacheKey
   * @returns
   */
  async getCache (cacheKey) {
    let location = await this.#cache.get(cacheKey)
    if (location && this.#spaces.length > 0) {
      const site = location.site.filter((site) =>
        site.space && this.#spaces.includes(site.space)
      )
      location = site.length > 0 ? { digest: location.digest, site } : undefined
    }
    return location
  }

  /**
   *
   * @param {API.MultihashDigest} digest
   * @param {Kind} kind
   */
  async #executeReadClaims (digest, kind) {
    const result = await this.#client.queryClaims({
      hashes: [digest],
      match: this.#spaces && { subject: this.#spaces },
      kind
    })

    if (result.error) return

    // process any location claims
    for (const claim of result.ok.claims.values()) {
      if (claim.type === 'assert/location') {
        if (claim.range?.length != null) {
          await addOrSetLocation(this.#cache, contentMultihash(claim), {
            location: claim.location.map(l => new URL(l)),
            range: { offset: claim.range.offset, length: claim.range.length },
            space: claim.space
          })
        } else {
          await addOrSetLocation(this.#knownShards, contentMultihash(claim), {
            location: claim.location.map(l => new URL(l)),
            range: claim.range ? { offset: claim.range.offset } : undefined,
            space: claim.space
          })
        }
      }
    }

    // fetch location claims for any indexes we don't have a known shard for
    for (const claim of result.ok.claims.values()) {
      if (claim.type === 'assert/index') {
        const location = await this.#getShard(claim.index.multihash)
        if (!location) {
          await this.#readClaims(claim.index.multihash, 'location')
        }
      }
    }

    // read any indexes in this request
    for (const index of result.ok.indexes.values()) {
      for (const [shardDigest, slices] of index.shards) {
        for (const [sliceDigest, position] of slices) {
          this.#knownSlices.set(sliceDigest, { shardDigest, position })
        }
      }
    }
  }

  /**
   * Read claims for the passed CID and populate the cache.
   * @param {API.MultihashDigest} digest
   * @param {Kind} kind
   */
  async #internalReadClaims (digest, kind) {
    if (this.#claimFetched[kind].has(digest)) {
      return this.#claimFetched[kind].get(digest)
    }
    const promise = this.#executeReadClaims(digest, kind)
    this.#claimFetched[kind].set(digest, promise)
    return promise
  }

  /**
   * Read claims for the passed CID and populate the cache.
   * @param {API.MultihashDigest} digest
   */
  #readClaims = withSimpleSpan('readClaims', this.#internalReadClaims, this)

  /** @type {API.Locator['scopeToSpaces']} */
  scopeToSpaces (spaces) {
    return new IndexingServiceLocator({
      client: this.#client,
      spaces: [...new Set([...this.#spaces, ...spaces]).values()],
      cache: this.#cache
    })
  }
}

/**
 * Create a new content claims blob locator.
 * @param {LocatorOptions} options
 * @returns {API.Locator}
 */
export const create = (options) => new IndexingServiceLocator(options)

/**
 * @template {API.OptionalRangeSite} T
 * @param {API.AsyncDigestMap<API.MultihashDigest, { digest: API.MultihashDigest, site: T[] }>} cache
 * @param {API.MultihashDigest} digest
 * @param {T} site
 */
const addOrSetLocation = async (cache, digest, site) => {
  const location = await cache.get(digest)
  if (location) {
    location.site.push(site)
  } else {
    await cache.set(digest, {
      digest,
      site: [site]
    })
  }
}

/**
 * @template {API.MultihashDigest<number>} Key
 * @template {any} Value
 */
export class SimpleAsyncDigestMap {
  /** @type {DigestMap<Key, Value>} */
  #digestMap

  constructor () {
    this.#digestMap = new DigestMap()
  }

  /**
   *
   * @param {Key} key
   */
  async get (key) {
    return this.#digestMap.get(key)
  }

  /**
   *
   * @param {Key} key
   * @param {Value} value
   */
  async set (key, value) {
    this.#digestMap.set(key, value)
  }
}
