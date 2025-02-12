import { DigestMap } from '@web3-storage/blob-index'
import { NotFoundError } from '../lib.js'
import { withSimpleSpan } from '../tracing/tracing.js'
import { contentMultihash } from '@web3-storage/content-claims/client'
import { Batcher } from '../batcher/batcher.js'
import { base58btc } from 'multiformats/bases/base58'

import defer from 'p-defer'

const MAX_BATCH_SIZE = 16

/**
 * @import * as API from '../api.js'
 * @import {Kind, IndexingServiceClient as ServiceClient} from '@storacha/indexing-service-client/api'
 * @import { DID } from '@ucanto/interface'
 * @import { ShardDigest, Position } from '@web3-storage/blob-index/types'
 * @import { DeferredPromise} from 'p-defer'
 */

/**
 * @typedef {Object} LocatorOptions
 * @property {ServiceClient} client An Indexing Service client instance.
 * @property {DID[]} [spaces] The Spaces to search for the content. If
 * missing, the locator will search all Spaces.
 */

/** @typedef { Record<Kind, DigestMap<API.MultihashDigest, DeferredPromise<void>>> } Work */

/**
 * @returns Work
 */
const newWork = () => ({
  standard: new DigestMap(),
  index_or_location: new DigestMap(),
  location: new DigestMap()
})

export class IndexingServiceLocator {
  #client
  #spaces

  /**
   * Cached location entries.
   * @type {DigestMap<API.MultihashDigest, API.Location>}
   */
  #cache

  /** @type {DigestMap<API.MultihashDigest, { shardDigest: ShardDigest; position: Position; }>} */
  #knownSlices

  /**
   * Known Shards are locations claims we have a URL for but no length. They can be combined with known
   * slices to make a location entry, but can't be used for blob fetching on their own
  * @type {DigestMap<API.MultihashDigest, API.ShardLocation>}
   *
   */
  #knownShards

  /**
   * Known indexes are indexes claims we are aware of
   * @type {DigestMap<API.MultihashDigest, API.MultihashDigest>}
   *
   */
  #knownIndexes

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

  /** @type {Batcher<Work>} */
  #batcher

  /**
   *
   * @param {LocatorOptions} options
   */
  constructor ({ client, spaces }) {
    this.#client = client
    this.#spaces = spaces ?? []
    this.#cache = new DigestMap()
    this.#claimFetched = {
      index_or_location: new DigestMap(),
      location: new DigestMap(),
      standard: new DigestMap()
    }
    this.#knownSlices = new DigestMap()
    this.#knownShards = new DigestMap()
    this.#knownIndexes = new DigestMap()
    this.#batcher = new Batcher(this.#processBatch.bind(this), newWork)
  }

  /** @param {API.MultihashDigest} digest */
  async locate (digest) {
    // get the cached data for this CID (CAR CID & offset)
    let location = this.#cache.get(digest)
    if (!location) {
      // no full cached data -- but perhaps we have the shard already?
      let knownSlice = this.#knownSlices.get(digest)
      if (!knownSlice) {
        let knownIndex = this.#knownIndexes.get(digest)
        if (!knownIndex) {
          // nope we don't know anything really here, better read for the digest
          await this.#readClaims(digest, 'standard')
          knownIndex = this.#knownIndexes.get(digest)
        }
        if (knownIndex) {
          const location = this.#getShard(knownIndex)
          if (!location) {
            await this.#readClaims(knownIndex, 'location')
          }
        }
        // if we now have and index, read the shard
        knownSlice = this.#knownSlices.get(digest)
      }
      if (knownSlice) {
        // read the shard
        await this.#readShard(digest, knownSlice.shardDigest, knownSlice.position)
      }
      // seeing as we just read the index for this CID we _should_ have some
      // index information for it now.
      location = this.#cache.get(digest)
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
    let location = this.#getShard(shard)
    if (!location) {
      await this.#readClaims(shard, 'location')
      location = this.#getShard(shard)
      // if not then, well, it's not found!
      if (!location) return
    }
    this.#cache.set(digest, {
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
  #getShard (shardKey) {
    const knownShard = this.#knownShards.get(shardKey)
    if (knownShard) {
      return knownShard
    }
    return this.#cache.get(shardKey)
  }

  /**
   *
   * @param {API.MultihashDigest[]} digests
   * @param {Kind} kind
   */
  async #executeReadClaims (digests, kind) {
    const result = await this.#client.queryClaims({
      hashes: digests,
      match: this.#spaces && { subject: this.#spaces },
      kind
    })
    console.log(result)
    if (result.error) return

    // process any location claims
    for (const claim of result.ok.claims.values()) {
      if (claim.type === 'assert/location') {
        if (claim.range?.length != null) {
          addOrSetLocation(this.#cache, contentMultihash(claim), {
            location: claim.location.map(l => new URL(l)),
            range: { offset: claim.range.offset, length: claim.range.length },
            space: claim.space
          })
        } else {
          addOrSetLocation(this.#knownShards, contentMultihash(claim), {
            location: claim.location.map(l => new URL(l)),
            range: claim.range ? { offset: claim.range.offset } : undefined,
            space: claim.space
          })
        }
      }
      if (claim.type === 'assert/index') {
        this.#knownIndexes.set(contentMultihash(claim), claim.index.multihash)
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
    /** @type {DeferredPromise<void>} */
    const deferred = defer()
    this.#batcher.schedule((work) => {
      work[kind].set(digest, deferred)
    })
    this.#claimFetched[kind].set(digest, deferred.promise)
    return deferred.promise
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
      spaces: [...new Set([...this.#spaces, ...spaces]).values()]
    })
  }

  /**
   * @param {Work} work
   */
  async #processBatch (work) {
    console.log(Object.entries(work).map(([kind, requests]) => `${kind}, ${Array.from(requests.keys()).map((digest) => base58btc.encode(digest.bytes)).join(',')}`))
    for (const [kind, requests] of Object.entries(work)) {
      let nextBatch = []
      const batches = []
      for (const [digest, deferred] of requests.entries()) {
        nextBatch.push({ digest, deferred })
        if (nextBatch.length >= MAX_BATCH_SIZE) {
          batches.push(nextBatch)
          nextBatch = []
        }
      }
      if (nextBatch.length > 0) {
        batches.push(nextBatch)
      }

      for (const batch of batches) {
        const digests = batch.map((next) => next.digest)
        await this.#executeReadClaims(digests, /** @type {Kind} */(kind))
        for (const next of batch) {
          next.deferred.resolve()
        }
      }
    }
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
 * @param {DigestMap<API.MultihashDigest, { digest: API.MultihashDigest, site: T[] }>} cache
 * @param {API.MultihashDigest} digest
 * @param {T} site
 */
const addOrSetLocation = (cache, digest, site) => {
  const location = cache.get(digest)
  if (location) {
    location.site.push(site)
  } else {
    cache.set(digest, {
      digest,
      site: [site]
    })
  }
}
