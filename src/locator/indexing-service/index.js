import { Client } from '@storacha/indexing-service-client'
import { NotFoundError } from '../../lib.js'
import { DigestMap } from '@web3-storage/blob-index'
import * as Digest from 'multiformats/hashes/digest'
import { AssertLocation } from './schemas.js'

// Only imported for type information, but a TS bug prevents us from using
// `@import` for this: https://github.com/microsoft/TypeScript/issues/60563
// eslint-disable-next-line no-unused-vars
import * as API from '../../api.js'

/**
 * @import { z } from 'zod'
 * @import { MultihashDigest } from 'multiformats'
 * @import { Result, DID } from '@ucanto/interface'
 * @import { ShardDigest, Position } from '@web3-storage/blob-index/types'
 */

/**
 * @typedef {Object} LocatorOptions
 * @property {Client} [client] An Indexing Service client instance.
 * @property {DID[]} [spaces] The Spaces to search for the content. If
 * missing, the locator will search all Spaces.
 */

/** @implements {API.Locator} */
export class IndexingServiceLocator {
  #client
  #spaces
  /** @type {DigestMap<MultihashDigest, { shardDigest: ShardDigest; position: Position; }>} */
  #knownSlices
  /** @type {DigestMap<MultihashDigest, z.infer<typeof AssertLocation>>} */
  #knownLocationClaimsCaps

  /**
   * @param {LocatorOptions} [options]
   */
  constructor ({ client, spaces } = {}) {
    this.#client = client ?? new Client()
    this.#spaces = spaces ?? []
    this.#knownSlices = new DigestMap()
    this.#knownLocationClaimsCaps = new DigestMap()
  }

  /** @type {API.Locator['locate']} */
  async locate (digest) {
    // If we don't know about it yet, fetch claims and indexes.
    if (!this.#knownSlices.has(digest)) {
      const result = await this.#client.queryClaims({
        hashes: [digest],
        match: this.#spaces && { subject: this.#spaces }
      })

      if (result.error) return result

      for (const claim of result.ok.claims.values()) {
        for (const cap of claim.capabilities) {
          const result = AssertLocation.safeParse(cap)
          if (result.success) {
            this.#knownLocationClaimsCaps.set(
              Digest.decode(result.data.nb.content.digest),
              result.data
            )
          }
        }
      }

      for (const index of result.ok.indexes.values()) {
        for (const [shardDigest, slices] of index.shards) {
          for (const [sliceDigest, position] of slices) {
            this.#knownSlices.set(sliceDigest, { shardDigest, position })
          }
        }
      }
    }

    // If we still don't know about it, it doesn't exist.
    const slice = this.#knownSlices.get(digest)
    const contentLocationClaim =
      slice && this.#knownLocationClaimsCaps.get(slice.shardDigest)

    if (!contentLocationClaim) return { error: new NotFoundError(digest) }

    return {
      ok: {
        digest,
        site: [
          {
            location: contentLocationClaim.nb.location.map(
              (loc) => new URL(loc)
            ),
            range: { offset: slice.position[0], length: slice.position[1] },
            space: contentLocationClaim.nb.space?.did()
          }
        ]
      }
    }
  }

  /** @type {API.Locator['scopeToSpaces']} */
  scopeToSpaces (spaces) {
    return new IndexingServiceLocator({
      client: this.#client,
      spaces: [...new Set([...this.#spaces, ...spaces]).values()]
    })
  }
}
