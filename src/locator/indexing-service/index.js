import { Client } from '@storacha/indexing-service-client'
import { NotFoundError } from '../../lib.js'
import { DigestMap } from '@web3-storage/blob-index'
import * as Digest from 'multiformats/hashes/digest'
import { AssertLocation } from './schemas.js'

/**
 * @import { z } from 'zod'
 * @import { MultihashDigest } from 'multiformats'
 * @import { Result, Principal } from '@ucanto/interface'
 * @import { ShardDigest, Position } from '@web3-storage/blob-index/types'
 * @import * as API from '../../api.js'
 */

/**
 * @typedef {Object} LocatorOptions
 * @property {URL} [serviceURL] The URL of the Indexing Service.
 * @property {Principal[]} [spaces] The Spaces to search for the content. If
 * missing, the locator will search all Spaces.
 * @property {typeof globalThis.fetch} [fetch] The fetch function to use for
 * HTTP requests. Defaults to `globalThis.fetch`.
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
  constructor ({ serviceURL, spaces, fetch } = {}) {
    this.#client = new Client({ serviceURL, fetch })
    this.#spaces = spaces
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

      // TK: What to do with errors that `locate()` doesn't know about?
      if (result.error) throw new Error('TK')

      // TK: Have we validated the claims?

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
}
