import { z } from 'zod'
import * as DID from '@ipld/dag-ucan/did'
import { CID } from 'multiformats/cid'
import { Client } from '@storacha/indexing-service-client'
import * as API from '../api.js'
import { NetworkError } from '../lib.js'
import { DigestMap } from '@web3-storage/blob-index'
import * as Digest from 'multiformats/hashes/digest'

/**
 * @import { MultihashDigest } from 'multiformats'
 * @import { Result, Principal } from '@ucanto/interface'
 * @import * as UCAN from "@ipld/dag-ucan"
 */

/**
 * Zod schema for the bytes of a DID. Transforms to a
 * {@link UCAN.PrincipalView<ID>}
 */
const DIDBytes = z.instanceof(Uint8Array).transform((bytes, ctx) => {
  try {
    return DID.decode(bytes)
  } catch (error) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'Invalid DID encoding.',
      params: { decodeError: error }
    })
    return z.NEVER
  }
})

const CIDObject = z.unknown().transform((cid, ctx) => {
  const cidOrNull = CID.asCID(cid)
  if (cidOrNull) {
    return cidOrNull
  } else {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'Invalid CID.'
    })
    return z.NEVER
  }
})

/**
 * Zod schema for a location assertion capability.
 */
const AssertLocation = z.object({
  can: z.literal('assert/location'),
  nb: z.object({
    // TK: Actually, a full link is allowed here by the (other) schema.
    content: z.object({ digest: z.instanceof(Uint8Array) }),
    location: z.array(z.string()),
    space: DIDBytes.optional()
  })
})

/**
 * Zod schema for an index assertion capability.
 */
const AssertIndex = z.object({
  can: z.literal('assert/index'),
  with: z.unknown(),
  nb: z.object({
    content: CIDObject,
    index: CIDObject
  })
})

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
  #knownSlices
  /** @type {DigestMap<MultihashDigest, z.infer<typeof AssertLocation>>} */
  #knownLocationClaimsCaps

  /**
   * @param {LocatorOptions} [options]
   */
  constructor({ serviceURL, spaces, fetch } = {}) {
    this.#client = new Client({ serviceURL, fetch })
    this.#spaces = spaces
    this.#knownSlices = new DigestMap()
    this.#knownLocationClaimsCaps = new DigestMap()
  }

  /** @type {API.Locator['locate']} */
  async locate(digest) {
    if (!this.#knownSlices.has(digest)) {
      const result = await this.#client.queryClaims({
        hashes: [digest],
        match: this.#spaces && { subject: this.#spaces }
      })

      // TK: What to do with errors that `locate()` doesn't know about?
      if (result.error) throw result.error

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

    const { shardDigest, position } = this.#knownSlices.get(digest) || {}
    if (!shardDigest || !position) throw new Error('TK')

    const contentLocationClaim = this.#knownLocationClaimsCaps.get(shardDigest)
    if (!contentLocationClaim) throw new Error('TK')

    return {
      ok: {
        digest,
        site: [
          {
            location: contentLocationClaim.nb.location.map(
              (loc) => new URL(loc)
            ),
            range: { offset: position[0], length: position[1] },
            space: contentLocationClaim.nb.space?.did()
          }
        ]
      }
    }
  }
}
