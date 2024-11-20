import { z } from 'zod'
import * as DID from '@ipld/dag-ucan/did'
import { CID } from 'multiformats/cid'
import { Client } from '@storacha/indexing-service-client'
import * as API from '../api.js'
import { NetworkError } from '../lib.js'
import { equals } from 'multiformats/bytes'
import { DigestMap } from '@web3-storage/blob-index'

/**
 * @import { MultihashDigest } from 'multiformats'
 * @import { Result, Principal } from '@ucanto/interface'
 * @import * as UCAN from "@ipld/dag-ucan"
 * @import { ShardDigest, SliceDigest, Position } from '@web3-storage/blob-index/types'
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

  /**
   * @param {LocatorOptions} [options]
   */
  constructor({ serviceURL, spaces, fetch } = {}) {
    this.#client = new Client({ serviceURL, fetch })
    this.#spaces = spaces
  }

  /** @type {API.Locator['locate']} */
  async locate(digest) {
    const result = await this.#client.queryClaims({
      hashes: [digest],
      match: this.#spaces && { subject: this.#spaces }
    })

    // TK: What to do with errors that `locate()` doesn't know about?
    if (result.error) throw result.error

    // TK: Have we validated the claims?

    const claimCapabilities = [...result.ok.claims.values()].flatMap(
      (claim) => claim.capabilities
    )

    const indexClaim = claimCapabilities
      .map((cap) => {
        const result = AssertIndex.safeParse(cap)
        return result.success && result.data
      })
      .find(
        (parsed) =>
          parsed && equals(parsed.nb.content.multihash.bytes, digest.bytes)
      )

    if (!indexClaim) throw 'TK'

    const index = result.ok.indexes.get(indexClaim.nb.index.toString())
    if (!index) throw 'TK'

    const slices = new DigestMap(
      [...index.shards.entries()].flatMap(([shardDigest, shard]) =>
        [...shard.entries()].map(
          ([sliceDigest, position]) =>
            /** @type {[SliceDigest, {shardDigest: ShardDigest, position: Position}]} */ ([
              sliceDigest,
              {
                shardDigest,
                position
              }
            ])
        )
      )
    )

    const { shardDigest, position } = slices.get(digest) || {}

    if (!shardDigest || !position) throw 'TK'

    const contentLocationClaim = claimCapabilities
      .map((cap) => {
        const result = AssertLocation.safeParse(cap)
        return result.success && result.data
      })
      .find(
        (parsed) =>
          parsed && equals(parsed.nb.content.digest, shardDigest.bytes)
      )

    if (!contentLocationClaim) throw 'TK'

    /** @type {import('../api.js').Site[]} */
    const site = [
      {
        location: contentLocationClaim.nb.location.map((loc) => new URL(loc)),
        range: { offset: position[0], length: position[1] },
        space: contentLocationClaim.nb.space?.did()
      }
    ]

    return { ok: { digest, site } }
  }
}
