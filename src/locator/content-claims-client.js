import { DigestMap, ShardedDAGIndex } from '@storacha/blob-index'
import * as Assert from '@storacha/capabilities/assert'
import * as LegacyClaims from '@web3-storage/content-claims/client'
import { withSimpleSpan } from '../tracing/tracing.js'
import * as ed25519 from '@ucanto/principal/ed25519'
import { base58btc } from 'multiformats/bases/base58'
import { NotFoundError } from '../lib.js'
import * as Claim from '@storacha/indexing-service-client/claim'
import { from } from '@storacha/indexing-service-client/query-result'

/**
 * @import * as API from '../api.js'
 * @import { IndexingServiceQueryClient, Query, QueryError, QueryOk, Result, Kind } from '@storacha/indexing-service-client/api'
 * @import { Claim as LegacyClaim, KnownClaimTypes, LocationClaim } from '@web3-storage/content-claims/client/api'
 * @import { R2Bucket } from '@cloudflare/workers-types'
 */

/**
 * Legacy claims are wire compatible but location claims use `Schema.didBytes`
 * instead of `Schema.principal`.
 *
 * @param {LegacyClaim} lc
 */
const fromLegacyClaim = lc => {
  const dlg = lc.delegation()
  const blocks = new Map([...dlg.export()].map(b => [String(b.cid), b]))
  return Claim.view({ root: dlg.cid, blocks })
}

/**
 * @typedef {{ serviceURL?: URL, carpark?: R2Bucket, carparkPublicBucketURL?: URL}} LocatorOptions
 */

/**
 * ContentClaimsClient mimics the indexing service client using the content claims service
 * @implements {IndexingServiceQueryClient}
 */
export class ContentClaimsClient {
  /**
   * @type {DigestMap<API.MultihashDigest, boolean>}
   */
  #indexCids

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
   * @type {ed25519.EdSigner | undefined}
   */
  #signer

  /**
   * @param {LocatorOptions} [options]
   */
  constructor (options) {
    this.#indexCids = new DigestMap()
    this.#serviceURL = options?.serviceURL
    this.#carpark = options?.carpark
    this.#carparkPublicBucketURL = options?.carparkPublicBucketURL
  }

  async #getSigner () {
    if (!this.#signer) {
      this.#signer = await ed25519.Signer.generate()
    }
    return this.#signer
  }

  /**
   * @param {Query} q
   * @returns {Promise<Result<QueryOk, QueryError>>}
   */
  async queryClaims (q) {
    /** @type {LegacyClaim[]} */
    const claims = []
    /** @type {Map<string, import('@storacha/blob-index/types').ShardedDAGIndexView>} */
    const indexes = new Map()
    const kind = q.kind || 'standard'
    for (const digest of q.hashes) {
      const digestClaims = (await LegacyClaims.read(digest, { serviceURL: this.#serviceURL })).filter((claim) => allowedClaimTypes[kind].includes(claim.type))
      let indexBytes
      if (digestClaims.length === 0) {
        const backups = await this.#carparkBackup(digest)
        if (backups) {
          claims.push(backups.claim)
          indexBytes = backups.indexBytes
        }
      } else {
        claims.push(...digestClaims)
        for (const claim of digestClaims) {
          if (claim.type === 'assert/index') {
            this.#indexCids.set(claim.index.multihash, true)
          }
          if (claim.type === 'assert/location' && this.#indexCids.has(LegacyClaims.contentMultihash(claim))) {
            try {
              const fetchRes = await fetchIndex(claim)
              indexBytes = await fetchRes.bytes()
            } catch (err) {
              console.warn('unable to fetch index', err instanceof Error ? err.message : 'unknown error')
            }
          }
        }
      }
      if (indexBytes) {
        const decodeRes = ShardedDAGIndex.extract(indexBytes)
        if (decodeRes.error) {
          console.warn('failed to decode index', decodeRes.error)
          continue
        }
        indexes.set(base58btc.encode(digest.bytes), decodeRes.ok)
      }
    }
    return /** @type {Result<QueryOk, QueryError>} */ (
      await from({ claims: claims.map(fromLegacyClaim), indexes })
    )
  }

  /**
   *
   * @param {*} digest
   * @returns
   */
  async #carparkBackup (digest) {
    let indexBytes
    if (this.#carpark === undefined || this.#carparkPublicBucketURL === undefined) {
      return
    }
    if (this.#indexCids.has(digest)) {
      const obj = await withSimpleSpan('carPark.get', this.#carpark.get, this.#carpark)(toBlobKey(digest))
      if (!obj) {
        return
      }
      indexBytes = new Uint8Array(await obj.arrayBuffer())
    } else {
      const obj = await this.#carpark.head(toBlobKey(digest))
      if (!obj) {
        return
      }
    }
    return {
      claim: await LegacyClaims.decodeDelegation(await Assert.location
        .invoke({
          issuer: await this.#getSigner(),
          audience: await this.#getSigner(),
          with: (await this.#getSigner()).did(),
          nb: {
            content: { digest: digest.bytes },
            location: [
            /** @type {API.URI<import('@ucanto/principal/ed25519').Protocol>} */((new URL(toBlobKey(digest), this.#carparkPublicBucketURL)).href)
            ]
          }
        })
        .delegate()),
      indexBytes
    }
  }
}

/** @type {Record<Kind, Array<KnownClaimTypes | "unknown">>} */
const allowedClaimTypes = {
  standard: ['assert/location', 'assert/partition', 'assert/inclusion', 'assert/index', 'assert/equals', 'assert/relation'],
  index_or_location: ['assert/location', 'assert/index'],
  location: ['assert/location']
}

/** @param {import('multiformats').MultihashDigest} digest */
const toBlobKey = digest => {
  const mhStr = base58btc.encode(digest.bytes)
  return `${mhStr}/${mhStr}.blob`
}

const fetchIndex = withSimpleSpan('fetchIndex',
  /**
 * Fetch a blob from the passed location.
 * @param {LocationClaim} locationClaim
 */
  async (locationClaim) => {
    for (const uri of locationClaim.location) {
      const url = new URL(uri)
      /** @type {HeadersInit} */
      const headers = {}
      if (locationClaim.range) {
        headers.Range = `bytes=${locationClaim.range.offset}-${
            locationClaim.range.length ? locationClaim.range.offset + locationClaim.range.length - 1 : ''}`
      }
      const res = await fetch(url, { headers })
      if (!res.ok || !res.body) {
        console.warn(`failed to fetch ${url}: ${res.status} ${await res.text()}`)
        continue
      }
      return res
    }

    throw new NotFoundError(LegacyClaims.contentMultihash(locationClaim))
  }
)
