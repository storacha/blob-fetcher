// eslint-disable-next-line
import * as API from '../../src/api.js'
import http from 'node:http'
import { Writable } from 'node:stream'
import { walkClaims } from '@web3-storage/content-claims/server'
import { DigestMap, ShardedDAGIndex } from '@web3-storage/blob-index'
import * as Digest from 'multiformats/hashes/digest'
import { base58btc } from 'multiformats/bases/base58'
import { CARWriterStream, code as carCode } from 'carstream'
import { sha256 } from 'multiformats/hashes/sha2'
import * as Link from 'multiformats/link'
import { Assert } from '@web3-storage/content-claims/capability'

/**
 * @typedef {import('@web3-storage/content-claims/server/api').ClaimFetcher} ClaimFetcher
 * @typedef {{ claimsStore: ClaimStorage, claimsURL: URL }} ClaimsServerContext
 */

/**
 * @template {{}} T
 * @param {(assert: import('entail').Assert, ctx: T & ClaimsServerContext) => unknown} testfn
 */
export const withClaimsServer = testfn =>
  /** @type {(assert: import('entail').Assert, ctx: T) => unknown} */
  // eslint-disable-next-line no-extra-parens
  (async (assert, ctx) => {
    const claimsStore = new ClaimStorage()
    const server = http.createServer((req, res) => {
      const url = new URL(req.url ?? '', claimsURL)
      const digest = Digest.decode(base58btc.decode(url.pathname.split('/')[3]))
      walkClaims({ claimFetcher: claimsStore }, digest, new Set())
        .pipeThrough(new CARWriterStream())
        .pipeTo(Writable.toWeb(res))
    })
    await new Promise(resolve => server.listen(resolve))
    // @ts-expect-error
    const { port } = server.address()
    const claimsURL = new URL(`http://127.0.0.1:${port}`)
    try {
      await testfn(assert, { ...ctx, claimsStore, claimsURL })
    } finally {
      server.close()
    }
  })

/** @implements {ClaimFetcher} */
class ClaimStorage {
  constructor () {
    /** @type {DigestMap<API.MultihashDigest, import('@web3-storage/content-claims/server/api').Claim[]>} */
    this.data = new DigestMap()
  }

  /** @param {import('@web3-storage/content-claims/server/api').Claim} claim */
  async put (claim) {
    const claims = this.data.get(claim.content) ?? []
    claims.push(claim)
    this.data.set(claim.content, claims)
  }

  /** @param {API.MultihashDigest} content */
  async get (content) {
    return this.data.get(content) ?? []
  }
}

/**
 * @param {import('@ucanto/interface').Signer} signer
 * @param {URL} location
 * @param {import('@web3-storage/blob-index/types').ShardedDAGIndex} index
 */
export const generateLocationClaims = async (signer, location, index) => {
  /** @type {import('@web3-storage/content-claims/server/api').Claim[]} */
  const claims = []
  for (const [, slices] of index.shards) {
    for (const [slice, pos] of slices) {
      claims.push(await generateLocationClaim(signer, slice, location, pos[0], pos[1]))
    }
  }
  return claims
}

/**
 * @param {import('@ucanto/interface').Signer} signer
 * @param {API.MultihashDigest} digest
 * @param {URL} location
 * @param {number} offset
 * @param {number} length
 */
export const generateLocationClaim = async (signer, digest, location, offset, length) => {
  const invocation = Assert.location.invoke({
    issuer: signer,
    audience: signer,
    with: signer.did(),
    nb: {
      content: { digest: digest.bytes },
      location: [
        /** @type {API.URI} */
        (location.toString())
      ],
      range: { offset, length }
    }
  })
  const block = await encode(invocation)
  return {
    claim: block.cid,
    bytes: block.bytes,
    content: digest,
    value: block.value
  }
}

/**
 * Encode a claim to a block.
 * @param {import('@ucanto/interface').IssuedInvocation<import('@web3-storage/content-claims/server/api').AnyAssertCap>} invocation
 */
const encode = async invocation => {
  const view = await invocation.buildIPLDView()
  const bytes = await view.archive()
  if (bytes.error) throw new Error('failed to archive')
  return {
    cid: Link.create(carCode, await sha256.digest(bytes.ok)),
    bytes: bytes.ok,
    value: invocation.capabilities[0]
  }
}

/**
 * @param {import('@ucanto/interface').Signer} signer
 * @param {import('multiformats').UnknownLink} content
 * @param {import('@web3-storage/blob-index/types').ShardedDAGIndex} index
 */
export const generateIndexClaim = async (signer, content, index) => {
  const res = await ShardedDAGIndex.archive(index)
  if (!res.ok) throw new Error('failed to archive')

  const indexLink = Link.create(carCode, await sha256.digest(res.ok))
  const invocation = Assert.index.invoke({
    issuer: signer,
    audience: signer,
    with: signer.did(),
    nb: { content, index: indexLink }
  })
  const block = await encode(invocation)
  return {
    claim: block.cid,
    bytes: block.bytes,
    content: content.multihash,
    value: block.value
  }
}
