// eslint-disable-next-line
import * as API from "../../src/api.js";
import http from 'node:http'
import { Writable } from 'node:stream'
import { walkClaims } from '@web3-storage/content-claims/server'
import { DigestMap, ShardedDAGIndex } from '@storacha/blob-index'
import * as Digest from 'multiformats/hashes/digest'
import { base58btc } from 'multiformats/bases/base58'
import { CARWriterStream, code as carCode } from 'carstream'
import { sha256 } from 'multiformats/hashes/sha2'
import * as Link from 'multiformats/link'
import * as Assert from '@storacha/capabilities/assert'
import { extract } from '@storacha/blob-index/sharded-dag-index'
import { from } from '@storacha/indexing-service-client/query-result'
import * as Claim from '@storacha/indexing-service-client/claim'
import * as Delegation from '@ucanto/core/delegation'

/**
 * @import { Claim as LegacyClaim, Kind, ShardedDAGIndexView } from "@storacha/indexing-service-client/api"
 * @import { KnownClaimTypes } from "@web3-storage/content-claims/client/api"
 * @import { Assert as EntailAssert } from 'entail'
 */
/**
 * @typedef {import('@web3-storage/content-claims/server/api').ClaimFetcher} ClaimFetcher
 * @typedef {{ claimsStore: ClaimStorage, claimsURL: URL }} ClaimsServerContext
 * @typedef {{ indexerURL: URL }} IndexingServerContext
 */

/**
 * @template {{}} T
 * @param {(assert: EntailAssert, ctx: T & ClaimsServerContext) => unknown} testfn
 */
export const withClaimsServer = (testfn) =>
  /** @type {(assert: EntailAssert, ctx: T) => unknown} */
  // eslint-disable-next-line no-extra-parens
  (
    async (assert, ctx) => {
      const claimsStore = new ClaimStorage()
      const server = http.createServer((req, res) => {
        const url = new URL(req.url ?? '', claimsURL)
        const digest = Digest.decode(
          base58btc.decode(url.pathname.split('/')[3])
        )
        walkClaims({ claimFetcher: claimsStore }, digest, new Set())
          .pipeThrough(new CARWriterStream())
          .pipeTo(Writable.toWeb(res))
      })
      await new Promise((resolve) => server.listen(resolve))
      // @ts-expect-error
      const { port } = server.address()
      const claimsURL = new URL(`http://127.0.0.1:${port}`)
      try {
        await testfn(assert, { ...ctx, claimsStore, claimsURL })
      } finally {
        server.close()
      }
    }
  )

/**
 * @template {ClaimsServerContext} T
 * @param {(assert: EntailAssert, ctx: T & IndexingServerContext) => unknown} testfn
 */
export const withTestIndexer = (testfn) =>
  /** @type {(assert: EntailAssert, ctx: T) => unknown} */
  // eslint-disable-next-line no-extra-parens
  (
    async (assert, ctx) => {
      const server = http.createServer(async (req, res) => {
        const url = new URL(req.url ?? '', indexerURL)
        const hashes = url.searchParams
          .getAll('multihash')
          .map((hash) => Digest.decode(base58btc.decode(hash)))
        /**
         * @param {string} kindString
         * @returns {kindString is Kind}
         */
        const isKind = (kindString) =>
          ['index_or_location', 'location', 'standard'].includes(kindString)
        const kind = url.searchParams.get('kind') || 'standard'
        if (!isKind(kind)) {
          res.writeHead(400, { 'Content-Type': 'text/plain' })
          res.end('kind must be a string')
          return
        }
        /** @type {{ hash: API.MultihashDigest, kind: Kind, indexForMh?: API.MultihashDigest }[]} */
        const jobs = hashes.map((hash) => ({
          hash,
          kind
        }))
        /** @type {Record<Kind, KnownClaimTypes[]>} */
        const claimMatches = {
          index_or_location: ['assert/location', 'assert/index'],
          location: ['assert/location'],
          standard: ['assert/index', 'assert/location', 'assert/equals'],
          standard_compressed: [
            'assert/index',
            'assert/location',
            'assert/equals'
          ]
        }
        /** @type {LegacyClaim[]} */
        let claims = []
        /** @type {Map<string, ShardedDAGIndexView>} */
        const indexes = new Map()
        for (;;) {
          const job = jobs.shift()
          if (!job) break
          const rawClaims = await ctx.claimsStore.get(job.hash)
          const parsedClaims = []
          for (const rawClaim of rawClaims) {
            const extractRes = await Delegation.extract(rawClaim.bytes)
            if (extractRes.error) {
              throw new Error('failed to extract claim', {
                cause: extractRes.error
              })
            }
            const claim = fromDelegation(extractRes.ok)
            if (claimMatches[job.kind].includes(claim.type)) {
              parsedClaims.push(claim)
            }
          }
          claims = claims.concat(parsedClaims)
          for (const claim of parsedClaims) {
            switch (claim.type) {
              case 'assert/index':
                jobs.push({
                  hash: claim.index.multihash,
                  kind: 'index_or_location',
                  indexForMh: job.hash
                })
                break
              case 'assert/location':
                if (job.indexForMh !== undefined) {
                  /** @type {HeadersInit} */
                  const headers = {}
                  if (claim.range) {
                    headers.Range = `bytes=${claim.range.offset}-${
                      claim.range.length
                        ? claim.range.offset + claim.range.length - 1
                        : ''
                    }`
                  }
                  const indexRes = await fetch(claim.location[0], { headers })
                  if (indexRes.status >= 200 && indexRes.status < 300) {
                    const result = extract(await indexRes.bytes())
                    if (result.ok) {
                      indexes.set(base58btc.encode(job.hash.bytes), result.ok)
                      result.ok.shards.forEach((shardIndex, hash) => {
                        if (job.indexForMh && shardIndex.has(job.indexForMh)) {
                          jobs.push({ hash, kind: 'index_or_location' })
                        }
                      })
                    }
                  }
                }
            }
          }
        }
        const qr = await from({ claims, indexes })
        if (qr.ok) {
          /** @type {ReadableStream<import('carstream/api').Block>} */
          const readable = new ReadableStream({
            async pull (controller) {
              for (const block of qr.ok.iterateIPLDBlocks()) {
                controller.enqueue(block)
              }
              controller.close()
            }
          })
          readable
            .pipeThrough(new CARWriterStream([qr.ok.root.cid]))
            .pipeTo(Writable.toWeb(res))
        }
      })
      await new Promise((resolve) => server.listen(resolve))
      // @ts-expect-error
      const { port } = server.address()
      const indexerURL = new URL(`http://127.0.0.1:${port}`)
      try {
        await testfn(assert, { ...ctx, indexerURL })
      } finally {
        server.close()
      }
    }
  )

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
 * @param {import('@storacha/blob-index/types').ShardedDAGIndex} index
 */
export const generateLocationClaims = async (signer, location, index) => {
  /** @type {import('@web3-storage/content-claims/server/api').Claim[]} */
  const claims = []
  for (const [, slices] of index.shards) {
    for (const [slice, pos] of slices) {
      claims.push(
        await generateLocationClaim(signer, slice, location, pos[0], pos[1])
      )
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
export const generateLocationClaim = async (
  signer,
  digest,
  location,
  offset,
  length
) => {
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
const encode = async (invocation) => {
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
 * @param {import('@storacha/blob-index/types').ShardedDAGIndex} index
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

/** @param {import('@ucanto/interface').Delegation} dlg */
export const fromDelegation = (dlg) => {
  const blocks = new Map([...dlg.export()].map((b) => [String(b.cid), b]))
  return Claim.view({ root: dlg.cid, blocks })
}
