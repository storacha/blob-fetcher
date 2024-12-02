import http from 'node:http'
import { MemoryBucket } from '@web3-storage/public-bucket'
import * as Server from '@web3-storage/public-bucket/server/node'
import { base58btc } from 'multiformats/bases/base58'

/** @typedef {{ bucket: MemoryBucket, bucketURL: URL }} BucketServerContext */

/** @param {(assert: import('entail').Assert, ctx: BucketServerContext) => unknown} testfn */
export const withBucketServer = testfn =>
  /** @type {(assert: import('entail').Assert) => unknown} */
  // eslint-disable-next-line
  (async (assert) => {
    const bucket = new MemoryBucket()
    const server = http.createServer(Server.createHandler({ bucket }))
    await new Promise(resolve => server.listen(resolve))
    // @ts-expect-error
    const { port } = server.address()
    const bucketURL = new URL(`http://127.0.0.1:${port}`)
    try {
      await testfn(assert, { bucket, bucketURL })
    } finally {
      server.close()
    }
  })

/** @param {import('multiformats').MultihashDigest} digest */
export const contentKey = (digest) => `${base58btc.encode(digest.bytes)}.blob`
