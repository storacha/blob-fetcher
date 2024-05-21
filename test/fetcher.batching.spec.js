// eslint-disable-next-line
import * as API from '../src/api.js'
import { sha256 } from 'multiformats/hashes/sha2'
import { equals } from 'multiformats/bytes'
import { fromShardArchives } from '@web3-storage/blob-index/util'
import { CARWriterStream } from 'carstream'
import { exporter } from 'ipfs-unixfs-exporter'
import * as BatchingFetcher from '../src/fetcher/batching.js'
import { randomBytes } from './helpers/random.js'
import { concat } from './helpers/stream.js'
import * as UnixFS from './helpers/unixfs.js'
import { contentKey, withBucketServer } from './helpers/bucket.js'
import { asBlockstore } from './helpers/unixfs-exporter.js'
import { patchFetch } from './helpers/fetch.js'
import { createLocator } from './helpers/locator.js'

patchFetch(6) // simulates cloudflare worker environment with max 6 concurrent reqs

export const testSimpleBlobFetcher = {
  'skip should fetch a file': withBucketServer(async (/** @type {import('entail').assert} assert */ assert, ctx) => {
    const fileBytes = await randomBytes(10 * 1024 * 1024)

    const { readable, writable } = new TransformStream({}, UnixFS.withCapacity(1048576 * 32))
    const writer = UnixFS.createWriter({ writable, settings: UnixFS.settings })

    const [root, carBytes] = await Promise.all([
      (async () => {
        const file = UnixFS.createFileWriter(writer)
        file.write(fileBytes)
        const { cid } = await file.close()
        writer.close()
        return cid
      })(),
      concat(readable.pipeThrough(new CARWriterStream()))
    ])
    const carDigest = await sha256.digest(carBytes)

    ctx.bucket.put(contentKey(carDigest), carBytes)

    const index = await fromShardArchives(root, [carBytes])
    const locator = createLocator(carDigest, new URL(contentKey(carDigest), ctx.bucketURL), index)
    const fetcher = BatchingFetcher.create(locator)
    const blockstore = asBlockstore(fetcher)

    // @ts-expect-error exporter expects instance not interface
    const exportedBytes = await concat((await exporter(root, blockstore)).content())
    assert.ok(equals(exportedBytes, fileBytes))
  }),

  'skip should benchmark 500MB': withBucketServer(async (/** @type {import('entail').assert} assert */ assert, ctx) => {
    const fileBytes = await randomBytes(500 * 1024 * 1024)

    const { readable, writable } = new TransformStream({}, UnixFS.withCapacity(1048576 * 32))
    const writer = UnixFS.createWriter({ writable, settings: UnixFS.settings })

    const [root, carBytes] = await Promise.all([
      (async () => {
        const file = UnixFS.createFileWriter(writer)
        file.write(fileBytes)
        const { cid } = await file.close()
        writer.close()
        return cid
      })(),
      concat(readable.pipeThrough(new CARWriterStream()))
    ])
    const carDigest = await sha256.digest(carBytes)

    ctx.bucket.put(contentKey(carDigest), carBytes)

    const index = await fromShardArchives(root, [carBytes])
    const locator = createLocator(carDigest, new URL(contentKey(carDigest), ctx.bucketURL), index)
    const fetcher = BatchingFetcher.create(locator)
    const blockstore = asBlockstore(fetcher)

    console.time('export 500MiB (batching)')
    // @ts-expect-error exporter expects instance not interface
    const exportedBytes = await concat((await exporter(root, blockstore)).content())
    assert.ok(equals(exportedBytes, fileBytes))
    console.timeEnd('export 500MiB (batching)')
  })
}
