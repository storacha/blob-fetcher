// eslint-disable-next-line
import * as API from '../src/api.js'
import * as Link from 'multiformats/link'
import * as raw from 'multiformats/codecs/raw'
import { sha256 } from 'multiformats/hashes/sha2'
import { equals } from 'multiformats/bytes'
import { fromShardArchives } from '@web3-storage/blob-index/util'
import * as UnixFS from '@ipld/unixfs'
import { CARWriterStream } from 'carstream'
import { exporter } from 'ipfs-unixfs-exporter'
import * as SimpleFetcher from '../src/fetcher/simple.js'
import * as BatchingFetcher from '../src/fetcher/batching.js'
import { randomBytes, randomInt } from './helpers/random.js'
import { concat } from './helpers/stream.js'
import { settings } from './helpers/unixfs.js'
import { contentKey, withBucketServer } from './helpers/bucket.js'
import { asBlockstore } from './helpers/unixfs-exporter.js'
import { patchFetch } from './helpers/fetch.js'
import { createLocator } from './helpers/locator.js'

// simulates cloudflare worker environment with max 6 concurrent reqs
patchFetch({ concurrency: 6, lag: 50 })

export const testFetcher = {}

;[
  { name: 'simple', FetcherFactory: SimpleFetcher },
  { name: 'batching', FetcherFactory: BatchingFetcher }
].forEach(({ name, FetcherFactory }) => {
  testFetcher[name] = {
    'should fetch a file': withBucketServer(async (/** @type {import('entail').assert} assert */ assert, ctx) => {
      const fileBytes = await randomBytes(10 * 1024 * 1024)

      const { readable, writable } = new TransformStream({}, UnixFS.withCapacity(1048576 * 32))
      const writer = UnixFS.createWriter({ writable, settings })

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
      const fetcher = FetcherFactory.create(locator)
      const blockstore = asBlockstore(fetcher)

      // @ts-expect-error exporter expects instance not interface
      const exportedBytes = await concat((await exporter(root, blockstore)).content())
      assert.ok(equals(exportedBytes, fileBytes))
    }),

    'should fetch bytes with range': withBucketServer(async (/** @type {import('entail').assert} assert */ assert, ctx) => {
      const bytes = await randomBytes(1 * 1024 * 1024)
      const root = Link.create(raw.code, await sha256.digest(bytes))

      const readable = new ReadableStream({
        pull (controller) {
          controller.enqueue({ cid: root, bytes })
          controller.close()
        }
      })

      const carBytes = await concat(readable.pipeThrough(new CARWriterStream([root])))
      const carDigest = await sha256.digest(carBytes)

      const index = await fromShardArchives(root, [carBytes])
      const locator = createLocator(carDigest, new URL(contentKey(carDigest), ctx.bucketURL), index)
      const fetcher = FetcherFactory.create(locator)

      ctx.bucket.put(contentKey(carDigest), carBytes)

      const first = randomInt(bytes.length / 2)
      const last = first + randomInt(bytes.length / 2) - 1
      const range = /** @type {API.AbsoluteRange} */ ([first, last])
      const res = await fetcher.fetch(root.multihash, { range })

      assert.ok(res.ok)
      assert.deepEqual(await res.ok.bytes(), bytes.slice(range[0], range[1] + 1))
    }),

    'should benchmark 500MB': withBucketServer(async (/** @type {import('entail').assert} assert */ assert, ctx) => {
      const fileBytes = await randomBytes(500 * 1024 * 1024)

      const { readable, writable } = new TransformStream({}, UnixFS.withCapacity(1048576 * 32))
      const writer = UnixFS.createWriter({ writable, settings })

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
      const fetcher = FetcherFactory.create(locator)
      const blockstore = asBlockstore(fetcher)

      console.time(`export 500MiB (${name})`)
      // @ts-expect-error exporter expects instance not interface
      const exportedBytes = await concat((await exporter(root, blockstore)).content())
      assert.ok(equals(exportedBytes, fileBytes))
      console.timeEnd(`export 500MiB (${name})`)
    })
  }
})
