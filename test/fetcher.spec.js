// eslint-disable-next-line
import * as API from '../src/api.js'
import * as Link from 'multiformats/link'
import * as raw from 'multiformats/codecs/raw'
import { sha256 } from 'multiformats/hashes/sha2'
import { equals } from 'multiformats/bytes'
import { ShardedDAGIndex } from '@web3-storage/blob-index'
import { fromShardArchives } from '@web3-storage/blob-index/util'
import * as UnixFS from '@ipld/unixfs'
import { CARWriterStream } from 'carstream'
import { exporter } from 'ipfs-unixfs-exporter'
import * as ed25519 from '@ucanto/principal/ed25519'
import * as SimpleFetcher from '../src/fetcher/simple.js'
import * as BatchingFetcher from '../src/fetcher/batching.js'
import * as ContentClaimsLocator from '../src/locator/content-claims.js'
import { randomBytes, randomInt, createRandomFile } from './helpers/random.js'
import { concat } from './helpers/stream.js'
import { settings } from './helpers/unixfs.js'
import { contentKey } from './helpers/bucket.js'
import { generateIndexClaim, generateLocationClaim, generateLocationClaims } from './helpers/claims.js'
import { asBlockstore } from './helpers/unixfs-exporter.js'
import * as Fetch from './helpers/fetch.js'
import * as Result from './helpers/result.js'
import { withContext } from './helpers/context.js'

// simulates cloudflare worker environment with max 6 concurrent reqs
Fetch.patch({ concurrency: 6, lag: 50 })

export const testFetcher = {}

const typesOfFetchers = [
  { name: 'simple', FetcherFactory: SimpleFetcher },
  { name: 'batching', FetcherFactory: BatchingFetcher }
]

typesOfFetchers.forEach(({ name, FetcherFactory }) => {
  testFetcher[name] = {
    'should fetch a file': withContext(async (/** @type {import('entail').assert} assert */ assert, ctx) => {
      const { root, fileBytes } = await createRandomFile(ctx)

      const locator = ContentClaimsLocator.create({ serviceURL: ctx.claimsURL })
      const fetcher = FetcherFactory.create(locator)
      const blockstore = asBlockstore(fetcher)

      // @ts-expect-error exporter expects instance not interface
      const exportedBytes = await concat((await exporter(root, blockstore)).content())
      assert.ok(equals(exportedBytes, fileBytes))
    }),

    'should fetch bytes with range': withContext(async (/** @type {import('entail').assert} assert */ assert, ctx) => {
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

      const signer = await ed25519.generate()
      const index = await fromShardArchives(root, [carBytes])
      const claims = await generateLocationClaims(signer, new URL(contentKey(carDigest), ctx.bucketURL), index)
      for (const claim of claims) {
        ctx.claimsStore.put(claim)
      }

      const locator = ContentClaimsLocator.create({ serviceURL: ctx.claimsURL })
      const fetcher = FetcherFactory.create(locator)

      ctx.bucket.put(contentKey(carDigest), carBytes)

      const first = randomInt(bytes.length / 2)
      const last = first + randomInt(bytes.length / 2) - 1
      const range = /** @type {API.AbsoluteRange} */ ([first, last])
      const blob = Result.unwrap(await fetcher.fetch(root.multihash, { range }))

      assert.deepEqual(await blob.bytes(), bytes.slice(range[0], range[1] + 1))
    }),

    'should benchmark 500MB': withContext(async (/** @type {import('entail').assert} assert */ assert, ctx) => {
      Fetch.resetCount()
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

      const signer = await ed25519.generate()
      const index = await fromShardArchives(root, [carBytes])
      const claims = await generateLocationClaims(signer, new URL(contentKey(carDigest), ctx.bucketURL), index)
      for (const claim of claims) {
        ctx.claimsStore.put(claim)
      }

      const locator = ContentClaimsLocator.create({ serviceURL: ctx.claimsURL })
      const fetcher = FetcherFactory.create(locator)
      const blockstore = asBlockstore(fetcher)

      console.time(`export 500MiB (${name})`)
      // @ts-expect-error exporter expects instance not interface
      const exportedBytes = await concat((await exporter(root, blockstore)).content())
      assert.ok(equals(exportedBytes, fileBytes))
      console.timeEnd(`export 500MiB (${name})`)
      console.log(`sub-requests: ${Fetch.count()}`)
    }),

    'should benchmark 500MB with index': withContext(async (/** @type {import('entail').assert} assert */ assert, ctx) => {
      Fetch.resetCount()
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

      const signer = await ed25519.generate()
      const index = await fromShardArchives(root, [carBytes])
      index.shards.get(carDigest)?.set(carDigest, [0, carBytes.length])

      const indexBytes = Result.try(await ShardedDAGIndex.archive(index))
      const indexDigest = await sha256.digest(indexBytes)

      const blobClaims = await generateLocationClaims(signer, new URL(contentKey(carDigest), ctx.bucketURL), index)
      for (const claim of blobClaims) {
        ctx.claimsStore.put(claim)
      }

      ctx.bucket.put(contentKey(indexDigest), indexBytes)
      ctx.claimsStore.put(await generateLocationClaim(signer, indexDigest, new URL(contentKey(indexDigest), ctx.bucketURL), 0, indexBytes.length))
      ctx.claimsStore.put(await generateIndexClaim(signer, root, index))

      const locator = ContentClaimsLocator.create({ serviceURL: ctx.claimsURL })
      const fetcher = FetcherFactory.create(locator)
      const blockstore = asBlockstore(fetcher)

      console.time(`export 500MiB with index (${name})`)
      // @ts-expect-error exporter expects instance not interface
      const exportedBytes = await concat((await exporter(root, blockstore)).content())
      assert.ok(equals(exportedBytes, fileBytes))
      console.timeEnd(`export 500MiB with index (${name})`)
      console.log(`sub-requests: ${Fetch.count()}`)
    })
  }
})
