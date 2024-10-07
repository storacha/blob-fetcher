import { sha256 } from 'multiformats/hashes/sha2'
import { fromShardArchives } from '@web3-storage/blob-index/util'
import * as UnixFS from '@ipld/unixfs'
import { CARWriterStream } from 'carstream'
import * as ed25519 from '@ucanto/principal/ed25519'
import { concat } from './stream.js'
import { settings } from './unixfs.js'
import { contentKey } from './bucket.js'
import { generateLocationClaims } from './claims.js'
import { webcrypto } from 'node:crypto'

/** @param {number} size */
export const randomBytes = async (size) => {
  const bytes = new Uint8Array(size)
  while (size) {
    const chunk = new Uint8Array(Math.min(size, 65_536))
    webcrypto.getRandomValues(chunk)
    size -= chunk.length
    bytes.set(chunk, size)
  }
  return bytes
}

/** @param {number} max */
export const randomInt = (max) => Math.floor(Math.random() * max)

/**
 *
 * @param {import('./helpers/context.js').Context} ctx
 * @returns {Promise<{root: Link.Link, fileBytes: Uint8Array}>}
 */
export async function createRandomFile (ctx) {
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

  const signer = await ed25519.generate()
  const index = await fromShardArchives(root, [carBytes])
  const claims = await generateLocationClaims(signer, new URL(contentKey(carDigest), ctx.bucketURL), index)
  for (const claim of claims) {
    ctx.claimsStore.put(claim)
  }
  return {
    root,
    fileBytes
  }
}
