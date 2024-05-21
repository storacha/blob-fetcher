// eslint-disable-next-line
import * as API from '../../src/api.js'
import { equals } from 'multiformats/bytes'
// import { base58btc } from 'multiformats/bases/base58'
import { NotFoundError } from '../../src/lib.js'

/**
 * @param {API.MultihashDigest} blobDigest
 * @param {URL} location
 * @param {import('@web3-storage/blob-index/types').ShardedDAGIndex} index
 */
export const createLocator = (blobDigest, location, index) =>
  /** @type {API.Locator} */
  ({
    async locate (digest) {
      for (const [shard, slices] of index.shards) {
        if (equals(shard.bytes, blobDigest.bytes)) {
          for (const [slice, pos] of slices) {
            if (equals(slice.bytes, digest.bytes)) {
              // console.log('located', base58btc.encode(digest.bytes), '=>', location.toString(), pos)
              return {
                ok: {
                  digest,
                  site: [{
                    location: [location],
                    range: { offset: pos[0], length: pos[1] }
                  }]
                }
              }
            }
          }
        }
      }
      return { error: new NotFoundError(digest) }
    }
  })
