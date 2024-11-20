import * as fs from 'node:fs'
import * as path from 'node:path'
import { base58btc } from 'multiformats/bases/base58'
import * as Digest from 'multiformats/hashes/digest'
import { IndexingServiceLocator } from '../../src/locator/indexing-service.js'

/**
 * @import { Suite } from 'entail'
 */

// Overcome a bug in uvu: While printing an object diff for a failure message,
// uvu will copy properties off objects to plain Objects, which then breaks
// `URL`'s `toJSON()`, which insists on being called on a real `URL`.
//
// This monkey-patch notices that `toJSON()` is being called on a plain Object,
// and reconstructs a real `URL` to call the original `toJSON()` on.
const originalToJSON = URL.prototype.toJSON
URL.prototype.toJSON = function () {
  if (this.constructor === Object) {
    return originalToJSON.call(new URL(this.href))
  } else {
    return originalToJSON.call(this)
  }
}

/** @type {Suite} */
export const testIndexingServiceLocator = {
  'can locate a single Slice': async (assert) => {
    const digestString = 'zQmRm3SMS4EbiKYy7VeV3zqXqzyr76mq9b2zg3Tij3VhKUG'
    const digest = Digest.decode(base58btc.decode(digestString))
    const fixturePath = path.join(
      import.meta.dirname,
      '..',
      'fixtures',
      `${digestString}.queryresult.car`
    )
    const responseData = await fs.promises.readFile(fixturePath)

    const locator = new IndexingServiceLocator({
      fetch: async (requested) => {
        if (
          requested.toString() ===
          'https://indexing.storacha.network/claims?multihash=zQmRm3SMS4EbiKYy7VeV3zqXqzyr76mq9b2zg3Tij3VhKUG'
        ) {
          return new Response(responseData)
        }
        throw new Error(`Unexpected request: ${requested}`)
      }
    })

    const result = await locator.locate(digest)

    assert.deepEqual(result, {
      ok: {
        digest,
        site: [
          {
            location: [new URL('http://127.0.0.1:3294/')],
            range: { offset: 96, length: 32 },
            space: 'did:key:z6MknPSFymrj8qjrmiMiRFR2S1xWHM6tazqFEKN1udEtyu9W'
          }
        ]
      }
    })
  }
}
