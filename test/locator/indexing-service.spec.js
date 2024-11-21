import * as fs from 'node:fs'
import * as path from 'node:path'
import { base58btc } from 'multiformats/bases/base58'
import * as Digest from 'multiformats/hashes/digest'
import * as ed25519 from '@ucanto/principal/ed25519'
import * as QueryResult from '@storacha/indexing-service-client/query-result'
import { Assert } from '@web3-storage/content-claims/capability'
import { createTestCID } from '../util/createTestCID.js'
import { ShardedDAGIndex } from '@web3-storage/blob-index'
import { IndexingServiceLocator } from '../../src/locator/indexing-service/index.js'

/**
 * @import { Suite, Result, Assert as AssertObj } from 'entail'
 * @import { Await } from '@ipld/dag-ucan'
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

/**
 * Assert that the given result is an OK result, and return the value.
 * @template {{}} T
 * @param {Result<T, {}>} result
 * @param {AssertObj} assert
 * @returns {asserts result is { ok: T }}
 */
const assertResultOk = (result, assert) => {
  if (result.error) {
    assert.fail(new Error('Result was an error', { cause: result.error }))
  }
}

/**
 * Create a stub fetch function that responds to URLs with the given responses.
 * @param {Record<string, () => Await<BodyInit | null>>} responses
 * @returns {typeof globalThis.fetch}
 */
function stubFetch (responses) {
  return async (requested) => {
    const responseData = responses[requested.toString()]
    if (responseData) {
      return new Response(await responseData())
    } else {
      throw new Error(`Unexpected request: ${requested}`)
    }
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

    const locator = new IndexingServiceLocator({
      fetch: stubFetch({
        'https://indexing.storacha.network/claims?multihash=zQmRm3SMS4EbiKYy7VeV3zqXqzyr76mq9b2zg3Tij3VhKUG':
          () => fs.promises.readFile(fixturePath)
      })
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
  },
  'caches all the information it receives': async (assert) => {
    const content1Link = createTestCID('content1')
    // content2Link is in the same shard, but a different slice
    const content2Link = createTestCID('content2')
    // content3Link is in a different shard, but the same index
    const content3Link = createTestCID('content3')
    // content4Link is in a different index
    const content4Link = createTestCID('content4')

    const index1 = ShardedDAGIndex.create(content1Link)
    const shard1Link = createTestCID('shard1')
    const shard2Link = createTestCID('shard2')
    index1.setSlice(shard1Link.multihash, content1Link.multihash, [110, 120])
    index1.setSlice(shard1Link.multihash, content2Link.multihash, [210, 220])
    index1.setSlice(shard2Link.multihash, content3Link.multihash, [310, 320])

    const index2 = ShardedDAGIndex.create(content4Link)
    const shard3Link = createTestCID('shard3')
    index2.setSlice(shard3Link.multihash, content4Link.multihash, [410, 420])

    const indexingService = await ed25519.Signer.generate()
    const queryResultResult = await QueryResult.from({
      claims: [
        await Assert.location
          .invoke({
            issuer: indexingService,
            audience: indexingService,
            with: indexingService.did(),
            nb: {
              content: { digest: shard1Link.multihash.bytes },
              location: [
                'http://example.com/shard1/replica1',
                'http://example.com/shard1/replica2'
              ]
              // TK: Schema needs to be updated to include `space`
            }
          })
          .delegate(),
        await Assert.location
          .invoke({
            issuer: indexingService,
            audience: indexingService,
            with: indexingService.did(),
            nb: {
              content: { digest: shard2Link.multihash.bytes },
              location: ['http://example.com/shard2/replica1']
              // TK: Schema needs to be updated to include `space`
            }
          })
          .delegate(),
        await Assert.location
          .invoke({
            issuer: indexingService,
            audience: indexingService,
            with: indexingService.did(),
            nb: {
              content: { digest: shard3Link.multihash.bytes },
              location: ['http://example.com/shard3/replica1']
              // TK: Schema needs to be updated to include `space`
            }
          })
          .delegate()
      ],
      // TK What is the context id?
      indexes: new Map([
        ['the context id', index1],
        ['another context id', index2]
      ])
    })

    assertResultOk(queryResultResult, assert)
    const archiveResult = await queryResultResult.ok.archive()
    assertResultOk(archiveResult, assert)

    const locator = new IndexingServiceLocator({
      fetch: stubFetch({
        [`https://indexing.storacha.network/claims?multihash=${base58btc.encode(
          content1Link.multihash.bytes
        )}`]: () => archiveResult.ok,
        [`https://indexing.storacha.network/claims?multihash=${base58btc.encode(
          content2Link.multihash.bytes
        )}`]: () => {
          assert.fail(new Error('Should not have requested content2'))
          return null
        },
        [`https://indexing.storacha.network/claims?multihash=${base58btc.encode(
          content3Link.multihash.bytes
        )}`]: () => {
          assert.fail(new Error('Should not have requested content3'))
          return null
        },
        [`https://indexing.storacha.network/claims?multihash=${base58btc.encode(
          content4Link.multihash.bytes
        )}`]: () => {
          assert.fail(new Error('Should not have requested content4'))
          return null
        }
      })
    })

    assert.deepEqual(await locator.locate(content1Link.multihash), {
      ok: {
        digest: content1Link.multihash,
        site: [
          {
            location: [
              new URL('http://example.com/shard1/replica1'),
              new URL('http://example.com/shard1/replica2')
            ],
            range: { offset: 110, length: 120 },
            space: undefined
          }
        ]
      }
    })

    assert.deepEqual(await locator.locate(content2Link.multihash), {
      ok: {
        digest: content2Link.multihash,
        site: [
          {
            location: [
              new URL('http://example.com/shard1/replica1'),
              new URL('http://example.com/shard1/replica2')
            ],
            range: { offset: 210, length: 220 },
            space: undefined
          }
        ]
      }
    })

    assert.deepEqual(await locator.locate(content3Link.multihash), {
      ok: {
        digest: content3Link.multihash,
        site: [
          {
            location: [new URL('http://example.com/shard2/replica1')],
            range: { offset: 310, length: 320 },
            space: undefined
          }
        ]
      }
    })

    assert.deepEqual(await locator.locate(content4Link.multihash), {
      ok: {
        digest: content4Link.multihash,
        site: [
          {
            location: [new URL('http://example.com/shard3/replica1')],
            range: { offset: 410, length: 420 },
            space: undefined
          }
        ]
      }
    })
  }
}
