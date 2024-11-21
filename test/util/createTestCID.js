import { CID } from 'multiformats'
import * as json from 'multiformats/codecs/json'
import { identity } from 'multiformats/hashes/identity'

/**
 * Creates a JSON CID from a string label. The CID should be both unique and
 * consistent for any value of {@link label}, making it useful for testing.
 *
 * @param {string} label
 * @returns {CID<unknown, number, number, 1>}
 */
export const createTestCID = (label) => {
  return CID.createV1(json.code, identity.digest(json.encode(label)))
}
