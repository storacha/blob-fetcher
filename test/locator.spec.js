import * as ContentClaimsLocator from '../src/locator/content-claims.js'
import { withContext } from './helpers/context.js'
import { createRandomFile } from './helpers/random.js'

export const testLocator = {
  'it should return claims from ContentClaimsLocator#locate': withContext(async (/** @type {import('entail').assert} assert */ assert, ctx) => {
    const { root } = await createRandomFile(ctx)
    const locator = ContentClaimsLocator.create({ serviceURL: ctx.claimsURL })
    const result = await locator.locate(root.multihash)
    assert.ok(result.ok)
    const location = result.ok
    assert.equal(location.claims.length, 1)
  })
}
