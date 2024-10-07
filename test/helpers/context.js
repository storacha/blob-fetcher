import { withBucketServer } from './bucket.js'
import { withClaimsServer } from './claims.js'

/**
 * @typedef {import('./helpers/bucket.js').BucketServerContext & import('./helpers/claims.js').ClaimsServerContext} Context
 * @param {(assert: import('entail').assert, ctx: Context) => unknown} testfn
 */
export const withContext = testfn => withBucketServer(withClaimsServer(testfn))
