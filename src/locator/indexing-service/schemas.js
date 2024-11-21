import { z } from 'zod'
import { CID } from 'multiformats/cid'
import * as DID from '@ipld/dag-ucan/did'

/**
 * Zod schema for the bytes of a DID. Transforms to a
 * {@link UCAN.PrincipalView<ID>}
 */
export const DIDBytes = z.instanceof(Uint8Array).transform((bytes, ctx) => {
  try {
    return DID.decode(bytes)
  } catch (error) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'Invalid DID encoding.',
      params: { decodeError: error }
    })
    return z.NEVER
  }
})

export const CIDObject = z.unknown().transform((cid, ctx) => {
  const cidOrNull = CID.asCID(cid)
  if (cidOrNull) {
    return cidOrNull
  } else {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'Invalid CID.'
    })
    return z.NEVER
  }
})

/**
 * Zod schema for a location assertion capability.
 */
export const AssertLocation = z.object({
  can: z.literal('assert/location'),
  nb: z.object({
    // TK: Actually, a full link is allowed here by the (other) schema.
    content: z.object({ digest: z.instanceof(Uint8Array) }),
    location: z.array(z.string()),
    space: DIDBytes.optional()
  })
})
