import { ByteView, MultihashDigest } from 'multiformats'
import { Failure, Result, URI, DID } from '@ucanto/interface'
import { QueryError } from '@storacha/indexing-service-client/api'

export { ByteView, MultihashDigest } from 'multiformats'
export { Failure, Result, URI, DID, Principal } from '@ucanto/interface'

/**
 * An absolute byte range to extract - always an array of two values
 * corresponding to the first and last bytes (both inclusive). e.g.
 * 
 * ```
 * [100, 200]
 * ```
 */
export type AbsoluteRange = [first: number, last: number]

/**
 * A suffix byte range - always an array of one value corresponding to the
 * first byte to start extraction from (inclusive). e.g.
 * 
 * ```
 * [900]
 * ```
 * 
 * If it is unknown how large a resource is, the last `n` bytes
 * can be requested by specifying a negative value:
 * 
 * ```
 * [-100]
 * ```
 */
export type SuffixRange = [first: number]

/**
 * Byte range to extract - an array of one or two values corresponding to the
 * first and last bytes (both inclusive). e.g.
 * 
 * ```
 * [100, 200]
 * ```
 * 
 * Omitting the second value requests all remaining bytes of the resource. e.g.
 * 
 * ```
 * [900]
 * ```
 * 
 * Alternatively, if it's unknown how large a resource is, the last `n` bytes
 * can be requested by specifying a negative value:
 * 
 * ```
 * [-100]
 * ```
 */
export type Range = AbsoluteRange | SuffixRange

export type ByteGetter = (range: AbsoluteRange) => Promise<ReadableStream<Uint8Array>>

export interface EncoderOptions {
  /** Mime type of each part. */
  contentType?: string
  /** Total size of the object in bytes. */
  totalSize?: number
  /** Stream queuing strategy. */
  strategy?: QueuingStrategy<Uint8Array>
}

export interface Abortable {
  signal: AbortSignal
}

export interface Sliceable {
  /** Byte range to extract. */
  range: Range
}

export interface SpaceLimited {
  spaces: DID[]
}

export type FetchOptions = Partial<Abortable & Sliceable & SpaceLimited>

export type LocateOptions = Partial<Abortable & SpaceLimited>

export interface Blob {
  digest: MultihashDigest
  bytes(): Promise<Uint8Array>
  stream(): ReadableStream<Uint8Array>
  clone(): Blob
}

export interface Location {
  digest: MultihashDigest
  site: Site[]
}

export interface Site {
  location: URL[]
  range: ByteRange
  space?: DID
}

export interface ByteRange {
  offset: number
  length: number
}

type FetchError = NotFound | Aborted | NetworkError | QueryError

export interface Locator {
  /** Retrieves the location of a blob of content. */
  locate (digest: MultihashDigest, options?: LocateOptions): Promise<Result<Location, FetchError>>
  /**
   * Returns a similar locator which only locates content belonging to the given
   * Spaces.
   */
  scopeToSpaces(spaces: DID[]): Locator
}

export interface Fetcher {
  /** Fetches the bytes that correspond to the passed multihash digest. */
  fetch (digest: MultihashDigest, options?: FetchOptions): Promise<Result<Blob, FetchError>>
}

export interface NotFound extends Failure {
  name: 'NotFound'
  digest: ByteView<MultihashDigest>
}

export interface Aborted extends Failure {
  name: 'Aborted'
  digest: ByteView<MultihashDigest>
}

export interface NetworkError extends Failure {
  name: 'NetworkError'
  url: URI
}
