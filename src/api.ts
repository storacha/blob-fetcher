import { ByteView, MultihashDigest } from 'multiformats'
import { Failure, Result, URI } from '@ucanto/interface'
import { Range } from 'multipart-byte-range'

export { ByteView, MultihashDigest } from 'multiformats'
export { Failure, Result, URI } from '@ucanto/interface'
export { Range, SuffixRange, AbsoluteRange } from 'multipart-byte-range'

export interface Abortable {
  signal: AbortSignal
}

export interface Sliceable {
  /** Byte range to extract. */
  range: Range
}

export type FetchOptions = Partial<Abortable> & Partial<Sliceable>

export type LocateOptions = Partial<Abortable>

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
}

export interface ByteRange {
  offset: number
  length: number
}

export interface Locator {
  /** Retrieves the location of a blob of content. */
  locate (digest: MultihashDigest, options?: LocateOptions): Promise<Result<Location, NotFound|Aborted|NetworkError>>
}

export interface Fetcher {
  /** Fetches the bytes that correspond to the passed multihash digest. */
  fetch (digest: MultihashDigest, options?: FetchOptions): Promise<Result<Blob, NotFound|Aborted|NetworkError>>
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
