import { ByteView, MultihashDigest } from 'multiformats'
import { Failure, Result, URI } from '@ucanto/interface'

export { ByteView, MultihashDigest } from 'multiformats'
export { Failure, Result, URI } from '@ucanto/interface'

export interface Abortable {
  signal: AbortSignal
}

export type GetOptions = Partial<Abortable>

export interface Blob {
  digest: MultihashDigest
  bytes(): Promise<Uint8Array>
  stream(): ReadableStream<Uint8Array>
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
  locate (digest: MultihashDigest, options?: GetOptions): Promise<Result<Location, NotFound|Aborted|NetworkError>>
}

export interface Fetcher {
  /** Fetches the bytes that correspond to the passed multihash digest. */
  fetch (digest: MultihashDigest, options?: GetOptions): Promise<Result<Blob, NotFound|Aborted|NetworkError>>
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
