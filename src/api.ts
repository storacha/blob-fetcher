import { ByteView, MultihashDigest } from 'multiformats'
import { Failure, Result, URI, DID } from '@ucanto/interface'
import { QueryError } from '@storacha/indexing-service-client/api'
import { Range } from 'multipart-byte-range'

export { ByteView, MultihashDigest } from 'multiformats'
export { Failure, Result, URI, DID, Principal } from '@ucanto/interface'
export { Range, SuffixRange, AbsoluteRange } from 'multipart-byte-range'

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

type OptionalLengthByteRange = Partial<Pick<ByteRange, 'length'>> & Omit<ByteRange, 'length'>
export type OptionalRangeSite = { range?: OptionalLengthByteRange } & Omit<Site, 'range'>
export type ShardLocation = { digest: MultihashDigest, site: OptionalRangeSite[] }

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

export interface AsyncDigestMap<Key extends MultihashDigest<number>, Value> {
  get(key: Key) : Promise<Value|undefined>
  set(key: Key, value: Value) : Promise<void>
}