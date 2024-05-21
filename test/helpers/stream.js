/** @param {ReadableStream<Uint8Array>|AsyncIterable<Uint8Array>} readable */
export const concat = async readable => {
  const chunks = []
  // @ts-expect-error wen async iterable ReadableStream?
  for await (const chunk of readable) {
    chunks.push(chunk)
  }
  return new Uint8Array(await new Blob(chunks).arrayBuffer())
}
