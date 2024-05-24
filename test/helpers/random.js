import { webcrypto } from 'node:crypto'

/** @param {number} size */
export const randomBytes = async (size) => {
  const bytes = new Uint8Array(size)
  while (size) {
    const chunk = new Uint8Array(Math.min(size, 65_536))
    webcrypto.getRandomValues(chunk)
    size -= chunk.length
    bytes.set(chunk, size)
  }
  return bytes
}

/** @param {number} max */
export const randomInt = (max) => Math.floor(Math.random() * max)
