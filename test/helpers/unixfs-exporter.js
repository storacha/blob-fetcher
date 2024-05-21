/** @param {import('../../src/api.js').Fetcher} fetcher */
export const asBlockstore = fetcher => ({
  /** @param {import('multiformats').Link} cid */
  async get (cid) {
    const res = await fetcher.fetch(cid.multihash)
    if (res.error) {
      console.error(res.error)
      throw res.error
    }
    return res.ok.bytes
  }
})
