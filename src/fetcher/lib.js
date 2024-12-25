/**
 * @param {import('../api.js').Range} range
 * @param {number} totalSize
 * @returns {import('../api.js').AbsoluteRange}
 */
export const resolveRange = (range, totalSize) => {
  let last = range[1]
  if (last == null) {
    last = totalSize - 1
  }
  const first = range[1] == null && range[0] < 0 ? (last + 1 + range[0]) : range[0]
  return [first, last]
}
