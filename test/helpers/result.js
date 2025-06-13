/**
 * Returns contained `ok` if result is and throws `error` if result is not ok.
 *
 * @template T
 * @param {import('@ucanto/interface').Result<T, {}>} result
 */
export const unwrap = ({ ok, error }) => {
  if (error) {
    throw error
  } else if (ok == null) {
    throw new Error('invalid result, no error and null/undefined ok')
  } else {
    return ok
  }
}

/**
 * Also expose as `Result.try` which is arguably more clear.
 */
export { unwrap as try }
