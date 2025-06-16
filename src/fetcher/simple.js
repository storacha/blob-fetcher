// eslint-disable-next-line
import * as API from '../api.js'
import { resolveRange } from './lib.js'
import { NetworkError, NotFoundError } from '../lib.js'
import { withResultSpan } from '../tracing/tracing.js'

/** @implements {API.Fetcher} */
class SimpleFetcher {
  #locator
  #fetch

  /**
   * @param {API.Locator} locator
   * @param {typeof globalThis.fetch} [fetch]
   */
  constructor (locator, fetch = globalThis.fetch.bind(globalThis)) {
    this.#locator = locator
    this.#fetch = fetch
  }

  /**
   * @param {API.MultihashDigest} digest
   * @param {API.FetchOptions} [options]
   */
  async fetch (digest, options) {
    const locResult = await this.#locator.locate(digest, options)
    if (locResult.error) return locResult
    return fetchBlob(locResult.ok, options?.range, this.#fetch)
  }
}

/**
 * Create a new blob fetcher.
 * @param {API.Locator} locator
 * @param {typeof globalThis.fetch} [fetch]
 * @returns {API.Fetcher}
 */
export const create = (locator, fetch = globalThis.fetch.bind(globalThis)) => new SimpleFetcher(locator, fetch)

// Add retry logic and progress indicators
const MAX_RETRIES = 3;

export const fetchBlob = withResultSpan('fetchBlob',
  /**
 * Fetch a blob from the passed location.
 * @param {API.Location} location
 * @param {API.Range} [range]
 * @param {typeof globalThis.fetch} [fetch]
 */
  async (location, range, fetch = globalThis.fetch.bind(globalThis)) => {
    let networkError;

    for (const site of location.site) {
      for (const url of site.location) {
        let resolvedRange = [site.range.offset, site.range.offset + site.range.length - 1];
        if (range) {
          const relRange = resolveRange(range, site.range.length);
          resolvedRange = [site.range.offset + relRange[0], site.range.offset + relRange[1]];
        }
        const headers = { Range: `bytes=${resolvedRange[0]}-${resolvedRange[1]}` };

        for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
          try {
            const res = await fetch(url, { headers });
            if (!res.ok || !res.body) {
              console.warn(`failed to fetch ${url}: ${res.status} ${await res.text()}`);
              continue;
            }

            // Progress indicator
            const contentLength = res.headers.get('Content-Length');
            const total = contentLength ? parseInt(contentLength, 10) : null;
            let loaded = 0;

            const reader = res.body.getReader();
            const stream = new ReadableStream({
              start(controller) {
                function push() {
                  reader.read().then(({ done, value }) => {
                    if (done) {
                      controller.close();
                      return;
                    }
                    loaded += value.length;
                    if (total) {
                      console.log(`Progress: ${(loaded / total * 100).toFixed(2)}%`);
                    }
                    controller.enqueue(value);
                    push();
                  });
                }
                push();
              }
            });

            return { ok: new Blob(location.digest, new Response(stream)) };
          } catch (err) {
            console.warn(`Attempt ${attempt + 1} failed for ${url}: ${err.message}`);
            networkError = new NetworkError(url, { cause: err });
          }
        }
      }
    }

    return { error: networkError || new NotFoundError(location.digest) };
  }
);

/** @implements {API.Blob} */
class Blob {
  #digest
  #response

  /**
   * @param {API.MultihashDigest} digest
   * @param {Response} response
   */
  constructor (digest, response) {
    this.#digest = digest
    this.#response = response
  }

  get digest () {
    return this.#digest
  }

  async bytes () {
    return new Uint8Array(await this.#response.arrayBuffer())
  }

  stream () {
    if (!this.#response.body) throw new Error('missing response body')
    return this.#response.body
  }

  clone () {
    return new Blob(this.#digest, this.#response.clone())
  }
}
