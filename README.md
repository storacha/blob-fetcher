# @web3-storage/blob-fetcher

A blob fetcher that batches requests and reads multipart byterange responses.

## Install

```sh
npm install @web3-storage/blob-fetcher
```

## Usage

Example

```js
import * as SimpleFetcher from '@web3-storage/blob-fetcher/fetcher/simple'
import * as ContentClaimsLocator from '@web3-storage/blob-fetcher/locator/content-claims'
import * as Digest from 'multiformats/hashes/digest'
import { base58btc } from 'multiformats/bases/base58'

const locator = ContentClaimsLocator.create()
const fetcher = SimpleFetcher.create(locator)

const digest = Digest.decode(base58btc.decode('zQmZ3Q2KuYrg3LiizMcArupHjv3dDdn3r4MLPtANTsj3ut5'))
const res = await fetcher.fetch(digest)
if (!res.ok) throw res.error

const bytes = await res.ok.bytes()
```


## Contributing

Feel free to join in. All welcome. [Open an issue](https://github.com/w3s-project/blob-fetcher/issues)!

## License

Dual-licensed under [MIT / Apache 2.0](https://github.com/w3s-project/blob-fetcher/blob/main/LICENSE.md)

