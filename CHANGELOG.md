# Changelog

## [3.0.0](https://github.com/storacha/blob-fetcher/compare/v2.5.0...v3.0.0) (2025-02-05)


### ⚠ BREAKING CHANGES

* **locator:** content claims is now just a client, not a locator

### Features

* **locator:** unify locator with seperate clients ([29bf47f](https://github.com/storacha/blob-fetcher/commit/29bf47f28030208427b17c8be6ce781efda1a44b))

## [2.5.0](https://github.com/storacha/blob-fetcher/compare/v2.4.3...v2.5.0) (2025-01-09)


### Features

* **batching:** fetch from single range ([24ff66c](https://github.com/storacha/blob-fetcher/commit/24ff66ce27e7868ffe032f07098230083ec33425))
* **batching:** kick off fetchblobs in parallel ([33afdb1](https://github.com/storacha/blob-fetcher/commit/33afdb1ec4f8ee15db8f7e1d27ca5e6ac27943d5))
* **batching:** resolve blocks as soon as we have them ([34c5a81](https://github.com/storacha/blob-fetcher/commit/34c5a810d6d69f3ab3a80ddb5cc2cbc2ce3da5cd))
* **blob-fetcher:** revert no multipart-byte-range ([46fa971](https://github.com/storacha/blob-fetcher/commit/46fa9711ea516f622db1d35ec33c721176304b72))
* **fetcher:** allow passing a custom fetch implementation ([c828a19](https://github.com/storacha/blob-fetcher/commit/c828a191667e5c1cffa7f1c49a348e7c583a5006))
* **tracing:** add tracing ([b24812e](https://github.com/storacha/blob-fetcher/commit/b24812eb554f243a4424fc07e966ed774ed64b7f))


### Bug Fixes

* **blob-fetcher:** remove unused package ([46fa971](https://github.com/storacha/blob-fetcher/commit/46fa9711ea516f622db1d35ec33c721176304b72))

## [2.4.3](https://github.com/storacha/blob-fetcher/compare/v2.4.2...v2.4.3) (2024-12-03)


### Bug Fixes

* Fix build error ([a96fc06](https://github.com/storacha/blob-fetcher/commit/a96fc0641e7682faab04f4e347ff0b3a823e1415))

## [2.4.2](https://github.com/storacha/blob-fetcher/compare/v2.4.1...v2.4.2) (2024-12-03)


### Bug Fixes

* Force release ([5e36fe5](https://github.com/storacha/blob-fetcher/commit/5e36fe5f0588b8b5267bdbb9052a90b6276faab5))

## [2.4.1](https://github.com/storacha/blob-fetcher/compare/v2.4.0...v2.4.1) (2024-12-03)


### Bug Fixes

* Force release ([e080c60](https://github.com/storacha/blob-fetcher/commit/e080c605566327fc852f4aa3f2908d88f2500af0))

## [2.4.0](https://github.com/storacha/blob-fetcher/compare/v2.3.1...v2.4.0) (2024-12-02)


### Features

* **api:** add space to options and site ([13ad2f0](https://github.com/storacha/blob-fetcher/commit/13ad2f0f03bf7c5063d3e12111feb569ae5a19a2))


### Bug Fixes

* slice length ([#20](https://github.com/storacha/blob-fetcher/issues/20)) ([ee1ffcc](https://github.com/storacha/blob-fetcher/commit/ee1ffcc593a205b5918c869c13e13281ffaa640c))

## [2.3.1](https://github.com/storacha/blob-fetcher/compare/v2.3.0...v2.3.1) (2024-10-31)


### Bug Fixes

* **release:** just release the damn package ([b8405a0](https://github.com/storacha/blob-fetcher/commit/b8405a09169f14a965942ebd988f9d07033aface))

## [2.3.0](https://github.com/storacha/blob-fetcher/compare/v2.2.0...v2.3.0) (2024-10-31)


### Features

* **content-claims:** allow carpark fallback on content claims ([f657b2a](https://github.com/storacha/blob-fetcher/commit/f657b2a75e0de9cdd8041bd3824434bc70d5a457))
* **content-claims:** handle missing shard locations ([445d913](https://github.com/storacha/blob-fetcher/commit/445d91344cc02cd6003eac11f50699c873a27b30))


### Bug Fixes

* **content-claims:** properly set location claims for urls ([fe39b43](https://github.com/storacha/blob-fetcher/commit/fe39b43f07d14918d1d65c84775bbc77899a54a7))

## [2.2.0](https://github.com/w3s-project/blob-fetcher/compare/v2.1.3...v2.2.0) (2024-06-06)


### Features

* use index claim ([#10](https://github.com/w3s-project/blob-fetcher/issues/10)) ([8876ec4](https://github.com/w3s-project/blob-fetcher/commit/8876ec4dd25dcec741f67f0c88c512d42ae93836))

## [2.1.3](https://github.com/w3s-project/blob-fetcher/compare/v2.1.2...v2.1.3) (2024-05-29)


### Bug Fixes

* upgrade to latest content claims ([746a9c2](https://github.com/w3s-project/blob-fetcher/commit/746a9c22be8017593089d21d97cd62215939977d))

## [2.1.2](https://github.com/w3s-project/blob-fetcher/compare/v2.1.1...v2.1.2) (2024-05-28)


### Bug Fixes

* queue processing error ([9c03ff0](https://github.com/w3s-project/blob-fetcher/commit/9c03ff0f4a22a7f5164117cdcefd6bed8854797e))

## [2.1.1](https://github.com/w3s-project/blob-fetcher/compare/v2.1.0...v2.1.1) (2024-05-28)


### Bug Fixes

* collect all location claims ([109290c](https://github.com/w3s-project/blob-fetcher/commit/109290cdeddb7a625e36e1484778a1e68905c576))

## [2.1.0](https://github.com/w3s-project/blob-fetcher/compare/v2.0.0...v2.1.0) (2024-05-24)


### Features

* add range option to fetcher ([#5](https://github.com/w3s-project/blob-fetcher/issues/5)) ([55b7c19](https://github.com/w3s-project/blob-fetcher/commit/55b7c1951074ea5508cf2158159c58bd0c5043ef))

## [2.0.0](https://github.com/w3s-project/blob-fetcher/compare/v1.1.0...v2.0.0) (2024-05-23)


### ⚠ BREAKING CHANGES

* allow streaming from fetcher

### Features

* allow streaming from fetcher ([2dc4bb5](https://github.com/w3s-project/blob-fetcher/commit/2dc4bb5f675250453d8009de402d7290c0ab3242))

## [1.1.0](https://github.com/w3s-project/blob-fetcher/compare/v1.0.1...v1.1.0) (2024-05-22)


### Features

* batching fetcher ([981f71a](https://github.com/w3s-project/blob-fetcher/commit/981f71a464c410ffd5a1b3bb7fef05c0f823c9ce))

## [1.0.1](https://github.com/w3s-project/blob-fetcher/compare/v1.0.0...v1.0.1) (2024-05-21)


### Bug Fixes

* optional service URL ([02d814b](https://github.com/w3s-project/blob-fetcher/commit/02d814bc3477b91f49be2ab4259b7eab5d5fbe07))

## 1.0.0 (2024-05-21)


### Features

* initial commit ([1b80d5b](https://github.com/w3s-project/blob-fetcher/commit/1b80d5b7590e4cbe7835a657fa5a7d2c73fe7172))
