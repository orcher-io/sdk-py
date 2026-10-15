# Changelog

## [0.4.9](https://github.com/orcher-io/sdk-py/compare/v0.4.8...v0.4.9) (2026-10-15)


### Features

* build on sdk-core 0.15 and carry restart timeouts and terminated-child reasons ([#51](https://github.com/orcher-io/sdk-py/issues/51)) ([c9a329e](https://github.com/orcher-io/sdk-py/commit/c9a329e9c7cf9c2e8746d7808aafa72cb2695ac4))
* run the worker on the shared worker host ([#47](https://github.com/orcher-io/sdk-py/issues/47)) ([7a0490b](https://github.com/orcher-io/sdk-py/commit/7a0490b9ff59587fcf52502559facb232014c88d))


### Bug Fixes

* read a workflow run's attempt as the engine numbers it ([#53](https://github.com/orcher-io/sdk-py/issues/53)) ([733fb93](https://github.com/orcher-io/sdk-py/commit/733fb934eb23301c2ca437392146fe12a968d2fb))
* send the default max attempts when a retry policy omits it ([#55](https://github.com/orcher-io/sdk-py/issues/55)) ([e3c790f](https://github.com/orcher-io/sdk-py/commit/e3c790f493c9f1644b635db6aaa070841744adf4))

## [0.4.8](https://github.com/orcher-io/sdk-py/compare/v0.4.7...v0.4.8) (2026-10-11)


### Bug Fixes

* derive a child workflow's default id from its parent ([#43](https://github.com/orcher-io/sdk-py/issues/43)) ([5eb71e6](https://github.com/orcher-io/sdk-py/commit/5eb71e635ad0b8e6aa2c7d64d2203f3bb11767dd)), closes [#41](https://github.com/orcher-io/sdk-py/issues/41)
* import auto-discovered modules once and report failures ([#39](https://github.com/orcher-io/sdk-py/issues/39)) ([8976996](https://github.com/orcher-io/sdk-py/commit/89769961fea4ef2750b43af8d1d4b6d1b549ad76)), closes [#21](https://github.com/orcher-io/sdk-py/issues/21)
* keep the state an update handler sets across activations ([#40](https://github.com/orcher-io/sdk-py/issues/40)) ([ad0c010](https://github.com/orcher-io/sdk-py/commit/ad0c0108da0e1c2d7b9779518a44c5a4fbffa7f1)), closes [#19](https://github.com/orcher-io/sdk-py/issues/19)
* pass several start args to a workflow one to one ([#38](https://github.com/orcher-io/sdk-py/issues/38)) ([80a2f93](https://github.com/orcher-io/sdk-py/commit/80a2f933b8086cf8ea41da17545eb638fa368ade)), closes [#14](https://github.com/orcher-io/sdk-py/issues/14)
* return falsy workflow results instead of None ([#37](https://github.com/orcher-io/sdk-py/issues/37)) ([8b5d152](https://github.com/orcher-io/sdk-py/commit/8b5d152908ab77c305f1aaaa1f93a75b40ca83e7)), closes [#6](https://github.com/orcher-io/sdk-py/issues/6)

## [0.4.7](https://github.com/orcher-io/sdk-py/compare/v0.4.6...v0.4.7) (2026-10-10)


### Bug Fixes

* keep the type of a class or function decorated with [@workflow](https://github.com/workflow) ([#33](https://github.com/orcher-io/sdk-py/issues/33)) ([5b24875](https://github.com/orcher-io/sdk-py/commit/5b24875dcebfa144e6fc14d477bcf526a952c5b6))
* name orcher-sdk in the missing native module error ([#32](https://github.com/orcher-io/sdk-py/issues/32)) ([cf73809](https://github.com/orcher-io/sdk-py/commit/cf738092b106a3bb9e19fef6a315d1f0605c656f))
* restore dataclass results of [@tasks](https://github.com/tasks) methods ([#34](https://github.com/orcher-io/sdk-py/issues/34)) ([14bae5b](https://github.com/orcher-io/sdk-py/commit/14bae5b0b46932458e2c169e2a3f42fec62332e0))

## [0.4.6](https://github.com/orcher-io/sdk-py/compare/v0.4.5...v0.4.6) (2026-10-09)


### Features

* let a workflow clean up when it is cancelled ([#29](https://github.com/orcher-io/sdk-py/issues/29)) ([397945f](https://github.com/orcher-io/sdk-py/commit/397945f269ef5a12a45495580a8f281d271b9dd9))

## [0.4.5](https://github.com/orcher-io/sdk-py/compare/v0.4.4...v0.4.5) (2026-10-09)


### Features

* build on orcher-sdk-core 0.10, with a cleanup limit on cancellation ([dbf481a](https://github.com/orcher-io/sdk-py/commit/dbf481ac59d73184aea6375aa728e9529af6b511))

## [0.4.4](https://github.com/orcher-io/sdk-py/compare/v0.4.3...v0.4.4) (2026-10-08)


### Features

* TLS and mTLS for workers, automatic TLS for https:// URLs ([#25](https://github.com/orcher-io/sdk-py/issues/25)) ([9833b4b](https://github.com/orcher-io/sdk-py/commit/9833b4bcb28fbeba488133d64d4cedc6bb5c7bc8))

## [0.4.3](https://github.com/orcher-io/sdk-py/compare/v0.4.2...v0.4.3) (2026-10-07)


### Bug Fixes

* accept interceptor factories in WorkerBuilder.interceptor() ([#10](https://github.com/orcher-io/sdk-py/issues/10)) ([3e49a88](https://github.com/orcher-io/sdk-py/commit/3e49a88d57da847b9581863b0cab4c81bac29f01))

## [0.4.2](https://github.com/orcher-io/sdk-py/compare/v0.4.1...v0.4.2) (2026-10-05)


### Bug Fixes

* report the steps each activation reached, and build on orcher-sdk-core 0.9.0 so code that no longer replays a run is caught ([#9](https://github.com/orcher-io/sdk-py/issues/9)) ([505fc51](https://github.com/orcher-io/sdk-py/commit/505fc51a95ae4a52ca1ac282c264cef8212e19ca))


### Documentation

* read the version badge from the registry with a short cache ([#7](https://github.com/orcher-io/sdk-py/issues/7)) ([9b05380](https://github.com/orcher-io/sdk-py/commit/9b05380f74ff372666fb9e0b6a38f8074e16f0cd))

## [0.4.1](https://github.com/orcher-io/sdk-py/compare/v0.4.0...v0.4.1) (2026-10-04)


### Bug Fixes

* build on orcher-sdk-core 0.8.1 for 32 MiB messages, and count task attempts from 1 ([#4](https://github.com/orcher-io/sdk-py/issues/4)) ([7a6d939](https://github.com/orcher-io/sdk-py/commit/7a6d93967e918e9e8baa94b554add398e0054078)), closes [#3](https://github.com/orcher-io/sdk-py/issues/3)

## Changelog
