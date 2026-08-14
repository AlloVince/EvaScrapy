# CHANGELOG

<!-- version list -->

## v2.1.2 (2026-08-14)

### Bug Fixes

- **ci**: Create GitHub releases
  ([`7c5c3c4`](https://github.com/AlloVince/EvaScrapy/commit/7c5c3c4b71bb15bc48dd05f667bab6ad86734e46))


## v2.1.1 (2026-08-14)

### Bug Fixes

- For cookies
  ([`7e4b472`](https://github.com/AlloVince/EvaScrapy/commit/7e4b472eb895ef358af97793318e28e1554613bf))

### Documentation

- New
  ([`77291df`](https://github.com/AlloVince/EvaScrapy/commit/77291df4d9a663b118e8241bf0cab098535a6d24))


## v2.1.0 (2026-08-10)

### Chores

- Remove requirements.txt, migrate to pyproject.toml
  ([`055be09`](https://github.com/AlloVince/EvaScrapy/commit/055be09e1e17a6371d0493c413185469045b24f7))

### Documentation

- Memory
  ([`388e2de`](https://github.com/AlloVince/EvaScrapy/commit/388e2de636919affce9aa6c35d65db51b938ce29))

### Features

- Add nats
  ([`2c5e25d`](https://github.com/AlloVince/EvaScrapy/commit/2c5e25d3df8ec247a69ed979bcb3c01de2c0e112))


## v2.0.1 (2026-08-10)

### Bug Fixes

- Build_command must be string in semantic-release v10
  ([`58e33ab`](https://github.com/AlloVince/EvaScrapy/commit/58e33ab2733ec5607c2742a53b64cbb8ab6614ad))

- Ci
  ([`88b2282`](https://github.com/AlloVince/EvaScrapy/commit/88b2282297e4a6ec5bafefecf56295e78a764efa))

- Ci
  ([`53b05ef`](https://github.com/AlloVince/EvaScrapy/commit/53b05ef13090dd72e417c90a883b8ddb2b94716f))

- Docs
  ([`46019ed`](https://github.com/AlloVince/EvaScrapy/commit/46019edcf2799cf4a1b1f682723b1f163c93d4dd))

- Support Python 3.14 crawler runtime
  ([`659f653`](https://github.com/AlloVince/EvaScrapy/commit/659f653b663786e699bd88fcb50d5d3a430baf11))

- Use python-semantic-release action instead of publish-action
  ([`1c7a7ed`](https://github.com/AlloVince/EvaScrapy/commit/1c7a7edbf660949dea80af5c5ae12c745a09d65a))

- Use uv sync instead of uv pip install --system
  ([`8ae97d1`](https://github.com/AlloVince/EvaScrapy/commit/8ae97d128e26426a9d62396ba54c2b43f8a0a5f8))


## v2.0.0 (2019-06-28)

### Features

- Upgrade depends & python 3.7
  ([`c778209`](https://github.com/AlloVince/EvaScrapy/commit/c7782098501df0cb63f68bb233773710e86a78a1))

### Breaking Changes

- Requests verison up


## v1.4.3 (2019-06-28)

### Bug Fixes

- Fix urllib3 version
  ([`9d65385`](https://github.com/AlloVince/EvaScrapy/commit/9d6538527c18bede5dfa9aa8c1ed9793f03d4dc4))


## v1.4.2 (2019-06-28)

### Bug Fixes

- Freeze requests version due to #4160
  ([`1e784b5`](https://github.com/AlloVince/EvaScrapy/commit/1e784b5ce2e4b956b0c6362ba8a97986cc7a0d88))


## v1.4.1 (2019-06-07)

### Bug Fixes

- Added handle_torrent for base_spider
  ([`e94420b`](https://github.com/AlloVince/EvaScrapy/commit/e94420b387a79d3d858213187fdff35e6fe7cbb4))


## v1.4.0 (2019-06-06)

### Continuous Integration

- Add pytest
  ([`faed294`](https://github.com/AlloVince/EvaScrapy/commit/faed294c51c5e167c286570d8c15711fd9a0a1c8))

- Fix ci
  ([`2b0613a`](https://github.com/AlloVince/EvaScrapy/commit/2b0613afd3a645abc6f7af603851924163341611))

### Features

- Add new pipeline for elastic repeat check
  ([`e806da1`](https://github.com/AlloVince/EvaScrapy/commit/e806da1352c6991b30245aa53d69221a66def2f0))


## v1.3.2 (2018-12-05)

### Bug Fixes

- Timezone for docker
  ([`7c2bde5`](https://github.com/AlloVince/EvaScrapy/commit/7c2bde5576707c5f6affa8aae70d68c3f6ecea68))


## v1.3.1 (2018-12-05)

### Bug Fixes

- Set timezone
  ([`eccdc6a`](https://github.com/AlloVince/EvaScrapy/commit/eccdc6aeff9c9616f0b27435292e3196ca20e76b))

- Set timezone
  ([`4276af9`](https://github.com/AlloVince/EvaScrapy/commit/4276af9611d63ad6e70646a142262224cad0c378))


## v1.3.0 (2018-12-04)

### Bug Fixes

- Add git to CI
  ([`110d61b`](https://github.com/AlloVince/EvaScrapy/commit/110d61b9858b8d0390c595155bf61a5c0d876a2f))

### Features

- Uniform logging
  ([`40bb252`](https://github.com/AlloVince/EvaScrapy/commit/40bb25214fe421e3bc37d37b77442518e917638d))


## v1.2.1 (2018-11-29)

### Bug Fixes

- Fixed allowed_deep_domains issue
  ([`84ea34a`](https://github.com/AlloVince/EvaScrapy/commit/84ea34a93835f1060d6547eeb484f5f9db196c7e))

- Upgrade pip in ci
  ([`a6ad4dc`](https://github.com/AlloVince/EvaScrapy/commit/a6ad4dca3abf3eddfa8faa8cefe5c7dc4bd42bd6))


## v1.2.0 (2018-10-23)

### Features

- Added global cookie middleware
  ([`01384ec`](https://github.com/AlloVince/EvaScrapy/commit/01384ecaff88c877e69009c3e9cfaf4cdbb4d06b))


## v1.1.2 (2018-10-17)

### Bug Fixes

- Fix extension issue
  ([`fce56d9`](https://github.com/AlloVince/EvaScrapy/commit/fce56d9d21e7799e130e2e540c765d82b378165c))


## v1.1.1 (2018-10-17)

### Bug Fixes

- S3 support binary file
  ([`e4f5af4`](https://github.com/AlloVince/EvaScrapy/commit/e4f5af4903b70a848f17ac25e5a6207ece92e6aa))

### Testing

- Fix unit test
  ([`e2dea61`](https://github.com/AlloVince/EvaScrapy/commit/e2dea6135e5970e7b62d07dd994474805753818c))


## v1.1.0 (2018-10-17)

### Bug Fixes

- Cache info_hash
  ([`2408678`](https://github.com/AlloVince/EvaScrapy/commit/2408678d1029780f097d2ddf5ef5eb2d8c60d08f))

### Continuous Integration

- Added docker staging build
  ([`553acd0`](https://github.com/AlloVince/EvaScrapy/commit/553acd0b4cb48c7283f5d9a4234df9537fe5e714))

- Fix build staging
  ([`eed0b43`](https://github.com/AlloVince/EvaScrapy/commit/eed0b4305665f476bbaa13ec84f67722d10fb5ea))

- Pip install fix
  ([`ee16ba7`](https://github.com/AlloVince/EvaScrapy/commit/ee16ba79bbeb4636f78dc457e47ab273a7dd85ca))

### Features

- Add torrent file pipeline
  ([`6a3876e`](https://github.com/AlloVince/EvaScrapy/commit/6a3876e59194bfe922ad9ee4f94fd0e1fbe97357))

### Refactoring

- Added queue based item
  ([`9194b4c`](https://github.com/AlloVince/EvaScrapy/commit/9194b4c5dfab9a85ad12606116d0d303b15036e9))

- Mv rules under items
  ([`8ffc5a6`](https://github.com/AlloVince/EvaScrapy/commit/8ffc5a694aba2a0b15d422a5aedffd39b28ba3da))


## v1.0.0 (2018-10-06)

- Initial Release
