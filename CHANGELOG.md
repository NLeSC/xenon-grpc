# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased

## [3.0.2] - 2020-03-23

### Changed

* Depends on Xenon 3.1.0

## [3.0.1] - 2019-09-11

### Changed

* Depends on Xenon 3.0.4
* Depends on Xenon cloud adaptors 3.0.2

## [3.0.0] - 2019-06-14

### Added

* getDefaultRuntime rpc to SchedulerService ([#44](https://github.com/xenon-middleware/xenon-grpc/issues/44))
* start_time and temp_space fields to JobDescription message ([#43](https://github.com/xenon-middleware/xenon-grpc/issues/43))
* [at](https://linux.die.net/man/1/at) scheduler

### Changed

* Replaced tasks+cores+nodes fields in JobDescription message with nodes+processes+thread fields ([#625](https://github.com/xenon-middleware/xenon/issues/625)).
* Require Java 11 or greater, as xenon package has same compatibility ([#42](https://github.com/xenon-middleware/xenon-grpc/issues/42https://github.com/xenon-middleware/xenon-grpc/issues/42))
* Upgraded to Xenon 3.0.0 ([#40](https://github.com/xenon-middleware/xenon-grpc/issues/40))

### Removed

* hdfs filesystem
* options field in JobDescription message ([#630](https://github.com/xenon-middleware/xenon/issues/630))

## [2018-03-14] 2.3.0

### Added

* scheduler argument for job description (#38)

### Changed

* Depends on Xenon 2.6.0

## [2018-03-06] 2.2.1

### Fixed

* hadoop/grpc netty version conflict (#37)

## [2018-03-05] 2.2.0

### Added

* support for KeytabCredential (#33)
* supportedCredentials (#35)

### Changed

* Depends on Xenon 2.5.0

### Fixed

* FileSystemAdaptorDescription fields synced (#36)

## [2018-02-26] 2.1.0

### Added

* Name to JobDescription and JobStatus
* Max memory to JobDescription

### Changed

* Use latest dependencies and plugins
* Depends on Xenon 2.4.0

## [2018-01-04] 2.0.1

### Changed

* Use latest dependencies and plugins

### Fixed

* Class of exception lost in translation [#32]
* Interactive job: sometimes output to stdout is repeated, sometimes skipped [#34]

## [2017-11-07] 2.0.0

Initial release
