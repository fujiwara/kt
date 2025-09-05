# Changelog

## [v15.1.3](https://github.com/fujiwara/kt/compare/v15.1.2...v15.1.3) - 2025-09-05

## [v15.1.2](https://github.com/fujiwara/kt/compare/v15.1.1...v15.1.2) - 2025-09-05
- Use draft release by @fujiwara in https://github.com/fujiwara/kt/pull/37

## [v15.1.1](https://github.com/fujiwara/kt/compare/v15.1.0...v15.1.1) - 2025-09-05
- Fix workflows by @fujiwara in https://github.com/fujiwara/kt/pull/34
- tagpr creates a release with a draft status. by @fujiwara in https://github.com/fujiwara/kt/pull/36

## [v15.1.0](https://github.com/fujiwara/kt/compare/v15.0.2...v15.1.0) - 2025-08-30
- Add SASL_SSL authentication mode support by @fujiwara in https://github.com/fujiwara/kt/pull/31
- Add comprehensive SASL authentication support with multiple mechanisms by @fujiwara in https://github.com/fujiwara/kt/pull/33

## [v15.0.2](https://github.com/fujiwara/kt/compare/v15.0.1...v15.0.2) - 2025-08-29
- Remove deprecated BuildNameToCertificate calls by @fujiwara in https://github.com/fujiwara/kt/pull/29

## [v15.0.1](https://github.com/fujiwara/kt/compare/v15.0.0...v15.0.1) - 2025-08-29
- Add detailed verbose logging for argument parsing by @fujiwara in https://github.com/fujiwara/kt/pull/26
- Fix deprecated Sarama rebalance strategy configuration by @fujiwara in https://github.com/fujiwara/kt/pull/28

## [v15.0.0](https://github.com/fujiwara/kt/compare/v14.2.0...v15.0.0) - 2025-08-22
- Migrate CLI from flag package to Kong by @fujiwara in https://github.com/fujiwara/kt/pull/21
- Improve error handling by @fujiwara in https://github.com/fujiwara/kt/pull/23
- Update README documentation with missing features and examples by @fujiwara in https://github.com/fujiwara/kt/pull/24
- Fix missing randomString function in system_test.go by @fujiwara in https://github.com/fujiwara/kt/pull/25

## [v14.2.0](https://github.com/fujiwara/kt/compare/v14.1.0...v14.2.0) - 2025-08-22
- Add jq filtering support with -jq and -raw flags by @fujiwara in https://github.com/fujiwara/kt/pull/20

## [v14.1.0](https://github.com/fujiwara/kt/compare/v14.0.3...v14.1.0) - 2025-08-16
- switch to goccy/go-json by @fujiwara in https://github.com/fujiwara/kt/pull/13
- Add adaptive output buffering by @fujiwara in https://github.com/fujiwara/kt/pull/15
- Add -quiet flag to produce command for silent operation by @fujiwara in https://github.com/fujiwara/kt/pull/16
- Fix output buffering effectiveness by removing unnecessary flushes by @fujiwara in https://github.com/fujiwara/kt/pull/17
- Add periodic flush for pipeline compatibility while maintaining bufering. by @fujiwara in https://github.com/fujiwara/kt/pull/18

## [v14.0.3](https://github.com/fujiwara/kt/compare/v14.0.2...v14.0.3) - 2025-07-29
- Fix infinite wait issue with -until flag when no new messages arrive by @fujiwara in https://github.com/fujiwara/kt/pull/11

## [v14.0.2](https://github.com/fujiwara/kt/compare/v14.0.1...v14.0.2) - 2025-07-29
- Improve SSL certificate management and separate unit/integration tests by @fujiwara in https://github.com/fujiwara/kt/pull/8
- update go modules by @fujiwara in https://github.com/fujiwara/kt/pull/10

## [v14.0.1](https://github.com/fujiwara/kt/compare/v14.0.0...v14.0.1) - 2025-07-28
- Add timestamp-based offset support to consume command by @fujiwara in https://github.com/fujiwara/kt/pull/6

## [v14.0.0](https://github.com/fujiwara/kt/commits/v14.0.0) - 2025-07-28
- Signal handling by @fujiwara in https://github.com/fujiwara/kt/pull/1
- Implement proper consumer group functionality by @fujiwara in https://github.com/fujiwara/kt/pull/2
- Add until flag feature by @fujiwara in https://github.com/fujiwara/kt/pull/3
- Update GitHub Actions workflow and fix consume timeout handling by @fujiwara in https://github.com/fujiwara/kt/pull/4
