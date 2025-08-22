# Changelog

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
