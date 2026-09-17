# Changelog

All notable changes to kite are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

- Pending changes for the next release.

## [0.1.0] - 2026-09-17

- Renamed the project from kannon to kite.
- Added consume mode flags, including `-c` and `-i`.
- Added the curl installer and `kite install`.
- Added `--json`, `--format`, and `--quiet`.
- Added shell completions and actionable CLI diagnostics.
- Added `--show-config` for inspecting effective configuration.
- Added consume support for gzip, Snappy, and LZ4 codecs.
- Added colored diagnostics, live statistics, and produce summaries.
- Measured true send-to-ack produce latency.
- Added Docker-backed end-to-end checks in CI.
- Dropped zstd support to keep the binary below the size gate.

[Unreleased]: https://github.com/addisonhuddy/kite/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/addisonhuddy/kite/releases/tag/v0.1.0
