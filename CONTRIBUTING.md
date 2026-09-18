# Contributing

## Prerequisites

- Zig 0.16.0
- Docker for end-to-end tests

## Checks

```sh
zig fmt --check src build.zig
zig build
zig build test
scripts/cli-check.sh
scripts/check-size.sh
scripts/e2e-docker.sh
```

The size check gates the binary at 600 KB. The Docker end-to-end check needs
Docker. See [TESTING.md](TESTING.md) for details.

## Pull requests

- Keep PRs small and focused.
- Add tests for behavior changes.
- Keep the binary under the size gate.
- Write a clear PR title; release notes are generated from merged PR titles.
