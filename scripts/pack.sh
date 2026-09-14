#!/usr/bin/env bash
# Pack the built binary with UPX/LZMA: zig-out/bin/kite shrinks to ~35%
# (~200 KiB) for distribution. Requires `upx` on PATH — not needed for
# development; use it when producing release artifacts.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${1:-zig-out/bin/kite}
command -v upx >/dev/null || { echo "upx not found — https://upx.github.io" >&2; exit 1; }
upx --best --lzma "$BIN"
ls -la "$BIN"
