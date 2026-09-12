#!/usr/bin/env bash
# Hard gate: zig-out/bin/kannon must stay under 1 MiB.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${1:-zig-out/bin/kannon}
MAX=$((1024 * 1024))
size=$(stat -c%s "$BIN" 2>/dev/null || stat -f%z "$BIN")
echo "$BIN: $size bytes"
if [ "$size" -ge "$MAX" ]; then
    echo "FAIL: $BIN is over 1 MiB ($size >= $MAX)" >&2
    exit 1
fi
echo "ok: under 1 MiB"
