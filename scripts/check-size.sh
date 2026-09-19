#!/usr/bin/env bash
# Hard gate: zig-out/bin/kite must stay under 640 KB.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${1:-zig-out/bin/kite}
MAX=640000
size=$(stat -c%s "$BIN" 2>/dev/null || stat -f%z "$BIN")
echo "$BIN: $size bytes"
if [ "$size" -ge "$MAX" ]; then
    echo "FAIL: $BIN is over 640 KB ($size >= $MAX)" >&2
    exit 1
fi
echo "ok: under 640 KB"
