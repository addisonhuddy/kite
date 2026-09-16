#!/usr/bin/env bash
# Smoke test: produce → consume roundtrip against any Kafka 4.0+ broker.
# Needs a kite.properties on the search path (see README "Configuration")
# and an existing topic — kite never auto-creates topics by design.
#
#   scripts/smoke.sh <topic>
set -euo pipefail
cd "$(dirname "$0")/.."

K=zig-out/bin/kite
TOPIC=${1:?"usage: scripts/smoke.sh <existing-topic>"}
M=smoke-$(date +%s)-$RANDOM # unique marker so pre-existing records don't collide
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

[ -x "$K" ] || { echo "run zig build first" >&2; exit 1; }

{
    printf '%s-a\n%s-b\n%s-c\n' "$M" "$M" "$M"
    printf '%s-key\t%s-keyed\n' "$M" "$M"
} >"$TMP/in"

echo "== produce =="
"$K" "$TOPIC" <"$TMP/in"

echo "== consume roundtrip =="
"$K" -c --from-beginning -t 5000 "$TOPIC" | grep "^$M" | sort >"$TMP/out"
sort "$TMP/in" >"$TMP/expected"
diff -u "$TMP/expected" "$TMP/out"

echo "ok: produce/consume roundtrip on '$TOPIC'"
