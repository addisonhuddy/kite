#!/usr/bin/env bash
# Smoke test: produce → consume roundtrip against any Kafka 4.0+ broker.
# Needs a broker configured (-b is not used here, so set BOOTSTRAP_SERVERS
# or put a kite.properties on the search path; see README "Configuration")
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

echo "== json roundtrip =="
printf '{"key":"%s-jk","value":{"m":"%s","n":1},"headers":{"h":"v"}}\n' "$M" "$M" | "$K" --json "$TOPIC"
"$K" -c -B --json "$TOPIC" | grep -F "\"key\":\"$M-jk\"" >"$TMP/json"
[ "$(wc -l <"$TMP/json")" -eq 1 ] || { echo "FAIL: expected one json record"; cat "$TMP/json"; exit 1; }
grep -Fq "\"headers\":[{\"key\":\"h\",\"value\":\"v\"}]" "$TMP/json" || { echo "FAIL: headers"; cat "$TMP/json"; exit 1; }
grep -Fq "\"value\":\"{\\\"m\\\":\\\"$M\\\",\\\"n\\\":1}\"" "$TMP/json" || { echo "FAIL: value"; cat "$TMP/json"; exit 1; }

echo "== offset out of range =="
set +e
"$K" -c --partition 0 --offset 9223372036854775806 --idle 1s "$TOPIC" >"$TMP/oor.out" 2>"$TMP/oor.err"
status=$?
set -e
[ "$status" -eq 1 ] && [ ! -s "$TMP/oor.out" ] && grep -q "is out of range for partition 0" "$TMP/oor.err" || {
    echo "FAIL: out-of-range offset should error, not replay"; cat "$TMP/oor.err"; exit 1;
}

echo "ok: produce/consume roundtrip on '$TOPIC'"
