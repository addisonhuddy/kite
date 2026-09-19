#!/usr/bin/env bash
# Smoke test: produce → consume roundtrip against any Kafka 4.0+ broker.
# Needs a broker configured (-b is not used here, so set BOOTSTRAP_SERVERS
# or put a kite.properties on the search path; see README "Configuration")
# and a topic — kite creates a missing one automatically when not on a
# terminal.
#
#   scripts/smoke.sh <topic>
set -euo pipefail
cd "$(dirname "$0")/.."

K=zig-out/bin/kite
TOPIC=${1:?"usage: scripts/smoke.sh <topic>"}
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

echo "== ack latency =="
# Stdin stalls for 2s, but avg ack latency measures send-to-ack only and
# must stay well under that.
(sleep 2; printf '%s-slow\n' "$M") | "$K" "$TOPIC" 2>"$TMP/lat.err"
printf '%s-slow\n' "$M" >>"$TMP/in"
grep -Fq "avg ack latency" "$TMP/lat.err" || { echo "FAIL: no avg ack latency"; cat "$TMP/lat.err"; exit 1; }
! grep -Fq "avg per request" "$TMP/lat.err" || { echo "FAIL: stale 'avg per request' label"; cat "$TMP/lat.err"; exit 1; }
ms=$(grep -o '[0-9.]*ms avg ack latency' "$TMP/lat.err" | head -1 | grep -o '^[0-9.]*')
awk -v ms="$ms" 'BEGIN { exit !(ms + 0 < 1000) }' || {
    echo "FAIL: avg ack latency ${ms}ms >= 1000ms (stdin wait leaked into the metric)"; exit 1;
}

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
