#!/bin/sh
# Exercise the kite + wing JSON Schema walkthrough and assert decoded values.
# Requires Kafka :9092, SR :8081, kite/wing on PATH (or KITE/WING); run from repo root.
set -eu

KITE=${KITE:-kite}
WING=${WING:-wing}
export BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-localhost:9092}
export SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

for topic in orders orders-raw orders-copy; do
  "$KITE" topic delete -y --if-exists "$topic" >/dev/null 2>&1 || :
done
"$WING" rm orders-value -y >/dev/null 2>&1 || :
"$WING" rm orders-copy-value -y >/dev/null 2>&1 || :

"$KITE" topic create --if-not-exists orders >/dev/null 2>&1
"$KITE" topic create --if-not-exists orders-raw >/dev/null 2>&1
"$KITE" topic create --if-not-exists orders-copy >/dev/null 2>&1

"$WING" push --check < examples/wing/orders.schema.json >/dev/null
printf 'PASS schema lint\n'
"$WING" push orders < examples/wing/orders.schema.json >/dev/null
printf 'PASS schema registration\n'

"$WING" write orders < examples/wing/orders.jsonl |
  "$KITE" produce --json orders >/dev/null 2>&1
printf 'PASS validated produce\n'

set +e
"$WING" write orders < examples/wing/orders-bad.jsonl \
  > "$tmp_dir/bad.out" 2> "$tmp_dir/bad.err"
bad_status=$?
set -e
[ "$bad_status" -eq 2 ]
[ ! -s "$tmp_dir/bad.out" ]
grep -Fq '/order_id: expected integer, got string' "$tmp_dir/bad.err"
cat "$tmp_dir/bad.err" >&2
printf 'PASS bad record rejected (exit %s)\n' "$bad_status"

"$KITE" consume -B -n 3 --idle 1s --json orders > "$tmp_dir/orders.jsonl"
[ "$(jq -s 'length' "$tmp_dir/orders.jsonl")" -eq 2 ]
"$WING" read < "$tmp_dir/orders.jsonl" | jq -c .value > "$tmp_dir/orders.values"
printf '%s\n' \
  '{"order_id":1,"customer":"Ada","total":12.5}' \
  '{"order_id":2,"customer":"Grace","total":21}' |
  cmp - "$tmp_dir/orders.values"
cat "$tmp_dir/orders.values"
printf 'PASS decode and no write from invalid record\n'

"$KITE" produce --csv orders-raw < examples/wing/orders.csv >/dev/null 2>&1
"$KITE" consume -B -n 2 --idle 3s --json orders-raw |
  "$WING" write orders --fit |
  "$KITE" produce --json orders >/dev/null 2>&1
"$KITE" consume -B -n 4 --idle 3s --json orders > "$tmp_dir/all-orders.jsonl"
"$WING" read < "$tmp_dir/all-orders.jsonl" | jq -c .value > "$tmp_dir/all-orders.values"
printf '%s\n' \
  '{"order_id":1,"customer":"Ada","total":12.5}' \
  '{"order_id":2,"customer":"Grace","total":21}' \
  '{"order_id":1,"customer":"Ada","total":12.5,"currency":"USD"}' \
  '{"order_id":2,"customer":"Grace","total":21,"currency":"USD"}' |
  cmp - "$tmp_dir/all-orders.values"
printf 'PASS CSV fit and defaults\n'

"$WING" get orders --meta | "$WING" push orders-copy --meta >/dev/null
"$KITE" consume -B -n 2 --idle 3s --json orders |
  "$WING" read |
  jq -c '.value.total *= 2' |
  "$WING" write orders-copy |
  "$KITE" produce --json orders-copy >/dev/null 2>&1
"$KITE" consume -B -n 2 --idle 3s --json orders-copy > "$tmp_dir/orders-copy.jsonl"
"$WING" read < "$tmp_dir/orders-copy.jsonl" | jq -c .value > "$tmp_dir/orders-copy.values"
printf '%s\n' \
  '{"order_id":1,"customer":"Ada","total":25}' \
  '{"order_id":2,"customer":"Grace","total":42}' |
  cmp - "$tmp_dir/orders-copy.values"
cat "$tmp_dir/orders-copy.values"
printf 'PASS jq edit and re-validation\n'
