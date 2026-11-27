#!/usr/bin/env bash
# End-to-end test against a real Kafka in Docker: single-node KRaft broker,
# then scripts/smoke.sh plus a few broker-dependent edge cases.
#
# Needs: docker, JDK 17+, a built zig-out/bin/kite (built here if missing).
# Env: KITE_KAFKA_IMAGE (default apache/kafka:4.0.0),
#      KITE_E2E_TIMEOUT (broker readiness deadline, seconds; default 120),
#      KITE_E2E_KEEP=1 to keep the container running for debugging.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=${KITE_KAFKA_IMAGE:-apache/kafka:4.0.0}
NAME=kite-e2e-kafka
TOPIC=kite-e2e
TIMEOUT=${KITE_E2E_TIMEOUT:-120}
TMP=$(mktemp -d)
status=1

cleanup() {
    rm -rf "$TMP"
    if [ "${KITE_E2E_KEEP:-0}" = 1 ]; then
        echo "keeping container $NAME (KITE_E2E_KEEP=1)"
        return
    fi
    if [ "$status" -ne 0 ]; then
        echo "== broker logs (tail) ==" >&2
        docker logs --tail 200 "$NAME" >&2 2>/dev/null || true
    fi
    docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() { echo "FAIL $1"; exit 1; }

echo "== pull $IMAGE =="
docker pull "$IMAGE" >/dev/null || fail "image-pull: cannot pull $IMAGE"

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --rm --name "$NAME" -p 127.0.0.1:9092:9092 "$IMAGE" >/dev/null

echo "== wait for broker (up to ${TIMEOUT}s) =="
deadline=$((SECONDS + TIMEOUT))
ready=0
while [ "$SECONDS" -lt "$deadline" ]; do
    if docker exec "$NAME" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 2
done
[ "$ready" -eq 1 ] || fail "readiness: broker did not answer kafka-topics --list within ${TIMEOUT}s"

docker exec "$NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic "$TOPIC" \
    --partitions 1 --replication-factor 1 >/dev/null

export BOOTSTRAP_SERVERS=127.0.0.1:9092
[ -x zig-out/bin/kite ] || zig build

echo "== smoke.sh =="
scripts/smoke.sh "$TOPIC"

# README Quickstart, verbatim apart from the topic name: a fresh topic is
# auto-created by produce, and `-B -n 1 --idle 3s --json | jq -r .value` must print
# exactly the record just produced (a consume without -B starts at latest and
# prints nothing).
QS_TOPIC="kite-quickstart-$(date +%s)-$RANDOM"
printf 'hello\n' | zig-out/bin/kite produce "$QS_TOPIC"
if command -v jq >/dev/null 2>&1; then
    got=$(zig-out/bin/kite consume -B -n 1 --idle 3s --json "$QS_TOPIC" 2>/dev/null | jq -r .value)
else
    got=$(zig-out/bin/kite consume -B -n 1 --idle 3s --json "$QS_TOPIC" 2>/dev/null | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')
fi
[ "$got" = "hello" ] || fail "quickstart: expected 'hello' from the README consume line, got '$got'"
echo "PASS quickstart"

# Invalid UTF-8 in record fields must survive a jq pass and a JSON produce.
B64_SRC="kite-b64-src-$(date +%s)-$RANDOM"
B64_DST="kite-b64-dst-$(date +%s)-$RANDOM"
printf '%s\n' '{"key_b64":"gP9r","value_b64":"AYD+/w==","headers":[{"key":"trace","value_b64":"gGg="}]}' \
    | zig-out/bin/kite produce --json "$B64_SRC"
zig-out/bin/kite consume -B -n 1 --idle 3s --json "$B64_SRC" 2>/dev/null \
    | jq -c . \
    | zig-out/bin/kite produce --json "$B64_DST"
source_bytes=$(zig-out/bin/kite consume -B -n 1 --idle 3s --json "$B64_SRC" 2>/dev/null \
    | jq -c '[.key_b64,.headers,.value_b64]')
destination_bytes=$(zig-out/bin/kite consume -B -n 1 --idle 3s --json "$B64_DST" 2>/dev/null \
    | jq -c '[.key_b64,.headers,.value_b64]')
[ "$destination_bytes" = "$source_bytes" ] || fail "json-base64: bytes changed across consume | jq | produce"
echo "PASS json-base64 jq roundtrip"

expect_json_produce_error() {
    local input=$1 expected=$2 label=$3 rc
    set +e
    printf '%s\n' "$input" | zig-out/bin/kite produce --json "$B64_SRC" >"$TMP/$label.out" 2>"$TMP/$label.err"
    rc=$?
    set -e
    [ "$rc" -eq 1 ] && grep -Fq "$expected" "$TMP/$label.err" || {
        echo "FAIL $label: expected status 1 and '$expected'"
        cat "$TMP/$label.err"
        exit 1
    }
}
expect_json_produce_error '{"value_b64":"!!!!"}' 'line 1: "value_b64" is not valid base64' invalid-value-base64
expect_json_produce_error '{"key_b64":"????","value":"x"}' 'line 1: "key_b64" is not valid base64' invalid-key-base64
expect_json_produce_error '{"value":"x","value_b64":"eA=="}' 'line 1: use "value" or "value_b64", not both' conflicting-value-fields
expect_json_produce_error '{"key":"k","key_b64":"aw==","value":"x"}' 'line 1: use "key" or "key_b64", not both' conflicting-key-fields
expect_json_produce_error '{"value":"x","headers":[{"key":"h","value":"x","value_b64":"eA=="}]}' \
    'line 1: use "value" or "value_b64", not both' conflicting-header-fields
expect_json_produce_error '{"key":"k"}' 'JSON object has no "value" or "value_b64" field' missing-json-value
echo "PASS json-base64 input errors"

# Stopping bounds: `-n 1` on an empty topic bounds records, not time, so it
# must still be running after 3s; `-n 1 --idle 500ms` must stop on its own.
EMPTY_TOPIC="kite-empty-$(date +%s)-$RANDOM"
docker exec "$NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic "$EMPTY_TOPIC" \
    --partitions 1 --replication-factor 1 >/dev/null
set +e
timeout 3s zig-out/bin/kite consume -B -n 1 "$EMPTY_TOPIC" >/dev/null 2>&1
rc=$?
set -e
[ "$rc" -eq 124 ] || fail "bounds: 'kite consume -n 1' on an empty topic exited $rc within 3s; -n should only bound records"
set +e
timeout 10s zig-out/bin/kite consume -B -n 1 --idle 500ms "$EMPTY_TOPIC" >/dev/null 2>&1
rc=$?
set -e
[ "$rc" -eq 0 ] || fail "bounds: 'kite consume -n 1 --idle 500ms' on an empty topic exited $rc; expected a clean idle stop"
echo "PASS bounds"

# CSV over a pipe must yield the same records as file redirection (a pipe's
# first read is not seekable and used to be mistaken for EOF), including
# --key column selection; a slow, chunked writer must not truncate or hang.
csv_topic() {
    local t="kite-csv-$1-$(date +%s)-$RANDOM"
    docker exec "$NAME" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --create --topic "$t" \
        --partitions 1 --replication-factor 1 >/dev/null
    echo "$t"
}
# One JSON line per record with the per-topic/per-run fields dropped so two
# topics can be compared (values with embedded newlines stay on one line).
csv_dump() {
    timeout 20s zig-out/bin/kite consume -B --idle 2s --json "$1" 2>/dev/null \
        | sed -E 's/^\{"topic":"[^"]*","partition":[0-9]+,"offset":([0-9]+),"timestamp":[0-9]+,/{"offset":\1,/'
}
for fixture in users events; do
    file="examples/data/$fixture.csv"
    if [ "$fixture" = events ]; then keyopt=(--key user_id); records=8; else keyopt=(); records=6; fi
    t_file=$(csv_topic "file-$fixture")
    t_pipe=$(csv_topic "pipe-$fixture")
    t_slow=$(csv_topic "slow-$fixture")
    zig-out/bin/kite produce --csv "${keyopt[@]}" "$t_file" <"$file"
    cat "$file" | timeout 20s zig-out/bin/kite produce --csv "${keyopt[@]}" "$t_pipe" \
        || fail "csv-pipe: 'cat $file | kite produce --csv' exited $?"
    while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
        sleep 0.2
    done <"$file" | timeout 30s zig-out/bin/kite produce --csv "${keyopt[@]}" "$t_slow" \
        || fail "csv-pipe: slow chunked pipe for $file exited $?"
    want=$(csv_dump "$t_file")
    n=$(printf '%s\n' "$want" | grep -c .)
    [ "$n" -eq "$records" ] || fail "csv-pipe: file redirection of $file produced $n records, expected $records"
    [ "$(csv_dump "$t_pipe")" = "$want" ] || fail "csv-pipe: pipe input of $file differs from file redirection"
    [ "$(csv_dump "$t_slow")" = "$want" ] || fail "csv-pipe: slow pipe input of $file differs from file redirection"
    if [ "$fixture" = events ]; then
        printf '%s\n' "$want" | grep -qF '"key":"u-1","headers":[],"value":"{\"event_id\":\"e-1\",\"user_id\":\"u-1\"' \
            || fail "csv-pipe: --key user_id did not set the key / keep the user_id field"
    fi
done
echo "PASS csv-pipe"

# Early pipe closure: once `head` exits, kite sees the closed sink and must
# exit 0 without SIGPIPE noise on stderr (README: status 0).
err=$(mktemp)
set +e
zig-out/bin/kite consume -B -t 5s "$TOPIC" 2>"$err" | head -n 1 >/dev/null
rc=${PIPESTATUS[0]}
set -e
[ "$rc" -eq 0 ] || fail "early-pipe: kite consume exited $rc after head closed the pipe"
if grep -qiE 'sigpipe|broken pipe' "$err"; then
    cat "$err"
    fail "early-pipe: SIGPIPE noise on stderr"
fi
rm -f "$err"
echo "PASS early-pipe"

# /dev/full: a write error must surface as a nonzero exit with a kite: line.
if [ -w /dev/full ] && ! echo probe >/dev/full 2>/dev/null; then
    err=$(mktemp)
    set +e
    zig-out/bin/kite consume -B -t 5s "$TOPIC" >/dev/full 2>"$err"
    rc=$?
    set -e
    [ "$rc" -ne 0 ] || fail "dev-full: kite consume exited 0 despite full sink"
    grep -q "kite: cannot write stdout" "$err" || {
        cat "$err"
        fail "dev-full: no 'cannot write stdout' error line"
    }
    rm -f "$err"
    echo "PASS dev-full"
else
    echo "SKIP dev-full"
fi

# ---------------------------------------------------------------- topics
echo "== topic management =="
TT=kite-topic-e2e
TERR=$(mktemp)
zig-out/bin/kite topic delete -y --if-exists "$TT" >/dev/null 2>&1 || true

zig-out/bin/kite topic create -p 3 -r 1 --set retention.ms=3600000 "$TT" 2>"$TERR" || {
    cat "$TERR"; fail "topic: create -p 3 -r 1 failed";
}
zig-out/bin/kite topic list | grep -qx "$TT" || fail "topic: $TT missing from 'topic list'"
line=$(zig-out/bin/kite topic list --json | grep "\"name\":\"$TT\"")
{ grep -q '"partitions":3' <<<"$line" &&
  grep -q '"replication_factor":1' <<<"$line" &&
  grep -q '"internal":false' <<<"$line"; } \
    || fail "topic: list --json row wrong: $line"

set +e
err=$(zig-out/bin/kite topic create "$TT" 2>&1 >/dev/null)
rc=$?
set -e
[ "$rc" -eq 1 ] && grep -q "topic '$TT' already exists" <<<"$err" \
    || fail "topic: second create exited $rc, want 1 'already exists' ($err)"
zig-out/bin/kite topic create --if-not-exists "$TT" 2>/dev/null \
    || fail "topic: create --if-not-exists did not exit 0"

zig-out/bin/kite topic update --partitions 5 "$TT" 2>/dev/null \
    || fail "topic: update --partitions 5 failed"
zig-out/bin/kite topic list --json | grep "\"name\":\"$TT\"" | grep -q '"partitions":5' \
    || fail "topic: partitions not 5 after update"

zig-out/bin/kite topic update --set retention.ms=7200000 "$TT" 2>/dev/null \
    || fail "topic: update --set failed"
docker exec "$NAME" /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server localhost:9092 --entity-type topics \
    --entity-name "$TT" --describe 2>/dev/null | grep -q 'retention.ms=7200000' \
    || fail "topic: retention.ms=7200000 not visible to kafka-configs"
zig-out/bin/kite topic update --unset retention.ms "$TT" 2>/dev/null \
    || fail "topic: update --unset failed"
if docker exec "$NAME" /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server localhost:9092 --entity-type topics \
    --entity-name "$TT" --describe 2>/dev/null | grep -q 'retention.ms='; then
    fail "topic: retention.ms still set after --unset"
fi

set +e
err=$(zig-out/bin/kite topic update --partitions 2 "$TT" 2>&1 >/dev/null)
rc=$?
set -e
[ "$rc" -eq 1 ] && grep -q "INVALID_PARTITIONS" <<<"$err" \
    || fail "topic: shrink exited $rc, want 1 INVALID_PARTITIONS ($err)"

zig-out/bin/kite topic delete -y "$TT" 2>/dev/null || fail "topic: delete -y failed"
gone=0
for _ in 1 2 3 4 5 6 7 8 9 10; do
    zig-out/bin/kite topic list | grep -qx "$TT" || { gone=1; break; }
    sleep 1
done
[ "$gone" -eq 1 ] || fail "topic: $TT still in list after delete"
set +e
err=$(zig-out/bin/kite topic delete -y "$TT" 2>&1 >/dev/null)
rc=$?
set -e
[ "$rc" -eq 1 ] && grep -q "topic '$TT' does not exist" <<<"$err" \
    || fail "topic: second delete exited $rc, want 1 'does not exist' ($err)"
zig-out/bin/kite topic delete -y --if-exists "$TT" 2>/dev/null \
    || fail "topic: delete --if-exists did not exit 0"

# Internal topics are hidden by default and shown with -a (the earlier
# consume tests created __consumer_offsets on this broker).
if zig-out/bin/kite topic list -a | grep -qx '__consumer_offsets'; then
    if zig-out/bin/kite topic list | grep -qx '__consumer_offsets'; then
        fail "topic: __consumer_offsets visible without -a"
    fi
else
    echo "SKIP topic: __consumer_offsets assertion (broker has not created it)"
fi

# A freshly created topic is consumable immediately (waitForTopic poll).
zig-out/bin/kite topic create "$TT" 2>/dev/null || fail "topic: recreate failed"
timeout 15s zig-out/bin/kite consume -n 0 --idle 500ms "$TT" >/dev/null 2>&1 \
    || fail "topic: consume on fresh topic failed"
zig-out/bin/kite topic delete -y "$TT" >/dev/null 2>&1 || true
rm -f "$TERR"
echo "PASS topic"

echo "== transactional produce =="
TXN_TOPIC=kite-txn-e2e
zig-out/bin/kite topic delete -y --if-exists "$TXN_TOPIC" >/dev/null 2>&1 || true
zig-out/bin/kite topic create -p 3 -r 1 "$TXN_TOPIC" >/dev/null 2>&1 \
    || fail "txn: create $TXN_TOPIC failed"

printf 'k1\tv1\nk2\tv2\nk3\tv3\nk4\tv4\nk5\tv5\nk6\tv6\n' \
    | zig-out/bin/kite produce --txn kite-e2e-tx1 "$TXN_TOPIC" 2>/dev/null \
    || fail "txn: committed produce failed"
got=$(zig-out/bin/kite consume -B -n 6 --idle 3s --format value "$TXN_TOPIC" 2>/dev/null | sort)
want=$(printf 'v1\nv2\nv3\nv4\nv5\nv6')
[ "$got" = "$want" ] || fail "txn: committed consume mismatch: got=[$got]"

# Empty stdin with --txn succeeds cleanly.
zig-out/bin/kite produce --txn kite-e2e-tx1 "$TXN_TOPIC" </dev/null 2>/dev/null \
    || fail "txn: empty stdin failed"

# Reusing the same transactional ID commits a second transaction.
printf 'k7\tv7\n' \
    | zig-out/bin/kite produce --txn kite-e2e-tx1 "$TXN_TOPIC" 2>/dev/null \
    || fail "txn: second run with same --txn ID failed"
got=$(zig-out/bin/kite consume -B -n 7 --idle 3s --format value "$TXN_TOPIC" 2>/dev/null | sort)
want=$(printf 'v1\nv2\nv3\nv4\nv5\nv6\nv7')
[ "$got" = "$want" ] || fail "txn: second commit mismatch: got=[$got]"

# Aborted run (mid-stream parse error after linger flushes first batch):
# read_committed sees nothing; --read-uncommitted sees the flushed records.
TXN_ABORT=kite-txn-abort-e2e
zig-out/bin/kite topic delete -y --if-exists "$TXN_ABORT" >/dev/null 2>&1 || true
zig-out/bin/kite topic create -p 3 -r 1 "$TXN_ABORT" >/dev/null 2>&1 \
    || fail "txn: create $TXN_ABORT failed"
set +e
(
    printf '{"key":"a","value":"before-err-1"}\n{"key":"b","value":"before-err-2"}\n'
    sleep 0.3
    printf 'not-valid-json\n'
) | zig-out/bin/kite produce --txn kite-e2e-abort --json "$TXN_ABORT" 2>/dev/null
rc=$?
set -e
wait_complete_abort() {
    local tid=$1
    local desc=""
    for _ in $(seq 1 75); do
        desc=$(docker exec "$NAME" /opt/kafka/bin/kafka-transactions.sh --bootstrap-server localhost:9092 describe --transactional-id "$tid" 2>/dev/null || true)
        if echo "$desc" | grep -q "CompleteAbort"; then
            return 0
        fi
        sleep 0.2
    done
    fail "txn: $tid did not reach CompleteAbort: [$desc]"
}

[ "$rc" -eq 1 ] || fail "txn: aborted produce exited $rc, want 1"
wait_complete_abort kite-e2e-abort
printf '{"key":"a","value":"sentinel-a"}\n{"key":"b","value":"sentinel-b"}\n' \
    | zig-out/bin/kite produce --txn kite-e2e-abort-sentinel --json "$TXN_ABORT" >/dev/null 2>&1 \
    || fail "txn: committed sentinel produce to $TXN_ABORT failed"
rc_out=$(zig-out/bin/kite consume -B -n 2 --idle 3s --format value "$TXN_ABORT" 2>/dev/null | sort)
rc_want=$(printf 'sentinel-a\nsentinel-b')
[ "$rc_out" = "$rc_want" ] || fail "txn: read_committed did not see only sentinels after abort: [$rc_out]"
ru_out=$(zig-out/bin/kite consume -B --read-uncommitted -n 4 --idle 3s --format value "$TXN_ABORT" 2>/dev/null | sort)
ru_want=$(printf 'before-err-1\nbefore-err-2\nsentinel-a\nsentinel-b')
[ "$ru_out" = "$ru_want" ] || fail "txn: read_uncommitted did not see aborted + sentinel records: [$ru_out]"

# Aborted run on SIGINT after records flushed: read_committed sees only later sentinels.
TXN_INT=kite-txn-sigint-e2e
zig-out/bin/kite topic delete -y --if-exists "$TXN_INT" >/dev/null 2>&1 || true
zig-out/bin/kite topic create -p 3 -r 1 "$TXN_INT" >/dev/null 2>&1 \
    || fail "txn: create $TXN_INT failed"
TXN_FIFO=$(mktemp -u)
mkfifo "$TXN_FIFO"
zig-out/bin/kite produce --txn kite-e2e-sigint "$TXN_INT" <"$TXN_FIFO" 2>/dev/null &
TXN_PID=$!
exec 3>"$TXN_FIFO"
printf 'k1\tbefore-int-1\nk2\tbefore-int-2\n' >&3
sleep 0.3
printf 'k3\tpartial-without-newline' >&3
sleep 0.1
kill -INT "$TXN_PID" 2>/dev/null || true
set +e
wait "$TXN_PID"
rc=$?
set -e
exec 3>&-
rm -f "$TXN_FIFO"
[ "$rc" -eq 130 ] || fail "txn: SIGINT produce exited $rc, want 130"
wait_complete_abort kite-e2e-sigint
printf 'k1\tsentinel-int-1\nk2\tsentinel-int-2\n' \
    | zig-out/bin/kite produce --txn kite-e2e-sigint-sentinel "$TXN_INT" >/dev/null 2>&1 \
    || fail "txn: committed sentinel produce to $TXN_INT failed"
rc_out=$(zig-out/bin/kite consume -B -n 2 --idle 3s --format value "$TXN_INT" 2>/dev/null | sort)
rc_want=$(printf 'sentinel-int-1\nsentinel-int-2')
[ "$rc_out" = "$rc_want" ] || fail "txn: read_committed did not see only sentinels after SIGINT abort: [$rc_out]"
ru_out=$(zig-out/bin/kite consume -B --read-uncommitted -n 4 --idle 3s --format value "$TXN_INT" 2>/dev/null | sort)
ru_want=$(printf 'before-int-1\nbefore-int-2\nsentinel-int-1\nsentinel-int-2')
[ "$ru_out" = "$ru_want" ] || fail "txn: read_uncommitted did not see SIGINT-aborted + sentinel records: [$ru_out]"

python3 scripts/check-txn-faults.py "$PWD/zig-out/bin/kite" "$NAME"

zig-out/bin/kite topic delete -y "$TXN_TOPIC" "$TXN_ABORT" "$TXN_INT" >/dev/null 2>&1 || true
echo "PASS txn"

echo "== consumer groups =="
GROUP_LOG=${KITE_GROUP_LOG_DIR:-$HOME/kite-group-e2e-$(date +%s)}
mkdir -p "$GROUP_LOG"
echo "group logs: $GROUP_LOG"
python3 scripts/check-groups.py "$PWD/zig-out/bin/kite" "$GROUP_LOG"

status=0
echo "ok: e2e against $IMAGE"
