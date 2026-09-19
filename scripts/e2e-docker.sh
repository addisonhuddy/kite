#!/usr/bin/env bash
# End-to-end test against a real Kafka in Docker: single-node KRaft broker,
# then scripts/smoke.sh plus a few broker-dependent edge cases.
#
# Needs: docker, a built zig-out/bin/kite (built here if missing).
# Env: KITE_KAFKA_IMAGE (default apache/kafka:4.0.0),
#      KITE_E2E_TIMEOUT (broker readiness deadline, seconds; default 120),
#      KITE_E2E_KEEP=1 to keep the container running for debugging.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=${KITE_KAFKA_IMAGE:-apache/kafka:4.0.0}
NAME=kite-e2e-kafka
TOPIC=kite-e2e
TIMEOUT=${KITE_E2E_TIMEOUT:-120}
status=1

cleanup() {
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
# auto-created by produce, and `-B --idle 3s --json | jq -r .value` must print
# exactly the record just produced (a consume without -B starts at latest and
# prints nothing).
QS_TOPIC="kite-quickstart-$(date +%s)-$RANDOM"
printf 'hello\n' | zig-out/bin/kite "$QS_TOPIC"
if command -v jq >/dev/null 2>&1; then
    got=$(zig-out/bin/kite -c -B --idle 3s --json "$QS_TOPIC" 2>/dev/null | jq -r .value)
else
    got=$(zig-out/bin/kite -c -B --idle 3s --json "$QS_TOPIC" 2>/dev/null | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')
fi
[ "$got" = "hello" ] || fail "quickstart: expected 'hello' from the README consume line, got '$got'"
echo "PASS quickstart"

# Stopping bounds: `-n 1` on an empty topic bounds records, not time, so it
# must still be running after 3s; `-n 1 --idle 500ms` must stop on its own.
EMPTY_TOPIC="kite-empty-$(date +%s)-$RANDOM"
docker exec "$NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic "$EMPTY_TOPIC" \
    --partitions 1 --replication-factor 1 >/dev/null
set +e
timeout 3s zig-out/bin/kite -c -B -n 1 "$EMPTY_TOPIC" >/dev/null 2>&1
rc=$?
set -e
[ "$rc" -eq 124 ] || fail "bounds: 'kite -c -n 1' on an empty topic exited $rc within 3s; -n should only bound records"
set +e
timeout 10s zig-out/bin/kite -c -B -n 1 --idle 500ms "$EMPTY_TOPIC" >/dev/null 2>&1
rc=$?
set -e
[ "$rc" -eq 0 ] || fail "bounds: 'kite -c -n 1 --idle 500ms' on an empty topic exited $rc; expected a clean idle stop"
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
    timeout 20s zig-out/bin/kite -c -B --idle 2s --json "$1" 2>/dev/null \
        | sed -E 's/^\{"topic":"[^"]*","partition":[0-9]+,"offset":([0-9]+),"timestamp":[0-9]+,/{"offset":\1,/'
}
for fixture in users events; do
    file="examples/data/$fixture.csv"
    if [ "$fixture" = events ]; then keyopt=(--key user_id); records=8; else keyopt=(); records=6; fi
    t_file=$(csv_topic "file-$fixture")
    t_pipe=$(csv_topic "pipe-$fixture")
    t_slow=$(csv_topic "slow-$fixture")
    zig-out/bin/kite --csv "${keyopt[@]}" "$t_file" <"$file"
    cat "$file" | timeout 20s zig-out/bin/kite --csv "${keyopt[@]}" "$t_pipe" \
        || fail "csv-pipe: 'cat $file | kite --csv' exited $?"
    while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
        sleep 0.2
    done <"$file" | timeout 30s zig-out/bin/kite --csv "${keyopt[@]}" "$t_slow" \
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
zig-out/bin/kite -c -B -t 5s "$TOPIC" 2>"$err" | head -n 1 >/dev/null
rc=${PIPESTATUS[0]}
set -e
[ "$rc" -eq 0 ] || fail "early-pipe: kite -c exited $rc after head closed the pipe"
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
    zig-out/bin/kite -c -B -t 5s "$TOPIC" >/dev/full 2>"$err"
    rc=$?
    set -e
    [ "$rc" -ne 0 ] || fail "dev-full: kite -c exited 0 despite full sink"
    grep -q "kite: cannot write stdout" "$err" || {
        cat "$err"
        fail "dev-full: no 'cannot write stdout' error line"
    }
    rm -f "$err"
    echo "PASS dev-full"
else
    echo "SKIP dev-full"
fi

status=0
echo "ok: e2e against $IMAGE"
