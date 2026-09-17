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
if [ -w /dev/full ] && ! : >/dev/full 2>/dev/null; then
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
