#!/usr/bin/env bash
# Smoke test: exercises kannon against the docker-compose Kafka on all four
# listeners. Run from the repo root after `docker compose up -d` +
# `scripts/docker-init.sh` and `zig build`.
set -euo pipefail
cd "$(dirname "$0")/.."

ROOT=$PWD
K=$ROOT/zig-out/bin/kannon
C=kannon-kafka
B=/opt/kafka/bin
M=smoke-$(date +%s)-$RANDOM # unique marker so pre-existing records don't collide
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

consume() { # topic, flags... — prints all records sorted
    local topic=$1; shift
    docker exec "$C" $B/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 --topic "$topic" \
        --from-beginning --timeout-ms 8000 "$@" 2>/dev/null
}

write_props() { printf '%s\n' "$@" > "$TMP/kannon.properties"; }
expect_err() { # runs kannon, expects failure matching a pattern in stderr
    if (cd "$TMP" && printf 'x\n' | "$K" "$1") 2>"$TMP/err"; then
        echo "FAIL: expected kannon to fail on $1"; exit 1
    fi
    grep -qi "$2" "$TMP/err" || { echo "FAIL: stderr was: $(cat "$TMP/err")"; exit 1; }
    echo ok
}

echo "== 1. plaintext produce + consume =="
(cd "$TMP" && write_props bootstrap.servers=localhost:9092 security.protocol=PLAINTEXT)
(cd "$TMP" && printf "$M-a\n$M-b\n$M-c\n" | "$K" t1)
consume t1 | grep -c "$M" | grep -q '^3$' || { echo "FAIL"; exit 1; }
echo ok

echo "== 2. multi-partition topic =="
(cd "$TMP" && seq 1 10 | sed "s/^/$M-/" | "$K" multi)
consume multi | grep -c "$M" | grep -q '^10$' || { echo "FAIL"; exit 1; }
echo ok

echo "== 3. unknown topic fails fast =="
expect_err nosuchtopic "does not exist"

echo "== 4. SSL with test CA =="
docker cp docker/tls/ca.crt "$C":/tmp/ca.crt
docker exec "$C" bash -c 'printf "security.protocol=SSL\nssl.truststore.location=/tmp/ca.crt\nssl.truststore.type=PEM\n" > /tmp/ssl.props'
(cd "$TMP" && write_props bootstrap.servers=localhost:9093 security.protocol=SSL \
    "ssl.truststore.location=$ROOT/docker/tls/ca.crt")
(cd "$TMP" && printf "$M-tls1\n$M-tls2\n" | "$K" t-ssl)
docker exec "$C" $B/kafka-console-consumer.sh --bootstrap-server localhost:9093 \
    --topic t-ssl --from-beginning --timeout-ms 8000 --consumer.config /tmp/ssl.props 2>/dev/null \
    | grep -c "$M" | grep -q '^2$' || { echo "FAIL"; exit 1; }
echo ok

echo "== 5. SSL without CA fails cert verification =="
(cd "$TMP" && write_props bootstrap.servers=localhost:9093 security.protocol=SSL)
expect_err t-ssl "certificate verification failed"

echo "== 6. SASL_PLAINTEXT + PLAIN =="
docker exec "$C" bash -c 'printf "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"alice-secret\";\n" > /tmp/p.props'
(cd "$TMP" && write_props bootstrap.servers=localhost:9095 security.protocol=SASL_PLAINTEXT \
    sasl.mechanism=PLAIN sasl.username=alice sasl.password=alice-secret)
(cd "$TMP" && printf "$M-p1\n" | "$K" t-sasl)
docker exec "$C" $B/kafka-console-consumer.sh --bootstrap-server localhost:9095 \
    --topic t-sasl --from-beginning --timeout-ms 8000 --consumer.config /tmp/p.props 2>/dev/null \
    | grep -c "$M-p1" | grep -q '^1$' || { echo "FAIL"; exit 1; }
echo ok

echo "== 7. SASL_SSL + PLAIN =="
(cd "$TMP" && write_props bootstrap.servers=localhost:9094 security.protocol=SASL_SSL \
    sasl.mechanism=PLAIN sasl.username=alice sasl.password=alice-secret \
    "ssl.truststore.location=$ROOT/docker/tls/ca.crt")
(cd "$TMP" && printf "$M-ps1\n" | "$K" t-sasl)
echo ok

echo "== 8. bad SASL password fails cleanly =="
(cd "$TMP" && write_props bootstrap.servers=localhost:9095 security.protocol=SASL_PLAINTEXT \
    sasl.mechanism=PLAIN sasl.username=alice sasl.password=wrong)
expect_err t-sasl "auth"

echo "== 9. SCRAM-SHA-256 =="
(cd "$TMP" && write_props bootstrap.servers=localhost:9095 security.protocol=SASL_PLAINTEXT \
    sasl.mechanism=SCRAM-SHA-256 sasl.username=scram256 sasl.password=scram256-secret)
(cd "$TMP" && printf "$M-r256\n" | "$K" t-scram)
docker exec "$C" bash -c 'printf "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-256\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"scram256\" password=\"scram256-secret\";\n" > /tmp/s256.props'
docker exec "$C" $B/kafka-console-consumer.sh --bootstrap-server localhost:9095 \
    --topic t-scram --from-beginning --timeout-ms 8000 --consumer.config /tmp/s256.props 2>/dev/null \
    | grep -c "$M-r256" | grep -q '^1$' || { echo "FAIL"; exit 1; }
echo ok

echo "== 10. SCRAM-SHA-512 =="
(cd "$TMP" && write_props bootstrap.servers=localhost:9095 security.protocol=SASL_PLAINTEXT \
    sasl.mechanism=SCRAM-SHA-512 sasl.username=scram512 sasl.password=scram512-secret)
(cd "$TMP" && printf "$M-r512\n" | "$K" t-scram)
echo ok

echo "== 11. keys + headers (-H flag and inline <TAB> fields) =="
(cd "$TMP" && write_props bootstrap.servers=localhost:9092)
(cd "$TMP" && printf "$M-k\th1: v1\t$M-v1\n$M-pure\n" | "$K" -H "static-h: $M-sv" t1)
consume t1 --property print.key=true --property print.headers=true \
    --property key.separator='|' --property headers.delimiter=';' \
    | grep "$M" | grep -qF "static-h:$M-sv,h1:v1|$M-k|$M-v1" || { echo "FAIL"; exit 1; }
consume t1 --property print.key=true --property print.headers=true \
    --property key.separator='|' --property headers.delimiter=';' \
    | grep -qF "static-h:$M-sv|null|$M-pure" || { echo "FAIL"; exit 1; }
echo ok

echo "ALL SMOKE TESTS PASSED"
