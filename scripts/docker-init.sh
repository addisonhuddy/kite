#!/usr/bin/env bash
# Run from the HOST after `docker compose up -d`: creates test topics and
# SCRAM users by execing into the running broker container (localhost:9092
# inside the container is the broker itself).
set -euo pipefail

C=${KANNON_CONTAINER:-kannon-kafka}
B=/opt/kafka/bin
BS=localhost:9092

docker exec "$C" $B/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
    --topic t1 --partitions 1 --replication-factor 1
docker exec "$C" $B/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
    --topic multi --partitions 4 --replication-factor 1
docker exec "$C" $B/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
    --topic t-ssl --partitions 2 --replication-factor 1
docker exec "$C" $B/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
    --topic t-sasl --partitions 2 --replication-factor 1
docker exec "$C" $B/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
    --topic t-scram --partitions 2 --replication-factor 1

# SCRAM users live in KRaft cluster metadata; PLAIN users are static in JAAS.
docker exec "$C" $B/kafka-configs.sh --bootstrap-server $BS \
    --alter --entity-type users --entity-name scram256 \
    --add-config 'SCRAM-SHA-256=[password=scram256-secret]'
docker exec "$C" $B/kafka-configs.sh --bootstrap-server $BS \
    --alter --entity-type users --entity-name scram512 \
    --add-config 'SCRAM-SHA-512=[password=scram512-secret]'

echo "init done"
