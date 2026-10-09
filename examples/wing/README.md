# kite + wing: JSON Schema records

This walkthrough runs from the kite repository root and needs kite v0.4.0+
and wing v0.1.0+. kite moves Kafka records; wing handles Confluent Schema
Registry and JSON Schema validation. They have separate configuration:
`BOOTSTRAP_SERVERS` or `kite.yaml` for kite, and `SCHEMA_REGISTRY_URL` or
`wing.yaml` for wing.

## 0. Start Kafka and Schema Registry

Start a local Kafka broker and Registry so the following commands can reach
`localhost:9092` and `localhost:8081`.

```sh
docker network create wing-local
docker run -d --name wing-local-kafka --network wing-local \
  -p 127.0.0.1:9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@wing-local-kafka:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENERS=INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://wing-local-kafka:29092,EXTERNAL://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka-native:latest
docker run -d --name wing-local-sr --network wing-local \
  -p 127.0.0.1:8081:8081 \
  -e SCHEMA_REGISTRY_HOST_NAME=wing-local-sr \
  -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://wing-local-kafka:29092 \
  mirror.gcr.io/confluentinc/cp-schema-registry:8.0.0
until curl -fsS http://localhost:8081/subjects >/dev/null; do sleep 2; done
curl -fsS http://localhost:8081/subjects  # prints: []
```

## 1. Install kite and wing

Install kite and wing, then configure the Kafka broker and Registry
independently.

```sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/wing/main/install.sh | sh
export BOOTSTRAP_SERVERS=localhost:9092 SCHEMA_REGISTRY_URL=http://localhost:8081
kite --version  # kite 0.4.0 or later
wing -V  # wing 0.1.0 or later
```

## 2. Lint the schema offline

Check the JSON Schema before making a Registry request.

```sh
wing push --check < examples/wing/orders.schema.json
# prints: wing push: ok
```

## 3. Register the value schema

Register the schema under the `orders-value` subject used by TopicNameStrategy.

```sh
wing push orders < examples/wing/orders.schema.json
# prints the new schema GUID
```

## 4. Validate and produce JSONL orders

`wing write orders` accepts bare JSON objects, validates each value, and adds
the Confluent GUID header before kite produces the JSON records.

```sh
kite topic create --if-not-exists orders
# prints: kite: created topic 'orders' with 1 partition(s), replication factor 1
wing write orders < examples/wing/orders.jsonl | kite produce --json orders
# prints: wing write: 2 written, 0 fitted
# prints: 2 record(s) produced to 'orders' across 1 of 1 partition(s)
```

## 5. Reject an invalid record

The invalid `order_id` is rejected with exit 2; this single bad record emits
no stdout for kite to produce.

```sh
set -o pipefail
printf '%s\n' '{"order_id":"three","customer":"Lin","total":5}' |
  wing write orders | kite produce --json orders
# wing write: line 1: /order_id: expected integer, got string [/properties/order_id/type]
# exit 2
```

This one-record input sends nothing; with mixed valid and invalid input,
earlier valid records may already be produced before the invalid line is
rejected.

## 6. Consume and decode

Read the two produced records from the beginning, resolve their schema
headers, and keep only each decoded value.

```sh
kite consume -B -n 2 --idle 3s --json orders | wing read | jq -c .value
# prints: {"order_id":1,"customer":"Ada","total":12.5}
# prints: {"order_id":2,"customer":"Grace","total":21}
```

## 7. Fit CSV values to the schema

CSV columns arrive as strings; `--fit` coerces their types and fills the
schema's `currency` default before producing to `orders`.

```sh
kite topic create --if-not-exists orders-raw
kite produce --csv orders-raw < examples/wing/orders.csv
kite consume -B -n 2 --idle 3s --json orders-raw |
  wing write orders --fit | kite produce --json orders
kite consume -B -n 4 --idle 3s --json orders | wing read | jq -c .value
# prints: {"order_id":1,"customer":"Ada","total":12.5}
# prints: {"order_id":2,"customer":"Grace","total":21}
# prints: {"order_id":1,"customer":"Ada","total":12.5,"currency":"USD"}
# prints: {"order_id":2,"customer":"Grace","total":21,"currency":"USD"}
```

## 8. Edit with jq and re-validate

Copy the schema to the destination topic's subject, then double each decoded
total with jq and validate the edited values before producing them.

```sh
wing get orders --meta | wing push orders-copy --meta
# prints the new schema GUID
kite topic create --if-not-exists orders-copy
kite consume -B -n 2 --idle 3s --json orders |
  wing read | jq -c '.value.total *= 2' |
  wing write orders-copy | kite produce --json orders-copy
kite consume -B -n 2 --idle 3s --json orders-copy | wing read | jq -c .value
# prints: {"order_id":1,"customer":"Ada","total":25}
# prints: {"order_id":2,"customer":"Grace","total":42}
```

## 9. Clean up

Remove the example topics and Registry subjects, then remove only the Docker
containers and network created in step 0.

```sh
kite topic delete -y --if-exists orders orders-raw orders-copy
wing rm orders-value -y
wing rm orders-copy-value -y
docker rm -f wing-local-sr wing-local-kafka
docker network rm wing-local
```

Run all checks and pipelines in steps 2–8 again with
[`run.sh`](run.sh); it cleans its topics and subjects before each run.
