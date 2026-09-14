# Testing kite

## Unit tests

```console
$ zig build test
```

Covers the wire encoders (byte-exact varint/compact fixtures), record batch
CRC-32C, the properties parser, and the SCRAM implementation against the
RFC 7677 test vector.

## End-to-end: docker-compose Kafka 4.0.0

`docker-compose.yml` brings up a single-node KRaft broker (`apache/kafka:4.0.0`)
with four listeners:

| Port | Listener | Auth |
| --- | --- | --- |
| 9092 | PLAINTEXT | none |
| 9093 | SSL | TLS (test CA) |
| 9094 | SASL_SSL | TLS + SASL |
| 9095 | SASL_PLAINTEXT | SASL |

Both SASL listeners enable `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`.

### Setup

```console
$ scripts/gen-tls.sh         # one-time: creates docker/tls/ CA + broker PKCS12 keystore
$ docker compose up -d       # waits for the broker to become healthy
$ scripts/docker-init.sh     # creates topics + SCRAM users (execs into the broker)
```

Test credentials created by `docker-init.sh` / `kafka_server_jaas.conf`:

| User | Password | Mechanism |
| --- | --- | --- |
| `alice` | `alice-secret` | PLAIN |
| `admin` | `admin-secret` | PLAIN |
| `scram256` | `scram256-secret` | SCRAM-SHA-256 |
| `scram512` | `scram512-secret` | SCRAM-SHA-512 |

Topics: `t1` (1 partition), `multi` (4), `t-ssl` (2), `t-sasl` (2), `t-scram` (2).
Auto-creation is disabled — producing to an unknown topic fails fast.

### Smoke test

```console
$ zig build
$ scripts/smoke.sh
```

Exercises every listener end-to-end: plaintext produce/consume roundtrip,
multi-partition, unknown-topic error, TLS with the test CA, TLS cert-verification
failure, SASL PLAIN on both SASL listeners, bad-password failure, and
SCRAM-SHA-256/512.

The smoke script also includes Kite consumer checks. For compression-specific
Fetch coverage, produce batches with Kafka's `compression.type` set to
`none`, `gzip`, `snappy`, `lz4`, and `zstd`, then compare
`kite consume --from-beginning -t 3000 TOPIC | sort` with the input.

### Debugging

Set `KITE_DEBUG=1` to dump sent/received frames and TLS internals to stderr:

```console
$ KITE_DEBUG=1 sh -c 'echo hi | ./zig-out/bin/kite t1'
```

### Teardown

```console
$ docker compose down -v
```
