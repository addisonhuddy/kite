# kannon

An ultra-lightweight Kafka producer CLI written in Zig. `kannon` reads lines
from stdin and produces each line as a record to a topic:

```console
$ kannon my-topic < file.txt
3 record(s) produced to 'my-topic'
```

No JVM, no librdkafka, no dependencies — a single static binary under 1 MiB.
Speaks the Kafka wire protocol directly: flexible versions only, record batch
v2, acks=all. **Requires Kafka 4.0+** (KRaft) on the broker side.

## Build

Requires Zig 0.15.x:

```console
$ zig build            # produces zig-out/bin/kannon (ReleaseSmall, stripped)
$ scripts/check-size.sh   # hard gate: fails if the binary is >= 1 MiB
```

Cross-compile for other targets, e.g.:

```console
$ zig build -Dtarget=aarch64-macos     # Apple Silicon
$ zig build -Dtarget=x86_64-macos      # Intel Mac
```

## Usage

```console
$ kannon [-H 'name: value']... <topic>   # one record per stdin line
$ echo hello | kannon my-topic
```

Each stdin line is one record. A plain line is value-only; a TAB separates
the line into fields: first field = record key, last = value, any middle
fields are per-record `name: value` headers.

```console
$ printf 'value only\n' | kannon t1
$ printf 'key\tvalue\n' | kannon t1                    # key + value
$ printf 'key\ttrace-id: 42\tsrc: cli\tvalue\n' | kannon t1   # key + headers + value
```

`-H 'name: value'` (repeatable, curl-style) attaches a header to every
record:

```console
$ kannon -H 'source: import-job' -H 'env: prod' my-topic < file.txt
```

- The final line is produced even without a trailing newline.
- Records are buffered per partition and flushed when a `batch.size` cap is
  hit, after a `linger.ms` linger, or at EOF.
- Keyed records partition by murmur2 (like Kafka's default partitioner);
  unkeyed records round-robin so all partitions fill together.
- Produces are pipelined: one connection per partition with up to ~96 MiB
  in flight before acks are awaited.
- `acks=-1`; retriable errors are retried with exponential backoff and a
  metadata refresh. Retried records may reorder within their partition.
- Idempotent produce is on by default (`enable.idempotence`): the producer
  gets a producer id/epoch via InitProducerId, each batch carries a
  per-partition sequence number, and at most 5 requests stay un-acked per
  partition — broker dedup makes retries exactly-once. Set
  `enable.idempotence=false` to disable.

## Configuration

kannon reads `kannon.properties` (Java properties format, `key=value` lines,
`#`/`!` comments). Search order — first match wins:

1. `./kannon.properties` (current directory)
2. `$XDG_CONFIG_HOME/kannon/kannon.properties`
3. `~/.config/kannon/kannon.properties`

| Key | Required | Values |
| --- | --- | --- |
| `bootstrap.servers` | yes | Comma-separated `host:port` list. Each is tried in order; the first reachable broker is used for bootstrapping. |
| `security.protocol` | no (default `PLAINTEXT`) | `PLAINTEXT`, `SSL`, `SASL_SSL`, `SASL_PLAINTEXT` |
| `sasl.mechanism` | required for `SASL_*` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `sasl.username` | required for `SASL_*` | |
| `sasl.password` | required for `SASL_*` | |
| `ssl.truststore.location` | optional for `SSL`/`SASL_SSL` | Path to a PEM CA bundle. Falls back to the system trust store when unset. |
| `batch.size` | no (default `1048576`) | Per-partition record buffer cap in bytes — flush when exceeded. |
| `linger.ms` | no (default `50`) | Flush pending records after this delay when stdin stalls. |
| `enable.idempotence` | no (default `true`) | Idempotent producer: producer id + per-partition sequences, exactly-once on retry. |

Unknown keys are ignored with a warning, so a shared `server.properties`-style
file works.

## Diagnostics

All diagnostics go to stderr; stdout carries only the final
`N record(s) produced to '<topic>'` line.

- `-v` / `--verbose` — connection lifecycle (bootstrap, per-broker connects,
  drops), partition counts, the assigned producer id, and every retry attempt
  with its backoff.
- `KANNON_DEBUG=1` — hex-dumps outbound request frames and logs TLS
  handshake errors.
- `KANNON_TIME=1` — prints `read/send/drain` millisecond totals and the
  connection count on exit (perf tuning).

## Examples

PLAINTEXT:

```properties
bootstrap.servers=localhost:9092
security.protocol=PLAINTEXT
```

TLS with a custom CA:

```properties
bootstrap.servers=kafka.example.com:9093
security.protocol=SSL
ssl.truststore.location=/path/to/ca.pem
```

SASL_SSL + PLAIN (e.g. Confluent Cloud):

```properties
bootstrap.servers=pkc-xxxx.us-east-1.aws.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.username=<api-key>
sasl.password=<api-secret>
ssl.truststore.location=/etc/ssl/cert.pem
```

SASL + SCRAM-SHA-512:

```properties
bootstrap.servers=kafka.example.com:9095
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.username=myuser
sasl.password=mypass
```

## Testing

See [TESTING.md](TESTING.md) — a docker-compose Kafka 4.0.0 harness exercising
all four listeners (PLAINTEXT / SSL / SASL_SSL / SASL_PLAINTEXT) and
`scripts/smoke.sh` end-to-end.

## Internals

- `src/protocol.zig` — varint/compact encoders, request framing, record batch v2 + CRC-32C
- `src/transport.zig` — TCP + TLS (std.crypto.tls) connection with framed send/recv
- `src/client.zig` — bootstrap, ApiVersions negotiation, SASL, metadata, produce+retry
- `src/scram.zig` — RFC 5802 SCRAM-SHA-256/512 client with server-signature verification
- `src/config.zig` — `kannon.properties` loader
