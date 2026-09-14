# kite

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)

An ultra-lightweight Kafka producer/consumer CLI written in Zig. `kite`
reads lines from stdin and produces each line as a record to a topic:

```console
$ kite my-topic < file.txt
3 record(s) produced to 'my-topic'
```

`kite consume` fetches records back to stdout in the same format, so records
round-trip losslessly (keys and headers included):

```console
$ kite consume --from-beginning -t 3000 my-topic
```

No JVM, no librdkafka, no dependencies — a single static binary under 1 MiB.
Speaks the Kafka wire protocol directly: flexible versions only, record batch
v2, acks=all. **Requires Kafka 4.0+** (KRaft) on the broker side.

## Quickstart

Point kite at any Kafka 4.0+ broker (see
[Configuration](#configuration)):

```console
$ cp examples/config/plaintext.properties kite.properties   # set bootstrap.servers
$ zig build
$ zig-out/bin/kite my-topic < examples/data/lines.txt
5 record(s) produced to 'my-topic'
$ zig-out/bin/kite consume --from-beginning -t 3000 my-topic
line one
line two
...
```

## Build

Requires Zig 0.16.x:

```console
$ zig build            # produces zig-out/bin/kite (ReleaseSmall, stripped)
$ scripts/check-size.sh   # hard gate: fails if the binary is >= 1 MiB
```

The release artifact is only `zig-out/bin/kite` — `examples/` is
documentation/sample data and is not referenced by the build.

Cross-compile for other targets, e.g.:

```console
$ zig build -Dtarget=aarch64-macos     # Apple Silicon
$ zig build -Dtarget=x86_64-macos      # Intel Mac
```

## Usage

```console
$ kite [-v] [-H 'name: value']... [--csv [--key col]] <topic>
$ kite consume [options] <topic>       # fetch records to stdout
$ echo hello | kite my-topic
```

Each stdin line is one record. A plain line is value-only; a TAB separates
the line into fields: first field = record key, last = value, any middle
fields are per-record `name: value` headers.

Cookbook, using the files in [`examples/data/`](examples/data):

```console
$ kite t1 < examples/data/lines.txt              # 5 value-only records
$ printf 'value only\n' | kite t1
$ kite t1 < examples/data/keyed.tsv              # key<TAB>value
$ printf 'key\tvalue\n' | kite t1
$ kite t1 < examples/data/headers.tsv            # key, headers, value
$ printf 'key\ttrace-id: 42\tsrc: cli\tvalue\n' | kite t1
```

`-H 'name: value'` (repeatable, curl-style) attaches a header to every
record:

```console
$ kite -H 'source: import-job' -H 'env: prod' t1 < examples/data/lines.txt
```

Bulk load and stream:

```console
$ seq 1 100000 | kite t1
$ tail -f app.log | kite logs                    # produces as lines arrive
```

CSV input (see "CSV input" below):

```console
$ kite --csv t1 < examples/data/users.csv
$ kite --csv --key user_id t1 < examples/data/events.csv
```

Watch keys and headers land on the broker — `kite consume` prints them in
the same `key<TAB>headers<TAB>value` layout the producer accepts:

```console
$ kite consume --from-beginning -t 3000 t1
key	trace-id: 42	src: cli	value
```

Diagnostics on stderr (see "Diagnostics"):

```console
$ kite -v t1 < examples/data/lines.txt           # connection/retry info
$ KITE_DEBUG=1 kite t1 < examples/data/lines.txt  # hex-dump frames
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

## Consuming

`kite consume` writes one record per line in the same format accepted by the
producer, making consume-to-produce pipelines lossless for keys and headers:

```console
$ kite consume --from-beginning -t 3000 my-topic     # dump a whole topic
$ kite consume --offset 42 --partition 1 -n 10 my-topic
$ kite consume my-topic                            # follow new records (Ctrl-C to stop)
```

Copy a topic by piping consume straight back into produce — keys and headers
survive the hop:

```console
$ kite consume --from-beginning -t 5000 src-topic | kite dst-topic
```

The default starting position is the latest offset. Use `--from-beginning` for
the earliest offset or `--offset N` for an absolute offset on every selected
partition. `--partition P` selects one partition, `-n MAX` stops after a record
count, and `-t IDLE_MS` stops after an idle interval. `-v` logs fetch ranges
and high watermarks to stderr.

## CSV input

`--csv` parses stdin as RFC 4180 CSV: the first row supplies column names
and every following row becomes one record whose value is a JSON object:

```console
$ kite --csv my-topic < data.csv
```

```text
id,name,note              →  {"id":"1","name":"alice","note":"hi"}
1,alice,hi
```

- Quoted fields may contain commas, `""` escapes, and embedded newlines —
  a quoted newline does not split the record.
- `--key <col>` uses a column as the record key (it stays in the JSON
  value), so keyed rows get murmur2 partitioning:
  `kite --csv --key id my-topic < data.csv`
- All fields are emitted as JSON strings — no type guessing, so IDs like
  `007` survive intact.
- CRLF endings and a UTF-8 BOM are handled; a row with the wrong number
  of fields aborts with `csv row N: expected M field(s), got K`.

## Configuration

kite reads `kite.properties` (Java properties format, `key=value` lines,
`#`/`!` comments). Search order — first match wins:

1. `./kite.properties` (current directory)
2. `$XDG_CONFIG_HOME/kite/kite.properties`
3. `~/.config/kite/kite.properties`

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
| `fetch.max.bytes` | no (default `8388608`) | Maximum bytes requested per fetch; values must be below the 16 MiB transport limit. |
| `fetch.max.wait.ms` | no (default `500`) | Maximum broker wait for a fetch response; kept below the socket timeout. |
| `enable.idempotence` | no (default `true`) | Idempotent producer: producer id + per-partition sequences, exactly-once on retry. |

Unknown keys are ignored with a warning, so a shared `server.properties`-style
file works.

## Diagnostics

All diagnostics go to stderr; stdout carries only the final
`N record(s) produced to '<topic>'` line.

- `-v` / `--verbose` — connection lifecycle (bootstrap, per-broker connects,
  drops), partition counts, the assigned producer id, and every retry attempt
  with its backoff.
- `KITE_DEBUG=1` — hex-dumps outbound request frames and logs TLS
  handshake errors.
- `KITE_TIME=1` — prints `read/send/drain` millisecond totals and the
  connection count on exit (perf tuning).

## Examples

`kite.properties` templates live in [`examples/config/`](examples/config) —
copy one into place and edit `bootstrap.servers` / credentials:

```console
$ cp examples/config/plaintext.properties kite.properties
```

| File | For |
| --- | --- |
| [`plaintext.properties`](examples/config/plaintext.properties) | PLAINTEXT, no auth (local broker) |
| [`ssl.properties`](examples/config/ssl.properties) | TLS with a custom CA |
| [`sasl-ssl-plain.properties`](examples/config/sasl-ssl-plain.properties) | SASL_SSL + PLAIN, Confluent Cloud style (`<api-key>`/`<api-secret>`) |
| [`sasl-scram.properties`](examples/config/sasl-scram.properties) | SASL_PLAINTEXT + SCRAM-SHA-512 |

Sample stdin inputs are in [`examples/data/`](examples/data): `lines.txt`
(value-only), `keyed.tsv` (key + value), `headers.tsv` (key + headers +
value), `users.csv` and `events.csv` (for `--csv` / `--csv --key`).
The name `kite.properties` is gitignored on purpose — the templates use
different names so they stay tracked.

## Testing

See [TESTING.md](TESTING.md) — `zig build test` unit tests plus a
broker-agnostic `scripts/smoke.sh` produce/consume roundtrip for any
Kafka 4.0+ cluster.

## Internals

- `src/protocol.zig` — varint/compact encoders, request framing, record batch v2 decode/encode + CRC-32C
- `src/decompress.zig` — gzip, zstd, Snappy, and LZ4 record-batch decoders
- `src/transport.zig` — TCP + TLS (std.crypto.tls) connection with framed send/recv
- `src/client.zig` — bootstrap, ApiVersions negotiation, SASL, metadata, produce/fetch + retry
- `src/consumer.zig` — ListOffsets/Fetch consumer loop and output formatting
- `src/scram.zig` — RFC 5802 SCRAM-SHA-256/512 client with server-signature verification
- `src/config.zig` — `kite.properties` loader
- `src/csv.zig` — quote-aware row reader, field unescaping, row→JSON

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
