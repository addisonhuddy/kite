# kite

Kite is a small Kafka command-line producer and consumer written in Zig.
`kite TOPIC` reads stdin, one record per line, while `kite consume TOPIC`
writes records to stdout.

```sh
printf 'hello\n' | zig-out/bin/kite events
zig-out/bin/kite consume --from-beginning -t 3000 events
```

## Contents

- [Prerequisites](#prerequisites)
- [Install](#install)
- [Quickstart](#quickstart)
- [Common recipes](#common-recipes)
- [Consuming](#consuming)
- [Input/output format and CSV](#inputoutput-format-and-csv)
- [Configuration](#configuration)
- [Output streams](#output-streams)
- [Troubleshooting](#troubleshooting)
- [Development and testing](#development-and-testing)
- [License](#license)

## Prerequisites

- Zig 0.16.x.
- A reachable Kafka 4.0+ KRaft broker.
- Permission to read and write the topic.
- The topic must already exist; kite never creates topics.

## Install

Build from source:

```sh
zig build
```

The executable is `zig-out/bin/kite`. Optionally install it for your user:

```sh
install -m755 zig-out/bin/kite ~/.local/bin/kite
```

Put `~/.local/bin` on `PATH` if it is not there already. Cross-compile with,
for example:

```sh
zig build -Dtarget=aarch64-macos
zig build -Dtarget=x86_64-linux
```

The source build is the supported installation method.

## Quickstart

Choose an existing topic, copy a configuration template, edit the broker
address (and credentials if needed), then build:

```sh
cp examples/config/plaintext.properties kite.properties
zig build
```

Produce the checked-in sample data:

```sh
zig-out/bin/kite events < examples/data/lines.txt
```

```text
5 record(s) produced to 'events'
```

Read from the beginning and stop after 3 seconds without a record:

```sh
zig-out/bin/kite consume --from-beginning -t 3000 events
```

```text
line one
line two
...
```

After installing `kite` as described above, the same commands can use
`kite` instead of `zig-out/bin/kite`.

## Common recipes

The files in [`examples/data/`](examples/data) are ready-to-use inputs.

```sh
# plain values, one per line
zig-out/bin/kite events < examples/data/lines.txt
# key<TAB>value
zig-out/bin/kite events < examples/data/keyed.tsv
# key<TAB>headers<TAB>value
zig-out/bin/kite events < examples/data/headers.tsv
# attach a header to every record (-H is repeatable)
zig-out/bin/kite -H 'source: import' events < examples/data/lines.txt
# CSV rows as JSON values, optionally keyed by a column
zig-out/bin/kite --csv events < examples/data/users.csv
zig-out/bin/kite --csv --key user_id events < examples/data/events.csv
# stream a log as it grows
tail -f app.log | zig-out/bin/kite logs
# follow new records (Ctrl-C to stop)
zig-out/bin/kite consume events
# read one partition from an offset, at most 10 records
zig-out/bin/kite consume --partition 0 --offset 42 -n 10 -t 3000 events
# copy a topic
zig-out/bin/kite consume --from-beginning -t 5000 src | zig-out/bin/kite dst
```

The consumer output is in the same textual shape accepted by the producer,
subject to the format boundaries described below.

## Consuming

By default, `kite consume` starts at the latest offset, selects all partitions,
and follows new records indefinitely. `--from-beginning` starts at the earliest
available offset; `--offset N` starts at offset N in every selected partition.
`--partition P` selects one partition.

`-n MAX` stops after MAX records. Without `-t`, a count-limited read may wait
indefinitely for enough records. `-t IDLE_MS` stops after that many
milliseconds without a record. An idle-bounded read is not a guarantee of a
complete topic snapshot.

## Input/output format and CSV

Plain input is one value per line. TAB-separated input has these shapes:

```text
value
key<TAB>value
key<TAB>name: value<TAB>...<TAB>value
```

The TAB and newline delimiters are not escaped. Binary data or records
containing delimiters are therefore not generally safe to roundtrip. Null and
empty keys, values, and header values may not remain distinct: a null key is
printed as an empty field, and a null header value as `name: `. Compatible
textual keys and headers can roundtrip. A consume-to-produce pipe does not
preserve ordering across partitions, offsets, or timestamps.

`--csv` reads RFC 4180 CSV. The first row supplies column names and each later
row becomes a JSON object value. `--key COL` uses a CSV column as the Kafka
record key while retaining it in the JSON value. Quoted commas, escaped
quotes, embedded newlines, CRLF endings, and a UTF-8 BOM are supported; all
JSON fields are strings.

## Configuration

Kite reads the first `kite.properties` found in this order:

1. `./kite.properties`
2. `$XDG_CONFIG_HOME/kite/kite.properties`
3. `~/.config/kite/kite.properties`

The parser supports a `key=value` subset of Java properties. Blank lines and
`#`/`!` comments are accepted; unknown keys are ignored with a warning.

| Key | Default | Meaning |
| --- | --- | --- |
| `bootstrap.servers` | required | Comma-separated `host:port` brokers. |
| `security.protocol` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_SSL`, or `SASL_PLAINTEXT`. |
| `sasl.mechanism` | none | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. |
| `sasl.username` | none | Required for SASL. |
| `sasl.password` | none | Required for SASL. |
| `ssl.truststore.location` | system trust store | Optional PEM CA bundle for TLS. |
| `batch.size` | `1048576` | Per-partition producer buffer cap in bytes. |
| `linger.ms` | `50` | Producer flush delay when stdin stalls. |
| `fetch.max.bytes` | `8388608` | Maximum bytes requested per fetch. |
| `fetch.max.wait.ms` | `500` | Maximum broker wait for a fetch. |
| `enable.idempotence` | `true` | Broker deduplicates retried producer batches. |

Templates are in [`examples/config/`](examples/config). Idempotence means
broker deduplication of retried batches, not end-to-end exactly-once
processing.

## Output streams

The producer writes its success summary to stdout and diagnostics to stderr.
The consumer writes records to stdout and diagnostics, including a bounded-run
summary, to stderr.

- `-v` / `--verbose` enables connection, retry, and fetch diagnostics.
- `KITE_DEBUG=1` enables frame and TLS diagnostics.
- `KITE_TIME=1` prints producer timing totals and connection count.

The default stripped binary is about 565 KiB and must remain below 1 MiB.
`scripts/pack.sh` can produce an optional UPX/LZMA artifact of about 200 KiB.

## Troubleshooting

- **No configuration:** copy a template to one of the search-path locations,
  edit `bootstrap.servers`, and retry with `-v`.
- **Topic does not exist:** create the topic with your Kafka administration
  tooling; kite never creates topics.
- **Bootstrap unreachable:** check DNS, firewall rules, host/port, and run
  with `-v` for connection diagnostics.
- **Authentication or TLS failure:** check the SASL settings and CA bundle;
  use `KITE_DEBUG=1` for handshake details.
- **Consumer is waiting:** the default starts at latest. Use
  `--from-beginning`, `--offset`, or a bounded `-t` as appropriate.

## Development and testing

```sh
zig build
zig build test
scripts/cli-check.sh
scripts/check-size.sh
scripts/pack.sh
scripts/smoke.sh EXISTING_TOPIC
```

See [TESTING.md](TESTING.md) for broker-agnostic and broker-backed checks.

Internals:

- `src/protocol.zig` — Kafka encoders, framing, and record batches
- `src/decompress.zig` — gzip, zstd, Snappy, and LZ4 decoders
- `src/transport.zig` — TCP and TLS transport
- `src/client.zig` — bootstrap, metadata, SASL, produce, fetch, and retry
- `src/consumer.zig` — ListOffsets/Fetch loop and output formatting
- `src/scram.zig` — SCRAM-SHA-256/512 client
- `src/config.zig` — `kite.properties` loader
- `src/csv.zig` — CSV reader and JSON conversion

## License

Kite is licensed under the [Apache License 2.0](LICENSE).
