# kite

**kite is an ultra-lightweight Kafka CLI built for agents and sandboxes.**
One binary under 600 KB, no JVM, no runtime, no daemon. stdin in, stdout out,
non-zero exit on failure.

## Quickstart

```sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh
export BOOTSTRAP_SERVERS=localhost:9092
printf 'hello\n' | kite events                 # produce
kite -c -B --idle 3s events                    # consume, stop after 3 s idle
kite -c -B --json events | jq -c .value        # with partition/offset metadata
```

## Why kite

If you are an agent reading this (or a human who also likes Kafka), here's
why I think you will love kite.

- **Small.** The stripped binary is about 585 KB on Linux and 560 KB on macOS
  arm64, gated in CI below 600,000 bytes. It fits in a container layer, a
  sandbox, or a tool call without anyone noticing.
- **Unix philosophy.** kite does one thing per invocation and composes with
  everything else: `tail -f app.log | kite logs`, `kite -c src | jq | kite dst`.
  Records are lines. Diagnostics go to stderr, data goes to stdout.
- **Fast.** Written in Zig: native code, no garbage collector, no JVM
  startup, starts and exits in milliseconds. Produce runs are batched and
  idempotent by default; consume runs are bounded with `-n`/`-t` so a script
  always terminates.
- **Easy install.** `curl | sh`, or `zig build && zig-out/bin/kite -i`. No
  root, no package manager, no `JAVA_HOME`.
- **Predictable for automation.** Every error is a one-line `kite: ...` on
  stderr with exit code 1 and a `Try 'kite --help'` hint. Help pages are
  plain text, ≤ 80 columns, no ANSI unless stderr is a terminal.

kite is not a Kafka admin tool: it never creates topics, manages consumer
groups, or commits offsets. Point it at an existing topic and move data.

## Install

### curl (Linux x86_64/aarch64, macOS x86_64/arm64)

```sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh
```

The script downloads the `v0.1.0` release binary for your platform (override
with `KITE_VERSION=vX.Y.Z`), copies it to `~/.local/bin` (override with
`KITE_INSTALL_DIR`), and offers to add that directory to your `PATH`. If the
release asset is missing the script says so and points at the releases page;
until a release is published, build from source instead. Pass `-s -- --yes`
to skip the prompt in non-interactive shells:

```sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh -s -- --yes
```

Prebuilt binaries and `SHA256SUMS` are on the
[releases page](https://github.com/addisonhuddy/kite/releases).

### From source

Requires Zig 0.16.x.

```sh
zig build
zig-out/bin/kite -i          # copies to ~/.local/bin and offers a PATH line
```

`kite -i` accepts `--dir DIR` and `-y`/`--yes`. To install manually instead:

```sh
install -m755 zig-out/bin/kite ~/.local/bin/kite
```

Cross-compile with, for example, `zig build -Dtarget=aarch64-macos` or
`zig build -Dtarget=x86_64-linux`.

## Command reference

Produce is the default mode. `-c`/`--consume` switches to consume,
`-i`/`--install` to quickly add kite to your PATH. Mode flags may appear
anywhere on the command line; there are no reserved topic names.

```text
kite [OPTIONS] TOPIC          Produce stdin lines to TOPIC (default).
kite -c [OPTIONS] TOPIC       Consume TOPIC to stdout.
kite -i [OPTIONS]             Add kite to PATH.
kite --version                Print the version.
```

| Mode | Option | Meaning |
| --- | --- | --- |
| produce, consume | `-b`, `--bootstrap HOSTS` | Comma-separated `host:port` brokers (overrides env and file). |
| produce, consume | `--config FILE` | Read this properties file instead of searching. |
| produce, consume | `--json` | Produce: read one JSON object per line. Consume: write one per record. |
| produce | `-H 'name: value'` | Add a header to every record (repeatable). |
| produce | `--csv` | Read RFC 4180 CSV; each row becomes a JSON object value. |
| produce | `--key COL` | Use CSV column `COL` as the record key (requires `--csv`). |
| consume | `-B`, `--from-beginning` | Start at the earliest offset. |
| consume | `--offset N` | Start at offset N in every selected partition; errors if N is out of range. |
| consume | `--partition P` | Read one partition only. |
| consume | `-n`, `--max MAX` | Stop after MAX records. |
| consume | `-t`, `--idle DUR` | Stop after DUR without a record (`3s`, `500ms`, `1m`; bare number = ms). |
| consume | `-f`, `--follow` | Never stop on idle, even when stdout is a pipe. |
| install | `--dir DIR` | Install to DIR (default `$KITE_INSTALL_DIR` or `~/.local/bin`). |
| install | `-y`, `--yes` | Add the install directory to `PATH` without prompting. |
| produce, consume | `-v`, `--verbose` | Connection, retry, and fetch diagnostics on stderr. |
| all | `-h`, `--help` | Plain-text help for the selected mode. |

`kite --help`, `kite -c --help`, and `kite -i --help` print the full pages.

## Common recipes

The files in [`examples/data/`](examples/data) are ready-to-use inputs.

```sh
# plain values, one per line
kite events < examples/data/lines.txt
# key<TAB>value
kite events < examples/data/keyed.tsv
# key<TAB>headers<TAB>value
kite events < examples/data/headers.tsv
# attach a header to every record (-H is repeatable)
kite -H 'source: import' events < examples/data/lines.txt
# CSV rows as JSON values, optionally keyed by a column
kite --csv events < examples/data/users.csv
kite --csv --key user_id events < examples/data/events.csv
# stream a log as it grows
tail -f app.log | kite logs
# follow new records on a terminal (Ctrl-C to stop; summary printed)
kite -c events
# follow into a pipe (without -f a piped read stops after 5 s idle)
kite -c -f events | grep ERROR
# read one partition from an offset, at most 10 records
kite -c --partition 0 --offset 42 -n 10 --idle 3s events
# snapshot a topic into a file for an agent to read (stops after 5 s idle)
kite -c -B events > events.txt
# structured records with partition/offset/timestamp/headers
kite -c -B --json events | jq -c 'select(.partition == 0) | .value'
# copy a topic, preserving keys and headers
kite -c -B --json src | kite --json dst
# transform in flight
kite -c -B raw | jq -c '{id, ts}' | kite clean
# one-off broker without a config file
kite -b broker1:9092,broker2:9092 -c -B -n 5 events
```

The consumer output is in the same textual shape accepted by the producer,
subject to the format boundaries described below.

## Consuming

By default, `kite -c` starts at the latest offset and selects all partitions.
`-B`/`--from-beginning` starts at the earliest available offset; `--offset N`
starts at offset N in every selected partition and fails with the valid range
(`valid offsets are 0..334 (next offset 335)`) if N is outside it, rather
than silently replaying from the beginning. `--partition P` selects one
partition.

When the read stops depends on stdout:

- **Terminal:** follow new records until Ctrl-C. Ctrl-C stops cleanly, prints
  the summary, and exits 130.
- **Pipe or file, no bound given:** stop after 5 s without a record, so
  scripts and agents never hang by accident. The reason is stated in the
  summary (`... (idle timeout)`).
- `-n`/`--max MAX` stops after MAX records; `-t`/`--idle DUR` stops after DUR
  without a record (`3s`, `500ms`, `1m`, or a bare number of milliseconds);
  `-f`/`--follow` never stops on idle. `--follow` and `--idle` are mutually
  exclusive.

An idle-bounded read is not a guarantee of a complete topic snapshot. If the
downstream process closes the pipe (`kite -c -f events | head`), kite exits
quietly with status 0 instead of dying from SIGPIPE.

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

### JSON records (`--json`)

`kite -c --json` writes one object per record with full metadata, so bytes
containing TABs or newlines and null-vs-empty distinctions survive:

```json
{"topic":"events","partition":1,"offset":7,"timestamp":1789579403024,"key":null,"headers":[{"key":"h","value":"v"}],"value":"line one"}
```

`kite --json TOPIC` reads the same shape (only `value` is required):

```json
{"key":"user-1","value":{"a":1},"headers":{"source":"import"}}
```

A string `value` is sent as its decoded bytes; any other JSON value (object,
array, number, boolean) is sent verbatim, so JSON payloads can be embedded
without double encoding. `headers` may be an object or an array of
`{"key","value"}`; `-H` headers are added to every record. Malformed lines
fail with `kite: line N: ...`. `--json` and `--csv` are mutually exclusive.

## Configuration

Settings are resolved in this order, highest precedence first:

1. Flags: `-b`/`--bootstrap HOSTS`.
2. Environment variables (below).
3. A properties file: `--config FILE`, else `$KAFKA_PROPERTIES`, else the first of
   `./kite.properties`, `$XDG_CONFIG_HOME/kite/kite.properties`,
   `~/.config/kite/kite.properties`.

A file is optional when `-b` or `BOOTSTRAP_SERVERS` supplies the
brokers. With none of these, kite fails with a message listing all three
ways to configure it. A `--config`/`KAFKA_PROPERTIES` path that does not exist is
an error rather than a silent fallback. `-v` prints which sources were used.

The parser supports a `key=value` subset of Java properties. Blank lines and
`#`/`!` comments are accepted; unknown keys are ignored with a warning.

Environment variable names are the Kafka property names upper-cased with `.`
replaced by `_`, so `bootstrap.servers` becomes `BOOTSTRAP_SERVERS`.

| Key | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `bootstrap.servers` | `BOOTSTRAP_SERVERS` | required | Comma-separated `host:port` brokers. |
| `security.protocol` | `SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_SSL`, or `SASL_PLAINTEXT`. |
| `sasl.mechanism` | `SASL_MECHANISM` | none | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. |
| `sasl.username` | `SASL_USERNAME` | none | Required for SASL. |
| `sasl.password` | `SASL_PASSWORD` | none | Required for SASL. |
| `ssl.truststore.location` | `SSL_TRUSTSTORE_LOCATION` | system trust store | Optional PEM CA bundle for TLS. |
| `batch.size` | — | `1048576` | Per-partition producer buffer cap in bytes. |
| `linger.ms` | — | `50` | Producer flush delay when stdin stalls. |
| `fetch.max.bytes` | — | `8388608` | Maximum bytes requested per fetch. |
| `fetch.max.wait.ms` | — | `500` | Maximum broker wait for a fetch. |
| `enable.idempotence` | — | `true` | Broker deduplicates retried producer batches. |

Templates are in [`examples/config/`](examples/config). Idempotence means
broker deduplication of retried batches, not end-to-end exactly-once
processing.

## Output streams and exit codes

- Producer: stdout is never written; the summary and diagnostics go to
  stderr.
- Consumer: records on stdout; diagnostics and the end-of-run summary on
  stderr.
- Exit 0 on success, 1 on any error (usage, configuration, connection,
  broker), 130 when a consume is stopped with Ctrl-C. Usage errors print
  `kite: MESSAGE` followed by a one-line `Try 'kite --help'` hint.

Every run ends with a summary on stderr. Produce reports the record count,
partitions used, bytes, elapsed time, message and byte rates, the last
acknowledged offset per partition (`p0=333, p1=332`), the number of produce
requests and retried batches, average send-to-ack latency per request
attempt (excludes waiting on stdin and connection setup), and connections
used. Consume reports the record count and why it stopped (`(idle timeout)`,
`(interrupted)`), plus the same throughput line and per-partition offsets.

While stderr is a terminal, a live one-line status on stderr shows the topic,
count, message rate, byte rate, current offset, and elapsed time; a consumer
that has not yet received anything shows `waiting for records ... Ctrl-C to
stop` with a hint such as `new records only; use -B for history`. The live
line is disabled by `-v` / `--verbose`. Terminal colors can be disabled with
a non-empty `NO_COLOR`, forced with `KITE_COLOR=always`, or disabled
explicitly with `KITE_COLOR=never`. Nothing is colored when the stream is a
pipe.

- `-v` / `--verbose` enables connection, retry, and fetch diagnostics.
- `KITE_DEBUG=1` enables frame and TLS diagnostics.
- `KITE_TIME=1` prints producer timing totals and connection count.

The default stripped binary is about 585 KB on Linux (about 560 KB on macOS
arm64) and must remain below 600,000 bytes.
`scripts/pack.sh` can produce an optional UPX/LZMA artifact of about 200 KiB.

## Troubleshooting

- **`no broker configured`:** pass `-b HOST:PORT`, set
  `BOOTSTRAP_SERVERS`, or copy a template to one of the search-path
  locations and edit `bootstrap.servers`. `-v` prints which sources were
  used.
- **Topic does not exist:** create the topic with your Kafka administration
  tooling; kite never creates topics. On some hosted clusters a missing topic
  surfaces as an authorization error instead.
- **`connection refused by HOST:PORT` / `cannot resolve host`:** check the
  address, DNS, and firewall rules; run with `-v` for connection diagnostics.
- **Authentication or TLS failure:** check the SASL settings and CA bundle;
  use `KITE_DEBUG=1` for handshake details.
- **`not authorized to write/read`:** the principal lacks ACLs for that
  operation on the topic.
- **`batch of N exceeds the broker's max.message.bytes`:** split the input or
  raise the topic/broker limit.
- **`offset N is out of range`:** the message shows the valid range and the
  next offset; use `-B` for the earliest or omit `--offset` for the latest.
- **Consumer shows `waiting for records`:** the default starts at latest. Use
  `-B`, `--offset`, or a bounded `--idle` as appropriate.
- **`kite consume` produced to a topic named `consume`:** modes are flags, not
  subcommands. Use `kite -c TOPIC`.

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
Releases are cut by pushing a `v*` tag; the release workflow cross-compiles
Linux and macOS binaries and attaches them with `SHA256SUMS`.

Internals:

- `src/protocol.zig` — Kafka encoders, framing, and record batches
- `src/decompress.zig` — gzip, Snappy, and LZ4 decoders (no zstd, to keep the
  binary small)
- `src/transport.zig` — TCP and TLS transport
- `src/client.zig` — bootstrap, metadata, SASL, produce, fetch, and retry
- `src/consumer.zig` — ListOffsets/Fetch loop and output formatting
- `src/scram.zig` — SCRAM-SHA-256/512 client
- `src/config.zig` — `kite.properties` loader
- `src/csv.zig` — CSV reader and JSON conversion
- `src/cli.zig` — argument parsing, mode selection, help text

## License

Kite is licensed under the [Apache License 2.0](LICENSE).
