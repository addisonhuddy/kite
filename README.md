# kite

**The Kafka CLI for agents and shell pipelines.** One ~560 KB binary,
no JVM, no runtime, no daemon. stdin in, stdout out, non-zero exit on failure.

```sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh
printf 'hello\n' | kite events                 # produce
kite -c --from-beginning -t 3000 events        # consume, stop after 3 s idle
```

## Contents

- [Why kite](#why-kite)
- [Install](#install)
- [Quickstart](#quickstart)
- [Command reference](#command-reference)
- [Common recipes](#common-recipes)
- [Consuming](#consuming)
- [Input/output format and CSV](#inputoutput-format-and-csv)
- [Configuration](#configuration)
- [Output streams and exit codes](#output-streams-and-exit-codes)
- [Troubleshooting](#troubleshooting)
- [Development and testing](#development-and-testing)
- [License](#license)

## Why kite

If you are an agent (or a human writing scripts for one) and need to read or
write Kafka, kite is the shortest path:

| | kite | `kafka-console-*.sh` | kcat | Python/Node client |
| --- | --- | --- | --- | --- |
| Install | one `curl \| sh`, ~560 KB binary | JDK + 100 MB distribution | package manager + librdkafka | interpreter + package + native lib |
| Startup | milliseconds, no VM warm-up | seconds (JVM) | fast | interpreter start |
| Interface | stdin/stdout lines, flags, exit codes | verbose Java flags, log noise on stdout | flags | write code first |
| Dependencies | none (Zig, no shared libraries) | Java | librdkafka, OpenSSL | many |
| Surface area | produce, consume, install. That's it. | dozens of tools | large flag set | full API |

- **Small.** The stripped binary is about 560 KB on Linux and 535 KB on macOS
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
- **Speaks modern Kafka.** Kafka 4.0+ KRaft brokers, PLAINTEXT / SSL /
  SASL_SSL / SASL_PLAINTEXT, PLAIN and SCRAM-SHA-256/512, gzip/Snappy/LZ4
  consumer decompression.

kite is not a Kafka admin tool: it never creates topics, manages consumer
groups, or commits offsets. Point it at an existing topic and move data.

## Install

### curl (Linux x86_64/aarch64, macOS x86_64/arm64)

```sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh
```

The script downloads the `v0.1.0` release binary for your platform, copies it
to `~/.local/bin` (override with `KITE_INSTALL_DIR`), and offers to add that
directory to your `PATH`. Pass `-s -- --yes` to skip the prompt in
non-interactive shells:

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

## Quickstart

Prerequisites: a reachable Kafka 4.0+ KRaft broker, an existing topic, and
permission to read and write it.

Copy a configuration template and edit the broker address (and credentials if
needed):

```sh
cp examples/config/plaintext.properties kite.properties
```

Produce the checked-in sample data:

```sh
kite events < examples/data/lines.txt
```

```text
5 record(s) produced to 'events'
```

Read from the beginning and stop after 3 seconds without a record:

```sh
kite -c --from-beginning -t 3000 events
```

```text
line one
line two
...
```

## Command reference

Produce is the default mode. `-c`/`--consume` switches to consume,
`-i`/`--install` to install. Mode flags may appear anywhere on the command
line; there are no reserved topic names.

```text
kite [OPTIONS] TOPIC          Produce stdin lines to TOPIC (default).
kite -c [OPTIONS] TOPIC       Consume TOPIC to stdout.
kite -i [OPTIONS]             Install this executable.
kite --version                Print the version.
```

| Mode | Option | Meaning |
| --- | --- | --- |
| produce | `-H 'name: value'` | Add a header to every record (repeatable). |
| produce | `--csv` | Read RFC 4180 CSV; each row becomes a JSON object value. |
| produce | `--key COL` | Use CSV column `COL` as the record key (requires `--csv`). |
| consume | `--from-beginning` | Start at the earliest offset. |
| consume | `--offset N` | Start at offset N in every selected partition. |
| consume | `--partition P` | Read one partition only. |
| consume | `-n MAX` | Stop after MAX records. |
| consume | `-t IDLE_MS` | Stop after IDLE_MS without a record. |
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
# follow new records (Ctrl-C to stop)
kite -c events
# read one partition from an offset, at most 10 records
kite -c --partition 0 --offset 42 -n 10 -t 3000 events
# snapshot a topic into a file for an agent to read
kite -c --from-beginning -t 5000 events > events.txt
# copy a topic
kite -c --from-beginning -t 5000 src | kite dst
# transform in flight
kite -c --from-beginning -t 5000 raw | jq -c '{id, ts}' | kite clean
```

The consumer output is in the same textual shape accepted by the producer,
subject to the format boundaries described below.

## Consuming

By default, `kite -c` starts at the latest offset, selects all partitions, and
follows new records indefinitely. `--from-beginning` starts at the earliest
available offset; `--offset N` starts at offset N in every selected partition.
`--partition P` selects one partition.

`-n MAX` stops after MAX records. Without `-t`, a count-limited read may wait
indefinitely for enough records. `-t IDLE_MS` stops after that many
milliseconds without a record. An idle-bounded read is not a guarantee of a
complete topic snapshot. For scripted use, always pass `-t` (and usually
`-n`) so the process terminates.

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

## Output streams and exit codes

- Producer: success summary on stdout, diagnostics on stderr.
- Consumer: records on stdout; diagnostics, including a bounded-run summary,
  on stderr.
- Exit 0 on success, 1 on any error (usage, configuration, connection,
  broker). Usage errors print `kite: MESSAGE` followed by a one-line
  `Try 'kite --help'` hint.

While stderr is a terminal, producer and bounded consumer runs also show a
live one-line rate and byte statistic on stderr. The live line is disabled by
`-v` / `--verbose`; final summaries include a second detail line with elapsed
time, message rate, byte rate, and the last offset when available. Terminal
colors can be disabled with a non-empty `NO_COLOR`, forced with
`KITE_COLOR=always`, or disabled explicitly with `KITE_COLOR=never`. Nothing
is colored when the stream is a pipe.

- `-v` / `--verbose` enables connection, retry, and fetch diagnostics.
- `KITE_DEBUG=1` enables frame and TLS diagnostics.
- `KITE_TIME=1` prints producer timing totals and connection count.

The default stripped binary is about 560 KB on Linux (about 535 KB on macOS
arm64) and must remain below 600,000 bytes.
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
