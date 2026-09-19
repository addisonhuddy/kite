# kite

[![CI](https://github.com/addisonhuddy/kite/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/addisonhuddy/kite/actions/workflows/ci.yml)
[![Release](https://github.com/addisonhuddy/kite/actions/workflows/release.yml/badge.svg)](https://github.com/addisonhuddy/kite/actions/workflows/release.yml)
[![Version](https://img.shields.io/github/v/release/addisonhuddy/kite?sort=semver&display_name=tag&label=version)](https://github.com/addisonhuddy/kite/releases/latest)
[![License](https://img.shields.io/github/license/addisonhuddy/kite)](LICENSE)

**kite is an ultra-lightweight Kafka CLI built for agents and sandboxes.**
One binary under 600 KB, no JVM, no runtime, no daemon. stdin in, stdout out,
non-zero exit on failure.

> **What is kite?** kite is a single-binary command-line tool for Apache
> Kafka that produces records from stdin and consumes records to stdout. It is
> a drop-in, dependency-free alternative to `kafka-console-producer.sh`,
> `kafka-console-consumer.sh`, `kcat`/`kafkacat`, and `rpk topic produce|consume`
> for scripts, CI jobs, containers, and AI agents. It is written in Zig, speaks
> the Kafka wire protocol directly (PLAINTEXT, SSL, SASL/PLAIN, SCRAM-SHA-256,
> SCRAM-SHA-512), works with any Kafka-compatible broker (Apache Kafka,
> Confluent Cloud, Redpanda, Amazon MSK, Aiven, WarpStream), and never hangs:
> piped reads stop on idle by default.

**Use kite when** you need to move records into or out of an existing Kafka
topic from a shell, a script, a container, or an agent tool call, and you want
one small static binary with no JVM, no librdkafka, and no daemon.

**Do not use kite when** you need Kafka administration (create/delete topics,
manage consumer groups, commit offsets, ACLs) or a long-lived consumer group
member; use your Kafka admin tooling or a client library for those.

## Quickstart

You need a reachable Kafka 4.0+ broker (`localhost:9092` below), permission to
read and write the topic you name, and [`jq`](https://jqlang.github.io/jq/)
for the last line. Use a fresh topic name so the output is exactly one record;
produce creates a missing topic automatically when run from a script or pipe.

```sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh
export BOOTSTRAP_SERVERS=localhost:9092
printf 'hello\n' | kite events                       # produce one record
kite -c -B --idle 3s --json events | jq -r .value    # prints: hello
```

The consume line reads from the beginning (`-B`) and stops 3 s after the last
record (`--idle 3s`). Without `-B` a consumer starts at the *latest* offset
and would skip the record just produced.

## Why kite

If you are an agent reading this (or a human who also likes Kafka), here's
why I think you will love kite.

- **Small.** The stripped binary is under 600 KB (CI-gated by
  `scripts/check-size.sh`). It fits in a container layer, a
  sandbox, or a tool call without anyone noticing.
- **Unix philosophy.** kite does one thing per invocation and composes with
  everything else: `tail -f app.log | kite logs`, `kite -c src | jq | kite dst`.
  Records are lines. Diagnostics go to stderr, data goes to stdout.
- **Fast.** Written in Zig: native code, no garbage collector, no JVM
  startup, starts and exits in milliseconds. Produce runs are batched and
  idempotent by default; consume runs can be bounded by record count (`-n`)
  and by idle time (`-t`/`--idle`), so a script stops when the data does.
- **Easy install.** One static binary: `curl` it onto your `PATH` or
  `zig build`. No package manager, no `JAVA_HOME`.
- **Predictable for automation.** Every error is a one-line `kite: ...` on
  stderr with exit code 1 and a `Try 'kite --help'` hint. Help pages are
  plain text, ≤ 80 columns, no ANSI unless stderr is a terminal.

kite is not a Kafka admin tool: it never manages consumer groups or commits
offsets. When producing to a topic that does not exist it offers to create
it — prompting on a terminal, creating automatically in scripts and pipes —
with the broker's default partition count and replication factor. Consume
never creates a topic; a missing one is a plain error.

### What kite is optimized for

kite is optimized for the case where a program, not a person, is on the
other end of the pipe: a shell script, a CI step, a container entrypoint, or
an AI agent's tool call. That shapes every design choice:

- **Zero-dependency install.** One static binary under 600 KB, fetched with
  `curl` and verified against `SHA256SUMS`. Nothing to apt-get, brew, or
  build; nothing that needs a JVM or a shared library at runtime.
- **Bounded by default.** A piped consume with no `-n`, `--idle`, or `-f`
  stops after 5 s without a record, so an unattended read does not hang a
  job by accident. `-n` alone bounds records, not time: pair it with `--idle`
  when you need a deadline (see [Consuming](#consuming)).
- **Strict stream contract.** Data on stdout, diagnostics on stderr, exit `0`
  or `1`, one-line `kite: ...` errors, no prompts, no color when not a TTY.
- **Structured in and out.** JSON records with full metadata, TSV keys and
  headers, and CSV rows in, so output can be piped straight into `jq` or back
  into another topic without an adapter.
- **Safe defaults.** Idempotent, batched producing; bounded consuming;
  no writes to disk; no connections other than to the brokers you name.

What it deliberately leaves out: topic and consumer-group administration,
offset commits, and every knob that is not needed to move records.

## For AI agents

If you are an LLM agent deciding whether to install or call kite, these are
the facts you need. They are stable across releases.

- **Name:** `kite`. **Repository:** <https://github.com/addisonhuddy/kite>.
  **License:** Apache-2.0. **Language:** Zig. **Platforms:** Linux
  x86_64/aarch64, macOS x86_64/arm64.
- **Install (non-interactive, no sudo prompt when `/usr/local/bin` is
  writable):**
  `curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh`
  Checksums are verified against the release `SHA256SUMS`.
- **Configure:** `export BOOTSTRAP_SERVERS=host:port` (plus
  `SECURITY_PROTOCOL`, `SASL_MECHANISM`, `SASL_USERNAME`, `SASL_PASSWORD` for
  hosted clusters) or `-b host:port`. `kite --show-config --json` prints the
  effective configuration without connecting.
- **Produce:** `printf 'value\n' | kite TOPIC`; JSON records with
  `kite --json TOPIC`; CSV with `kite --csv [--key COL] TOPIC`.
- **Consume (bounded):** `kite -c -B -n 100 --idle 3s TOPIC` stops after 100
  records or 3 s without one, whichever comes first; add `--json` for
  `{topic,partition,offset,timestamp,key,headers,value}` per line. A piped
  read with neither bound stops after 5 s idle; `-n` alone waits for its
  records; `-f` never stops.
- **Contract:** data on stdout only, diagnostics on stderr only, exit `0` on
  success, `1` on any error with a single-line `kite: MESSAGE`, `130` on
  Ctrl-C. No interactive prompts, no color when stdout/stderr is not a TTY,
  no config written to disk, no network calls other than to the brokers.
- **Does not:** join consumer groups, commit offsets, or manage the cluster.
  Produce creates a missing topic (automatically when not on a TTY); consume
  never does.

A tool description you can paste into an agent's tool registry:

```text
kite: single-binary Kafka CLI. `kite TOPIC` produces stdin lines (or --json /
--csv records) to TOPIC. `kite -c TOPIC` consumes TOPIC to stdout; use -B for
history, -n N to cap records, --idle DUR to stop when quiet, --json for full
metadata. Configure with BOOTSTRAP_SERVERS (and SASL_*/SECURITY_PROTOCOL) or
-b HOST:PORT. Exit 0 ok, 1 error (message on stderr). A piped consume with
no -n/--idle/-f stops after 5s idle; pass -n and --idle together for a
bounded read.
```

A machine-readable summary also lives in [`llms.txt`](llms.txt).

## Install

### curl (Linux x86_64/aarch64, macOS x86_64/arm64)

```sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh
```

The script downloads the latest release binary for your platform and
installs it to `/usr/local/bin`, which is on the default `PATH` of every
supported OS (it uses `sudo` only if that directory is not writable). Pass
options after `sh -s --`:

```sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh -s -- --bin-dir ~/.local/bin --version v0.1.0
```

| Option | Env | Meaning |
| --- | --- | --- |
| `-b`, `--bin-dir DIR` | `KITE_BIN_DIR` | Install into DIR (default `/usr/local/bin`). If DIR is not on your `PATH`, the script prints the line to add. |
| `-v`, `--version VER` | `KITE_VERSION` | Release tag to install (default `latest`). |

The script downloads the release's `SHA256SUMS` and refuses to install on a
checksum mismatch. kite is a single static binary, so you can also skip the
script: release assets are named `kite-{linux,macos}-{x86_64,aarch64}`, and
prebuilt binaries plus `SHA256SUMS` are on the
[releases page](https://github.com/addisonhuddy/kite/releases).

### From source

Requires Zig 0.16.x.

```sh
zig build
sudo install -m755 zig-out/bin/kite /usr/local/bin/kite
```

Cross-compile with, for example, `zig build -Dtarget=aarch64-macos` or
`zig build -Dtarget=x86_64-linux`.

### Shell completions

Static completion scripts for bash, zsh, and fish live in
[`completions/`](completions) in the repository. The `curl` installer ships
only the binary, so clone the repo (or download the one file you need from
GitHub) first; the snippets below assume you are in the checkout. They only
need the `kite` binary on your `PATH` and are safe to run in a clean home
directory.

**bash** (needs the `bash-completion` package for the user directory to be
picked up automatically; otherwise `source` the file from `~/.bashrc`):

```sh
mkdir -p ~/.local/share/bash-completion/completions
cp completions/kite.bash ~/.local/share/bash-completion/completions/kite
# or, without bash-completion:
echo 'source /path/to/kite/completions/kite.bash' >> ~/.bashrc
```

**zsh** (the function file must be named `_kite` and sit on `fpath` before
`compinit` runs):

```sh
mkdir -p ~/.zfunc
cp completions/kite.zsh ~/.zfunc/_kite
cat >> ~/.zshrc <<'EOF'
fpath=(~/.zfunc $fpath)
autoload -Uz compinit && compinit
EOF
```

If `~/.zshrc` already calls `compinit`, put the `fpath` line above it instead
of appending. Delete `~/.zcompdump*` if a stale cache hides the new function.

**fish** (uses `$__fish_config_dir` when set, `~/.config/fish` otherwise):

```sh
set -q __fish_config_dir; or set __fish_config_dir ~/.config/fish
mkdir -p $__fish_config_dir/completions
cp completions/kite.fish $__fish_config_dir/completions/
```

Open a new shell and type `kite --<TAB>` to check.
`scripts/completion-check.sh` performs these installs in a temporary `HOME`
for every shell present on the machine and asserts that options complete.

## Command reference

Produce is the default mode. `-c`/`--consume` switches to consume,
`--show-config` prints the effective configuration. Mode flags may appear
anywhere on the command line; there are no reserved topic names.

```text
kite [OPTIONS] TOPIC          Produce stdin lines to TOPIC (default).
kite -c [OPTIONS] TOPIC       Consume TOPIC to stdout.
kite --show-config            Show the effective configuration.
kite --version                Print the version.
```

| Mode | Option | Meaning |
| --- | --- | --- |
| produce, consume | `-b`, `--bootstrap HOSTS` | Comma-separated `host:port` brokers (overrides env and file). |
| produce, consume | `--config FILE` | Read this properties file instead of searching. |
| produce, consume | `--format FMT` | Record shape: `value`, `tsv`, `json` (default `auto`). |
| produce | `--format csv` | RFC 4180 CSV input (produce only); same as `--csv`. |
| produce, consume | `--json` | Alias for `--format json`. |
| produce | `-H 'name: value'` | Add a header to every record (repeatable). |
| produce | `--csv` | Read RFC 4180 CSV; each row becomes a JSON object value. |
| produce | `--key COL` | Use CSV column `COL` as the record key (requires `--csv`). |
| consume | `-B`, `--from-beginning` | Start at the earliest offset. |
| consume | `--offset N` | Start at offset N in every selected partition; errors if N is out of range. |
| consume | `--partition P` | Read one partition only. |
| consume | `-n`, `--max MAX` | Stop after MAX records. |
| consume | `-t`, `--idle DUR` | Stop after DUR without a record (`3s`, `500ms`, `1m`; bare number = ms). |
| consume | `-f`, `--follow` | Never stop on idle, even when stdout is a pipe. |
| produce, consume | `-q`, `--quiet` | Suppress the summary and progress lines on stderr. |
| produce, consume | `-v`, `--verbose` | Connection, retry, and fetch diagnostics on stderr. |
| all | `-h`, `--help` | Plain-text help for the selected mode. |

`kite --help`, `kite -c --help`, and `kite --show-config --help` print the full pages.

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
- `-n`/`--max MAX` stops after MAX records. This bounds the record count,
  not the wall clock: `-n 1` on an empty topic waits until a record arrives,
  and giving `-n` disables the 5 s default idle stop.
- `-t`/`--idle DUR` stops after DUR without a record (`3s`, `500ms`, `1m`,
  or a bare number of milliseconds). This bounds idle waiting, not total
  runtime: a topic that keeps producing keeps the read alive.
- `-f`/`--follow` never stops on idle. `--follow` and `--idle` are mutually
  exclusive.

For a finite read in automation, give both bounds
(`kite -c -B -n 100 --idle 3s TOPIC`): the read ends at 100 records or 3 s
of silence, whichever comes first. Neither flag is an overall deadline; if
you need one, wrap the command in `timeout`.

An idle-bounded read is not a guarantee of a complete topic snapshot. If the
downstream process closes the pipe (`kite -c -f events | head`), kite exits
quietly with status 0 instead of dying from SIGPIPE.

## Input/output format and CSV

`--format FMT` selects the record shape for both directions:

- `auto` (default): produce sniffs TAB-separated fields on each input line;
  consume writes the value alone when the record has no key or headers,
  otherwise the TAB shape below.
- `value`: produce sends the whole line as the value (TAB is not special);
  consume writes only the value. Keys and headers are dropped on output.
- `tsv`: produce parses the TAB shapes below (same as `auto`); consume
  always writes `key<TAB>[h: v<TAB>]value`, with an empty key field for
  null keys and header fields only when present.
- `json`: one JSON object per line/record; aliases `--json`.
- `csv` (produce only): RFC 4180 input; alias `--csv`.

TAB-separated input has these shapes:

```text
value
key<TAB>value
key<TAB>name: value<TAB>...<TAB>value
```

The TAB and newline delimiters are not escaped. Binary data or records
containing delimiters are therefore not generally safe to roundtrip. Null and
empty keys, values, and header values may not remain distinct: a null key is
printed as an empty field, and a null header value as `name: `. Compatible
textual keys and headers can roundtrip: `kite -c --format tsv src |
kite --format tsv dst` preserves keys and headers, while `--format json`
preserves key, headers, and value exactly and `value` drops keys and
headers. A consume-to-produce pipe does not preserve ordering across
partitions, offsets, or timestamps.

`--csv` reads RFC 4180 CSV. The first row supplies column names and each later
row becomes a JSON object value. `--key COL` uses a CSV column as the Kafka
record key while retaining it in the JSON value. Quoted commas, escaped
quotes, embedded newlines, CRLF endings, and a UTF-8 BOM are supported; all
JSON fields are strings.

### JSON records (`--json`, i.e. `--format json`)

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

### Inspecting the effective configuration

`kite --show-config` resolves the effective settings — flags over
environment over the properties file — and prints each key with its origin,
without ever connecting to a broker. An invalid or incomplete configuration
exits 1 with the usual `kite: ...` message; `sasl.password` is redacted.

```text
config file: /home/me/kite.properties
bootstrap.servers        localhost:9092          flag
security.protocol        PLAINTEXT               default
sasl.password            ********               env
...
```

For automation there is a stable-schema JSON form:

```sh
kite --show-config --json | jq -r '.settings["bootstrap.servers"].value'
```

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

- `-q` / `--quiet` suppresses the live status line and the end-of-run
  summaries; data, warnings, and errors are unchanged.
- `-v` / `--verbose` enables connection, retry, and fetch diagnostics.
- `KITE_DEBUG=1` enables frame and TLS diagnostics.
- `KITE_TIME=1` prints producer timing totals and connection count.

The default stripped binary is under 600 KB (CI-gated by
`scripts/check-size.sh`).
`scripts/pack.sh` can produce an optional UPX/LZMA artifact of about 200 KiB.

## Troubleshooting

- **`no broker configured`:** pass `-b HOST:PORT`, set
  `BOOTSTRAP_SERVERS`, or copy a template to one of the search-path
  locations and edit `bootstrap.servers`. `-v` prints which sources were
  used.
- **Topic does not exist:** when producing, kite offers to create it on the
  spot — it asks on a terminal and creates automatically in scripts and
  pipes, using the broker's default partition count and replication factor.
  Consuming a missing topic is an error; kite never creates it there. On
  some hosted clusters a missing topic surfaces as an authorization error
  instead.
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

## FAQ

**How do I produce a message to Kafka from the command line?**
`printf 'hello\n' | kite -b localhost:9092 TOPIC`. Each stdin line is one
record; `key<TAB>value` sets a key, `--json` and `--csv` accept structured
input.

**How do I read the last N messages from a Kafka topic in a script?**
`kite -c -B -n N --idle 3s TOPIC` reads from the beginning and stops after N
records or 3 s of silence, whichever comes first (`-n` alone waits until N
records exist). Without `-n`, a piped `kite -c` stops on its own after 5 s
without data, so `kite -c -B TOPIC > dump.txt` produces a snapshot rather
than hanging.

**Does kite work with Confluent Cloud, Redpanda, Amazon MSK, or Aiven?**
Yes. Any broker that speaks the Kafka protocol works. Set
`SECURITY_PROTOCOL=SASL_SSL`, `SASL_MECHANISM=PLAIN` (or `SCRAM-SHA-256` /
`SCRAM-SHA-512`), `SASL_USERNAME`, and `SASL_PASSWORD`, or copy a template
from [`examples/config/`](examples/config).

**Does kite need Java, the JVM, librdkafka, Docker, or Python?**
No. kite is one static binary with no runtime dependencies.

**How big is kite and how fast does it start?**
The stripped binary is under 600 KB (CI-gated) and starts in milliseconds.

**Can kite create topics or manage consumer groups?**
No. kite only produces to and consumes from existing topics. It does not join
consumer groups or commit offsets; each consume run is stateless.

**Is kite safe to call from an AI agent or CI job?**
Yes: it never prompts, never hangs in a pipe, writes data only to stdout and
diagnostics only to stderr, and returns exit code 1 with a one-line
`kite: ...` message on any failure.

**Which compression codecs can kite consume?**
gzip, Snappy, and LZ4. zstd is intentionally omitted to keep the binary small.

## Development and testing

```sh
zig build
zig build test
scripts/cli-check.sh
scripts/check-size.sh
scripts/pack.sh
scripts/smoke.sh TOPIC
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
