# kite

[![CI](https://github.com/addisonhuddy/kite/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/addisonhuddy/kite/actions/workflows/ci.yml)
[![Release](https://github.com/addisonhuddy/kite/actions/workflows/release.yml/badge.svg)](https://github.com/addisonhuddy/kite/actions/workflows/release.yml)
[![Version](https://img.shields.io/github/v/release/addisonhuddy/kite?sort=semver&display_name=tag&label=version)](https://github.com/addisonhuddy/kite/releases/latest)
[![License](https://img.shields.io/github/license/addisonhuddy/kite)](LICENSE)

**kite is an ultra-lightweight Kafka CLI built for agents and sandboxes.**
One binary under 640 KB, no JVM, no runtime, no daemon. stdin in, stdout out,
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

## Run Kafka locally with Docker

Already have a Kafka 4.0+ broker? Skip to the [Quickstart](#quickstart).
Otherwise, a single-node broker on your laptop is one `docker run` away. The
commands below need [Docker](https://docs.docker.com/get-docker/), `curl`,
and (for the JSON example only) [`jq`](https://jqlang.github.io/jq/):

```sh
docker --version && curl --version | head -1 && jq --version
```

Start Kafka (the image kite is tested against; bound to loopback only) and
wait until it answers:

```sh
docker run -d --name kite-kafka -p 127.0.0.1:9092:9092 apache/kafka:4.0.0

until docker exec kite-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done
echo "kafka ready"
```

Check on it or tear it down later with:

```sh
docker ps --filter name=kite-kafka          # status
docker logs kite-kafka | tail               # broker log
docker rm -f kite-kafka                     # stop and remove
```

The default image auto-creates topics, so the Quickstart below works
unchanged. For a fuller check against the same image, `scripts/e2e-docker.sh`
runs the whole broker-backed suite in a throwaway container; see
[TESTING.md](TESTING.md#docker-harness).

### Remapped ports and several brokers: advertised listeners

`-b HOST:PORT` is only how kite finds the *first* broker. The metadata
that broker returns lists every broker by its **advertised listener**
(`advertised.listeners`), and kite connects to those addresses for the
actual produce and fetch traffic. If they are not reachable from where
kite runs, the bootstrap connection succeeds and the very next step
fails with `connection refused` or a timeout.

The default image advertises `localhost:9092`, which is why the
`-p 127.0.0.1:9092:9092` mapping above works and why simply remapping
the host port does not: with `-p 19092:9092` the broker still tells kite
to come back on `localhost:9092`. Set the advertised address to what the
host sees. Any `KAFKA_*` variable replaces the image's whole default
config, so the KRaft basics have to come along:

```sh
docker run -d --name kite-kafka -p 127.0.0.1:19092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka:4.0.0

until kite -q -b localhost:19092 probe </dev/null 2>/dev/null; do sleep 2; done
```

The readiness loop from the previous section will *not* work here:
`kafka-topics.sh` inside the container follows the same advertised
address, and `localhost:19092` means nothing in there. Probe from the
host with kite instead, as shown (an empty produce creates the topic
and exits 0 once the broker answers).

With several brokers each one needs its own host port *and* its own
advertised address on that port, while brokers keep talking to each
other over the Docker network. Two listeners per broker do that:
`INTERNAL` (container names, used between brokers) and `EXTERNAL` (what
the host sees). The common settings, then one container per broker:

```sh
docker network create kafka-net
common=(
  -e KAFKA_PROCESS_ROLES=broker,controller
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-1:9093,2@kafka-2:9093
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
  -e KAFKA_LISTENERS=INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=2
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=2
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
  -e CLUSTER_ID=5L6g3nShT-eMCtK--X86sw
)
docker run -d --name kafka-1 --network kafka-net -p 127.0.0.1:9092:9092 "${common[@]}" \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka-1:29092,EXTERNAL://localhost:9092 \
  apache/kafka:4.0.0
docker run -d --name kafka-2 --network kafka-net -p 127.0.0.1:9093:9092 "${common[@]}" \
  -e KAFKA_NODE_ID=2 \
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka-2:29092,EXTERNAL://localhost:9093 \
  apache/kafka:4.0.0

kite -b localhost:9092,localhost:9093 -c -B events
```

The same rule explains the classic symptoms on any deployment, not just
Docker: a broker that advertises an internal hostname (`kafka-1`, a
private IP, a Kubernetes service name) is reachable for bootstrap through
a tunnel or port-forward yet fails right after, because kite is being
sent to an address only the cluster's own network can resolve. The
error names the advertised address (`connection refused by kafka-1:9092`),
and `kite -v` logs `connected broker N at HOST:PORT` for each one it
reaches. Fix it on the broker side; a client cannot rewrite those
addresses.

## Quickstart

You need a reachable Kafka 4.0+ broker (`localhost:9092` below, e.g. the
Docker one above), permission to read and write the topic you name, and
[`jq`](https://jqlang.github.io/jq/) for the last line only. Use a fresh
topic name so the output is exactly one record; produce creates a missing
topic automatically when run from a script or pipe.

```sh
curl -fsSL https://raw.githubusercontent.com/addisonhuddy/kite/main/install.sh | sh
export BOOTSTRAP_SERVERS=localhost:9092
printf 'hello\n' | kite events                       # produce one record
kite -c -B --idle 3s --json events | jq -r .value    # prints: hello
```

What to expect:

- **stdout is data, stderr is diagnostics.** The produce line prints a short
  summary (`1 record(s) produced to 'events' ...`) on stderr and nothing on stdout;
  the consume line prints the record on stdout and its summary on stderr, so
  `| jq` only ever sees records.
- **`-B` is required here.** Without `--from-beginning` a consumer starts at
  the *latest* offset and would skip the record produced before it started.
- **`--idle 3s`** stops the read 3 s after the last record; otherwise a piped
  consume stops after 5 s idle.
- **`jq` is optional.** It only extracts `.value` from the JSON record; drop
  `--json | jq -r .value` to print plain values.
- **Topic creation depends on the broker.** kite asks the broker to create a
  missing topic when producing from a script or pipe; a broker policy or
  missing ACL can refuse that, in which case create the topic with your admin
  tooling first.

See [Troubleshooting](#troubleshooting) if the roundtrip does not print
`hello`.

## Why kite

If you are an agent reading this (or a human who also likes Kafka), here's
why I think you will love kite.

- **Small.** The stripped binary is under 640 KB (CI-gated by
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

- **Zero-dependency install.** One static binary under 640 KB, fetched with
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
script and fetch the latest release directly (assets are named
`kite-{linux,macos}-{x86_64,aarch64}`):

```sh
curl -fsSL -o kite https://github.com/addisonhuddy/kite/releases/latest/download/kite-linux-x86_64
chmod +x kite && sudo mv kite /usr/local/bin/
```

Prebuilt binaries plus `SHA256SUMS` for every version are on the
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
`--show-config` prints the effective configuration, `--targets` lists
the clusters in kite.yaml. Mode flags may appear anywhere on the command
line; there are no reserved topic names.

```text
kite [OPTIONS] TOPIC          Produce stdin lines to TOPIC (default).
kite -c [OPTIONS] TOPIC       Consume TOPIC to stdout.
kite --show-config            Show the effective configuration.
kite --targets                List the clusters defined in kite.yaml.
kite --version                Print the version.
```

| Mode | Option | Meaning |
| --- | --- | --- |
| produce, consume | `-b`, `--bootstrap HOSTS` | Comma-separated `host:port` brokers (overrides env and file). |
| produce, consume | `--config FILE` | Read this properties file instead of searching. |
| produce, consume | `--target NAME`, `@NAME` | Use cluster `NAME` from kite.yaml (or `$KITE_TARGET`). |
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

`kite --help`, `kite -c --help`, `kite --show-config --help`, and
`kite --targets --help` print the full pages.

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
# named cluster from kite.yaml (see "Multiple clusters" below)
kite @prod events < examples/data/lines.txt
kite --target prod events < examples/data/lines.txt   # same thing
# which clusters are there, and which one is selected?
kite --targets
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
3. A config file: `--config FILE`, else `$KAFKA_PROPERTIES`, else the first of
   `./kite.yaml`, `./kite.properties`,
   `$XDG_CONFIG_HOME/kite/kite.yaml`, `$XDG_CONFIG_HOME/kite/kite.properties`,
   `~/.config/kite/kite.yaml`, `~/.config/kite/kite.properties`.

A file is optional when `-b` or `BOOTSTRAP_SERVERS` supplies the
brokers. With none of these, kite fails with a message listing all three
ways to configure it. A `--config`/`KAFKA_PROPERTIES` path that does not exist is
an error rather than a silent fallback. `-v` prints which sources were used.

The properties parser supports a `key=value` subset of Java properties.
Blank lines and `#`/`!` comments are accepted; unknown keys are ignored
with a warning. `.yaml`/`.yml` files are parsed as a strict YAML subset
(see "Multiple clusters" below).

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

### Multiple clusters (targets)

`kite.yaml` holds several clusters. `clusters:` maps each name to its
settings (any key from the table above); `defaults:` holds shared
settings applied to every cluster; `default:` names the cluster used
when nothing selects one. Cluster names match `[A-Za-z0-9_-]+`.

```yaml
default: dev

defaults:
  linger.ms: 20

clusters:
  dev:
    bootstrap.servers: localhost:9092
  prod:
    bootstrap.servers: pkc-xyz.us-east-1.aws.confluent.cloud:9092
    security.protocol: SASL_SSL
    sasl.mechanism: PLAIN
    sasl.username: KEY
    sasl.password: SECRET
```

Only a strict YAML subset is parsed: block mappings, plain and quoted
scalars, comments. Lists, flow syntax, anchors, and block scalars are
rejected with a `file:line` error.

The cluster is selected by, in order: `--target NAME`, `$KITE_TARGET`,
the file's own `default:` key, else no cluster and only `defaults:`/
unprefixed keys apply. Passing `--target` without any config file is an
error, as is selecting a name the file does not define — including any
name on a properties file, which always describes a single cluster.

`@NAME` is short for `--target NAME` and goes anywhere on the command
line, so switching clusters is one word: `kite @prod events`,
`kite -c @local-b -B events`. A bare `@` is an error; two different
clusters on one command line (`@prod --target dev`) are rejected rather
than letting the last one win. Topics never start with `@`, so there is
no ambiguity.

`kite --targets` lists the clusters, one per line and sorted, with ` *`
after the one that `--target`/`@NAME`/`$KITE_TARGET`/`default:` would
select, and a note on stderr naming the file it read (`-q` drops the
note). It reads only the `clusters:` keys and never resolves a
configuration or connects to a broker, so it works even while a cluster
is still half written or lacks `bootstrap.servers`. `--json` yields
`{"file":..,"target":..,"targets":[..]}`. It exits 1 when there is no
config file at all or the selected name is not defined.

```sh
$ kite --targets
dev *
prod
kite: clusters from ./kite.yaml
$ kite --targets --json | jq -r '.targets[]'
dev
prod
```

Shell completion follows suit: after `--target ` or `@`, the Bash
completion offers the cluster names from `./kite.yaml`.

Within a cluster, value precedence is: flags > the cluster's keys >
environment > `defaults:`/base keys > built-in defaults. A named cluster
is a complete definition, so ambient `BOOTSTRAP_SERVERS` cannot silently
redirect `--target prod`; environment variables still fill in keys the
cluster omits (for example `SASL_PASSWORD` while the cluster supplies
the brokers).

`kite --show-config` prints the selected name on a `target:` line and
reports `target` as the origin of keys that came from the cluster; the
JSON form adds `"target"` and `"targets"` (all names defined in the
file). See [`kafka-configs.yaml`](kafka-configs.yaml) for a fuller sample
(plaintext dev, TLS staging, SASL prod) and
[`examples/config/kite.yaml`](examples/config/kite.yaml).

### Inspecting the effective configuration

`kite --show-config` resolves the effective settings — flags over
environment over the properties file — and prints each key with its origin,
without ever connecting to a broker. An invalid or incomplete configuration
exits 1 with the usual `kite: ...` message; `sasl.password` is redacted.

```text
config file: /home/me/kite.properties
target: prod
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

The default stripped binary is under 640 KB (CI-gated by
`scripts/check-size.sh`).
`scripts/pack.sh` can produce an optional UPX/LZMA artifact of about 200 KiB.

## Troubleshooting

First run against a local broker:

- **`connection refused by localhost:9092`:** nothing is listening on 9092.
  Start the broker (see [Run Kafka locally with Docker](#run-kafka-locally-with-docker))
  and wait for the readiness loop to finish; `docker ps --filter
  name=kite-kafka` shows whether the container is up.
- **Bootstrap works, then `connection refused by HOST:PORT` for another
  address (or a hang) on the first produce/fetch:** the broker advertises
  an address kite cannot reach — typical after remapping the Docker port
  or with several brokers on one host. See
  [advertised listeners](#remapped-ports-and-several-brokers-advertised-listeners).
- **`Cannot connect to the Docker daemon` / `docker: command not found`:**
  start Docker Desktop or `sudo systemctl start docker`, or install Docker;
  on Linux, add your user to the `docker` group to run without `sudo`.
- **`jq: command not found`:** install `jq` (`apt install jq`, `brew install
  jq`) or drop `--json | jq -r .value` from the consume line; kite itself does
  not need it.
- **Produce fails with `not authorized` or a topic-creation error:** the
  broker disables automatic topic creation or the principal lacks the ACL;
  create the topic with your admin tooling and retry.
- **Consume prints nothing and exits after the idle timeout:** `-B` was
  omitted, so the read started at the latest offset, after the record you
  produced. Add `-B` (or produce again while the consumer is running).

General:

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
The stripped binary is under 640 KB (CI-gated) and starts in milliseconds.

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
