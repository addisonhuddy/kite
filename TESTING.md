# Testing kite

## Unit tests

```sh
zig build test
```

This covers the wire encoders, record batches, properties parser, CSV reader,
SCRAM vectors, and the isolated CLI parser.

## CLI regression checks

Build first, then run the offline checks:

```sh
scripts/cli-check.sh
```

The script points `HOME`, `XDG_CONFIG_HOME`, and the working directory at an
empty temporary tree. It captures stdout, stderr, and exit status separately,
checking both help pages and actionable parser errors without a broker.

## Shell completions

```sh
scripts/completion-check.sh
```

For each of bash, zsh, and fish that is installed (others are skipped), the
script installs the completion file into a temporary `HOME` exactly as the
README "Shell completions" snippets do, then drives the shell's real
completion machinery (`_kite` under bash, `compinit` + a zpty-driven widget
under zsh, `complete -C` under fish) and asserts that `kite -c --fr` offers
`--from-beginning` and `kite --cs` offers `--csv`.

## Binary size

```sh
scripts/check-size.sh
```

The default stripped binary must stay below 600,000 bytes. `scripts/pack.sh` is an
optional UPX/LZMA packaging step.

## End-to-end tests

The broker-dependent checks live in `scripts/smoke.sh` and run in one of two
ways: against a broker you already have (this section), or inside a throwaway
Docker broker that `scripts/e2e-docker.sh` starts for you (next section). CI
uses the Docker harness.

### Existing Kafka 4.0+ broker

Point kite at a broker with `BOOTSTRAP_SERVERS` (or copy an [`examples/config/`](examples/config)
template to `./kite.properties` and set `bootstrap.servers` plus any
authentication), build, and run:

```sh
export BOOTSTRAP_SERVERS=localhost:9092
zig build
scripts/smoke.sh TOPIC
```

The topic is created on first produce if it does not exist (consume never
creates topics). `smoke.sh` produces
marked plain and keyed records, consumes them from the beginning with an idle
timeout, compares the roundtrip, checks a `--json` produce/consume roundtrip
(key, nested value, headers), and asserts that an out-of-range `--offset`
fails with the valid range instead of replaying the partition.

### Docker harness

`scripts/e2e-docker.sh` runs the full suite against a real broker with no
setup beyond Docker: it pulls `apache/kafka` (pinned tag), starts a
single-node KRaft container on `127.0.0.1:9092`, creates a `kite-e2e` topic,
and runs `smoke.sh` plus broker-dependent edge cases (the README Quickstart on
a fresh topic, `-n` vs `--idle` stopping behaviour, early pipe closure,
`/dev/full` write errors):

```sh
zig build
scripts/e2e-docker.sh
```

Environment overrides: `KITE_KAFKA_IMAGE` (default `apache/kafka:4.0.0`),
`KITE_E2E_TIMEOUT` (broker readiness deadline in seconds, default 120),
`KITE_E2E_KEEP=1` (keep the container for debugging). On failure the script
dumps the broker logs and always removes the container otherwise.

This runs in CI as the `e2e` job (blocking, after the unit job).

Behaviours worth checking by hand on a terminal (not covered by the scripts):

```sh
kite -c EXISTING_TOPIC                 # 'waiting for records' line, Ctrl-C -> summary, exit 130
kite -c -B EXISTING_TOPIC | head -2    # exits 0 promptly once head closes the pipe
kite -c -B EXISTING_TOPIC | wc -l      # stops after 5 s idle without -t/--idle/-f
seq 1 100000 | kite EXISTING_TOPIC     # live rate line, then produce summary with per-partition offsets
kite -b 127.0.0.1:1 -c EXISTING_TOPIC  # 'connection refused by 127.0.0.1:1'
```

For consumer compression coverage, produce batches with another client using
`none`, `gzip`, `snappy`, and `lz4` (zstd batches fail with
`UnsupportedCompression`), then compare:

```sh
zig-out/bin/kite -c --from-beginning -t 3000 EXISTING_TOPIC | sort
```

## Debugging

Set `KITE_DEBUG=1` to dump frames and TLS details to stderr:

```sh
KITE_DEBUG=1 sh -c 'echo hi | ./zig-out/bin/kite EXISTING_TOPIC'
```
