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

## Binary size

```sh
scripts/check-size.sh
```

The default stripped binary must stay below 600,000 bytes. `scripts/pack.sh` is an
optional UPX/LZMA packaging step.

## End-to-end: any Kafka 4.0+ broker

There is no bundled broker harness. Point kite at a broker with
`BOOTSTRAP_SERVERS` (or copy an [`examples/config/`](examples/config)
template to `./kite.properties` and set `bootstrap.servers` plus any
authentication), build, and run:

```sh
export BOOTSTRAP_SERVERS=localhost:9092
zig build
scripts/smoke.sh EXISTING_TOPIC
```

The topic must already exist; kite never creates topics. `smoke.sh` produces
marked plain and keyed records, consumes them from the beginning with an idle
timeout, compares the roundtrip, checks a `--json` produce/consume roundtrip
(key, nested value, headers), and asserts that an out-of-range `--offset`
fails with the valid range instead of replaying the partition.

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
