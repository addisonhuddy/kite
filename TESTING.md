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

The default stripped binary must stay below 1 MiB. `scripts/pack.sh` is an
optional UPX/LZMA packaging step.

## End-to-end: any Kafka 4.0+ broker

There is no bundled broker harness. Copy an
[`examples/config/`](examples/config) template to `./kite.properties`, set
`bootstrap.servers` and any required authentication, build, and run:

```sh
cp examples/config/plaintext.properties kite.properties
zig build
scripts/smoke.sh EXISTING_TOPIC
```

The topic must already exist; kite never creates topics. `smoke.sh` produces
marked plain and keyed records, consumes them from the beginning with an idle
timeout, and compares the roundtrip.

For consumer compression coverage, produce batches with another client using
`none`, `gzip`, `snappy`, `lz4`, and `zstd`, then compare:

```sh
zig-out/bin/kite consume --from-beginning -t 3000 EXISTING_TOPIC | sort
```

## Debugging

Set `KITE_DEBUG=1` to dump frames and TLS details to stderr:

```sh
KITE_DEBUG=1 sh -c 'echo hi | ./zig-out/bin/kite EXISTING_TOPIC'
```
