# examples/

Copy-pasteable inputs and `kite.properties` templates. None of these files
are part of the build — the release artifact is just `zig-out/bin/kite`.

## Quickstart

```console
$ cp examples/config/plaintext.properties kite.properties
$ zig build
$ zig-out/bin/kite my-topic < examples/data/lines.txt
```

## Contents

- `data/` — stdin-ready inputs: plain lines, keyed/header TSV rows, and CSV
  files for `--csv` / `--csv --key`.
- `config/` — `kite.properties` templates for PLAINTEXT, SSL,
  SASL_SSL+PLAIN (Confluent Cloud), SASL+SCRAM, and the local docker-compose
  harness. Copy one to `./kite.properties` (or
  `~/.config/kite/kite.properties`) and edit `bootstrap.servers` /
  credentials.
