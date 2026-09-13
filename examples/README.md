# examples/

Copy-pasteable inputs and `kannon.properties` templates. None of these files
are part of the build — the release artifact is just `zig-out/bin/kannon`.

## Quickstart

```console
$ cp examples/config/plaintext.properties kannon.properties
$ zig build
$ zig-out/bin/kannon my-topic < examples/data/lines.txt
```

## Contents

- `data/` — stdin-ready inputs: plain lines, keyed/header TSV rows, and CSV
  files for `--csv` / `--csv --key`.
- `config/` — `kannon.properties` templates for PLAINTEXT, SSL,
  SASL_SSL+PLAIN (Confluent Cloud), SASL+SCRAM, and the local docker-compose
  harness. Copy one to `./kannon.properties` (or
  `~/.config/kannon/kannon.properties`) and edit `bootstrap.servers` /
  credentials.
