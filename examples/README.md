# examples/

These are copy-pasteable stdin inputs and `kite.properties` templates. They
are not part of the build; the release artifact is `zig-out/bin/kite`.

## Contents

- `data/` — plain lines, keyed/header TSV rows, and CSV files for `--csv` and
  `--csv --key`.
- `wing/` — Confluent Schema Registry and JSON Schema walkthrough, fixtures,
  and a rerunnable kite + wing example.
- `config/` — templates for PLAINTEXT, SSL, SASL_SSL + PLAIN, and SASL +
  SCRAM. Copy one to `./kite.properties` or
  `~/.config/kite/kite.properties`, then edit the broker and credentials.
- `demo.tape` — [VHS](https://github.com/charmbracelet/vhs) script for the
  README's `demo.gif`. Re-record from the repo root with
  `vhs examples/demo.tape` (needs `kite`, `jq`, and a broker on
  `localhost:9092`).

See the [Common recipes](../README.md#common-recipes) section in the main
README for the canonical cookbook.
