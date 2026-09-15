# examples/

These are copy-pasteable stdin inputs and `kite.properties` templates. They
are not part of the build; the release artifact is `zig-out/bin/kite`.

## Contents

- `data/` — plain lines, keyed/header TSV rows, and CSV files for `--csv` and
  `--csv --key`.
- `config/` — templates for PLAINTEXT, SSL, SASL_SSL + PLAIN, and SASL +
  SCRAM. Copy one to `./kite.properties` or
  `~/.config/kite/kite.properties`, then edit the broker and credentials.

See the [Common recipes](../README.md#common-recipes) section in the main
README for the canonical cookbook.
