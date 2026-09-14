# examples/

Copy-pasteable inputs and `kite.properties` templates. None of these files
are part of the build — the release artifact is just `zig-out/bin/kite`.

## Quickstart

```console
$ cp examples/config/plaintext.properties kite.properties   # set bootstrap.servers
$ zig build
$ zig-out/bin/kite my-topic < examples/data/lines.txt
5 record(s) produced to 'my-topic'
$ zig-out/bin/kite consume --from-beginning -t 3000 my-topic
```

## Contents

- `data/` — stdin-ready inputs: plain lines, keyed/header TSV rows, and CSV
  files for `--csv` / `--csv --key`.
- `config/` — `kite.properties` templates for PLAINTEXT, SSL,
  SASL_SSL+PLAIN (Confluent Cloud), and SASL+SCRAM. Copy one to
  `./kite.properties` (or `~/.config/kite/kite.properties`) and edit
  `bootstrap.servers` / credentials.

## Cookbook

```console
# produce
$ zig-out/bin/kite t1 < examples/data/lines.txt                 # value-only lines
$ zig-out/bin/kite t1 < examples/data/keyed.tsv                 # key<TAB>value
$ zig-out/bin/kite t1 < examples/data/headers.tsv               # key, headers, value
$ zig-out/bin/kite --csv t1 < examples/data/users.csv           # rows → JSON values
$ zig-out/bin/kite --csv --key user_id t1 < examples/data/events.csv
$ tail -f app.log | zig-out/bin/kite logs                       # stream a log live

# consume
$ zig-out/bin/kite consume --from-beginning -t 3000 t1          # dump a topic
$ zig-out/bin/kite consume -n 10 --partition 0 t1               # one partition, 10 records
$ zig-out/bin/kite consume --from-beginning -t 5000 src | zig-out/bin/kite dst
                                                                # copy a topic
```
