# Testing kite

## Unit tests

```console
$ zig build test
```

Covers the wire encoders (byte-exact varint/compact fixtures), record batch
CRC-32C, the properties parser, and the SCRAM implementation against the
RFC 7677 test vector.

## End-to-end: any Kafka 4.0+ broker

There is no bundled broker harness — point kite at any Kafka 4.0+ (KRaft)
cluster by copying an [`examples/config/`](examples/config) template to
`./kite.properties` and setting `bootstrap.servers` (plus auth if needed):

```console
$ cp examples/config/plaintext.properties kite.properties   # edit as needed
$ zig build
$ scripts/smoke.sh <topic>
```

`smoke.sh` produces a few uniquely-marked records (plain and keyed) to an
existing topic, reads them back with `kite consume --from-beginning`, and
diffs the roundtrip. The topic must already exist — kite never auto-creates
topics on produce.

For consumer compression coverage, produce batches to a topic with another
client using `compression.type` set to `none`, `gzip`, `snappy`, `lz4`, and
`zstd`, then compare `kite consume --from-beginning -t 3000 TOPIC | sort`
against the input.

### Debugging

Set `KITE_DEBUG=1` to dump sent/received frames and TLS internals to stderr:

```console
$ KITE_DEBUG=1 sh -c 'echo hi | ./zig-out/bin/kite t1'
```
