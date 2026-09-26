#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=$(realpath "${1:-zig-out/bin/kite}")
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

[ -x "$BIN" ] || { echo "run zig build first" >&2; exit 1; }
mkdir -p "$TMP/home" "$TMP/xdg" "$TMP/work"

run_case() {
    local name=$1 expected_status=$2 expected_out=$3 expected_err=$4
    shift 4
    local out="$TMP/$name.out" err="$TMP/$name.err" status
    set +e
    (cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" "$@" >"$out" 2>"$err")
    status=$?
    set -e
    if [ "$status" -ne "$expected_status" ]; then
        echo "FAIL $name: exit $status (want $expected_status)"
        return 1
    fi
    if [ "$expected_out" = nonempty ] && [ ! -s "$out" ]; then
        echo "FAIL $name: stdout is empty"
        return 1
    elif [ "$expected_out" = empty ] && [ -s "$out" ]; then
        echo "FAIL $name: stdout is not empty"
        return 1
    fi
    if [ "$expected_err" = empty ] && [ -s "$err" ]; then
        echo "FAIL $name: stderr is not empty"
        return 1
    elif [ "$expected_err" != empty ] && ! grep -Fq "$expected_err" "$err"; then
        echo "FAIL $name: stderr lacks '$expected_err'"
        return 1
    fi
    echo "PASS $name"
}

run_case root-help 0 nonempty empty --help
cp "$TMP/root-help.out" "$TMP/root-help.long"
run_case root-short-help 0 nonempty empty -h
cmp -s "$TMP/root-help.out" "$TMP/root-short-help.out" || {
    echo "FAIL root help differs between -h and --help"
    exit 1
}
run_case consume-help 0 nonempty empty consume --help
run_case consume-short-help 0 nonempty empty consume -h
cmp -s "$TMP/consume-help.out" "$TMP/consume-short-help.out" || {
    echo "FAIL consume help differs between -h and --help"
    exit 1
}
cmp -s "$TMP/root-help.out" "$TMP/consume-help.out" && {
    echo "FAIL consume help is identical to root help"
    exit 1
}
run_case produce-help 0 nonempty empty produce --help
run_case targets-help 0 nonempty empty targets --help
run_case config-help 0 nonempty empty config --help
for page in "$TMP/root-help.out" "$TMP/consume-help.out" "$TMP/produce-help.out" "$TMP/targets-help.out" "$TMP/config-help.out"; do
    if awk 'length($0) > 80 { bad=1 } END { exit bad }' "$page"; then :; else
        echo "FAIL help line exceeds 80 columns"; exit 1
    fi
    if LC_ALL=C grep -q $'\033' "$page"; then
        echo "FAIL help contains an ESC byte"; exit 1
    fi
done

# Run the true no-argument invocation separately.
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" >"$TMP/no-args.out" 2>"$TMP/no-args.err")
status=$?
set -e
[ "$status" -eq 1 ] && grep -Fq "kite: missing command" "$TMP/no-args.err" || {
    echo "FAIL no-args"; exit 1;
}
[ ! -s "$TMP/no-args.out" ] || { echo "FAIL no-args: stdout is not empty"; exit 1; }
echo "PASS no-args"

run_case consume-no-args 1 empty "kite: missing TOPIC" consume
run_case produce-no-args 1 empty "kite: missing TOPIC" produce
run_case alias-produce-no-args 1 empty "kite: missing TOPIC" p
run_case alias-consume-no-args 1 empty "kite: missing TOPIC" c
run_case empty-topic 1 empty "kite: TOPIC must not be empty" produce ""
run_case unknown-command 1 empty "kite: unknown command 'events' (kite now needs a command: kite produce events)" events
run_case unknown-command-suggest 1 empty "kite: unknown command 'produse'; did you mean 'produce'?" produse
run_case unknown-option-first 1 empty "kite: unknown option '--bogus'" --bogus
run_case unknown-option 1 empty "kite: unknown option '--bogus'" produce --bogus
run_case missing-header 1 empty "kite: -H requires a value" produce -H
run_case malformed-header 1 empty "kite: malformed header 'nocolon'" produce -H nocolon demo
run_case missing-key 1 empty "kite: --key requires a value" produce --key
run_case key-without-csv 1 empty "kite: --key requires --csv" produce --key id demo
run_case extra-topic 1 empty "kite: unexpected argument 'b'" produce a b
run_case offset-text 1 empty "kite: --offset: 'abc' is not a non-negative integer" consume --offset abc demo
run_case offset-negative 1 empty "kite: --offset: '-1' is not a non-negative integer" consume --offset -1 demo
run_case offset-overflow 1 empty "kite: --offset: '99999999999999999999' is not a non-negative integer" consume --offset 99999999999999999999 demo
run_case partition-text 1 empty "kite: --partition: 'x' is not a non-negative integer" consume --partition x demo
run_case count-text 1 empty "kite: -n: 'x' is not a non-negative integer" consume -n x demo
run_case timeout-negative 1 empty "kite: -t: '-5' is not a duration (want e.g. 3000, 3s, 500ms, 1m)" consume -t -5 demo
run_case idle-text 1 empty "kite: --idle: 'abc' is not a duration" consume --idle abc demo
run_case follow-idle 1 empty "kite: --follow cannot be combined with --idle" consume -f --idle 1s demo
run_case max-text 1 empty "kite: --max: 'x' is not a non-negative integer" consume --max x demo
run_case produce-only-in-consume 1 empty "kite: '-H' is a produce option; use 'kite produce [OPTIONS] TOPIC'" consume -H 'a: b' demo
run_case consume-only-in-produce 1 empty "kite: '--offset' is a consume option; use 'kite consume [OPTIONS] TOPIC'" produce --offset 1 demo
run_case csv-json 1 empty "kite: --csv cannot be combined with --json" produce --csv --json demo
run_case bootstrap-missing 1 empty "kite: -b requires a value" produce -b
run_case config-missing 1 empty "kite: --config requires a value" produce --config
run_case conflict-first 1 empty "kite: --offset cannot be combined with --from-beginning" consume --from-beginning --offset 1 demo
run_case conflict-second 1 empty "kite: --offset cannot be combined with --from-beginning" consume --offset 1 --from-beginning demo
run_case consume-unknown 1 empty "kite: unknown option '--bogus'" consume --bogus demo
run_case consume-extra 1 empty "kite: unexpected argument 'b'" consume a b
run_case quiet-verbose 1 empty "kite: --quiet cannot be combined with --verbose" produce -q -v demo
run_case format-bad 1 empty "kite: --format: 'x' is not a format (want value, tsv, json, or csv)" produce --format x demo
run_case format-conflict 1 empty "kite: --json cannot be combined with --format tsv" produce --json --format tsv demo
run_case consume-format-csv 1 empty "kite: --format csv is only valid when producing" consume --format csv demo
run_case typo-suggest 1 empty "did you mean '--from-beginning'?" consume --from-begining demo

# 0.1 spellings get migration errors, not silent reinterpretation.
run_case migrate-dash-c 1 empty "kite: '-c' is now a command: kite consume [@CLUSTER] TOPIC" -c demo
run_case migrate-consume 1 empty "kite: '--consume' is now a command: kite consume [@CLUSTER] TOPIC" --consume demo
run_case migrate-show-config 1 empty "kite: '--show-config' is now: kite config" --show-config
run_case migrate-targets 1 empty "kite: '--targets' is now: kite targets" --targets
run_case migrate-target 1 empty "kite: '--target NAME' was replaced by the @NAME positional (e.g. @prod)" --target x produce demo
run_case migrate-target-eq 1 empty "kite: '--target NAME' was replaced by the @NAME positional (e.g. @prod)" --target=prod
run_case migrate-target-mid 1 empty "kite: '--target NAME' was replaced by the @NAME positional (e.g. @prod)" produce --target x events

run_case produce-no-config 1 empty "no broker configured. Pass -b HOST:PORT, set BOOTSTRAP_SERVERS, or create kite.properties" produce demo
run_case consume-no-config 1 empty "no broker configured" consume demo
run_case config-file-missing 1 empty "kite: config file 'nope.properties' not found" produce --config nope.properties demo
run_case version 0 nonempty empty --version
run_case version-short 0 nonempty empty -V
run_case version-extra 1 empty "unknown option '--bogus'" --version --bogus
run_case option-looking-value 1 empty "requires --csv" produce --key -c demo

# -b / BOOTSTRAP_SERVERS bypass the properties-file search entirely and
# reach the connect step (which fails fast against a closed port).
run_case bootstrap-flag 1 empty "connection refused by 127.0.0.1:1" produce -b 127.0.0.1:1 demo
run_case bootstrap-invalid-port 1 empty "invalid bootstrap server '127.0.0.1:notaport'" produce -b 127.0.0.1:notaport demo
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" BOOTSTRAP_SERVERS=127.0.0.1:1 "$BIN" consume demo >"$TMP/bootstrap-env.out" 2>"$TMP/bootstrap-env.err")
status=$?
set -e
[ "$status" -eq 1 ] || { echo "FAIL bootstrap-env: exit $status"; exit 1; }
grep -Fq "connection refused by 127.0.0.1:1" "$TMP/bootstrap-env.err" || {
    echo "FAIL bootstrap-env: stderr lacks connect error"; cat "$TMP/bootstrap-env.err"; exit 1;
}
echo "PASS bootstrap-env"
printf 'bootstrap.servers=127.0.0.1:2\n' >"$TMP/work/kite.properties"
run_case flag-over-file 1 empty "connection refused by 127.0.0.1:1" produce -b 127.0.0.1:1 demo
run_case file-used 1 empty "connection refused by 127.0.0.1:2" produce demo
printf 'security.protocol=BAD\nbootstrap.servers=127.0.0.1:2\n' >"$TMP/work/kite.properties"
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" SECURITY_PROTOCOL=PLAINTEXT "$BIN" produce demo >"$TMP/env-over-invalid-file.out" 2>"$TMP/env-over-invalid-file.err")
status=$?
set -e
[ "$status" -eq 1 ] && grep -Fq "connection refused by 127.0.0.1:2" "$TMP/env-over-invalid-file.err" || {
    echo "FAIL env-over-invalid-file"; cat "$TMP/env-over-invalid-file.err"; exit 1;
}
echo "PASS env-over-invalid-file"
printf 'security.protocol=PLAINTEXT\n' >"$TMP/work/kite.properties"
run_case file-without-bootstrap 1 empty "has no bootstrap.servers; add it, or pass -b HOST:PORT / set BOOTSTRAP_SERVERS" produce demo
rm "$TMP/work/kite.properties"

# kite config: offline config inspection.
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" BOOTSTRAP_SERVERS=h:1 "$BIN" config >"$TMP/show-config-text.out" 2>"$TMP/show-config-text.err")
status=$?
set -e
[ "$status" -eq 0 ] \
    && grep -Eq '^bootstrap\.servers +h:1 +env$' "$TMP/show-config-text.out" \
    && grep -Eq '^security\.protocol +PLAINTEXT +default$' "$TMP/show-config-text.out" \
    && [ ! -s "$TMP/show-config-text.err" ] || {
    echo "FAIL show-config-text"; cat "$TMP/show-config-text.out"; cat "$TMP/show-config-text.err"; exit 1;
}
echo "PASS show-config-text"

printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.username=u\nsasl.password=hunter2\nbootstrap.servers=f:2\n' >"$TMP/work/kite.properties"
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" config >"$TMP/show-config-redact.out" 2>&1)
status=$?
set -e
[ "$status" -eq 0 ] && grep -Fq '********' "$TMP/show-config-redact.out" \
    && ! grep -Fq hunter2 "$TMP/show-config-redact.out" || {
    echo "FAIL show-config-redact"; cat "$TMP/show-config-redact.out"; exit 1;
}
echo "PASS show-config-redact"

set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" config --json -b h:1 >"$TMP/show-config-json.out" 2>"$TMP/show-config-json.err")
status=$?
set -e
[ "$status" -eq 0 ] \
    && grep -Fq '{"file":' "$TMP/show-config-json.out" \
    && grep -Fq '"sasl.password":{"value":"********","source":"file","redacted":true}' "$TMP/show-config-json.out" \
    && grep -Fq '"bootstrap.servers":{"value":"h:1","source":"flag"}' "$TMP/show-config-json.out" \
    || { echo "FAIL show-config-json"; cat "$TMP/show-config-json.out"; exit 1; }
if command -v jq >/dev/null 2>&1; then
    jq -e '.settings' "$TMP/show-config-json.out" >/dev/null || {
        echo "FAIL show-config-json: jq rejected .settings"; exit 1;
    }
fi
echo "PASS show-config-json"

printf 'security.protocol=bogus\nbootstrap.servers=f:2\n' >"$TMP/work/kite.properties"
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" config >"$TMP/show-config-invalid.out" 2>"$TMP/show-config-invalid.err")
status=$?
set -e
[ "$status" -eq 1 ] && [ ! -s "$TMP/show-config-invalid.out" ] \
    && grep -Fq "invalid security.protocol" "$TMP/show-config-invalid.err" || {
    echo "FAIL show-config-invalid"; cat "$TMP/show-config-invalid.err"; exit 1;
}
echo "PASS show-config-invalid"
rm "$TMP/work/kite.properties"

run_case show-config-topic 1 empty "kite: unexpected argument 'demo'" config demo
run_case show-config-format 1 empty "kite: --format: only json is valid with kite config" config --format tsv

# @NAME: named clusters in kite.yaml.
run_case at-target-no-file 1 empty "requires a properties file" produce @x demo
run_case at-target-empty 1 empty "'@' must be followed by a cluster name" produce @ demo
run_case at-target-conflict 1 empty "kite: @prod cannot be combined with @dev" produce @prod @dev demo
run_case at-target-repeat 1 empty "requires a properties file" produce @x @x demo
run_case targets-no-file 1 empty "kite: no config file found" targets
run_case targets-topic 1 empty "kite: unexpected argument 'demo'" targets demo
run_case targets-bootstrap 1 empty "kite: --bootstrap is not valid with kite targets" targets -b x:1
cat >"$TMP/work/kite.yaml" <<'EOF'
defaults:
  linger.ms: 20
  bootstrap.servers: base:1
clusters:
  dev:
    bootstrap.servers: dev:2
  prod:
    bootstrap.servers: prod:3
EOF
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" config @x >"$TMP/target-unknown.out" 2>"$TMP/target-unknown.err")
status=$?
set -e
[ "$status" -eq 1 ] \
    && grep -Fq "no cluster 'x'" "$TMP/target-unknown.err" \
    && grep -Fq "available: dev, prod" "$TMP/target-unknown.err" || {
    echo "FAIL target-unknown"; cat "$TMP/target-unknown.err"; exit 1;
}
echo "PASS target-unknown"
# `kite targets` lists clusters without needing a complete config; '*' marks
# the selected one, and stderr carries only the source note.
run_case targets-list 0 nonempty "kite: clusters from ./kite.yaml" targets
printf 'dev\nprod\n' | cmp -s - "$TMP/targets-list.out" || {
    echo "FAIL targets-list: unexpected stdout"; cat "$TMP/targets-list.out"; exit 1;
}
run_case targets-at 0 nonempty empty targets -q @prod
printf 'dev\nprod *\n' | cmp -s - "$TMP/targets-at.out" || {
    echo "FAIL targets-at: unexpected stdout"; cat "$TMP/targets-at.out"; exit 1;
}
run_case targets-unknown 1 empty "no cluster 'x' in ./kite.yaml (available: dev, prod)" targets @x
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" KITE_TARGET=prod "$BIN" targets --json >"$TMP/targets-json.out" 2>"$TMP/targets-json.err")
status=$?
set -e
[ "$status" -eq 0 ] \
    && [ "$(cat "$TMP/targets-json.out")" = '{"file":"./kite.yaml","target":"prod","targets":["dev","prod"]}' ] \
    && [ ! -s "$TMP/targets-json.err" ] || {
    echo "FAIL targets-json"; cat "$TMP/targets-json.out"; cat "$TMP/targets-json.err"; exit 1;
}
echo "PASS targets-json"
# @NAME selects a cluster for `kite config` exactly like $KITE_TARGET.
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" config @prod --json >"$TMP/at-target-json.out" 2>"$TMP/at-target-json.err")
status=$?
set -e
[ "$status" -eq 0 ] \
    && grep -Fq '"target":"prod"' "$TMP/at-target-json.out" \
    && grep -Fq '"bootstrap.servers":{"value":"prod:3","source":"target"}' "$TMP/at-target-json.out" || {
    echo "FAIL at-target-json"; cat "$TMP/at-target-json.out"; cat "$TMP/at-target-json.err"; exit 1;
}
echo "PASS at-target-json"
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" KITE_TARGET=prod "$BIN" config --json >"$TMP/target-json.out" 2>"$TMP/target-json.err")
status=$?
set -e
[ "$status" -eq 0 ] \
    && grep -Fq '"target":"prod"' "$TMP/target-json.out" \
    && grep -Fq '"bootstrap.servers":{"value":"prod:3","source":"target"}' "$TMP/target-json.out" \
    && grep -Fq '"linger.ms":{"value":"20","source":"file"}' "$TMP/target-json.out" || {
    echo "FAIL target-json"; cat "$TMP/target-json.out"; cat "$TMP/target-json.err"; exit 1;
}
echo "PASS target-json"
printf 'bootstrap.servers=base:1\n' >"$TMP/work/kite.properties"
rm "$TMP/work/kite.yaml"
run_case target-properties-file 1 empty "properties files define a single cluster" produce @dev demo
run_case targets-properties-file 0 empty "defines no clusters" targets
rm "$TMP/work/kite.properties"
cat >"$TMP/work/kite.yaml" <<'EOF'
default: dev
clusters:
  dev:
    bootstrap.servers: dev:2
EOF
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" config --json >"$TMP/target-default.out" 2>"$TMP/target-default.err")
status=$?
set -e
[ "$status" -eq 0 ] \
    && grep -Fq '"target":"dev"' "$TMP/target-default.out" \
    && grep -Fq '"bootstrap.servers":{"value":"dev:2","source":"target"}' "$TMP/target-default.out" || {
    echo "FAIL target-default"; cat "$TMP/target-default.out"; cat "$TMP/target-default.err"; exit 1;
}
echo "PASS target-default"
printf 'default: dev\nclusters\n  dev:\n' >"$TMP/work/kite.yaml"
run_case target-yaml-syntax 1 empty "kite.yaml:2: missing ':'" produce demo
cat >"$TMP/work/foo.yml" <<'EOF'
clusters:
  prod:
    bootstrap.servers: prod:3
EOF
rm "$TMP/work/kite.yaml"
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" config --config foo.yml @prod --json >"$TMP/target-yml.out" 2>"$TMP/target-yml.err")
status=$?
set -e
[ "$status" -eq 0 ] \
    && grep -Fq '"target":"prod"' "$TMP/target-yml.out" \
    && grep -Fq '"bootstrap.servers":{"value":"prod:3","source":"target"}' "$TMP/target-yml.out" || {
    echo "FAIL target-yml"; cat "$TMP/target-yml.out"; cat "$TMP/target-yml.err"; exit 1;
}
echo "PASS target-yml"

grep -Fq "Try 'kite --help' for the list of commands." "$TMP/unknown-option-first.err"
grep -Fq "Try 'kite --help' for the list of commands." "$TMP/migrate-dash-c.err"
grep -Fq "Try 'kite consume --help' for examples." "$TMP/consume-unknown.err"
for name in empty-topic unknown-option missing-header malformed-header missing-key key-without-csv extra-topic; do
    grep -Fq "Try 'kite produce --help' for examples." "$TMP/$name.err" || {
        echo "FAIL $name: missing produce help hint"; exit 1;
    }
done
for name in consume-no-args offset-text offset-negative offset-overflow partition-text count-text timeout-negative conflict-first conflict-second consume-unknown consume-extra; do
    grep -Fq "Try 'kite consume --help' for examples." "$TMP/$name.err" || {
        echo "FAIL $name: missing consume help hint"; exit 1;
    }
done
echo "PASS help hints"
