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
run_case consume-help 0 nonempty empty -c --help
run_case consume-short-help 0 nonempty empty -c -h
run_case install-help 0 nonempty empty -i --help
run_case install-unknown 1 empty "unknown option" -i --bogus
cmp -s "$TMP/consume-help.out" "$TMP/consume-short-help.out" || {
    echo "FAIL consume help differs between -h and --help"
    exit 1
}
cmp -s "$TMP/root-help.out" "$TMP/consume-help.out" && {
    echo "FAIL consume help is identical to root help"
    exit 1
}
for page in "$TMP/root-help.out" "$TMP/consume-help.out"; do
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
[ "$status" -eq 1 ] && grep -Fq "kite: missing TOPIC" "$TMP/no-args.err" || {
    echo "FAIL no-args"; exit 1;
}
[ ! -s "$TMP/no-args.out" ] || { echo "FAIL no-args: stdout is not empty"; exit 1; }
echo "PASS no-args"

run_case consume-no-args 1 empty "kite: missing TOPIC" -c
run_case empty-topic 1 empty "kite: TOPIC must not be empty" ""
run_case unknown-option 1 empty "kite: unknown option '--bogus'" --bogus
run_case missing-header 1 empty "kite: -H requires a value" -H
run_case malformed-header 1 empty "kite: malformed header 'nocolon'" -H nocolon demo
run_case missing-key 1 empty "kite: --key requires a value" --key
run_case key-without-csv 1 empty "kite: --key requires --csv" --key id demo
run_case extra-topic 1 empty "kite: unexpected argument 'b'" a b
run_case offset-text 1 empty "kite: --offset: 'abc' is not a non-negative integer" -c --offset abc demo
run_case offset-negative 1 empty "kite: --offset: '-1' is not a non-negative integer" -c --offset -1 demo
run_case offset-overflow 1 empty "kite: --offset: '99999999999999999999' is not a non-negative integer" -c --offset 99999999999999999999 demo
run_case partition-text 1 empty "kite: --partition: 'x' is not a non-negative integer" -c --partition x demo
run_case count-text 1 empty "kite: -n: 'x' is not a non-negative integer" -c -n x demo
run_case timeout-negative 1 empty "kite: -t: '-5' is not a duration (want e.g. 3000, 3s, 500ms, 1m)" -c -t -5 demo
run_case idle-text 1 empty "kite: --idle: 'abc' is not a duration" -c --idle abc demo
run_case follow-idle 1 empty "kite: --follow cannot be combined with --idle" -c -f --idle 1s demo
run_case max-text 1 empty "kite: --max: 'x' is not a non-negative integer" -c --max x demo
run_case produce-only-in-consume 1 empty "kite: '-H' is a produce option and is not valid with -c" -c -H 'a: b' demo
run_case consume-only-in-produce 1 empty "kite: '--offset' is a consume option; use 'kite -c [OPTIONS] TOPIC'" --offset 1 demo
run_case csv-json 1 empty "kite: --csv cannot be combined with --json" --csv --json demo
run_case bootstrap-missing 1 empty "kite: -b requires a value" -b
run_case config-missing 1 empty "kite: --config requires a value" --config
run_case conflict-first 1 empty "kite: --offset cannot be combined with --from-beginning" -c --from-beginning --offset 1 demo
run_case conflict-second 1 empty "kite: --offset cannot be combined with --from-beginning" -c --offset 1 --from-beginning demo
run_case consume-unknown 1 empty "kite: unknown option '--bogus'" -c --bogus demo
run_case consume-extra 1 empty "kite: unexpected argument 'b'" -c a b
run_case quiet-verbose 1 empty "kite: --quiet cannot be combined with --verbose" -q -v demo
run_case format-bad 1 empty "kite: --format: 'x' is not a format (want value, tsv, json, or csv)" --format x demo
run_case format-conflict 1 empty "kite: --json cannot be combined with --format tsv" --json --format tsv demo
run_case consume-format-csv 1 empty "kite: --format csv is only valid when producing" -c --format csv demo
run_case typo-suggest 1 empty "did you mean '--from-beginning'?" -c --from-begining demo

run_case install-copy 0 nonempty "Add that line" -i --dir "$TMP/bin"
[ -x "$TMP/bin/kite" ] || { echo "FAIL install-copy: binary missing"; exit 1; }
run_case install-relative 0 nonempty "$TMP/work/rel/bin" -i --dir rel/bin
[ -x "$TMP/work/rel/bin/kite" ] || { echo "FAIL install-relative: binary missing"; exit 1; }

set +e
(cd "$TMP/work" && SHELL=/bin/bash HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" -i --dir "$TMP/bin" --yes >"$TMP/install-yes.out" 2>"$TMP/install-yes.err")
status=$?
set -e
[ "$status" -eq 0 ] || { echo "FAIL install-yes: exit $status"; exit 1; }
grep -Fq "export PATH=\"$TMP/bin:\$PATH\"" "$TMP/home/.bashrc" || {
    echo "FAIL install-yes: PATH line missing"; exit 1;
}
echo "PASS install-yes"

mkdir -p "$TMP/home2"
printf '%s\n' '# managed by dotfiles' >"$TMP/home2/real.bashrc"
ln -s real.bashrc "$TMP/home2/.bashrc"
set +e
(cd "$TMP/work" && SHELL=/bin/bash HOME="$TMP/home2" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" -i --dir "$TMP/bin" --yes >"$TMP/install-symlink.out" 2>"$TMP/install-symlink.err")
status=$?
set -e
[ "$status" -eq 0 ] || { echo "FAIL install-symlink: exit $status"; exit 1; }
[ -L "$TMP/home2/.bashrc" ] || { echo "FAIL install-symlink: rc symlink replaced"; exit 1; }
grep -Fq "export PATH=\"$TMP/bin:\$PATH\"" "$TMP/home2/real.bashrc" || {
    echo "FAIL install-symlink: PATH line missing"; exit 1;
}
echo "PASS install-symlink"

run_case produce-no-config 1 empty "no broker configured. Pass -b HOST:PORT, set BOOTSTRAP_SERVERS, or create kite.properties" demo
run_case consume-no-config 1 empty "no broker configured" -c demo
run_case config-file-missing 1 empty "kite: config file 'nope.properties' not found" --config nope.properties demo
run_case version 0 nonempty empty --version
run_case version-extra 1 empty "unknown option '--version'" --version --bogus
run_case mode-looking-option-value 1 empty "requires --csv" --key -c demo
run_case mode-conflict 1 empty "cannot be combined" -c -i demo
run_case topic-named-consume 1 empty "no broker configured" consume
run_case flag-after-topic 1 empty "no broker configured" events -c

# -b / BOOTSTRAP_SERVERS bypass the properties-file search entirely and
# reach the connect step (which fails fast against a closed port).
run_case bootstrap-flag 1 empty "connection refused by 127.0.0.1:1" -b 127.0.0.1:1 demo
run_case bootstrap-invalid-port 1 empty "invalid bootstrap server '127.0.0.1:notaport'" -b 127.0.0.1:notaport demo
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" BOOTSTRAP_SERVERS=127.0.0.1:1 "$BIN" -c demo >"$TMP/bootstrap-env.out" 2>"$TMP/bootstrap-env.err")
status=$?
set -e
[ "$status" -eq 1 ] || { echo "FAIL bootstrap-env: exit $status"; exit 1; }
grep -Fq "connection refused by 127.0.0.1:1" "$TMP/bootstrap-env.err" || {
    echo "FAIL bootstrap-env: stderr lacks connect error"; cat "$TMP/bootstrap-env.err"; exit 1;
}
echo "PASS bootstrap-env"
printf 'bootstrap.servers=127.0.0.1:2\n' >"$TMP/work/kite.properties"
run_case flag-over-file 1 empty "connection refused by 127.0.0.1:1" -b 127.0.0.1:1 demo
run_case file-used 1 empty "connection refused by 127.0.0.1:2" demo
printf 'security.protocol=BAD\nbootstrap.servers=127.0.0.1:2\n' >"$TMP/work/kite.properties"
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" SECURITY_PROTOCOL=PLAINTEXT "$BIN" demo >"$TMP/env-over-invalid-file.out" 2>"$TMP/env-over-invalid-file.err")
status=$?
set -e
[ "$status" -eq 1 ] && grep -Fq "connection refused by 127.0.0.1:2" "$TMP/env-over-invalid-file.err" || {
    echo "FAIL env-over-invalid-file"; cat "$TMP/env-over-invalid-file.err"; exit 1;
}
echo "PASS env-over-invalid-file"
printf 'security.protocol=PLAINTEXT\n' >"$TMP/work/kite.properties"
run_case file-without-bootstrap 1 empty "has no bootstrap.servers; add it, or pass -b HOST:PORT / set BOOTSTRAP_SERVERS" demo
rm "$TMP/work/kite.properties"

# --show-config: offline config inspection.
set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" BOOTSTRAP_SERVERS=h:1 "$BIN" --show-config >"$TMP/show-config-text.out" 2>"$TMP/show-config-text.err")
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
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" --show-config >"$TMP/show-config-redact.out" 2>&1)
status=$?
set -e
[ "$status" -eq 0 ] && grep -Fq '********' "$TMP/show-config-redact.out" \
    && ! grep -Fq hunter2 "$TMP/show-config-redact.out" || {
    echo "FAIL show-config-redact"; cat "$TMP/show-config-redact.out"; exit 1;
}
echo "PASS show-config-redact"

set +e
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" --show-config --json -b h:1 >"$TMP/show-config-json.out" 2>"$TMP/show-config-json.err")
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
(cd "$TMP/work" && HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" --show-config >"$TMP/show-config-invalid.out" 2>"$TMP/show-config-invalid.err")
status=$?
set -e
[ "$status" -eq 1 ] && [ ! -s "$TMP/show-config-invalid.out" ] \
    && grep -Fq "invalid security.protocol" "$TMP/show-config-invalid.err" || {
    echo "FAIL show-config-invalid"; cat "$TMP/show-config-invalid.err"; exit 1;
}
echo "PASS show-config-invalid"
rm "$TMP/work/kite.properties"

run_case show-config-conflict 1 empty "kite: --show-config cannot be combined with --consume" -c --show-config demo

grep -Fq "Try 'kite --help' for examples." "$TMP/unknown-option.err"
grep -Fq "Try 'kite -c --help' for examples." "$TMP/consume-unknown.err"
for name in empty-topic unknown-option missing-header malformed-header missing-key key-without-csv extra-topic; do
    grep -Fq "Try 'kite --help' for examples." "$TMP/$name.err" || {
        echo "FAIL $name: missing root help hint"; exit 1;
    }
done
for name in consume-no-args offset-text offset-negative offset-overflow partition-text count-text timeout-negative conflict-first conflict-second consume-unknown consume-extra; do
    grep -Fq "Try 'kite -c --help' for examples." "$TMP/$name.err" || {
        echo "FAIL $name: missing consume help hint"; exit 1;
    }
done
echo "PASS help hints"
