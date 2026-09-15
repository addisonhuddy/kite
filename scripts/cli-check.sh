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
run_case install-help 0 nonempty empty install --help
run_case install-unknown 1 empty "unknown option" install --bogus
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

run_case consume-no-args 1 empty "kite: missing TOPIC" consume
run_case empty-topic 1 empty "kite: TOPIC must not be empty" ""
run_case unknown-option 1 empty "kite: unknown option '--bogus'" --bogus
run_case missing-header 1 empty "kite: -H requires a value" -H
run_case malformed-header 1 empty "kite: malformed header 'nocolon'" -H nocolon demo
run_case missing-key 1 empty "kite: --key requires a value" --key
run_case key-without-csv 1 empty "kite: --key requires --csv" --key id demo
run_case extra-topic 1 empty "kite: unexpected argument 'b'" a b
run_case offset-text 1 empty "kite: --offset: 'abc' is not a non-negative integer" consume --offset abc demo
run_case offset-negative 1 empty "kite: --offset: '-1' is not a non-negative integer" consume --offset -1 demo
run_case offset-overflow 1 empty "kite: --offset: '99999999999999999999' is not a non-negative integer" consume --offset 99999999999999999999 demo
run_case partition-text 1 empty "kite: --partition: 'x' is not a non-negative integer" consume --partition x demo
run_case count-text 1 empty "kite: -n: 'x' is not a non-negative integer" consume -n x demo
run_case timeout-negative 1 empty "kite: -t: '-5' is not a non-negative integer" consume -t -5 demo
run_case conflict-first 1 empty "kite: --offset cannot be combined with --from-beginning" consume --from-beginning --offset 1 demo
run_case conflict-second 1 empty "kite: --offset cannot be combined with --from-beginning" consume --offset 1 --from-beginning demo
run_case consume-unknown 1 empty "kite: unknown option '--bogus'" consume --bogus demo
run_case consume-extra 1 empty "kite: unexpected argument 'b'" consume a b

run_case install-copy 0 nonempty "Add that line" install --dir "$TMP/bin"
[ -x "$TMP/bin/kite" ] || { echo "FAIL install-copy: binary missing"; exit 1; }

set +e
(cd "$TMP/work" && SHELL=/bin/bash HOME="$TMP/home" XDG_CONFIG_HOME="$TMP/xdg" "$BIN" install --dir "$TMP/bin" --yes >"$TMP/install-yes.out" 2>"$TMP/install-yes.err")
status=$?
set -e
[ "$status" -eq 0 ] || { echo "FAIL install-yes: exit $status"; exit 1; }
grep -Fq "export PATH=\"$TMP/bin:\$PATH\"" "$TMP/home/.bashrc" || {
    echo "FAIL install-yes: PATH line missing"; exit 1;
}
echo "PASS install-yes"

run_case produce-no-config 1 empty "no kite.properties found" demo
run_case consume-no-config 1 empty "no kite.properties found" consume demo

grep -Fq "Try 'kite --help' for examples." "$TMP/unknown-option.err"
grep -Fq "Try 'kite consume --help' for examples." "$TMP/consume-unknown.err"
for name in empty-topic unknown-option missing-header malformed-header missing-key key-without-csv extra-topic; do
    grep -Fq "Try 'kite --help' for examples." "$TMP/$name.err" || {
        echo "FAIL $name: missing root help hint"; exit 1;
    }
done
for name in consume-no-args offset-text offset-negative offset-overflow partition-text count-text timeout-negative conflict-first conflict-second consume-unknown consume-extra; do
    grep -Fq "Try 'kite consume --help' for examples." "$TMP/$name.err" || {
        echo "FAIL $name: missing consume help hint"; exit 1;
    }
done
echo "PASS help hints"
