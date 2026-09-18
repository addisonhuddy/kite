#!/usr/bin/env bash
# Verify the PATH line written by `kite --add-to-path` survives shell quoting: eval the
# emitted line in a fresh shell and check PATH's first entry is the literal
# install directory, spaces and all.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=$(realpath "${1:-zig-out/bin/kite}")
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

[ -x "$BIN" ] || { echo "run zig build first" >&2; exit 1; }

fail() { echo "FAIL $1"; exit 1; }

# A directory with a space, a dollar sign, and a double quote — the worst
# case for a double-quoted PATH line.
dir="$TMP/we ird \$dir\"q"

check_eval() {
    local label=$1 shell_env=$2 shell_bin=$3
    local home="$TMP/home-$label"
    mkdir -p "$home"
    set +e
    (cd "$TMP" && HOME="$home" SHELL="$shell_env" PATH=/usr/bin:/bin "$BIN" --add-to-path --dir "$dir" </dev/null >"$TMP/$label.out" 2>"$TMP/$label.err")
    set -e
    local line
    line=$(grep -F 'export PATH=' "$TMP/$label.err") || fail "$label: no export PATH line emitted"
    local got
    got=$("$shell_bin" -c 'eval "$1"; printf %s "$PATH"' _ "$line")
    [ "${got%%:*}" = "$dir" ] || fail "$label: PATH first entry '${got%%:*}' != '$dir'"
    echo "PASS $label"
}

check_eval bash /bin/bash bash

if command -v zsh >/dev/null 2>&1; then
    check_eval zsh /usr/bin/zsh zsh
else
    echo "SKIP zsh (not installed)"
fi

if command -v fish >/dev/null 2>&1; then
    home="$TMP/home-fish"
    mkdir -p "$home"
    set +e
    (cd "$TMP" && HOME="$home" SHELL=/usr/bin/fish PATH=/usr/bin:/bin "$BIN" --add-to-path --dir "$dir" </dev/null >"$TMP/fish.out" 2>"$TMP/fish.err")
    set -e
    line=$(grep -F 'fish_add_path ' "$TMP/fish.err") || fail "fish: no fish_add_path line emitted"
    got=$(fish -c 'eval "$argv"; printf %s "$PATH"' -- "$line")
    [ "${got%% *}" = "$dir" ] || fail "fish: PATH first entry '${got%% *}' != '$dir'"
    echo "PASS fish"
else
    echo "SKIP fish (not installed)"
fi

# Running the unattended install twice must add the line exactly once.
home="$TMP/home-dup"
mkdir -p "$home"
for _ in 1 2; do
    (cd "$TMP" && HOME="$home" SHELL=/bin/bash PATH=/usr/bin:/bin "$BIN" --add-to-path --dir "$TMP/dupbin" --yes </dev/null >/dev/null 2>&1)
done
count=$(grep -cF 'export PATH=' "$home/.bashrc")
[ "$count" -eq 1 ] || fail "duplicate: export line appears $count times in .bashrc"
echo "PASS duplicate-detection"
