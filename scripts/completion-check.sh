#!/usr/bin/env bash
# Install the shell completions exactly as the README "Shell completions"
# section says, into a throwaway HOME, and assert that each shell actually
# completes kite options. Shells that are not installed are skipped.
#
#   scripts/completion-check.sh
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# A `kite` on PATH so fish's `complete -C` treats the command as real.
mkdir -p "$TMP/bin"
if [ -x zig-out/bin/kite ]; then
    ln -s "$ROOT/zig-out/bin/kite" "$TMP/bin/kite"
else
    printf '#!/bin/sh\nexit 0\n' >"$TMP/bin/kite"
    chmod +x "$TMP/bin/kite"
fi
export PATH="$TMP/bin:$PATH"
# A kite.yaml in the cwd, so @NAME completes cluster names.
mkdir -p "$TMP/work"
printf 'clusters:\n  local-a:\n    bootstrap.servers: a:1\n  local-b:\n    bootstrap.servers: b:1\n' >"$TMP/work/kite.yaml"
cd "$TMP/work"

status=0
fail() {
    echo "FAIL $1" >&2
    status=1
}
expect() { # expect NAME OUTPUT NEEDLE...
    local name=$1 out=$2 needle
    shift 2
    for needle in "$@"; do
        if ! grep -qxF -- "$needle" <<<"$out"; then
            echo "--- $name completions:" >&2
            echo "$out" >&2
            fail "$name: missing '$needle'"
            return
        fi
    done
    echo "PASS $name"
}

# ---------------------------------------------------------------- bash
if command -v bash >/dev/null; then
    H="$TMP/bash-home"
    mkdir -p "$H"
    (
        export HOME=$H
        mkdir -p ~/.local/share/bash-completion/completions
        cp "$ROOT/completions/kite.bash" ~/.local/share/bash-completion/completions/kite
    )
    out=$(HOME=$H bash --noprofile --norc -c '
        source ~/.local/share/bash-completion/completions/kite
        complete -p kite >/dev/null || exit 1
        COMP_WORDS=(kite consume --fr); COMP_CWORD=2; COMP_LINE="kite consume --fr"; COMP_POINT=${#COMP_LINE}
        _kite && printf "%s\n" "${COMPREPLY[@]}"
        COMP_WORDS=(kite produce --cs); COMP_CWORD=2; COMP_LINE="kite produce --cs"; COMP_POINT=${#COMP_LINE}
        _kite && printf "%s\n" "${COMPREPLY[@]}"
        COMP_WORDS=(kite con); COMP_CWORD=1; COMP_LINE="kite con"; COMP_POINT=${#COMP_LINE}
        _kite && printf "%s\n" "${COMPREPLY[@]}"
        COMP_WORDS=(kite produce @local); COMP_CWORD=2; COMP_LINE="kite produce @local"; COMP_POINT=${#COMP_LINE}
        _kite && printf "%s\n" "${COMPREPLY[@]}"
    ') || fail "bash: completion script did not load"
    expect bash "$out" --from-beginning --csv consume config @local-b
else
    echo "SKIP bash"
fi

# ---------------------------------------------------------------- zsh
if command -v zsh >/dev/null; then
    H="$TMP/zsh-home"
    mkdir -p "$H"
    (
        export HOME=$H
        mkdir -p ~/.zfunc
        cp "$ROOT/completions/kite.zsh" ~/.zfunc/_kite
        cat >>~/.zshrc <<'EOF'
fpath=(~/.zfunc $fpath)
autoload -Uz compinit && compinit
EOF
    )
    # Drive the real completion system: an interactive zsh under zpty, with a
    # widget that runs _main_complete and prints every candidate compadd saw.
    cat >"$TMP/zsh-driver.zsh" <<'EOF'
zmodload zsh/zpty
zmodload zsh/datetime
typeset -g buf=""
# Read the pty until $buf matches PATTERN (or 10s pass); everything read is
# echoed to stderr so a failure leaves a transcript.
waitfor() {
    local deadline=$((EPOCHREALTIME + 10)) chunk
    while (( EPOCHREALTIME < deadline )); do
        if zpty -r -t kite_zsh chunk; then
            buf+=$chunk
            print -rn -- "$chunk" >&2
            [[ $buf == ${~1} ]] && return 0
            # compaudit found group-writable system fpath dirs (CI runners
            # have them) and asks whether to continue: answer y. That is a
            # property of the machine, not of the README instructions.
            if [[ $buf == *'abort compinit [n]? ' ]]; then
                zpty -w -n kite_zsh y
                buf=""
            fi
        else
            sleep 0.05
        fi
    done
    print -r -- "TIMEOUT waiting for $1" >&2
    return 1
}
zpty -b kite_zsh zsh -i
# Let .zshrc (and any compaudit prompt) finish before typing.
waitfor '*[%$#] *' || exit 1
zpty -w kite_zsh 'PROMPT=""; RPROMPT=""; setopt no_beep; zstyle ":completion:*" completer _complete
__cands=()
compadd() { local -a __r; builtin compadd -O __r "$@"; __cands+=($__r) }
__comptest() { __cands=(); _main_complete; print -rl -- "<<" $__cands ">>" }
zle -C __comptest complete-word __comptest
bindkey "^I" __comptest
print READY'
waitfor '*READY*' || exit 1
for line in "$@"; do
    buf=""
    zpty -w -n kite_zsh "$line"$'\t'
    waitfor '*>>*' || exit 1
    print -r -- "$buf"
    zpty -w kite_zsh $'\x15' # ^U: clear the line for the next case
done
zpty -w kite_zsh 'exit'
EOF
    raw=$(HOME=$H TERM=dumb timeout 60s zsh -f "$TMP/zsh-driver.zsh" 'kite consume --fr' 'kite produce --cs' 'kite con' 2>"$TMP/zsh-transcript") || true
    out=$(tr -d '\r' <<<"$raw" | sed -n '/<</,/>>/p' | sed 's/.*<<//; s/>>.*//' | tr -d ' ' | grep -v '^$') || true
    [ -n "$out" ] || { echo "--- zsh transcript:" >&2; cat -v "$TMP/zsh-transcript" >&2; echo >&2; }
    expect zsh "$out" --from-beginning --csv consume config
else
    echo "SKIP zsh"
fi

# ---------------------------------------------------------------- fish
if command -v fish >/dev/null; then
    H="$TMP/fish-home"
    mkdir -p "$H"
    HOME=$H XDG_CONFIG_HOME= fish -c '
        set -q __fish_config_dir; or set __fish_config_dir ~/.config/fish
        mkdir -p $__fish_config_dir/completions
        cp '"$ROOT"'/completions/kite.fish $__fish_config_dir/completions/'
    out=$(HOME=$H XDG_CONFIG_HOME= fish -c 'complete -C "kite consume --fr"; complete -C "kite produce --cs"; complete -C "kite con"' | cut -f1)
    expect fish "$out" --from-beginning --csv consume config
else
    echo "SKIP fish"
fi

exit $status
