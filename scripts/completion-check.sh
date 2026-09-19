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
        cp completions/kite.bash ~/.local/share/bash-completion/completions/kite
    )
    out=$(HOME=$H bash --noprofile --norc -c '
        source ~/.local/share/bash-completion/completions/kite
        complete -p kite >/dev/null || exit 1
        COMP_WORDS=(kite -c --fr); COMP_CWORD=2; COMP_LINE="kite -c --fr"; COMP_POINT=${#COMP_LINE}
        _kite && printf "%s\n" "${COMPREPLY[@]}"
        COMP_WORDS=(kite --cs); COMP_CWORD=1; COMP_LINE="kite --cs"; COMP_POINT=${#COMP_LINE}
        _kite && printf "%s\n" "${COMPREPLY[@]}"
    ') || fail "bash: completion script did not load"
    expect bash "$out" --from-beginning --csv
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
        cp completions/kite.zsh ~/.zfunc/_kite
        cat >>~/.zshrc <<'EOF'
fpath=(~/.zfunc $fpath)
autoload -Uz compinit && compinit
EOF
    )
    # Drive the real completion system: an interactive zsh under zpty, with a
    # widget that runs _main_complete and prints every candidate compadd saw.
    cat >"$TMP/zsh-driver.zsh" <<'EOF'
zmodload zsh/zpty
zpty -b kite_zsh zsh -i
# compaudit may find group-writable system fpath dirs (CI runners do) and ask
# whether to continue; that is a machine property, not a README problem.
zpty -r -m kite_zsh line '*(Ignore insecure directories*|% )*' || exit 1
[[ $line == *'insecure directories'* ]] && zpty -w kite_zsh y
zpty -w kite_zsh 'PROMPT=""; RPROMPT=""; setopt no_beep; zstyle ":completion:*" completer _complete
__cands=()
compadd() { local -a __r; builtin compadd -O __r "$@"; __cands+=($__r) }
__comptest() { __cands=(); _main_complete; print -rl -- "<<" $__cands ">>" }
zle -C __comptest complete-word __comptest
bindkey "^I" __comptest
print READY'
zpty -r -m kite_zsh line '*READY*' || exit 1
for line in "$@"; do
    zpty -w -n kite_zsh "$line"$'\t'
    zpty -r -m kite_zsh line '*>>*' || exit 1
    print -r -- "$line"
    zpty -w kite_zsh $'\x15' # ^U: clear the line for the next case
done
zpty -w kite_zsh 'exit'
EOF
    raw=$(cd "$H" && HOME=$H TERM=dumb timeout 30s zsh -f "$TMP/zsh-driver.zsh" 'kite -c --fr' 'kite --cs' 2>&1) || true
    out=$(tr -d '\r' <<<"$raw" | sed -n '/<</,/>>/p' | sed 's/.*<<//; s/>>.*//' | tr -d ' ' | grep -v '^$') || true
    [ -n "$out" ] || { echo "--- zsh raw transcript:" >&2; echo "$raw" >&2; }
    expect zsh "$out" --from-beginning --csv
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
        cp completions/kite.fish $__fish_config_dir/completions/'
    out=$(HOME=$H XDG_CONFIG_HOME= fish -c 'complete -C "kite -c --fr"; complete -C "kite --cs"' | cut -f1)
    expect fish "$out" --from-beginning --csv
else
    echo "SKIP fish"
fi

exit $status
