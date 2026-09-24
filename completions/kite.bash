# bash completion for kite — source this file or drop it in
# /etc/bash_completion.d/ (or ~/.local/share/bash-completion/completions/).

_kite() {
    local cur prev mode w
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD - 1]}"
    mode=produce
    for w in "${COMP_WORDS[@]:1:COMP_CWORD-1}"; do
        case "$w" in
            -c | --consume) mode=consume ;;
            --show-config) mode=show-config ;;
        esac
    done

    case "$prev" in
        --format)
            COMPREPLY=($(compgen -W "value tsv json csv" -- "$cur"))
            return
            ;;
        --config)
            COMPREPLY=($(compgen -f -- "$cur"))
            return
            ;;
        --target)
            if [ -f kite.properties ]; then
                local names
                names=$(sed -n 's/^target\.\([A-Za-z0-9_-]*\)\..*/\1/p' kite.properties | sort -u)
                COMPREPLY=($(compgen -W "$names" -- "$cur"))
            fi
            return
            ;;
        -b | --bootstrap | -H | --key | --offset | --partition | -n | --max | -t | --idle)
            return
            ;;
    esac

    local opts
    case "$mode" in
        consume)
            opts="-c --consume -b --bootstrap --config --target --format --json \
                -B --from-beginning --offset --partition -n --max -t --idle \
                -f --follow -q --quiet -v --verbose -h --help"
            ;;
        show-config)
            opts="--show-config -b --bootstrap --config --target --format --json -q --quiet -v --verbose -h --help"
            ;;
        *)
            opts="-c --consume -V --version --show-config \
                -b --bootstrap --config --target --format --json -H --csv --key \
                -q --quiet -v --verbose -h --help"
            ;;
    esac
    COMPREPLY=($(compgen -W "$opts" -- "$cur"))
}

complete -F _kite kite
