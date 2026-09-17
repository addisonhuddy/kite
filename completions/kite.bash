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
            -i | --install) mode=install ;;
            --show-config) mode=show-config ;;
        esac
    done

    case "$prev" in
        --format)
            COMPREPLY=($(compgen -W "value tsv json csv" -- "$cur"))
            return
            ;;
        --config | --dir)
            COMPREPLY=($(compgen -f -- "$cur"))
            return
            ;;
        -b | --bootstrap | -H | --key | --offset | --partition | -n | --max | -t | --idle)
            return
            ;;
    esac

    local opts
    case "$mode" in
        consume)
            opts="-c --consume -b --bootstrap --config --format --json \
                -B --from-beginning --offset --partition -n --max -t --idle \
                -f --follow -q --quiet -v --verbose -h --help"
            ;;
        install)
            opts="-i --install --dir -y --yes -h --help"
            ;;
        show-config)
            opts="--show-config -b --bootstrap --config --format --json -q --quiet -v --verbose -h --help"
            ;;
        *)
            opts="-c --consume -i --install -V --version --show-config \
                -b --bootstrap --config --format --json -H --csv --key \
                -q --quiet -v --verbose -h --help"
            ;;
    esac
    COMPREPLY=($(compgen -W "$opts" -- "$cur"))
}

complete -F _kite kite
