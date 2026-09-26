# bash completion for kite — source this file or drop it in
# /etc/bash_completion.d/ (or ~/.local/share/bash-completion/completions/).

_kite() {
    local cur prev cmd w
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD - 1]}"
    cmd=""
    for w in "${COMP_WORDS[@]:1:COMP_CWORD-1}"; do
        case "$w" in
            produce | p | consume | c | targets | config) cmd=$w ;;
        esac
    done

    # @NAME picks a cluster from kite.yaml.
    if [[ $cur == @* ]] && [ -f kite.yaml ]; then
        local names
        names=$(awk '/^clusters:/{f=1;next} f&&/^[^ ]/{f=0} f&&/^  [A-Za-z0-9_-]+:/{sub(/^  /,"@");sub(/:.*/,"");print}' kite.yaml | sort -u)
        COMPREPLY=($(compgen -W "$names" -- "$cur"))
        return
    fi

    case "$prev" in
        --format)
            COMPREPLY=($(compgen -W "value tsv json csv" -- "$cur"))
            return
            ;;
        --config)
            COMPREPLY=($(compgen -f -- "$cur"))
            return
            ;;
        -b | --bootstrap | -H | --key | --offset | --partition | -n | --max | -t | --idle)
            return
            ;;
    esac

    local opts
    case "$cmd" in
        consume | c)
            opts="-b --bootstrap --config --format --json \
                -B --from-beginning --offset --partition -n --max -t --idle \
                -f --follow -q --quiet -v --verbose -h --help"
            ;;
        config)
            opts="-b --bootstrap --config --format --json -q --quiet -v --verbose -h --help"
            ;;
        targets)
            opts="--config --format --json -q --quiet -v --verbose -h --help"
            ;;
        produce | p)
            opts="-b --bootstrap --config --format --json -H --csv --key \
                -q --quiet -v --verbose -h --help"
            ;;
        *)
            opts="produce p consume c targets config \
                -V --version -h --help"
            ;;
    esac
    COMPREPLY=($(compgen -W "$opts" -- "$cur"))
}

complete -F _kite kite
