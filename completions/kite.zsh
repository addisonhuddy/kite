#compdef kite

# zsh completion for kite — install as _kite on fpath before compinit runs:
#   mkdir -p ~/.zfunc && cp completions/kite.zsh ~/.zfunc/_kite
#   # in ~/.zshrc: fpath=(~/.zfunc $fpath); autoload -Uz compinit && compinit

# Complete the cluster names under `clusters:` in ./kite.yaml.
_kite_targets() {
    local -a names
    names=(${(f)"$(awk '/^clusters:/{f=1;next} f&&/^[^ ]/{f=0} f&&/^  [A-Za-z0-9_-]+:/{sub(/^  /,"");sub(/:.*/,"");print}' kite.yaml 2>/dev/null | sort -u)"})
    compadd -a names
}

# Complete @NAME cluster positionals.
_kite_at_targets() {
    local -a names
    names=(${(f)"$(awk '/^clusters:/{f=1;next} f&&/^[^ ]/{f=0} f&&/^  [A-Za-z0-9_-]+:/{sub(/^  /,"@");sub(/:.*/,"");print}' kite.yaml 2>/dev/null | sort -u)"})
    compadd -a names
}

_kite() {
    local -a shared
    shared=(
        '(-b --bootstrap)'{-b,--bootstrap}'[comma-separated host:port brokers]:hosts:'
        '--config[read this properties file]:file:_files'
        '--format[record shape]:format:(value tsv json csv)'
        '--json[one JSON object per record (same as --format json)]'
        '(-q --quiet)'{-q,--quiet}'[suppress summary and progress lines]'
        '(-v --verbose)'{-v,--verbose}'[diagnostics on stderr]'
        '(-h --help)'{-h,--help}'[show help]'
    )

    if (( CURRENT == 2 )); then
        _arguments -s \
            '(-V --version)'{-V,--version}'[print version]' \
            '(-h --help)'{-h,--help}'[show help]' \
            '1:command:(produce p consume c targets config)'
        return
    fi

    case "$words[2]" in
        consume | c)
            _arguments -s $shared \
                '(-B --from-beginning)'{-B,--from-beginning}'[start at earliest offset]' \
                '--offset[start at offset N]:offset:' \
                '--partition[read one partition]:partition:' \
                '(-n --max)'{-n,--max}'[stop after MAX records]:max:' \
                '(-t --idle)'{-t,--idle}'[stop after idle duration]:duration:' \
                '(-f --follow)'{-f,--follow}'[never stop on idle]' \
                '*:arg:_kite_at_targets'
            ;;
        config)
            _arguments -s \
                '(-b --bootstrap)'{-b,--bootstrap}'[comma-separated host:port brokers]:hosts:' \
                '--config[read this properties file]:file:_files' \
                '--format[output shape]:format:(json)' \
                '--json[JSON output]' \
                '(-q --quiet)'{-q,--quiet}'[suppress progress lines]' \
                '(-v --verbose)'{-v,--verbose}'[diagnostics]' \
                '(-h --help)'{-h,--help}'[show help]' \
                '*:cluster:_kite_at_targets'
            ;;
        targets)
            _arguments -s \
                '--config[read this config file]:file:_files' \
                '--format[output shape]:format:(json)' \
                '--json[JSON output]' \
                '(-q --quiet)'{-q,--quiet}'[suppress the source note]' \
                '(-v --verbose)'{-v,--verbose}'[diagnostics]' \
                '(-h --help)'{-h,--help}'[show help]' \
                '*:cluster:_kite_at_targets'
            ;;
        produce | p)
            _arguments -s $shared \
                '-H[add a name: value header]:header:' \
                '--csv[read RFC 4180 CSV (same as --format csv)]' \
                '--key[use CSV column as record key]:column:' \
                '*:arg:_kite_at_targets'
            ;;
    esac
}

_kite "$@"
