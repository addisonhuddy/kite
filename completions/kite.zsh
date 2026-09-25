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

_kite() {
    local -a shared
    shared=(
        '(-b --bootstrap)'{-b,--bootstrap}'[comma-separated host:port brokers]:hosts:'
        '--config[read this properties file]:file:_files'
        '--target[use cluster NAME from kite.yaml]:name:_kite_targets'
        '--format[record shape]:format:(value tsv json csv)'
        '--json[one JSON object per record (same as --format json)]'
        '(-q --quiet)'{-q,--quiet}'[suppress summary and progress lines]'
        '(-v --verbose)'{-v,--verbose}'[diagnostics on stderr]'
        '(-h --help)'{-h,--help}'[show help]'
        '(-V --version)'{-V,--version}'[print version]'
    )

    if (( ${words[(I)(-c|--consume)]} )); then
        _arguments -s $shared \
            '(-c --consume)'{-c,--consume}'[consume mode]' \
            '(-B --from-beginning)'{-B,--from-beginning}'[start at earliest offset]' \
            '--offset[start at offset N]:offset:' \
            '--partition[read one partition]:partition:' \
            '(-n --max)'{-n,--max}'[stop after MAX records]:max:' \
            '(-t --idle)'{-t,--idle}'[stop after idle duration]:duration:' \
            '(-f --follow)'{-f,--follow}'[never stop on idle]' \
            '*:topic:'
    elif (( ${words[(I)--targets]} )); then
        _arguments -s \
            '--targets[list clusters in kite.yaml]' \
            '--config[read this config file]:file:_files' \
            '--target[mark cluster NAME as selected]:name:_kite_targets' \
            '--format[output shape]:format:(json)' \
            '--json[JSON output]' \
            '(-q --quiet)'{-q,--quiet}'[suppress the source note]' \
            '(-v --verbose)'{-v,--verbose}'[diagnostics]' \
            '(-h --help)'{-h,--help}'[show help]'
    elif (( ${words[(I)--show-config]} )); then
        _arguments -s \
            '--show-config[show effective configuration]' \
            '(-b --bootstrap)'{-b,--bootstrap}'[comma-separated host:port brokers]:hosts:' \
            '--config[read this properties file]:file:_files' \
            '--target[use cluster NAME from kite.yaml]:name:_kite_targets' \
            '--format[output shape]:format:(json)' \
            '--json[JSON output]' \
            '(-q --quiet)'{-q,--quiet}'[suppress progress lines]' \
            '(-v --verbose)'{-v,--verbose}'[diagnostics]' \
            '(-h --help)'{-h,--help}'[show help]'
    else
        _arguments -s $shared \
            '(-c --consume)'{-c,--consume}'[consume mode]' \
            '--show-config[show effective configuration]' \
            '--targets[list clusters in kite.yaml]' \
            '-H[add a name: value header]:header:' \
            '--csv[read RFC 4180 CSV (same as --format csv)]' \
            '--key[use CSV column as record key]:column:' \
            '*:topic:'
    fi
}

_kite "$@"
