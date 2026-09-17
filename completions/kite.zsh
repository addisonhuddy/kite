#compdef kite

# zsh completion for kite — place on fpath as _kite, e.g.
#   cp completions/kite.zsh ~/.zfunc/_kite && fpath=(~/.zfunc $fpath)

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
    elif (( ${words[(I)--show-config]} )); then
        _arguments -s \
            '--show-config[show effective configuration]' \
            '(-b --bootstrap)'{-b,--bootstrap}'[comma-separated host:port brokers]:hosts:' \
            '--config[read this properties file]:file:_files' \
            '--format[output shape]:format:(json)' \
            '--json[JSON output]' \
            '(-q --quiet)'{-q,--quiet}'[suppress progress lines]' \
            '(-v --verbose)'{-v,--verbose}'[diagnostics]' \
            '(-h --help)'{-h,--help}'[show help]'
    elif (( ${words[(I)(-i|--install)]} )); then
        _arguments -s \
            '(-i --install)'{-i,--install}'[install mode]' \
            '--dir[install to DIR]:directory:_files -/' \
            '(-y --yes)'{-y,--yes}'[add to PATH without prompting]' \
            '(-h --help)'{-h,--help}'[show help]'
    else
        _arguments -s $shared \
            '(-c --consume)'{-c,--consume}'[consume mode]' \
            '(-i --install)'{-i,--install}'[install mode]' \
            '--show-config[show effective configuration]' \
            '-H[add a name: value header]:header:' \
            '--csv[read RFC 4180 CSV (same as --format csv)]' \
            '--key[use CSV column as record key]:column:' \
            '*:topic:'
    fi
}

_kite "$@"
