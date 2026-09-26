# fish completion for kite — install with
#   mkdir -p ~/.config/fish/completions && cp completions/kite.fish ~/.config/fish/completions/

set -l cmds produce p consume c targets config

# First word: the four commands (plus the p/c aliases).
complete -c kite -n 'test (count (commandline -opc)[2..-1]) -eq 0' -f -a 'produce p consume c targets config'

# Cluster names from kite.yaml for @NAME positionals.
complete -c kite -f -a '(begin; test -f kite.yaml; and awk "/^clusters:/{f=1;next} f&&/^[^ ]/{f=0} f&&/^  [A-Za-z0-9_-]+:/{sub(/^  /,\"@\");sub(/:.*/,\"\");print}" kite.yaml | sort -u; end)'

# Options valid for every command.
complete -c kite -n '__fish_seen_subcommand_from produce p consume c targets config' -s q -l quiet -d 'Suppress summary and progress lines'
complete -c kite -n '__fish_seen_subcommand_from produce p consume c targets config' -s v -l verbose -d 'Diagnostics on stderr'
complete -c kite -n '__fish_seen_subcommand_from produce p consume c targets config' -s h -l help -d 'Show help'

# produce and consume.
complete -c kite -n '__fish_seen_subcommand_from produce p consume c' -s b -l bootstrap -d 'Comma-separated host:port brokers' -x
complete -c kite -n '__fish_seen_subcommand_from produce p consume c targets config' -l config -d 'Read this properties file' -rF
complete -c kite -n '__fish_seen_subcommand_from produce p consume c targets config' -l format -d 'Record shape' -xa 'value tsv json csv'
complete -c kite -n '__fish_seen_subcommand_from produce p consume c targets config' -l json -d 'One JSON object per record (--format json)'

# produce only.
complete -c kite -n '__fish_seen_subcommand_from produce p' -s H -d 'Add a name: value header' -x
complete -c kite -n '__fish_seen_subcommand_from produce p' -l csv -d 'Read RFC 4180 CSV (--format csv)'
complete -c kite -n '__fish_seen_subcommand_from produce p' -l key -d 'Use CSV column as record key' -x

# consume only.
complete -c kite -n '__fish_seen_subcommand_from consume c' -s B -l from-beginning -d 'Start at earliest offset'
complete -c kite -n '__fish_seen_subcommand_from consume c' -l offset -d 'Start at offset N' -x
complete -c kite -n '__fish_seen_subcommand_from consume c' -l partition -d 'Read one partition' -x
complete -c kite -n '__fish_seen_subcommand_from consume c' -s n -l max -d 'Stop after MAX records' -x
complete -c kite -n '__fish_seen_subcommand_from consume c' -s t -l idle -d 'Stop after idle duration' -x
complete -c kite -n '__fish_seen_subcommand_from consume c' -s f -l follow -d 'Never stop on idle'
