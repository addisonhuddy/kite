# fish completion for kite — install with
#   mkdir -p ~/.config/fish/completions && cp completions/kite.fish ~/.config/fish/completions/

# First word: the commands (plus the p/c aliases).
complete -c kite -n 'test (count (commandline -opc)[2..-1]) -eq 0' -f -a 'produce p consume c cluster topic'

# Cluster names from kite.yaml for @NAME positionals.
complete -c kite -f -a '(begin; test -f kite.yaml; and awk "/^clusters:/{f=1;next} f&&/^[^ ]/{f=0} f&&/^  [A-Za-z0-9_-]+:/{sub(/^  /,\"@\");sub(/:.*/,\"\");print}" kite.yaml | sort -u; end)'

# Options valid for every command.
complete -c kite -n '__fish_seen_subcommand_from produce p consume c cluster topic' -s q -l quiet -d 'Suppress summary and progress lines'
complete -c kite -n '__fish_seen_subcommand_from produce p consume c cluster topic' -s v -l verbose -d 'Diagnostics on stderr'
complete -c kite -n '__fish_seen_subcommand_from produce p consume c cluster topic' -s h -l help -d 'Show help'

# produce and consume.
complete -c kite -n '__fish_seen_subcommand_from produce p consume c topic' -s b -l bootstrap -d 'Comma-separated host:port brokers' -x
complete -c kite -n '__fish_seen_subcommand_from produce p consume c cluster topic' -l config -d 'Read this properties file' -rF
complete -c kite -n '__fish_seen_subcommand_from produce p consume c' -l format -d 'Record shape' -xa 'value tsv json csv'
complete -c kite -n '__fish_seen_subcommand_from produce p consume c' -l json -d 'One JSON object per record (--format json)'

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

# cluster.
complete -c kite -n '__fish_seen_subcommand_from cluster; and test (count (commandline -opc)[2..-1]) -eq 0' -f -a 'list set init'
complete -c kite -n '__fish_seen_subcommand_from cluster' -l format -d 'Output shape' -xa 'json'
complete -c kite -n '__fish_seen_subcommand_from cluster' -l json -d 'JSON output'
# kite cluster set NAME completes cluster names from kite.yaml.
complete -c kite -n '__fish_seen_subcommand_from cluster; and __fish_seen_subcommand_from set' -f -a '(begin; test -f kite.yaml; and awk "/^clusters:/{f=1;next} f&&/^[^ ]/{f=0} f&&/^  [A-Za-z0-9_-]+:/{sub(/^  /,\"\");sub(/:.*/,\"\");print}" kite.yaml | sort -u; end)'

# topic.
complete -c kite -n '__fish_seen_subcommand_from topic; and test (count (commandline -opc)[2..-1]) -eq 0' -f -a 'list create delete update'
complete -c kite -n '__fish_seen_subcommand_from topic; and __fish_seen_subcommand_from list' -s a -l all -d 'Include internal topics'
complete -c kite -n '__fish_seen_subcommand_from topic; and __fish_seen_subcommand_from list' -l json -d 'One JSON object per topic'
complete -c kite -n '__fish_seen_subcommand_from topic; and __fish_seen_subcommand_from create update' -s p -l partitions -d 'Partition count' -x
complete -c kite -n '__fish_seen_subcommand_from topic; and __fish_seen_subcommand_from create' -s r -l replication-factor -d 'Replication factor' -x
complete -c kite -n '__fish_seen_subcommand_from topic; and __fish_seen_subcommand_from create update' -s s -l set -d 'Set a topic config KEY=VALUE' -x
complete -c kite -n '__fish_seen_subcommand_from topic; and __fish_seen_subcommand_from update' -l unset -d 'Remove a topic config' -x
complete -c kite -n '__fish_seen_subcommand_from topic; and __fish_seen_subcommand_from delete' -s y -l yes -d 'Delete without confirming'
complete -c kite -n '__fish_seen_subcommand_from topic; and __fish_seen_subcommand_from delete' -l if-exists -d 'A missing topic is not an error'
complete -c kite -n '__fish_seen_subcommand_from topic; and __fish_seen_subcommand_from create' -l if-not-exists -d 'An existing topic is not an error'
