# fish completion for kite — install with
#   cp completions/kite.fish ~/.config/fish/completions/

complete -c kite -s c -l consume -d 'Consume instead of produce'
complete -c kite -l show-config -d 'Show the effective configuration'
complete -c kite -s V -l version -d 'Print the version'
complete -c kite -s b -l bootstrap -d 'Comma-separated host:port brokers' -x
complete -c kite -l config -d 'Read this properties file' -rF
complete -c kite -l format -d 'Record shape' -xa 'value tsv json csv'
complete -c kite -l json -d 'One JSON object per record (--format json)'
complete -c kite -s q -l quiet -d 'Suppress summary and progress lines'
complete -c kite -s v -l verbose -d 'Diagnostics on stderr'
complete -c kite -s h -l help -d 'Show help'
complete -c kite -s H -d 'Add a name: value header' -x
complete -c kite -l csv -d 'Read RFC 4180 CSV (--format csv)'
complete -c kite -l key -d 'Use CSV column as record key' -x
complete -c kite -s B -l from-beginning -d 'Start at earliest offset'
complete -c kite -l offset -d 'Start at offset N' -x
complete -c kite -l partition -d 'Read one partition' -x
complete -c kite -s n -l max -d 'Stop after MAX records' -x
complete -c kite -s t -l idle -d 'Stop after idle duration' -x
complete -c kite -s f -l follow -d 'Never stop on idle'
