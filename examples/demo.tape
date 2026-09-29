# kite in about 17 seconds: create a topic, produce, consume, inspect, delete.
#
#   vhs examples/demo.tape        -> examples/demo.gif
#
# Needs: vhs (https://github.com/charmbracelet/vhs), kite on PATH, jq,
# and a Kafka 4.0+ broker on localhost:9092 (see README "Run Kafka locally").
# The hidden setup deletes any leftover `events` topic, so reruns are clean.

Output examples/demo.gif

Set Shell bash
Set FontSize 22
Set Width 1260
Set Height 640
Set Padding 24
Set Theme "Catppuccin Mocha"
Set TypingSpeed 25ms
Set Framerate 30

Env BOOTSTRAP_SERVERS "localhost:9092"

Hide
Type "kite topic delete -y -q --if-exists events; clear" Enter
Sleep 1s
Show

# 1. topic create: partitions and configs from flags
Type "kite topic create -p 3 events"
Sleep 300ms
Enter
Sleep 1s

# 2. produce: stdin in, one line per record
Type "printf 'hello\nkafka\n' | kite produce events"
Sleep 300ms
Enter
Sleep 1s

# 3. consume: stdout out, stops as soon as the records arrive
Type "kite consume -q -B -n 2 --json events | jq -c '{partition,value}'"
Sleep 300ms
Enter
Sleep 1.3s

# 4. topic list: one JSON object per topic
Type "kite topic list --json | jq -c ."
Sleep 300ms
Enter
Sleep 1.3s

# 5. topic delete: -y skips the confirmation prompt
Type "kite topic delete -y events"
Sleep 300ms
Enter
Sleep 1s

# 6. the whole point: one static binary, no JVM
Type "ls -lh $(command -v kite) | awk '{print $5}'"
Sleep 300ms
Enter
Sleep 2s
