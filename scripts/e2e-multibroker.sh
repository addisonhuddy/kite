#!/usr/bin/env bash
# Three-node KRaft fault tests. Needs docker, python3, timeout and zig.
# KITE_KAFKA_IMAGE defaults to apache/kafka:4.0.0 (GCR mirror fallback).
# KITE_E2E_KEEP=1 retains containers; logs always survive in KITE_E2E_LOG_DIR.
set -euo pipefail
cd "$(dirname "$0")/.."
IMAGE=${KITE_KAFKA_IMAGE:-apache/kafka:4.0.0}
NAME=kite-multibroker
TIMEOUT=${KITE_E2E_TIMEOUT:-120}
LOG=${KITE_E2E_LOG_DIR:-$HOME/kite-multibroker-$(date +%s)}
mkdir -p "$LOG"
LOG=$(realpath "$LOG")
KITE=$PWD/zig-out/bin/kite
ALL=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
export BOOTSTRAP_SERVERS=$ALL
status=0
cleanup() {
    for i in 1 2 3; do docker logs "$NAME-$i" >"$LOG/broker-$i.log" 2>&1 || true; done
    echo "logs: $LOG"
    if [ "${KITE_E2E_KEEP:-0}" = 1 ]; then echo "keeping $NAME-{1,2,3} and network $NAME"; return; fi
    for i in 1 2 3; do docker rm -f "$NAME-$i" >/dev/null 2>&1 || true; done
    docker network rm "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT
fail() { echo "FAIL $1"; status=1; }
run() {
    local label=$1
    shift
    # A separate errexit shell is important: conditionals disable errexit in functions.
    export LOG KITE NAME TIMEOUT ALL
    export -f $(declare -F | awk '{print $3}')
    if bash -euo pipefail -c '"$@"' -- "$@" >"$LOG/$label.log" 2>&1; then
        echo "PASS $label"
    else
        fail "$label"
        cat "$LOG/$label.log"
        find "$LOG" -name "$label*.err" -exec sh -c 'echo "== $1 =="; cat "$1"' sh {} \;
    fi
}
cli() {
    local command=$1
    shift
    if [ "$command" = topic ]; then
        local action=$1
        shift
        timeout 150s "$KITE" topic "$action" -v "$@"
    else
        timeout 150s "$KITE" "$command" -v "$@"
    fi
}
admin() { docker exec "$NAME-${ADMIN_NODE:-1}" "/opt/kafka/bin/$1" "${@:2}"; }
ready() {
    local deadline=$((SECONDS + TIMEOUT))
    until admin kafka-topics.sh --bootstrap-server "$NAME-1:9092,$NAME-2:9092,$NAME-3:9092" --list >/dev/null 2>&1; do
        [ "$SECONDS" -lt "$deadline" ] || return 1
        sleep 2
    done
}
restore() {
    for i in 1 2 3; do docker start "$NAME-$i" >/dev/null; done
    ADMIN_NODE=1 ready
    # Let replicas rejoin ISR before the next fault.
    sleep 8
}
create() { cli topic create -p 6 -r 3 "$1"; }
fixture() {
    python3 - "$1" "${2:-0}" "${3:-0}" <<'PY'
import sys,time
n,delay,start=int(sys.argv[1]),float(sys.argv[2]),int(sys.argv[3])
for i in range(start,start+n):
    print(f'key-{i}\tvalue-{i}')
    if i % 1000 == 999:
        sys.stdout.flush()
        if delay: time.sleep(delay)
PY
}
verify() {
    python3 - "$1" "$2" <<'PY'
import json,sys
rows=[json.loads(s) for s in open(sys.argv[1])]
n=int(sys.argv[2]); values=[r['value'] for r in rows]
assert len(rows)==n, f'count {len(rows)} != {n}'
assert set(values)=={f'value-{i}' for i in range(n)}, 'missing or duplicate values'
print(f'verified {n} records, no duplicates')
PY
}
dump() {
    cli consume -B --idle 15s --json "$1" >"$LOG/$2.jsonl" 2>"$LOG/$2-consume.err"
    verify "$LOG/$2.jsonl" "$3"
}
leader() {
    admin kafka-topics.sh --bootstrap-server "$NAME-1:9092" --describe --topic "$1" >"$LOG/$1-metadata.txt"
    # Pick the broker leading the most partitions.
    awk '{for(i=1;i<=NF;i++) if($i=="Leader:") print $(i+1)}' "$LOG/$1-metadata.txt" | sort | uniq -c | sort -rn | head -1 | awk '{print $2}'
}
wait_records() {
    local file=$1 pid=$2 deadline=$((SECONDS + 40))
    until [ -f "$file" ] && [ "$(wc -l <"$file")" -ge 1000 ]; do
        kill -0 "$pid" 2>/dev/null || return 1
        [ "$SECONDS" -lt "$deadline" ] || return 1
        sleep 0.2
    done
}
echo "== pull $IMAGE =="
if ! docker pull "$IMAGE" >"$LOG/pull.log" 2>&1; then
    if [ "$IMAGE" = apache/kafka:4.0.0 ]; then
        IMAGE=mirror.gcr.io/apache/kafka:4.0.0
        docker pull "$IMAGE" >>"$LOG/pull.log" 2>&1 || { fail image-pull; exit 1; }
    else fail image-pull; exit 1; fi
fi
[ -x "$KITE" ] || zig build
docker network create "$NAME" >/dev/null
for i in 1 2 3; do
    docker run -d --name "$NAME-$i" --network "$NAME" -p "127.0.0.1:$((19091+i)):9094" \
        -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
        -e KAFKA_NODE_ID="$i" -e KAFKA_PROCESS_ROLES=broker,controller \
        -e KAFKA_LISTENERS=INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:9094 \
        -e "KAFKA_ADVERTISED_LISTENERS=INTERNAL://$NAME-$i:9092,EXTERNAL://127.0.0.1:$((19091+i))" \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT \
        -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e "KAFKA_CONTROLLER_QUORUM_VOTERS=1@$NAME-1:9093,2@$NAME-2:9093,3@$NAME-3:9093" \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 -e KAFKA_MIN_INSYNC_REPLICAS=2 \
        -e KAFKA_AUTO_LEADER_REBALANCE_ENABLE=false "$IMAGE" >/dev/null
done
ready || { fail readiness; exit 1; }
sleep 5

baseline() {
    create mb-baseline 2>"$LOG/01-routing-create.err"
    fixture 20000 | cli produce mb-baseline 2>"$LOG/01-routing-produce.err"
    dump mb-baseline 01-routing 20000
    for i in 1 2 3; do
        grep -q "connected broker $i at" "$LOG/01-routing-consume.err"
        grep -q "connected broker $i at" "$LOG/01-routing-produce.err"
    done
}
run 01-routing baseline

# Read controller_id from the same broker Metadata API used by kite. v8 has
# a non-flexible prefix, so no Kafka client dependency is needed here.
controller=$(python3 - <<'PY'
import socket,struct
s=socket.create_connection(('127.0.0.1',19092))
body=struct.pack('>hhih',3,8,1,0)+struct.pack('>i???',-1,True,False,False)
s.sendall(struct.pack('>i',len(body))+body)
def read(n):
    b=b''
    while len(b)<n:
        chunk=s.recv(n-len(b))
        if not chunk: raise EOFError()
        b+=chunk
    return b
b=read(struct.unpack('>i',read(4))[0]); p=8
def integer():
    global p
    v=struct.unpack_from('>i',b,p)[0]; p+=4; return v
def string():
    global p
    n=struct.unpack_from('>h',b,p)[0]; p+=2
    if n>=0: p+=n
for _ in range(integer()):
    integer(); string(); integer(); string()
string()
print(integer())
PY
) || controller=unknown
echo "metadata controller_id=$controller" | tee "$LOG/controller.txt"
export controller
admin_ops() {
    local i=$1 t=mb-admin-$1
    export BOOTSTRAP_SERVERS=127.0.0.1:$((19091+i))
    [ "$controller" != unknown ]
    cli topic create -p 6 -r 3 "$t"
    cli topic update --partitions 9 "$t"
    cli topic update --set retention.ms=7200000 "$t"
    admin kafka-topics.sh --bootstrap-server "$NAME-1:9092" --describe --topic "$t" | grep 'PartitionCount: 9'
    admin kafka-configs.sh --bootstrap-server "$NAME-1:9092" --entity-type topics --entity-name "$t" --describe | grep 'retention.ms=7200000'
    cli topic delete -y "$t"
    local deadline=$((SECONDS + 30))
    while cli topic list | grep -x "$t"; do
        [ "$SECONDS" -lt "$deadline" ] || return 1
        sleep 1
    done
}
for i in 1 2 3; do run "02-admin-$i" admin_ops "$i"; done

produce_fault() {
    local label=$1 mode=$2 t=mb-$1 victim pid rc=0
    export BOOTSTRAP_SERVERS=$ALL
    create "$t" 2>"$LOG/$label-create.err"
    victim=$(leader "$t")
    [ -n "$victim" ]
    if [ "$mode" = single ]; then export BOOTSTRAP_SERVERS=127.0.0.1:$((19091+victim)); fi
    (fixture 200000 0.1 | cli produce "$t" 2>"$LOG/$label-produce.err") & pid=$!
    # Wait for actual acknowledged traffic, not just a live input pipe.
    timeout 30s "$KITE" consume -v -B -n 1000 --idle 25s --json "$t" >"$LOG/$label-progress.jsonl" 2>"$LOG/$label-progress.err"
    [ "$(wc -l <"$LOG/$label-progress.jsonl")" -eq 1000 ]
    kill -0 "$pid"
    echo "stopping leader $victim during produce pid=$pid"
    docker stop -t 1 "$NAME-$victim"
    wait "$pid" || rc=$?
    export BOOTSTRAP_SERVERS=$ALL
    ADMIN_NODE=$((victim % 3 + 1)) dump "$t" "$label" 200000 || rc=1
    [ "$rc" -eq 0 ]
}
run 03-produce-election produce_fault 03-produce-election all
restore

consume_fault() {
    local label=04-consume-election t=mb-consume victim consumer rc=0
    create "$t" 2>"$LOG/$label-create.err"
    fixture 200000 | cli produce "$t" 2>"$LOG/$label-produce.err"
    victim=$(leader "$t")
    # Backpressure keeps the consumer active until the fault is injected.
    (cli consume -B -n 200000 --idle 30s --json "$t" 2>"$LOG/$label-consume.err" |
        python3 -c 'import sys,time
with open(sys.argv[1], "w") as out:
    for i,line in enumerate(sys.stdin,1):
        out.write(line)
        if i % 1000 == 0:
            out.flush()
            time.sleep(0.02)' "$LOG/$label.jsonl") & consumer=$!
    wait_records "$LOG/$label.jsonl" "$consumer"
    kill -0 "$consumer"
    docker stop -t 1 "$NAME-$victim"
    wait "$consumer" || rc=$?
    verify "$LOG/$label.jsonl" 200000 || rc=1
    [ "$rc" -eq 0 ]
}
run 04-consume-election consume_fault
restore
run 05-single-bootstrap-down produce_fault 05-single-bootstrap-down single
restore
bootstrap_down() {
    create mb-bootstrap-down 2>"$LOG/05-list-bootstrap-down-create.err"
    docker stop -t 1 "$NAME-1"
    fixture 20000 | cli produce mb-bootstrap-down 2>"$LOG/05-list-bootstrap-down-produce.err"
    dump mb-bootstrap-down 05-list-bootstrap-down 20000
}
run 05-list-bootstrap-down bootstrap_down
restore

idle_election() {
    local label=06-idle-election t=mb-idle consumer rc=0 victim
    create "$t" 2>"$LOG/$label-create.err"
    # Move leaders off their preferred replicas, then restore all replicas.
    victim=$(leader "$t")
    docker stop -t 1 "$NAME-$victim"
    sleep 5
    restore
    cli consume -B -n 20000 --idle 60s --json "$t" >"$LOG/$label.jsonl" 2>"$LOG/$label-consume.err" & consumer=$!
    sleep 3
    kill -0 "$consumer"
    admin kafka-topics.sh --bootstrap-server "$NAME-1:9092" --describe --topic "$t" >"$LOG/$label-before.txt"
    admin kafka-leader-election.sh --bootstrap-server "$NAME-1:9092" --election-type preferred --all-topic-partitions
    admin kafka-topics.sh --bootstrap-server "$NAME-1:9092" --describe --topic "$t" >"$LOG/$label-after.txt"
    # Prove the election changed leaders, rather than testing a no-op.
    ! diff "$LOG/$label-before.txt" "$LOG/$label-after.txt"
    fixture 20000 | cli produce "$t" 2>"$LOG/$label-produce.err" || rc=$?
    wait "$consumer" || rc=$?
    verify "$LOG/$label.jsonl" 20000 || rc=1
    [ "$rc" -eq 0 ]
}
run 06-idle-election idle_election
exit "$status"
