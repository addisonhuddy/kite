#!/usr/bin/env python3
"""Consumer-group checks against the broker started by e2e-docker.sh."""
import json
import os
from pathlib import Path
import pty
import signal
import subprocess
import sys
import time

kite, directory = sys.argv[1:]
root = Path(directory)
workers = []


def run(*args, data=None):
    result = subprocess.run([kite, *args], input=data, text=True,
                            capture_output=True, timeout=90)
    assert result.returncode == 0, (args, result.returncode, result.stderr)
    return result.stdout


def rows(text):
    return [json.loads(line) for line in text.splitlines() if line]


def terminal(*args):
    master, slave = pty.openpty()
    try:
        result = subprocess.run([kite, *args], stdout=slave, stderr=subprocess.PIPE,
                                text=True, timeout=90)
        os.close(slave)
        slave = None
        output = b""
        while True:
            try:
                chunk = os.read(master, 4096)
            except OSError:
                break
            if not chunk:
                break
            output += chunk
        assert result.returncode == 0, (args, result.stderr)
        return output.decode().replace("\r\n", "\n")
    finally:
        os.close(master)
        if slave is not None:
            os.close(slave)


def aligned_table(output, headers):
    lines = output.splitlines()
    assert len(lines) > 1, output
    assert lines[0].split() == headers, output
    assert "\t" not in output, output
    starts = [lines[0].index(header) for header in headers]
    for line in lines[1:]:
        cells = [line[start:starts[i + 1]].strip() if i + 1 < len(starts) else line[start:].strip()
                 for i, start in enumerate(starts)]
        assert cells == line.split(), output
        assert all(line[start] != " " for start in starts), output


def consume(group, topic, *args):
    return rows(run("consume", "-g", group, "--json", *args, topic))


def produce(topic, start, end):
    run("produce", topic, data="".join(f"key-{i}\tvalue-{i}\n" for i in range(start, end)))


def wait_for(predicate, label, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        for process, _, errors in workers:
            assert process.poll() is None, (label, "worker exited", process.returncode, errors.read_text())
        time.sleep(0.2)
    raise AssertionError(f"timed out: {label}")


def worker(topic, name):
    path = root / f"group-{name}.jsonl"
    err = root / f"group-{name}.err"
    with path.open("w") as out, err.open("w") as errors:
        process = subprocess.Popen([kite, "consume", "-g", "kite-scale", "-B", "-f",
                                    "-v", "--json", topic], stdout=out, stderr=errors)
    item = (process, path, err)
    workers.append(item)
    return item


def read_worker(item):
    text = item[1].read_text()
    return rows(text[:text.rfind("\n") + 1])


def stop(item):
    workers.remove(item)
    item[0].send_signal(signal.SIGINT)
    assert item[0].wait(timeout=40) == 130, item[2].read_text()


try:
    topic = "kite-group-resume"
    run("topic", "create", "-p", "1", topic)
    produce(topic, 0, 10)
    first = consume("kite-resume", topic, "-B", "-n", "4", "--idle", "10s")
    assert [r["offset"] for r in first] == list(range(4)), first
    second = consume("kite-resume", topic, "-B", "--idle", "2s")
    assert [r["offset"] for r in second] == list(range(4, 10)), second
    assert consume("kite-resume", topic, "--idle", "2s") == []
    assert consume("kite-latest", topic, "--idle", "2s") == []
    assert consume("kite-earliest", topic, "-B", "-n", "1", "--idle", "10s")[0]["offset"] == 0
    print("PASS groups resume/max and earliest/latest without commits", flush=True)

    produce(topic, 10, 13)
    description = rows(run("group", "describe", "kite-resume", "--json"))
    assert len(description) == 1, description
    assert (description[0]["committed"], description[0]["end"], description[0]["lag"]) == (10, 13, 3), description
    assert description[0]["member_id"] == description[0]["client_id"] == "", description
    assert any(g["group"] == "kite-resume" for g in rows(run("group", "list", "--json")))
    print("PASS groups list/describe lag and leave", flush=True)

    description_table = terminal("group", "describe", "kite-resume")
    aligned_table(description_table, ["GROUP", "TOPIC", "PARTITION", "COMMITTED", "END", "LAG", "MEMBER_ID", "CLIENT_ID"])
    listing_table = terminal("group", "list")
    aligned_table(listing_table, ["GROUP", "STATE"])
    (root / "group-describe-terminal.txt").write_text(description_table)
    (root / "group-list-terminal.txt").write_text(listing_table)
    assert run("group", "describe", "kite-resume").splitlines() == [f"kite-resume\t{topic}\t0\t10\t13\t3\t-\t-"]
    assert any(line.split("\t") == ["kite-resume", "Empty"] for line in run("group", "list").splitlines())
    print("PASS groups terminal columns and piped TSV", flush=True)

    scale = "kite-group-scale"
    run("topic", "create", "-p", "6", scale)
    a, b = worker(scale, "a"), worker(scale, "b")

    def both_assigned():
        result = subprocess.run([kite, "group", "describe", "kite-scale", "--json"],
                                text=True, capture_output=True, timeout=90)
        if result.returncode and "GROUP_ID_NOT_FOUND" in result.stderr:
            return False
        assert result.returncode == 0, result.stderr
        assignment = rows(result.stdout)
        return len(assignment) == 6 and len({r["member_id"] for r in assignment if r["member_id"]}) == 2

    wait_for(both_assigned, "two members assigned")
    produce(scale, 0, 600)
    wait_for(lambda: len(read_worker(a)) + len(read_worker(b)) >= 600, "initial 600 records")
    initial_a, initial_b = read_worker(a), read_worker(b)
    assert initial_a and initial_b, (len(initial_a), len(initial_b))
    assert {r["partition"] for r in initial_a}.isdisjoint({r["partition"] for r in initial_b})
    wait_for(lambda: all(r["lag"] == 0 for r in rows(run("group", "describe", "kite-scale", "--json"))), "initial commits")
    stop(a)
    produce(scale, 600, 1200)
    wait_for(lambda: len(read_worker(a)) + len(read_worker(b)) >= 1200, "SIGINT handover")
    stop(b)
    all_rows = read_worker(a) + read_worker(b)
    assert len(all_rows) == 1200, len(all_rows)
    assert {r["value"] for r in all_rows} == {f"value-{i}" for i in range(1200)}
    description = rows(run("group", "describe", "kite-scale", "--json"))
    assert len(description) == 6 and all(r["lag"] == 0 and not r["member_id"] for r in description), description
    print("PASS groups two workers disjoint/full coverage and SIGINT handover", flush=True)

    broken = "kite-group-broken"
    run("topic", "create", "-p", "1", broken)
    run("produce", broken, data="".join(f"{i}:" + "x" * 8192 + "\n" for i in range(64)))
    with (root / "group-broken.err").open("w") as errors:
        process = subprocess.Popen([kite, "consume", "-g", "kite-broken", "-B", "--idle", "3s", broken],
                                   stdout=subprocess.PIPE, stderr=errors)
        head = subprocess.Popen(["head", "-1"], stdin=process.stdout, stdout=subprocess.DEVNULL)
        process.stdout.close()
        assert head.wait(timeout=30) == 0
        assert process.wait(timeout=40) == 0
    replay = consume("kite-broken", broken, "-B", "--idle", "2s")
    assert [r["offset"] for r in replay] == list(range(64)), [r["offset"] for r in replay]
    print("PASS groups broken pipe replays uncommitted round without skips", flush=True)

    transactional = "kite-group-transactional"
    run("topic", "create", "-p", "1", transactional)
    result = subprocess.run([
        "docker", "exec", "kite-e2e-kafka", "/opt/kafka/bin/kafka-producer-perf-test.sh",
        "--topic", transactional, "--num-records", "20", "--record-size", "16",
        "--throughput", "-1", "--transactional-id", "kite-txn-writer",
        "--transaction-duration-ms", "100", "--producer-props", "bootstrap.servers=localhost:9092",
    ], text=True, capture_output=True, timeout=90)
    assert result.returncode == 0, result.stderr
    records = consume("kite-txn", transactional, "-B", "--idle", "10s")
    assert len(records) == 20, len(records)
    description = rows(run("group", "describe", "kite-txn", "--json"))
    assert len(description) == 1 and description[0]["lag"] == 0, description
    assert description[0]["end"] > 20, description
    print("PASS groups transactional control markers commit fetch position and lag zero", flush=True)

    result = subprocess.run([
        "docker", "exec", "kite-e2e-kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "localhost:9092", "--topic", topic,
        "--group", "kite-classic", "--from-beginning", "--max-messages", "13",
        "--consumer-property", "group.protocol=classic",
    ], text=True, capture_output=True, timeout=90)
    assert result.returncode == 0, result.stderr
    description = rows(run("group", "describe", "kite-classic", "--json"))
    assert len(description) == 1 and description[0]["lag"] == 0, description
    assert not description[0]["member_id"] and not description[0]["client_id"], description
    print("PASS groups classic describe falls back to committed offsets", flush=True)
finally:
    for process, _, _ in workers:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=40)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
