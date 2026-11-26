#!/usr/bin/env python3
"""Consumer-group checks against the broker started by e2e-docker.sh."""
import json
from pathlib import Path
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


def check_transaction_isolation():
    topic = "kite-transaction-isolation"
    split_topic = "kite-transaction-split"
    run("topic", "create", "-p", "1", topic)
    run("topic", "create", "-p", "1", split_topic)
    assert run("consume", "--offset", "0", "--idle", "1s", split_topic) == ""
    empty_out_of_range = subprocess.run(
        [kite, "consume", "--offset", "1", "--idle", "1s", split_topic],
        text=True, capture_output=True, timeout=15
    )
    assert empty_out_of_range.returncode == 1 and not empty_out_of_range.stdout, empty_out_of_range
    assert (
        "partition is empty; valid offset is 0 "
        "(high watermark 0; read-committed records available up to last stable offset 0)"
    ) in empty_out_of_range.stderr, empty_out_of_range
    docker = ["docker", "exec", "kite-e2e-kafka"]
    jar_path = subprocess.check_output(
        [*docker, "sh", "-c", "echo /opt/kafka/libs/kafka-clients-*.jar"], text=True
    ).strip()
    jar = root / Path(jar_path).name
    subprocess.run(["docker", "cp", f"kite-e2e-kafka:{jar_path}", jar], check=True)
    source = Path(__file__).with_name("TransactionFixture.java")
    subprocess.run(["javac", "--release", "17", "-cp", jar, "-d", root, source], check=True)
    jar.unlink()
    subprocess.run(["docker", "cp", root / "TransactionFixture.class", "kite-e2e-kafka:/tmp/"], check=True)
    fixture_log = root / "transactions.out"
    fixture_errors = root / "transactions.err"
    latest_log = root / "transactions-latest.err"
    with fixture_log.open("w") as out, fixture_errors.open("w") as errors:
        fixture = subprocess.Popen(
            ["docker", "exec", "-i", "kite-e2e-kafka", "java",
             "-cp", "/tmp:/opt/kafka/libs/*", "TransactionFixture", topic, split_topic],
            stdin=subprocess.PIPE, stdout=out, stderr=errors, text=True
        )
    latest = None
    try:
        def ready():
            assert fixture.poll() is None, fixture_errors.read_text()
            return "READY" in fixture_log.read_text()

        wait_for(ready, "open transaction")
        committed = ["committed"]
        uncommitted = ["aborted-first", "committed", "aborted-last", "open", "after-open"]
        for flags, expected, end, boundary_suffix in [
            ((), committed, 6, "; read-committed records available up to last stable offset 6"),
            (("--read-uncommitted",), uncommitted, 8, ""),
        ]:
            output = run("consume", "-B", "--format", "value", "--idle", "1s", *flags, topic)
            assert output.splitlines() == expected, (flags, output)
            group = f"kite-isolation-{end}"
            records = consume(group, topic, "-B", "--idle", "2s", *flags)
            assert [r["value"] for r in records] == expected, (flags, records)
            description = rows(run("group", "describe", group, "--json"))
            assert len(description) == 1 and description[0]["committed"] == end, description
            assert description[0]["end"] == 8, description
            out_of_range = subprocess.run(
                [kite, "consume", "--offset", "9", "--idle", "1s", *flags, topic],
                text=True, capture_output=True, timeout=15
            )
            assert out_of_range.returncode == 1 and not out_of_range.stdout, out_of_range
            assert (
                "kite: cannot start consuming: offset 9 is out of range for partition 0: "
                f"valid offsets are 0..8 (high watermark 8{boundary_suffix})"
            ) in out_of_range.stderr.splitlines(), out_of_range
            assert run("consume", "--offset", "8", "--idle", "1s", *flags, topic) == ""

        assert run("consume", "--offset", "7", "--idle", "1s", "--format", "value", topic) == ""
        assert run("consume", "--offset", "7", "--idle", "1s", "--format", "value",
                   "--read-uncommitted", topic) == "after-open\n"
        print("PASS seek between last stable offset and high watermark, and diagnostics above it", flush=True)
        print("PASS high watermark is a valid seek on empty and nonempty partitions", flush=True)

        # With a one-byte budget Kafka returns one batch at a time for progress.
        tiny_config = root / "tiny-fetch.properties"
        tiny_config.write_text("fetch.max.bytes=1\n")
        tiny_args = ("consume", "--config", str(tiny_config), "--offset", "1", "--json", "--idle", "2s")
        tiny = subprocess.run(
            [kite, *tiny_args, "-v", "-n", "1", split_topic], text=True, capture_output=True, timeout=15
        )
        assert tiny.returncode == 0, tiny.stderr
        assert [(r["offset"], r["value"]) for r in rows(tiny.stdout)] == [(4, "committed")], tiny
        skipped_fetches = [
            line for line in tiny.stderr.splitlines()
            if "offsets empty..empty" in line and not line.endswith("records bytes 0")
        ]
        assert len(skipped_fetches) == 3, tiny.stderr  # Two aborted batches and their abort marker.
        tiny_uncommitted = rows(run(*tiny_args, "--read-uncommitted", "-n", "3", split_topic))
        assert [(r["offset"], r["value"]) for r in tiny_uncommitted] == [
            (1, "aborted-middle"), (2, "aborted-tail"), (4, "committed"),
        ], tiny_uncommitted
        print("PASS seek inside multi-batch abort across small Kafka fetches", flush=True)

        with latest_log.open("w") as errors:
            latest = subprocess.Popen(
                [kite, "consume", "-v", "--format", "value", "-n", "1", "--idle", "15s", topic],
                stdout=subprocess.PIPE, stderr=errors, text=True
            )
        wait_for(lambda: "offsets empty..empty" in latest_log.read_text(), "first fetch at latest", seconds=10)
        fixture.communicate("\n", timeout=30)
        assert fixture.returncode == 0, fixture_errors.read_text()
        output, _ = latest.communicate(timeout=20)
        assert latest.returncode == 0 and output == "open\n", (output, latest_log.read_text())
    finally:
        if latest is not None and latest.poll() is None:
            latest.kill()
            latest.wait()
        if fixture.poll() is None:
            fixture.kill()
            fixture.wait()
    print("PASS Kafka transaction isolation, stable latest and group commits", flush=True)


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

    check_transaction_isolation()

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
