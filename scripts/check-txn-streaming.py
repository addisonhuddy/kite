#!/usr/bin/env python3
"""Exercise commit and abort after a mid-stream drain on the Docker broker."""
import resource
import signal
import subprocess
import sys
import tempfile
import time

kite = sys.argv[1]
count = 5000
tail = b"x" * (32768 - 5)
sentinel = b"sentinel\n"


def value(i):
    return f"{i:05d}".encode() + tail


def cap_memory():
    limit = 128 * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


def consume(topic, *args, check=True):
    return subprocess.run(
        [kite, "consume", "--format", "value", "--idle", "2s", *args, topic],
        capture_output=True, timeout=20, check=check,
    )


def last_record_visible(topic):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        result = consume(topic, "--read-uncommitted", "--offset", str(count - 1), "-n", "1", check=False)
        if result.returncode == 0 and result.stdout == value(count - 1) + b"\n":
            return
        time.sleep(0.05)
    raise AssertionError(f"{topic}: last uncommitted record not visible: {result.stderr!r}")


def check(mode, expected_rc, diagnostic):
    topic = f"kite-txn-stream-{mode}"
    subprocess.run([kite, "topic", "create", "-p", "1", "-r", "1", topic], check=True, timeout=30)
    with tempfile.TemporaryFile() as log, subprocess.Popen(
        [kite, "produce", "-v", "--json", "--txn", topic, topic],
        stdin=subprocess.PIPE, stderr=log, preexec_fn=cap_memory,
    ) as proc:
        try:
            for i in range(count):
                proc.stdin.write(b'{"value":"' + value(i) + b'"}\n')
            proc.stdin.flush()
            last_record_visible(topic)
            assert proc.poll() is None, f"{mode}: producer exited before the transaction ended"
            # The producer is still reading stdin, not draining at EOF.
            if mode == "invalid":
                proc.stdin.write(b"not-valid-json\n")
            elif mode == "sigint":
                proc.send_signal(signal.SIGINT)
            else:
                proc.stdin.write(b'{"value":"sentinel"}\n')
            if mode != "sigint":
                proc.stdin.close()
            rc = proc.wait(timeout=30)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            log.seek(0)
            stderr = log.read()
        assert rc == expected_rc, (mode, rc, stderr)
        assert b"draining for a slot" in stderr, (mode, stderr)
        assert diagnostic in stderr, (mode, stderr)

    if mode == "commit":
        with tempfile.TemporaryFile() as records:
            subprocess.run(
                [kite, "consume", "-B", "--idle", "2s", "--format", "value", topic],
                stdout=records, check=True, timeout=60,
            )
            records.seek(0)
            for i in range(count):
                assert records.readline() == value(i) + b"\n", i
            assert records.read() == sentinel
    else:
        subprocess.run([kite, "produce", topic], input=sentinel, check=True, timeout=20)
        assert consume(topic, "-B").stdout == sentinel, f"{mode}: abort leaked records or blocked the sentinel"
    print(f"PASS txn-stream: {mode} after mid-stream drain under 128 MiB", flush=True)


check("commit", 0, b"5001 record(s) produced")
check("invalid", 1, b"line 5001:")
check("sigint", 130, b"transaction aborted (interrupted)")
