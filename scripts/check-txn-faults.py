#!/usr/bin/env python3
"""Fault-injection tests for transactional produce against the e2e-docker.sh broker."""
import os
import signal
import socket
import struct
import subprocess
import sys
import threading
import time

kite, container = sys.argv[1:]
BROKER_HOST = "127.0.0.1"
BROKER_PORT = 9092
API_METADATA = 3
API_FIND_COORDINATOR = 10
API_ADD_PARTITIONS_TO_TXN = 24
API_END_TXN = 26


def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def read_frame(sock):
    hdr = recv_exact(sock, 4)
    if hdr is None:
        return None
    (size,) = struct.unpack(">I", hdr)
    body = recv_exact(sock, size)
    if body is None:
        return None
    return body


def write_frame(sock, body):
    sock.sendall(struct.pack(">I", len(body)) + body)


class KafkaFaultProxy:
    def __init__(self, drop_api, on_drop=None):
        self.drop_api = drop_api
        self.on_drop = on_drop
        self.drops = 0
        self.stop_evt = threading.Event()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((BROKER_HOST, 0))
        self.port = self.sock.getsockname()[1]
        self.sock.listen(16)
        self.sock.settimeout(0.2)
        self.thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.thread.start()

    def _accept_loop(self):
        while not self.stop_evt.is_set():
            try:
                client, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle, args=(client,), daemon=True).start()

    def _handle(self, client):
        upstream = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15)
        try:
            while not self.stop_evt.is_set():
                req = read_frame(client)
                if req is None:
                    break
                api_key, _api_ver, _corr_id = struct.unpack(">hhi", req[:8])
                write_frame(upstream, req)
                resp = read_frame(upstream)
                if resp is None:
                    break
                if api_key == self.drop_api:
                    self.drops += 1
                    if self.on_drop is not None:
                        self.on_drop(self.drops)
                    break  # Drop reply by closing client socket after broker processed it.
                if api_key in (API_METADATA, API_FIND_COORDINATOR):
                    resp = resp[:4] + resp[4:].replace(
                        struct.pack(">I", BROKER_PORT),
                        struct.pack(">I", self.port),
                    )
                write_frame(client, resp)
        except OSError:
            pass
        finally:
            for s in (client, upstream):
                try:
                    s.close()
                except OSError:
                    pass

    def close(self):
        self.stop_evt.set()
        try:
            self.sock.close()
        except OSError:
            pass
        self.thread.join(timeout=2)


def run_kite(*args, data=None, check=True):
    res = subprocess.run([kite, *args], input=data, text=True, capture_output=True, timeout=90)
    if check:
        assert res.returncode == 0, (args, res.returncode, res.stderr)
    return res


def describe_txn(txn_id):
    res = subprocess.run(
        [
            "docker", "exec", container,
            "/opt/kafka/bin/kafka-transactions.sh",
            "--bootstrap-server", "localhost:9092",
            "describe", "--transactional-id", txn_id,
        ],
        text=True, capture_output=True, timeout=30,
    )
    assert res.returncode == 0, (txn_id, res.returncode, res.stderr)
    return res.stdout


# Fault 1 (P1): EndTxn(commit) reply is lost after Kafka commits, then SIGINT arrives.
# kite must not switch to EndTxn(abort) or claim "transaction aborted".
t1 = "kite-txn-fault-commit-lost"
txn1 = "kite-e2e-fault-commit-lost"
run_kite("topic", "delete", "-y", "--if-exists", t1, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t1)

proc_holder = []


def send_sigint_once(drop_count):
    if drop_count == 1 and proc_holder:
        os.kill(proc_holder[0].pid, signal.SIGINT)


proxy1 = KafkaFaultProxy(API_END_TXN, on_drop=send_sigint_once)
try:
    p = subprocess.Popen(
        [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy1.port}", "--txn", txn1, t1],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    proc_holder.append(p)
    _, err = p.communicate("k1\tcommitted-despite-lost-reply\n", timeout=30)
    assert proxy1.drops >= 1, "expected proxy to drop EndTxn reply"
    assert p.returncode == 130, (p.returncode, err)
    assert "outcome unknown" in err, f"expected unknown-outcome message, got: {err!r}"
    assert "transaction aborted" not in err, f"must not claim aborted after commit sent: {err!r}"
finally:
    proxy1.close()

out1 = run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t1).stdout.strip()
assert out1 == "committed-despite-lost-reply", f"expected committed record under read_committed, got: {out1!r}"
print("PASS txn-fault: lost EndTxn(commit) reply + SIGINT reports unknown outcome")


# Fault 2 (P2): Every AddPartitionsToTxn reply is lost after Kafka registers the partition.
# kite must still send EndTxn(abort) on cleanup so the txn does not stay Ongoing.
t2 = "kite-txn-fault-addparts-lost"
txn2 = "kite-e2e-fault-addparts-lost"
run_kite("topic", "delete", "-y", "--if-exists", t2, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t2)

proxy2 = KafkaFaultProxy(API_ADD_PARTITIONS_TO_TXN)
try:
    res = run_kite(
        "produce", "--bootstrap", f"{BROKER_HOST}:{proxy2.port}", "--txn", txn2, t2,
        data="k1\tnever-produced\n", check=False,
    )
    assert proxy2.drops >= 1, "expected proxy to drop AddPartitionsToTxn reply"
    assert res.returncode == 1, (res.returncode, res.stderr)
finally:
    proxy2.close()

desc2 = describe_txn(txn2)
assert "CompleteAbort" in desc2, f"expected CompleteAbort after lost AddPartitionsToTxn reply, got:\n{desc2}"

# Produce a committed sentinel to the same partition and verify read_committed sees it immediately.
run_kite("produce", "--txn", "kite-e2e-fault-sentinel", t2, data="k1\tsentinel-after-abort\n")
out2 = run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t2).stdout.strip()
assert out2 == "sentinel-after-abort", f"expected sentinel under read_committed, got: {out2!r}"
print("PASS txn-fault: lost AddPartitionsToTxn reply aborts cleanly and unblocks LSO")

run_kite("topic", "delete", "-y", t1, t2, check=False)
