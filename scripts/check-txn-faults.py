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
API_PRODUCE = 0
API_METADATA = 3
API_FIND_COORDINATOR = 10
API_API_VERSIONS = 18
API_ADD_PARTITIONS_TO_TXN = 24
API_END_TXN = 26
ERR_NOT_COORDINATOR = 16


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


def decode_uvarint(buf, pos):
    val = 0
    shift = 0
    while True:
        b = buf[pos]
        pos += 1
        val |= (b & 0x7F) << shift
        if not (b & 0x80):
            return val, pos
        shift += 7


def parse_end_txn_committed(req):
    # Header v2: api_key(2) api_ver(2) corr_id(4) client_id(nullable i16) tag_buf(uvarint)
    pos = 8
    (cid_len,) = struct.unpack(">h", req[pos : pos + 2])
    pos += 2
    if cid_len > 0:
        pos += cid_len
    _, pos = decode_uvarint(req, pos)  # header tag buffer
    # Body (EndTxn v4): compact_string txn_id, producer_id(8), producer_epoch(2), committed(1)
    txn_len_plus1, pos = decode_uvarint(req, pos)
    pos += max(0, txn_len_plus1 - 1)
    pos += 8 + 2
    return req[pos] != 0


class KafkaFaultProxy:
    def __init__(self, handler):
        self.handler = handler
        self.lock = threading.Lock()
        self.commit_requests = 0
        self.abort_requests = 0
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
                api_key, _api_ver, corr_id = struct.unpack(">hhi", req[:8])
                if api_key == API_END_TXN:
                    is_commit = parse_end_txn_committed(req)
                    with self.lock:
                        if is_commit:
                            self.commit_requests += 1
                        else:
                            self.abort_requests += 1
                write_frame(upstream, req)
                resp = read_frame(upstream)
                if resp is None:
                    break
                if api_key in (API_METADATA, API_FIND_COORDINATOR):
                    resp = resp[:4] + resp[4:].replace(
                        struct.pack(">I", BROKER_PORT),
                        struct.pack(">I", self.port),
                    )
                action = self.handler(api_key, corr_id, req, resp)
                if action == "drop":
                    break
                if action == "send_and_close":
                    write_frame(client, resp)
                    break
                if isinstance(action, bytes):
                    write_frame(client, action)
                    continue
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


def wait_complete_abort(txn_id, timeout=15.0):
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        res = subprocess.run(
            [
                "docker", "exec", container,
                "/opt/kafka/bin/kafka-transactions.sh",
                "--bootstrap-server", "localhost:9092",
                "describe", "--transactional-id", txn_id,
            ],
            text=True, capture_output=True, timeout=30,
        )
        last = res.stdout + res.stderr
        if res.returncode == 0 and "CompleteAbort" in res.stdout:
            return
        time.sleep(0.2)
    raise AssertionError(f"{txn_id} did not reach CompleteAbort within {timeout}s:\n{last}")


# Fault 1 (R1 P1): EndTxn(commit) reply is lost after Kafka commits, then SIGINT arrives.
t1 = "kite-txn-fault-commit-lost"
txn1 = "kite-e2e-fault-commit-lost"
run_kite("topic", "delete", "-y", "--if-exists", t1, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t1)

proc1 = []
dropped1 = [0]


def fault1(api_key, corr_id, req, resp):
    if api_key == API_END_TXN and parse_end_txn_committed(req):
        dropped1[0] += 1
        if dropped1[0] == 1 and proc1:
            os.kill(proc1[0].pid, signal.SIGINT)
        return "drop"
    return None


proxy1 = KafkaFaultProxy(fault1)
try:
    p = subprocess.Popen(
        [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy1.port}", "--txn", txn1, t1],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    proc1.append(p)
    _, err = p.communicate("k1\tcommitted-despite-lost-reply\n", timeout=30)
    assert dropped1[0] >= 1, "expected proxy to drop EndTxn reply"
    assert p.returncode == 130, (p.returncode, err)
    assert "outcome unknown" in err, f"expected unknown-outcome message, got: {err!r}"
    assert "transaction aborted" not in err, f"must not claim aborted: {err!r}"
    assert proxy1.abort_requests == 0, f"must not send abort after commit dispatched: {proxy1.abort_requests}"
finally:
    proxy1.close()

out1 = run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t1).stdout.strip()
assert out1 == "committed-despite-lost-reply", f"expected committed record under read_committed, got: {out1!r}"
print("PASS txn-fault: lost EndTxn(commit) reply + SIGINT reports unknown outcome without aborting")


# Fault 2 (R2 P1): NOT_COORDINATOR returned after Kafka committed, then SIGINT arrives.
t2 = "kite-txn-fault-not-coord"
txn2 = "kite-e2e-fault-not-coord"
run_kite("topic", "delete", "-y", "--if-exists", t2, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t2)

proc2 = []
forged2 = [0]


def fault2(api_key, corr_id, req, resp):
    if api_key == API_END_TXN and parse_end_txn_committed(req):
        forged2[0] += 1
        if forged2[0] == 1 and proc2:
            os.kill(proc2[0].pid, signal.SIGINT)
            # EndTxn v4 response header v1 + throttle_time_ms=0 + NOT_COORDINATOR(16) + tag_buf=0
            return struct.pack(">iBihB", corr_id, 0, 0, ERR_NOT_COORDINATOR, 0)
    return None


proxy2 = KafkaFaultProxy(fault2)
try:
    p = subprocess.Popen(
        [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy2.port}", "--txn", txn2, t2],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    proc2.append(p)
    _, err = p.communicate("k1\tcommitted-despite-not-coord\n", timeout=30)
    assert forged2[0] >= 1, "expected proxy to forge NOT_COORDINATOR"
    assert p.returncode == 130, (p.returncode, err)
    assert "outcome unknown" in err, f"expected unknown-outcome message, got: {err!r}"
    assert "transaction aborted" not in err, f"must not claim aborted: {err!r}"
    assert proxy2.abort_requests == 0, f"must not send abort after NOT_COORDINATOR on commit: {proxy2.abort_requests}"
finally:
    proxy2.close()

out2 = run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t2).stdout.strip()
assert out2 == "committed-despite-not-coord", f"expected committed record under read_committed, got: {out2!r}"
print("PASS txn-fault: NOT_COORDINATOR after commit + SIGINT never sends abort")


# Fault 3 (R2 P1): SIGINT arrives during coordinator reconnect ApiVersions before retry commit is dispatched.
# Reject the first EndTxn(commit) with NOT_COORDINATOR without forwarding it to Kafka,
# then send SIGINT while the reconnect's ApiVersions handshake is in flight.
# kite must check stop right after connFor() and exit 130 without sending a second commit.
t3 = "kite-txn-fault-sigint-reconnect"
txn3 = "kite-e2e-fault-sigint-reconnect"
run_kite("topic", "delete", "-y", "--if-exists", t3, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t3)

proc3 = []
state3 = {"rejected": False, "signaled": False, "forwarded_commits": 0}


class RejectFirstCommitProxy(KafkaFaultProxy):
    def _handle(self, client):
        upstream = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15)
        try:
            while not self.stop_evt.is_set():
                req = read_frame(client)
                if req is None:
                    break
                api_key, _api_ver, corr_id = struct.unpack(">hhi", req[:8])
                if api_key == API_END_TXN and parse_end_txn_committed(req):
                    with self.lock:
                        self.commit_requests += 1
                    if not state3["rejected"]:
                        state3["rejected"] = True
                        write_frame(client, struct.pack(">iBihB", corr_id, 0, 0, ERR_NOT_COORDINATOR, 0))
                        continue
                    state3["forwarded_commits"] += 1
                elif api_key == API_END_TXN:
                    with self.lock:
                        self.abort_requests += 1
                write_frame(upstream, req)
                resp = read_frame(upstream)
                if resp is None:
                    break
                if api_key in (API_METADATA, API_FIND_COORDINATOR):
                    resp = resp[:4] + resp[4:].replace(
                        struct.pack(">I", BROKER_PORT),
                        struct.pack(">I", self.port),
                    )
                if api_key == API_API_VERSIONS and state3["rejected"] and not state3["signaled"] and proc3:
                    state3["signaled"] = True
                    os.kill(proc3[0].pid, signal.SIGINT)
                    time.sleep(0.05)
                write_frame(client, resp)
        except OSError:
            pass
        finally:
            for s in (client, upstream):
                try:
                    s.close()
                except OSError:
                    pass


proxy3 = RejectFirstCommitProxy(lambda *_: None)
try:
    p = subprocess.Popen(
        [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy3.port}", "--txn", txn3, t3],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    proc3.append(p)
    _, err = p.communicate("k1\tnever-committed-on-reconnect-sigint\n", timeout=30)
    assert state3["rejected"] and state3["signaled"], state3
    assert p.returncode == 130, (p.returncode, err)
    assert "outcome unknown" in err, f"expected unknown-outcome message, got: {err!r}"
    assert state3["forwarded_commits"] == 0, f"must not dispatch commit after SIGINT during reconnect: {state3}"
    assert proxy3.abort_requests == 0, f"must not send abort after commit dispatched: {proxy3.abort_requests}"
finally:
    proxy3.close()

out3 = run_kite("consume", "-B", "--idle", "2s", "--format", "value", t3).stdout.strip()
assert out3 == "", f"record should not have been committed: {out3!r}"
print("PASS txn-fault: SIGINT during coordinator reconnect stops before dispatching commit")


# Fault 4 (R2 P2): Truncated EndTxn(commit) reply after Kafka committed, then SIGINT arrives.
t4 = "kite-txn-fault-truncated"
txn4 = "kite-e2e-fault-truncated"
run_kite("topic", "delete", "-y", "--if-exists", t4, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t4)

proc4 = []
trunc4 = [0]


def fault4(api_key, corr_id, req, resp):
    if api_key == API_END_TXN and parse_end_txn_committed(req):
        trunc4[0] += 1
        if trunc4[0] == 1 and proc4:
            os.kill(proc4[0].pid, signal.SIGINT)
            # Valid 4-byte correlation_id + header tag byte, empty body (Truncated).
            return struct.pack(">iB", corr_id, 0)
    return None


proxy4 = KafkaFaultProxy(fault4)
try:
    p = subprocess.Popen(
        [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy4.port}", "--txn", txn4, t4],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    proc4.append(p)
    _, err = p.communicate("k1\tcommitted-despite-truncated-reply\n", timeout=30)
    assert trunc4[0] >= 1, "expected proxy to truncate EndTxn reply"
    assert p.returncode == 130, (p.returncode, err)
    assert "outcome unknown" in err, f"expected unknown-outcome message, got: {err!r}"
    assert proxy4.abort_requests == 0, f"must not send abort after truncated commit reply: {proxy4.abort_requests}"
finally:
    proxy4.close()

out4 = run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t4).stdout.strip()
assert out4 == "committed-despite-truncated-reply", f"expected committed record under read_committed, got: {out4!r}"
print("PASS txn-fault: truncated EndTxn(commit) reply + SIGINT reports unknown outcome")


# Fault 5 (R1 P2): Every AddPartitionsToTxn reply is lost after Kafka registers the partition.
t5 = "kite-txn-fault-addparts-lost"
txn5 = "kite-e2e-fault-addparts-lost"
run_kite("topic", "delete", "-y", "--if-exists", t5, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t5)

dropped5 = [0]


def fault5(api_key, corr_id, req, resp):
    if api_key == API_ADD_PARTITIONS_TO_TXN:
        dropped5[0] += 1
        return "drop"
    return None


proxy5 = KafkaFaultProxy(fault5)
try:
    res = run_kite(
        "produce", "--bootstrap", f"{BROKER_HOST}:{proxy5.port}", "--txn", txn5, t5,
        data="k1\tnever-produced\n", check=False,
    )
    assert dropped5[0] >= 1, "expected proxy to drop AddPartitionsToTxn reply"
    assert res.returncode == 1, (res.returncode, res.stderr)
    assert proxy5.abort_requests >= 1, f"expected EndTxn(abort) cleanup, got {proxy5.abort_requests}"
finally:
    proxy5.close()

wait_complete_abort(txn5)
run_kite("produce", "--txn", "kite-e2e-fault5-sentinel", t5, data="k1\tsentinel-after-abort\n")
out5 = run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t5).stdout.strip()
assert out5 == "sentinel-after-abort", f"expected sentinel under read_committed, got: {out5!r}"
print("PASS txn-fault: lost AddPartitionsToTxn reply aborts cleanly and unblocks LSO")

run_kite("topic", "delete", "-y", t1, t2, t3, t4, t5, check=False)
