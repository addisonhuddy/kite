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
    def __init__(self, handler, before_forward=None):
        self.handler = handler
        self.before_forward = before_forward
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
                if self.before_forward:
                    self.before_forward(api_key, req)
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
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            res = subprocess.run(
                [
                    "docker", "exec", container,
                    "/opt/kafka/bin/kafka-transactions.sh",
                    "--bootstrap-server", "localhost:9092",
                    "describe", "--transactional-id", txn_id,
                ],
                text=True, capture_output=True, timeout=remaining,
            )
        except subprocess.TimeoutExpired:
            break
        last = res.stdout + res.stderr
        if res.returncode == 0 and "CompleteAbort" in res.stdout:
            return
        time.sleep(max(0, min(0.2, deadline - time.monotonic())))
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

# Hold an already-dispatched Produce on the wire while a parse failure aborts.
t6 = "kite-txn-fault-produce-held"
txn6 = "kite-e2e-fault-produce-held"
run_kite("topic", "delete", "-y", "--if-exists", t6, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t6)
produce_held = threading.Event()
release_produce = threading.Event()
produce_replied = threading.Event()
abort_before_reply = threading.Event()


def before_forward6(api_key, req):
    if api_key == API_PRODUCE:
        produce_held.set()
        release_produce.wait(10)
    elif api_key == API_END_TXN and not parse_end_txn_committed(req) and not produce_replied.is_set():
        abort_before_reply.set()


def fault6(api_key, corr_id, req, resp):
    if api_key == API_PRODUCE:
        produce_replied.set()


proxy6 = KafkaFaultProxy(fault6, before_forward6)
try:
    with subprocess.Popen(
        [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy6.port}", "--txn", txn6, "--json", t6],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) as p:
        try:
            p.stdin.write('{"value":"aborted-after-drain"}\n')
            p.stdin.flush()
            assert produce_held.wait(5), "expected linger to dispatch Produce"
            p.stdin.write("invalid-json\n")
            p.stdin.flush()
            assert not abort_before_reply.wait(1), "abort overtook outstanding Produce"
            release_produce.set()
            _, err = p.communicate(timeout=10)
            assert p.returncode == 1 and "json" in err.lower(), (p.returncode, err)
            assert produce_replied.is_set() and proxy6.abort_requests == 1, err
            assert not abort_before_reply.is_set(), "abort arrived before Produce settled"
        finally:
            release_produce.set()
            if p.poll() is None:
                p.kill()
finally:
    proxy6.close()

wait_complete_abort(txn6)
run_kite("produce", t6, data="sentinel-after-drain\n")
assert run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t6).stdout == "sentinel-after-drain\n"
assert run_kite("consume", "-B", "--read-uncommitted", "-n", "2", "--idle", "3s", "--format", "value", t6).stdout == "aborted-after-drain\nsentinel-after-drain\n"
print("PASS txn-fault: abort waits for in-flight Produce and leaves LSO unblocked")


# Lose an accepted commit reply, then stall FindCoordinator during recovery.
t7 = "kite-txn-fault-sigint-discovery"
txn7 = "kite-e2e-fault-sigint-discovery"
run_kite("topic", "delete", "-y", "--if-exists", t7, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t7)
commit_reply_lost = threading.Event()
discovery_held = threading.Event()
release_discovery = threading.Event()


def fault7(api_key, corr_id, req, resp):
    if api_key == API_END_TXN and parse_end_txn_committed(req):
        commit_reply_lost.set()
        return "drop"
    if api_key == API_FIND_COORDINATOR and commit_reply_lost.is_set():
        discovery_held.set()
        release_discovery.wait(10)
        return "drop"


proxy7 = KafkaFaultProxy(fault7)
try:
    with subprocess.Popen(
        [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy7.port}", "--txn", txn7, t7],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) as p:
        try:
            p.stdin.write("k1\tcommitted-before-rediscovery\n")
            p.stdin.close()
            p.stdin = None
            assert discovery_held.wait(5), "expected coordinator rediscovery after commit reply loss"
            p.send_signal(signal.SIGINT)
            _, err = p.communicate(timeout=3)
            assert p.returncode == 130 and "outcome unknown" in err, (p.returncode, err)
            assert proxy7.commit_requests == 1 and proxy7.abort_requests == 0, err
        finally:
            release_discovery.set()
            if p.poll() is None:
                p.kill()
finally:
    proxy7.close()

assert run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t7).stdout == "committed-before-rediscovery\n"
print("PASS txn-fault: SIGINT during stalled coordinator rediscovery exits promptly without abort")


# Fault 8 (R4 P1): Unsettled Produce triggers KIP-360 epoch bump via InitProducerId
# instead of plain EndTxn(abort), so a straggling Produce released after CompleteAbort
# is fenced by epoch and cannot create a hanging transaction.
API_INIT_PRODUCER_ID = 22
t8 = "kite-txn-fault-epoch-bump"
txn8 = "kite-e2e-fault-epoch-bump"
run_kite("topic", "delete", "-y", "--if-exists", t8, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t8)

state8 = {
    "held_produce": None,
    "produce_captured": threading.Event(),
    "init_count": 0,
    "end_txn_aborts": 0,
}


class UnsettledProduceProxy(KafkaFaultProxy):
    def _handle(self, client):
        upstream = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15)
        try:
            while not self.stop_evt.is_set():
                req = read_frame(client)
                if req is None:
                    break
                api_key, _api_ver, _corr_id = struct.unpack(">hhi", req[:8])
                if api_key == API_INIT_PRODUCER_ID:
                    with self.lock:
                        state8["init_count"] += 1
                elif api_key == API_END_TXN and not parse_end_txn_committed(req):
                    with self.lock:
                        state8["end_txn_aborts"] += 1
                elif api_key == API_PRODUCE and state8["held_produce"] is None:
                    # Capture the Produce frame without forwarding it yet and drop
                    # the client connection so kite cannot confirm the write.
                    state8["held_produce"] = req
                    state8["produce_captured"].set()
                    break
                write_frame(upstream, req)
                resp = read_frame(upstream)
                if resp is None:
                    break
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


proxy8 = UnsettledProduceProxy(lambda *_: None)
try:
    with subprocess.Popen(
        [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy8.port}", "--txn", txn8, "--json", t8],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) as p:
        p.stdin.write('{"key":"k1","value":"late-straggler"}\n')
        p.stdin.flush()
        assert state8["produce_captured"].wait(10), "expected linger to dispatch Produce"
        _, err = p.communicate("invalid-json\n", timeout=30)
        assert p.returncode == 1, (p.returncode, err)
        assert state8["init_count"] >= 2, f"expected KIP-360 InitProducerId epoch bump, got init_count={state8['init_count']} err={err!r}"
        assert state8["end_txn_aborts"] == 1, f"expected epoch-checked EndTxn(abort) before epoch bump, got {state8['end_txn_aborts']}"
        assert "PRODUCER_FENCED" not in err and "outcome unknown" not in err, f"unexpected abort warning on clean epoch bump: {err!r}"
finally:
    proxy8.close()

wait_complete_abort(txn8)
# Now release the straggling Produce frame directly to Kafka AFTER CompleteAbort.
late_conn = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15)
try:
    write_frame(late_conn, state8["held_produce"])
    late_resp = read_frame(late_conn)
    assert late_resp is not None, "expected Kafka to reject late straggling Produce"
finally:
    late_conn.close()

run_kite("produce", "--txn", "kite-e2e-fault8-sentinel", t8, data="k1\tsentinel-after-epoch-bump\n")
assert run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t8).stdout == "sentinel-after-epoch-bump\n"
assert run_kite("consume", "-B", "--read-uncommitted", "-n", "1", "--idle", "3s", "--format", "value", t8).stdout == "sentinel-after-epoch-bump\n"
print("PASS txn-fault: unsettled Produce aborts via KIP-360 epoch bump and fences late straggler")


# Fault 9 (R4 P2): SIGINT during withheld ApiVersions on coordinator reconnect exits promptly.
t9 = "kite-txn-fault-sigint-handshake"
txn9 = "kite-e2e-fault-sigint-handshake"
run_kite("topic", "delete", "-y", "--if-exists", t9, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t9)

commit_lost9 = threading.Event()
apiversions_held9 = threading.Event()
release_apiversions9 = threading.Event()


def fault9(api_key, corr_id, req, resp):
    if api_key == API_END_TXN and parse_end_txn_committed(req):
        commit_lost9.set()
        return "drop"
    if api_key == API_API_VERSIONS and commit_lost9.is_set():
        apiversions_held9.set()
        release_apiversions9.wait(10)
        return "drop"
    return None


proxy9 = KafkaFaultProxy(fault9)
try:
    with subprocess.Popen(
        [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy9.port}", "--txn", txn9, t9],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) as p:
        try:
            p.stdin.write("k1\tcommitted-before-handshake-sigint\n")
            p.stdin.close()
            p.stdin = None
            assert apiversions_held9.wait(5), "expected reconnect ApiVersions after commit reply loss"
            t0 = time.monotonic()
            p.send_signal(signal.SIGINT)
            _, err = p.communicate(timeout=3)
            elapsed = time.monotonic() - t0
            assert p.returncode == 130 and "outcome unknown" in err, (p.returncode, err)
            assert proxy9.commit_requests == 1 and proxy9.abort_requests == 0, err
            assert elapsed < 2.0, f"SIGINT blocked {elapsed:.2f}s on reconnect ApiVersions"
        finally:
            release_apiversions9.set()
            if p.poll() is None:
                p.kill()
finally:
    proxy9.close()

assert run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t9).stdout == "committed-before-handshake-sigint\n"
print("PASS txn-fault: SIGINT during stalled reconnect ApiVersions exits promptly without abort")


# Fault 10 (P1 regression): Stale fenced producer with an unsettled Produce must NOT
# abort a replacement producer's ongoing transaction under the same transactional.id.
t10 = "kite-txn-fault-replacement-fencing"
txn10 = "kite-e2e-fault-replacement-fencing"
run_kite("topic", "delete", "-y", "--if-exists", t10, check=False)
run_kite("topic", "create", "-p", "1", "-r", "1", t10)

state10_a = {
    "produce_captured": threading.Event(),
    "init_count": 0,
    "end_txn_aborts": 0,
}


class StaleProducerProxy(KafkaFaultProxy):
    def _handle(self, client):
        upstream = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15)
        try:
            while not self.stop_evt.is_set():
                req = read_frame(client)
                if req is None:
                    break
                api_key, _api_ver, _corr_id = struct.unpack(">hhi", req[:8])
                if api_key == API_INIT_PRODUCER_ID:
                    with self.lock:
                        state10_a["init_count"] += 1
                elif api_key == API_END_TXN and not parse_end_txn_committed(req):
                    with self.lock:
                        state10_a["end_txn_aborts"] += 1
                elif api_key == API_PRODUCE and not state10_a["produce_captured"].is_set():
                    state10_a["produce_captured"].set()
                    break
                write_frame(upstream, req)
                resp = read_frame(upstream)
                if resp is None:
                    break
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


replacement_acked = threading.Event()


def fault10_b(api_key, corr_id, req, resp):
    if api_key == API_PRODUCE:
        replacement_acked.set()
    return None


proxy10_a = StaleProducerProxy(lambda *_: None)
proxy10_b = KafkaFaultProxy(fault10_b)
try:
    with subprocess.Popen(
        [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy10_a.port}", "--txn", txn10, "--json", t10],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) as stale_p:
        try:
            stale_p.stdin.write('{"key":"k1","value":"stale-unsettled"}\n')
            stale_p.stdin.flush()
            assert state10_a["produce_captured"].wait(10), "expected stale producer to dispatch Produce"

            # Start replacement producer with the same transactional.id and hold its txn Ongoing.
            with subprocess.Popen(
                [kite, "produce", "--bootstrap", f"{BROKER_HOST}:{proxy10_b.port}", "--txn", txn10, t10],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            ) as repl_p:
                try:
                    repl_p.stdin.write("k1\treplacement-committed\n")
                    repl_p.stdin.flush()
                    assert replacement_acked.wait(10), "expected replacement producer Produce ack"

                    # Trigger unsettled-Produce cleanup on the stale producer.
                    _, stale_err = stale_p.communicate("invalid-json\n", timeout=30)
                    assert stale_p.returncode == 1, (stale_p.returncode, stale_err)
                    assert state10_a["init_count"] == 1, f"stale producer must not send KIP-360 InitProducerId after fencing, got init_count={state10_a['init_count']} err={stale_err!r}"
                    assert state10_a["end_txn_aborts"] == 1, f"expected epoch-checked EndTxn(abort) from stale producer, got {state10_a['end_txn_aborts']}"
                    assert "outcome unknown" in stale_err and "kafka-transactions.sh describe" in stale_err, stale_err

                    # Replacement producer must still commit cleanly.
                    _, repl_err = repl_p.communicate(timeout=15)
                    assert repl_p.returncode == 0, f"replacement producer failed to commit: rc={repl_p.returncode} err={repl_err!r}"
                finally:
                    if repl_p.poll() is None:
                        repl_p.kill()
        finally:
            if stale_p.poll() is None:
                stale_p.kill()
finally:
    proxy10_a.close()
    proxy10_b.close()

assert run_kite("consume", "-B", "-n", "1", "--idle", "3s", "--format", "value", t10).stdout == "replacement-committed\n"
print("PASS txn-fault: stale fenced producer cannot abort replacement transaction")

run_kite("topic", "delete", "-y", t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, check=False)
