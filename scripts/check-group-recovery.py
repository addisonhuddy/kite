"""Scripted Kafka broker faults exercise group heartbeat and recovery boundaries."""

import json
import concurrent.futures
import contextlib
import os
import socketserver
import struct
import subprocess
import sys
import threading
import time


def pack(fmt, *values):
    return struct.pack(">" + fmt, *values)


def text(value):
    return bytes([len(value) + 1]) + value


def string(data, pos):
    size = data[pos] - 1
    return data[pos + 1:pos + 1 + size], pos + 1 + size


def batch(offset=0):
    record = b"\x14\x00\x00\x00\x01\x08kept\x00"
    data = pack("hiqqqhii", 0, 0, 0, 0, -1, -1, -1, 1) + record
    crc = 0xffffffff
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    body = pack("ibI", 0, 2, crc ^ 0xffffffff) + data
    return pack("qi", offset, len(body)) + body


class Broker(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, mode, parent=None):
        super().__init__(("127.0.0.1", 0), Handler)
        self.joined = None
        self.beats = []
        self.epochs = []
        self.commits = []
        self.mode = mode
        self.delivered = None
        self.fenced = False
        self.generation = 1
        self.parent = parent
        self.node = 2 if parent else 1
        self.secondary = None
        self.tertiary = None
        self.fetch_old_fault = False
        self.fetch_nodes = []
        self.started = time.monotonic()
        self.reset_started = None
        self.reset_fetches = []
        self.assignment_changed = None
        self.revoked_commit = False
        self.revoked_released = False
        self.partition_deliveries = []
        self.fetches = []
        self.commit_recovered = None
        self.fetch_connections = []
        self.position_fetches = []
        self.position_lists = []
        self.produced = False


def positions_response(broker, node, api, req):
    pos = 11 + struct.unpack_from(">h", req, 8)[0]
    if api == 3:
        servers = [(1, broker), (2, broker.secondary)]
        if broker.tertiary:
            servers.append((3, broker.tertiary))
        addresses = b"".join(pack("i", n) + text(b"127.0.0.1") + pack("i", server.server_address[1]) + b"\x00\x00" for n, server in servers)
        partitions = b"".join(pack("hiii", 0, p, p + 1 if broker.tertiary else 1 if p == 0 else 2, 1) + b"\x01\x01\x01\x00" for p in range(3))
        topic = b"\x00\x00" + text(b"example") + b"\x00" * 17 + b"\x04" + partitions + b"\x00" * 5
        return b"\x00" * 4 + bytes([len(servers) + 1]) + addresses + b"\x00" + pack("i", 1) + b"\x02" + topic + b"\x00"
    if api == 68:
        _, pos = string(req, pos)
        member, pos = string(req, pos)
        broker.beats.append(time.monotonic())
        assignment = b"\x01\x02" + b"\x00" * 16 + b"\x04" + pack("iii", 0, 1, 2) + b"\x00\x00\x00"
        return b"\x00" * 7 + text(member) + pack("ii", 1, 1000) + assignment
    if api == 9:
        _, pos = string(req, pos + 1)
        _, pos = string(req, pos)
        _, pos = string(req, pos + 5)
        count = req[pos] - 1
        parts = struct.unpack_from(">" + "i" * count, req, pos + 1)
        broker.position_fetches.append(parts)
        offsets = b"".join(pack("iqi", p, -1, -1) + b"\x00" * 4 for p in parts)
        return b"\x00" * 4 + b"\x02" + text(b"g") + b"\x02" + text(b"example") + bytes([count + 1]) + offsets + b"\x00" * 5
    if api == 2:
        _, pos = string(req, pos + 6)
        count = req[pos] - 1
        parts = [struct.unpack_from(">i", req, pos + 1 + 17 * i)[0] for i in range(count)]
        broker.position_lists.append((node, parts, time.monotonic()))
        if broker.mode == "position-slow-leaders":
            time.sleep(0.3)
        if broker.mode == "position-timeout" and node == 1:
            time.sleep(1)
            return None
        offset = 1 if broker.mode == "position-latest" and broker.produced else 0
        offsets = b"".join(pack("ihqqi", p, 0, 0, offset, 0) + b"\x00" for p in parts)
        return b"\x00" * 4 + b"\x02" + text(b"example") + bytes([count + 1]) + offsets + b"\x00\x00"
    if api == 1:
        _, pos = string(req, pos + 26)
        count = req[pos] - 1
        pos += 1
        if node == 1 and broker.mode != "position-slow-leaders":
            broker.produced = True
            wait, = struct.unpack_from(">i", req, 15 + struct.unpack_from(">h", req, 8)[0])
            time.sleep(wait / 1000)
        parts = b""
        for _ in range(count):
            partition, _, offset = struct.unpack_from(">iiq", req, pos)
            pos += 33
            limit = 1 if broker.mode == "position-slow-leaders" else (4 if broker.mode == "position-timeout" else 1) if partition == 1 else 0
            records = batch(offset) if offset < limit else b""
            parts += pack("ihqqq", partition, 0, limit, limit, -1) + b"\x00" + pack("i", -1) + text(records) + b"\x00"
        return b"\x00" * 10 + b"\x02" + text(b"example") + bytes([count + 1]) + parts + b"\x00\x00"
    if api == 8:
        broker.commits.append(time.monotonic())
        parts = b"".join(pack("ih", p, 0) + b"\x00" for p in range(3))
        return b"\x00" * 4 + b"\x02" + text(b"example") + b"\x04" + parts + b"\x00\x00"


def assignment_response(broker, api, req):
    pos = 11 + struct.unpack_from(">h", req, 8)[0]
    if api == 3:
        address = pack("i", 1) + text(b"127.0.0.1") + pack("i", broker.server_address[1]) + b"\x00\x00"
        partitions = b"".join(pack("hiii", 0, p, 1, 1) + b"\x01\x01\x01\x00" for p in range(4))
        topic = b"\x00\x00" + text(b"example") + b"\x00" * 17 + b"\x05" + partitions + b"\x00" * 5
        return b"\x00" * 4 + b"\x02" + address + b"\x00" + pack("i", 1) + b"\x02" + topic + b"\x00"
    if api == 68:
        _, pos = string(req, pos)
        member, pos = string(req, pos)
        epoch, = struct.unpack_from(">i", req, pos)
        pos += 10  # epoch, null instance/rack, rebalance timeout
        topics = req[pos] - 1
        pos += 1
        for _ in range(max(0, topics)):
            _, pos = string(req, pos)
        pos += 2  # null regex and assignor
        owned = None
        if req[pos] > 1:
            pos += 17  # topic array length and UUID
            count = req[pos] - 1
            owned = list(struct.unpack_from(">" + "i" * count, req, pos + 1))
        if epoch >= 0:
            broker.beats.append(time.monotonic())
            if broker.revoked_commit and owned is not None:
                assert 2 not in owned, owned
                broker.revoked_released = True
        if broker.delivered:
            broker.assignment_changed = broker.assignment_changed or time.monotonic()
        assigned = (0, 1, 3) if broker.assignment_changed else (0, 2)
        assignment = b"\x01\x02" + b"\x00" * 16 + bytes([len(assigned) + 1]) + pack("i" * len(assigned), *assigned) + b"\x00\x00\x00"
        return b"\x00" * 7 + text(member) + pack("ii", 2 if broker.assignment_changed else 1, 1000) + assignment
    if api == 9:
        offsets = b"".join(pack("iqi", p, -1, -1) + b"\x00" * 4 for p in range(4))
        return b"\x00" * 4 + b"\x02" + text(b"g") + b"\x02" + text(b"example") + b"\x05" + offsets + b"\x00" * 5
    if api == 2:
        _, pos = string(req, pos + 6)
        count = req[pos] - 1
        parts = b""
        for i in range(count):
            partition, = struct.unpack_from(">i", req, pos + 1 + 17 * i)
            unresolved = partition == 1 and time.monotonic() - broker.assignment_changed < 8
            parts += pack("ihqqi", partition, 6 if unresolved else 0, 0, 0, 0) + b"\x00"
        return b"\x00" * 4 + b"\x02" + text(b"example") + bytes([count + 1]) + parts + b"\x00\x00"
    if api == 1:
        _, pos = string(req, pos + 26)
        count = req[pos] - 1
        pos += 1
        parts = b""
        for _ in range(count):
            partition, _, offset = struct.unpack_from(">iiq", req, pos)
            pos += 33
            limit = {0: 2 if broker.revoked_released else 1, 1: 1, 2: 0, 3: 1}[partition]
            records = batch(offset) if offset < limit else b""
            if records:
                broker.partition_deliveries.append((partition, offset, time.monotonic()))
                broker.delivered = broker.delivered or time.monotonic()
            parts += pack("ihqqq", partition, 0, limit, limit, -1) + b"\x00" + pack("i", -1) + text(records) + b"\x00"
        time.sleep(0.01)
        return b"\x00" * 10 + b"\x02" + text(b"example") + bytes([count + 1]) + parts + b"\x00\x00"
    if api == 8:
        broker.commits.append(time.monotonic())
        broker.revoked_commit = bool(broker.assignment_changed)
        code = 14 if not broker.revoked_released else 0
        parts = b"".join(pack("ih", p, code) + b"\x00" for p in range(4))
        return b"\x00" * 4 + b"\x02" + text(b"example") + b"\x05" + parts + b"\x00\x00"


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        broker = self.server.parent or self.server
        try:
            while header := self.rfile.read(4):
                size, = struct.unpack(">I", header)
                req = self.rfile.read(size)
                api, _, corr = struct.unpack_from(">hhi", req)
                if api == 1:
                    broker.fetches.append(time.monotonic())
                    broker.fetch_connections.append(self.connection)
                    if broker.mode == "transport-connection" and len(broker.fetches) == 2:
                        return
                if broker.mode.startswith("position-") and api in (1, 2, 3, 8, 9, 68):
                    response = positions_response(broker, self.server.node, api, req)
                    if response is None:
                        return
                    payload = pack("i", corr) + b"\x00" + response
                    self.wfile.write(pack("I", len(payload)) + payload)
                    continue
                if broker.mode == "assignment" and api in (1, 2, 3, 8, 9, 68):
                    response = assignment_response(broker, api, req)
                    payload = pack("i", corr) + b"\x00" + response
                    self.wfile.write(pack("I", len(payload)) + payload)
                    continue
                if api == 1 and broker.mode == "throttle-idle":
                    payload = pack("i", corr) + b"\x00" + pack("ihi", 30000, 0, 0) + b"\x01\x00"
                    self.wfile.write(pack("I", len(payload)) + payload)
                    continue
                if api == 10 and (broker.mode == "no-join" or (broker.mode == "follow-join" and time.monotonic() - broker.started < 50)):
                    return
                if api == 68 and broker.mode == "no-join-heartbeat":
                    return
                split = broker.mode in ("healthy", "partition-bound", "follow-partition", "reset-fair")
                connection_test = broker.mode.endswith("-connection")
                paired = split or connection_test
                if api == 3 and split and broker.fetch_old_fault and not (broker.mode == "follow-partition" and time.monotonic() - broker.started >= 50):
                    return
                if api == 1 and (broker.mode == "leader" or split):
                    broker.fetch_nodes.append(self.server.node)
                    if self.server.node == 1 and broker.mode != "reset-fair" and not (broker.mode == "follow-partition" and time.monotonic() - broker.started >= 50):
                        broker.fetch_old_fault = True
                        time.sleep(1.1)
                        return
                elapsed = time.monotonic() - broker.delivered if broker.delivered else 0
                if broker.mode == "follow-coordinator" and broker.delivered and elapsed < 50 and api in (8, 9, 10, 68):
                    return
                if broker.mode in ("idle-confirmed", "idle-unconfirmed", "bounded", "follow-fetch") and broker.delivered and api == 1:
                    if broker.mode != "follow-fetch" or elapsed < 50:
                        return
                adopting = broker.joined and time.monotonic() - broker.joined < 8
                if adopting and ((broker.mode == "metadata" and api == 3) or (broker.mode == "offsets" and api == 9)):
                    return
                tags = b"\x00"
                if api == 18:
                    if broker.mode == "position-slow-leaders":
                        time.sleep(0.2)
                    tags = b""
                    versions = ((0, 11), (1, 12), (2, 8), (3, 12), (8, 9), (9, 9), (10, 6), (68, 1))
                    response = b"\x00\x00" + bytes([len(versions) + 1]) + b"".join(pack("hhh", k, v, v) + b"\x00" for k, v in versions) + b"\x00" * 5
                elif api == 3:
                    address = pack("i", 1) + text(b"127.0.0.1") + pack("i", broker.server_address[1]) + b"\x00\x00"
                    leader = 2 if broker.mode == "leader" and broker.fetch_old_fault else 1
                    if broker.mode == "metadata" and not broker.joined:
                        leader = -1
                    if leader == 2 or split:
                        address += pack("i", 2) + text(b"127.0.0.1") + pack("i", broker.secondary.server_address[1]) + b"\x00\x00"
                    partition = pack("hiii", 0, 0, leader, 1) + b"\x01\x01\x01\x00"
                    if paired:
                        partition += pack("hiii", 0, 1, 2 if split else 1, 1) + b"\x01\x01\x01\x00"
                    topic = b"\x00\x00" + text(b"example") + b"\x00" * 17 + bytes([3 if paired else 2]) + partition + b"\x00" * 5
                    response = b"\x00" * 4 + bytes([3 if leader == 2 or split else 2]) + address + b"\x00" + pack("i", 1) + b"\x02" + topic + b"\x00"
                elif api == 10:
                    response = b"\x00" * 4 + b"\x02" + text(b"g") + pack("i", 1) + text(b"127.0.0.1") + pack("i", broker.server_address[1]) + b"\x00" * 5
                elif api == 68:
                    pos = 11 + struct.unpack_from(">h", req, 8)[0]
                    _, pos = string(req, pos)
                    member, pos = string(req, pos)
                    epoch, = struct.unpack_from(">i", req, pos)
                    now = time.monotonic()
                    if epoch >= 0:
                        broker.joined = broker.joined or now
                        broker.beats.append(now)
                        broker.epochs.append(epoch)
                    if broker.mode == "follow-coordinator" and broker.delivered and not broker.fenced and epoch > 0:
                        broker.fenced = True
                        broker.generation = 2
                        response = pack("ih", 0, 110) + b"\x00\x00" + pack("ii", 0, 1000) + b"\xff\x00"
                        payload = pack("i", corr) + tags + response
                        self.wfile.write(pack("I", len(payload)) + payload)
                        continue
                    if broker.mode == "short-heartbeat-session" and epoch > 0:
                        if len(broker.beats) >= 2 and broker.beats[-1] - broker.beats[-2] > 0.15:
                            broker.fenced = True
                            response = pack("ih", 0, 25) + b"\x00\x00" + pack("ii", 0, 100) + b"\xff\x00"
                            payload = pack("i", corr) + tags + response
                            self.wfile.write(pack("I", len(payload)) + payload)
                            continue
                        time.sleep(0.06)
                    if broker.mode == "short-heartbeat-slow-coordinator" and epoch > 0:
                        time.sleep(0.09)
                    assignment = b"\x01\x02" + b"\x00" * 16 + b"\x02" + pack("i", 0) + b"\x00\x00\x00"
                    if paired:
                        assignment = b"\x01\x02" + b"\x00" * 16 + b"\x03" + pack("ii", 0, 1) + b"\x00\x00\x00"
                    interval = 1 if broker.mode == "short-heartbeat-1ms" else 2 if broker.mode == "short-heartbeat-2ms" else 100 if broker.mode.startswith("short-heartbeat") else 1000
                    response = b"\x00" * 7 + text(member) + pack("ii", broker.generation, interval) + assignment
                elif api == 9:
                    response = b"\x00" * 4 + b"\x02" + text(b"g") + b"\x02" + text(b"example") + b"\x02" + pack("iqi", 0, -1, -1) + b"\x00" * 9
                    if paired:
                        offsets = b"".join(pack("iqi", p, -1, -1) + b"\x00" * 4 for p in (0, 1))
                        response = b"\x00" * 4 + b"\x02" + text(b"g") + b"\x02" + text(b"example") + b"\x03" + offsets + b"\x00" * 5
                elif api == 2:
                    code = 6 if broker.mode == "adoption" and adopting else 0
                    if broker.mode == "reset-fail" and broker.reset_started:
                        code = 6
                    response = b"\x00" * 4 + b"\x02" + text(b"example") + b"\x02" + pack("ihqqi", self.server.node - 1 if split else 0, code, 0, 0, 0) + b"\x00\x00\x00"
                    if connection_test:
                        pos = 11 + struct.unpack_from(">h", req, 8)[0]
                        _, pos = string(req, pos + 6)
                        count = req[pos] - 1
                        parts = b"".join(pack("ihqqi", struct.unpack_from(">i", req, pos + 1 + 17 * i)[0], 0, 0, 0, 0) + b"\x00" for i in range(count))
                        response = b"\x00" * 4 + b"\x02" + text(b"example") + bytes([count + 1]) + parts + b"\x00\x00"
                elif api == 1:
                    fetch_code = 0
                    if broker.mode == "reset-fair" and self.server.node == 1:
                        fetch_code = 1
                    if broker.mode in ("reset", "reset-fail"):
                        broker.reset_started = broker.reset_started or time.monotonic()
                        broker.reset_fetches.append(time.monotonic())
                        if time.monotonic() - broker.reset_started < 50:
                            fetch_code = 1
                            time.sleep(0.05)
                    records = batch(1 if broker.delivered and broker.mode == "follow-fetch" else 0)
                    if split:
                        records = batch(broker.fetch_nodes.count(2) - 1)
                        if broker.mode not in ("healthy", "reset-fair") and broker.delivered and not (broker.mode == "follow-partition" and self.server.node == 1):
                            records = b""
                    if broker.mode == "short-heartbeat-slow-leader":
                        time.sleep(0.09)
                    if broker.mode.startswith("short-heartbeat") and broker.delivered:
                        delay = 0.25 if broker.mode in ("short-heartbeat", "short-heartbeat-session") else 0
                        if delay and time.monotonic() - broker.delivered < delay:
                            wait, = struct.unpack_from(">i", req, 15 + struct.unpack_from(">h", req, 8)[0])
                            time.sleep(wait / 1000)
                            records = b""
                        else:
                            records = batch(1)
                    if broker.delivered and broker.mode in ("commit-bound", "follow-commit"):
                        records = b""
                    if broker.mode == "follow-commit":
                        pos = 11 + struct.unpack_from(">h", req, 8)[0]
                        _, pos = string(req, pos + 26)
                        offset, = struct.unpack_from(">q", req, pos + 9)
                        records = batch(offset)
                    broker.delivered = broker.delivered or time.monotonic()
                    end = max(1, struct.unpack_from(">q", records)[0] + 1) if records else 1
                    part = pack("ihqqq", self.server.node - 1 if split else 0, fetch_code, end, end, -1) + b"\x00" + pack("i", -1) + text(records) + b"\x00"
                    response = b"\x00" * 10 + b"\x02" + text(b"example") + b"\x02" + part + b"\x00\x00"
                    if connection_test:
                        parts = b""
                        for p in (0, 1):
                            code = 6 if p == 0 and len(broker.fetches) == 2 else 0
                            records = b"" if code else batch(1 if p == 0 and len(broker.fetches) > 2 else 0)
                            end = 2 if p == 0 else 1
                            parts += pack("ihqqq", p, code, end, end, -1) + b"\x00" + pack("i", -1) + text(records) + b"\x00"
                        response = b"\x00" * 10 + b"\x02" + text(b"example") + b"\x03" + parts + b"\x00\x00"
                        if broker.mode == "request-connection" and len(broker.fetches) == 2:
                            response = pack("ihi", 0, 6, 0) + b"\x01\x00"
                elif api == 8:
                    broker.commits.append(time.monotonic())
                    code = 14 if broker.mode in ("idle-unconfirmed", "bounded", "commit-bound") or (broker.mode == "follow-commit" and elapsed < 50) or (broker.mode == "idle-confirmed" and elapsed < 1.7) else 0
                    if code == 0:
                        broker.commit_recovered = broker.commit_recovered or time.monotonic()
                    response = b"\x00" * 4 + b"\x02" + text(b"example") + b"\x02" + pack("ih", 1 if split else 0, code) + b"\x00\x00\x00"
                else:
                    raise AssertionError(api)
                payload = pack("i", corr) + tags + response
                self.wfile.write(pack("I", len(payload)) + payload)
        except (BrokenPipeError, ConnectionResetError):
            pass


def check(mode):
    with contextlib.ExitStack() as stack:
        broker = stack.enter_context(Broker(mode.removesuffix("-legacy")))
        if mode in ("leader", "healthy", "partition-bound", "follow-partition", "reset-fair") or mode.startswith("position-"):
            broker.secondary = stack.enter_context(Broker(mode, broker))
            threading.Thread(target=broker.secondary.serve_forever, daemon=True).start()
        if mode == "position-slow-leaders":
            broker.tertiary = stack.enter_context(Broker(mode, broker))
            broker.tertiary.node = 3
            threading.Thread(target=broker.tertiary.serve_forever, daemon=True).start()
        threading.Thread(target=broker.serve_forever, daemon=True).start()
        if mode == "throttle-idle":
            flags = ["--idle", "200ms"]
        elif mode in ("reset", "reset-fail"):
            flags = ["--idle", "2m", "-n", "1"]
        elif mode == "assignment":
            flags = ["-v", "--idle", "20s", "-n", "4"]
        elif mode.startswith("position-"):
            flags = ["--idle", "2s" if mode == "position-slow-leaders" else "300ms", "-n", "3" if mode == "position-slow-leaders" else "4" if mode == "position-timeout" else "1"]
        elif "-connection" in mode:
            flags = ["--idle", "20s", "-n", "3"]
        elif mode == "healthy":
            flags = ["--idle", "20s", "-n", "2"]
        elif mode.startswith("short-heartbeat"):
            flags = ["--idle", "5s", "-n", "2"]
        elif mode.startswith("follow"):
            flags = ["--follow", "-n", "1" if mode == "follow-join" else "3" if mode == "follow-commit" else "2"]
        elif mode.startswith("idle"):
            flags = ["--idle", "1500ms"]
        elif mode in ("bounded", "commit-bound", "partition-bound", "no-join", "no-join-heartbeat"):
            flags = ["--idle", "2m"]
        else:
            flags = ["--idle", "20s", "-n", "1"]
        result = subprocess.run(
            [sys.argv[1], "consume", *([] if mode == "throttle-idle" or mode.endswith("-legacy") else ["-g", "g"]), *([] if mode == "position-latest" else ["-B"]), *flags, "--json", "example"],
            env={**os.environ, "BOOTSTRAP_SERVERS": f"127.0.0.1:{broker.server_address[1]}"},
            capture_output=True, text=True, timeout=65,
        )
        broker.shutdown()
        if broker.secondary:
            broker.secondary.shutdown()
        if broker.tertiary:
            broker.tertiary.shutdown()
        rows = [json.loads(line) for line in result.stdout.splitlines()]
        if mode == "throttle-idle":
            assert result.returncode == 0 and not rows, (result.stderr, rows)
            assert time.monotonic() - broker.started < 2, result.stderr
            print("PASS group recovery throttle-idle", flush=True)
            return
        if mode == "reset-fail":
            assert result.returncode == 1 and not rows, result.stderr
            assert "next run may replay records" in result.stderr, result.stderr
            assert 44 <= time.monotonic() - broker.reset_started < 52
            print("PASS group recovery reset-fail", flush=True)
            return
        if mode in ("no-join", "no-join-heartbeat"):
            assert result.returncode == 1 and not rows and not broker.beats, result.stderr
            assert "before joining group" in result.stderr, result.stderr
            assert 35 <= time.monotonic() - broker.started < 50
            print(f"PASS group recovery {mode}", flush=True)
            return
        assert rows and all(row["value"] == "kept" for row in rows), (mode, result.stderr, rows)
        if "-connection" in mode:
            assert result.returncode == 0 and len(rows) == 3, (result.stderr, rows)
            assert sorted((r["partition"], r["offset"]) for r in rows) == [(0, 0), (0, 1), (1, 0)], rows
            assert len(set(broker.fetch_connections)) == (1 if mode.startswith("partition") else 2), broker.fetch_connections
            print(f"PASS group recovery {mode}", flush=True)
            return
        assert broker.commits, (mode, result.stderr)
        if mode.startswith("position-"):
            assert result.returncode == 0, (mode, result.stderr)
            assert broker.position_fetches == [(0, 1, 2)], broker.position_fetches
            if mode == "position-slow-leaders":
                assert sorted((r["partition"], r["offset"]) for r in rows) == [(0, 0), (1, 0), (2, 0)], rows
                assert {n for n, _, _ in broker.position_lists} == {1, 2, 3}, broker.position_lists
                assert sum(n == 1 for n, _, _ in broker.position_lists) == 1, broker.position_lists
                assert sum(n == 2 for n, _, _ in broker.position_lists) == 1, broker.position_lists
                assert max(b - a for a, b in zip(broker.beats, broker.beats[1:])) < 2, broker.beats
                assert time.monotonic() - broker.started < 12, result.stderr
                print(f"PASS group recovery {mode}", flush=True)
                return
            assert any(parts == [1, 2] for _, parts, _ in broker.position_lists), broker.position_lists
            assert all(at < broker.fetches[0] for _, _, at in broker.position_lists[:2]), broker.position_lists
            assert [r["offset"] for r in rows] == list(range(4 if mode == "position-timeout" else 1)), rows
            assert all(r["partition"] == 1 for r in rows), rows
            assert time.monotonic() - broker.started < 8, (mode, result.stderr)
            if mode == "position-timeout":
                attempts = [at for n, _, at in broker.position_lists if n == 1]
                assert len(attempts) > 1, broker.position_lists
                for a, b in zip(attempts, attempts[1:]):
                    assert any(a < at < b for at in broker.fetches), (broker.position_lists, broker.fetches)
            print(f"PASS group recovery {mode}", flush=True)
            return
        if mode == "assignment":
            assert result.returncode == 0 and len(rows) == 4, (result.stderr, rows)
            assert sorted((r["partition"], r["offset"]) for r in rows) == [(0, 0), (0, 1), (1, 0), (3, 0)], rows
            assert broker.revoked_released, result.stderr
            for partition, offset, delivered in broker.partition_deliveries:
                if partition == 3 or (partition == 0 and offset == 1):
                    assert delivered - broker.assignment_changed < 8, broker.partition_deliveries
                if partition == 1:
                    assert delivered - broker.assignment_changed >= 8, broker.partition_deliveries
            assert max(b - a for a, b in zip(broker.beats, broker.beats[1:])) < 2, broker.beats
            print("PASS group recovery assignment", flush=True)
            return
        if mode == "partition-bound":
            assert result.returncode == 1 and "next run may replay records" in result.stderr, result.stderr
            assert len(rows) == 1 and broker.fetch_nodes.count(2) > 10, (rows, broker.fetch_nodes)
            assert 44 <= time.monotonic() - broker.started < 52
            print("PASS group recovery partition-bound", flush=True)
            return
        if mode in ("idle-unconfirmed", "bounded", "commit-bound"):
            assert result.returncode == 1, (mode, result.stderr)
            assert "final commit failed" in result.stderr and "next run may replay records" in result.stderr, result.stderr
            if mode in ("bounded", "commit-bound"):
                assert 44 <= time.monotonic() - broker.delivered < 52
            if mode == "commit-bound":
                assert len(broker.commits) > 10 and len(broker.beats) > 30
        else:
            assert result.returncode == 0, (mode, result.stderr)
        if mode == "follow-join":
            assert len(rows) == 1 and time.monotonic() - broker.started >= 50, rows
            assert result.stderr.count("recovery ongoing") >= 2, result.stderr
        elif mode.startswith("follow"):
            origin = broker.started if mode == "follow-partition" else broker.delivered
            assert len(rows) == (3 if mode == "follow-commit" else 2) and time.monotonic() - origin >= 50, (mode, rows)
            assert result.stderr.count("recovery ongoing") >= 2, result.stderr
            if mode == "follow-coordinator":
                assert broker.fenced and broker.epochs.count(0) >= 2, broker.epochs
            elif mode == "follow-partition":
                assert [row["partition"] for row in rows] == [1, 0], rows
            elif mode == "follow-commit":
                assert [row["offset"] for row in rows] == [0, 1, 2], rows
                assert len(broker.fetches) == 3 and broker.fetches[1] >= broker.commit_recovered, broker.fetches
                assert len(broker.commits) > 10 and len(broker.beats) > 30, (broker.commits, broker.beats)
                assert max(b - a for a, b in zip(broker.beats, broker.beats[1:])) < 2, broker.beats
            else:
                assert [row["offset"] for row in rows] == [0, 1], rows
        elif mode == "idle-confirmed":
            assert any(t - broker.delivered >= 1.7 for t in broker.commits), broker.commits
        elif mode == "leader":
            assert broker.fetch_nodes == [1, 2], broker.fetch_nodes
        elif mode == "healthy":
            assert broker.fetch_old_fault and broker.fetch_nodes.count(2) == 2, broker.fetch_nodes
            assert len(rows) == 2 and all(row["partition"] == 1 for row in rows), rows
        elif mode.startswith("short-heartbeat"):
            assert [row["offset"] for row in rows] == [0, 1], rows
            assert time.monotonic() - broker.started < 3, result.stderr
            if mode in ("short-heartbeat", "short-heartbeat-session"):
                assert len(broker.beats) >= 3, broker.beats
            if mode == "short-heartbeat":
                assert max(b - a for a, b in zip(broker.beats, broker.beats[1:])) < 0.3, broker.beats
            if mode == "short-heartbeat-session":
                assert not broker.fenced, broker.beats
                assert max(b - a for a, b in zip(broker.beats, broker.beats[1:])) < 0.15, broker.beats
        elif mode == "reset":
            assert time.monotonic() - broker.reset_started >= 50
            assert len(broker.reset_fetches) > 500, len(broker.reset_fetches)
            assert "recovery ongoing" not in result.stderr, result.stderr
        elif mode == "reset-fair":
            assert broker.fetch_nodes == [1, 2], broker.fetch_nodes
            assert rows[0]["partition"] == 1, rows
        elif mode in ("adoption", "metadata", "offsets"):
            assert len(broker.beats) >= 8, (mode, broker.beats)
            assert max(b - a for a, b in zip(broker.beats, broker.beats[1:])) < 2, (mode, broker.beats)
            assert broker.epochs[0] == 0 and all(e == 1 for e in broker.epochs[1:]), broker.epochs
        print(f"PASS group recovery {mode}", flush=True)


with concurrent.futures.ThreadPoolExecutor() as pool:
    list(pool.map(check, sys.argv[2:] or ("position-slow-leaders", "position-backlog", "position-latest", "position-timeout", "partition-connection", "partition-connection-legacy", "request-connection", "transport-connection", "assignment", "leader", "healthy", "short-heartbeat", "short-heartbeat-slow-coordinator", "short-heartbeat-slow-leader", "short-heartbeat-session", "short-heartbeat-1ms", "short-heartbeat-2ms", "partition-bound", "reset", "reset-fail", "reset-fair", "throttle-idle", "adoption", "metadata", "offsets", "idle-confirmed", "idle-unconfirmed", "bounded", "commit-bound", "follow-commit", "follow-fetch", "follow-partition", "follow-coordinator", "no-join", "no-join-heartbeat", "follow-join")))
