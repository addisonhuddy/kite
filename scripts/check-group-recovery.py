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
        self.fetch_old_fault = False
        self.fetch_nodes = []
        self.started = time.monotonic()


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        broker = self.server.parent or self.server
        try:
            while header := self.rfile.read(4):
                size, = struct.unpack(">I", header)
                req = self.rfile.read(size)
                api, _, corr = struct.unpack_from(">hhi", req)
                if api == 10 and (broker.mode == "no-join" or (broker.mode == "follow-join" and time.monotonic() - broker.started < 50)):
                    return
                if api == 1 and broker.mode == "leader":
                    broker.fetch_nodes.append(self.server.node)
                    if self.server.node == 1:
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
                    tags = b""
                    versions = ((0, 11), (1, 12), (2, 8), (3, 12), (8, 9), (9, 9), (10, 6), (68, 1))
                    response = b"\x00\x00" + bytes([len(versions) + 1]) + b"".join(pack("hhh", k, v, v) + b"\x00" for k, v in versions) + b"\x00" * 5
                elif api == 3:
                    address = pack("i", 1) + text(b"127.0.0.1") + pack("i", broker.server_address[1]) + b"\x00\x00"
                    leader = 2 if broker.mode == "leader" and broker.fetch_old_fault else 1
                    if leader == 2:
                        address += pack("i", 2) + text(b"127.0.0.1") + pack("i", broker.secondary.server_address[1]) + b"\x00\x00"
                    partition = pack("hiii", 0, 0, leader, 1) + b"\x01\x01\x01\x00"
                    topic = b"\x00\x00" + text(b"example") + b"\x00" * 17 + b"\x02" + partition + b"\x00" * 5
                    response = b"\x00" * 4 + bytes([3 if leader == 2 else 2]) + address + b"\x00" + pack("i", 1) + b"\x02" + topic + b"\x00"
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
                    assignment = b"\x01\x02" + b"\x00" * 16 + b"\x02" + pack("i", 0) + b"\x00\x00\x00"
                    response = b"\x00" * 7 + text(member) + pack("ii", broker.generation, 1000) + assignment
                elif api == 9:
                    response = b"\x00" * 4 + b"\x02" + text(b"g") + b"\x02" + text(b"example") + b"\x02" + pack("iqi", 0, -1, -1) + b"\x00" * 9
                elif api == 2:
                    code = 6 if broker.mode == "adoption" and adopting else 0
                    response = b"\x00" * 4 + b"\x02" + text(b"example") + b"\x02" + pack("ihqqi", 0, code, 0, 0, 0) + b"\x00\x00\x00"
                elif api == 1:
                    records = batch(1 if broker.delivered and broker.mode == "follow-fetch" else 0)
                    if broker.delivered and broker.mode in ("commit-bound", "follow-commit"):
                        records = batch(1) if broker.mode == "follow-commit" and elapsed >= 50 else b""
                    broker.delivered = broker.delivered or time.monotonic()
                    part = pack("ihqqq", 0, 0, 1, -1, -1) + b"\x00" + pack("i", -1) + text(records) + b"\x00"
                    response = b"\x00" * 10 + b"\x02" + text(b"example") + b"\x02" + part + b"\x00\x00"
                elif api == 8:
                    broker.commits.append(time.monotonic())
                    code = 14 if broker.mode in ("idle-unconfirmed", "bounded", "commit-bound") or (broker.mode == "follow-commit" and elapsed < 50) or (broker.mode == "idle-confirmed" and elapsed < 1.7) else 0
                    response = b"\x00" * 4 + b"\x02" + text(b"example") + b"\x02" + pack("ih", 0, code) + b"\x00\x00\x00"
                else:
                    raise AssertionError(api)
                payload = pack("i", corr) + tags + response
                self.wfile.write(pack("I", len(payload)) + payload)
        except (BrokenPipeError, ConnectionResetError):
            pass


def check(mode):
    with contextlib.ExitStack() as stack:
        broker = stack.enter_context(Broker(mode))
        if mode == "leader":
            broker.secondary = stack.enter_context(Broker(mode, broker))
            threading.Thread(target=broker.secondary.serve_forever, daemon=True).start()
        threading.Thread(target=broker.serve_forever, daemon=True).start()
        if mode.startswith("follow"):
            flags = ["--follow", "-n", "1" if mode == "follow-join" else "2"]
        elif mode.startswith("idle"):
            flags = ["--idle", "1500ms"]
        elif mode in ("bounded", "commit-bound", "no-join"):
            flags = ["--idle", "2m"]
        else:
            flags = ["--idle", "20s", "-n", "1"]
        result = subprocess.run(
            [sys.argv[1], "consume", "-g", "g", "-B", *flags, "--json", "example"],
            env={**os.environ, "BOOTSTRAP_SERVERS": f"127.0.0.1:{broker.server_address[1]}"},
            capture_output=True, text=True, timeout=65,
        )
        broker.shutdown()
        if broker.secondary:
            broker.secondary.shutdown()
        rows = [json.loads(line) for line in result.stdout.splitlines()]
        if mode == "no-join":
            assert result.returncode == 1 and not rows and not broker.beats, result.stderr
            assert "before joining group" in result.stderr, result.stderr
            assert 35 <= time.monotonic() - broker.started < 50
            print("PASS group recovery no-join", flush=True)
            return
        assert rows and all(row["value"] == "kept" for row in rows), (mode, result.stderr, rows)
        assert broker.commits, (mode, result.stderr)
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
            assert len(rows) == 2 and time.monotonic() - broker.delivered >= 50, (mode, rows)
            assert result.stderr.count("recovery ongoing") >= 2, result.stderr
            if mode == "follow-coordinator":
                assert broker.fenced and broker.epochs.count(0) >= 2, broker.epochs
            else:
                assert [row["offset"] for row in rows] == [0, 1], rows
        elif mode == "idle-confirmed":
            assert any(t - broker.delivered >= 1.7 for t in broker.commits), broker.commits
        elif mode == "leader":
            assert broker.fetch_nodes == [1, 2], broker.fetch_nodes
        elif mode in ("adoption", "metadata", "offsets"):
            assert len(broker.beats) >= 8, (mode, broker.beats)
            assert max(b - a for a, b in zip(broker.beats, broker.beats[1:])) < 2, (mode, broker.beats)
            assert broker.epochs[0] == 0 and all(e == 1 for e in broker.epochs[1:]), broker.epochs
        print(f"PASS group recovery {mode}", flush=True)


with concurrent.futures.ThreadPoolExecutor() as pool:
    list(pool.map(check, sys.argv[2:] or ("leader", "adoption", "metadata", "offsets", "idle-confirmed", "idle-unconfirmed", "bounded", "commit-bound", "follow-commit", "follow-fetch", "follow-coordinator", "no-join", "follow-join")))
