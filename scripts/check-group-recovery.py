"""Scripted Kafka broker faults exercise group heartbeat and recovery boundaries."""

import json
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


def batch():
    record = b"\x14\x00\x00\x00\x01\x08kept\x00"
    data = pack("hiqqqhii", 0, 0, 0, 0, -1, -1, -1, 1) + record
    crc = 0xffffffff
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    body = pack("ibI", 0, 2, crc ^ 0xffffffff) + data
    return pack("qi", 0, len(body)) + body


class Broker(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.joined = None
        self.beats = []
        self.epochs = []
        self.commits = []


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        broker = self.server
        try:
            while header := self.rfile.read(4):
                size, = struct.unpack(">I", header)
                req = self.rfile.read(size)
                api, _, corr = struct.unpack_from(">hhi", req)
                tags = b"\x00"
                if api == 18:
                    tags = b""
                    versions = ((0, 11), (1, 12), (2, 8), (3, 12), (8, 9), (9, 9), (10, 6), (68, 1))
                    response = b"\x00\x00" + bytes([len(versions) + 1]) + b"".join(pack("hhh", k, v, v) + b"\x00" for k, v in versions) + b"\x00" * 5
                elif api == 3:
                    address = pack("i", 1) + text(b"127.0.0.1") + pack("i", broker.server_address[1]) + b"\x00\x00"
                    partition = pack("hiii", 0, 0, 1, 1) + b"\x01\x01\x01\x00"
                    topic = b"\x00\x00" + text(b"example") + b"\x00" * 17 + b"\x02" + partition + b"\x00" * 5
                    response = b"\x00" * 4 + b"\x02" + address + b"\x00" + pack("i", 1) + b"\x02" + topic + b"\x00"
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
                    assignment = b"\x01\x02" + b"\x00" * 16 + b"\x02" + pack("i", 0) + b"\x00\x00\x00"
                    response = b"\x00" * 7 + text(member) + pack("ii", 1, 1000) + assignment
                elif api == 9:
                    response = b"\x00" * 4 + b"\x02" + text(b"g") + b"\x02" + text(b"example") + b"\x02" + pack("iqi", 0, -1, -1) + b"\x00" * 9
                elif api == 2:
                    code = 6 if time.monotonic() - broker.joined < 8 else 0
                    response = b"\x00" * 4 + b"\x02" + text(b"example") + b"\x02" + pack("ihqqi", 0, code, 0, 0, 0) + b"\x00\x00\x00"
                elif api == 1:
                    records = batch()
                    part = pack("ihqqq", 0, 0, 1, -1, -1) + b"\x00" + pack("i", -1) + text(records) + b"\x00"
                    response = b"\x00" * 10 + b"\x02" + text(b"example") + b"\x02" + part + b"\x00\x00"
                elif api == 8:
                    broker.commits.append(time.monotonic())
                    response = b"\x00" * 4 + b"\x02" + text(b"example") + b"\x02" + pack("ih", 0, 0) + b"\x00\x00\x00"
                else:
                    raise AssertionError(api)
                payload = pack("i", corr) + tags + response
                self.wfile.write(pack("I", len(payload)) + payload)
        except (BrokenPipeError, ConnectionResetError):
            pass


with Broker() as broker:
    threading.Thread(target=broker.serve_forever, daemon=True).start()
    result = subprocess.run(
        [sys.argv[1], "consume", "-g", "g", "-B", "-n", "1", "--idle", "20s", "--json", "example"],
        env={**os.environ, "BOOTSTRAP_SERVERS": f"127.0.0.1:{broker.server_address[1]}"},
        capture_output=True, text=True, timeout=30,
    )
    broker.shutdown()
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["value"] == "kept", result.stdout
    assert len(broker.beats) >= 8, broker.beats
    assert max(b - a for a, b in zip(broker.beats, broker.beats[1:])) < 2, broker.beats
    assert broker.epochs[0] == 0 and all(e == 1 for e in broker.epochs[1:]), broker.epochs
    assert broker.commits
    print("PASS group adoption retries yield to heartbeats without rejoining")
