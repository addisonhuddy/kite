"""Two scripted brokers exercise Fetch recovery past the idle deadline."""

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


def text(s):
    return bytes([len(s) + 1]) + s


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


def fetch_part(part, error=0, records=b""):
    end = int(bool(records))
    return pack("ihqqq", part, error, end, end, -1) + b"\x00" + pack("i", -1) + text(records) + b"\x00"


requests = []
metadata_count = 0
ports = {}


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        global metadata_count
        node = self.server.node
        while header := self.rfile.read(4):
            size, = struct.unpack(">I", header)
            req = self.rfile.read(size)
            api, _, corr = struct.unpack_from(">hhi", req)
            requests.append((node, api))
            tags = b"\x00"
            if api == 18:
                tags = b""
                versions = b"".join(pack("hhh", k, ver, ver) + b"\x00" for k, ver in ((0, 11), (1, 12), (2, 8), (3, 12)))
                response = b"\x00\x00\x05" + versions + b"\x00" * 5
            elif api == 3:
                metadata_count += 1
                brokers = b"".join(pack("i", i) + text(b"127.0.0.1") + pack("i", port) + b"\x00\x00" for i, port in ports.items())
                leaders = (1 if metadata_count == 1 else 2, 2)
                partitions = b"".join(pack("hiii", 0, i, leader, metadata_count) + b"\x01\x01\x01\x00" for i, leader in enumerate(leaders))
                topic = b"\x00\x00" + text(b"example") + b"\x00" * 17 + b"\x03" + partitions + b"\x00" * 5
                response = b"\x00" * 4 + b"\x03" + brokers + b"\x00" + pack("i", 1) + b"\x02" + topic + b"\x00"
            elif api == 1:
                if node == 1:
                    parts = [fetch_part(0, error=6)]  # NOT_LEADER_OR_FOLLOWER
                elif metadata_count == 1:
                    time.sleep(0.75)  # The healthy partition is empty past --idle.
                    parts = [fetch_part(1)]
                else:
                    # The queued record was already available on the new leader.
                    parts = [fetch_part(0, records=batch()), fetch_part(1)]
                response = b"\x00" * 10 + b"\x02" + text(b"example") + bytes([len(parts) + 1]) + b"".join(parts) + b"\x00\x00"
            else:
                raise AssertionError(api)
            payload = pack("i", corr) + tags + response
            self.wfile.write(pack("I", len(payload)) + payload)


servers = [socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler) for _ in range(2)]
for i, server in enumerate(servers, 1):
    server.node = i
    ports[i] = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

result = subprocess.run(
    [sys.argv[1], "consume", "-v", "--offset", "0", "--idle", "500ms", "-n", "1", "--json", "example"],
    env={**os.environ, "BOOTSTRAP_SERVERS": f"127.0.0.1:{ports[1]}"},
    capture_output=True, text=True, timeout=10,
)
print(result.stderr, end="")
print(f"exit={result.returncode}; metadata requests={metadata_count}; "
      f"Fetch broker sequence={[node for node, api in requests if api == 1]}; stdout={result.stdout.strip()!r}")
assert result.returncode == 0
assert metadata_count == 2
assert [node for node, api in requests if api == 1] == [1, 2, 2]
assert json.loads(result.stdout)["value"] == "kept"
