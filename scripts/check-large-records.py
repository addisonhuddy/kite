"""Scripted Kafka broker tests for large-record fetch budgets, rotation, and frame limits."""

import json
import os
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


def pack(fmt, *values):
    return struct.pack(">" + fmt, *values)


def uvarint(n):
    out = bytearray()
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def zigzag(n):
    return uvarint((n << 1) ^ (n >> 63))


def compact_bytes(b):
    return uvarint(len(b) + 1) + b


def read_uvarint(buf, pos):
    val = 0
    shift = 0
    while True:
        b = buf[pos]
        pos += 1
        val |= (b & 0x7F) << shift
        if not (b & 0x80):
            return val, pos
        shift += 7


def read_compact_string(buf, pos):
    n, pos = read_uvarint(buf, pos)
    if n == 0:
        return None, pos
    n -= 1
    return buf[pos : pos + n], pos + n


def _make_crc32c_table():
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
        table.append(crc)
    return table


_CRC32C_TABLE = _make_crc32c_table()


def crc32c(data):
    crc = 0xFFFFFFFF
    table = _CRC32C_TABLE
    for byte in data:
        crc = table[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def record_batch(offset, payload):
    # v2 single-record uncompressed batch.
    body = b"\x00" + zigzag(0) + zigzag(0) + zigzag(-1) + zigzag(len(payload)) + payload + uvarint(0)
    record = zigzag(len(body)) + body
    data = pack("hiqqqhii", 0, 0, 0, 0, -1, -1, -1, 1) + record
    # Compute CRC32C fast on large repeated payloads by using a small table loop.
    inner = pack("ibI", 0, 2, crc32c(data)) + data
    return pack("qi", offset, len(inner)) + inner


def parse_fetch_request(req):
    # Header v2: api(2), ver(2), corr(4), client_id(nullable string: i16 len + bytes), tag(uvarint).
    client_len, = struct.unpack_from(">h", req, 8)
    pos = 10 + max(0, client_len)
    _, pos = read_uvarint(req, pos)  # header tags
    # Fetch v12 body: replica_id(4), max_wait(4), min_bytes(4), max_bytes(4),
    # isolation(1), session_id(4), session_epoch(4).
    _, _, _, max_bytes, _, _, _ = struct.unpack_from(">iiiibii", req, pos)
    pos += 25
    topic_count, pos = read_uvarint(req, pos)
    parts = []
    for _ in range(topic_count - 1):
        _, pos = read_compact_string(req, pos)
        part_count, pos = read_uvarint(req, pos)
        for _ in range(part_count - 1):
            pidx, _, offset, _, _, part_max_bytes = struct.unpack_from(">iiqiqi", req, pos)
            pos += 32
            _, pos = read_uvarint(req, pos)  # part tags
            parts.append((pidx, offset, part_max_bytes))
        _, pos = read_uvarint(req, pos)  # topic tags
    return max_bytes, parts


def parse_commit_request(req):
    client_len, = struct.unpack_from(">h", req, 8)
    pos = 10 + max(0, client_len)
    _, pos = read_uvarint(req, pos)
    _, pos = read_compact_string(req, pos)  # group
    pos += 4  # generation/member_epoch
    _, pos = read_compact_string(req, pos)  # member_id
    _, pos = read_compact_string(req, pos)  # group_instance_id (nullable)
    topic_count, pos = read_uvarint(req, pos)
    committed = {}
    for _ in range(topic_count - 1):
        _, pos = read_compact_string(req, pos)
        part_count, pos = read_uvarint(req, pos)
        for _ in range(part_count - 1):
            pidx, offset, _ = struct.unpack_from(">iqi", req, pos)
            pos += 16
            _, pos = read_compact_string(req, pos)  # metadata
            _, pos = read_uvarint(req, pos)  # part tags
            committed[pidx] = offset
        _, pos = read_uvarint(req, pos)  # topic tags
    return committed


class LargeBroker(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, mode, partitions=1, batches_by_part=None, oversize_at_offset=None):
        super().__init__(("127.0.0.1", 0), Handler)
        self.mode = mode
        self.partitions = partitions
        self.batches_by_part = batches_by_part or {}
        self.oversize_at_offset = oversize_at_offset
        self.fetch_requests = []
        self.commits = []
        self.left_group = False


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        broker = self.server
        try:
            while header := self.rfile.read(4):
                size, = struct.unpack(">I", header)
                req = self.rfile.read(size)
                api, _, corr = struct.unpack_from(">hhi", req)
                tags = b"\x00"
                if api == 18:  # ApiVersions
                    tags = b""
                    versions = ((0, 11), (1, 12), (2, 8), (3, 12), (8, 9), (9, 9), (10, 6), (68, 1))
                    response = (
                        b"\x00\x00"
                        + uvarint(len(versions) + 1)
                        + b"".join(pack("hhh", k, v, v) + b"\x00" for k, v in versions)
                        + b"\x00" * 5
                    )
                elif api == 3:  # Metadata
                    addr = pack("i", 1) + compact_bytes(b"127.0.0.1") + pack("i", broker.server_address[1]) + b"\x00\x00"
                    parts = b"".join(
                        pack("hiii", 0, p, 1, 1) + b"\x01\x01\x01\x00" for p in range(broker.partitions)
                    )
                    topic = (
                        b"\x00\x00"
                        + compact_bytes(b"large")
                        + b"\x00" * 17
                        + uvarint(broker.partitions + 1)
                        + parts
                        + b"\x00" * 5
                    )
                    response = b"\x00" * 4 + b"\x02" + addr + b"\x00" + pack("i", 1) + b"\x02" + topic + b"\x00"
                elif api == 2:  # ListOffsets
                    parts = b"".join(
                        pack("ihqqi", p, 0, -1, 0, -1) + b"\x00" for p in range(broker.partitions)
                    )
                    response = (
                        b"\x00" * 4
                        + b"\x02"
                        + compact_bytes(b"large")
                        + uvarint(broker.partitions + 1)
                        + parts
                        + b"\x00\x00"
                    )
                elif api == 10:  # FindCoordinator
                    response = (
                        b"\x00" * 4
                        + b"\x02"
                        + compact_bytes(b"g")
                        + pack("i", 1)
                        + compact_bytes(b"127.0.0.1")
                        + pack("i", broker.server_address[1])
                        + b"\x00" * 5
                    )
                elif api == 68:  # ConsumerGroupHeartbeat
                    client_len, = struct.unpack_from(">h", req, 8)
                    pos = 11 + max(0, client_len)
                    _, pos = read_compact_string(req, pos)  # group
                    member, pos = read_compact_string(req, pos)
                    epoch, = struct.unpack_from(">i", req, pos)
                    if epoch < 0:
                        broker.left_group = True
                    part_ids = b"".join(pack("i", p) for p in range(broker.partitions))
                    assignment = (
                        b"\x01\x02"
                        + b"\x00" * 16
                        + uvarint(broker.partitions + 1)
                        + part_ids
                        + b"\x00\x00\x00"
                    )
                    response = b"\x00" * 7 + compact_bytes(member) + pack("ii", 1, 1000) + assignment
                elif api == 9:  # OffsetFetch
                    parts = b"".join(
                        pack("iqi", p, -1, -1) + b"\x00\x00\x00\x00" for p in range(broker.partitions)
                    )
                    response = (
                        b"\x00" * 4
                        + b"\x02"
                        + compact_bytes(b"g")
                        + b"\x02"
                        + compact_bytes(b"large")
                        + uvarint(broker.partitions + 1)
                        + parts
                        + b"\x00\x00\x00\x00\x00"
                    )
                elif api == 8:  # OffsetCommit
                    committed = parse_commit_request(req)
                    broker.commits.append(committed)
                    parts = b"".join(pack("ih", p, 0) + b"\x00" for p in sorted(committed))
                    response = (
                        b"\x00" * 4
                        + b"\x02"
                        + compact_bytes(b"large")
                        + uvarint(len(committed) + 1)
                        + parts
                        + b"\x00\x00"
                    )
                elif api == 1:  # Fetch
                    max_bytes, req_parts = parse_fetch_request(req)
                    broker.fetch_requests.append((max_bytes, req_parts))
                    if broker.oversize_at_offset is not None and any(
                        off == broker.oversize_at_offset for _, off, _ in req_parts
                    ):
                        # Send a frame length strictly exceeding 128 MiB; kite must fail closed immediately.
                        self.wfile.write(pack("I", (128 << 20) + 1))
                        self.wfile.flush()
                        return
                    resp_parts = bytearray()
                    for idx, (pidx, offset, _) in enumerate(req_parts):
                        p_batches = broker.batches_by_part.get(pidx, {})
                        # In rotation mode, simulate KIP-74 oversized batch behavior:
                        # only deliver an oversized partition when it is first in the Fetch request.
                        if broker.mode == "rotation" and idx != 0:
                            rec = b""
                        else:
                            rec = p_batches.get(offset, b"")
                        end = max(p_batches.keys()) + 1 if p_batches else 0
                        resp_parts += (
                            pack("ihqqq", pidx, 0, end, end, -1)
                            + b"\x00"
                            + pack("i", -1)
                            + compact_bytes(rec)
                            + b"\x00"
                        )
                    response = (
                        b"\x00" * 10
                        + b"\x02"
                        + compact_bytes(b"large")
                        + uvarint(len(req_parts) + 1)
                        + bytes(resp_parts)
                        + b"\x00\x00"
                    )
                else:
                    raise AssertionError(f"unexpected api {api}")
                payload = pack("i", corr) + tags + response
                self.wfile.write(pack("I", len(payload)) + payload)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass


def run_kite(kite, port, args, timeout=15):
    return subprocess.run(
        [kite, *args],
        env={**os.environ, "BOOTSTRAP_SERVERS": f"127.0.0.1:{port}"},
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def main():
    kite = sys.argv[1]

    # 1. Verify partition_max_bytes honors fetch.max.bytes (default 8 MiB and custom 24 MiB).
    small_batch = record_batch(0, b"ok")
    for label, cfg_val, expected in (("default", None, 8 << 20), ("custom-24m", 24 << 20, 24 << 20)):
        with LargeBroker("budget", batches_by_part={0: {0: small_batch}}) as broker:
            threading.Thread(target=broker.serve_forever, daemon=True).start()
            with tempfile.TemporaryDirectory() as tmp:
                extra = []
                if cfg_val is not None:
                    cfg_path = Path(tmp) / "kite.properties"
                    cfg_path.write_text(f"fetch.max.bytes={cfg_val}\n")
                    extra = ["--config", str(cfg_path)]
                res = run_kite(kite, broker.server_address[1], ["consume", *extra, "-B", "-n", "1", "--format", "value", "large"])
            assert res.returncode == 0 and res.stdout == "ok\n", (label, res.returncode, res.stderr)
            assert broker.fetch_requests, label
            max_bytes, parts = broker.fetch_requests[0]
            assert max_bytes == expected, (label, max_bytes, expected)
            assert parts[0][2] == expected, (label, parts[0][2], expected)
    print("PASS partition_max_bytes honors fetch.max.bytes", flush=True)

    # 2. Verify 15 MiB -> 17 MiB -> trailing small record succeed in standalone and group modes.
    # Build batches using repeated chunks (fast to construct).
    payload_15m = b"a" * (15 << 20)
    payload_17m = b"b" * (17 << 20)
    batches = {
        0: {
            0: record_batch(0, payload_15m),
            1: record_batch(1, payload_17m),
            2: record_batch(2, b"tail"),
        }
    }
    for group_flags, name in (([], "standalone"), (["-g", "g"], "group")):
        with LargeBroker("large", batches_by_part=batches) as broker:
            threading.Thread(target=broker.serve_forever, daemon=True).start()
            res = run_kite(
                kite,
                broker.server_address[1],
                ["consume", *group_flags, "-B", "--idle", "400ms", "--format", "value", "large"],
                timeout=30,
            )
            assert res.returncode == 0, (name, res.returncode, res.stderr)
            lines = res.stdout.splitlines()
            assert [len(line) for line in lines] == [15 << 20, 17 << 20, 4], (name, [len(l) for l in lines])
            assert lines[2] == "tail", name
            if group_flags:
                assert broker.commits and broker.commits[-1] == {0: 3}, broker.commits
                assert broker.left_group
    print("PASS 15 MiB and 17 MiB records delivered in standalone and group consume", flush=True)

    # 3. Verify multi-partition rotation delivers records from every partition even when only parts[0] returns data.
    rot_batches = {
        0: {0: record_batch(0, b"p0-rec")},
        1: {0: record_batch(0, b"p1-rec")},
    }
    for group_flags, name in (([], "standalone"), (["-g", "g"], "group")):
        with LargeBroker("rotation", partitions=2, batches_by_part=rot_batches) as broker:
            threading.Thread(target=broker.serve_forever, daemon=True).start()
            res = run_kite(
                kite,
                broker.server_address[1],
                ["consume", *group_flags, "-B", "-n", "2", "--idle", "2s", "--json", "large"],
            )
            assert res.returncode == 0, (name, res.returncode, res.stderr)
            rows = [json.loads(line) for line in res.stdout.splitlines()]
            assert sorted((r["partition"], r["value"]) for r in rows) == [(0, "p0-rec"), (1, "p1-rec")], (name, rows)
    print("PASS multi-partition fetch order rotates to prevent starvation", flush=True)

    # 4. Verify ResponseTooLarge (> 128 MiB frame) fails closed immediately in all 4 modes.
    oversize_batches = {0: {0: record_batch(0, b"before-oversize")}}
    for flags, name, is_group in (
        (["-B", "--idle", "1500ms", "--format", "value"], "standalone-idle", False),
        (["-B", "--follow", "--format", "value"], "standalone-follow", False),
        (["-g", "g", "-B", "--idle", "1500ms", "--format", "value"], "group-idle", True),
        (["-g", "g", "-B", "--follow", "--format", "value"], "group-follow", True),
    ):
        with LargeBroker("oversize", batches_by_part=oversize_batches, oversize_at_offset=1) as broker:
            threading.Thread(target=broker.serve_forever, daemon=True).start()
            started = time.monotonic()
            res = run_kite(kite, broker.server_address[1], ["consume", *flags, "large"], timeout=10)
            elapsed = time.monotonic() - started
            assert res.returncode == 1, (name, res.returncode, res.stdout, res.stderr)
            assert elapsed < 5, (name, elapsed, res.stderr)
            assert "frame limit" in res.stderr, (name, res.stderr)
            assert res.stdout == "before-oversize\n", (name, res.stdout)
            if is_group:
                assert broker.commits and broker.commits[-1] == {0: 1}, (name, broker.commits)
                assert broker.left_group, name
    print("PASS ResponseTooLarge fails closed immediately in standalone/group and idle/follow", flush=True)


if __name__ == "__main__":
    main()
