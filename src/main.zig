//! kannon — ultra-lightweight producer-only Kafka CLI.
//! `kannon <topic> < file` sends each stdin line as one record value.

const std = @import("std");
const config = @import("config.zig");
const client = @import("client.zig");
const protocol = @import("protocol.zig");
const transport = @import("transport.zig");
const scram = @import("scram.zig");

// unused-import anchors so `zig build test` covers every module
comptime {
    _ = config;
    _ = client;
    _ = protocol;
    _ = transport;
    _ = scram;
}

fn out(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

fn fatal(comptime fmt: []const u8, args: anytype) noreturn {
    out("kannon: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

fn fatalErr(c: *const client.Client, comptime fmt: []const u8) noreturn {
    const detail = c.errDetail();
    if (detail.len > 0)
        fatal(fmt ++ ": {s}", .{detail})
    else
        fatal(fmt, .{});
}

fn usageExit(code: u8) noreturn {
    out("usage: kannon [-v] [-H 'name: value']... <topic>\n" ++
        "  reads records from stdin, one per line:\n" ++
        "    value                            value only\n" ++
        "    key<TAB>value                    record key + value\n" ++
        "    key<TAB>h1: v1<TAB>...<TAB>value key + headers + value\n" ++
        "  -H 'name: value' adds the header to every record (repeatable)\n" ++
        "  -v, --verbose   connection/retry diagnostics on stderr\n", .{});
    std.process.exit(code);
}

fn usage() noreturn {
    usageExit(1);
}

/// Parse a 'name: value' header (curl -H style). Name/value are trimmed.
fn parseHeaderArg(s: []const u8) protocol.Header {
    const colon = std.mem.indexOfScalar(u8, s, ':') orelse
        fatal("malformed header '{s}' (want 'name: value')", .{s});
    const name = std.mem.trim(u8, s[0..colon], " \t");
    if (name.len == 0) fatal("malformed header '{s}' (want 'name: value')", .{s});
    return .{ .key = name, .value = std.mem.trim(u8, s[colon + 1 ..], " \t") };
}

/// Parse one stdin line into a record. First TAB-field = key (empty = null),
/// last = value, any middle fields = 'name: value' headers.
fn parseLine(
    alloc: std.mem.Allocator,
    line: []const u8,
    static_headers: []const protocol.Header,
    lineno: u64,
) protocol.Record {
    if (std.mem.indexOfScalar(u8, line, '\t') == null)
        return .{ .value = line, .headers = static_headers };

    var it = std.mem.splitScalar(u8, line, '\t');
    const keyf = it.next().?;
    var headers: std.ArrayListUnmanaged(protocol.Header) = .empty;
    headers.appendSlice(alloc, static_headers) catch fatal("out of memory", .{});
    var value: []const u8 = "";
    while (it.next()) |f| {
        if (it.peek() == null) {
            value = f;
        } else {
            if (std.mem.indexOfScalar(u8, f, ':') == null)
                fatal("line {d}: malformed header '{s}' (want 'name: value')", .{ lineno, f });
            headers.append(alloc, parseHeaderArg(f)) catch fatal("out of memory", .{});
        }
    }
    return .{
        .key = if (keyf.len == 0) null else keyf,
        .value = value,
        .headers = headers.items,
    };
}

/// Per-partition pending record buffer.
const Pending = struct {
    records: std.ArrayListUnmanaged(protocol.Record) = .empty,
    bytes: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const alloc = init.arena.allocator();

    const args = init.minimal.args.toSlice(alloc) catch fatal("out of memory", .{});
    var static_headers: std.ArrayListUnmanaged(protocol.Header) = .empty;
    var topic_arg: ?[]const u8 = null;
    var verbose = false;
    var ai: usize = 1;
    while (ai < args.len) : (ai += 1) {
        const a = args[ai];
        if (std.mem.eql(u8, a, "-H")) {
            ai += 1;
            if (ai >= args.len) usage();
            static_headers.append(alloc, parseHeaderArg(args[ai])) catch fatal("out of memory", .{});
        } else if (std.mem.startsWith(u8, a, "-H") and a.len > 2) {
            static_headers.append(alloc, parseHeaderArg(a[2..])) catch fatal("out of memory", .{});
        } else if (std.mem.eql(u8, a, "-v") or std.mem.eql(u8, a, "--verbose")) {
            verbose = true;
        } else if (std.mem.eql(u8, a, "-h") or std.mem.eql(u8, a, "--help")) {
            usageExit(0);
        } else if (std.mem.startsWith(u8, a, "-")) {
            usage();
        } else if (topic_arg == null) {
            topic_arg = a;
        } else usage();
    }
    const topic = topic_arg orelse usage();
    if (topic.len == 0) usage();

    var cfg = config.load(io, alloc, init.environ_map) catch |err| switch (err) {
        error.ConfigNotFound => fatal(
            "no kannon.properties found (searched ./kannon.properties, $XDG_CONFIG_HOME/kannon/kannon.properties, ~/.config/kannon/kannon.properties)",
            .{},
        ),
        error.MissingBootstrapServers => fatal("kannon.properties is missing required key bootstrap.servers", .{}),
        error.InvalidSecurityProtocol => fatal("invalid security.protocol (want PLAINTEXT, SSL, SASL_SSL, or SASL_PLAINTEXT)", .{}),
        error.InvalidSaslMechanism => fatal("invalid sasl.mechanism (want PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)", .{}),
        error.MissingSaslMechanism => fatal("security.protocol=SASL_* requires sasl.mechanism", .{}),
        error.MissingSaslCredentials => fatal("sasl.mechanism set but sasl.username/sasl.password missing", .{}),
        else => fatal("failed to load kannon.properties: {s}", .{@errorName(err)}),
    };
    cfg.verbose = verbose;

    var cli = client.Client.init(alloc, io, init.environ_map, &cfg);
    cli.bootstrap() catch fatalErr(&cli, "could not reach any bootstrap server");

    cli.refreshMetadata(topic) catch |err| switch (err) {
        error.TopicNotFound => fatal("topic '{s}' does not exist", .{topic}),
        error.TopicAuthorizationFailed => fatal("not authorized to read topic '{s}'", .{topic}),
        else => fatalErr(&cli, "metadata lookup failed"),
    };

    const nparts = cli.partitionCount();
    var pend = alloc.alloc(Pending, nparts) catch fatal("out of memory", .{});
    for (pend) |*p| p.* = .{};

    var stdin_buf: [64 * 1024]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buf);
    const r = &stdin_reader.interface;
    const stdin_fd = std.Io.File.stdin().handle;

    var rr: usize = 0; // round-robin cursor for unkeyed records
    var total: u64 = 0;
    const timing = init.environ_map.get("KANNON_TIME") != null;
    var t_read: u64 = 0;
    var t_flush: u64 = 0;
    var t_drain: u64 = 0;
    var timer = Lap.init(io);
    read_loop: while (true) {
        // Linger: with pending records and no stdin data within linger_ms,
        // flush rather than block indefinitely on a slow producer. Skip the
        // poll when a full line is already buffered — no read() can block.
        if (pendingBytes(pend) > 0 and std.mem.indexOfScalar(u8, r.buffered(), '\n') == null) {
            var fds = [_]std.posix.pollfd{.{ .fd = stdin_fd, .events = std.posix.POLL.IN, .revents = 0 }};
            const nready = std.posix.poll(&fds, @intCast(@min(cfg.linger_ms, std.math.maxInt(i32)))) catch 1;
            if (nready == 0) {
                flushAll(&cli, topic, pend) catch |err| produceFatal(&cli, err);
                continue;
            }
        }

        const owned = nextLine(r, alloc) catch fatal("failed reading stdin", .{}) orelse break :read_loop;
        t_read += timer.lap();
        total += 1;
        const rec = parseLine(alloc, owned, static_headers.items, total);
        // Keyed records partition by murmur2 like Kafka's default partitioner;
        // unkeyed records round-robin so every partition fills together.
        const target: usize = if (rec.key) |k| blk: {
            const h = std.hash.murmur.Murmur2_32.hashWithSeed(k, 0x9747b28c);
            break :blk (h & 0x7fffffff) % nparts;
        } else blk: {
            const t = rr;
            rr = (rr + 1) % nparts;
            break :blk t;
        };
        const p = &pend[target];
        p.records.append(alloc, rec) catch fatal("out of memory", .{});
        p.bytes += recordSize(rec);
        if (p.bytes + batch_overhead >= cfg.batch_size) {
            // Cap hit: flush every partition's pending buffer in one pipelined
            // round so all leader conns go in flight together.
            flushAll(&cli, topic, pend) catch |err| produceFatal(&cli, err);
            t_flush += timer.lap();
            if (cli.outstanding_bytes >= 96 << 20) {
                cli.produceDrainUntil(topic, 96 << 20) catch |err| produceFatal(&cli, err);
                t_drain += timer.lap();
            }
        }
    }

    flushAll(&cli, topic, pend) catch |err| produceFatal(&cli, err);
    cli.produceDrain(topic) catch |err| produceFatal(&cli, err);
    if (timing) std.debug.print("read {d}ms send {d}ms drain {d}ms conns {d}\n", .{ t_read / 1_000_000, t_flush / 1_000_000, (t_drain + timer.lap()) / 1_000_000, cli.conns.count() });

    var buf: [256]u8 = undefined;
    var w = std.Io.File.stdout().writer(io, &buf);
    w.interface.print("{d} record(s) produced to '{s}'\n", .{ total, topic }) catch {};
    w.interface.flush() catch {};
    cli.deinit();
}

/// Lap timer over the monotonic `Io` clock (replaces std.time.Timer).
const Lap = struct {
    io: std.Io,
    last: std.Io.Timestamp,
    fn init(io: std.Io) Lap {
        return .{ .io = io, .last = .now(io, .awake) };
    }
    fn lap(l: *Lap) u64 {
        const n = std.Io.Timestamp.now(l.io, .awake);
        const d = l.last.durationTo(n);
        l.last = n;
        return @intCast(@max(0, d.toNanoseconds()));
    }
};

fn pendingBytes(pend: []Pending) usize {
    var n: usize = 0;
    for (pend) |p| n += p.bytes;
    return n;
}

/// Estimated encoded size of a record: key + value + header bytes plus
/// varint framing (~16B/record, ~8B/header). Charged against batch_size so
/// encoded batches stay under the broker's ~1MiB message.max.bytes.
fn recordSize(rec: protocol.Record) usize {
    var n: usize = 16 + rec.value.len;
    if (rec.key) |k| n += k.len;
    for (rec.headers) |h| {
        n += h.key.len + 8;
        if (h.value) |v| n += v.len;
    }
    return n;
}

/// Record-batch header (61B) plus slack, charged against batch_size on
/// every flush check.
const batch_overhead = 96;

fn stripCr(s: []const u8) []const u8 {
    return if (s.len > 0 and s[s.len - 1] == '\r') s[0 .. s.len - 1] else s;
}

/// Read one line (without the trailing newline). Lines longer than the
/// reader's buffer spill into the arena. Returns null at EOF.
fn nextLine(r: *std.Io.Reader, alloc: std.mem.Allocator) !?[]const u8 {
    const maybe = r.takeDelimiter('\n') catch |err| switch (err) {
        error.ReadFailed => return error.ReadFailed,
        error.StreamTooLong => {
            var lw = std.Io.Writer.Allocating.init(alloc);
            _ = r.streamDelimiterEnding(&lw.writer, '\n') catch |e2| switch (e2) {
                error.WriteFailed => return error.OutOfMemory,
                error.ReadFailed => return error.ReadFailed,
            };
            // Consume the newline if the line was newline-terminated.
            if (r.peekByte()) |b| {
                if (b == '\n') r.toss(1);
            } else |_| {}
            return stripCr(lw.written());
        },
    };
    const line = maybe orelse return null;
    const owned = try alloc.dupe(u8, stripCr(line));
    return owned;
}

fn flushAll(c: *client.Client, topic: []const u8, pend: []Pending) !void {
    var parts: std.ArrayListUnmanaged(usize) = .empty;
    var sets: std.ArrayListUnmanaged([]const protocol.Record) = .empty;
    defer parts.deinit(c.alloc);
    defer sets.deinit(c.alloc);
    for (pend, 0..) |*p, i| {
        if (p.records.items.len == 0) continue;
        try parts.append(c.alloc, i);
        try sets.append(c.alloc, p.records.items);
    }
    if (parts.items.len == 0) return;
    try c.produceEnqueue(topic, parts.items, sets.items);
    for (parts.items) |i| {
        pend[i].records.clearRetainingCapacity();
        pend[i].bytes = 0;
    }
}

fn produceFatal(c: *client.Client, err: anyerror) noreturn {
    switch (err) {
        error.ProduceFailed, error.MetadataFailed => {
            const detail = c.errDetail();
            if (detail.len > 0) fatal("{s}", .{detail});
            fatal("produce failed", .{});
        },
        else => fatal("produce failed: {s}", .{@errorName(err)}),
    }
}
