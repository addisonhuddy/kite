//! kite — ultra-lightweight Kafka CLI.
//! `kite <topic> < file` sends each stdin line as one record value.

const std = @import("std");
const config = @import("config.zig");
const client = @import("client.zig");
const cli_args = @import("cli.zig");
const protocol = @import("protocol.zig");
const transport = @import("transport.zig");
const scram = @import("scram.zig");
const csv = @import("csv.zig");
const consumer = @import("consumer.zig");
const install = @import("install.zig");
const term = @import("term.zig");
const stats_mod = @import("stats.zig");

// unused-import anchors so `zig build test` covers every module
comptime {
    _ = config;
    _ = client;
    _ = cli_args;
    _ = protocol;
    _ = transport;
    _ = scram;
    _ = csv;
    _ = consumer;
    _ = install;
    _ = term;
    _ = stats_mod;
}

// Panics print just the message — pulls in no DWARF/stack-trace machinery.
pub const panic = std.debug.simple_panic;

fn out(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

fn fatal(comptime fmt: []const u8, args: anytype) noreturn {
    if (term.color.enabled)
        out(term.red ++ "kite:" ++ term.reset ++ " " ++ fmt ++ "\n", args)
    else
        out("kite: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

fn fatalErr(c: *const client.Client, comptime fmt: []const u8) noreturn {
    const detail = c.errDetail();
    if (detail.len > 0)
        fatal(fmt ++ ": {s}", .{detail})
    else
        fatal(fmt, .{});
}

fn writeText(init: std.process.Init, file: std.Io.File, text: []const u8) void {
    var buf: [4096]u8 = undefined;
    var w = file.writer(init.io, &buf);
    w.interface.writeAll(text) catch {};
    w.interface.flush() catch {};
}

fn parseFatal(init: std.process.Init, message: []const u8, usage_text: []const u8) noreturn {
    var buf: [4096]u8 = undefined;
    var w = std.Io.File.stderr().writer(init.io, &buf);
    if (term.color.enabled)
        w.interface.print("{s}kite:{s} {s}\n{s}", .{ term.red, term.reset, message, usage_text }) catch {}
    else
        w.interface.print("kite: {s}\n{s}", .{ message, usage_text }) catch {};
    w.interface.flush() catch {};
    std.process.exit(1);
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
            headers.append(alloc, cli_args.parseHeaderArg(f) catch
                fatal("line {d}: malformed header '{s}' (want 'name: value')", .{ lineno, f })) catch
                fatal("out of memory", .{});
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
    term.color = .{ .enabled = term.detect(io, std.Io.File.stderr(), init.environ_map) };

    const args = init.minimal.args.toSlice(alloc) catch fatal("out of memory", .{});
    if (args.len > 1 and std.mem.eql(u8, args[1], "consume")) {
        runConsume(init, args[2..], alloc);
        return;
    }
    if (args.len > 1 and std.mem.eql(u8, args[1], "install")) {
        install.run(init, args[2..], alloc);
        return;
    }
    const parsed = cli_args.parseProduce(alloc, args[1..]);
    const produce = switch (parsed) {
        .help => {
            if (term.detect(io, std.Io.File.stdout(), init.environ_map)) {
                term.color.enabled = true;
                const page = term.renderHelp(alloc, cli_args.produce_help) catch fatal("out of memory", .{});
                writeText(init, std.Io.File.stdout(), page);
            } else writeText(init, std.Io.File.stdout(), cli_args.produce_help);
            return;
        },
        .err => |message| parseFatal(init, message, cli_args.produce_usage),
        .ok => |value| value,
    };
    const topic = produce.topic;
    const static_headers = produce.headers;
    const verbose = produce.verbose;
    const csv_mode = produce.csv;
    const csv_key_col = produce.key_col;

    var cfg = config.load(io, alloc, init.environ_map) catch |err| switch (err) {
        error.ConfigNotFound => fatal(
            "no kite.properties found (searched ./kite.properties, $XDG_CONFIG_HOME/kite/kite.properties, ~/.config/kite/kite.properties)",
            .{},
        ),
        error.MissingBootstrapServers => fatal("kite.properties is missing required key bootstrap.servers", .{}),
        error.InvalidSecurityProtocol => fatal("invalid security.protocol (want PLAINTEXT, SSL, SASL_SSL, or SASL_PLAINTEXT)", .{}),
        error.InvalidSaslMechanism => fatal("invalid sasl.mechanism (want PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)", .{}),
        error.MissingSaslMechanism => fatal("security.protocol=SASL_* requires sasl.mechanism", .{}),
        error.MissingSaslCredentials => fatal("sasl.mechanism set but sasl.username/sasl.password missing", .{}),
        else => fatal("failed to load kite.properties: {s}", .{@errorName(err)}),
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

    var cols: [][]const u8 = &.{};
    var key_idx: ?usize = null;
    if (csv_mode) {
        const hdr = csv.nextRow(r, alloc) catch fatal("failed reading stdin", .{}) orelse
            fatal("empty csv input (no header row)", .{});
        cols = csv.splitFields(alloc, hdr) catch fatal("malformed csv header row", .{});
        if (cols.len > 0) cols[0] = csv.stripBom(cols[0]);
        if (csv_key_col) |kc| {
            for (cols, 0..) |c, i| {
                if (std.mem.eql(u8, c, kc)) key_idx = i;
            }
            if (key_idx == null) fatal("--key '{s}': no such csv column", .{kc});
        }
    }

    var rr: usize = 0; // round-robin cursor for unkeyed records
    var total: u64 = 0;
    const timing = init.environ_map.get("KITE_TIME") != null;
    var t_read: u64 = 0;
    var t_flush: u64 = 0;
    var t_drain: u64 = 0;
    var timer = Lap.init(io);
    var stats = stats_mod.Stats.init(io, topic, (std.Io.File.stderr().isTty(io) catch false) and !verbose);
    read_loop: while (true) {
        // Linger: with pending records and no stdin data within linger_ms,
        // flush rather than block indefinitely on a slow producer. Skip the
        // poll when a full line is already buffered — no read() can block.
        const ready = if (csv_mode)
            csv.rowReady(r)
        else
            std.mem.indexOfScalar(u8, r.buffered(), '\n') != null;
        if (pendingBytes(pend) > 0 and !ready) {
            var fds = [_]std.posix.pollfd{.{ .fd = stdin_fd, .events = std.posix.POLL.IN, .revents = 0 }};
            const nready = std.posix.poll(&fds, @intCast(@min(cfg.linger_ms, std.math.maxInt(i32)))) catch 1;
            if (nready == 0) {
                flushAll(&cli, topic, pend) catch |err| produceFatal(&cli, err);
                if (cli.last_offset) |off| stats.noteOffset(off);
                stats.maybeRender();
                continue;
            }
        }

        const rec = if (csv_mode) blk: {
            const row = csv.nextRow(r, alloc) catch fatal("failed reading stdin", .{}) orelse
                break :read_loop;
            break :blk csvRecord(alloc, row, cols, key_idx, static_headers, total + 1);
        } else blk: {
            const owned = nextLine(r, alloc) catch fatal("failed reading stdin", .{}) orelse
                break :read_loop;
            break :blk parseLine(alloc, owned, static_headers, total + 1);
        };
        t_read += timer.lap();
        total += 1;
        stats.add(1, recordSize(rec));
        stats.maybeRender();
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
            if (cli.last_offset) |off| stats.noteOffset(off);
            t_flush += timer.lap();
            if (cli.outstanding_bytes >= 96 << 20) {
                cli.produceDrainUntil(topic, 96 << 20) catch |err| produceFatal(&cli, err);
                if (cli.last_offset) |off| stats.noteOffset(off);
                stats.maybeRender();
                t_drain += timer.lap();
            }
        }
    }

    flushAll(&cli, topic, pend) catch |err| produceFatal(&cli, err);
    cli.produceDrain(topic) catch |err| produceFatal(&cli, err);
    if (cli.last_offset) |off| stats.noteOffset(off);
    stats.finish();
    if (timing) std.debug.print("read {d}ms send {d}ms drain {d}ms conns {d}\n", .{ t_read / 1_000_000, t_flush / 1_000_000, (t_drain + timer.lap()) / 1_000_000, cli.conns.count() });

    var buf: [256]u8 = undefined;
    var w = std.Io.File.stdout().writer(io, &buf);
    w.interface.print("{d} record(s) produced to '{s}'\n", .{ total, topic }) catch {};
    w.interface.flush() catch {};
    cli.deinit();
}

fn runConsume(init: std.process.Init, args: []const []const u8, alloc: std.mem.Allocator) noreturn {
    const parsed = cli_args.parseConsume(alloc, args);
    const consume = switch (parsed) {
        .help => {
            if (term.detect(init.io, std.Io.File.stdout(), init.environ_map)) {
                term.color.enabled = true;
                const page = term.renderHelp(alloc, cli_args.consume_help) catch fatal("out of memory", .{});
                writeText(init, std.Io.File.stdout(), page);
            } else writeText(init, std.Io.File.stdout(), cli_args.consume_help);
            std.process.exit(0);
        },
        .err => |message| parseFatal(init, message, cli_args.consume_usage),
        .ok => |value| value,
    };
    const topic_name = consume.topic;
    const verbose = consume.verbose;

    var cfg = config.load(init.io, alloc, init.environ_map) catch |err| switch (err) {
        error.ConfigNotFound => fatal(
            "no kite.properties found (searched ./kite.properties, $XDG_CONFIG_HOME/kite/kite.properties, ~/.config/kite/kite.properties)",
            .{},
        ),
        error.MissingBootstrapServers => fatal("kite.properties is missing required key bootstrap.servers", .{}),
        error.InvalidSecurityProtocol => fatal("invalid security.protocol (want PLAINTEXT, SSL, SASL_SSL, or SASL_PLAINTEXT)", .{}),
        error.InvalidSaslMechanism => fatal("invalid sasl.mechanism (want PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)", .{}),
        error.MissingSaslMechanism => fatal("security.protocol=SASL_* requires sasl.mechanism", .{}),
        error.MissingSaslCredentials => fatal("sasl.mechanism set but sasl.username/sasl.password missing", .{}),
        else => fatal("failed to load kite.properties: {s}", .{@errorName(err)}),
    };
    cfg.verbose = verbose;
    var cli = client.Client.init(alloc, init.io, init.environ_map, &cfg);
    cli.bootstrap() catch fatalErr(&cli, "could not reach any bootstrap server");
    cli.refreshMetadata(topic_name) catch |err| switch (err) {
        error.TopicNotFound => fatal("topic '{s}' does not exist", .{topic_name}),
        error.TopicAuthorizationFailed => fatal("not authorized to read topic '{s}'", .{topic_name}),
        else => fatalErr(&cli, "metadata lookup failed"),
    };

    var stdout_buf: [64 * 1024]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(init.io, &stdout_buf);
    var stats = stats_mod.Stats.init(init.io, topic_name, (std.Io.File.stderr().isTty(init.io) catch false) and
        !(std.Io.File.stdout().isTty(init.io) catch false) and !verbose);
    const consumed = consumer.run(&cli, .{
        .topic = topic_name,
        .start = consume.start,
        .offset = consume.offset,
        .partition = consume.partition,
        .max_records = consume.max_records,
        .idle_ms = consume.idle_ms,
        .stats = &stats,
    }, &stdout.interface) catch |err| switch (err) {
        error.PartitionNotFound => fatal("partition {d} not found in topic '{s}'", .{ consume.partition orelse -1, topic_name }),
        error.FetchFailed => fatalErr(&cli, "consume failed"),
        else => fatalErr(&cli, "consume failed"),
    };
    stdout.interface.flush() catch {};
    if (consume.max_records != null or consume.idle_ms != null)
        std.debug.print("{d} record(s) consumed from '{s}'\n", .{ consumed, topic_name });
    if (consume.max_records != null or consume.idle_ms != null) stats.finish();
    cli.deinit();
    std.process.exit(0);
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

/// Turn a CSV row into a record: JSON object value, optional column key.
fn csvRecord(
    alloc: std.mem.Allocator,
    row: []u8,
    cols: []const []const u8,
    key_idx: ?usize,
    static_headers: []const protocol.Header,
    rowno: u64,
) protocol.Record {
    const fields = csv.splitFields(alloc, row) catch
        fatal("csv row {d}: unterminated quoted field", .{rowno});
    if (fields.len != cols.len)
        fatal("csv row {d}: expected {d} field(s), got {d}", .{ rowno, cols.len, fields.len });
    var jw = std.Io.Writer.Allocating.init(alloc);
    csv.rowJson(&jw.writer, cols, fields) catch fatal("out of memory", .{});
    return .{
        .key = if (key_idx) |ki| fields[ki] else null,
        .value = jw.written(),
        .headers = static_headers,
    };
}

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
