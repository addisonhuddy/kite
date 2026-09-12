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

const batch_bytes_cap = 1 << 20; // ~1MB per partition before flush
const linger_ms: i32 = 50;

fn out(comptime fmt: []const u8, args: anytype) void {
    var buf: [1024]u8 = undefined;
    var w = std.fs.File.stderr().writer(&buf);
    w.interface.print(fmt, args) catch {};
    w.interface.flush() catch {};
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

fn usage() noreturn {
    fatal("usage: kannon <topic>   (reads records, one per line, from stdin)", .{});
}

/// Per-partition pending record buffer.
const Pending = struct {
    lines: std.ArrayListUnmanaged([]const u8) = .empty,
    bytes: usize = 0,
};

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const alloc = arena.allocator();

    const args = std.process.argsAlloc(alloc) catch fatal("out of memory", .{});
    if (args.len != 2) usage();
    const topic = args[1];
    if (topic.len == 0) usage();

    var cfg = config.load(alloc) catch |err| switch (err) {
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

    var cli = client.Client.init(alloc, &cfg);
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
    var stdin_reader = std.fs.File.stdin().reader(&stdin_buf);
    const r = &stdin_reader.interface;
    const stdin_fd = std.fs.File.stdin().handle;

    var sticky: usize = 0;
    var total: u64 = 0;
    read_loop: while (true) {
        // Linger: with pending records and no stdin data within linger_ms,
        // flush rather than block indefinitely on a slow producer.
        if (pendingBytes(pend) > 0) {
            var fds = [_]std.posix.pollfd{.{ .fd = stdin_fd, .events = std.posix.POLL.IN, .revents = 0 }};
            const nready = std.posix.poll(&fds, linger_ms) catch 1;
            if (nready == 0) {
                flushAll(&cli, topic, pend) catch |err| produceFatal(&cli, err);
                continue;
            }
        }

        const owned = nextLine(r, alloc) catch fatal("failed reading stdin", .{}) orelse break :read_loop;
        const p = &pend[sticky];
        p.lines.append(alloc, owned) catch fatal("out of memory", .{});
        p.bytes += owned.len;
        total += 1;
        if (p.bytes >= batch_bytes_cap) {
            flushPartition(&cli, topic, pend, sticky) catch |err| produceFatal(&cli, err);
            sticky = (sticky + 1) % nparts;
        }
    }

    flushAll(&cli, topic, pend) catch |err| produceFatal(&cli, err);

    var buf: [256]u8 = undefined;
    var w = std.fs.File.stdout().writer(&buf);
    w.interface.print("{d} record(s) produced to '{s}'\n", .{ total, topic }) catch {};
    w.interface.flush() catch {};
    cli.deinit();
}

fn pendingBytes(pend: []Pending) usize {
    var n: usize = 0;
    for (pend) |p| n += p.bytes;
    return n;
}

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
    for (0..pend.len) |i| try flushPartition(c, topic, pend, i);
}

fn flushPartition(c: *client.Client, topic: []const u8, pend: []Pending, i: usize) !void {
    const p = &pend[i];
    if (p.lines.items.len == 0) return;
    try c.produceToPartition(topic, i, p.lines.items);
    p.lines.clearRetainingCapacity();
    p.bytes = 0;
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
