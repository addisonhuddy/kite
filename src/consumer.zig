const std = @import("std");
const client = @import("client.zig");
const protocol = @import("protocol.zig");
const stats = @import("stats.zig");
const json = @import("json.zig");
const format_mod = @import("format.zig");

const Encoder = protocol.Encoder;
const Decoder = protocol.Decoder;

pub const Options = struct {
    pub const Start = enum { earliest, latest, offset };
    topic: []const u8,
    start: Start = .latest,
    offset: i64 = 0,
    partition: ?i32 = null,
    max_records: ?u64 = null,
    idle_ms: ?u64 = null,
    /// Record output shape; `auto` sniffs keys/headers like the producer.
    format: format_mod.Format = .auto,
    stats: ?*stats.Stats = null,
    /// Set asynchronously (e.g. by a SIGINT handler) to stop after the
    /// current fetch round.
    stop: ?*const std.atomic.Value(bool) = null,
    /// Polled between fetch rounds so a closed downstream pipe stops the
    /// consumer even when no new records arrive to trigger a write error.
    sink_closed: ?*const fn () bool = null,
};

pub const Error = error{
    PartitionNotFound,
    FetchFailed,
    /// `--offset` outside the partition's [earliest, latest] range; detail in
    /// `Client.errDetail()`.
    OffsetOutOfRange,
};

const Cursor = struct {
    pidx: i32,
    leader: i32,
    offset: i64,
};

const ListPart = struct {
    pidx: i32,
    cursor: *Cursor,
};

const FetchPart = struct {
    pidx: i32,
    cursor: *Cursor,
};

pub fn run(c: *client.Client, opts: Options, out: *std.Io.Writer) !u64 {
    var cursors = try c.alloc.alloc(Cursor, c.partitions.items.len);
    defer c.alloc.free(cursors);
    var cursor_count: usize = 0;
    for (c.partitions.items) |p| {
        if (opts.partition) |wanted| if (wanted != p.index) continue;
        cursors[cursor_count] = .{
            .pidx = p.index,
            .leader = p.leader,
            .offset = if (opts.start == .offset) opts.offset else 0,
        };
        cursor_count += 1;
    }
    if (cursor_count == 0) return error.PartitionNotFound;

    var round_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer round_arena.deinit();
    var round_alloc = round_arena.allocator();

    if (opts.start != .offset) {
        try listOffsets(c, &round_arena, opts.topic, cursors[0..cursor_count], opts.start);
        _ = round_arena.reset(.retain_capacity);
        round_alloc = round_arena.allocator();
    }

    var count: u64 = 0;
    var last_record = std.Io.Timestamp.now(c.io, .awake);
    var retry_attempts: u8 = 0;
    while (true) {
        if (opts.stop) |flag| if (flag.load(.acquire)) break;
        if (opts.max_records) |max| if (count >= max) break;
        if (opts.sink_closed) |closed| if (closed()) return error.WriteFailed;
        if (opts.idle_ms) |idle| {
            const elapsed = last_record.durationTo(std.Io.Timestamp.now(c.io, .awake));
            if (elapsed.toMilliseconds() >= idle) break;
        }

        _ = round_arena.reset(.retain_capacity);
        round_alloc = round_arena.allocator();
        var progress = false;
        var leader_seen: std.AutoHashMapUnmanaged(i32, void) = .empty;
        for (cursors[0..cursor_count]) |*cursor| {
            if (leader_seen.contains(cursor.leader)) continue;
            try leader_seen.put(round_alloc, cursor.leader, {});

            var parts: std.ArrayListUnmanaged(FetchPart) = .empty;
            for (cursors[0..cursor_count]) |*candidate|
                if (candidate.leader == cursor.leader)
                    try parts.append(round_alloc, .{ .pidx = candidate.pidx, .cursor = candidate });

            fetchLeader(c, round_alloc, opts, parts.items, out, &count, &progress) catch |err| switch (err) {
                error.RetryFetch => {
                    retry_attempts += 1;
                    if (retry_attempts >= 6) {
                        c.setErr("fetch to broker {d}: giving up after 6 attempts", .{cursor.leader});
                        return error.FetchFailed;
                    }
                    c.dropConn(client.Client.connKey(cursor.leader, 0));
                    c.sleep(@min(@as(u64, 100) << @intCast(retry_attempts - 1), 3000));
                    _ = c.refreshMetadata(opts.topic) catch {};
                    for (cursors[0..cursor_count]) |*refresh| {
                        refresh.leader = c.partitionLeader(refresh.pidx) orelse refresh.leader;
                    }
                    continue;
                },
                else => {
                    retry_attempts = 0;
                    return err;
                },
            };
            retry_attempts = 0;
            if (opts.max_records) |max| if (count >= max) break;
        }
        leader_seen.deinit(round_alloc);
        try out.flush();
        if (opts.stats) |s| s.maybeRender();
        if (progress) last_record = std.Io.Timestamp.now(c.io, .awake);
    }
    return count;
}

fn listOffsets(
    c: *client.Client,
    arena: *std.heap.ArenaAllocator,
    topic: []const u8,
    cursors: []Cursor,
    start: Options.Start,
) !void {
    const alloc = arena.allocator();
    var leaders: std.AutoHashMapUnmanaged(i32, void) = .empty;
    for (cursors) |cursor| try leaders.put(alloc, cursor.leader, {});

    var it = leaders.keyIterator();
    while (it.next()) |leader_ptr| {
        const leader = leader_ptr.*;
        var parts: std.ArrayListUnmanaged(ListPart) = .empty;
        for (cursors) |*cursor| if (cursor.leader == leader)
            try parts.append(alloc, .{ .pidx = cursor.pidx, .cursor = cursor });
        try listOffsetsLeader(c, alloc, topic, parts.items, start);
    }
    leaders.deinit(alloc);
}

fn listOffsetsLeader(
    c: *client.Client,
    alloc: std.mem.Allocator,
    topic: []const u8,
    parts: []const ListPart,
    start: Options.Start,
) !void {
    const Ctx = struct {
        topic: []const u8,
        parts: []const ListPart,
        start: Options.Start,
    };
    const body = struct {
        fn f(e: *Encoder, ctx: Ctx) protocol.ProtoError!void {
            try e.i32v(-1);
            try e.i8v(0);
            try e.compactArrayLen(1);
            try e.compactString(ctx.topic);
            try e.compactArrayLen(ctx.parts.len);
            for (ctx.parts) |part| {
                try e.i32v(part.pidx);
                try e.i32v(-1);
                try e.i64v(if (ctx.start == .earliest) -2 else -1);
                try e.tagBuffer();
            }
            try e.tagBuffer();
            try e.tagBuffer();
        }
    }.f;
    const conn = try c.connFor(parts[0].cursor.leader, client.Client.connKey(parts[0].cursor.leader, 0));
    const resp = c.sendRequestAlloc(alloc, conn, protocol.api_key.list_offsets, protocol.version.list_offsets, Ctx{
        .topic = topic,
        .parts = parts,
        .start = start,
    }, body) catch return error.RetryFetch;
    var d = Decoder.init(resp.body);
    _ = try d.i32v();
    const topics = try d.compactArrayLen();
    var ti: i64 = 0;
    while (ti < topics) : (ti += 1) {
        _ = try d.compactString();
        const nparts = try d.compactArrayLen();
        var pi: i64 = 0;
        while (pi < nparts) : (pi += 1) {
            const pidx = try d.i32v();
            const code: protocol.ErrorCode = @enumFromInt(try d.i16v());
            _ = try d.i64v();
            const offset = try d.i64v();
            _ = try d.i32v();
            try d.tagBuffer();
            if (code != .none) {
                c.setErr("list offsets partition {d}: {s}", .{ pidx, code.name() });
                return error.FetchFailed;
            }
            for (parts) |part| {
                if (part.pidx == pidx) part.cursor.offset = offset;
            }
        }
        try d.tagBuffer();
    }
    try d.tagBuffer();
}

fn fetchLeader(
    c: *client.Client,
    alloc: std.mem.Allocator,
    opts: Options,
    parts: []const FetchPart,
    out: *std.Io.Writer,
    count: *u64,
    progress: *bool,
) !void {
    const Ctx = struct {
        topic: []const u8,
        parts: []const FetchPart,
        max_wait_ms: u64,
        max_bytes: usize,
    };
    const body = struct {
        fn f(e: *Encoder, ctx: Ctx) protocol.ProtoError!void {
            try e.i32v(-1);
            try e.i32v(@intCast(ctx.max_wait_ms));
            try e.i32v(1);
            try e.i32v(@intCast(ctx.max_bytes));
            try e.i8v(0);
            try e.i32v(0);
            try e.i32v(-1);
            try e.compactArrayLen(1);
            try e.compactString(ctx.topic);
            try e.compactArrayLen(ctx.parts.len);
            for (ctx.parts) |part| {
                try e.i32v(part.pidx);
                try e.i32v(-1);
                try e.i64v(part.cursor.offset);
                try e.i32v(-1);
                try e.i64v(-1);
                try e.i32v(1 << 20);
                try e.tagBuffer();
            }
            try e.tagBuffer();
            try e.compactArrayLen(0);
            try e.compactString("");
            try e.tagBuffer();
        }
    }.f;
    const leader = parts[0].cursor.leader;
    const conn = c.connFor(leader, client.Client.connKey(leader, 0)) catch return error.RetryFetch;
    const resp = c.sendRequestAlloc(alloc, conn, protocol.api_key.fetch, protocol.version.fetch, Ctx{
        .topic = opts.topic,
        .parts = parts,
        .max_wait_ms = c.cfg.fetch_max_wait_ms,
        .max_bytes = c.cfg.fetch_max_bytes,
    }, body) catch return error.RetryFetch;

    var d = Decoder.init(resp.body);
    _ = try d.i32v();
    const top_code: protocol.ErrorCode = @enumFromInt(try d.i16v());
    _ = try d.i32v();
    if (top_code != .none) return error.RetryFetch;
    const topics = try d.compactArrayLen();
    var ti: i64 = 0;
    while (ti < topics) : (ti += 1) {
        _ = try d.compactString();
        const nparts = try d.compactArrayLen();
        var pi: i64 = 0;
        while (pi < nparts) : (pi += 1) {
            const pidx = try d.i32v();
            const code: protocol.ErrorCode = @enumFromInt(try d.i16v());
            const high_watermark = try d.i64v();
            _ = try d.i64v();
            _ = try d.i64v();
            const aborted = try d.compactArrayLen();
            if (aborted >= 0) {
                for (0..@as(usize, @intCast(aborted))) |_| {
                    _ = try d.i64v();
                    _ = try d.i64v();
                    try d.tagBuffer();
                }
            }
            _ = try d.i32v();
            const records = try d.compactBytes();
            try d.tagBuffer();
            const part = findPart(parts, pidx) orelse continue;
            if (code == .offset_out_of_range) {
                if (opts.start == .offset) {
                    try reportOffsetRange(c, alloc, opts, part.cursor);
                    return error.OffsetOutOfRange;
                }
                try resetOffset(c, alloc, opts, part.cursor);
                continue;
            }
            if (code != .none) {
                if (code.retriable()) return error.RetryFetch;
                c.setErr("fetch partition {d}: {s}", .{ pidx, code.name() });
                return error.FetchFailed;
            }
            var range = Range{};
            var next_from_batches: ?i64 = null;
            if (records) |blob| {
                var ctx = RecordSink{
                    .out = out,
                    .count = count,
                    .max = opts.max_records,
                    .range = &range,
                    .stats = opts.stats,
                    .format = opts.format,
                    .topic = opts.topic,
                    .pidx = pidx,
                };
                next_from_batches = protocol.decodeBatches(alloc, blob, &ctx, onRecord) catch |err| switch (err) {
                    error.WriteFailed => return error.WriteFailed,
                    else => {
                        c.setErr("decode fetch partition {d}: {s}", .{ pidx, @errorName(err) });
                        return error.FetchFailed;
                    },
                };
            }
            if (range.last) |last| {
                if (part.cursor.offset <= last + 1) part.cursor.offset = last + 1;
            }
            if (next_from_batches) |next| {
                if (part.cursor.offset < next) part.cursor.offset = next;
            }
            if (range.first != null) {
                progress.* = true;
                c.vlog("fetch partition {d}: offsets {d}..{d}, high watermark {d}, records bytes {d}", .{
                    pidx, range.first.?, range.last.?, high_watermark, if (records) |blob| blob.len else 0,
                });
            } else {
                c.vlog("fetch partition {d}: offsets empty..empty, high watermark {d}, records bytes {d}", .{
                    pidx, high_watermark, if (records) |blob| blob.len else 0,
                });
            }
        }
        try d.tagBuffer();
    }
    try d.tagBuffer();
}

const Range = struct {
    first: ?i64 = null,
    last: ?i64 = null,
};

const RecordSink = struct {
    out: *std.Io.Writer,
    count: *u64,
    max: ?u64,
    range: *Range,
    stats: ?*stats.Stats,
    format: format_mod.Format,
    topic: []const u8,
    pidx: i32,
};

fn writeJsonRecord(w: *std.Io.Writer, topic: []const u8, pidx: i32, offset: i64, ts: i64, rec: protocol.Record) !void {
    try w.writeAll("{\"topic\":");
    try json.writeString(w, topic);
    try w.print(",\"partition\":{d},\"offset\":{d},\"timestamp\":{d},\"key\":", .{ pidx, offset, ts });
    if (rec.key) |key| try json.writeString(w, key) else try w.writeAll("null");
    try w.writeAll(",\"headers\":[");
    for (rec.headers, 0..) |header, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeAll("{\"key\":");
        try json.writeString(w, header.key);
        try w.writeAll(",\"value\":");
        if (header.value) |value| try json.writeString(w, value) else try w.writeAll("null");
        try w.writeByte('}');
    }
    try w.writeAll("],\"value\":");
    try json.writeString(w, rec.value);
    try w.writeByte('}');
}

/// key<TAB>[h: v<TAB>]*value — the shape `auto` and `tsv` share.
fn writeTsvRecord(w: *std.Io.Writer, rec: protocol.Record) !void {
    if (rec.key) |key| try w.writeAll(key);
    try w.writeByte('\t');
    for (rec.headers) |header| {
        try w.writeAll(header.key);
        try w.writeAll(": ");
        if (header.value) |value| try w.writeAll(value);
        try w.writeByte('\t');
    }
    try w.writeAll(rec.value);
}

fn onRecord(ctx: *RecordSink, offset: i64, timestamp_ms: i64, rec: protocol.Record) !void {
    if (ctx.max) |max| if (ctx.count.* >= max) return;
    if (ctx.stats) |s| s.beforeOutput();
    switch (ctx.format) {
        .json => try writeJsonRecord(ctx.out, ctx.topic, ctx.pidx, offset, timestamp_ms, rec),
        .value => try ctx.out.writeAll(rec.value),
        .tsv => try writeTsvRecord(ctx.out, rec),
        .auto, .csv => if (rec.key == null and rec.headers.len == 0)
            try ctx.out.writeAll(rec.value)
        else
            try writeTsvRecord(ctx.out, rec),
    }
    try ctx.out.writeByte('\n');
    ctx.count.* += 1;
    if (ctx.stats) |s| {
        s.add(1, rec.value.len + if (rec.key) |key| key.len else 0);
        s.noteOffset(ctx.pidx, offset);
    }
    ctx.range.first = ctx.range.first orelse offset;
    ctx.range.last = offset;
}

fn findPart(parts: []const FetchPart, pidx: i32) ?FetchPart {
    for (parts) |part| if (part.pidx == pidx) return part;
    return null;
}

fn resetOffset(c: *client.Client, alloc: std.mem.Allocator, opts: Options, cursor: *Cursor) !void {
    const start: Options.Start = if (opts.start == .offset) .earliest else opts.start;
    const part = [_]ListPart{.{ .pidx = cursor.pidx, .cursor = cursor }};
    try listOffsetsLeader(c, alloc, opts.topic, &part, start);
    c.vlog("partition {d}: offset out of range; reset to {d}", .{ cursor.pidx, cursor.offset });
}

/// Fill `Client.errDetail()` with the partition's valid offset range so the
/// user can pick an offset that exists instead of silently replaying.
fn reportOffsetRange(c: *client.Client, alloc: std.mem.Allocator, opts: Options, cursor: *Cursor) !void {
    const requested = cursor.offset;
    var probe = cursor.*;
    const part = [_]ListPart{.{ .pidx = probe.pidx, .cursor = &probe }};
    listOffsetsLeader(c, alloc, opts.topic, &part, .earliest) catch {
        c.setErr("offset {d} is out of range for partition {d}", .{ requested, cursor.pidx });
        return;
    };
    const earliest = probe.offset;
    listOffsetsLeader(c, alloc, opts.topic, &part, .latest) catch {
        c.setErr("offset {d} is out of range for partition {d}", .{ requested, cursor.pidx });
        return;
    };
    const latest = probe.offset;
    if (earliest == latest)
        c.setErr("offset {d} is out of range for partition {d}: partition is empty (next offset {d})", .{
            requested, cursor.pidx, latest,
        })
    else
        c.setErr("offset {d} is out of range for partition {d}: valid offsets are {d}..{d} (next offset {d})", .{
            requested, cursor.pidx, earliest, latest - 1, latest,
        });
}

test "json record output escapes and includes metadata" {
    var buf: [512]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    const headers = [_]protocol.Header{ .{ .key = "h", .value = "v" }, .{ .key = "n", .value = null } };
    try writeJsonRecord(&w, "t", 2, 41, 1700000000000, .{ .key = "k\"q", .value = "line\nbreak\ttab", .headers = &headers });
    try std.testing.expectEqualStrings(
        "{\"topic\":\"t\",\"partition\":2,\"offset\":41,\"timestamp\":1700000000000,\"key\":\"k\\\"q\",\"headers\":[{\"key\":\"h\",\"value\":\"v\"},{\"key\":\"n\",\"value\":null}],\"value\":\"line\\nbreak\\ttab\"}",
        w.buffered(),
    );
}

test "json record output without key or headers" {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeJsonRecord(&w, "t", 0, 0, -1, .{ .value = "x" });
    try std.testing.expectEqualStrings(
        "{\"topic\":\"t\",\"partition\":0,\"offset\":0,\"timestamp\":-1,\"key\":null,\"headers\":[],\"value\":\"x\"}",
        w.buffered(),
    );
}

test "text record output honors value and tsv formats" {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    var count: u64 = 0;
    var range = Range{};
    const headers = [_]protocol.Header{.{ .key = "h", .value = "v" }};
    var sink = RecordSink{
        .out = &w,
        .count = &count,
        .max = null,
        .range = &range,
        .stats = null,
        .format = .value,
        .topic = "t",
        .pidx = 0,
    };
    try onRecord(&sink, 0, 0, .{ .key = "k", .value = "val", .headers = &headers });
    try std.testing.expectEqualStrings("val\n", w.buffered());

    w = .fixed(&buf);
    sink.format = .tsv;
    try onRecord(&sink, 1, 0, .{ .value = "v2", .headers = &headers });
    try std.testing.expectEqualStrings("\th: v\tv2\n", w.buffered());

    w = .fixed(&buf);
    sink.format = .auto;
    try onRecord(&sink, 2, 0, .{ .value = "plain" });
    try onRecord(&sink, 3, 0, .{ .key = "k", .value = "v3" });
    try std.testing.expectEqualStrings("plain\nk\tv3\n", w.buffered());
}
