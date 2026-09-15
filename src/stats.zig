const std = @import("std");
const term = @import("term.zig");
pub fn fmtCount(value: u64, buf: []u8) []const u8 {
    if (value < 1000) return std.fmt.bufPrint(buf, "{d}", .{value}) catch buf[0..0];
    if (value < 1_000_000) return std.fmt.bufPrint(buf, "{d:.1}k", .{@as(f64, @floatFromInt(value)) / 1000.0}) catch buf[0..0];
    if (value < 1_000_000_000) return std.fmt.bufPrint(buf, "{d:.1}M", .{@as(f64, @floatFromInt(value)) / 1_000_000.0}) catch buf[0..0];
    return std.fmt.bufPrint(buf, "{d:.1}G", .{@as(f64, @floatFromInt(value)) / 1_000_000_000.0}) catch buf[0..0];
}

pub fn fmtBytes(value: u64, buf: []u8) []const u8 {
    if (value < 1024) return std.fmt.bufPrint(buf, "{d} B", .{value}) catch buf[0..0];
    if (value < 1024 * 1024) return std.fmt.bufPrint(buf, "{d:.1} KiB", .{@as(f64, @floatFromInt(value)) / 1024.0}) catch buf[0..0];
    if (value < 1024 * 1024 * 1024) return std.fmt.bufPrint(buf, "{d:.1} MiB", .{@as(f64, @floatFromInt(value)) / (1024.0 * 1024.0)}) catch buf[0..0];
    return std.fmt.bufPrint(buf, "{d:.1} GiB", .{@as(f64, @floatFromInt(value)) / (1024.0 * 1024.0 * 1024.0)}) catch buf[0..0];
}

pub const Stats = struct {
    io: std.Io,
    topic: []const u8,
    started: std.Io.Timestamp,
    last_render: std.Io.Timestamp,
    win_start: std.Io.Timestamp,
    win_records: u64 = 0,
    win_bytes: u64 = 0,
    records: u64 = 0,
    bytes: u64 = 0,
    last_offset: ?i64 = null,
    live: bool,
    last_rate: f64 = 0,
    last_byte_rate: f64 = 0,

    pub fn init(io: std.Io, topic: []const u8, live: bool) Stats {
        const now = std.Io.Timestamp.now(io, .awake);
        return .{ .io = io, .topic = topic, .started = now, .last_render = now, .win_start = now, .live = live };
    }

    pub fn add(s: *Stats, n_records: u64, n_bytes: u64) void {
        s.records += n_records;
        s.bytes += n_bytes;
        s.win_records += n_records;
        s.win_bytes += n_bytes;
    }

    pub fn noteOffset(s: *Stats, off: i64) void {
        if (s.last_offset == null or off > s.last_offset.?) s.last_offset = off;
    }

    fn rates(s: *Stats, now: std.Io.Timestamp) struct { records: f64, bytes: f64 } {
        const elapsed = s.win_start.durationTo(now).toNanoseconds();
        if (elapsed >= 1_000_000_000) {
            s.last_rate = @as(f64, @floatFromInt(s.win_records)) / (@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0);
            s.last_byte_rate = @as(f64, @floatFromInt(s.win_bytes)) / (@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0);
            s.win_records = 0;
            s.win_bytes = 0;
            s.win_start = now;
        } else if (elapsed >= 100_000_000) {
            s.last_rate = @as(f64, @floatFromInt(s.win_records)) / (@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0);
            s.last_byte_rate = @as(f64, @floatFromInt(s.win_bytes)) / (@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0);
        }
        return .{ .records = s.last_rate, .bytes = s.last_byte_rate };
    }

    pub fn maybeRender(s: *Stats) void {
        if (!s.live) return;
        const now = std.Io.Timestamp.now(s.io, .awake);
        if (s.last_render.durationTo(now).toNanoseconds() < 250_000_000) return;
        s.last_render = now;
        const rate = s.rates(now);
        var b1: [32]u8 = undefined;
        var b2: [32]u8 = undefined;
        var b3: [32]u8 = undefined;
        var offset_buf: [48]u8 = undefined;
        var line: [256]u8 = undefined;
        const count = fmtCount(s.records, &b1);
        const msg_rate = fmtCount(@intFromFloat(@max(0, rate.records)), &b2);
        const byte_rate = fmtBytes(@intFromFloat(@max(0, rate.bytes)), &b3);
        const elapsed = s.started.durationTo(now).toNanoseconds();
        const offset = if (s.last_offset) |off| std.fmt.bufPrint(&offset_buf, "  offset {d}", .{off}) catch "" else "";
        const text = if (term.color.enabled) blk: {
            break :blk std.fmt.bufPrint(&line, "\r\x1b[K{s}{s}{s}  {s}{s}{s} msgs  {s}{s}{s} msg/s  {s}{s}{s}/s{s}  {d:.1}s", .{
                term.cyan, s.topic,                                            term.reset,
                term.bold, count,                                              term.reset,
                term.bold, msg_rate,                                           term.reset,
                term.bold, byte_rate,                                          term.reset,
                offset,    @as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0,
            }) catch return;
        } else blk: {
            break :blk std.fmt.bufPrint(&line, "\r\x1b[K{s}  {s} msgs  {s} msg/s  {s}/s{s}  {d:.1}s", .{
                s.topic, count, msg_rate, byte_rate, offset, @as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0,
            }) catch return;
        };
        var buf: [512]u8 = undefined;
        var w = std.Io.File.stderr().writer(s.io, &buf);
        w.interface.writeAll(text) catch {};
        w.interface.flush() catch {};
    }

    pub fn finish(s: *Stats) void {
        const now = std.Io.Timestamp.now(s.io, .awake);
        if (s.live) {
            var clear_buf: [64]u8 = undefined;
            var w = std.Io.File.stderr().writer(s.io, &clear_buf);
            w.interface.writeAll("\r\x1b[K") catch {};
            w.interface.flush() catch {};
        }
        const elapsed = s.started.durationTo(now).toNanoseconds();
        const secs = @as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0;
        const rate = if (secs > 0) @as(f64, @floatFromInt(s.records)) / secs else 0;
        const byte_rate = if (secs > 0) @as(f64, @floatFromInt(s.bytes)) / secs else 0;
        var bytes_buf: [32]u8 = undefined;
        var rate_buf: [32]u8 = undefined;
        var brate_buf: [32]u8 = undefined;
        const total = fmtBytes(s.bytes, &bytes_buf);
        const msg_rate = fmtCount(@intFromFloat(@max(0, rate)), &rate_buf);
        const bytes_rate = fmtBytes(@intFromFloat(@max(0, byte_rate)), &brate_buf);
        var out_buf: [512]u8 = undefined;
        var out = std.Io.File.stderr().writer(s.io, &out_buf);
        if (term.color.enabled) {
            if (s.last_offset) |off|
                out.interface.print("{s}{s}{s} in {d:.1}s ({s}{s}{s} msg/s, {s}{s}{s}/s), last offset {d}\n", .{
                    term.bold, total, term.reset, secs, term.bold, msg_rate, term.reset, term.bold, bytes_rate, term.reset, off,
                }) catch {}
            else
                out.interface.print("{s}{s}{s} in {d:.1}s ({s}{s}{s} msg/s, {s}{s}{s}/s)\n", .{
                    term.bold, total, term.reset, secs, term.bold, msg_rate, term.reset, term.bold, bytes_rate, term.reset,
                }) catch {};
        } else if (s.last_offset) |off|
            out.interface.print("{s} in {d:.1}s ({s} msg/s, {s}/s), last offset {d}\n", .{ total, secs, msg_rate, bytes_rate, off }) catch {}
        else
            out.interface.print("{s} in {d:.1}s ({s} msg/s, {s}/s)\n", .{ total, secs, msg_rate, bytes_rate }) catch {};
        out.interface.flush() catch {};
    }
};

test "formats counts and bytes" {
    var count: [32]u8 = undefined;
    var bytes: [32]u8 = undefined;
    try std.testing.expectEqualStrings("1.2k", fmtCount(1234, &count));
    try std.testing.expectEqualStrings("1.5M", fmtCount(1_500_000, &count));
    try std.testing.expectEqualStrings("1.0 KiB", fmtBytes(1024, &bytes));
    try std.testing.expectEqualStrings("1.0 MiB", fmtBytes(1024 * 1024, &bytes));
}
