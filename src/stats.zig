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

/// Live progress on stderr plus a final summary. `live` turns on the
/// self-updating status line; `clear_before_output` additionally erases it
/// before each record is written so it never interleaves with data when
/// stdout is the same terminal.
pub const Stats = struct {
    io: std.Io,
    topic: []const u8,
    /// Short description of what is being waited for, e.g. "from latest".
    waiting_hint: []const u8 = "",
    started: std.Io.Timestamp,
    last_render: std.Io.Timestamp,
    win_start: std.Io.Timestamp,
    win_records: u64 = 0,
    win_bytes: u64 = 0,
    records: u64 = 0,
    bytes: u64 = 0,
    offsets: std.ArrayListUnmanaged(PartOffset) = .empty,
    live: bool,
    /// Aligned end-of-run block instead of the one-line summary (TTY only).
    pretty: bool = false,
    clear_before_output: bool = false,
    shown: bool = false,
    last_rate: f64 = 0,
    last_byte_rate: f64 = 0,

    pub const PartOffset = struct { pidx: i32, offset: i64 };

    pub fn init(io: std.Io, topic: []const u8, live: bool) Stats {
        const now = std.Io.Timestamp.now(io, .awake);
        return .{ .io = io, .topic = topic, .started = now, .last_render = now, .win_start = now, .live = live };
    }

    pub fn deinit(s: *Stats) void {
        s.offsets.deinit(std.heap.page_allocator);
    }

    pub fn add(s: *Stats, n_records: u64, n_bytes: u64) void {
        s.records += n_records;
        s.bytes += n_bytes;
        s.win_records += n_records;
        s.win_bytes += n_bytes;
    }

    /// Record the highest offset seen for `pidx`.
    pub fn noteOffset(s: *Stats, pidx: i32, off: i64) void {
        var i: usize = 0;
        while (i < s.offsets.items.len and s.offsets.items[i].pidx < pidx) i += 1;
        if (i < s.offsets.items.len and s.offsets.items[i].pidx == pidx) {
            if (off > s.offsets.items[i].offset) s.offsets.items[i].offset = off;
            return;
        }
        s.offsets.insert(std.heap.page_allocator, i, .{ .pidx = pidx, .offset = off }) catch {};
    }

    pub fn elapsedSecs(s: *const Stats) f64 {
        const now = std.Io.Timestamp.now(s.io, .awake);
        return @as(f64, @floatFromInt(s.started.durationTo(now).toNanoseconds())) / 1_000_000_000.0;
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

    /// Erase the live status line (no-op if none is showing) so the caller can
    /// print its own summary lines before `finish`.
    pub fn clearLine(s: *Stats) void {
        if (!s.shown) return;
        std.debug.print("\r\x1b[K", .{});
        s.shown = false;
    }

    /// Call before writing a record to stdout so the status line does not
    /// end up glued to the data on a shared terminal.
    pub fn beforeOutput(s: *Stats) void {
        if (s.live and s.clear_before_output) s.clearLine();
    }

    fn maxOffset(s: *const Stats) ?i64 {
        var best: ?i64 = null;
        for (s.offsets.items) |po| if (best == null or po.offset > best.?) {
            best = po.offset;
        };
        return best;
    }

    pub fn maybeRender(s: *Stats) void {
        if (!s.live) return;
        const now = std.Io.Timestamp.now(s.io, .awake);
        if (s.shown and s.last_render.durationTo(now).toNanoseconds() < 250_000_000) return;
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
        const secs = @as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0;
        const offset = if (s.maxOffset()) |off| std.fmt.bufPrint(&offset_buf, "  offset {d}", .{off}) catch "" else "";
        const c = term.color.enabled;
        const cyan = if (c) term.cyan else "";
        const bold = if (c) term.bold else "";
        const dim = if (c) term.dim else "";
        const reset = if (c) term.reset else "";
        const text = if (s.records == 0)
            std.fmt.bufPrint(&line, "\r\x1b[K{s}{s}{s}  {s}waiting for records{s}{s}{s}  {d:.1}s  {s}Ctrl-C to stop{s}", .{
                cyan, s.topic, reset, dim, s.waiting_hint, spinner(elapsed), reset, secs, dim, reset,
            }) catch return
        else
            std.fmt.bufPrint(&line, "\r\x1b[K{s}{s}{s}  {s}{s:>6}{s} msgs  {s}{s:>6}{s} msg/s  {s}{s:>10}{s}/s{s}  {d:.1}s", .{
                cyan, s.topic, reset, bold, count, reset, bold, msg_rate, reset, bold, byte_rate, reset, offset, secs,
            }) catch return;
        std.debug.print("{s}", .{text});
        s.shown = true;
    }

    fn spinner(elapsed_ns: i128) []const u8 {
        const frames = [_][]const u8{ "   ", ".  ", ".. ", "..." };
        const idx: usize = @intCast(@divTrunc(elapsed_ns, 400_000_000) & 3);
        return frames[idx];
    }

    /// Erase the live line and print the final throughput summary:
    /// `<bytes> in <secs>s (<msg/s>, <bytes/s>), last offset(s) ...`.
    pub fn finish(s: *Stats) void {
        const now = std.Io.Timestamp.now(s.io, .awake);
        s.clearLine();
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
        var line: [1024]u8 = undefined;
        var out: std.Io.Writer = .fixed(&line);
        const bold = if (term.color.enabled) term.bold else "";
        const reset = if (term.color.enabled) term.reset else "";
        out.print("{s}{s}{s} in {d:.2}s ({s}{s}{s} msg/s, {s}{s}{s}/s)", .{
            bold, total, reset, secs, bold, msg_rate, reset, bold, bytes_rate, reset,
        }) catch return;
        s.writeOffsets(&out);
        out.writeByte('\n') catch return;
        std.debug.print("{s}", .{out.buffered()});
    }

    pub const Mark = enum { ok, empty, interrupted };

    fn styled(w: *std.Io.Writer, color: bool, code: []const u8, text: []const u8) void {
        if (color) w.writeAll(code) catch {};
        w.writeAll(text) catch {};
        if (color) w.writeAll(term.reset) catch {};
    }

    pub fn writeLabel(w: *std.Io.Writer, color: bool, name: []const u8) void {
        w.writeAll("  ") catch {};
        styled(w, color, term.dim, name);
        w.splatByteAll(' ', 10 - name.len) catch {};
    }

    /// Aligned end-of-run block for a terminal:
    /// `✓ N records <verb> <topic>  <detail>`, then data/rate/offsets rows
    /// whose values line up in a fixed 10-char label column.
    pub fn writeSummary(
        s: *Stats,
        w: *std.Io.Writer,
        verb: []const u8,
        detail: []const u8,
        reason: []const u8,
        mark: Mark,
        color: bool,
    ) void {
        const now = std.Io.Timestamp.now(s.io, .awake);
        const secs = @as(f64, @floatFromInt(s.started.durationTo(now).toNanoseconds())) / 1_000_000_000.0;
        const rate = if (secs > 0) @as(f64, @floatFromInt(s.records)) / secs else 0;
        const byte_rate = if (secs > 0) @as(f64, @floatFromInt(s.bytes)) / secs else 0;
        var bytes_buf: [32]u8 = undefined;
        var rate_buf: [32]u8 = undefined;
        var brate_buf: [32]u8 = undefined;
        var count_buf: [32]u8 = undefined;
        const total = fmtBytes(s.bytes, &bytes_buf);
        const msg_rate = fmtCount(@intFromFloat(@max(0, rate)), &rate_buf);
        const bytes_rate = fmtBytes(@intFromFloat(@max(0, byte_rate)), &brate_buf);
        const count = fmtCount(s.records, &count_buf);

        switch (mark) {
            .ok => styled(w, color, term.green, "\u{2713}"),
            .empty => styled(w, color, term.dim, "\u{25cb}"),
            .interrupted => styled(w, color, term.yellow, "\u{25a0}"),
        }
        w.writeByte(' ') catch {};
        styled(w, color, term.bold, count);
        w.print(" record{s} {s} ", .{ if (s.records == 1) "" else "s", verb }) catch {};
        styled(w, color, term.cyan, s.topic);
        if (detail.len > 0) w.print("  {s}", .{detail}) catch {};
        if (reason.len > 0) {
            w.writeAll("  ") catch {};
            styled(w, color, term.dim, reason);
        }
        w.writeByte('\n') catch {};

        writeLabel(w, color, "data");
        w.print("{s} in {d:.2}s\n", .{ total, secs }) catch {};
        writeLabel(w, color, "rate");
        w.print("{s} msg/s  {s}/s\n", .{ msg_rate, bytes_rate }) catch {};
        if (s.offsets.items.len > 0) {
            writeLabel(w, color, "offsets");
            s.writeOffsetsPretty(w);
            w.writeByte('\n') catch {};
        }
    }

    /// `p0=1` / `p0=83  p1=83 ...` / `12 partitions, max 1234`.
    pub fn writeOffsetsPretty(s: *Stats, w: *std.Io.Writer) void {
        if (s.offsets.items.len == 1) {
            const po = s.offsets.items[0];
            w.print("p{d}={d}", .{ po.pidx, po.offset }) catch {};
        } else if (s.offsets.items.len <= 8) {
            for (s.offsets.items, 0..) |po, i| {
                w.print("{s}p{d}={d}", .{ if (i == 0) "" else "  ", po.pidx, po.offset }) catch {};
            }
        } else {
            w.print("{d} partitions, max {d}", .{ s.offsets.items.len, s.maxOffset().? }) catch {};
        }
    }

    fn writeOffsets(s: *Stats, w: *std.Io.Writer) void {
        if (s.offsets.items.len == 0) return;
        if (s.offsets.items.len == 1) {
            const po = s.offsets.items[0];
            w.print(", last offset {d} (partition {d})", .{ po.offset, po.pidx }) catch {};
            return;
        }
        if (s.offsets.items.len <= 8) {
            w.writeAll(", last offsets") catch {};
            for (s.offsets.items, 0..) |po, i| {
                w.print("{s} p{d}={d}", .{ if (i == 0) "" else ",", po.pidx, po.offset }) catch {};
            }
        } else {
            w.print(", {d} partitions, max offset {d}", .{ s.offsets.items.len, s.maxOffset().? }) catch {};
        }
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

test "tracks last offset per partition" {
    var st = Stats.init(std.testing.io, "t", false);
    defer st.deinit();
    st.noteOffset(1, 5);
    st.noteOffset(0, 9);
    st.noteOffset(1, 3);
    st.noteOffset(1, 7);
    try std.testing.expectEqual(@as(usize, 2), st.offsets.items.len);
    try std.testing.expectEqual(@as(?i64, 9), st.maxOffset());
    var buf: [128]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    st.writeOffsets(&w);
    try std.testing.expectEqualStrings(", last offsets p0=9, p1=7", w.buffered());
}

test "pretty offsets formatting" {
    var st = Stats.init(std.testing.io, "t", false);
    defer st.deinit();
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);

    st.noteOffset(0, 1);
    st.writeOffsetsPretty(&w);
    try std.testing.expectEqualStrings("p0=1", w.buffered());

    st.noteOffset(1, 83);
    w = .fixed(&buf);
    st.writeOffsetsPretty(&w);
    try std.testing.expectEqualStrings("p0=1  p1=83", w.buffered());

    var big = Stats.init(std.testing.io, "t", false);
    defer big.deinit();
    for (0..9) |i| big.noteOffset(@intCast(i), @intCast(1000 + i));
    w = .fixed(&buf);
    big.writeOffsetsPretty(&w);
    try std.testing.expectEqualStrings("9 partitions, max 1008", w.buffered());
}

test "pretty summary block without color" {
    var st = Stats.init(std.testing.io, "events", false);
    defer st.deinit();
    st.add(500, 9450);
    st.noteOffset(0, 83);
    st.noteOffset(1, 83);
    var buf: [1024]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    st.writeSummary(&w, "produced to", "2 of 2 partitions", "", .ok, false);
    const out = w.buffered();
    try std.testing.expect(std.mem.startsWith(u8, out, "\u{2713} 500 records produced to events  2 of 2 partitions\n"));
    try std.testing.expect(std.mem.indexOf(u8, out, "\n  data      ") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\n  rate      ") != null);
    try std.testing.expect(std.mem.endsWith(u8, out, "  offsets   p0=83  p1=83\n"));
    try std.testing.expect(std.mem.indexOfScalar(u8, out, 0x1b) == null);
}
