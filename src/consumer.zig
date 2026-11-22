const std = @import("std");
const client = @import("client.zig");
const protocol = @import("protocol.zig");
const stats = @import("stats.zig");
const term = @import("term.zig");
const json = @import("json.zig");
const format_mod = @import("format.zig");

const Encoder = protocol.Encoder;
const Decoder = protocol.Decoder;

pub const Options = struct {
    pub const Start = enum { earliest, latest, offset };
    topic: []const u8,
    group: ?[]const u8 = null,
    fetch_wait_ms: ?u64 = null,
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
    /// Colorize record output (keys, headers, JSON syntax) for a terminal.
    color: bool = false,
};

pub const Error = error{
    PartitionNotFound,
    FetchFailed,
    /// `--offset` outside the partition's [earliest, latest] range; detail in
    /// `Client.errDetail()`.
    OffsetOutOfRange,
};

pub const Cursor = struct {
    pidx: i32,
    leader: i32,
    offset: i64,
    emitted: i64 = -1,
    committed: i64 = -1,
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
    if (opts.group != null) return runGroup(c, opts, out);
    var cursors = try c.alloc.alloc(Cursor, c.partitions.items.len);
    defer c.alloc.free(cursors);
    var cursor_count: usize = 0;
    for (c.partitions.items) |p| {
        if (opts.partition) |wanted| if (wanted != p.index) continue;
        cursors[cursor_count] = .{
            .pidx = p.index,
            .leader = p.leader,
            .offset = if (opts.start == .offset) opts.offset else -1,
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

    var scratch: std.Io.Writer.Allocating = .init(c.alloc);
    defer scratch.deinit();

    var count: u64 = 0;
    var last_record = std.Io.Timestamp.now(c.io, .awake);
    var retry: client.Retry = .{};
    while (true) {
        if (opts.stop) |flag| if (flag.load(.acquire)) break;
        if (opts.max_records) |max| if (count >= max) break;
        if (opts.sink_closed) |closed| if (closed()) return error.WriteFailed;
        if (opts.idle_ms) |idle| {
            const elapsed = last_record.durationTo(std.Io.Timestamp.now(c.io, .awake));
            // After a leader error, fetch from the refreshed routes before
            // treating the lack of records as idle.
            if (retry.started == null and elapsed.toMilliseconds() >= idle) break;
        }

        _ = round_arena.reset(.retain_capacity);
        round_alloc = round_arena.allocator();
        var progress = false;
        var failed_leader: ?i32 = null;
        var leader_seen: std.AutoHashMapUnmanaged(i32, void) = .empty;
        for (cursors[0..cursor_count]) |*cursor| {
            if (leader_seen.contains(cursor.leader)) continue;
            try leader_seen.put(round_alloc, cursor.leader, {});

            var parts: std.ArrayListUnmanaged(FetchPart) = .empty;
            for (cursors[0..cursor_count]) |*candidate|
                if (candidate.leader == cursor.leader)
                    try parts.append(round_alloc, .{ .pidx = candidate.pidx, .cursor = candidate });

            fetchLeader(c, round_alloc, opts, parts.items, out, &scratch, &count, &progress) catch |err| switch (err) {
                error.RetryFetch => {
                    c.dropNode(cursor.leader);
                    failed_leader = cursor.leader;
                },
                else => {
                    return err;
                },
            };
            if (opts.max_records) |max| if (count >= max) break;
        }
        leader_seen.deinit(round_alloc);
        try out.flush();
        if (opts.stats) |s| s.maybeRender();
        if (progress) last_record = std.Io.Timestamp.now(c.io, .awake);
        if (opts.max_records) |max| if (count >= max) break;
        if (failed_leader) |node| {
            if (!retry.pause(c)) {
                c.setErr("fetch to broker {d}: recovery timed out after 30s", .{node});
                return error.FetchFailed;
            }
            refreshLeaders(c, opts.topic, cursors[0..cursor_count]);
        } else {
            retry = .{};
        }
    }
    return count;
}

const group_rebalance_timeout_ms = 30000;
const group_commit_floor_ms = 1000;
const heartbeat_slack_ms = 100;

const GroupSession = struct {
    c: *client.Client,
    opts: Options,
    member: [32]u8,
    epoch: i32 = 0,
    interval: u64 = 5000,
    owned: std.ArrayList(Cursor) = .empty,
    assignment_seen: bool = false,
    heartbeat_due: bool = true,
    last_heartbeat: std.Io.Timestamp,
    last_record: std.Io.Timestamp,
    last_commit: ?std.Io.Timestamp = null,
    stale_retries: u8 = 0,

    fn beginAttempt(s: *GroupSession) void {
        s.c.group_deadline = std.Io.Timestamp.now(s.c.io, .awake).addDuration(.fromMilliseconds(@intCast(@min(30000, s.interval *| 3))));
    }

    fn recoverEpoch(s: *GroupSession, err: anyerror) bool {
        switch (err) {
            error.FencedMemberEpoch, error.UnknownMemberId, error.GroupDeadlineExceeded => s.rejoin(),
            error.StaleMemberEpoch => {
                if (s.stale_retries != 0) return false;
                s.stale_retries += 1;
                s.heartbeat_due = true;
            },
            else => return false,
        }
        return true;
    }

    fn rejoin(s: *GroupSession) void {
        s.owned.clearRetainingCapacity();
        s.epoch = 0;
        s.heartbeat_due = true;
        s.assignment_seen = false;
        s.stale_retries = 0;
        s.c.vlog("group '{s}': fenced; rejoining", .{s.opts.group.?});
    }

    fn commit(s: *GroupSession, alloc: std.mem.Allocator, force: bool) !void {
        const now = std.Io.Timestamp.now(s.c.io, .awake);
        if (!force) if (s.last_commit) |last| {
            if (last.durationTo(now).toMilliseconds() < group_commit_floor_ms) return;
        };
        var parts: std.ArrayList(protocol.CommitPartition) = .empty;
        for (s.owned.items) |cursor| {
            if (pendingCommit(cursor)) |offset| try parts.append(alloc, .{ .partition = cursor.pidx, .offset = offset });
        }
        if (parts.items.len == 0) return;
        try s.c.commitGroupOffsets(alloc, .{
            .group = s.opts.group.?,
            .member_id = &s.member,
            .member_epoch = s.epoch,
            .topics = &.{.{ .name = s.opts.topic, .partitions = parts.items }},
        });
        s.last_commit = std.Io.Timestamp.now(s.c.io, .awake);
        for (s.owned.items) |*cursor| if (pendingCommit(cursor.*)) |offset| {
            cursor.committed = offset;
        };
        s.c.vlog("committed {d} offsets for group {s}", .{ parts.items.len, s.opts.group.? });
    }

    fn beat(s: *GroupSession, arena: *std.heap.ArenaAllocator) !void {
        const alloc = arena.allocator();
        const parts = try alloc.alloc(i32, s.owned.items.len);
        for (s.owned.items, parts) |cursor, *p| p.* = cursor.pidx;
        const joining = s.epoch == 0;
        const full = joining or s.heartbeat_due;
        const result = try s.c.heartbeat(alloc, .{
            .group = s.opts.group.?,
            .member_id = &s.member,
            .member_epoch = s.epoch,
            .rebalance_timeout_ms = if (full) group_rebalance_timeout_ms else -1,
            .topics = if (full) &.{s.opts.topic} else null,
            .assignment = if (full) (if (parts.len == 0) &.{} else &.{.{ .topic_id = s.c.topic_id, .partitions = parts }}) else null,
            .retry_assignment = if (parts.len == 0) &.{} else &.{.{ .topic_id = s.c.topic_id, .partitions = parts }},
        });
        if (result.code == .fenced_member_epoch or result.code == .unknown_member_id) {
            s.rejoin();
            return;
        }
        try s.c.groupResult(result.code);
        s.epoch = result.member_epoch;
        s.interval = @intCast(@max(1, result.heartbeat_interval_ms));
        s.last_heartbeat = std.Io.Timestamp.now(s.c.io, .awake);
        s.heartbeat_due = false;
        if (joining) s.c.vlog("joined group '{s}', member {s}, epoch {d}", .{ s.opts.group.?, s.member, s.epoch });
        if (result.assignment) |assignment| {
            if (!s.assignment_seen) s.last_record = std.Io.Timestamp.now(s.c.io, .awake);
            s.assignment_seen = true;
            var assigned: []const i32 = &.{};
            var refreshed = false;
            for (assignment) |t| {
                if (!std.mem.eql(u8, &t.topic_id, &s.c.topic_id)) {
                    if (!refreshed) try s.c.refreshMetadata(s.opts.topic);
                    refreshed = true;
                    if (!std.mem.eql(u8, &t.topic_id, &s.c.topic_id)) return error.UnknownTopicAssignment;
                }
                assigned = t.partitions;
            }
            if (!assignmentChanged(s.owned.items, assigned)) return;
            s.commit(alloc, true) catch |err| {
                // Leaving after a failed revoke commit lets the next owner replay progress.
                s.c.vlog("group '{s}': revoke commit failed ({s}); progress may replay", .{ s.opts.group.?, @errorName(err) });
                return err;
            };
            var next: std.ArrayList(Cursor) = .empty;
            errdefer next.deinit(s.c.alloc);
            var added: std.ArrayList(i32) = .empty;
            for (assigned) |p| {
                var retained: ?Cursor = null;
                for (s.owned.items) |cursor| if (cursor.pidx == p) {
                    retained = cursor;
                };
                try next.append(s.c.alloc, retained orelse .{ .pidx = p, .leader = s.c.partitionLeader(p) orelse -1, .offset = -1 });
                if (retained == null) try added.append(alloc, p);
            }
            if (added.items.len > 0) {
                try s.c.refreshMetadata(s.opts.topic);
                const fetched = try s.c.fetchGroupOffsets(alloc, .{
                    .group = s.opts.group.?,
                    .member_id = &s.member,
                    .member_epoch = s.epoch,
                    .topics = &.{.{ .name = s.opts.topic, .partitions = added.items }},
                });
                for (next.items) |*cursor| {
                    cursor.leader = s.c.partitionLeader(cursor.pidx) orelse -1;
                    for (added.items) |p| {
                        if (p != cursor.pidx) continue;
                        for (fetched.offsets) |offset| {
                            if (offset.partition == p) {
                                cursor.offset = offset.offset;
                                cursor.committed = offset.offset;
                            }
                        }
                    }
                }
                try listOffsets(s.c, arena, s.opts.topic, next.items, s.opts.start);
            }
            s.owned.deinit(s.c.alloc);
            s.owned = next;
            s.heartbeat_due = true;
            s.c.vlog("group '{s}': assignment {d} partition(s), epoch {d}", .{ s.opts.group.?, s.owned.items.len, s.epoch });
        }
    }

    fn leave(s: *GroupSession) void {
        if (s.epoch <= 0) return;
        s.beginAttempt();
        var arena = std.heap.ArenaAllocator.init(s.c.alloc);
        defer arena.deinit();
        _ = s.c.heartbeat(arena.allocator(), .{ .group = s.opts.group.?, .member_id = &s.member, .member_epoch = -1 }) catch {};
    }

    fn finalCommitFailed(s: *GroupSession, err: anyerror) error{FinalCommitFailed} {
        s.c.setErr("{s}; next run may replay records", .{@errorName(err)});
        std.debug.print("kite: warning: final commit failed; next run may replay records\n", .{});
        return error.FinalCommitFailed;
    }
};

fn pendingCommit(cursor: Cursor) ?i64 {
    return if (cursor.emitted >= 0 and cursor.emitted != cursor.committed) cursor.emitted else null;
}

fn assignmentChanged(owned: []const Cursor, assigned: []const i32) bool {
    if (owned.len != assigned.len) return true;
    for (owned) |cursor| {
        var found = false;
        for (assigned) |p| if (p == cursor.pidx) {
            found = true;
        };
        if (!found) return true;
    }
    return false;
}

test "group assignment changes and commit watermarks" {
    const owned = [_]Cursor{
        .{ .pidx = 0, .leader = 1, .offset = 4, .emitted = 4, .committed = 4 },
        .{ .pidx = 2, .leader = 1, .offset = 9, .emitted = 8, .committed = 5 },
    };
    try std.testing.expect(!assignmentChanged(&owned, &.{ 2, 0 }));
    try std.testing.expect(assignmentChanged(&owned, &.{ 0, 1 }));
    try std.testing.expect(assignmentChanged(&owned, &.{0}));
    try std.testing.expect(assignmentChanged(&owned, &.{}));
    try std.testing.expect(!assignmentChanged(&.{}, &.{}));
    try std.testing.expect(pendingCommit(owned[0]) == null);
    try std.testing.expectEqual(@as(?i64, 8), pendingCommit(owned[1]));
    try std.testing.expect(pendingCommit(.{ .pidx = 0, .leader = 1, .offset = 100 }) == null);
}

test "fenced group discards cursors without committing" {
    var c: client.Client = undefined;
    const cfg = @import("config.zig").Config{ .bootstrap_servers = &.{} };
    c.cfg = &cfg;
    var session: GroupSession = undefined;
    session.c = &c;
    session.opts = .{ .topic = "t", .group = "g" };
    session.owned = .empty;
    defer session.owned.deinit(std.testing.allocator);
    try session.owned.append(std.testing.allocator, .{ .pidx = 0, .leader = 1, .offset = 3, .emitted = 3 });
    session.epoch = 5;
    session.assignment_seen = true;
    session.heartbeat_due = false;
    session.rejoin();
    try std.testing.expectEqual(@as(usize, 0), session.owned.items.len);
    try std.testing.expectEqual(@as(i32, 0), session.epoch);
    try std.testing.expect(session.heartbeat_due);
    try std.testing.expect(!session.assignment_seen);
}

test "stale epochs request one heartbeat and retry" {
    var session: GroupSession = undefined;
    session.stale_retries = 0;
    session.heartbeat_due = false;
    try std.testing.expect(session.recoverEpoch(error.StaleMemberEpoch));
    try std.testing.expect(session.heartbeat_due);
    try std.testing.expect(!session.recoverEpoch(error.StaleMemberEpoch));
    try std.testing.expect(!session.recoverEpoch(error.GroupFailed));
}

test "commit floor skips network requests until due" {
    var c: client.Client = undefined;
    c.io = std.testing.io;
    var session: GroupSession = undefined;
    session.c = &c;
    session.last_commit = std.Io.Timestamp.now(c.io, .awake);
    try session.commit(std.testing.allocator, false);
}

test "failed final commits preserve pending progress and report replay" {
    var c: client.Client = undefined;
    var session: GroupSession = undefined;
    session.c = &c;
    session.epoch = 3;
    session.owned = .empty;
    defer session.owned.deinit(std.testing.allocator);
    try session.owned.append(std.testing.allocator, .{ .pidx = 0, .leader = 1, .offset = 4, .emitted = 4, .committed = 2 });
    for ([_]anyerror{ error.FencedMemberEpoch, error.UnknownMemberId, error.GroupDeadlineExceeded }) |err| {
        try std.testing.expectEqual(error.FinalCommitFailed, session.finalCommitFailed(err));
        try std.testing.expectEqual(@as(i32, 3), session.epoch);
        try std.testing.expectEqual(@as(?i64, 4), pendingCommit(session.owned.items[0]));
        try std.testing.expect(std.mem.indexOf(u8, c.errDetail(), @errorName(err)) != null);
        try std.testing.expect(std.mem.indexOf(u8, c.errDetail(), "next run may replay records") != null);
    }
}

fn runGroup(c: *client.Client, opts: Options, out: *std.Io.Writer) !u64 {
    defer c.group_deadline = null;
    var random: [16]u8 = undefined;
    c.io.randomSecure(&random) catch c.io.random(&random);
    const now = std.Io.Timestamp.now(c.io, .awake);
    var session: GroupSession = .{ .c = c, .opts = opts, .member = std.fmt.bytesToHex(random, .lower), .last_heartbeat = now, .last_record = now };
    defer session.owned.deinit(c.alloc);
    defer session.leave();
    var arena = std.heap.ArenaAllocator.init(c.alloc);
    defer arena.deinit();
    var scratch: std.Io.Writer.Allocating = .init(c.alloc);
    defer scratch.deinit();
    var count: u64 = 0;
    var leader_index: usize = 0;
    var retry: client.Retry = .{};
    while (true) {
        if (opts.stop) |flag| if (flag.load(.acquire)) break;
        if (opts.max_records) |max| if (count >= max) break;
        if (opts.sink_closed) |closed| if (closed()) return error.WriteFailed;
        _ = arena.reset(.retain_capacity);
        session.beginAttempt();
        const elapsed = session.last_heartbeat.durationTo(std.Io.Timestamp.now(c.io, .awake)).toMilliseconds();
        if (session.heartbeat_due or elapsed >= session.interval) {
            session.beat(&arena) catch |err| {
                if (!session.recoverEpoch(err)) return err;
            };
            if (session.heartbeat_due) continue;
        }
        if (opts.idle_ms) |idle| {
            if (session.assignment_seen and retry.started == null and session.last_record.durationTo(std.Io.Timestamp.now(c.io, .awake)).toMilliseconds() >= idle) break;
        }
        if (session.owned.items.len == 0) {
            c.sleep(@min(50, @max(1, session.interval / 4)));
            continue;
        }
        const leader = session.owned.items[leader_index % session.owned.items.len].leader;
        leader_index +%= 1;
        var parts: std.ArrayList(FetchPart) = .empty;
        for (session.owned.items) |*cursor| if (cursor.leader == leader) {
            try parts.append(arena.allocator(), .{ .pidx = cursor.pidx, .cursor = cursor });
        };
        var fetch_opts = opts;
        const since_beat = session.last_heartbeat.durationTo(std.Io.Timestamp.now(c.io, .awake)).toMilliseconds();
        const until_beat = session.interval -| @as(u64, @intCast(@max(0, since_beat)));
        if (until_beat <= heartbeat_slack_ms) {
            c.sleep(until_beat);
            continue;
        }
        fetch_opts.fetch_wait_ms = @min(c.cfg.fetch_max_wait_ms, @max(1, until_beat -| heartbeat_slack_ms));
        var progress = false;
        var failed = false;
        fetchLeader(c, arena.allocator(), fetch_opts, parts.items, out, &scratch, &count, &progress) catch |err| switch (err) {
            error.RetryFetch => {
                c.dropNode(leader);
                failed = true;
            },
            else => return err,
        };
        try out.flush();
        if (opts.sink_closed) |closed| if (closed()) return error.WriteFailed;
        session.beginAttempt();
        session.commit(arena.allocator(), false) catch |err| {
            if (!session.recoverEpoch(err)) return err;
            continue;
        };
        session.stale_retries = 0;
        if (progress) session.last_record = std.Io.Timestamp.now(c.io, .awake);
        if (opts.stats) |s| s.maybeRender();
        if (failed) {
            if (!retry.pause(c)) return error.FetchFailed;
            refreshLeaders(c, opts.topic, session.owned.items);
        } else retry = .{};
    }
    try out.flush();
    if (opts.sink_closed) |closed| if (closed()) return error.WriteFailed;
    session.beginAttempt();
    session.commit(arena.allocator(), true) catch |err| {
        if (err != error.StaleMemberEpoch) return session.finalCommitFailed(err);
        session.heartbeat_due = true;
        session.beat(&arena) catch |beat_err| return session.finalCommitFailed(beat_err);
        if (session.epoch <= 0) return session.finalCommitFailed(error.FencedMemberEpoch);
        session.commit(arena.allocator(), true) catch |commit_err| return session.finalCommitFailed(commit_err);
    };
    return count;
}

fn refreshLeaders(c: *client.Client, topic: []const u8, cursors: []Cursor) void {
    c.refreshMetadata(topic) catch return;
    for (cursors) |*cursor| cursor.leader = c.partitionLeader(cursor.pidx) orelse -1;
}

pub fn listOffsets(
    c: *client.Client,
    arena: *std.heap.ArenaAllocator,
    topic: []const u8,
    cursors: []Cursor,
    start: Options.Start,
) !void {
    var retry: client.Retry = .{};
    while (true) {
        listOffsetsOnce(c, arena.allocator(), topic, cursors, start) catch |err| {
            if (err != error.RetryFetch) return err;
            if (!retry.pause(c)) {
                c.setErr("list offsets: recovery timed out after 30s", .{});
                return error.FetchFailed;
            }
            refreshLeaders(c, topic, cursors);
            _ = arena.reset(.retain_capacity);
            continue;
        };
        return;
    }
}

fn listOffsetsOnce(
    c: *client.Client,
    alloc: std.mem.Allocator,
    topic: []const u8,
    cursors: []Cursor,
    start: Options.Start,
) !void {
    var leaders: std.AutoHashMapUnmanaged(i32, void) = .empty;
    for (cursors) |cursor| {
        if (cursor.offset < 0) try leaders.put(alloc, cursor.leader, {});
    }
    defer leaders.deinit(alloc);

    var needs_retry = false;
    var it = leaders.keyIterator();
    while (it.next()) |leader_ptr| {
        const leader = leader_ptr.*;
        var parts: std.ArrayListUnmanaged(ListPart) = .empty;
        for (cursors) |*cursor| if (cursor.offset < 0 and cursor.leader == leader)
            try parts.append(alloc, .{ .pidx = cursor.pidx, .cursor = cursor });
        listOffsetsLeader(c, alloc, topic, parts.items, start) catch |err| {
            if (err != error.RetryFetch) return err;
            c.dropNode(leader);
            needs_retry = true;
        };
    }
    if (needs_retry) return error.RetryFetch;
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
    const conn = c.connFor(parts[0].cursor.leader, client.Client.connKey(parts[0].cursor.leader, 0)) catch return error.RetryFetch;
    const resp = c.sendRequestAlloc(alloc, conn, protocol.api_key.list_offsets, protocol.version.list_offsets, Ctx{
        .topic = topic,
        .parts = parts,
        .start = start,
    }, body) catch return error.RetryFetch;
    var d = Decoder.init(resp.body);
    _ = try d.i32v();
    const topics = try d.compactArrayLen();
    var resolved: usize = 0;
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
                if (code.retriable()) continue;
                c.setErr("list offsets partition {d}: {s}", .{ pidx, code.name() });
                return error.FetchFailed;
            }
            for (parts) |part| {
                if (part.pidx == pidx and offset >= 0) {
                    part.cursor.offset = offset;
                    resolved += 1;
                    break;
                }
            }
        }
        try d.tagBuffer();
    }
    try d.tagBuffer();
    if (resolved != parts.len) return error.RetryFetch;
}

fn fetchLeader(
    c: *client.Client,
    alloc: std.mem.Allocator,
    opts: Options,
    parts: []const FetchPart,
    out: *std.Io.Writer,
    scratch: *std.Io.Writer.Allocating,
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
        .max_wait_ms = opts.fetch_wait_ms orelse c.cfg.fetch_max_wait_ms,
        .max_bytes = c.cfg.fetch_max_bytes,
    }, body) catch return error.RetryFetch;

    var d = Decoder.init(resp.body);
    _ = try d.i32v();
    const top_code: protocol.ErrorCode = @enumFromInt(try d.i16v());
    _ = try d.i32v();
    if (top_code != .none) {
        if (top_code.retriable()) return error.RetryFetch;
        c.setErr("fetch from broker {d}: {s}", .{ leader, top_code.name() });
        return error.FetchFailed;
    }
    const topics = try d.compactArrayLen();
    var needs_retry = false;
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
                resetOffset(c, alloc, opts, part.cursor) catch |err| {
                    if (err != error.RetryFetch) return err;
                    needs_retry = true;
                };
                continue;
            }
            if (code != .none) {
                if (code.retriable()) {
                    needs_retry = true;
                    continue;
                }
                c.setErr("fetch partition {d}: {s}", .{ pidx, code.name() });
                return error.FetchFailed;
            }
            var range = Range{};
            var next_from_batches: ?i64 = null;
            var limited = false;
            if (records) |blob| {
                var ctx = RecordSink{
                    .out = out,
                    .count = count,
                    .max = opts.max_records,
                    .min_offset = part.cursor.offset,
                    .range = &range,
                    .stats = opts.stats,
                    .format = opts.format,
                    .topic = opts.topic,
                    .pidx = pidx,
                    .color = opts.color,
                    .scratch = scratch,
                };
                next_from_batches = protocol.decodeBatches(alloc, blob, &ctx, onRecord) catch |err| switch (err) {
                    error.WriteFailed => return error.WriteFailed,
                    else => {
                        c.setErr("decode fetch partition {d}: {s}", .{ pidx, @errorName(err) });
                        return error.FetchFailed;
                    },
                };
                limited = ctx.limited;
            }
            if (range.last) |last| {
                if (part.cursor.offset <= last + 1) part.cursor.offset = last + 1;
                if (opts.group != null) part.cursor.emitted = last + 1;
            }
            if (next_from_batches) |next| {
                if (opts.group == null or !limited) {
                    if (part.cursor.offset < next) part.cursor.offset = next;
                }
            }
            if (opts.group != null and !limited) part.cursor.emitted = part.cursor.offset;
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
    if (needs_retry) return error.RetryFetch;
}

const Range = struct {
    first: ?i64 = null,
    last: ?i64 = null,
};

const RecordSink = struct {
    limited: bool = false,
    out: *std.Io.Writer,
    count: *u64,
    max: ?u64,
    min_offset: i64,
    range: *Range,
    stats: ?*stats.Stats,
    format: format_mod.Format,
    topic: []const u8,
    pidx: i32,
    color: bool = false,
    scratch: ?*std.Io.Writer.Allocating = null,
};

fn writeJsonRecord(w: *std.Io.Writer, topic: []const u8, pidx: i32, offset: i64, ts: i64, rec: protocol.Record) !void {
    try w.writeAll("{\"topic\":");
    try json.writeString(w, topic);
    try w.print(",\"partition\":{d},\"offset\":{d},\"timestamp\":{d},", .{ pidx, offset, ts });
    if (rec.key) |key| {
        try writeJsonBytesField(w, "key", "key_b64", key);
    } else {
        try w.writeAll("\"key\":null");
    }
    try w.writeAll(",\"headers\":[");
    for (rec.headers, 0..) |header, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeAll("{\"key\":");
        try json.writeString(w, header.key);
        if (header.value) |value| {
            try w.writeByte(',');
            try writeJsonBytesField(w, "value", "value_b64", value);
        } else {
            try w.writeAll(",\"value\":null");
        }
        try w.writeByte('}');
    }
    try w.writeAll("],");
    try writeJsonBytesField(w, "value", "value_b64", rec.value);
    try w.writeByte('}');
}

fn writeJsonBytesField(w: *std.Io.Writer, plain_name: []const u8, b64_name: []const u8, bytes: []const u8) !void {
    if (std.unicode.utf8ValidateSlice(bytes)) {
        try json.writeString(w, plain_name);
        try w.writeAll(":");
        try json.writeString(w, bytes);
    } else {
        try w.writeByte('"');
        try w.writeAll(b64_name);
        try w.writeAll("\":");
        try json.writeBase64String(w, bytes);
    }
}

/// key<TAB>[h: v<TAB>]*value — the shape `auto` and `tsv` share.
/// With `color` the key is cyan and each `name: value` header dim; the
/// TABs and the value stay plain.
fn writeTsvRecord(w: *std.Io.Writer, rec: protocol.Record, color: bool) !void {
    if (rec.key) |key| {
        if (color) try w.writeAll(term.cyan);
        try w.writeAll(key);
        if (color) try w.writeAll(term.reset);
    }
    try w.writeByte('\t');
    for (rec.headers) |header| {
        if (color) try w.writeAll(term.dim);
        try w.writeAll(header.key);
        try w.writeAll(": ");
        if (header.value) |value| try w.writeAll(value);
        if (color) try w.writeAll(term.reset);
        try w.writeByte('\t');
    }
    try w.writeAll(rec.value);
}

fn writeJsonRecordColored(
    w: *std.Io.Writer,
    scratch: *std.Io.Writer.Allocating,
    topic: []const u8,
    pidx: i32,
    offset: i64,
    ts: i64,
    rec: protocol.Record,
) !void {
    scratch.clearRetainingCapacity();
    try writeJsonRecord(&scratch.writer, topic, pidx, offset, ts, rec);
    try term.writeJsonColored(w, scratch.written());
}

fn onRecord(ctx: *RecordSink, offset: i64, timestamp_ms: i64, rec: protocol.Record) !void {
    if (offset < ctx.min_offset) return;
    if (ctx.max) |max| if (ctx.count.* >= max) {
        ctx.limited = true;
        return;
    };
    if (ctx.stats) |s| s.beforeOutput();
    switch (ctx.format) {
        .json => if (ctx.color)
            try writeJsonRecordColored(ctx.out, ctx.scratch.?, ctx.topic, ctx.pidx, offset, timestamp_ms, rec)
        else
            try writeJsonRecord(ctx.out, ctx.topic, ctx.pidx, offset, timestamp_ms, rec),
        .value => try ctx.out.writeAll(rec.value),
        .tsv => try writeTsvRecord(ctx.out, rec, ctx.color),
        .auto, .csv => if (rec.key == null and rec.headers.len == 0)
            try ctx.out.writeAll(rec.value)
        else
            try writeTsvRecord(ctx.out, rec, ctx.color),
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

test "json record output base64-encodes binary fields" {
    var buf: [512]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    const headers = [_]protocol.Header{.{ .key = "h", .value = &.{0x80} }};
    try writeJsonRecord(&w, "t", 0, 1, 2, .{
        .key = &.{ 0, 0xff, 0x80 },
        .value = &.{ 1, 0x80, 0xfe, 0xff },
        .headers = &headers,
    });
    try std.testing.expectEqualStrings(
        "{\"topic\":\"t\",\"partition\":0,\"offset\":1,\"timestamp\":2,\"key_b64\":\"AP+A\",\"headers\":[{\"key\":\"h\",\"value_b64\":\"gA==\"}],\"value_b64\":\"AYD+/w==\"}",
        w.buffered(),
    );
}

test "json record output keeps valid UTF-8 and control bytes plain" {
    var buf: [512]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    const headers = [_]protocol.Header{.{ .key = "h", .value = "é😀\x01\t\n" }};
    try writeJsonRecord(&w, "t", 0, 1, 2, .{
        .key = "é😀",
        .value = "é😀\x01\t\n",
        .headers = &headers,
    });
    try std.testing.expectEqualStrings(
        "{\"topic\":\"t\",\"partition\":0,\"offset\":1,\"timestamp\":2,\"key\":\"é😀\",\"headers\":[{\"key\":\"h\",\"value\":\"é😀\\u0001\\t\\n\"}],\"value\":\"é😀\\u0001\\t\\n\"}",
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
        .min_offset = 0,
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

    w = .fixed(&buf);
    sink.min_offset = 5;
    try onRecord(&sink, 4, 0, .{ .value = "before cursor" });
    try onRecord(&sink, 5, 0, .{ .value = "at cursor" });
    try std.testing.expectEqualStrings("at cursor\n", w.buffered());
    try std.testing.expectEqual(@as(u64, 5), count);
}
