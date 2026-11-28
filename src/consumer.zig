const std = @import("std");
const client = @import("client.zig");
const config = @import("config.zig");
const protocol = @import("protocol.zig");
const stats = @import("stats.zig");
const term = @import("term.zig");
const transport = @import("transport.zig");
const json = @import("json.zig");
const format_mod = @import("format.zig");

const Encoder = protocol.Encoder;
const Decoder = protocol.Decoder;
const round_arena_retain_limit: usize = 16 << 20;

pub const Options = struct {
    pub const Start = enum { earliest, latest, offset };
    topic: []const u8,
    group: ?[]const u8 = null,
    isolation: protocol.Isolation = .read_committed,
    follow: bool = false,
    fetch_wait_ms: ?u64 = null,
    fetch_idle_deadline: ?std.Io.Timestamp = null,
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
    leader_stale: bool = false,
    awaiting_position: bool = false,
    offsets_fetched: bool = false,
    position_attempted: bool = false,
    initial_fetch_pending: bool = false,
    fetch_recovery_deadline: ?std.Io.Timestamp = null,

    fn fetchSucceeded(cursor: *Cursor) void {
        cursor.fetch_recovery_deadline = null;
    }
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
        try listOffsets(c, &round_arena, opts.topic, cursors[0..cursor_count], opts.start, opts.isolation);
        _ = round_arena.reset(.retain_capacity);
        round_alloc = round_arena.allocator();
    }

    var scratch: std.Io.Writer.Allocating = .init(c.alloc);
    defer scratch.deinit();

    var count: u64 = 0;
    var last_record = std.Io.Timestamp.now(c.io, .awake);
    var retry: client.Retry = .{};
    var fetch_round: usize = 0;
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

        _ = round_arena.reset(.{ .retain_with_limit = round_arena_retain_limit });
        round_alloc = round_arena.allocator();
        var progress = false;
        var failed_leader: ?i32 = null;
        var idle_reached = false;
        var leader_seen: std.AutoHashMapUnmanaged(i32, void) = .empty;
        broker_loop: for (cursors[0..cursor_count]) |*cursor| {
            if (leader_seen.contains(cursor.leader)) continue;
            try leader_seen.put(round_alloc, cursor.leader, {});

            var parts: std.ArrayListUnmanaged(FetchPart) = .empty;
            for (cursors[0..cursor_count]) |*candidate|
                if (candidate.leader == cursor.leader)
                    try parts.append(round_alloc, .{ .pidx = candidate.pidx, .cursor = candidate });
            if (parts.items.len > 1) std.mem.rotate(FetchPart, parts.items, fetch_round % parts.items.len);

            var fetch_opts = opts;
            if (opts.idle_ms) |idle| fetch_opts.fetch_idle_deadline = last_record.addDuration(.fromMilliseconds(@intCast(idle)));
            fetchLeaderLegacy(c, round_alloc, fetch_opts, parts.items, out, &scratch, &count, &progress) catch |err| switch (err) {
                error.FetchIdleExpired => {
                    idle_reached = true;
                    break :broker_loop;
                },
                error.RetryFetch => {
                    failed_leader = cursor.leader;
                },
                else => {
                    if (err != error.WriteFailed) out.flush() catch {};
                    return err;
                },
            };
            if (opts.max_records) |max| if (count >= max) break;
        }
        fetch_round +%= 1;
        leader_seen.deinit(round_alloc);
        try out.flush();
        if (opts.stats) |s| s.maybeRender();
        if (progress) last_record = std.Io.Timestamp.now(c.io, .awake);
        if (idle_reached and !progress) break;
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
const group_session_assumed_ms = 45000;
const group_commit_floor_ms = 1000;
const group_rpc_min_timeout_ms = 1000;
const heartbeat_min_timeout_ms = 50;
const heartbeat_slack_ms = 100;

fn heartbeatSlack(interval: u64) u64 {
    return @min(heartbeat_slack_ms, interval / 4);
}

fn heartbeatBaseTimeoutMs(interval: u64) u64 {
    if (interval >= group_rpc_min_timeout_ms) return interval -| heartbeatSlack(interval);
    return @max(heartbeat_min_timeout_ms, interval * 2);
}

fn heartbeatTimeoutMs(interval: u64, timeouts: u8) u64 {
    const base = heartbeatBaseTimeoutMs(interval);
    const shift: u6 = @intCast(@min(timeouts, 10));
    return @min(group_session_assumed_ms / 2, base << shift);
}

const GroupSession = struct {
    c: *client.Client,
    opts: Options,
    member: [32]u8,
    epoch: i32 = 0,
    interval: u64 = 5000,
    owned: std.ArrayList(Cursor) = .empty,
    assignment_seen: bool = false,
    positions_pending_initial: bool = false,
    position_leader: ?i32 = null,
    position_fetch_due: bool = false,
    joined_once: bool = false,
    heartbeat_due: bool = true,
    hook_paused: bool = false,
    hook_timeouts: u32 = 0,
    hb_timeouts: u8 = 0,
    in_flight_hb: ?client.Client.InFlightHeartbeat = null,
    pending_beat: ?PendingHeartbeat = null,
    pending_error: ?anyerror = null,
    last_heartbeat: std.Io.Timestamp,
    last_record: std.Io.Timestamp,
    last_commit: ?std.Io.Timestamp = null,
    stale_retries: u8 = 0,
    attempt_started: std.Io.Timestamp = .zero,
    recovery_deadline: ?std.Io.Timestamp = null,
    commit_recovery_deadline: ?std.Io.Timestamp = null,
    last_recovery_warning: ?std.Io.Timestamp = null,

    fn beginAttempt(s: *GroupSession) void {
        s.attempt_started = std.Io.Timestamp.now(s.c.io, .awake);
        const timeout = @max(group_rpc_min_timeout_ms, s.interval -| heartbeatSlack(s.interval));
        s.c.group_deadline = s.attempt_started.addDuration(.fromMilliseconds(@intCast(timeout)));
    }

    fn beginHeartbeatAttempt(s: *GroupSession) void {
        s.attempt_started = std.Io.Timestamp.now(s.c.io, .awake);
        const timeout = if (s.epoch == 0)
            @max(group_rpc_min_timeout_ms, heartbeatTimeoutMs(s.interval, s.hb_timeouts))
        else
            heartbeatTimeoutMs(s.interval, s.hb_timeouts);
        s.c.group_deadline = s.attempt_started.addDuration(.fromMilliseconds(@intCast(timeout)));
    }

    fn beginRound(s: *GroupSession) void {
        s.beginAttempt();
        if (!s.opts.follow) if (s.recoveryDeadline()) |deadline| s.capDeadline(deadline);
        if (s.assignment_seen and s.idleReady()) if (s.opts.idle_ms) |idle| {
            const io_slack: u64 = if (s.recoveryDeadline() == null) 1000 else 0;
            s.capDeadline(s.last_record.addDuration(.fromMilliseconds(@intCast(idle +| io_slack))));
        };
    }

    fn capDeadline(s: *GroupSession, deadline: std.Io.Timestamp) void {
        if (deadline.nanoseconds < s.c.group_deadline.?.nanoseconds) s.c.group_deadline = deadline;
    }

    fn startRecovery(s: *GroupSession) void {
        if (s.recovery_deadline == null) s.recovery_deadline = s.last_heartbeat.addDuration(.fromMilliseconds(@intCast(group_session_assumed_ms -| s.interval)));
    }

    fn startFetchRecovery(s: *GroupSession, cursor: *Cursor) void {
        if (cursor.fetch_recovery_deadline == null) cursor.fetch_recovery_deadline = s.attempt_started.addDuration(.fromMilliseconds(group_session_assumed_ms));
    }

    fn applyFetchOutcomes(s: *GroupSession, parts: []const FetchPart, outcomes: []const FetchOutcome) bool {
        var failed = false;
        for (parts, outcomes) |part, outcome| {
            part.cursor.initial_fetch_pending = false;
            switch (outcome) {
                .retry => {
                    part.cursor.leader_stale = true;
                    s.startFetchRecovery(part.cursor);
                    failed = true;
                },
                .success, .offset_reset => part.cursor.fetchSucceeded(),
                .throttled => {},
            }
        }
        return failed;
    }

    fn recoveryDeadline(s: *GroupSession) ?std.Io.Timestamp {
        var deadline = s.commit_recovery_deadline;
        if (s.recovery_deadline) |next| if (deadline == null or next.nanoseconds < deadline.?.nanoseconds) {
            deadline = next;
        };
        for (s.owned.items) |cursor| if (cursor.fetch_recovery_deadline) |next| {
            if (deadline == null or next.nanoseconds < deadline.?.nanoseconds) deadline = next;
        };
        return deadline;
    }

    fn heartbeatSucceeded(s: *GroupSession) void {
        s.recovery_deadline = null;
    }

    fn idleExpired(s: *GroupSession, now: std.Io.Timestamp) bool {
        const idle = s.opts.idle_ms orelse return false;
        return s.assignment_seen and s.idleReady() and s.last_record.durationTo(now).toMilliseconds() >= idle;
    }

    fn idleReady(s: *GroupSession) bool {
        if (s.positions_pending_initial) return false;
        if (s.position_fetch_due and s.hasReadyPartition()) return false;
        for (s.owned.items) |cursor| if (!cursor.awaiting_position and cursor.initial_fetch_pending) return false;
        return true;
    }

    fn hasReadyPartition(s: *GroupSession) bool {
        for (s.owned.items) |cursor| if (s.fetchReady(cursor) and !cursor.leader_stale) return true;
        return false;
    }

    fn fetchReady(s: *GroupSession, cursor: Cursor) bool {
        return !cursor.awaiting_position and (!s.positions_pending_initial or !cursor.initial_fetch_pending);
    }

    fn checkRecovery(s: *GroupSession) !void {
        const deadline = s.recoveryDeadline() orelse return;
        const now = std.Io.Timestamp.now(s.c.io, .awake);
        if (s.opts.follow) {
            if (s.last_recovery_warning == null or s.last_recovery_warning.?.durationTo(now).toMilliseconds() >= 15000) {
                std.debug.print("kite: warning: consumer group recovery ongoing; retrying (records may replay after rejoining)\n", .{});
                s.last_recovery_warning = now;
            }
            return;
        }
        if (now.nanoseconds >= deadline.nanoseconds or s.idleExpired(now)) return s.recoveryFailed();
    }

    fn recoveryFailed(s: *GroupSession) error{FetchFailed} {
        s.c.setErr("consumer group: recovery timed out", .{});
        return error.FetchFailed;
    }

    fn recoverEpoch(s: *GroupSession, err: anyerror) bool {
        const failure = switch (err) {
            error.MetadataFailed, error.MetadataUnavailable => error.GroupDeadlineExceeded,
            else => err,
        };
        switch (failure) {
            error.FencedMemberEpoch, error.UnknownMemberId => s.rejoin(),
            error.GroupDeadlineExceeded => {
                s.heartbeat_due = true;
            },
            error.StaleMemberEpoch => {
                if (s.stale_retries != 0) return false;
                s.stale_retries += 1;
                s.heartbeat_due = true;
            },
            else => return false,
        }
        return true;
    }

    fn abortInFlightHeartbeat(s: *GroupSession) void {
        if (s.in_flight_hb) |*hb| {
            s.c.abortInFlightHeartbeat(hb);
            s.in_flight_hb = null;
        }
    }

    fn rejoin(s: *GroupSession) void {
        s.abortInFlightHeartbeat();
        if (s.recovery_deadline == null) s.recovery_deadline = std.Io.Timestamp.now(s.c.io, .awake).addDuration(.fromMilliseconds(group_session_assumed_ms));
        s.owned.clearRetainingCapacity();
        s.commit_recovery_deadline = null;
        s.epoch = 0;
        s.heartbeat_due = true;
        s.assignment_seen = false;
        s.positions_pending_initial = false;
        s.position_leader = null;
        s.position_fetch_due = false;
        s.stale_retries = 0;
        s.c.vlog("group '{s}': fenced; rejoining", .{s.opts.group.?});
    }

    fn commit(s: *GroupSession, alloc: std.mem.Allocator, force: bool) !void {
        const now = std.Io.Timestamp.now(s.c.io, .awake);
        var parts: std.ArrayList(protocol.CommitPartition) = .empty;
        defer parts.deinit(alloc);
        for (s.owned.items) |cursor| {
            if (pendingCommit(cursor)) |offset| try parts.append(alloc, .{ .partition = cursor.pidx, .offset = offset });
        }
        if (parts.items.len == 0) {
            s.commit_recovery_deadline = null;
            return;
        }
        if (!force) if (s.last_commit) |last| {
            if (last.durationTo(now).toMilliseconds() < group_commit_floor_ms) return;
        };
        s.c.commitGroupOffsets(alloc, .{
            .group = s.opts.group.?,
            .member_id = &s.member,
            .member_epoch = s.epoch,
            .topics = &.{.{ .name = s.opts.topic, .partitions = parts.items }},
        }) catch |err| {
            if (s.commit_recovery_deadline == null) s.commit_recovery_deadline = s.attempt_started.addDuration(.fromMilliseconds(group_session_assumed_ms));
            return err;
        };
        s.commit_recovery_deadline = null;
        s.last_commit = std.Io.Timestamp.now(s.c.io, .awake);
        for (s.owned.items) |*cursor| if (pendingCommit(cursor.*)) |offset| {
            cursor.committed = offset;
        };
        s.c.vlog("committed {d} offsets for group {s}", .{ parts.items.len, s.opts.group.? });
    }

    fn deinitPending(s: *GroupSession) void {
        s.abortInFlightHeartbeat();
        if (s.pending_beat) |pb| {
            for (pb.assignment) |t| s.c.alloc.free(t.partitions);
            s.c.alloc.free(pb.assignment);
            s.pending_beat = null;
        }
        s.pending_error = null;
    }

    fn waitHook(s: *GroupSession) transport.WaitHook {
        return .{
            .ctx = s,
            .io = s.c.io,
            .next_due = nextHeartbeatDue,
            .tick = heartbeatTick,
            .watch_fd = watchHeartbeatFd,
        };
    }

    fn watchHeartbeatFd(ctx: *anyopaque) ?std.posix.socket_t {
        const s: *GroupSession = @ptrCast(@alignCast(ctx));
        if (s.epoch <= 0 or s.hook_paused or s.pending_beat != null or s.pending_error != null) return null;
        if (s.in_flight_hb) |*hb| return s.c.inFlightHeartbeatFd(hb);
        return null;
    }

    fn nextHeartbeatDue(ctx: *anyopaque) ?std.Io.Timestamp {
        const s: *GroupSession = @ptrCast(@alignCast(ctx));
        if (s.epoch <= 0 or s.hook_paused or s.pending_beat != null or s.pending_error != null) return null;
        if (s.in_flight_hb) |*hb| {
            if (s.c.inFlightHeartbeatHasBuffered(hb)) return .zero;
            return hb.deadline;
        }
        if (s.heartbeat_due) return .zero;
        return s.last_heartbeat.addDuration(.fromMilliseconds(@intCast(s.interval)));
    }

    fn assignmentUnchanged(s: *const GroupSession, assignment: ?[]const protocol.TopicAssignment) bool {
        const list = assignment orelse return true;
        if (!s.assignment_seen) return false;
        var assigned: []const i32 = &.{};
        for (list) |t| {
            if (!std.mem.eql(u8, &t.topic_id, &s.c.topic_id)) return false;
            assigned = t.partitions;
        }
        return !assignmentChanged(s.owned.items, assigned);
    }

    fn startInFlightHeartbeat(s: *GroupSession, arena: *std.heap.ArenaAllocator, full: bool, in_hook: bool) bool {
        _ = arena.reset(.retain_capacity);
        const alloc = arena.allocator();
        const parts = alloc.alloc(i32, s.owned.items.len) catch {
            s.pending_error = error.OutOfMemory;
            s.hook_paused = true;
            return true;
        };
        for (s.owned.items, parts) |cursor, *p| p.* = cursor.pidx;
        const sent = std.Io.Timestamp.now(s.c.io, .awake);
        s.attempt_started = sent;
        const deadline = sent.addDuration(.fromMilliseconds(@intCast(heartbeatTimeoutMs(s.interval, s.hb_timeouts))));
        const inflight = s.c.startHeartbeat(alloc, .{
            .group = s.opts.group.?,
            .member_id = &s.member,
            .member_epoch = s.epoch,
            .rebalance_timeout_ms = if (full) group_rebalance_timeout_ms else -1,
            .topics = if (full) &.{s.opts.topic} else null,
            .assignment = if (full) (if (parts.len == 0) &.{} else &.{.{ .topic_id = s.c.topic_id, .partitions = parts }}) else null,
            .retry_fields = .{
                .rebalance_timeout_ms = group_rebalance_timeout_ms,
                .topics = &.{s.opts.topic},
                .assignment = if (parts.len == 0) &.{} else &.{.{ .topic_id = s.c.topic_id, .partitions = parts }},
            },
        }, sent, deadline) catch |err| {
            if (err == error.GroupDeadlineExceeded or err == error.IoFailed or err == error.IoTimedOut or err == error.MetadataFailed or err == error.MetadataUnavailable or err == error.CoordinatorNotAvailable) {
                s.startRecovery();
                s.heartbeat_due = true;
                s.hook_timeouts +|= 1;
                if (in_hook) s.hook_paused = true;
                return false;
            }
            s.pending_error = err;
            s.hook_paused = true;
            return true;
        };
        s.in_flight_hb = inflight;
        if (s.c.inFlightHeartbeatHasBuffered(&s.in_flight_hb.?)) {
            return s.stepHeartbeat(in_hook);
        }
        return false;
    }

    fn stepHeartbeat(s: *GroupSession, in_hook: bool) bool {
        if (s.epoch <= 0) return false;
        if (s.pending_beat != null or s.pending_error != null) return true;
        const prev_hook = s.c.wait_hook;
        const prev_deadline = s.c.group_deadline;
        const prev_attempt = s.attempt_started;
        s.c.wait_hook = null;
        defer {
            s.c.wait_hook = prev_hook;
            s.c.group_deadline = prev_deadline;
            s.attempt_started = prev_attempt;
        }
        var arena = std.heap.ArenaAllocator.init(s.c.alloc);
        defer arena.deinit();

        if (s.in_flight_hb) |*hb| {
            const res = s.c.pollHeartbeat(arena.allocator(), hb);
            if (res) |maybe_hb| {
                const reply = maybe_hb orelse return false;
                const sent = hb.sent;
                s.in_flight_hb = null;
                if (client.Client.isCoordinatorError(reply.code)) {
                    s.startRecovery();
                    s.heartbeat_due = true;
                    s.hook_timeouts +|= 1;
                    if (in_hook and s.hook_timeouts > 1) s.hook_paused = true;
                    return false;
                }
                const rtt_ms: u64 = @intCast(@max(0, sent.durationTo(std.Io.Timestamp.now(s.c.io, .awake)).toMilliseconds()));
                if (rtt_ms <= heartbeatBaseTimeoutMs(s.interval)) {
                    s.hb_timeouts = 0;
                }
                s.hook_timeouts = 0;
                if (reply.code == .none and s.assignmentUnchanged(reply.assignment)) {
                    s.epoch = reply.member_epoch;
                    s.interval = @intCast(@max(1, reply.heartbeat_interval_ms));
                    s.last_heartbeat = sent;
                    s.heartbeat_due = false;
                    s.heartbeatSucceeded();
                    return false;
                }
                const cloned = cloneAssignment(s.c.alloc, reply.assignment) catch |err| {
                    s.pending_error = err;
                    s.hook_paused = true;
                    return true;
                };
                if (s.pending_beat) |pb| {
                    for (pb.assignment) |t| s.c.alloc.free(t.partitions);
                    s.c.alloc.free(pb.assignment);
                }
                s.pending_beat = .{
                    .sent = sent,
                    .code = reply.code,
                    .member_epoch = reply.member_epoch,
                    .heartbeat_interval_ms = reply.heartbeat_interval_ms,
                    .has_assignment = reply.assignment != null,
                    .assignment = cloned,
                };
                s.hook_paused = true;
                return true;
            } else |err| {
                s.in_flight_hb = null;
                if (err == error.GroupDeadlineExceeded or err == error.IoFailed or err == error.IoTimedOut) {
                    if (err == error.GroupDeadlineExceeded) s.hb_timeouts +|= 1;
                    s.hook_timeouts +|= 1;
                    s.startRecovery();
                    s.heartbeat_due = true;
                    if (s.hook_timeouts == 1) {
                        return s.startInFlightHeartbeat(&arena, true, in_hook);
                    }
                    if (in_hook) s.hook_paused = true;
                    return false;
                }
                s.pending_error = err;
                s.hook_paused = true;
                return true;
            }
        }

        const now = std.Io.Timestamp.now(s.c.io, .awake);
        const elapsed = s.last_heartbeat.durationTo(now).toMilliseconds();
        if (!s.heartbeat_due and elapsed < s.interval) return false;
        const full = s.heartbeat_due or s.hook_timeouts > 0 or s.hb_timeouts > 0;
        return s.startInFlightHeartbeat(&arena, full, in_hook);
    }

    fn heartbeatTick(ctx: *anyopaque) bool {
        const s: *GroupSession = @ptrCast(@alignCast(ctx));
        return s.stepHeartbeat(true);
    }

    fn drainPending(s: *GroupSession, arena: *std.heap.ArenaAllocator) !void {
        while (true) {
            if (s.pending_error) |err| {
                s.pending_error = null;
                s.hook_paused = false;
                return err;
            }
            if (s.pending_beat) |pb| {
                s.pending_beat = null;
                defer {
                    for (pb.assignment) |t| s.c.alloc.free(t.partitions);
                    s.c.alloc.free(pb.assignment);
                }
                s.hook_paused = false;
                s.attempt_started = pb.sent;
                s.applyHeartbeat(arena, .{
                    .code = pb.code,
                    .message = null,
                    .member_id = &s.member,
                    .member_epoch = pb.member_epoch,
                    .heartbeat_interval_ms = pb.heartbeat_interval_ms,
                    .assignment = if (pb.has_assignment) pb.assignment else null,
                }) catch |err| {
                    if (err == error.WaitHookAborted) continue;
                    return err;
                };
                continue;
            }
            s.hook_paused = false;
            return;
        }
    }

    fn beat(s: *GroupSession, arena: *std.heap.ArenaAllocator) !void {
        s.abortInFlightHeartbeat();
        s.beginHeartbeatAttempt();
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
            .retry_fields = .{
                .rebalance_timeout_ms = group_rebalance_timeout_ms,
                .topics = &.{s.opts.topic},
                .assignment = if (parts.len == 0) &.{} else &.{.{ .topic_id = s.c.topic_id, .partitions = parts }},
            },
        });
        try s.applyHeartbeat(arena, result);
    }

    fn applyHeartbeat(s: *GroupSession, arena: *std.heap.ArenaAllocator, result: protocol.HeartbeatResult) !void {
        const joining = s.epoch == 0;
        if (result.code == .fenced_member_epoch or result.code == .unknown_member_id) {
            s.rejoin();
            return;
        }
        try s.c.groupResult(result.code);
        s.joined_once = true;
        s.epoch = result.member_epoch;
        s.interval = @intCast(@max(1, result.heartbeat_interval_ms));
        s.last_heartbeat = if (s.attempt_started.nanoseconds > 0) s.attempt_started else std.Io.Timestamp.now(s.c.io, .awake);
        s.heartbeat_due = false;
        s.beginRound();
        if (joining) s.c.vlog("joined group '{s}', member {s}, epoch {d}", .{ s.opts.group.?, s.member, s.epoch });
        if (result.assignment) |assignment| try s.initializeAssignment(arena, assignment);
        s.heartbeatSucceeded();
    }

    fn initializeAssignment(s: *GroupSession, arena: *std.heap.ArenaAllocator, assignment: []const protocol.TopicAssignment) !void {
        _ = try s.c.groupRemainingMs();
        const alloc = arena.allocator();
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
        var next: std.ArrayList(Cursor) = .empty;
        defer next.deinit(s.c.alloc);
        for (assigned) |p| {
            var retained: ?Cursor = null;
            for (s.owned.items) |cursor| if (cursor.pidx == p) {
                retained = cursor;
            };
            try next.append(s.c.alloc, retained orelse .{
                .pidx = p,
                .leader = s.c.partitionLeader(p) orelse -1,
                .offset = -1,
                .awaiting_position = true,
                .initial_fetch_pending = true,
                .fetch_recovery_deadline = s.attempt_started.addDuration(.fromMilliseconds(group_session_assumed_ms)),
            });
        }
        var commit_error: ?anyerror = null;
        s.commit(alloc, true) catch |err| {
            // Leaving after a failed revoke commit lets the next owner replay progress.
            s.c.vlog("group '{s}': revoke commit failed ({s}); progress may replay", .{ s.opts.group.?, @errorName(err) });
            commit_error = err;
        };
        for (next.items) |*cursor| for (s.owned.items) |owned| {
            if (cursor.pidx == owned.pidx) cursor.* = owned;
        };
        std.mem.swap(std.ArrayList(Cursor), &s.owned, &next);
        s.positions_pending_initial = false;
        for (s.owned.items) |cursor| if (cursor.awaiting_position and !cursor.offsets_fetched) {
            s.positions_pending_initial = true;
            break;
        };
        var pending_offsets = false;
        for (s.owned.items) |cursor| if (pendingCommit(cursor) != null) {
            pending_offsets = true;
            break;
        };
        if (!pending_offsets) s.commit_recovery_deadline = null;
        s.heartbeat_due = true;
        s.c.vlog("group '{s}': assignment {d} partition(s), epoch {d}", .{ s.opts.group.?, s.owned.items.len, s.epoch });
        if (commit_error) |err| return err;
        s.resolvePositions(arena) catch |err| if (!positionRetry(err)) return err;
    }

    fn resolvePositions(s: *GroupSession, arena: *std.heap.ArenaAllocator) !void {
        var added: std.ArrayList(i32) = .empty;
        const alloc = arena.allocator();
        var pending = false;
        for (s.owned.items) |cursor| if (cursor.awaiting_position) {
            pending = true;
            if (!cursor.offsets_fetched) try added.append(alloc, cursor.pidx);
        };
        if (!pending) return;
        _ = try s.c.groupRemainingMs();
        defer {
            s.position_fetch_due = true;
            const initial = s.positions_pending_initial;
            s.positions_pending_initial = false;
            for (s.owned.items) |*cursor| {
                if (!cursor.awaiting_position) continue;
                if (cursor.offsets_fetched and cursor.offset >= 0) {
                    cursor.awaiting_position = false;
                    cursor.fetch_recovery_deadline = null;
                } else if (!cursor.position_attempted) s.positions_pending_initial = true;
            }
            if (initial and !s.positions_pending_initial) s.last_record = std.Io.Timestamp.now(s.c.io, .awake);
        }
        if (added.items.len > 0) {
            for (s.owned.items) |*cursor| if (cursor.awaiting_position and !cursor.offsets_fetched) {
                cursor.position_attempted = true;
            };
            const fetched = try s.c.fetchGroupOffsets(arena.allocator(), .{
                .group = s.opts.group.?,
                .member_id = &s.member,
                .member_epoch = s.epoch,
                .topics = &.{.{ .name = s.opts.topic, .partitions = added.items }},
            });
            for (s.owned.items) |*cursor| {
                if (!cursor.awaiting_position or cursor.offsets_fetched) continue;
                for (fetched.offsets) |offset| if (offset.partition == cursor.pidx) {
                    cursor.offset = offset.offset;
                    cursor.committed = offset.offset;
                    cursor.offsets_fetched = true;
                    cursor.position_attempted = false;
                };
            }
        }
        var leaders: std.ArrayList(i32) = .empty;
        for (s.owned.items) |cursor| {
            if (!cursor.awaiting_position or !cursor.offsets_fetched or cursor.offset >= 0) continue;
            if (std.mem.indexOfScalar(i32, leaders.items, cursor.leader) == null) try leaders.append(alloc, cursor.leader);
        }
        if (leaders.items.len == 0) return;
        std.mem.sort(i32, leaders.items, {}, std.sort.asc(i32));
        var leader = leaders.items[0];
        if (s.position_leader) |last| for (leaders.items) |node| {
            if (node > last) {
                leader = node;
                break;
            }
        };
        s.position_leader = leader;
        var selected: ?*Cursor = null;
        var refresh = leader < 0;
        for (s.owned.items) |*cursor| if (cursor.awaiting_position and cursor.offsets_fetched and cursor.offset < 0 and cursor.leader == leader) {
            cursor.position_attempted = true;
            selected = cursor;
            refresh = refresh or cursor.leader_stale;
        };
        if (refresh) {
            try s.c.refreshMetadata(s.opts.topic);
            for (s.owned.items) |*cursor| if (cursor.awaiting_position) {
                cursor.leader = s.c.partitionLeader(cursor.pidx) orelse -1;
                cursor.leader_stale = false;
            };
            leader = selected.?.leader;
            if (leader < 0) return error.RetryFetch;
            s.position_leader = leader;
        }
        var parts: std.ArrayList(ListPart) = .empty;
        for (s.owned.items) |*cursor| if (cursor.awaiting_position and cursor.offsets_fetched and cursor.offset < 0 and cursor.leader == leader) {
            cursor.position_attempted = true;
            try parts.append(alloc, .{ .pidx = cursor.pidx, .cursor = cursor });
        };
        listOffsetsLeader(s.c, alloc, s.opts.topic, parts.items, s.opts.start, s.opts.isolation) catch |err| {
            for (parts.items) |part| if (part.cursor.offset < 0) {
                part.cursor.leader_stale = true;
            };
            return err;
        };
    }

    fn positionRetry(err: anyerror) bool {
        return err == error.RetryFetch or err == error.GroupDeadlineExceeded or err == error.MetadataFailed or err == error.MetadataUnavailable;
    }

    fn leave(s: *GroupSession) void {
        s.abortInFlightHeartbeat();
        if (s.epoch <= 0) return;
        const saved_err = s.c.err_ctx;
        defer s.c.err_ctx = saved_err;
        if (s.recoveryDeadline() != null) {
            s.beginRound();
            if (s.c.groupRemainingMs()) |_| {} else |_| return;
        } else s.beginAttempt();
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
    c.io = std.testing.io;
    const now = std.Io.Timestamp.now(c.io, .awake);
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .group = "g" }, .member = @splat('m'), .last_heartbeat = now, .last_record = now };
    defer session.owned.deinit(std.testing.allocator);
    try session.owned.append(std.testing.allocator, .{ .pidx = 0, .leader = 1, .offset = 3, .emitted = 3 });
    session.epoch = 5;
    session.assignment_seen = true;
    session.heartbeat_due = false;
    session.commit_recovery_deadline = .zero;
    session.rejoin();
    try std.testing.expect(session.commit_recovery_deadline == null);
    try std.testing.expect(session.recovery_deadline != null);
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

test "fences after long stalls start a current bounded rejoin budget" {
    var c: client.Client = undefined;
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    c.cfg = &cfg;
    c.io = std.testing.io;
    const now = std.Io.Timestamp.now(c.io, .awake);
    const old = now.addDuration(.fromMilliseconds(-2 * group_session_assumed_ms));
    for ([_]anyerror{ error.FencedMemberEpoch, error.UnknownMemberId }) |err| {
        var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .group = "g" }, .member = @splat('m'), .epoch = 3, .last_heartbeat = old, .last_record = old, .assignment_seen = true };
        try std.testing.expect(session.recoverEpoch(err));
        try session.checkRecovery();
        const deadline = session.recovery_deadline.?;
        try std.testing.expect(deadline.nanoseconds >= now.addDuration(.fromMilliseconds(group_session_assumed_ms)).nanoseconds);
        try std.testing.expectEqual(@as(i32, 0), session.epoch);
        try std.testing.expect(session.heartbeat_due);
        try std.testing.expect(!session.assignment_seen);
        session.rejoin();
        try std.testing.expectEqual(deadline, session.recovery_deadline.?);
        session.recovery_deadline = .zero;
        session.rejoin();
        try std.testing.expectError(error.FetchFailed, session.checkRecovery());
    }
}

test "commit floor skips network requests until due" {
    var c: client.Client = undefined;
    c.io = std.testing.io;
    var session: GroupSession = undefined;
    session.c = &c;
    session.last_commit = std.Io.Timestamp.now(c.io, .awake);
    session.owned = .empty;
    defer session.owned.deinit(std.testing.allocator);
    try session.owned.append(std.testing.allocator, .{ .pidx = 0, .leader = 1, .offset = 2, .emitted = 2, .committed = 1 });
    try session.commit(std.testing.allocator, false);
}

test "no pending offsets clear commit recovery even within the commit floor" {
    var c: client.Client = undefined;
    c.io = std.testing.io;
    const now = std.Io.Timestamp.now(c.io, .awake);
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t" }, .member = @splat('m'), .last_heartbeat = now, .last_record = now, .last_commit = now, .commit_recovery_deadline = .zero };
    try session.commit(std.testing.allocator, false);
    try std.testing.expect(session.commit_recovery_deadline == null);
    try session.owned.append(std.testing.allocator, .{ .pidx = 0, .leader = 1, .offset = 2, .emitted = 2, .committed = 2 });
    defer session.owned.deinit(std.testing.allocator);
    session.commit_recovery_deadline = .zero;
    try session.commit(std.testing.allocator, true);
    try std.testing.expect(session.commit_recovery_deadline == null);
}

test "recovery deadlines retain the member epoch and flushed progress" {
    var c: client.Client = undefined;
    c.io = std.testing.io;
    const now = std.Io.Timestamp.now(c.io, .awake);
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t" }, .member = @splat('m'), .epoch = 3, .last_heartbeat = now, .last_record = now, .heartbeat_due = false };
    defer session.owned.deinit(std.testing.allocator);
    try session.owned.append(std.testing.allocator, .{ .pidx = 0, .leader = 1, .offset = 4, .emitted = 4, .committed = 2 });
    session.beginAttempt();
    try std.testing.expect(try c.groupRemainingMs() > session.interval - 1000);
    try std.testing.expect(try c.groupRemainingMs() <= session.interval);
    try std.testing.expect(session.recoverEpoch(error.GroupDeadlineExceeded));
    try std.testing.expect(session.heartbeat_due);
    try std.testing.expectEqual(@as(i32, 3), session.epoch);
    try std.testing.expectEqual(@as(usize, 1), session.owned.items.len);
    try std.testing.expectEqual(@as(?i64, 4), pendingCommit(session.owned.items[0]));
}

test "persistent group fetch recovery survives successful heartbeats and honors idle" {
    var c: client.Client = undefined;
    c.io = std.testing.io;
    const now = std.Io.Timestamp.now(c.io, .awake);
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t" }, .member = @splat('m'), .last_heartbeat = now, .last_record = now, .assignment_seen = true };
    try session.owned.append(std.testing.allocator, .{ .pidx = 0, .leader = 1, .offset = 0 });
    defer session.owned.deinit(std.testing.allocator);
    session.beginRound();
    session.startFetchRecovery(&session.owned.items[0]);
    const deadline = session.owned.items[0].fetch_recovery_deadline.?;
    try std.testing.expectEqual(session.attempt_started.addDuration(.fromMilliseconds(group_session_assumed_ms)), deadline);
    for (0..3) |_| {
        session.last_heartbeat = std.Io.Timestamp.now(c.io, .awake);
        session.heartbeatSucceeded();
        session.beginRound();
        session.startFetchRecovery(&session.owned.items[0]);
        try std.testing.expectEqual(deadline, session.owned.items[0].fetch_recovery_deadline.?);
        try std.testing.expect(c.group_deadline.?.nanoseconds <= deadline.nanoseconds);
        try std.testing.expect(try c.groupRemainingMs() <= session.interval);
        try session.checkRecovery();
    }
    session.owned.items[0].fetch_recovery_deadline = now.addDuration(.fromMilliseconds(-1));
    session.beginRound();
    try std.testing.expectError(error.FetchFailed, session.checkRecovery());
    try std.testing.expectEqualStrings("consumer group: recovery timed out", c.errDetail());
    session.epoch = 3;
    session.leave();
    try std.testing.expectError(error.GroupDeadlineExceeded, c.groupRemainingMs());

    session.owned.items[0].fetch_recovery_deadline = null;
    session.opts.idle_ms = 100;
    session.last_record = now.addDuration(.fromMilliseconds(-200));
    session.beginRound();
    try std.testing.expect(try c.groupRemainingMs() > 0);
    session.startFetchRecovery(&session.owned.items[0]);
    try std.testing.expectError(error.FetchFailed, session.checkRecovery());
    session.leave();
    try std.testing.expectError(error.GroupDeadlineExceeded, c.groupRemainingMs());

    session.assignment_seen = false;
    session.beginRound();
    try session.checkRecovery();
    try std.testing.expect(try c.groupRemainingMs() > session.interval - 1000);
    session.owned.items[0].fetch_recovery_deadline = null;
    try std.testing.expect(session.recoveryDeadline() == null);
    try session.checkRecovery();
}

test "a successful empty assignment clears recovery from an earlier fetch failure" {
    const alloc = std.testing.allocator;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    var c = client.Client.init(alloc, std.testing.io, &env, &cfg);
    defer c.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const now = std.Io.Timestamp.now(c.io, .awake);
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .group = "g" }, .member = @splat('m'), .last_heartbeat = now, .last_record = now, .assignment_seen = true };
    try session.owned.append(std.testing.allocator, .{ .pidx = 0, .leader = 1, .offset = 0 });
    defer session.owned.deinit(std.testing.allocator);
    session.beginRound();
    session.startFetchRecovery(&session.owned.items[0]);
    session.startRecovery();
    try std.testing.expect(session.recovery_deadline != null);
    try std.testing.expect(session.owned.items[0].fetch_recovery_deadline != null);
    var result: protocol.HeartbeatResult = .{ .code = .none, .message = null, .member_id = "m", .member_epoch = 1, .heartbeat_interval_ms = 5000, .assignment = &.{} };
    try session.applyHeartbeat(&arena, result);
    try std.testing.expect(session.recoveryDeadline() == null);

    const expired = now.addDuration(.fromMilliseconds(-group_session_assumed_ms));
    session.last_record = expired;
    session.recovery_deadline = expired;
    result.assignment = null;
    try session.applyHeartbeat(&arena, result);
    session.beginRound();
    try session.checkRecovery();
    try std.testing.expect(try c.groupRemainingMs() > session.interval - 1000);
    try std.testing.expectEqual(@as(usize, 0), session.owned.items.len);
}

test "partition outage recovery survives unrelated fetches and drops on revocation" {
    const alloc = std.testing.allocator;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    var c = client.Client.init(alloc, std.testing.io, &env, &cfg);
    defer c.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const now = std.Io.Timestamp.now(c.io, .awake);
    const expired = now.addDuration(.fromMilliseconds(-1));
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .group = "g" }, .member = @splat('m'), .last_heartbeat = now, .last_record = now, .assignment_seen = true };
    defer session.owned.deinit(alloc);
    try session.owned.append(alloc, .{ .pidx = 0, .leader = 1, .offset = 0, .fetch_recovery_deadline = expired });
    try session.owned.append(alloc, .{ .pidx = 1, .leader = 2, .offset = 0 });
    for (0..3) |_| {
        session.beginAttempt();
        session.startFetchRecovery(&session.owned.items[0]);
        session.owned.items[1].fetchSucceeded();
        session.heartbeatSucceeded();
        try std.testing.expectEqual(expired, session.recoveryDeadline().?);
        try std.testing.expectError(error.FetchFailed, session.checkRecovery());
    }
    session.opts.follow = true;
    try session.checkRecovery();
    try std.testing.expect(session.last_recovery_warning != null);
    session.recovery_deadline = expired;
    session.commit_recovery_deadline = expired;
    session.owned.items[0].fetchSucceeded();
    try std.testing.expectEqual(expired, session.recovery_deadline.?);
    try std.testing.expectEqual(expired, session.commit_recovery_deadline.?);
    session.recovery_deadline = null;
    session.commit_recovery_deadline = null;
    try std.testing.expect(session.recoveryDeadline() == null);
    session.owned.items[0].fetch_recovery_deadline = expired;
    session.beginAttempt();
    const assigned = [_]protocol.TopicAssignment{.{ .topic_id = c.topic_id, .partitions = &.{1} }};
    try session.initializeAssignment(&arena, &assigned);
    try std.testing.expect(session.recoveryDeadline() == null);
    try std.testing.expectEqual(@as(usize, 1), session.owned.items.len);
    try std.testing.expectEqual(@as(i32, 1), session.owned.items[0].pidx);
}

test "assignment initialization timeouts preserve the existing recovery budget" {
    var c: client.Client = undefined;
    c.io = std.testing.io;
    const now = std.Io.Timestamp.now(c.io, .awake);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .group = "g" }, .member = @splat('m'), .epoch = 3, .last_heartbeat = now, .last_record = now, .assignment_seen = true };
    const deadline = now.addDuration(.fromMilliseconds(-1));
    session.recovery_deadline = deadline;
    var assignment = [_]protocol.TopicAssignment{.{ .topic_id = @splat(0), .partitions = &.{0} }};
    const result: protocol.HeartbeatResult = .{ .code = .none, .message = null, .member_id = "m", .member_epoch = 4, .heartbeat_interval_ms = 5000, .assignment = &assignment };
    for (0..3) |_| {
        session.beginRound();
        try std.testing.expectError(error.GroupDeadlineExceeded, session.applyHeartbeat(&arena, result));
        try std.testing.expect(session.recoverEpoch(error.GroupDeadlineExceeded));
        try std.testing.expectEqual(deadline, session.recovery_deadline.?);
        try std.testing.expectEqual(@as(usize, 0), session.owned.items.len);
        try std.testing.expect(session.heartbeat_due);
        try std.testing.expectEqual(@as(i32, 4), session.epoch);
        try std.testing.expectError(error.FetchFailed, session.checkRecovery());
    }
}

test "unfinished assignments yield to heartbeats without losing resolved offsets" {
    const alloc = std.testing.allocator;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    var c = client.Client.init(alloc, std.testing.io, &env, &cfg);
    defer c.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const now = std.Io.Timestamp.now(c.io, .awake);
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .group = "g" }, .member = @splat('m'), .epoch = 2, .last_heartbeat = now, .last_record = now };
    defer session.owned.deinit(alloc);
    try session.owned.append(alloc, .{ .pidx = 0, .leader = 1, .offset = 12 });
    try session.owned.append(alloc, .{ .pidx = 1, .leader = 1, .offset = -1, .awaiting_position = true, .offsets_fetched = true, .fetch_recovery_deadline = now.addDuration(.fromMilliseconds(group_session_assumed_ms)) });
    try session.owned.append(alloc, .{ .pidx = 2, .leader = 1, .offset = 0 });
    var assignment = [_]protocol.TopicAssignment{.{ .topic_id = c.topic_id, .partitions = &.{ 0, 1 } }};
    var result: protocol.HeartbeatResult = .{ .code = .none, .message = null, .member_id = "m", .member_epoch = 2, .heartbeat_interval_ms = 1000, .assignment = &assignment };
    try session.applyHeartbeat(&arena, result);
    try std.testing.expectEqual(@as(usize, 2), session.owned.items.len);
    try std.testing.expectEqual(@as(i32, 1), session.owned.items[1].pidx);
    try std.testing.expect(session.owned.items[1].awaiting_position);
    const deadline = session.owned.items[1].fetch_recovery_deadline.?;
    session.owned.items[1].offsets_fetched = true;
    session.owned.items[1].leader = 1;
    c.group_deadline = .zero;
    try std.testing.expectError(error.GroupDeadlineExceeded, session.resolvePositions(&arena));
    try std.testing.expectEqual(@as(i64, 12), session.owned.items[0].offset);
    try std.testing.expectEqual(@as(i64, -1), session.owned.items[1].offset);
    result.assignment = null;
    try session.applyHeartbeat(&arena, result);
    try std.testing.expectEqual(deadline, session.owned.items[1].fetch_recovery_deadline.?);
    session.owned.items[1].offset = 20;
    session.beginAttempt();
    try session.resolvePositions(&arena);
    try std.testing.expect(!session.owned.items[1].awaiting_position);
    try std.testing.expect(session.recoveryDeadline() == null);
    try std.testing.expectEqual(@as(i64, 12), session.owned.items[0].offset);
    try std.testing.expectEqual(@as(i64, 20), session.owned.items[1].offset);
    session.last_heartbeat = now.addDuration(.fromMilliseconds(-750));
    session.beginRound();
    try std.testing.expect(try c.groupRemainingMs() >= group_rpc_min_timeout_ms - 100);
}

test "follow ignores expired recovery budgets without renewing them" {
    var c: client.Client = undefined;
    c.io = std.testing.io;
    const now = std.Io.Timestamp.now(c.io, .awake);
    const expired = now.addDuration(.fromMilliseconds(-1));
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .follow = true }, .member = @splat('m'), .epoch = 3, .last_heartbeat = now, .last_record = now, .recovery_deadline = expired };
    session.beginRound();
    try std.testing.expect(try c.groupRemainingMs() > 0);
    try session.checkRecovery();
    const warned = session.last_recovery_warning.?;
    session.startRecovery();
    try session.checkRecovery();
    try std.testing.expectEqual(warned, session.last_recovery_warning.?);
    try std.testing.expectEqual(expired, session.recovery_deadline.?);
    session.opts.follow = false;
    try std.testing.expectError(error.FetchFailed, session.checkRecovery());
    session.heartbeatSucceeded();
    try session.checkRecovery();
}

test "idle waits for initial positions and first ready fetches but not ongoing outages" {
    var c: client.Client = undefined;
    c.io = std.testing.io;
    const now = std.Io.Timestamp.now(c.io, .awake);
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .idle_ms = 1 }, .member = @splat('m'), .last_heartbeat = now, .last_record = .zero, .assignment_seen = true, .positions_pending_initial = true };
    try session.owned.append(std.testing.allocator, .{ .pidx = 0, .leader = 1, .offset = -1, .awaiting_position = true, .initial_fetch_pending = true });
    defer session.owned.deinit(std.testing.allocator);
    try std.testing.expect(!session.idleExpired(now));
    session.positions_pending_initial = false;
    try std.testing.expect(session.idleExpired(now));
    session.owned.items[0].awaiting_position = false;
    session.owned.items[0].offset = 0;
    try std.testing.expect(!session.idleExpired(now));
    session.beginRound();
    try std.testing.expect(try c.groupRemainingMs() > 0);
    const parts = [_]FetchPart{.{ .pidx = 0, .cursor = &session.owned.items[0] }};
    try std.testing.expect(session.applyFetchOutcomes(&parts, &.{.retry}));
    try std.testing.expect(session.idleExpired(now));
    try std.testing.expect(session.owned.items[0].fetch_recovery_deadline != null);
    session.position_fetch_due = true;
    try std.testing.expect(session.idleExpired(now));
    session.owned.items[0].leader_stale = false;
    try std.testing.expect(!session.idleExpired(now));
    session.position_fetch_due = false;
    try std.testing.expect(session.idleExpired(now));
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

test "short heartbeat intervals separate liveness and data budgets and schedule from send time" {
    try std.testing.expectEqual(@as(u64, 100), heartbeatSlack(5000));
    try std.testing.expectEqual(@as(u64, 100), heartbeatSlack(400));
    try std.testing.expectEqual(@as(u64, 25), heartbeatSlack(100));
    try std.testing.expectEqual(@as(u64, 12), heartbeatSlack(50));
    try std.testing.expectEqual(@as(u64, 0), heartbeatSlack(2));
    try std.testing.expectEqual(@as(u64, 0), heartbeatSlack(1));

    try std.testing.expectEqual(@as(u64, 4900), heartbeatTimeoutMs(5000, 0));
    try std.testing.expectEqual(@as(u64, 900), heartbeatTimeoutMs(1000, 0));
    try std.testing.expectEqual(@as(u64, 200), heartbeatTimeoutMs(100, 0));
    try std.testing.expectEqual(@as(u64, 400), heartbeatTimeoutMs(100, 1));
    try std.testing.expectEqual(@as(u64, 800), heartbeatTimeoutMs(100, 2));
    try std.testing.expectEqual(@as(u64, 50), heartbeatTimeoutMs(2, 0));
    try std.testing.expectEqual(@as(u64, 50), heartbeatTimeoutMs(1, 0));

    const alloc = std.testing.allocator;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    var c = client.Client.init(alloc, std.testing.io, &env, &cfg);
    defer c.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const now = std.Io.Timestamp.now(c.io, .awake);

    for ([_]u64{ 1000, 999, 100, 2, 1 }) |interval| {
        var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .group = "g" }, .member = @splat('m'), .epoch = 1, .interval = interval, .last_heartbeat = now.addDuration(.fromMilliseconds(-750)), .last_record = now };
        session.beginRound();
        try std.testing.expect(try c.groupRemainingMs() >= group_rpc_min_timeout_ms - 100);
        session.beginHeartbeatAttempt();
        try std.testing.expect(try c.groupRemainingMs() <= heartbeatTimeoutMs(interval, 0));
    }

    const sent = now.addDuration(.fromMilliseconds(-60));
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .group = "g" }, .member = @splat('m'), .epoch = 1, .interval = 100, .last_heartbeat = sent, .last_record = now, .attempt_started = sent };
    const result: protocol.HeartbeatResult = .{ .code = .none, .message = null, .member_id = "m", .member_epoch = 1, .heartbeat_interval_ms = 100, .assignment = null };
    try session.applyHeartbeat(&arena, result);
    try std.testing.expectEqual(sent, session.last_heartbeat);
}

const PendingHeartbeat = struct {
    sent: std.Io.Timestamp,
    code: protocol.ErrorCode,
    member_epoch: i32,
    heartbeat_interval_ms: i32,
    has_assignment: bool,
    assignment: []protocol.TopicAssignment,
};

fn cloneAssignment(alloc: std.mem.Allocator, assignment: ?[]const protocol.TopicAssignment) ![]protocol.TopicAssignment {
    const src = assignment orelse return &.{};
    const out = try alloc.alloc(protocol.TopicAssignment, src.len);
    var built: usize = 0;
    errdefer {
        for (out[0..built]) |t| alloc.free(t.partitions);
        alloc.free(out);
    }
    for (src, out) |t, *dst| {
        dst.* = .{ .topic_id = t.topic_id, .partitions = try alloc.dupe(i32, t.partitions) };
        built += 1;
    }
    return out;
}

fn runGroup(c: *client.Client, opts: Options, out: *std.Io.Writer) !u64 {
    defer c.group_deadline = null;
    var random: [16]u8 = undefined;
    c.io.randomSecure(&random) catch c.io.random(&random);
    const now = std.Io.Timestamp.now(c.io, .awake);
    var session: GroupSession = .{ .c = c, .opts = opts, .member = std.fmt.bytesToHex(random, .lower), .last_heartbeat = now, .last_record = now };
    defer session.owned.deinit(c.alloc);
    defer session.deinitPending();
    defer session.leave();
    c.wait_hook = session.waitHook();
    defer c.wait_hook = null;
    var arena = std.heap.ArenaAllocator.init(c.alloc);
    defer arena.deinit();
    var scratch: std.Io.Writer.Allocating = .init(c.alloc);
    defer scratch.deinit();
    var count: u64 = 0;
    var leader_index: usize = 0;
    var retry: client.Retry = .{};
    var commit_retry: client.Retry = .{};
    var beat_timeouts: u32 = 0;
    var exit_error: ?anyerror = null;
    while (true) {
        _ = arena.reset(.{ .retain_with_limit = round_arena_retain_limit });
        session.drainPending(&arena) catch |err| {
            if (!session.recoverEpoch(err)) {
                exit_error = err;
                break;
            }
            session.startRecovery();
        };
        if (opts.stop) |flag| if (flag.load(.acquire)) break;
        if (opts.max_records) |max| if (count >= max) break;
        if (opts.sink_closed) |closed| if (closed()) return error.WriteFailed;
        session.checkRecovery() catch {
            if (session.recoveryDeadline()) |deadline| {
                if (std.Io.Timestamp.now(c.io, .awake).nanoseconds >= deadline.nanoseconds) exit_error = error.GroupRecoveryExpired;
            }
            break;
        };
        if (session.idleExpired(std.Io.Timestamp.now(c.io, .awake))) break;
        session.beginRound();
        const elapsed = session.last_heartbeat.durationTo(std.Io.Timestamp.now(c.io, .awake)).toMilliseconds();
        if (session.epoch == 0) {
            session.beat(&arena) catch |err| {
                if (err == error.WaitHookAborted) continue;
                if (!session.recoverEpoch(err)) {
                    exit_error = err;
                    break;
                }
                session.startRecovery();
                beat_timeouts +|= 1;
                if (beat_timeouts > 1) {
                    session.beginRound();
                    _ = retry.pause(c);
                }
            };
            if (!session.heartbeat_due and session.epoch > 0) beat_timeouts = 0;
            if (session.heartbeat_due or session.epoch == 0) continue;
        } else if (session.in_flight_hb != null or session.heartbeat_due or elapsed >= session.interval) {
            if (session.stepHeartbeat(false)) continue;
            if (session.in_flight_hb == null and session.heartbeat_due) {
                beat_timeouts +|= 1;
                if (beat_timeouts > 1) {
                    session.beginRound();
                    _ = retry.pause(c);
                }
            } else if (session.in_flight_hb == null and !session.heartbeat_due) {
                beat_timeouts = 0;
            }
        }
        if (session.idleExpired(std.Io.Timestamp.now(c.io, .awake))) break;
        if (opts.follow and session.commit_recovery_deadline != null) {
            session.commit(arena.allocator(), true) catch |err| {
                if (err == error.WaitHookAborted) continue;
                if (!session.recoverEpoch(err)) {
                    exit_error = err;
                    break;
                }
                _ = commit_retry.pause(c);
                continue;
            };
            commit_retry = .{};
            session.stale_retries = 0;
            continue;
        }
        if (session.owned.items.len == 0) {
            c.sleep(@min(50, @max(1, session.interval / 4)));
            continue;
        }
        if (!session.position_fetch_due or !session.hasReadyPartition()) {
            session.beginRound();
            session.resolvePositions(&arena) catch |err| {
                if (err == error.WaitHookAborted) continue;
                if (!GroupSession.positionRetry(err)) {
                    if (!session.recoverEpoch(err)) {
                        exit_error = err;
                        break;
                    }
                    continue;
                }
            };
        }
        session.beginRound();
        const selected = for (0..session.owned.items.len) |offset| {
            const idx = (leader_index +% offset) % session.owned.items.len;
            if (session.fetchReady(session.owned.items[idx])) break idx;
        } else {
            if (!session.positions_pending_initial) _ = retry.pause(c);
            continue;
        };
        leader_index = selected +% 1;
        if (session.owned.items[selected].leader_stale) {
            c.refreshMetadata(opts.topic) catch |err| {
                if (err == error.WaitHookAborted) continue;
                if (!session.recoverEpoch(err)) {
                    exit_error = err;
                    break;
                }
                _ = retry.pause(c);
                continue;
            };
            for (session.owned.items) |*cursor| {
                cursor.leader = c.partitionLeader(cursor.pidx) orelse -1;
                cursor.leader_stale = false;
            }
        }
        const leader = session.owned.items[selected].leader;
        var parts: std.ArrayList(FetchPart) = .empty;
        for (0..session.owned.items.len) |offset| {
            const cursor = &session.owned.items[(selected + offset) % session.owned.items.len];
            if (cursor.leader == leader and !cursor.leader_stale and !cursor.awaiting_position) {
                try parts.append(arena.allocator(), .{ .pidx = cursor.pidx, .cursor = cursor });
            }
        }
        var fetch_opts = opts;
        const remaining = c.groupRemainingMs() catch {
            session.heartbeat_due = true;
            continue;
        };
        if (remaining <= 1) {
            c.sleep(remaining);
            continue;
        }
        fetch_opts.fetch_wait_ms = @min(c.cfg.fetch_max_wait_ms, @min(@max(50, session.interval), remaining - @min(1000, remaining / 2)));
        if (session.idleReady()) if (opts.idle_ms) |idle| {
            const idle_remaining = idle -| @as(u64, @intCast(@max(0, session.last_record.durationTo(std.Io.Timestamp.now(c.io, .awake)).toMilliseconds())));
            if (idle_remaining <= 1) {
                c.sleep(idle_remaining);
                continue;
            }
            fetch_opts.fetch_wait_ms = @min(fetch_opts.fetch_wait_ms.?, idle_remaining / 2);
        };
        var progress = false;
        const outcomes = fetchLeader(c, arena.allocator(), fetch_opts, parts.items, out, &scratch, &count, &progress) catch |err| {
            if (err == error.WaitHookAborted) {
                try out.flush();
                if (opts.sink_closed) |closed| if (closed()) return error.WriteFailed;
                continue;
            }
            exit_error = err;
            break;
        };
        const failed = session.applyFetchOutcomes(parts.items, outcomes);
        session.position_fetch_due = false;
        try out.flush();
        if (opts.sink_closed) |closed| if (closed()) return error.WriteFailed;
        if (progress) session.last_record = std.Io.Timestamp.now(c.io, .awake);
        if (session.in_flight_hb != null and session.stepHeartbeat(false)) continue;
        session.beginRound();
        session.commit(arena.allocator(), false) catch |err| {
            if (err == error.WaitHookAborted) continue;
            if (!session.recoverEpoch(err)) {
                exit_error = err;
                break;
            }
            if (opts.follow) _ = commit_retry.pause(c);
            continue;
        };
        session.stale_retries = 0;
        if (opts.stats) |s| s.maybeRender();
        if (failed) {
            if (!retry.pause(c)) {
                session.heartbeat_due = true;
                continue;
            }
        } else retry = .{};
    }
    c.wait_hook = null;
    session.abortInFlightHeartbeat();
    session.drainPending(&arena) catch {};
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
    if (!opts.follow and !session.joined_once) {
        c.setErr("consumer group: recovery ended before joining group '{s}'", .{opts.group.?});
        return error.GroupJoinFailed;
    }
    if (exit_error) |err| {
        if (err == error.GroupRecoveryExpired) {
            std.debug.print("kite: warning: consumer group recovery expired; next run may replay records\n", .{});
            c.setErr("consumer group: recovery expired; next run may replay records", .{});
        }
        return err;
    }
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
    isolation: protocol.Isolation,
) !void {
    var retry: client.Retry = .{};
    while (true) {
        listOffsetsOnce(c, arena.allocator(), topic, cursors, start, isolation) catch |err| {
            if (err != error.RetryFetch) return err;
            if (!retry.pause(c)) {
                _ = try c.groupRemainingMs();
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
    isolation: protocol.Isolation,
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
        listOffsetsLeader(c, alloc, topic, parts.items, start, isolation) catch |err| {
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
    isolation: protocol.Isolation,
) !void {
    const Ctx = struct {
        topic: []const u8,
        parts: []const ListPart,
        start: Options.Start,
        isolation: protocol.Isolation,
    };
    const body = struct {
        fn f(e: *Encoder, ctx: Ctx) protocol.ProtoError!void {
            try e.i32v(-1);
            try e.i8v(@intFromEnum(ctx.isolation));
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
    const leader = parts[0].cursor.leader;
    const conn = c.connFor(leader, client.Client.connKey(leader, 0)) catch return error.RetryFetch;
    const resp = c.sendRequestAlloc(alloc, conn, protocol.api_key.list_offsets, protocol.version.list_offsets, Ctx{
        .topic = topic,
        .parts = parts,
        .start = start,
        .isolation = isolation,
    }, body) catch |err| {
        if (err == error.WaitHookAborted) {
            c.dropConn(client.Client.connKey(leader, 0));
            return err;
        }
        return error.RetryFetch;
    };
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

const FetchOutcome = enum { success, retry, throttled, offset_reset };

test "read-committed fetch with records and no last stable offset fails closed" {
    const alloc = std.testing.allocator;
    var c: client.Client = undefined;
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    c.cfg = &cfg;
    var cursor: Cursor = .{ .pidx = 0, .leader = 1, .offset = 0 };
    const parts = [_]FetchPart{.{ .pidx = 0, .cursor = &cursor }};
    var batches = Encoder.init(alloc);
    defer batches.deinit();
    try protocol.encodeRecordBatch(&batches, &.{.{ .value = "record" }}, 0, -1, -1, -1, false);
    var e = Encoder.init(alloc);
    defer e.deinit();
    try e.i32v(0);
    try e.i16v(0);
    try e.i32v(0);
    try e.compactArrayLen(1);
    try e.compactString("t");
    try e.compactArrayLen(1);
    try e.i32v(0);
    try e.i16v(0);
    try e.i64v(1);
    try e.i64v(-1);
    try e.i64v(0);
    try e.compactArrayLen(0);
    try e.i32v(-1);
    try e.compactBytes(batches.written());
    try e.tagBuffer();
    try e.tagBuffer();
    try e.tagBuffer();
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    var scratch: std.Io.Writer.Allocating = .init(alloc);
    defer scratch.deinit();
    var count: u64 = 0;
    var progress = false;
    var outcomes = [_]FetchOutcome{.retry};
    var opts: Options = .{ .topic = "t", .group = "g" };
    try std.testing.expectError(error.FetchFailed, decodeFetch(&c, alloc, opts, &parts, e.written(), &outcomes, &out.writer, &scratch, &count, &progress));
    try std.testing.expectEqualStrings("fetch partition 0: read-committed response with records has invalid last stable offset -1", c.errDetail());
    try std.testing.expectEqualStrings("", out.written());
    try std.testing.expectEqual(@as(i64, 0), cursor.offset);
    try std.testing.expectEqual(@as(?i64, null), pendingCommit(cursor));

    opts.isolation = .read_uncommitted;
    _ = try decodeFetch(&c, alloc, opts, &parts, e.written(), &outcomes, &out.writer, &scratch, &count, &progress);
    try std.testing.expectEqualStrings("record\n", out.written());
    try std.testing.expectEqual(@as(?i64, 1), pendingCommit(cursor));
}

test "mixed fetch replies update only each partition's recovery" {
    const alloc = std.testing.allocator;
    var c: client.Client = undefined;
    c.io = std.testing.io;
    c.wait_hook = null;
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    c.cfg = &cfg;
    const now = std.Io.Timestamp.now(c.io, .awake);
    const expired = now.addDuration(.fromMilliseconds(-1));
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .group = "g" }, .member = @splat('m'), .last_heartbeat = now, .last_record = now, .attempt_started = now, .recovery_deadline = expired, .commit_recovery_deadline = expired };
    defer session.owned.deinit(alloc);
    for (0..3) |i| try session.owned.append(alloc, .{ .pidx = @intCast(i), .leader = 1, .offset = 0 });
    const parts = [_]FetchPart{
        .{ .pidx = 0, .cursor = &session.owned.items[0] },
        .{ .pidx = 1, .cursor = &session.owned.items[1] },
    };
    var e = Encoder.init(alloc);
    defer e.deinit();
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    var scratch: std.Io.Writer.Allocating = .init(alloc);
    defer scratch.deinit();
    var count: u64 = 0;
    var progress = false;
    for ([_][2]i16{ .{ 6, 0 }, .{ 0, 6 }, .{ 0, 0 }, .{ 6, 6 } }) |codes| {
        e.reset();
        try e.i32v(0);
        try e.i16v(0);
        try e.i32v(0);
        try e.compactArrayLen(1);
        try e.compactString("t");
        try e.compactArrayLen(2);
        for (codes, 0..) |code, i| {
            try e.i32v(@intCast(i));
            try e.i16v(code);
            try e.i64v(0);
            try e.i64v(-1);
            try e.i64v(-1);
            try e.compactArrayLen(0);
            try e.i32v(-1);
            try e.compactBytes(null);
            try e.tagBuffer();
        }
        try e.tagBuffer();
        try e.tagBuffer();
        var outcomes = [_]FetchOutcome{ .retry, .retry };
        _ = try decodeFetch(&c, alloc, session.opts, &parts, e.written(), &outcomes, &out.writer, &scratch, &count, &progress);
        session.owned.items[0].fetch_recovery_deadline = expired;
        session.owned.items[1].fetch_recovery_deadline = null;
        session.owned.items[2].fetch_recovery_deadline = expired;
        try std.testing.expectEqual(codes[0] != 0 or codes[1] != 0, session.applyFetchOutcomes(&parts, &outcomes));
        for (codes, outcomes, 0..) |code, outcome, i| {
            try std.testing.expectEqual(if (code == 0) FetchOutcome.success else FetchOutcome.retry, outcome);
            const deadline = session.owned.items[i].fetch_recovery_deadline;
            if (code == 0) try std.testing.expect(deadline == null) else try std.testing.expectEqual(if (i == 0) expired else now.addDuration(.fromMilliseconds(group_session_assumed_ms)), deadline.?);
        }
        try std.testing.expectEqual(expired, session.owned.items[2].fetch_recovery_deadline.?);
        try std.testing.expectEqual(expired, session.recovery_deadline.?);
        try std.testing.expectEqual(expired, session.commit_recovery_deadline.?);
    }
    var outcomes = [_]FetchOutcome{ .retry, .retry };
    _ = try decodeFetch(&c, alloc, session.opts, &parts, &@as([12]u8, @splat(0)), &outcomes, &out.writer, &scratch, &count, &progress);
    try std.testing.expectEqualSlices(FetchOutcome, &.{ .retry, .retry }, &outcomes);
    try std.testing.expectError(error.Truncated, decodeFetch(&c, alloc, session.opts, &parts, &.{}, &outcomes, &out.writer, &scratch, &count, &progress));
    try std.testing.expectEqual(@as(u64, 0), count);
    try std.testing.expect(!progress);
}

test "throttled empty fetches retain outage state and yield at the heartbeat deadline" {
    const alloc = std.testing.allocator;
    var c: client.Client = undefined;
    c.io = std.testing.io;
    c.wait_hook = null;
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    c.cfg = &cfg;
    const now = std.Io.Timestamp.now(c.io, .awake);
    const expired = now.addDuration(.fromMilliseconds(-1));
    var session: GroupSession = .{ .c = &c, .opts = .{ .topic = "t", .group = "g" }, .member = @splat('m'), .last_heartbeat = now, .last_record = now, .recovery_deadline = expired, .commit_recovery_deadline = expired };
    defer session.owned.deinit(alloc);
    try session.owned.append(alloc, .{ .pidx = 0, .leader = 1, .offset = 0, .fetch_recovery_deadline = expired });
    const parts = [_]FetchPart{.{ .pidx = 0, .cursor = &session.owned.items[0] }};
    var e = Encoder.init(alloc);
    defer e.deinit();
    try e.i32v(1000);
    try e.i16v(0);
    try e.i32v(0);
    try e.compactArrayLen(0);
    try e.tagBuffer();
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    var scratch: std.Io.Writer.Allocating = .init(alloc);
    defer scratch.deinit();
    var count: u64 = 0;
    var progress = false;
    var outcomes = [_]FetchOutcome{.retry};
    c.group_deadline = now.addDuration(.fromMilliseconds(10));
    _ = try decodeFetch(&c, alloc, session.opts, &parts, e.written(), &outcomes, &out.writer, &scratch, &count, &progress);
    try std.testing.expect(now.durationTo(std.Io.Timestamp.now(c.io, .awake)).toMilliseconds() < 500);
    try std.testing.expectEqual(FetchOutcome.throttled, outcomes[0]);
    try std.testing.expect(!session.applyFetchOutcomes(&parts, &outcomes));
    try std.testing.expectEqual(expired, session.owned.items[0].fetch_recovery_deadline.?);
    try std.testing.expectEqual(expired, session.recovery_deadline.?);
    try std.testing.expectEqual(expired, session.commit_recovery_deadline.?);
    try std.testing.expect(!session.owned.items[0].leader_stale);
    session.opts.group = null;
    e.written()[2] = 0;
    e.written()[3] = 5;
    const before = std.Io.Timestamp.now(c.io, .awake);
    _ = try decodeFetch(&c, alloc, session.opts, &parts, e.written(), &outcomes, &out.writer, &scratch, &count, &progress);
    try std.testing.expect(before.durationTo(std.Io.Timestamp.now(c.io, .awake)).toMilliseconds() >= 5);
    try std.testing.expectEqual(FetchOutcome.throttled, outcomes[0]);
}

fn fetchLeaderLegacy(
    c: *client.Client,
    alloc: std.mem.Allocator,
    opts: Options,
    parts: []const FetchPart,
    out: *std.Io.Writer,
    scratch: *std.Io.Writer.Allocating,
    count: *u64,
    progress: *bool,
) !void {
    const outcomes = try fetchLeader(c, alloc, opts, parts, out, scratch, count, progress);
    for (outcomes) |outcome| if (outcome == .retry) return error.RetryFetch;
}

const FetchRequestCtx = struct {
    topic: []const u8,
    parts: []const FetchPart,
    max_wait_ms: u64,
    max_bytes: usize,
    isolation: protocol.Isolation,
};

fn encodeFetchBody(e: *Encoder, ctx: FetchRequestCtx) protocol.ProtoError!void {
    const max_bytes: i32 = @intCast(@min(ctx.max_bytes, std.math.maxInt(i32)));
    try e.i32v(-1);
    try e.i32v(@intCast(ctx.max_wait_ms));
    try e.i32v(1);
    try e.i32v(max_bytes);
    try e.i8v(@intFromEnum(ctx.isolation));
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
        try e.i32v(max_bytes);
        try e.tagBuffer();
    }
    try e.tagBuffer();
    try e.compactArrayLen(0);
    try e.compactString("");
    try e.tagBuffer();
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
) ![]const FetchOutcome {
    const outcomes = try alloc.alloc(FetchOutcome, parts.len);
    @memset(outcomes, .retry);
    const leader = parts[0].cursor.leader;
    const conn = c.connFor(leader, client.Client.connKey(leader, 0)) catch {
        c.dropNode(leader);
        return outcomes;
    };
    const resp = c.sendRequestAlloc(alloc, conn, protocol.api_key.fetch, protocol.version.fetch, FetchRequestCtx{
        .topic = opts.topic,
        .parts = parts,
        .max_wait_ms = opts.fetch_wait_ms orelse c.cfg.fetch_max_wait_ms,
        .max_bytes = c.cfg.fetch_max_bytes,
        .isolation = opts.isolation,
    }, encodeFetchBody) catch |err| {
        if (err == error.WaitHookAborted) {
            c.dropConn(client.Client.connKey(leader, 0));
            return err;
        }
        c.dropNode(leader);
        switch (err) {
            error.ResponseTooLarge => {
                c.setErr("fetch from broker {d}: response exceeds {d} byte frame limit", .{ leader, transport.max_response_len });
                return error.FetchFailed;
            },
            error.OutOfMemory => {
                c.setErr("fetch from broker {d}: out of memory", .{leader});
                return error.OutOfMemory;
            },
            else => return outcomes,
        }
    };

    return decodeFetch(c, alloc, opts, parts, resp.body, outcomes, out, scratch, count, progress);
}

test "fetch request honors fetch.max.bytes for partition_max_bytes" {
    const alloc = std.testing.allocator;
    var cursors = [_]Cursor{
        .{ .pidx = 0, .leader = 1, .offset = 10 },
        .{ .pidx = 1, .leader = 1, .offset = 20 },
    };
    const parts = [_]FetchPart{
        .{ .pidx = 0, .cursor = &cursors[0] },
        .{ .pidx = 1, .cursor = &cursors[1] },
    };
    for ([_]usize{ 8 << 20, 24 << 20 }) |budget| {
        var e = Encoder.init(alloc);
        defer e.deinit();
        try encodeFetchBody(&e, .{
            .topic = "big",
            .parts = &parts,
            .max_wait_ms = 500,
            .max_bytes = budget,
            .isolation = .read_committed,
        });
        var d = Decoder.init(e.written());
        try std.testing.expectEqual(@as(i32, -1), try d.i32v());
        try std.testing.expectEqual(@as(i32, 500), try d.i32v());
        try std.testing.expectEqual(@as(i32, 1), try d.i32v());
        try std.testing.expectEqual(@as(i32, @intCast(budget)), try d.i32v());
        _ = try d.i8v();
        _ = try d.i32v();
        _ = try d.i32v();
        try std.testing.expectEqual(@as(i64, 1), try d.compactArrayLen());
        try std.testing.expectEqualStrings("big", (try d.compactString()).?);
        try std.testing.expectEqual(@as(i64, 2), try d.compactArrayLen());
        for (cursors) |cursor| {
            try std.testing.expectEqual(cursor.pidx, try d.i32v());
            _ = try d.i32v();
            try std.testing.expectEqual(cursor.offset, try d.i64v());
            _ = try d.i32v();
            _ = try d.i64v();
            try std.testing.expectEqual(@as(i32, @intCast(budget)), try d.i32v());
            try d.tagBuffer();
        }
    }
}

fn decodeFetch(
    c: *client.Client,
    alloc: std.mem.Allocator,
    opts: Options,
    parts: []const FetchPart,
    response: []const u8,
    outcomes: []FetchOutcome,
    out: *std.Io.Writer,
    scratch: *std.Io.Writer.Allocating,
    count: *u64,
    progress: *bool,
) ![]const FetchOutcome {
    const leader = parts[0].cursor.leader;
    var d = Decoder.init(response);
    const throttle_ms: u64 = @intCast(@max(0, try d.i32v()));
    const top_code: protocol.ErrorCode = @enumFromInt(try d.i16v());
    _ = try d.i32v();
    if (top_code != .none) {
        if (top_code.retriable()) {
            c.dropNode(leader);
            return outcomes;
        }
        c.setErr("fetch from broker {d}: {s}", .{ leader, top_code.name() });
        return error.FetchFailed;
    }
    const topics = try d.compactArrayLen();
    var has_records = false;
    var has_partition_error = false;
    var ti: i64 = 0;
    while (ti < topics) : (ti += 1) {
        _ = try d.compactString();
        const nparts = try d.compactArrayLen();
        var pi: i64 = 0;
        while (pi < nparts) : (pi += 1) {
            const pidx = try d.i32v();
            const code: protocol.ErrorCode = @enumFromInt(try d.i16v());
            const high_watermark = try d.i64v();
            const last_stable_offset = try d.i64v();
            const end_offset = if (opts.isolation == .read_committed) last_stable_offset else high_watermark;
            _ = try d.i64v();
            const aborted_count: usize = @intCast(@max(0, try d.compactArrayLen()));
            if (aborted_count > (d.data.len - d.pos) / 17) return error.Truncated;
            const aborted: []protocol.AbortedTransaction = if (opts.isolation == .read_committed and aborted_count > 0)
                try alloc.alloc(protocol.AbortedTransaction, aborted_count)
            else
                &.{};
            defer if (aborted.len > 0) alloc.free(aborted);
            for (0..aborted_count) |i| {
                const producer_id = try d.i64v();
                const first_offset = try d.i64v();
                try d.tagBuffer();
                if (i < aborted.len) aborted[i] = .{ .producer_id = producer_id, .first_offset = first_offset };
            }
            _ = try d.i32v();
            const records = try d.compactBytes();
            try d.tagBuffer();
            const partition_has_records = if (records) |blob| blob.len > 0 else false;
            has_records = has_records or partition_has_records;
            has_partition_error = has_partition_error or code != .none;
            const part_idx = for (parts, 0..) |part, idx| {
                if (part.pidx == pidx) break idx;
            } else continue;
            const part = parts[part_idx];
            if (code == .offset_out_of_range) {
                if (opts.start == .offset) {
                    try reportOffsetRange(c, alloc, opts, part.cursor);
                    return error.OffsetOutOfRange;
                }
                resetOffset(c, alloc, opts, part.cursor) catch |err| {
                    if (err != error.RetryFetch) return err;
                    continue;
                };
                outcomes[part_idx] = .offset_reset;
                continue;
            }
            if (code != .none) {
                if (code.retriable()) {
                    continue;
                }
                c.setErr("fetch partition {d}: {s}", .{ pidx, code.name() });
                return error.FetchFailed;
            }
            if (opts.isolation == .read_committed and last_stable_offset < 0 and partition_has_records) {
                c.setErr("fetch partition {d}: read-committed response with records has invalid last stable offset {d}", .{ pidx, last_stable_offset });
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
                next_from_batches = protocol.decodeBatches(alloc, blob, .{ .end_offset = end_offset, .aborted = aborted }, &ctx, onRecord) catch |err| switch (err) {
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
            outcomes[part_idx] = .success;
            const boundary = if (opts.isolation == .read_committed) "last stable offset" else "high watermark";
            if (range.first != null) {
                progress.* = true;
                c.vlog("fetch partition {d}: offsets {d}..{d}, {s} {d}, records bytes {d}", .{
                    pidx, range.first.?, range.last.?, boundary, end_offset, if (records) |blob| blob.len else 0,
                });
            } else {
                c.vlog("fetch partition {d}: offsets empty..empty, {s} {d}, records bytes {d}", .{
                    pidx, boundary, end_offset, if (records) |blob| blob.len else 0,
                });
            }
        }
        try d.tagBuffer();
    }
    try d.tagBuffer();
    if (throttle_ms > 0) {
        if (!has_records and !has_partition_error) @memset(outcomes, .throttled);
        var delay = if (opts.group != null) @min(throttle_ms, c.groupRemainingMs() catch 0) else throttle_ms;
        if (opts.group == null) if (opts.fetch_idle_deadline) |deadline| {
            const remaining = std.Io.Timestamp.now(c.io, .awake).durationTo(deadline).toMilliseconds();
            delay = @min(delay, @as(u64, @intCast(@max(0, remaining))));
        };
        c.sleep(delay);
        if (opts.group == null) if (opts.fetch_idle_deadline) |deadline| {
            if (std.Io.Timestamp.now(c.io, .awake).nanoseconds >= deadline.nanoseconds) return error.FetchIdleExpired;
        };
    }
    return outcomes;
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
    if (rec.value) |value| {
        try writeJsonBytesField(w, "value", "value_b64", value);
    } else {
        try w.writeAll("\"value\":null");
    }
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
    try w.writeAll(rec.value orelse "");
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
    const value = rec.value orelse "";
    switch (ctx.format) {
        .json => if (ctx.color)
            try writeJsonRecordColored(ctx.out, ctx.scratch.?, ctx.topic, ctx.pidx, offset, timestamp_ms, rec)
        else
            try writeJsonRecord(ctx.out, ctx.topic, ctx.pidx, offset, timestamp_ms, rec),
        .value => try ctx.out.writeAll(value),
        .tsv => try writeTsvRecord(ctx.out, rec, ctx.color),
        .auto, .csv => if (rec.key == null and rec.headers.len == 0)
            try ctx.out.writeAll(value)
        else
            try writeTsvRecord(ctx.out, rec, ctx.color),
    }
    try ctx.out.writeByte('\n');
    ctx.count.* += 1;
    if (ctx.stats) |s| {
        s.add(1, value.len + if (rec.key) |key| key.len else 0);
        s.noteOffset(ctx.pidx, offset);
    }
    ctx.range.first = ctx.range.first orelse offset;
    ctx.range.last = offset;
}

fn resetOffset(c: *client.Client, alloc: std.mem.Allocator, opts: Options, cursor: *Cursor) !void {
    const start: Options.Start = if (opts.start == .offset) .earliest else opts.start;
    const part = [_]ListPart{.{ .pidx = cursor.pidx, .cursor = cursor }};
    try listOffsetsLeader(c, alloc, opts.topic, &part, start, opts.isolation);
    c.vlog("partition {d}: offset out of range; reset to {d}", .{ cursor.pidx, cursor.offset });
}

fn reportOffsetRange(c: *client.Client, alloc: std.mem.Allocator, opts: Options, cursor: *Cursor) !void {
    const requested = cursor.offset;
    var probe = cursor.*;
    const part = [_]ListPart{.{ .pidx = probe.pidx, .cursor = &probe }};
    listOffsetsLeader(c, alloc, opts.topic, &part, .earliest, opts.isolation) catch {
        c.setErr("offset {d} is out of range for partition {d}", .{ requested, cursor.pidx });
        return;
    };
    const log_start = probe.offset;
    listOffsetsLeader(c, alloc, opts.topic, &part, .latest, .read_uncommitted) catch {
        c.setErr("offset {d} is out of range for partition {d}", .{ requested, cursor.pidx });
        return;
    };
    const high_watermark = probe.offset;
    var boundary_buf: [96]u8 = undefined;
    const boundary = if (opts.isolation == .read_committed) blk: {
        listOffsetsLeader(c, alloc, opts.topic, &part, .latest, .read_committed) catch break :blk "";
        break :blk std.fmt.bufPrint(&boundary_buf, "; last stable offset {d}", .{probe.offset}) catch unreachable;
    } else "";
    c.setErr("offset {d} is out of range for partition {d}: log start offset {d}; high watermark {d}{s}", .{
        requested, cursor.pidx, log_start, high_watermark, boundary,
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

test "json record output distinguishes tombstone and empty values" {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeJsonRecord(&w, "t", 0, 0, 1, .{ .key = "k", .value = null });
    try std.testing.expectEqualStrings(
        "{\"topic\":\"t\",\"partition\":0,\"offset\":0,\"timestamp\":1,\"key\":\"k\",\"headers\":[],\"value\":null}",
        w.buffered(),
    );
    w = .fixed(&buf);
    try writeJsonRecord(&w, "t", 0, 1, 1, .{ .key = "k", .value = "" });
    try std.testing.expectEqualStrings(
        "{\"topic\":\"t\",\"partition\":0,\"offset\":1,\"timestamp\":1,\"key\":\"k\",\"headers\":[],\"value\":\"\"}",
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

test "group leave preserves prior err_ctx when leave heartbeat fails" {
    const alloc = std.testing.allocator;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    var c = client.Client.init(alloc, std.testing.io, &env, &cfg);
    defer c.deinit();
    const now = std.Io.Timestamp.now(c.io, .awake);
    var session: GroupSession = .{
        .c = &c,
        .opts = .{ .topic = "t", .group = "g" },
        .member = @splat('m'),
        .epoch = 1,
        .last_heartbeat = now,
        .last_record = now,
    };
    c.setErr("fetch from broker 1: response exceeds 134217728 byte frame limit", .{});
    session.leave();
    try std.testing.expectEqualStrings(
        "fetch from broker 1: response exceeds 134217728 byte frame limit",
        c.errDetail(),
    );
}
