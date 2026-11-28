//! Kafka client: bootstrap connect, ApiVersions handshake, SASL auth,
//! Metadata v12 topic resolution, Produce v11 with retries.

const std = @import("std");
const config = @import("config.zig");
const protocol = @import("protocol.zig");
const transport = @import("transport.zig");
const scram = @import("scram.zig");
const term = @import("term.zig");
const stats = @import("stats.zig");
const cli = @import("cli.zig");

const Encoder = protocol.Encoder;
const Decoder = protocol.Decoder;
const Conn = transport.Conn;

const BrokerAddr = struct { host: []const u8, port: u16 };

fn parseBootstrapServer(server: []const u8) !BrokerAddr {
    const colon = std.mem.lastIndexOfScalar(u8, server, ':') orelse
        return .{ .host = server, .port = 9092 };
    const host = server[0..colon];
    const port_text = server[colon + 1 ..];
    if (host.len == 0 or port_text.len == 0) return error.InvalidBootstrapServer;
    const port = std.fmt.parseInt(u16, port_text, 10) catch return error.InvalidBootstrapServer;
    if (port == 0) return error.InvalidBootstrapServer;
    return .{ .host = host, .port = port };
}

pub const Partition = struct { index: i32, leader: i32 };

const ApiRange = struct { min: i16, max: i16 };

pub const Retry = struct {
    started: ?std.Io.Timestamp = null,
    backoff_ms: u64 = 100,

    pub fn pause(r: *Retry, c: *Client) bool {
        return r.pauseUnlessStopped(c, null);
    }

    pub fn pauseOrAbort(r: *Retry, c: *Client) error{WaitHookAborted}!bool {
        const now = std.Io.Timestamp.now(c.io, .awake);
        const start = r.started orelse now;
        r.started = start;
        const elapsed = start.durationTo(now).toMilliseconds();
        if (c.group_deadline == null and elapsed >= 30000) return false;
        const remaining = if (c.group_deadline != null) c.groupRemainingMs() catch return false else @as(u64, @intCast(30000 - elapsed));
        const delay = @min(remaining, r.backoff_ms);
        if (c.sleepAbortable(delay)) return error.WaitHookAborted;
        r.backoff_ms = @min(r.backoff_ms * 2, 3000);
        _ = c.groupRemainingMs() catch return false;
        return c.group_deadline != null or start.durationTo(std.Io.Timestamp.now(c.io, .awake)).toMilliseconds() < 30000;
    }

    fn pauseUnlessStopped(r: *Retry, c: *Client, stop: ?*const std.atomic.Value(bool)) bool {
        const now = std.Io.Timestamp.now(c.io, .awake);
        const start = r.started orelse now;
        r.started = start;
        const elapsed = start.durationTo(now).toMilliseconds();
        if (c.group_deadline == null and elapsed >= 30000) return false;
        const remaining = if (c.group_deadline != null) c.groupRemainingMs() catch return false else @as(u64, @intCast(30000 - elapsed));
        const delay = @min(remaining, r.backoff_ms);
        if (stop) |flag| {
            const until = now.addDuration(.fromMilliseconds(@intCast(delay)));
            while (!flag.load(.acquire)) {
                const left = std.Io.Timestamp.now(c.io, .awake).durationTo(until).toMilliseconds();
                if (left <= 0) break;
                c.sleep(@intCast(@min(left, 50)));
            }
            if (flag.load(.acquire)) return false;
        } else c.sleep(delay);
        r.backoff_ms = @min(r.backoff_ms * 2, 3000);
        _ = c.groupRemainingMs() catch return false;
        return c.group_deadline != null or start.durationTo(std.Io.Timestamp.now(c.io, .awake)).toMilliseconds() < 30000;
    }
};

pub const Client = struct {
    group_deadline: ?std.Io.Timestamp = null,
    wait_hook: ?transport.WaitHook = null,
    control_busy: bool = false,
    alloc: std.mem.Allocator,
    io: std.Io,
    env: *std.process.Environ.Map,
    cfg: *const config.Config,
    /// conn_key (node<<32 | slot) -> connection. Produce uses a small pool of
    /// connections per partition: while a partition has un-acked batches in
    /// flight they all ride the same socket (order preserved); once drained it
    /// migrates to the next slot, so throughput scales with connection count
    /// on ingest-capped services without ever racing a partition's records.
    conns: std.AutoHashMapUnmanaged(u64, *Conn),
    /// pidx -> the conn key it is currently bound to + outstanding count.
    inflight: std.AutoHashMapUnmanaged(i32, Inflight),
    /// pidx -> alternation counter for picking the partition's next conn slot.
    alt: std.AutoHashMapUnmanaged(i32, u32),
    /// conn used for metadata; also the bootstrap connection
    control: ?*Conn,
    /// node_id -> advertised broker address from metadata
    brokers: std.AutoHashMapUnmanaged(i32, BrokerAddr),
    /// controller node_id from the last Metadata response (-1 = unknown)
    controller_id: i32 = -1,
    coordinator_group: ?[]const u8 = null,
    coordinator_node: i32 = -1,
    coordinator_discovery_cursor: usize = 0,
    /// partition index -> leader node_id for the target topic
    partitions: std.ArrayListUnmanaged(Partition),
    topic_id: [16]u8 = @splat(0),
    /// api_key -> negotiated range, from the last ApiVersions response
    api_ranges: std.AutoHashMapUnmanaged(i16, ApiRange),
    ca: ?std.crypto.Certificate.Bundle,
    /// diagnostic detail for error messages
    err_ctx: [512]u8,
    /// Queue of sent-but-unacknowledged ProduceRequests; drained by
    /// produceDrain(). Lets callers keep filling batches while responses fly.
    outstanding: std.ArrayListUnmanaged(Outstanding),
    outstanding_bytes: usize,
    /// Idempotent produce state: issued by InitProducerId (-1 = none/off).
    producer_id: i64 = -1,
    producer_epoch: i16 = -1,
    producer_inited: bool = false,
    /// Transactional produce state (null = non-transactional).
    txn_id: ?[]const u8 = null,
    txn_coordinator_node: i32 = -1,
    /// Partitions acknowledged by AddPartitionsToTxn in this transaction.
    txn_added_parts: std.AutoHashMapUnmanaged(i32, void) = .empty,
    /// Set once an AddPartitionsToTxn request has been sent on a coordinator
    /// connection, so error cleanup still aborts if the reply is lost.
    txn_add_attempted: bool = false,
    txn_produce_unsettled_until: ?std.Io.Timestamp = null,
    /// Active cancellation flag during EndTxn(commit) discovery/reconnect.
    txn_stop: ?*const std.atomic.Value(bool) = null,
    /// Set once any transactional RPC reports PRODUCER_FENCED or INVALID_PRODUCER_EPOCH,
    /// so cleanup never sends KIP-360 InitProducerId after a replacement producer took over.
    txn_fenced: bool = false,
    /// Specific fencing error code reported by the broker, if known.
    txn_fence_code: ?protocol.ErrorCode = null,
    /// Set once EndTxn(commit) has been dispatched on the wire for this transaction;
    /// a subsequent fence cannot prove the transaction was aborted.
    txn_commit_dispatched: bool = false,
    last_offset: ?i64 = null,
    /// Highest acked offset per partition (sorted by pidx), for the summary.
    last_offsets: std.ArrayListUnmanaged(PartOffset) = .empty,
    acked_records: u64 = 0,
    produce_requests: u64 = 0,
    produce_retries: u64 = 0,
    /// Send-complete→response-received time for ProduceRequests. Per-attempt:
    /// each retried request is timed separately; failed sends/receives are
    /// excluded; pipelined requests include time queued behind earlier
    /// responses on the same connection.
    produce_ack_ns: u64 = 0,
    produce_acked: u64 = 0,
    /// pidx -> next base sequence number (idempotent produce only).
    seqs: std.AutoHashMapUnmanaged(i32, i32),

    pub fn init(alloc: std.mem.Allocator, io: std.Io, env: *std.process.Environ.Map, cfg: *const config.Config) Client {
        return .{
            .alloc = alloc,
            .io = io,
            .env = env,
            .cfg = cfg,
            .conns = .empty,
            .inflight = .empty,
            .alt = .empty,
            .control = null,
            .brokers = .empty,
            .partitions = .empty,
            .api_ranges = .empty,
            .ca = null,
            .err_ctx = std.mem.zeroes([512]u8),
            .outstanding = .empty,
            .outstanding_bytes = 0,
            .seqs = .empty,
        };
    }

    pub fn deinit(c: *Client) void {
        if (c.coordinator_group) |g| c.alloc.free(g);
        c.dropControl();
        var it = c.conns.valueIterator();
        while (it.next()) |conn| {
            conn.*.close();
            c.alloc.destroy(conn.*);
        }
        c.conns.deinit(c.alloc);
        c.inflight.deinit(c.alloc);
        c.alt.deinit(c.alloc);
        c.freeBrokers(&c.brokers);
        c.partitions.deinit(c.alloc);
        c.api_ranges.deinit(c.alloc);
        for (c.outstanding.items) |*o| {
            for (o.encoders.items) |*e| e.deinit();
            o.encoders.deinit(c.alloc);
            c.alloc.free(o.pidx);
            c.alloc.free(o.batches);
            c.alloc.free(o.counts);
        }
        c.outstanding.deinit(c.alloc);
        c.seqs.deinit(c.alloc);
        c.txn_added_parts.deinit(c.alloc);
        c.last_offsets.deinit(c.alloc);
        if (c.ca) |*b| b.deinit(c.alloc);
    }

    pub fn setErr(c: *Client, comptime fmt: []const u8, args: anytype) void {
        _ = std.fmt.bufPrintZ(&c.err_ctx, fmt, args) catch {};
    }
    pub fn errDetail(c: *const Client) []const u8 {
        return std.mem.sliceTo(&c.err_ctx, 0);
    }

    pub const PartOffset = struct { pidx: i32, offset: i64 };

    fn noteLastOffset(c: *Client, pidx: i32, off: i64) !void {
        var i: usize = 0;
        while (i < c.last_offsets.items.len and c.last_offsets.items[i].pidx < pidx) i += 1;
        if (i < c.last_offsets.items.len and c.last_offsets.items[i].pidx == pidx) {
            if (off > c.last_offsets.items[i].offset) c.last_offsets.items[i].offset = off;
            return;
        }
        try c.last_offsets.insert(c.alloc, i, .{ .pidx = pidx, .offset = off });
    }

    const ProduceResult = struct {
        code: protocol.ErrorCode = .none,
        message: ?[]const u8 = null,
    };

    fn produceErr(c: *Client, topic: []const u8, pidx: i32, result: ProduceResult, batch_len: usize) void {
        var size_buf: [32]u8 = undefined;
        switch (result.code) {
            .message_too_large => c.setErr(
                "produce to {s}[{d}]: batch of {s} exceeds the broker's max.message.bytes (split the input or raise the topic/broker limit)",
                .{ topic, pidx, stats.fmtBytes(batch_len, &size_buf) },
            ),
            .topic_authorization_failed => c.setErr(
                "produce to {s}: not authorized to write (check ACLs for this principal)",
                .{topic},
            ),
            else => c.setErr("produce to {s}[{d}]: {s}", .{ topic, pidx, result.code.name() }),
        }
        if (result.message) |message| {
            const hint = if (result.code == .invalid_record and std.ascii.findIgnoreCase(message, "without key") != null)
                "; if the topic is compacted, records need a non-null key"
            else
                "";
            c.appendProduceErr(message, hint);
        }
    }

    fn appendProduceErr(c: *Client, message: []const u8, hint: []const u8) void {
        const separator = ": ";
        const suffix = c.err_ctx[c.errDetail().len..];
        if (suffix.len <= hint.len) return;
        const available = suffix.len - hint.len - 1;
        const len = @min(message.len, available -| separator.len);
        var pos: usize = 0;
        if (len > 0) {
            @memcpy(suffix[0..separator.len], separator);
            for (suffix[separator.len..][0..len], message[0..len]) |*out, byte| {
                out.* = if (std.ascii.isPrint(byte)) byte else '?';
            }
            pos = separator.len + len;
        }
        @memcpy(suffix[pos..][0..hint.len], hint);
        suffix[pos + hint.len] = 0;
    }

    /// Verbose diagnostic to stderr, gated on `cfg.verbose` (-v).
    pub fn vlog(c: *Client, comptime fmt: []const u8, args: anytype) void {
        if (!c.cfg.verbose) return;
        if (term.color.enabled)
            std.debug.print("{s}kite:{s} " ++ fmt ++ "\n", .{ term.dim, term.reset } ++ args)
        else
            std.debug.print("kite: " ++ fmt ++ "\n", args);
    }

    fn loadCa(c: *Client) !*std.crypto.Certificate.Bundle {
        if (c.ca) |*b| return b;
        var bundle: std.crypto.Certificate.Bundle = .empty;
        if (c.cfg.ssl_truststore_location) |path| {
            const f = std.Io.Dir.cwd().openFile(c.io, path, .{}) catch {
                c.setErr("cannot open ssl.truststore.location '{s}'", .{path});
                return error.TlsCaLoadFailed;
            };
            defer f.close(c.io);
            var fbuf: [4096]u8 = undefined;
            var fr = f.reader(c.io, &fbuf);
            const now_sec = std.Io.Timestamp.now(c.io, .real).toSeconds();
            bundle.addCertsFromFile(c.alloc, &fr, now_sec) catch {
                c.setErr("failed parsing CA bundle '{s}'", .{path});
                return error.TlsCaLoadFailed;
            };
        } else {
            bundle.rescan(c.alloc, c.io, .now(c.io, .real)) catch {
                c.setErr("failed loading system CA bundle", .{});
                return error.TlsCaLoadFailed;
            };
        }
        c.ca = bundle;
        return &c.ca.?;
    }

    fn connectOne(c: *Client, host: []const u8, port: u16) !*Conn {
        const remaining = try c.groupRemainingMs();
        const ca: ?*std.crypto.Certificate.Bundle = if (c.cfg.needsTls())
            try c.loadCa()
        else
            null;
        const conn = transport.connect(c.io, c.alloc, c.env, host, port, ca, if (c.group_deadline != null) remaining else null) catch |err| {
            switch (err) {
                error.TlsHandshakeAuthFailed => c.setErr(
                    "TLS certificate verification failed for {s}:{d} (check ssl.truststore.location)",
                    .{ host, port },
                ),
                error.TlsFailed => c.setErr("TLS handshake failed for {s}:{d}", .{ host, port }),
                error.ConnectionRefused => c.setErr(
                    "connection refused by {s}:{d} (is a broker listening there?)",
                    .{ host, port },
                ),
                error.ConnectTimedOut => c.setErr(
                    "cannot reach {s}:{d} (timed out or unreachable; check network/firewall)",
                    .{ host, port },
                ),
                error.HostNotFound => c.setErr("cannot resolve host '{s}'", .{host}),
                else => c.setErr("cannot connect to {s}:{d}", .{ host, port }),
            }
            return error.AllBootstrapFailed;
        };
        if (c.txn_stop) |flag| {
            if (flag.load(.acquire)) {
                conn.close();
                c.alloc.destroy(conn);
                return error.Interrupted;
            }
            const dl = c.group_deadline orelse std.Io.Timestamp.now(c.io, .awake).addDuration(.fromMilliseconds(15000));
            conn.setReadCancellation(flag, dl);
        }
        c.postConnect(conn) catch |err| {
            conn.close();
            c.alloc.destroy(conn);
            if (err == error.WaitHookAborted) return error.WaitHookAborted;
            if (c.txn_stop) |flag| if (flag.load(.acquire)) return error.Interrupted;
            var prev: [512]u8 = undefined;
            const d = c.errDetail();
            const n = @min(d.len, prev.len);
            @memcpy(prev[0..n], d[0..n]);
            if (n > 0)
                c.setErr("handshake {s}:{d}: {s} — {s}", .{ host, port, @errorName(err), prev[0..n] })
            else
                c.setErr("handshake {s}:{d}: {s}", .{ host, port, @errorName(err) });
            return error.AllBootstrapFailed;
        };
        conn.setReadCancellation(null, null);
        return conn;
    }

    /// Establish the control connection: try each bootstrap server until one
    /// fully connects (TCP + TLS + SASL + ApiVersions).
    pub fn bootstrap(c: *Client) !void {
        var first_err_ctx: [512]u8 = undefined;
        var have_err = false;
        for (c.cfg.bootstrap_servers) |server| {
            if (c.txn_stop) |flag| if (flag.load(.acquire)) return error.Interrupted;
            const addr = parseBootstrapServer(server) catch {
                c.setErr("invalid bootstrap server '{s}' (want HOST or HOST:PORT with port 1..65535)", .{server});
                if (!have_err) {
                    @memcpy(&first_err_ctx, &c.err_ctx);
                    have_err = true;
                }
                continue;
            };
            const conn = c.connectOne(addr.host, addr.port) catch {
                if (!have_err) {
                    @memcpy(&first_err_ctx, &c.err_ctx);
                    have_err = true;
                }
                continue;
            };
            c.control = conn;
            c.vlog("bootstrap connected {s}:{d}", .{ addr.host, addr.port });
            return;
        }
        if (have_err) @memcpy(&c.err_ctx, &first_err_ctx);
        return error.AllBootstrapFailed;
    }

    /// TLS + SASL auth + ApiVersions on a fresh connection.
    fn postConnect(c: *Client, conn: *Conn) !void {
        if (c.cfg.needsSasl()) try c.saslAuth(conn);
        try c.apiVersions(conn);
        for ([_][2]i16{
            .{ protocol.api_key.produce, protocol.version.produce },
            .{ protocol.api_key.metadata, protocol.version.metadata },
            .{ protocol.api_key.fetch, protocol.version.fetch },
            .{ protocol.api_key.list_offsets, protocol.version.list_offsets },
        }) |kv| {
            if (!c.checkVersion(kv[0], kv[1])) {
                const r = c.api_ranges.get(kv[0]);
                c.setErr(
                    "broker does not support api_key {d} v{d} (range {d}..{d}) — Kafka 4.0+ required",
                    .{ kv[0], kv[1], if (r) |x| x.min else -1, if (r) |x| x.max else -1 },
                );
                return error.MalformedResponse;
            }
        }
    }

    const Resp = transport.Response;

    fn sendRequest(
        c: *Client,
        conn: *Conn,
        key: i16,
        ver: i16,
        ctx: anytype,
        comptime body_fn: fn (*Encoder, @TypeOf(ctx)) protocol.ProtoError!void,
    ) !Resp {
        return c.sendRequestAlloc(c.alloc, conn, key, ver, ctx, body_fn);
    }

    pub fn sendRequestAlloc(
        c: *Client,
        alloc: std.mem.Allocator,
        conn: *Conn,
        key: i16,
        ver: i16,
        ctx: anytype,
        comptime body_fn: fn (*Encoder, @TypeOf(ctx)) protocol.ProtoError!void,
    ) !Resp {
        const server_wait: u64 = switch (key) {
            protocol.api_key.create_topics, protocol.api_key.delete_topics, protocol.api_key.create_partitions => 30000,
            else => if (comptime @typeInfo(@TypeOf(ctx)) == .@"struct" and @hasField(@TypeOf(ctx), "max_wait_ms")) ctx.max_wait_ms else 0,
        };
        const timeout = try c.requestTimeoutMs(server_wait);
        transport.setTimeout(conn, timeout);
        var e = Encoder.init(alloc);
        defer e.deinit();
        // ApiVersions and SaslHandshake are the non-/semi-flexible oddballs:
        // ApiVersions answers header v0; SaslHandshake v1 is not flexible at
        // all (header v1 request, header v0 response, legacy string types).
        const flexible = key != protocol.api_key.sasl_handshake;
        const resp_tags = key != protocol.api_key.api_versions and key != protocol.api_key.sasl_handshake;
        try protocol.encodeRequest(&e, key, ver, flexible, "kite", ctx, body_fn);
        try transport.send(conn, e.written());
        const on_control = c.control != null and conn == c.control.?;
        const prev_control_busy = c.control_busy;
        if (on_control) c.control_busy = true;
        defer if (on_control) {
            c.control_busy = prev_control_busy;
        };
        const active_hook: ?transport.WaitHook =
            if (c.wait_hook != null and key != protocol.api_key.consumer_group_heartbeat and key != protocol.api_key.find_coordinator and key != protocol.api_key.api_versions)
                c.wait_hook
            else
                null;
        const prev_hook = conn.net_reader.wait_hook;
        const prev_cancel = conn.net_reader.cancellation;
        if (active_hook) |hook| conn.net_reader.wait_hook = hook;
        if (active_hook != null or c.group_deadline != null) {
            const new_dl = c.group_deadline orelse std.Io.Timestamp.now(c.io, .awake).addDuration(.fromMilliseconds(@intCast(timeout)));
            const dl = if (prev_cancel) |pc| (if (pc.deadline.nanoseconds < new_dl.nanoseconds) pc.deadline else new_dl) else new_dl;
            const stop = if (prev_cancel) |pc| pc.stop else null;
            conn.setReadCancellation(stop, dl);
        }
        defer {
            conn.net_reader.wait_hook = prev_hook;
            conn.net_reader.cancellation = prev_cancel;
        }
        return try transport.recv(conn, alloc, protocol.lastCorrelationId(), resp_tags);
    }

    fn requestTimeoutMs(c: *Client, server_wait_ms: u64) !u64 {
        const timeout = @max(15000, server_wait_ms +| 1000);
        if (c.group_deadline == null) return timeout;
        const remaining = try c.groupRemainingMs();
        if (remaining <= server_wait_ms) return error.GroupDeadlineExceeded;
        return @min(remaining, timeout);
    }

    // -- ApiVersions ---------------------------------------------------------

    fn apiVersions(c: *Client, conn: *Conn) !void {
        const Ctx = struct {};
        const body = struct {
            fn f(e: *Encoder, _: Ctx) protocol.ProtoError!void {
                try e.compactString("kite");
                try e.compactString(cli.version);
                try e.tagBuffer();
            }
        }.f;
        const resp = try c.sendRequest(conn, protocol.api_key.api_versions, protocol.version.api_versions, Ctx{}, body);
        defer c.alloc.free(resp.frame);

        var d = Decoder.init(resp.body);
        const err_code = try d.i16v();
        if (err_code != 0) {
            c.setErr("ApiVersions: {s}", .{protocol.ErrorCode.name(@enumFromInt(err_code))});
            return error.MalformedResponse;
        }
        const n = try d.compactArrayLen();
        var i: i64 = 0;
        while (i < n) : (i += 1) {
            const k = try d.i16v();
            const mn = try d.i16v();
            const mx = try d.i16v();
            try d.tagBuffer();
            try c.api_ranges.put(c.alloc, k, .{ .min = mn, .max = mx });
        }
    }

    pub fn checkVersion(c: *Client, key: i16, want: i16) bool {
        const r = c.api_ranges.get(key) orelse return false;
        return want >= r.min and want <= r.max;
    }

    // -- SASL ----------------------------------------------------------------

    fn saslAuth(c: *Client, conn: *Conn) !void {
        const mech = config.mechName(c.cfg.sasl_mechanism.?);

        const hs = struct {
            fn f(e: *Encoder, m: []const u8) protocol.ProtoError!void {
                try e.string(m); // SaslHandshake v1 is not a flexible version
            }
        }.f;
        const resp = try c.sendRequest(conn, protocol.api_key.sasl_handshake, protocol.version.sasl_handshake, mech, hs);
        defer c.alloc.free(resp.frame);
        var d = Decoder.init(resp.body);
        const err_code = try d.i16v();
        if (err_code != 0) {
            const ec: protocol.ErrorCode = @enumFromInt(err_code);
            c.setErr("SaslHandshake mechanism={s}: {s}", .{ mech, ec.name() });
            return error.SaslHandshakeFailed;
        }

        switch (c.cfg.sasl_mechanism.?) {
            .plain => try c.saslPlain(conn),
            .scram_sha_256 => try c.saslScram(conn, .sha256),
            .scram_sha_512 => try c.saslScram(conn, .sha512),
        }
    }

    const SaslReply = struct {
        frame: []u8,
        auth_bytes: []const u8,
        err_code: i16,
        err_msg: ?[]const u8,
    };

    /// One SaslAuthenticate round. Caller frees `reply.frame` after consuming
    /// auth_bytes / err_msg (both slice into it).
    fn saslToken(c: *Client, conn: *Conn, token: []const u8) !SaslReply {
        const body = struct {
            fn f(e: *Encoder, t: []const u8) protocol.ProtoError!void {
                try e.compactBytes(t);
                try e.tagBuffer();
            }
        }.f;
        const resp = try c.sendRequest(conn, protocol.api_key.sasl_authenticate, protocol.version.sasl_authenticate, token, body);
        var d = Decoder.init(resp.body);
        const code = try d.i16v();
        const msg = try d.compactString();
        const auth_bytes = try d.compactBytes();
        return .{ .frame = resp.frame, .auth_bytes = auth_bytes orelse "", .err_code = code, .err_msg = msg };
    }

    fn saslPlain(c: *Client, conn: *Conn) !void {
        const user = c.cfg.sasl_username.?;
        const pass = c.cfg.sasl_password.?;
        const token = try std.fmt.allocPrint(c.alloc, "\x00{s}\x00{s}", .{ user, pass });
        defer c.alloc.free(token);
        const r = try c.saslToken(conn, token);
        defer c.alloc.free(r.frame);
        if (r.err_code != 0) {
            c.setErr("SASL PLAIN auth failed: {s}", .{r.err_msg orelse protocol.ErrorCode.name(@enumFromInt(r.err_code))});
            return error.SaslAuthFailed;
        }
    }

    fn saslScram(c: *Client, conn: *Conn, comptime sha: scram.Sha) !void {
        const S = scram.Scram(sha);
        const first = try S.clientFirst(c.io, c.alloc, c.cfg.sasl_username.?);
        defer c.alloc.free(first.msg);
        defer c.alloc.free(first.state.client_first_bare);
        var st = first.state;

        const r = try c.saslToken(conn, first.msg);
        defer c.alloc.free(r.frame);
        if (r.err_code != 0) {
            c.setErr("SCRAM client-first rejected: {s}", .{r.err_msg orelse ""});
            return error.SaslAuthFailed;
        }

        const final_msg = st.serverFirst(c.alloc, r.auth_bytes, c.cfg.sasl_password.?) catch {
            c.setErr("SCRAM server-first parse failed", .{});
            return error.ScramFailed;
        };
        defer c.alloc.free(final_msg);

        const r2 = try c.saslToken(conn, final_msg);
        defer c.alloc.free(r2.frame);
        if (r2.err_code != 0) {
            c.setErr("SCRAM client-final rejected: {s}", .{r2.err_msg orelse ""});
            return error.SaslAuthFailed;
        }
        st.serverFinal(r2.auth_bytes) catch {
            c.setErr("SCRAM server signature verification failed", .{});
            return error.SaslAuthFailed;
        };
    }

    // -- Metadata ------------------------------------------------------------

    /// Fetch metadata for `topic` over the control connection; refreshes the
    /// broker map and the topic's partition->leader table.
    pub fn refreshMetadata(c: *Client, topic: []const u8) !void {
        const resp = try c.metadataRequest(topic);
        defer c.alloc.free(resp.frame);

        var d = Decoder.init(resp.body);
        _ = try d.i32v(); // throttle
        try c.decodeBrokers(&d);
        _ = try d.compactString(); // cluster id
        c.controller_id = try d.i32v();

        if (try d.compactArrayLen() != 1) return error.MalformedResponse;
        const terr: protocol.ErrorCode = @enumFromInt(try d.i16v());
        const name = (try d.compactString()) orelse return error.MalformedResponse;
        if (!std.mem.eql(u8, name, topic)) return error.MalformedResponse;
        if (terr != .none) {
            c.setErr("metadata for topic '{s}': {s}", .{ topic, terr.name() });
            return switch (terr) {
                .unknown_topic_or_partition => error.TopicNotFound,
                .topic_authorization_failed => error.TopicAuthorizationFailed,
                else => if (terr.retriable()) error.MetadataUnavailable else error.MetadataFailed,
            };
        }
        if (d.data.len - d.pos < 16) return error.Truncated;
        const topic_id = d.data[d.pos..][0..16].*;
        try d.skip(16);
        _ = try d.boolean(); // is_internal
        const nparts = try d.compactArrayLen();
        if (nparts <= 0) {
            c.setErr("topic '{s}' has no partitions", .{topic});
            return error.MetadataFailed;
        }
        var partitions: std.ArrayListUnmanaged(Partition) = .empty;
        defer partitions.deinit(c.alloc);
        var p: i64 = 0;
        while (p < nparts) : (p += 1) {
            const part_error = try d.i16v();
            const pidx = try d.i32v();
            const leader = try d.i32v();
            _ = try d.i32v(); // leader epoch
            try d.skipCompactI32Array(); // replicas
            try d.skipCompactI32Array(); // isr
            try d.skipCompactI32Array(); // offline replicas
            try d.tagBuffer();
            try partitions.append(c.alloc, .{ .index = pidx, .leader = if (part_error == 0) leader else -1 });
        }
        _ = try d.i32v(); // topic_authorized_operations
        try d.tagBuffer();
        try d.tagBuffer();
        std.mem.swap(std.ArrayListUnmanaged(Partition), &c.partitions, &partitions);
        c.topic_id = topic_id;
        c.vlog("metadata: topic '{s}' has {d} partition(s)", .{ topic, c.partitions.items.len });
    }

    pub fn partitionCount(c: *const Client) usize {
        return c.partitions.items.len;
    }

    fn brokerNodeAt(c: *Client, idx: usize) ?i32 {
        const count = c.brokers.count();
        if (count == 0) return null;
        const target = idx % count;
        var it = c.brokers.keyIterator();
        var i: usize = 0;
        while (it.next()) |node| : (i += 1) {
            if (i == target) return node.*;
        }
        return null;
    }

    fn candidateDeadline(c: *const Client, remaining_candidates: usize) ?std.Io.Timestamp {
        const dl = c.group_deadline orelse return null;
        if (remaining_candidates <= 1) return dl;
        const now = std.Io.Timestamp.now(c.io, .awake);
        const rem = now.durationTo(dl).toMilliseconds();
        if (rem <= 0) return dl;
        const share: u64 = @max(@as(u64, 150), @as(u64, @intCast(rem)) / remaining_candidates);
        const cap = @min(@as(u64, @intCast(rem)), share);
        return now.addDuration(.fromMilliseconds(@intCast(cap)));
    }

    pub fn findCoordinator(c: *Client, group: []const u8) !i32 {
        if (c.coordinator_group) |cached| {
            if (std.mem.eql(u8, cached, group) and c.coordinator_node >= 0) return c.coordinator_node;
        }
        try c.requireVersion(protocol.api_key.find_coordinator, protocol.version.find_coordinator, "FindCoordinator");
        var retry: Retry = .{};
        var tried_control: usize = 0;
        while (true) {
            const resp = if (c.control_busy) blk: {
                const count = c.brokers.count();
                if (count == 0) return error.MetadataFailed;
                var tried: usize = 0;
                var last_err: anyerror = error.MetadataFailed;
                while (tried < count) : (tried += 1) {
                    const node = c.brokerNodeAt(c.coordinator_discovery_cursor) orelse break;
                    c.coordinator_discovery_cursor +%= 1;
                    const prev_deadline = c.group_deadline;
                    c.group_deadline = c.candidateDeadline(count - tried);
                    defer c.group_deadline = prev_deadline;
                    const conn = c.connFor(node, connKey(node, 0xFFFF_FFFC)) catch |err| {
                        c.dropConn(connKey(node, 0xFFFF_FFFC));
                        last_err = err;
                        continue;
                    };
                    if (c.sendRequest(conn, protocol.api_key.find_coordinator, protocol.version.find_coordinator, group, protocol.findCoordinatorRequest)) |r| {
                        break :blk r;
                    } else |err| {
                        c.dropConn(connKey(node, 0xFFFF_FFFC));
                        last_err = err;
                        continue;
                    }
                }
                if (try retry.pauseOrAbort(c)) continue;
                _ = try c.groupRemainingMs();
                return last_err;
            } else blk: {
                const count = c.brokers.count();
                const rem_candidates = if (count > tried_control) count - tried_control else 1;
                const prev_deadline = c.group_deadline;
                c.group_deadline = c.candidateDeadline(rem_candidates);
                const attempt: anyerror!Resp = inner: {
                    defer c.group_deadline = prev_deadline;
                    if (c.control == null) {
                        const skipped = c.reconnectControlAt(c.coordinator_discovery_cursor) catch |err| break :inner err;
                        if (skipped > 0) {
                            tried_control += skipped;
                            c.coordinator_discovery_cursor +%= skipped;
                            c.group_deadline = prev_deadline;
                            const send_rem = if (count > tried_control) count - tried_control else 1;
                            c.group_deadline = c.candidateDeadline(send_rem);
                        }
                    }
                    break :inner c.sendRequest(c.control.?, protocol.api_key.find_coordinator, protocol.version.find_coordinator, group, protocol.findCoordinatorRequest);
                };
                break :blk attempt catch |err| {
                    c.dropControl();
                    c.coordinator_discovery_cursor +%= 1;
                    tried_control += 1;
                    if (count > 1 and tried_control < count) {
                        if (c.groupRemainingMs()) |_| continue else |_| {}
                    }
                    tried_control = 0;
                    if (try retry.pauseOrAbort(c)) continue;
                    _ = try c.groupRemainingMs();
                    return err;
                };
            };
            defer c.alloc.free(resp.frame);
            var d = Decoder.init(resp.body);
            const coord = try protocol.coordinatorResponse(&d);
            if (isCoordinatorError(coord.code) and try retry.pauseOrAbort(c)) continue;
            _ = try c.groupRemainingMs();
            try c.groupResult(coord.code);
            if (coord.node < 0 or coord.host.len == 0 or coord.port < 1 or coord.port > 65535) return error.MalformedResponse;
            const name = try c.alloc.dupe(u8, group);
            errdefer c.alloc.free(name);
            const host = try c.alloc.dupe(u8, coord.host);
            errdefer c.alloc.free(host);
            const entry = try c.brokers.getOrPut(c.alloc, coord.node);
            if (entry.found_existing) c.alloc.free(entry.value_ptr.host);
            entry.value_ptr.* = .{ .host = host, .port = @intCast(coord.port) };
            if (c.coordinator_group) |old| c.alloc.free(old);
            c.coordinator_group = name;
            c.coordinator_node = coord.node;
            return coord.node;
        }
    }

    pub fn groupRemainingMs(c: *Client) !u64 {
        const deadline = c.group_deadline orelse return 15000;
        const remaining = std.Io.Timestamp.now(c.io, .awake).durationTo(deadline).toMilliseconds();
        if (remaining <= 0) return error.GroupDeadlineExceeded;
        return @intCast(remaining);
    }

    pub fn isCoordinatorError(code: protocol.ErrorCode) bool {
        return code == .not_coordinator or code == .coordinator_not_available or code == .group_load_in_progress;
    }

    pub fn groupResult(c: *Client, code: protocol.ErrorCode) !void {
        if (code == .none) return;
        c.setErr("consumer group: {s}", .{code.name()});
        if (code == .fenced_member_epoch) return error.FencedMemberEpoch;
        if (code == .unknown_member_id) return error.UnknownMemberId;
        if (code == .stale_member_epoch) return error.StaleMemberEpoch;
        if (code == .group_id_not_found) return error.GroupNotFound;
        return error.GroupFailed;
    }

    pub fn invalidateCoordinator(c: *Client) void {
        if (c.coordinator_node >= 0) {
            c.dropConn(connKey(c.coordinator_node, 0xFFFF_FFFE));
            c.dropConn(connKey(c.coordinator_node, 0xFFFF_FFFD));
        }
        c.coordinator_node = -1;
    }

    const GroupResp = struct {
        resp: Resp,
        node: i32,
    };

    fn invalidateCoordinatorEndpoint(c: *Client, node: i32, slot: u32) void {
        if (node >= 0) c.dropConn(connKey(node, slot));
        if (c.coordinator_node == node) c.coordinator_node = -1;
    }

    pub fn groupRequest(c: *Client, alloc: std.mem.Allocator, group: []const u8, key: i16, ver: i16, api: []const u8, ctx: anytype, comptime encode: fn (*Encoder, @TypeOf(ctx)) protocol.ProtoError!void) !GroupResp {
        try c.requireVersion(key, ver, api);
        const slot: u32 = if (key == protocol.api_key.consumer_group_heartbeat) 0xFFFF_FFFD else 0xFFFF_FFFE;
        var retry: Retry = .{};
        var request = ctx;
        while (true) {
            const node = try c.findCoordinator(group);
            const conn = c.connFor(node, connKey(node, slot)) catch |err| {
                c.invalidateCoordinatorEndpoint(node, slot);
                if (comptime @TypeOf(ctx) == protocol.Heartbeat) request = request.forRetry();
                if (try retry.pauseOrAbort(c)) continue;
                _ = try c.groupRemainingMs();
                return err;
            };
            const resp = c.sendRequestAlloc(alloc, conn, key, ver, request, encode) catch |err| {
                if (err == error.WaitHookAborted) {
                    c.dropConn(connKey(node, slot));
                    return err;
                }
                c.invalidateCoordinatorEndpoint(node, slot);
                if (comptime @TypeOf(ctx) == protocol.Heartbeat) request = request.forRetry();
                if (try retry.pauseOrAbort(c)) continue;
                _ = try c.groupRemainingMs();
                return err;
            };
            return .{ .resp = resp, .node = node };
        }
    }

    pub fn heartbeat(c: *Client, alloc: std.mem.Allocator, req: protocol.Heartbeat) !protocol.HeartbeatResult {
        var retry: Retry = .{};
        var request = req;
        while (true) {
            const res = try c.groupRequest(alloc, req.group, protocol.api_key.consumer_group_heartbeat, protocol.version.consumer_group_heartbeat, "ConsumerGroupHeartbeat", request, protocol.heartbeatRequest);
            var d = Decoder.init(res.resp.body);
            const result = try protocol.heartbeatResponse(&d, alloc);
            if (isCoordinatorError(result.code)) {
                c.invalidateCoordinatorEndpoint(res.node, 0xFFFF_FFFD);
                request = request.forRetry();
                if (try retry.pauseOrAbort(c)) continue;
                _ = try c.groupRemainingMs();
            }
            return result;
        }
    }

    pub fn commitGroupOffsets(c: *Client, alloc: std.mem.Allocator, req: protocol.OffsetCommit) !void {
        var retry: Retry = .{};
        while (true) {
            const res = try c.groupRequest(alloc, req.group, protocol.api_key.offset_commit, protocol.version.offset_commit, "OffsetCommit", req, protocol.offsetCommitRequest);
            var d = Decoder.init(res.resp.body);
            const result = try protocol.offsetCommitResponse(&d, alloc);
            var code: protocol.ErrorCode = .none;
            for (result) |p| {
                if (p.code != .none) code = p.code;
            }
            if (isCoordinatorError(code)) {
                c.invalidateCoordinatorEndpoint(res.node, 0xFFFF_FFFE);
                if (try retry.pauseOrAbort(c)) continue;
                _ = try c.groupRemainingMs();
            }
            try c.groupResult(code);
            return;
        }
    }

    pub fn listGroups(c: *Client, alloc: std.mem.Allocator, all: bool) ![]protocol.ListedGroup {
        try c.requireVersion(protocol.api_key.list_groups, protocol.version.list_groups, "ListGroups");
        const topics = try c.listTopics();
        for (topics) |t| c.alloc.free(t.name);
        c.alloc.free(topics);
        var groups: std.ArrayList(protocol.ListedGroup) = .empty;
        var answered: usize = 0;
        var nodes = c.brokers.keyIterator();
        while (nodes.next()) |node| {
            const result = c.listGroupsOnNode(alloc, node.*, all) catch |err| {
                c.dropConn(connKey(node.*, 0xFFFF_FFFE));
                std.debug.print("kite: warning: group list broker {d} failed ({s}); results may be incomplete\n", .{ node.*, @errorName(err) });
                continue;
            };
            answered += 1;
            for (result) |g| {
                var duplicate = false;
                for (groups.items) |old| {
                    if (std.mem.eql(u8, old.group, g.group)) duplicate = true;
                }
                if (!duplicate) try groups.append(alloc, g);
            }
        }
        if (answered == 0) return error.GroupFailed;
        std.mem.sort(protocol.ListedGroup, groups.items, {}, struct {
            fn less(_: void, a: protocol.ListedGroup, b: protocol.ListedGroup) bool {
                return std.mem.lessThan(u8, a.group, b.group);
            }
        }.less);
        return groups.toOwnedSlice(alloc);
    }

    fn listGroupsOnNode(c: *Client, alloc: std.mem.Allocator, node: i32, all: bool) ![]protocol.ListedGroup {
        const conn = try c.connFor(node, connKey(node, 0xFFFF_FFFE));
        var retry: Retry = .{};
        while (true) {
            const resp = try c.sendRequestAlloc(alloc, conn, protocol.api_key.list_groups, protocol.version.list_groups, all, protocol.listGroupsRequest);
            var d = Decoder.init(resp.body);
            const result = try protocol.listGroupsResponse(&d, alloc);
            if (isCoordinatorError(result.code) and retry.pause(c)) continue;
            try c.groupResult(result.code);
            return result.groups;
        }
    }

    pub fn describeGroup(c: *Client, alloc: std.mem.Allocator, group: []const u8) !protocol.DescribedGroup {
        if (c.checkVersion(protocol.api_key.consumer_group_describe, protocol.version.consumer_group_describe)) {
            if (c.describeGroupOnCoordinator(alloc, group, false)) |result| {
                return result;
            } else |err| switch (err) {
                error.GroupNotFound, error.ConsumerGroupDescribeUnsupported => {},
                else => return err,
            }
        }
        return c.describeGroupOnCoordinator(alloc, group, true);
    }

    fn describeGroupOnCoordinator(c: *Client, alloc: std.mem.Allocator, group: []const u8, comptime classic: bool) !protocol.DescribedGroup {
        const key = if (classic) protocol.api_key.describe_groups else protocol.api_key.consumer_group_describe;
        const ver = if (classic) protocol.version.describe_groups else protocol.version.consumer_group_describe;
        const api = if (classic) "DescribeGroups" else "ConsumerGroupDescribe";
        const decode = if (classic) protocol.describeClassicGroupResponse else protocol.describeGroupResponse;
        var retry: Retry = .{};
        while (true) {
            const res = try c.groupRequest(alloc, group, key, ver, api, group, protocol.describeGroupRequest);
            var d = Decoder.init(res.resp.body);
            const result = try decode(&d, alloc);
            if (!classic and result.code == .unsupported_version) return error.ConsumerGroupDescribeUnsupported;
            if (isCoordinatorError(result.code)) {
                c.invalidateCoordinatorEndpoint(res.node, 0xFFFF_FFFE);
                if (retry.pause(c)) continue;
            }
            try c.groupResult(result.code);
            return result;
        }
    }

    pub fn fetchGroupOffsets(c: *Client, alloc: std.mem.Allocator, req: protocol.OffsetFetch) !protocol.FetchedOffsets {
        var retry: Retry = .{};
        while (true) {
            const res = try c.groupRequest(alloc, req.group, protocol.api_key.offset_fetch, protocol.version.offset_fetch, "OffsetFetch", req, protocol.offsetFetchRequest);
            var d = Decoder.init(res.resp.body);
            const result = try protocol.offsetFetchResponse(&d, alloc);
            var code = result.code;
            for (result.offsets) |p| {
                if (p.code != .none) code = p.code;
            }
            if (isCoordinatorError(code)) {
                c.invalidateCoordinatorEndpoint(res.node, 0xFFFF_FFFE);
                if (try retry.pauseOrAbort(c)) continue;
            }
            if (code == .unstable_offset_commit and try retry.pauseOrAbort(c)) continue;
            _ = try c.groupRemainingMs();
            try c.groupResult(code);
            return result;
        }
    }

    // -- Topic administration --------------------------------------------------

    /// Fail with AdminFailed when the broker's ApiVersions range does not
    /// cover `key` v`ver`.
    fn requireVersion(c: *Client, key: i16, ver: i16, api_name: []const u8) !void {
        if (c.checkVersion(key, ver)) return;
        const r = c.api_ranges.get(key);
        c.setErr(
            "broker lacks {s} v{d} (range {d}..{d})",
            .{ api_name, ver, if (r) |x| x.min else -1, if (r) |x| x.max else -1 },
        );
        return error.AdminFailed;
    }

    /// Admin requests go to the controller when its node id/address is known
    /// — that is the only broker that can change topics — else to the control
    /// connection. Produce conn keys are pidx*conns_per_partition+slot, so
    /// slot 0xFFFF_FFFF for the controller cannot collide with them.
    fn adminConn(c: *Client) !*Conn {
        const conn = c.control orelse {
            c.setErr("no control connection", .{});
            return error.AdminFailed;
        };
        if (c.controller_id >= 0 and c.brokers.contains(c.controller_id))
            return c.connFor(c.controller_id, connKey(c.controller_id, 0xFFFF_FFFF)) catch conn;
        return conn;
    }

    /// Map a per-topic admin response code: none = ok, the named cases are
    /// the typed errors callers switch on, everything else is AdminFailed
    /// with err_ctx "{api} '{topic}': {CODE} ({msg})".
    fn adminResult(c: *Client, api: []const u8, topic: []const u8, code: protocol.ErrorCode, msg: ?[]const u8) !void {
        switch (code) {
            .none => {},
            .topic_already_exists => return error.TopicAlreadyExists,
            .unknown_topic_or_partition, .unknown_topic_id => return error.TopicNotFound,
            .topic_authorization_failed, .cluster_authorization_failed => return error.TopicAuthorizationFailed,
            else => {
                if (msg) |m| {
                    if (m.len > 0) {
                        c.setErr("{s} '{s}': {s} ({s})", .{ api, topic, code.name(), m });
                        return error.AdminFailed;
                    }
                }
                c.setErr("{s} '{s}': {s}", .{ api, topic, code.name() });
                return error.AdminFailed;
            },
        }
    }

    pub const TopicSpec = struct {
        /// -1 = broker default.
        partitions: i32 = -1,
        /// -1 = broker default.
        replication_factor: i16 = -1,
        configs: []const protocol.ConfigEntry = &.{},
    };

    pub const CreateResult = struct { partitions: i32, replication_factor: i16 };

    /// Create `topic` (CreateTopics v7) and report the partitions/replication
    /// factor the broker picked. A stale controller id gets one retry on the
    /// control connection (NOT_CONTROLLER).
    pub fn createTopic(c: *Client, topic: []const u8, spec: TopicSpec) !CreateResult {
        try c.requireVersion(protocol.api_key.create_topics, protocol.version.create_topics, "CreateTopics");
        const Ctx = struct { t: []const u8, spec: TopicSpec };
        const body = struct {
            fn f(e: *Encoder, ctx: Ctx) protocol.ProtoError!void {
                try e.compactArrayLen(1);
                try e.compactString(ctx.t);
                try e.i32v(ctx.spec.partitions); // -1 = broker default
                try e.i16v(ctx.spec.replication_factor);
                try e.compactArrayLen(0); // assignments
                try e.compactArrayLen(ctx.spec.configs.len);
                for (ctx.spec.configs) |cfg| {
                    try e.compactString(cfg.name);
                    try e.compactString(cfg.value);
                    try e.tagBuffer();
                }
                try e.tagBuffer();
                try e.i32v(30000); // timeout_ms
                try e.boolean(false); // validate_only
                try e.tagBuffer();
            }
        }.f;

        var conn = try c.adminConn();
        var attempt: usize = 0;
        while (attempt < 2) : (attempt += 1) {
            const resp = c.sendRequest(conn, protocol.api_key.create_topics, protocol.version.create_topics, Ctx{ .t = topic, .spec = spec }, body) catch {
                if (attempt == 0 and c.control != null and conn != c.control.?) {
                    c.dropNode(c.controller_id);
                    conn = c.control.?;
                    continue;
                }
                c.setErr("CreateTopics request failed", .{});
                return error.AdminFailed;
            };
            var d = Decoder.init(resp.body);
            _ = try d.i32v(); // throttle
            const n = try d.compactArrayLen();
            if (n < 1) {
                c.alloc.free(resp.frame);
                c.setErr("CreateTopics: empty topics array", .{});
                return error.AdminFailed;
            }
            _ = try d.compactString(); // name (we sent exactly one topic)
            try d.skip(16); // topic_id uuid
            const code: protocol.ErrorCode = @enumFromInt(try d.i16v());
            const msg = try d.compactString();
            const nparts = try d.i32v();
            const rf = try d.i16v();
            c.alloc.free(resp.frame);
            switch (code) {
                .not_controller => {
                    // Our controller id was stale or we never learned it;
                    // one retry through the control connection.
                    if (attempt == 0 and c.control != null and conn != c.control.?) {
                        conn = c.control.?;
                        continue;
                    }
                    c.setErr("CreateTopics '{s}': {s}", .{ topic, code.name() });
                    return error.AdminFailed;
                },
                else => {
                    try c.adminResult("CreateTopics", topic, code, msg);
                    c.vlog("CreateTopics '{s}': {s}", .{ topic, code.name() });
                    return .{ .partitions = nparts, .replication_factor = rf };
                },
            }
        }
        unreachable;
    }

    /// One-shot admin request shared by deleteTopic, createPartitions, and
    /// alterTopicConfigs: `key` selects the request layout at runtime so the
    /// encoder and sendRequest instantiate once.
    const AdminReq = struct {
        key: i16,
        ver: i16,
        api: []const u8,
        t: []const u8,
        count: i32 = 0,
        set: []const protocol.ConfigEntry = &.{},
        unset: []const []const u8 = &.{},
    };

    fn adminReqBody(e: *Encoder, r: AdminReq) protocol.ProtoError!void {
        if (r.key == protocol.api_key.delete_topics) {
            try e.compactArrayLen(1);
            try e.compactString(r.t);
            try e.raw(&[_]u8{0} ** 16); // topic_id: zero uuid
            try e.tagBuffer();
            try e.i32v(30000); // timeout_ms
            try e.tagBuffer();
        } else if (r.key == protocol.api_key.create_partitions) {
            try e.compactArrayLen(1);
            try e.compactString(r.t);
            try e.i32v(r.count);
            try e.uvarint(0); // assignments: null (broker assigns)
            try e.tagBuffer();
            try e.i32v(30000); // timeout_ms
            try e.boolean(false); // validate_only
            try e.tagBuffer();
        } else { // incremental_alter_configs
            try e.compactArrayLen(1);
            try e.i8v(2); // resource_type: TOPIC
            try e.compactString(r.t);
            try e.compactArrayLen(r.set.len + r.unset.len);
            for (r.set) |cfg| {
                try e.compactString(cfg.name);
                try e.i8v(0); // SET
                try e.compactString(cfg.value);
                try e.tagBuffer();
            }
            for (r.unset) |name| {
                try e.compactString(name);
                try e.i8v(1); // DELETE
                try e.compactString(null);
                try e.tagBuffer();
            }
            try e.tagBuffer();
            try e.boolean(false); // validate_only
            try e.tagBuffer();
        }
    }

    fn adminRequest(c: *Client, r: AdminReq) !void {
        try c.requireVersion(r.key, r.ver, r.api);
        var conn = try c.adminConn();
        var attempt: usize = 0;
        while (attempt < 2) : (attempt += 1) {
            const resp = c.sendRequest(conn, r.key, r.ver, r, adminReqBody) catch {
                if (attempt == 0 and c.control != null and conn != c.control.?) {
                    c.dropNode(c.controller_id);
                    conn = c.control.?;
                    continue;
                }
                c.setErr("{s} request failed", .{r.api});
                return error.AdminFailed;
            };
            defer c.alloc.free(resp.frame);
            var d = Decoder.init(resp.body);
            _ = try d.i32v(); // throttle
            const n = try d.compactArrayLen();
            if (n < 1) {
                c.setErr("{s}: empty results array", .{r.api});
                return error.AdminFailed;
            }
            var code: protocol.ErrorCode = undefined;
            var msg: ?[]const u8 = null;
            if (r.key == protocol.api_key.incremental_alter_configs) {
                code = @enumFromInt(try d.i16v());
                msg = try d.compactString();
                _ = try d.i8v(); // resource_type
                _ = try d.compactString(); // resource_name
            } else {
                _ = try d.compactString(); // name
                if (r.key == protocol.api_key.delete_topics)
                    try d.skip(16); // topic_id uuid
                code = @enumFromInt(try d.i16v());
                msg = try d.compactString();
            }
            try d.tagBuffer();
            if (code == .not_controller and attempt == 0 and c.control != null and conn != c.control.?) {
                conn = c.control.?;
                continue;
            }
            try c.adminResult(r.api, r.t, code, msg);
            return;
        }
    }

    /// Delete one topic (DeleteTopics v6).
    pub fn deleteTopic(c: *Client, topic: []const u8) !void {
        try c.adminRequest(.{
            .key = protocol.api_key.delete_topics,
            .ver = protocol.version.delete_topics,
            .api = "DeleteTopics",
            .t = topic,
        });
    }

    /// Grow `topic` to `count` partitions (CreatePartitions v3). Shrinking is
    /// INVALID_PARTITIONS; the broker's message is passed through in err_ctx.
    pub fn createPartitions(c: *Client, topic: []const u8, count: i32) !void {
        try c.adminRequest(.{
            .key = protocol.api_key.create_partitions,
            .ver = protocol.version.create_partitions,
            .api = "CreatePartitions",
            .t = topic,
            .count = count,
        });
    }

    /// Apply --set/--unset topic configs (IncrementalAlterConfigs v1): one
    /// resource (type TOPIC=2) with one op per entry, SET=0 / DELETE=1.
    pub fn alterTopicConfigs(c: *Client, topic: []const u8, set: []const protocol.ConfigEntry, unset: []const []const u8) !void {
        try c.adminRequest(.{
            .key = protocol.api_key.incremental_alter_configs,
            .ver = protocol.version.incremental_alter_configs,
            .api = "IncrementalAlterConfigs",
            .t = topic,
            .set = set,
            .unset = unset,
        });
    }

    pub const TopicInfo = struct {
        name: []const u8,
        partitions: u32,
        replication_factor: u32,
        internal: bool,
    };

    /// Metadata request body: `t` names one topic, null asks for all.
    fn metadataBody(e: *Encoder, t: ?[]const u8) protocol.ProtoError!void {
        if (t) |name| {
            try e.compactArrayLen(1);
            try e.raw(&[_]u8{0} ** 16); // topic_id: zero uuid
            try e.compactString(name);
            try e.tagBuffer();
        } else {
            try e.uvarint(0); // topics: null = all topics
        }
        try e.boolean(false); // allow_auto_topic_creation
        try e.boolean(false); // include_cluster_authorized_operations
        try e.boolean(false); // include_topic_authorized_operations
        try e.tagBuffer();
    }

    /// Metadata on the control connection, falling back to discovered and
    /// configured addresses until one answers.
    fn metadataRequest(c: *Client, t: ?[]const u8) !Resp {
        if (c.control) |conn| {
            if (c.sendRequest(conn, protocol.api_key.metadata, protocol.version.metadata, t, metadataBody)) |resp| {
                return resp;
            } else |err| {
                c.vlog("metadata request failed — reconnecting control connection", .{});
                c.dropControl();
                if (err == error.WaitHookAborted) return err;
            }
        }
        var brokers = c.brokers.valueIterator();
        while (brokers.next()) |addr| {
            return c.connectMetadata(t, addr.*) catch |err| {
                if (err == error.WaitHookAborted) return err;
                continue;
            };
        }
        for (c.cfg.bootstrap_servers) |server| {
            const addr = parseBootstrapServer(server) catch continue;
            return c.connectMetadata(t, addr) catch |err| {
                if (err == error.WaitHookAborted) return err;
                continue;
            };
        }
        c.setErr("metadata request failed", .{});
        return error.MetadataFailed;
    }

    fn connectMetadata(c: *Client, t: ?[]const u8, addr: BrokerAddr) !Resp {
        const conn = try c.connectOne(addr.host, addr.port);
        errdefer {
            conn.close();
            c.alloc.destroy(conn);
        }
        const prev_control_busy = c.control_busy;
        c.control_busy = true;
        defer c.control_busy = prev_control_busy;
        const resp = try c.sendRequest(conn, protocol.api_key.metadata, protocol.version.metadata, t, metadataBody);
        c.dropControl();
        c.control = conn;
        c.vlog("control connected {s}:{d}", .{ addr.host, addr.port });
        return resp;
    }

    /// Every topic the principal can see (Metadata v12, topics = null),
    /// sorted by name; per-topic errors are skipped. Refreshes the broker
    /// map/controller id like refreshMetadata but leaves `partitions` alone.
    /// Names are duped into c.alloc — the response frame is freed.
    pub fn listTopics(c: *Client) ![]TopicInfo {
        const resp = try c.metadataRequest(null);
        defer c.alloc.free(resp.frame);

        var d = Decoder.init(resp.body);
        _ = try d.i32v(); // throttle
        try c.decodeBrokers(&d);
        _ = try d.compactString(); // cluster id
        c.controller_id = try d.i32v();

        var topics: std.ArrayListUnmanaged(TopicInfo) = .empty;
        const ntopics = try d.compactArrayLen();
        var t: i64 = 0;
        while (t < ntopics) : (t += 1) {
            const terr: protocol.ErrorCode = @enumFromInt(try d.i16v());
            const name = (try d.compactString()) orelse "";
            try d.skip(16); // topic_id uuid
            const internal = try d.boolean();
            const nparts = try d.compactArrayLen();
            var rf: u32 = 0;
            var p: i64 = 0;
            while (p < nparts) : (p += 1) {
                _ = try d.i16v(); // partition error
                _ = try d.i32v(); // partition index
                _ = try d.i32v(); // leader
                _ = try d.i32v(); // leader epoch
                const nreplicas = try d.compactArrayLen();
                if (nreplicas > 0) try d.skip(@as(usize, @intCast(nreplicas)) * 4);
                if (p == 0) rf = @intCast(@max(0, nreplicas));
                try d.skipCompactI32Array(); // isr
                try d.skipCompactI32Array(); // offline replicas
                try d.tagBuffer();
            }
            _ = try d.i32v(); // topic_authorized_operations
            try d.tagBuffer();
            if (terr != .none) continue;
            try topics.append(c.alloc, .{
                .name = try c.alloc.dupe(u8, name),
                .partitions = @intCast(@max(0, nparts)),
                .replication_factor = rf,
                .internal = internal,
            });
        }
        // Insertion sort by name; topic counts are small.
        if (topics.items.len > 1) {
            for (topics.items[1..], 1..) |item, i| {
                var j = i;
                while (j > 0 and std.mem.order(u8, topics.items[j - 1].name, item.name) == .gt) : (j -= 1)
                    topics.items[j] = topics.items[j - 1];
                topics.items[j] = item;
            }
        }
        return topics.items;
    }

    /// Decode the Metadata broker array into c.brokers.
    const BrokerMap = std.AutoHashMapUnmanaged(i32, BrokerAddr);

    fn freeBrokers(c: *Client, brokers: *BrokerMap) void {
        var it = brokers.valueIterator();
        while (it.next()) |addr| c.alloc.free(addr.host);
        brokers.deinit(c.alloc);
    }

    fn decodeBrokers(c: *Client, d: *Decoder) !void {
        const nbrokers = try d.compactArrayLen();
        if (nbrokers < 0) return error.MalformedResponse;
        var brokers: BrokerMap = .empty;
        defer c.freeBrokers(&brokers);
        var i: i64 = 0;
        while (i < nbrokers) : (i += 1) {
            const node = try d.i32v();
            const host = (try d.compactString()) orelse "";
            const port = try d.i32v();
            _ = try d.compactString(); // rack
            try d.tagBuffer();
            if (node < 0 or host.len == 0 or port < 1 or port > 65535) continue;
            if (brokers.contains(node)) return error.MalformedResponse;
            const owned_host = try c.alloc.dupe(u8, host);
            errdefer c.alloc.free(owned_host);
            try brokers.put(c.alloc, node, .{ .host = owned_host, .port = @intCast(port) });
        }
        var old = c.brokers.iterator();
        while (old.next()) |entry| {
            if (brokers.get(entry.key_ptr.*)) |next| {
                if (next.port == entry.value_ptr.port and std.mem.eql(u8, next.host, entry.value_ptr.host)) continue;
            }
            c.dropNode(entry.key_ptr.*);
        }
        std.mem.swap(BrokerMap, &c.brokers, &brokers);
    }

    // -- Idempotent produce: InitProducerId ----------------------------------

    /// Ask the broker for a producer id + epoch (idempotent produce). Sends
    /// InitProducerId v4 on the control connection; retries transient errors.
    fn initProducerId(c: *Client) !void {
        if (!c.checkVersion(protocol.api_key.init_producer_id, protocol.version.init_producer_id)) {
            const r = c.api_ranges.get(protocol.api_key.init_producer_id);
            c.setErr(
                "broker lacks InitProducerId v{d} (range {d}..{d}); set enable.idempotence=false",
                .{ protocol.version.init_producer_id, if (r) |x| x.min else -1, if (r) |x| x.max else -1 },
            );
            return error.MalformedResponse;
        }
        const Ctx = struct {};
        const body = struct {
            fn f(e: *Encoder, _: Ctx) protocol.ProtoError!void {
                try e.compactString(null); // transactional_id: null = idempotent-only
                try e.i32v(60000); // transaction_timeout_ms (unused without txn id)
                try e.i64v(-1); // producer_id: -1 = broker assigns
                try e.i16v(-1); // producer_epoch
                try e.tagBuffer();
            }
        }.f;
        var retry: Retry = .{};
        while (true) {
            if (c.control == null) c.reconnectControl() catch {
                if (retry.pause(c)) continue;
                return error.ProduceFailed;
            };
            const conn = c.control.?;
            const resp = c.sendRequest(conn, protocol.api_key.init_producer_id, protocol.version.init_producer_id, Ctx{}, body) catch {
                c.setErr("InitProducerId request failed", .{});
                c.reconnectControl() catch {};
                if (retry.pause(c)) continue;
                return error.ProduceFailed;
            };
            defer c.alloc.free(resp.frame);
            var d = Decoder.init(resp.body);
            _ = try d.i32v(); // throttle
            const code: protocol.ErrorCode = @enumFromInt(try d.i16v());
            const pid = try d.i64v();
            const epoch = try d.i16v();
            if (code == .none) {
                c.producer_id = pid;
                c.producer_epoch = epoch;
                c.producer_inited = true;
                c.vlog("idempotent producer: id={d} epoch={d}", .{ pid, epoch });
                return;
            }
            if (!code.retriable() or !retry.pause(c)) {
                c.setErr("InitProducerId: {s}", .{code.name()});
                return error.ProduceFailed;
            }
        }
    }

    /// Ensure a producer id exists when idempotence is enabled.
    fn ensureProducerId(c: *Client) !void {
        if (c.cfg.enable_idempotence and !c.producer_inited)
            try c.initProducerId();
    }

    // -- Transactional produce -----------------------------------------------

    const txn_coord_slot: u32 = 0xFFFF_FFFD;

    fn invalidateTxnCoordinator(c: *Client) void {
        if (c.txn_coordinator_node >= 0) c.dropConn(connKey(c.txn_coordinator_node, txn_coord_slot));
        c.txn_coordinator_node = -1;
    }

    fn findTxnCoordinator(c: *Client, stop: ?*const std.atomic.Value(bool)) !i32 {
        if (c.txn_coordinator_node >= 0) return c.txn_coordinator_node;
        const txn_id = c.txn_id orelse return error.ProduceFailed;
        var retry: Retry = .{};
        while (true) {
            if (stop) |flag| if (flag.load(.acquire)) return error.Interrupted;
            _ = try c.groupRemainingMs();
            if (c.control == null) c.reconnectControl() catch {
                if (retry.pauseUnlessStopped(c, stop)) continue;
                c.setErr("FindCoordinator (txn '{s}'): control broker unreachable", .{txn_id});
                return error.ProduceFailed;
            };
            const conn = c.control.?;
            if (stop != null) {
                const wait_ms = try c.requestTimeoutMs(0);
                const until = std.Io.Timestamp.now(c.io, .awake).addDuration(.fromMilliseconds(@intCast(wait_ms)));
                conn.setReadCancellation(stop, until);
            }
            const resp = c.sendRequest(
                conn,
                protocol.api_key.find_coordinator,
                protocol.version.find_coordinator,
                txn_id,
                protocol.findTxnCoordinatorRequest,
            ) catch {
                c.dropControl();
                if (retry.pauseUnlessStopped(c, stop)) continue;
                c.setErr("FindCoordinator (txn '{s}'): request failed", .{txn_id});
                return error.ProduceFailed;
            };
            conn.setReadCancellation(null, null);
            defer c.alloc.free(resp.frame);
            var d = Decoder.init(resp.body);
            const coord = try protocol.coordinatorResponse(&d);
            if ((isCoordinatorError(coord.code) or coord.code.retriable()) and retry.pauseUnlessStopped(c, stop)) continue;
            if (coord.code != .none) {
                c.setErr("FindCoordinator (txn '{s}'): {s}", .{ txn_id, coord.code.name() });
                return error.ProduceFailed;
            }
            if (coord.node < 0 or coord.host.len == 0 or coord.port < 1 or coord.port > 65535)
                return error.MalformedResponse;
            const host = try c.alloc.dupe(u8, coord.host);
            errdefer c.alloc.free(host);
            const entry = try c.brokers.getOrPut(c.alloc, coord.node);
            if (entry.found_existing) c.alloc.free(entry.value_ptr.host);
            entry.value_ptr.* = .{ .host = host, .port = @intCast(coord.port) };
            c.txn_coordinator_node = coord.node;
            c.vlog("txn coordinator for '{s}': broker {d} ({s}:{d})", .{ txn_id, coord.node, coord.host, coord.port });
            return coord.node;
        }
    }

    /// Initialize a transactional producer: discover the transaction
    /// coordinator and obtain a producer id + epoch via InitProducerId v4.
    pub fn initTransaction(c: *Client, txn_id: []const u8) !void {
        try c.requireVersion(protocol.api_key.find_coordinator, protocol.version.find_coordinator, "FindCoordinator");
        try c.requireVersion(protocol.api_key.init_producer_id, protocol.version.init_producer_id, "InitProducerId");
        try c.requireVersion(protocol.api_key.add_partitions_to_txn, protocol.version.add_partitions_to_txn, "AddPartitionsToTxn");
        try c.requireVersion(protocol.api_key.end_txn, protocol.version.end_txn, "EndTxn");
        c.txn_id = txn_id;
        try c.initProducerIdTxn(txn_id, -1, -1);
    }

    fn isFencedError(code: protocol.ErrorCode) bool {
        return code == .producer_fenced or code == .invalid_producer_epoch;
    }

    fn recordFenced(c: *Client, code: protocol.ErrorCode) void {
        if (!isFencedError(code)) return;
        c.txn_fenced = true;
        if (c.txn_fence_code == null) c.txn_fence_code = code;
    }

    fn initProducerIdTxn(c: *Client, txn_id: []const u8, req_pid: i64, req_epoch: i16) !void {
        const InitArgs = struct { id: []const u8, timeout_ms: i32, pid: i64, epoch: i16 };
        const body = struct {
            fn f(e: *Encoder, args: InitArgs) protocol.ProtoError!void {
                try e.compactString(args.id);
                try e.i32v(args.timeout_ms); // transaction_timeout_ms
                try e.i64v(args.pid); // producer_id (-1 for fresh, current for KIP-360 epoch bump)
                try e.i16v(args.epoch); // producer_epoch
                try e.tagBuffer();
            }
        }.f;
        const init_args: InitArgs = .{
            .id = txn_id,
            .timeout_ms = c.cfg.transaction_timeout_ms,
            .pid = req_pid,
            .epoch = req_epoch,
        };
        var retry: Retry = .{};
        while (true) {
            const node = try c.findTxnCoordinator(null);
            const conn = c.connFor(node, connKey(node, txn_coord_slot)) catch {
                c.invalidateTxnCoordinator();
                if (retry.pause(c)) continue;
                c.setErr("InitProducerId (txn '{s}'): coordinator unreachable", .{txn_id});
                return error.ProduceFailed;
            };
            const resp = c.sendRequest(conn, protocol.api_key.init_producer_id, protocol.version.init_producer_id, init_args, body) catch {
                c.invalidateTxnCoordinator();
                if (retry.pause(c)) continue;
                c.setErr("InitProducerId (txn '{s}'): request failed", .{txn_id});
                return error.ProduceFailed;
            };
            defer c.alloc.free(resp.frame);
            var d = Decoder.init(resp.body);
            _ = try d.i32v(); // throttle_time_ms
            const code: protocol.ErrorCode = @enumFromInt(try d.i16v());
            const pid = try d.i64v();
            const epoch = try d.i16v();
            try d.tagBuffer();
            if (code == .none) {
                c.producer_id = pid;
                c.producer_epoch = epoch;
                c.producer_inited = true;
                c.vlog("transactional producer '{s}': id={d} epoch={d}", .{ txn_id, pid, epoch });
                return;
            }
            c.recordFenced(code);
            if (isCoordinatorError(code)) c.invalidateTxnCoordinator();
            if ((isCoordinatorError(code) or code.retriable()) and retry.pause(c)) continue;
            c.setErr("InitProducerId (txn '{s}'): {s}", .{ txn_id, code.name() });
            return error.ProduceFailed;
        }
    }

    fn addPartitionsToTxn(c: *Client, topic: []const u8, parts: []const i32) !void {
        const txn_id = c.txn_id orelse return;
        var remaining: std.ArrayListUnmanaged(i32) = .empty;
        defer remaining.deinit(c.alloc);
        try remaining.appendSlice(c.alloc, parts);

        var retry: Retry = .{};
        while (remaining.items.len > 0) {
            const node = try c.findTxnCoordinator(null);
            const conn = c.connFor(node, connKey(node, txn_coord_slot)) catch {
                c.invalidateTxnCoordinator();
                if (retry.pause(c)) continue;
                c.setErr("AddPartitionsToTxn ({s}): coordinator unreachable", .{topic});
                return error.ProduceFailed;
            };
            c.txn_add_attempted = true;
            const resp = c.sendRequest(
                conn,
                protocol.api_key.add_partitions_to_txn,
                protocol.version.add_partitions_to_txn,
                protocol.AddPartitionsToTxn{
                    .txn_id = txn_id,
                    .producer_id = c.producer_id,
                    .producer_epoch = c.producer_epoch,
                    .topic = topic,
                    .partitions = remaining.items,
                },
                protocol.addPartitionsToTxnRequest,
            ) catch {
                c.invalidateTxnCoordinator();
                if (retry.pause(c)) continue;
                c.setErr("AddPartitionsToTxn ({s}): request failed", .{topic});
                return error.ProduceFailed;
            };
            defer c.alloc.free(resp.frame);
            var d = Decoder.init(resp.body);
            const results = try protocol.addPartitionsToTxnResponse(&d, c.alloc);
            defer c.alloc.free(results);

            var coord_err = false;
            var next: std.ArrayListUnmanaged(i32) = .empty;
            errdefer next.deinit(c.alloc);
            for (results) |res| {
                if (res.code == .none) {
                    try c.txn_added_parts.put(c.alloc, res.partition, {});
                } else if (isCoordinatorError(res.code)) {
                    coord_err = true;
                    try next.append(c.alloc, res.partition);
                } else if (res.code == .operation_not_attempted or res.code.retriable()) {
                    try next.append(c.alloc, res.partition);
                } else {
                    c.recordFenced(res.code);
                    c.setErr("AddPartitionsToTxn ({s}[{d}]): {s}", .{ topic, res.partition, res.code.name() });
                    return error.ProduceFailed;
                }
            }
            for (remaining.items) |p| {
                if (!c.txn_added_parts.contains(p)) {
                    var listed = false;
                    for (next.items) |np| if (np == p) {
                        listed = true;
                        break;
                    };
                    if (!listed) return error.MalformedResponse;
                }
            }
            remaining.deinit(c.alloc);
            remaining = next;
            if (remaining.items.len == 0) return;
            if (coord_err) c.invalidateTxnCoordinator();
            if (!retry.pause(c)) {
                c.setErr("AddPartitionsToTxn ({s}): timed out after 30s", .{topic});
                return error.ProduceFailed;
            }
        }
    }

    pub fn hasActiveTxn(c: *const Client) bool {
        return c.txn_id != null and c.producer_inited and (c.txn_added_parts.count() > 0 or c.txn_add_attempted);
    }

    fn clearTxnState(c: *Client) void {
        c.txn_added_parts.clearRetainingCapacity();
        c.txn_add_attempted = false;
        c.txn_produce_unsettled_until = null;
        c.txn_commit_dispatched = false;
    }

    fn noteFencedAbort(c: *const Client, txn_id: []const u8, code: ?protocol.ErrorCode) void {
        const reason = if (code) |ec| ec.name() else "producer fenced";
        std.debug.print(
            "kite: transaction '{s}' aborted by Kafka: {s} (transaction.timeout.ms={d} exceeded, or another producer used this transactional ID)\n",
            .{ txn_id, reason, c.cfg.transaction_timeout_ms },
        );
    }

    /// Commit (`commit=true`) or abort (`commit=false`) the active transaction.
    /// No-op when no partitions were registered (or attempted) in the transaction.
    /// When `commit` is true and `stop` becomes set before the first commit
    /// request is dispatched, returns `error.Interrupted` so the caller can
    /// safely abort. Once a commit request has been dispatched, any lost or
    /// malformed reply or coordinator-movement error means the broker may have
    /// appended `PrepareCommit`: retries send only commit, and if `stop` is set
    /// it returns `error.CommitOutcomeUnknown` instead of switching to abort.
    /// When `commit` is false and the producer has been fenced (broker timeout
    /// or replacement producer) without a dispatched commit, Kafka is aborting
    /// the transaction, so local state is cleared and the abort succeeds.
    pub fn endTransaction(c: *Client, commit: bool, stop: ?*const std.atomic.Value(bool)) !void {
        if (!c.hasActiveTxn()) return;
        const txn_id = c.txn_id.?;
        const verb = if (commit) "commit" else "abort";
        const previous_deadline = c.group_deadline;
        defer c.group_deadline = previous_deadline;
        const deadline = std.Io.Timestamp.now(c.io, .awake).addDuration(.fromMilliseconds(30000));
        c.group_deadline = if (previous_deadline) |prev| .{ .nanoseconds = @min(prev.nanoseconds, deadline.nanoseconds) } else deadline;
        const commit_stop = if (commit) stop else null;
        const previous_stop = c.txn_stop;
        c.txn_stop = commit_stop;
        defer c.txn_stop = previous_stop;
        if (!commit and c.txn_commit_dispatched) {
            c.setErr("EndTxn (abort '{s}'): commit already dispatched (outcome unknown)", .{txn_id});
            return error.ProduceFailed;
        }
        // Skip settling in-flight produces when already fenced: the coordinator's
        // epoch bump and abort markers fence any stale requests on added partitions.
        if (!commit and c.txn_fenced) {
            c.clearTxnState();
            c.noteFencedAbort(txn_id, c.txn_fence_code);
            return;
        }
        const unsettled = if (!commit) c.settleProduceBeforeAbort() else false;
        // Fires when settling in-flight Produce responses itself discovers the fence.
        if (!commit and c.txn_fenced) {
            c.clearTxnState();
            c.noteFencedAbort(txn_id, c.txn_fence_code);
            return;
        }
        var retry: Retry = .{};
        var commit_ambiguous = commit and c.txn_commit_dispatched;
        while (true) {
            if (commit) if (stop) |s| if (s.load(.acquire)) {
                return if (commit_ambiguous) error.CommitOutcomeUnknown else error.Interrupted;
            };
            const node = c.findTxnCoordinator(commit_stop) catch |err| {
                if (commit) if (stop) |s| if (s.load(.acquire)) {
                    return if (commit_ambiguous) error.CommitOutcomeUnknown else error.Interrupted;
                };
                if (commit_ambiguous) {
                    c.setErr("EndTxn (commit '{s}'): coordinator lookup failed (outcome unknown; broker may have committed)", .{txn_id});
                    return error.ProduceFailed;
                }
                return err;
            };
            if (commit) if (stop) |s| if (s.load(.acquire)) {
                return if (commit_ambiguous) error.CommitOutcomeUnknown else error.Interrupted;
            };
            const conn = c.connFor(node, connKey(node, txn_coord_slot)) catch {
                c.invalidateTxnCoordinator();
                if (commit) if (stop) |s| if (s.load(.acquire)) {
                    return if (commit_ambiguous) error.CommitOutcomeUnknown else error.Interrupted;
                };
                if (retry.pauseUnlessStopped(c, commit_stop)) continue;
                if (commit_stop) |s| if (s.load(.acquire)) continue;
                if (commit_ambiguous)
                    c.setErr("EndTxn (commit '{s}'): coordinator unreachable (outcome unknown; broker may have committed)", .{txn_id})
                else
                    c.setErr("EndTxn ({s} '{s}'): coordinator unreachable", .{ verb, txn_id });
                return error.ProduceFailed;
            };
            // Recheck stop after a fresh connection / ApiVersions handshake,
            // right before dispatching EndTxn on the wire.
            if (commit) if (stop) |s| if (s.load(.acquire)) {
                return if (commit_ambiguous) error.CommitOutcomeUnknown else error.Interrupted;
            };
            if (commit) c.txn_commit_dispatched = true;
            const resp = c.sendRequest(
                conn,
                protocol.api_key.end_txn,
                protocol.version.end_txn,
                protocol.EndTxn{
                    .txn_id = txn_id,
                    .producer_id = c.producer_id,
                    .producer_epoch = c.producer_epoch,
                    .committed = commit,
                },
                protocol.endTxnRequest,
            ) catch {
                c.invalidateTxnCoordinator();
                if (commit) commit_ambiguous = true;
                if (commit) if (stop) |s| if (s.load(.acquire)) return error.CommitOutcomeUnknown;
                if (retry.pauseUnlessStopped(c, commit_stop)) continue;
                if (commit_stop) |s| if (s.load(.acquire)) return error.CommitOutcomeUnknown;
                if (commit_ambiguous)
                    c.setErr("EndTxn (commit '{s}'): request failed (outcome unknown; broker may have committed)", .{txn_id})
                else
                    c.setErr("EndTxn ({s} '{s}'): request failed", .{ verb, txn_id });
                return error.ProduceFailed;
            };
            defer c.alloc.free(resp.frame);
            var d = Decoder.init(resp.body);
            const code = protocol.endTxnResponse(&d) catch {
                c.invalidateTxnCoordinator();
                if (commit) commit_ambiguous = true;
                if (commit) if (stop) |s| if (s.load(.acquire)) return error.CommitOutcomeUnknown;
                if (retry.pauseUnlessStopped(c, commit_stop)) continue;
                if (commit_stop) |s| if (s.load(.acquire)) return error.CommitOutcomeUnknown;
                if (commit_ambiguous)
                    c.setErr("EndTxn (commit '{s}'): malformed response (outcome unknown; broker may have committed)", .{txn_id})
                else
                    c.setErr("EndTxn ({s} '{s}'): malformed response", .{ verb, txn_id });
                return error.ProduceFailed;
            };
            if (code == .none) {
                c.clearTxnState();
                if (!commit and unsettled) {
                    std.debug.print("kite: warning: in-flight produce was unconfirmed before abort; if broker partition verification is disabled, check for hanging transactions with: kafka-transactions.sh --bootstrap-server <host:port> find-hanging --topic <topic> --partition <p> --max-transaction-timeout 0 (then abort --topic <topic> --partition <p> --start-offset <n>)\n", .{});
                }
                c.vlog("transaction '{s}' {s}", .{ txn_id, if (commit) "committed" else "aborted" });
                return;
            }
            if (isFencedError(code)) {
                c.recordFenced(code);
                if (!commit) {
                    c.clearTxnState();
                    c.noteFencedAbort(txn_id, code);
                    return;
                }
                if (!commit_ambiguous) {
                    c.clearTxnState();
                    c.setErr(
                        "EndTxn (commit '{s}'): {s}; transaction aborted by Kafka (transaction.timeout.ms={d} exceeded, or another producer used this transactional ID)",
                        .{ txn_id, code.name(), c.cfg.transaction_timeout_ms },
                    );
                    return error.ProduceFailed;
                }
            }
            if (!commit and c.txn_added_parts.count() == 0 and code == .invalid_txn_state) {
                // AddPartitionsToTxn was attempted but never opened a transaction on the broker.
                c.txn_add_attempted = false;
                return;
            }
            if (isCoordinatorError(code)) c.invalidateTxnCoordinator();
            if (isCoordinatorError(code) or code.retriable()) {
                if (commit) commit_ambiguous = true;
                if (commit) if (stop) |s| if (s.load(.acquire)) return error.CommitOutcomeUnknown;
                if (retry.pauseUnlessStopped(c, commit_stop)) continue;
                if (commit_stop) |s| if (s.load(.acquire)) return error.CommitOutcomeUnknown;
            } else if (commit and code == .transaction_abortable) {
                if (!commit_ambiguous) c.txn_commit_dispatched = false;
                _ = c.abortTransactionBestEffort();
            }
            if (commit_ambiguous)
                c.setErr("EndTxn (commit '{s}'): {s} (outcome unknown; broker may have committed)", .{ txn_id, code.name() })
            else
                c.setErr("EndTxn ({s} '{s}'): {s}", .{ verb, txn_id, code.name() });
            return error.ProduceFailed;
        }
    }

    /// Best-effort abort for fatal/interrupt paths; preserves any prior err_ctx.
    /// Returns true if no transaction was active or the abort is confirmed
    /// (including when the broker already fenced the producer and aborted it).
    pub fn abortTransactionBestEffort(c: *Client) bool {
        if (!c.hasActiveTxn()) return true;
        const saved_err = c.err_ctx;
        defer c.err_ctx = saved_err;
        const txn_id = c.txn_id.?;
        c.endTransaction(false, null) catch {
            const detail = c.errDetail();
            if (detail.len > 0)
                std.debug.print("kite: warning: transaction abort outcome unknown ({s}); verify with: kafka-transactions.sh --bootstrap-server <host:port> describe --transactional-id '{s}'\n", .{ detail, txn_id })
            else
                std.debug.print("kite: warning: transaction abort outcome unknown; verify with: kafka-transactions.sh --bootstrap-server <host:port> describe --transactional-id '{s}'\n", .{txn_id});
            c.clearTxnState();
            return false;
        };
        return true;
    }

    // -- Produce -------------------------------------------------------------

    const produce_timeout_ms = 15000;

    fn markProduceUnsettled(c: *Client) void {
        c.markProduceUnsettledFrom(std.Io.Timestamp.now(c.io, .awake));
    }

    fn markProduceUnsettledFrom(c: *Client, sent: std.Io.Timestamp) void {
        if (c.txn_id == null) return;
        const until = sent.addDuration(.fromMilliseconds(produce_timeout_ms));
        if (c.txn_produce_unsettled_until) |prev| {
            if (prev.nanoseconds >= until.nanoseconds) return;
        }
        c.txn_produce_unsettled_until = until;
    }

    /// Await responses for every already-dispatched Produce request using one
    /// absolute deadline per response. Returns true if any Produce outcome
    /// could not be confirmed (caller must abort via KIP-360 epoch bump).
    fn settleProduceBeforeAbort(c: *Client) bool {
        var unsettled = c.txn_produce_unsettled_until != null;
        for (c.outstanding.items) |*o| {
            if (o.corr < 0) continue;
            const conn = c.conns.get(o.ckey) orelse {
                unsettled = true;
                o.corr = -1;
                continue;
            };
            const deadline = o.sent.addDuration(.fromMilliseconds(produce_timeout_ms + 1000));
            const rem_ms = std.Io.Timestamp.now(c.io, .awake).durationTo(deadline).toMilliseconds();
            if (rem_ms <= 0) {
                unsettled = true;
                c.dropConn(o.ckey);
                continue;
            }
            conn.setReadDeadline(deadline);
            var results: std.AutoHashMapUnmanaged(i32, ProduceResult) = .empty;
            defer results.deinit(c.alloc);
            const frame = c.produceRecv(conn, o.corr, &results, null, null) catch {
                conn.setReadDeadline(null);
                unsettled = true;
                c.dropConn(o.ckey);
                continue;
            };
            defer c.alloc.free(frame);
            conn.setReadDeadline(null);
            if (!results.contains(o.pidx[0])) {
                unsettled = true;
                c.dropConn(o.ckey);
                continue;
            }
            o.corr = -1;
        }
        return unsettled or c.txn_produce_unsettled_until != null;
    }

    /// A sent ProduceRequest awaiting its response. Batches stay alive in the
    /// encoders so retriable partitions can be resent verbatim.
    const Outstanding = struct {
        ckey: u64,
        corr: i32, // <0 = the send never completed; all parts retriable
        pidx: []i32,
        batches: [][]const u8,
        counts: []u32,
        bytes: usize,
        /// When the send completed; valid only when corr >= 0.
        sent: std.Io.Timestamp = undefined,
        encoders: std.ArrayListUnmanaged(Encoder) = .empty,
    };

    const Inflight = struct { node: i32, key: u64, n: usize };
    const conns_per_partition = 2;

    pub fn connKey(node: i32, slot: u32) u64 {
        return (@as(u64, @as(u32, @bitCast(node))) << 32) | slot;
    }

    /// Pick (and pin) the connection for partition `pidx`: if the partition
    /// still has un-acked requests, reuse that conn; otherwise migrate it to
    /// the next of its conns_per_partition slots.
    fn claimConn(c: *Client, pidx: i32) !struct { node: i32, key: u64 } {
        if (c.inflight.getPtr(pidx)) |inf| {
            inf.n += 1;
            return .{ .node = inf.node, .key = inf.key };
        }
        const node = c.partitionLeader(pidx) orelse {
            c.setErr("no leader for partition {d}", .{pidx});
            return error.MetadataFailed;
        };
        const a = c.alt.get(pidx) orelse 0;
        try c.alt.put(c.alloc, pidx, a + 1);
        const slot = @as(u32, @intCast(pidx)) * conns_per_partition + a % conns_per_partition;
        const key = connKey(node, slot);
        try c.inflight.put(c.alloc, pidx, .{ .node = node, .key = key, .n = 1 });
        return .{ .node = node, .key = key };
    }

    /// One of the partition's outstanding requests resolved; when the count
    /// hits zero the binding is released and the next batch may migrate conns.
    fn releaseConn(c: *Client, pidx: i32) void {
        if (c.inflight.getPtr(pidx)) |inf| {
            inf.n -= 1;
            if (inf.n == 0) _ = c.inflight.remove(pidx);
        }
    }

    /// Encode `sets` into record batches and send one ProduceRequest per
    /// partition on that partition's own connection — all in flight at once.
    /// Returns after the sends complete; call produceDrain() to collect
    /// responses (mandatory before exit, and whenever outstanding_bytes grows).
    pub fn produceEnqueue(
        c: *Client,
        topic: []const u8,
        parts: []const usize,
        sets: []const []const protocol.Record,
    ) !void {
        try c.ensureProducerId();
        if (c.txn_id != null) {
            var new_parts: std.ArrayListUnmanaged(i32) = .empty;
            defer new_parts.deinit(c.alloc);
            for (parts) |pi| {
                const pidx: i32 = @intCast(pi);
                if (!c.txn_added_parts.contains(pidx)) {
                    var seen = false;
                    for (new_parts.items) |np| if (np == pidx) {
                        seen = true;
                        break;
                    };
                    if (!seen) try new_parts.append(c.alloc, pidx);
                }
            }
            if (new_parts.items.len > 0) try c.addPartitionsToTxn(topic, new_parts.items);
        }
        for (parts, 0..) |pi, i| {
            const pidx: i32 = @intCast(pi);
            // Drain before allocating a sequence and queuing another batch.
            if (c.producer_inited) {
                if (c.inflight.get(pidx)) |inf| {
                    if (inf.n >= max_inflight)
                        c.vlog("partition {d}: {d} in flight — draining for a slot", .{ pidx, inf.n });
                }
                try c.produceDrainStop(topic, .{ .partition_inflight = .{ .pidx = pidx, .max = max_inflight - 1 } });
            }
            var o: Outstanding = .{
                .ckey = 0,
                .corr = -1,
                .pidx = try c.alloc.alloc(i32, 1),
                .batches = try c.alloc.alloc([]const u8, 1),
                .counts = try c.alloc.alloc(u32, 1),
                .bytes = 0,
            };
            // Assign the batch's base sequence up front; the counter advances
            // at encode time so a retried batch keeps its original sequence.
            const base_seq: i32 = if (c.producer_inited)
                c.seqs.get(pidx) orelse 0
            else
                -1;
            var be = Encoder.init(c.alloc);
            protocol.encodeRecordBatch(
                &be,
                sets[i],
                std.Io.Timestamp.now(c.io, .real).toMilliseconds(),
                c.producer_id,
                c.producer_epoch,
                base_seq,
                c.txn_id != null,
            ) catch {
                be.deinit();
                return error.OutOfMemory;
            };
            if (base_seq >= 0)
                try c.seqs.put(c.alloc, pidx, base_seq + @as(i32, @intCast(sets[i].len)));
            try o.encoders.append(c.alloc, be);
            o.pidx[0] = pidx;
            o.batches[0] = be.written();
            o.counts[0] = @intCast(sets[i].len);
            o.bytes = be.written().len;
            try c.outstanding.ensureUnusedCapacity(c.alloc, 1);
            c.outstanding_bytes += o.bytes;
            if (c.claimConn(pidx)) |claim| {
                o.ckey = claim.key;
                if (c.connFor(claim.node, claim.key)) |conn| {
                    o.corr = c.produceSend(conn, topic, o.pidx, o.batches) catch blk: {
                        c.markProduceUnsettled();
                        break :blk -1;
                    };
                    if (o.corr < 0)
                        c.dropConn(claim.key)
                    else
                        o.sent = std.Io.Timestamp.now(c.io, .awake);
                } else |_| {}
            } else |_| {}
            c.outstanding.appendAssumeCapacity(o);
            // Do not send a higher sequence on a new socket before retrying
            // requests lost with the old socket (or a batch with no leader).
            if (o.corr < 0) {
                try c.produceDrain(topic);
            }
        }
    }

    /// Drain every outstanding ProduceResponse in send order, then retry
    /// retriable partitions (backoff + metadata refresh) for up to 30s.
    pub fn produceDrain(c: *Client, topic: []const u8) !void {
        return c.produceDrainUntil(topic, 0);
    }

    /// Receive responses for the oldest outstanding requests while
    /// outstanding_bytes exceeds `floor` (floor 0 drains all), then retry
    /// retriable partitions. Requests left outstanding stay in flight so the
    /// caller can keep a steady window of bytes on the wire.
    pub fn produceDrainUntil(c: *Client, topic: []const u8, floor: usize) !void {
        return c.produceDrainStop(topic, .{ .bytes_floor = floor });
    }

    /// Broker requires ≤5 in-flight produce requests per partition for
    /// sequence dedup to hold.
    const max_inflight = 5;

    const DrainStop = union(enum) {
        /// Drain while outstanding_bytes exceeds the floor.
        bytes_floor: usize,
        /// Drain while partition pidx has more than `max` un-acked requests.
        partition_inflight: struct { pidx: i32, max: usize },
    };

    fn drainStopActive(c: *Client, stop: DrainStop) bool {
        return switch (stop) {
            .bytes_floor => |f| c.outstanding_bytes > f,
            .partition_inflight => |s| if (c.inflight.get(s.pidx)) |inf| inf.n > s.max else false,
        };
    }

    fn produceDrainStop(c: *Client, topic: []const u8, stop: DrainStop) !void {
        const PendingPart = struct { pidx: i32, batch: []const u8, count: u32 };
        var retry: std.ArrayListUnmanaged(PendingPart) = .empty;
        defer retry.deinit(c.alloc);

        var done: usize = 0;
        defer {
            for (c.outstanding.items[0..done]) |*o| {
                for (o.encoders.items) |*e| e.deinit();
                o.encoders.deinit(c.alloc);
                c.alloc.free(o.pidx);
                c.alloc.free(o.batches);
            }
            const rest = c.outstanding.items.len - done;
            std.mem.copyForwards(Outstanding, c.outstanding.items[0..rest], c.outstanding.items[done..]);
            c.outstanding.items.len = rest;
        }

        while (done < c.outstanding.items.len and (retry.items.len > 0 or c.drainStopActive(stop))) {
            const o = &c.outstanding.items[done];
            done += 1;
            c.outstanding_bytes -= o.bytes;
            c.releaseConn(o.pidx[0]);
            if (o.corr >= 0 and c.conns.contains(o.ckey)) {
                var results: std.AutoHashMapUnmanaged(i32, ProduceResult) = .empty;
                defer results.deinit(c.alloc);
                if (c.produceRecv(c.conns.get(o.ckey).?, o.corr, &results, o, null)) |frame| {
                    defer c.alloc.free(frame);
                    o.corr = -1;
                    c.recordAck(o.sent);
                    for (o.pidx, o.batches, o.counts) |pi, b, count| {
                        const result = results.get(pi) orelse ProduceResult{};
                        const code = result.code;
                        switch (code) {
                            // duplicate_sequence_number: broker already
                            // appended this batch — dedup success.
                            .none, .duplicate_sequence_number => {},
                            else => if (code.retriable()) {
                                c.dropConn(o.ckey);
                                try retry.append(c.alloc, .{ .pidx = pi, .batch = b, .count = count });
                            } else {
                                c.produceErr(topic, pi, result, b.len);
                                return error.ProduceFailed;
                            },
                        }
                    }
                } else |_| {
                    c.dropConn(o.ckey);
                    for (o.pidx, o.batches, o.counts) |pi, b, count|
                        try retry.append(c.alloc, .{ .pidx = pi, .batch = b, .count = count });
                }
            } else {
                for (o.pidx, o.batches, o.counts) |pi, b, count|
                    try retry.append(c.alloc, .{ .pidx = pi, .batch = b, .count = count });
            }
        }

        var budget: Retry = .{};
        while (retry.items.len > 0 and budget.pause(c)) {
            c.produce_retries += retry.items.len;
            c.vlog("retrying {d} batch(es) after metadata refresh", .{retry.items.len});
            c.refreshMetadata(topic) catch continue;

            const items = try retry.toOwnedSlice(c.alloc);
            defer c.alloc.free(items);
            for (items) |pp| {
                // A later sequence must not pass an earlier failed batch.
                var blocked = false;
                for (retry.items) |pending| {
                    if (pending.pidx == pp.pidx) {
                        blocked = true;
                        break;
                    }
                }
                if (blocked) {
                    try retry.append(c.alloc, pp);
                    continue;
                }
                const claim = c.claimConn(pp.pidx) catch {
                    try retry.append(c.alloc, pp);
                    continue;
                };
                const conn = c.connFor(claim.node, claim.key) catch {
                    c.releaseConn(pp.pidx);
                    try retry.append(c.alloc, pp);
                    continue;
                };
                const corr = c.produceSend(conn, topic, &.{pp.pidx}, &.{pp.batch}) catch {
                    c.markProduceUnsettled();
                    c.dropConn(claim.key);
                    c.releaseConn(pp.pidx);
                    try retry.append(c.alloc, pp);
                    continue;
                };
                const sent = std.Io.Timestamp.now(c.io, .awake);
                var results: std.AutoHashMapUnmanaged(i32, ProduceResult) = .empty;
                defer results.deinit(c.alloc);
                const frame = c.produceRecv(conn, corr, &results, null, pp.count) catch {
                    c.markProduceUnsettled();
                    c.dropConn(claim.key);
                    c.releaseConn(pp.pidx);
                    try retry.append(c.alloc, pp);
                    continue;
                };
                defer c.alloc.free(frame);
                c.recordAck(sent);
                const result = results.get(pp.pidx) orelse ProduceResult{};
                const code = result.code;
                switch (code) {
                    .none, .duplicate_sequence_number => c.releaseConn(pp.pidx),
                    else => if (code.retriable()) {
                        c.dropConn(claim.key);
                        c.releaseConn(pp.pidx);
                        try retry.append(c.alloc, pp);
                    } else {
                        c.produceErr(topic, pp.pidx, result, pp.batch.len);
                        return error.ProduceFailed;
                    },
                }
            }
        }
        if (retry.items.len > 0) {
            c.setErr("produce to {s}: delivery timed out after 30s", .{topic});
            return error.ProduceFailed;
        }
    }

    /// Mean send-complete→response time across acked ProduceRequests, or
    /// null when nothing received a response.
    pub fn avgAckMs(c: *const Client) ?f64 {
        if (c.produce_acked == 0) return null;
        return @as(f64, @floatFromInt(c.produce_ack_ns)) /
            @as(f64, @floatFromInt(c.produce_acked)) / 1e6;
    }

    fn recordAck(c: *Client, sent: std.Io.Timestamp) void {
        c.produce_ack_ns += ackNanos(sent, std.Io.Timestamp.now(c.io, .awake));
        c.produce_acked += 1;
    }

    pub fn partitionLeader(c: *Client, pidx: i32) ?i32 {
        for (c.partitions.items) |p|
            if (p.index == pidx) return if (p.leader >= 0) p.leader else null;
        return null;
    }

    fn dropControl(c: *Client) void {
        if (c.control) |old| {
            old.close();
            c.alloc.destroy(old);
            c.control = null;
        }
    }

    /// Rebuild control through discovered brokers, then configured bootstrap
    /// servers, with the full handshake.
    fn reconnectControl(c: *Client) !void {
        _ = try c.reconnectControlAt(0);
    }

    fn reconnectControlAt(c: *Client, start: usize) !usize {
        c.dropControl();
        const count = c.brokers.count();
        var tried: usize = 0;
        while (tried < count) : (tried += 1) {
            if (c.txn_stop) |flag| if (flag.load(.acquire)) return error.Interrupted;
            const node = c.brokerNodeAt(start +% tried) orelse break;
            const addr = c.brokers.get(node) orelse continue;
            const conn = c.connectOne(addr.host, addr.port) catch continue;
            c.control = conn;
            c.vlog("control connected {s}:{d}", .{ addr.host, addr.port });
            return tried;
        }
        if (c.txn_stop) |flag| if (flag.load(.acquire)) return error.Interrupted;
        try c.bootstrap();
        return count;
    }

    pub fn sleep(c: *Client, ms: u64) void {
        _ = c.sleepAbortable(ms);
    }

    pub fn sleepAbortable(c: *Client, ms: u64) bool {
        if (c.wait_hook == null or ms == 0) {
            std.Io.sleep(c.io, .fromMilliseconds(@intCast(ms)), .awake) catch {};
            return false;
        }
        const end = std.Io.Timestamp.now(c.io, .awake).addDuration(.fromMilliseconds(@intCast(ms)));
        while (true) {
            const now = std.Io.Timestamp.now(c.io, .awake);
            const left = now.durationTo(end).toMilliseconds();
            if (left <= 0) return false;
            var step: u64 = @intCast(left);
            var hook_due = false;
            if (c.wait_hook) |hook| {
                if (hook.next_due(hook.ctx)) |due| {
                    const until = now.durationTo(due).toMilliseconds();
                    if (until <= 0) {
                        if (hook.tick(hook.ctx)) return true;
                        continue;
                    }
                    if (@as(u64, @intCast(until)) <= step) {
                        step = @intCast(until);
                        hook_due = true;
                    }
                }
            }
            std.Io.sleep(c.io, .fromMilliseconds(@intCast(step)), .awake) catch {};
            if (hook_due) if (c.wait_hook) |hook| if (hook.tick(hook.ctx)) return true;
        }
    }

    pub fn connFor(c: *Client, node: i32, key: u64) !*Conn {
        if (node < 0) return error.MetadataFailed;
        if (c.conns.get(key)) |conn| {
            if (!conn.closed) return conn;
            c.dropConn(key);
        }
        const addr = c.brokers.get(node) orelse {
            c.setErr("no address for broker node {d}", .{node});
            return error.MetadataFailed;
        };
        const conn = try c.connectOne(addr.host, addr.port);
        errdefer {
            conn.close();
            c.alloc.destroy(conn);
        }
        c.vlog("connected broker {d} at {s}:{d}", .{ node, addr.host, addr.port });
        try c.conns.put(c.alloc, key, conn);
        return conn;
    }

    pub fn dropConn(c: *Client, key: u64) void {
        if (c.conns.fetchRemove(key)) |kv| {
            for (c.outstanding.items) |*o| {
                if (o.ckey == key and o.corr >= 0) {
                    c.markProduceUnsettledFrom(o.sent);
                    o.corr = -1;
                }
            }
            c.vlog("dropping connection to broker {d}", .{key >> 32});
            kv.value.close();
            c.alloc.destroy(kv.value);
        }
    }

    pub fn dropNode(c: *Client, node: i32) void {
        if (node < 0) return;
        while (true) {
            var keys = c.conns.keyIterator();
            var found: ?u64 = null;
            while (keys.next()) |key| {
                if (key.* >> 32 == @as(u32, @intCast(node))) {
                    found = key.*;
                    break;
                }
            }
            c.dropConn(found orelse return);
        }
    }

    /// Send a ProduceRequest covering `pidx`/`batches` (one batch per
    /// partition) on `conn` without waiting for the response; returns the
    /// correlation id. Batch bytes are spliced into the frame verbatim —
    /// never copied into a contiguous request buffer.
    fn produceSend(
        c: *Client,
        conn: *Conn,
        topic: []const u8,
        pidx: []const i32,
        batches: []const []const u8,
    ) !i32 {
        transport.setTimeout(conn, produce_timeout_ms + 1000);
        c.produce_requests += 1;
        var parts: std.ArrayListUnmanaged([]const u8) = .empty;
        defer parts.deinit(c.alloc);
        // Small per-partition prefix/tail encoders kept alive until the send
        // completes; their written() slices are the parts.
        var keep: std.ArrayListUnmanaged(Encoder) = .empty;
        defer {
            for (keep.items) |*k| k.deinit();
            keep.deinit(c.alloc);
        }

        var head = Encoder.init(c.alloc);
        try protocol.encodeRequestHeader(&head, protocol.api_key.produce, protocol.version.produce, true, "kite");
        try head.compactString(c.txn_id); // transactional_id
        try head.i16v(-1); // acks=all
        try head.i32v(produce_timeout_ms);
        try head.compactArrayLen(1); // one topic
        try head.compactString(topic);
        try head.compactArrayLen(pidx.len);
        try keep.append(c.alloc, head);
        try parts.append(c.alloc, keep.items[keep.items.len - 1].written());

        for (pidx, batches) |p, b| {
            var ph = Encoder.init(c.alloc);
            try ph.i32v(p);
            try ph.uvarint(b.len + 1); // compact records length
            try keep.append(c.alloc, ph);
            try parts.append(c.alloc, keep.items[keep.items.len - 1].written());
            try parts.append(c.alloc, b);
        }

        var tail = Encoder.init(c.alloc);
        try tail.tagBuffer(); // partition tags
        try tail.tagBuffer(); // topic tags
        try tail.tagBuffer(); // request tags
        try keep.append(c.alloc, tail);
        try parts.append(c.alloc, keep.items[keep.items.len - 1].written());

        try transport.sendv(conn, parts.items);
        return protocol.lastCorrelationId();
    }

    /// Read the ProduceResponse for correlation id `corr`. The caller owns
    /// the returned frame, which holds the per-partition result messages.
    fn produceRecv(
        c: *Client,
        conn: *Conn,
        corr: i32,
        results: *std.AutoHashMapUnmanaged(i32, ProduceResult),
        outstanding: ?*const Outstanding,
        retry_count: ?u32,
    ) ![]u8 {
        const resp = try transport.recv(conn, c.alloc, corr, true);
        errdefer c.alloc.free(resp.frame);

        var d = Decoder.init(resp.body);
        const ntopics = try d.compactArrayLen();
        var t: i64 = 0;
        while (t < ntopics) : (t += 1) {
            _ = try d.compactString(); // name
            const nparts = try d.compactArrayLen();
            var p: i64 = 0;
            while (p < nparts) : (p += 1) {
                const pidx = try d.i32v();
                const code: protocol.ErrorCode = @enumFromInt(try d.i16v());
                const base_offset = try d.i64v();
                _ = try d.i64v(); // log_append_time_ms
                _ = try d.i64v(); // log_start_offset
                const nerrs = try d.compactArrayLen(); // record_errors
                var r: i64 = 0;
                while (r < nerrs) : (r += 1) {
                    _ = try d.i32v();
                    _ = try d.compactString();
                    try d.tagBuffer();
                }
                const message = try d.compactString();
                try d.tagBuffer(); // partition tags
                try results.put(c.alloc, pidx, .{ .code = code, .message = message });
                c.recordFenced(code);
                if (code == .none) {
                    const count = if (outstanding) |o| blk: {
                        for (o.pidx, o.counts) |op, oc| if (op == pidx) break :blk oc;
                        break :blk 0;
                    } else retry_count orelse 0;
                    if (count > 0) {
                        const last = base_offset + @as(i64, count) - 1;
                        if (c.last_offset == null or last > c.last_offset.?) c.last_offset = last;
                        try c.noteLastOffset(pidx, last);
                        c.acked_records += count;
                    }
                }
            }
            try d.tagBuffer(); // topic tags
        }
        const throttle = try d.i32v();
        if (throttle > 0) {
            if (term.color.enabled)
                std.debug.print("{s}kite:{s} broker throttled produce {d}ms\n", .{ term.yellow, term.reset, throttle })
            else
                std.debug.print("kite: broker throttled produce {d}ms\n", .{throttle});
        }
        try d.tagBuffer();
        return resp.frame;
    }
};

test "classic describe uses the cached coordinator when consumer describe is unavailable" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    var c = Client.init(alloc, io, &env, &cfg);
    defer c.deinit();
    c.coordinator_group = try alloc.dupe(u8, "g");
    c.coordinator_node = 2;
    try c.api_ranges.put(alloc, protocol.api_key.describe_groups, .{ .min = 0, .max = 6 });
    try c.brokers.put(alloc, 3, .{ .host = try alloc.dupe(u8, "unreachable.invalid"), .port = 1 });
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(io, .{});
    defer server.deinit(io);
    const conn = try transport.connect(io, alloc, &env, "127.0.0.1", server.socket.address.getPort(), null, null);
    try c.conns.put(alloc, Client.connKey(2, 0xFFFF_FFFE), conn);
    const peer = try server.accept(io);
    defer peer.close(io);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    for (0..4) |case| {
        const unsupported = case == 3;
        const next_corr = protocol.lastCorrelationId() + 1;
        var buffer: [128]u8 = undefined;
        var writer = peer.writer(io, &buffer);
        if (unsupported) {
            try c.api_ranges.put(alloc, protocol.api_key.consumer_group_describe, .{ .min = 0, .max = 0 });
            const body = "\x00\x00\x00\x00\x02\x00\x23\x00\x02g\x01\x00\x00\x00\x00\x00\x00\x00\x00\x01\x01\x80\x00\x00\x00\x00\x00";
            var header: [8]u8 = undefined;
            std.mem.writeInt(u32, header[0..4], 5 + body.len, .big);
            std.mem.writeInt(i32, header[4..8], next_corr, .big);
            try writer.interface.writeAll(&header);
            try writer.interface.writeByte(0);
            try writer.interface.writeAll(body);
        }
        var response = Encoder.init(alloc);
        defer response.deinit();
        try response.i32v(next_corr + @as(i32, @intFromBool(unsupported)));
        try response.tagBuffer();
        try response.i32v(0);
        try response.compactArrayLen(1);
        try response.i16v(switch (case) {
            0, 3 => 0,
            1 => @intFromEnum(protocol.ErrorCode.group_id_not_found),
            else => @intFromEnum(protocol.ErrorCode.group_authorization_failed),
        });
        try response.compactString(null);
        for ([_][]const u8{ "g", if (case == 0 or unsupported) "Empty" else "", "", "" }) |field| try response.compactString(field);
        try response.compactArrayLen(0);
        try response.i32v(std.math.minInt(i32));
        try response.tagBuffer();
        try response.tagBuffer();
        var length: [4]u8 = undefined;
        std.mem.writeInt(u32, &length, @intCast(response.written().len), .big);
        try writer.interface.writeAll(&length);
        try writer.interface.writeAll(response.written());
        try writer.interface.flush();
        switch (case) {
            0, 3 => {
                const group = try c.describeGroup(arena.allocator(), "g");
                try std.testing.expectEqualStrings("Empty", group.state);
                try std.testing.expectEqual(@as(usize, 0), group.partitions.len);
            },
            1 => try std.testing.expectError(error.GroupNotFound, c.describeGroup(arena.allocator(), "g")),
            else => try std.testing.expectError(error.GroupFailed, c.describeGroup(arena.allocator(), "g")),
        }
        try std.testing.expectEqual(next_corr + @as(i32, @intFromBool(unsupported)), protocol.lastCorrelationId());
        try std.testing.expect(c.control == null);
        try std.testing.expectEqual(@as(u32, 1), c.conns.count());
    }
}

test "socket request timeouts exceed long polls and respect the recovery deadline" {
    var c: Client = undefined;
    c.io = std.testing.io;
    c.group_deadline = null;
    try std.testing.expectEqual(@as(u64, 15000), try c.requestTimeoutMs(0));
    try std.testing.expectEqual(@as(u64, 31000), try c.requestTimeoutMs(30000));
    c.group_deadline = std.Io.Timestamp.now(c.io, .awake).addDuration(.fromMilliseconds(10000));
    const timeout = try c.requestTimeoutMs(5000);
    try std.testing.expect(timeout > 5000 and timeout <= 10000);
    try std.testing.expectError(error.GroupDeadlineExceeded, c.requestTimeoutMs(10000));
    c.group_deadline = std.Io.Timestamp.now(c.io, .awake).addDuration(.fromMilliseconds(-1));
    try std.testing.expectError(error.GroupDeadlineExceeded, c.requestTimeoutMs(0));
}

test "group retry deadline is shared across nested retries" {
    var c: Client = undefined;
    c.io = std.testing.io;
    c.group_deadline = std.Io.Timestamp.now(c.io, .awake).addDuration(.fromMilliseconds(-1));
    var outer: Retry = .{};
    var inner: Retry = .{};
    try std.testing.expect(!outer.pause(&c));
    try std.testing.expect(!inner.pause(&c));
    try std.testing.expectError(error.GroupDeadlineExceeded, c.groupRemainingMs());
    c.group_deadline = std.Io.Timestamp.now(c.io, .awake).addDuration(.fromMilliseconds(45000));
    outer.started = std.Io.Timestamp.now(c.io, .awake).addDuration(.fromMilliseconds(-35000));
    outer.backoff_ms = 0;
    try std.testing.expect(outer.pause(&c));
    try std.testing.expect(try c.groupRemainingMs() > 44000);
    c.group_deadline = null;
    try std.testing.expectEqual(@as(u64, 15000), try c.groupRemainingMs());
    try std.testing.expectError(error.StaleMemberEpoch, c.groupResult(.stale_member_epoch));
}

fn ackNanos(sent: std.Io.Timestamp, now: std.Io.Timestamp) u64 {
    return @intCast(@max(0, sent.durationTo(now).toNanoseconds()));
}

test "avgAckMs averages send-to-ack nanoseconds" {
    var c: Client = undefined;
    c.produce_acked = 0;
    c.produce_ack_ns = 0;
    try std.testing.expectEqual(@as(?f64, null), c.avgAckMs());
    c.produce_acked = 2;
    c.produce_ack_ns = 3_000_000;
    try std.testing.expectEqual(@as(?f64, 1.5), c.avgAckMs());

    const sent: std.Io.Timestamp = .{ .nanoseconds = 1_000 };
    const now: std.Io.Timestamp = .{ .nanoseconds = 2_500 };
    try std.testing.expectEqual(@as(u64, 1_500), ackNanos(sent, now));
    try std.testing.expectEqual(@as(u64, 0), ackNanos(now, sent));
}

test "leaderless partitions cannot be claimed or connected" {
    var c: Client = undefined;
    c.partitions = .empty;
    try std.testing.expectEqual(@as(?i32, null), c.partitionLeader(0));
    try c.partitions.append(std.testing.allocator, .{ .index = 0, .leader = -1 });
    defer c.partitions.deinit(std.testing.allocator);
    c.inflight = .empty;
    try std.testing.expectEqual(@as(?i32, null), c.partitionLeader(0));
    try std.testing.expectError(error.MetadataFailed, c.claimConn(0));
    try std.testing.expectError(error.MetadataFailed, c.connFor(-1, Client.connKey(-1, 0)));
    c.dropNode(-1);
    c.partitions.items[0].leader = 2;
    try std.testing.expectEqual(@as(?i32, 2), c.partitionLeader(0));
}

test "draining all inflight batches releases the old leader binding" {
    var c: Client = undefined;
    c.alloc = std.testing.allocator;
    c.partitions = .empty;
    defer c.partitions.deinit(c.alloc);
    try c.partitions.append(c.alloc, .{ .index = 0, .leader = 2 });
    c.inflight = .empty;
    defer c.inflight.deinit(c.alloc);
    c.alt = .empty;
    defer c.alt.deinit(c.alloc);
    try c.inflight.put(c.alloc, 0, .{ .node = 1, .key = Client.connKey(1, 0), .n = 2 });
    c.releaseConn(0);
    try std.testing.expect(c.inflight.contains(0));
    c.releaseConn(0);
    try std.testing.expect(!c.inflight.contains(0));
    const claim = try c.claimConn(0);
    try std.testing.expectEqual(@as(i32, 2), claim.node);
    try std.testing.expectEqual(Client.connKey(2, 0), claim.key);
}

test "rebuilding broker metadata releases old hosts including an empty update" {
    var c: Client = undefined;
    c.alloc = std.testing.allocator;
    c.brokers = .empty;
    defer c.freeBrokers(&c.brokers);
    c.conns = .empty;
    var e = Encoder.init(c.alloc);
    defer e.deinit();
    try e.compactArrayLen(1);
    try e.i32v(1);
    try e.compactString("localhost");
    try e.i32v(9092);
    try e.compactString(null);
    try e.tagBuffer();
    var d = Decoder.init(e.written());
    try c.decodeBrokers(&d);
    d = Decoder.init(e.written());
    try c.decodeBrokers(&d);
    try std.testing.expectEqualStrings("localhost", c.brokers.get(1).?.host);
    d = Decoder.init(e.written()[0 .. e.written().len - 1]);
    try std.testing.expectError(error.Truncated, c.decodeBrokers(&d));
    try std.testing.expectEqualStrings("localhost", c.brokers.get(1).?.host);
    const empty = [_]u8{1};
    d = Decoder.init(&empty);
    try c.decodeBrokers(&d);
    try std.testing.expectEqual(@as(u32, 0), c.brokers.count());
}

test "produce error sanitizes and bounds broker detail" {
    var c: Client = undefined;
    c.err_ctx = @splat(0);
    c.produceErr("topic", 0, .{ .code = .invalid_request, .message = "bad\x1b[31m\n\r\x00\x7f\xff!" }, 0);
    try std.testing.expectEqualStrings("produce to topic[0]: INVALID_REQUEST: bad?[31m?????!", c.errDetail());

    const long_message = [_]u8{0x1b} ** 512;
    c.produceErr("topic", 0, .{ .code = .invalid_request, .message = &long_message }, 0);
    const detail = c.errDetail();
    try std.testing.expectEqual(c.err_ctx.len - 1, detail.len);
    try std.testing.expectEqual(@as(u8, '?'), detail[detail.len - 1]);
}

test "invalid record appends the compaction hint only for a missing key" {
    var c: Client = undefined;
    c.err_ctx = @splat(0);
    c.produceErr("topic", 0, .{ .code = .invalid_record, .message = "schema ID validation failed" }, 0);
    try std.testing.expectEqualStrings(
        "produce to topic[0]: INVALID_RECORD: schema ID validation failed",
        c.errDetail(),
    );

    c.produceErr("topic", 0, .{ .code = .invalid_record }, 0);
    try std.testing.expectEqualStrings("produce to topic[0]: INVALID_RECORD", c.errDetail());

    c.produceErr("topic", 0, .{ .code = .invalid_record, .message = "Compacted topic cannot accept message without key" }, 0);
    try std.testing.expectEqualStrings(
        "produce to topic[0]: INVALID_RECORD: Compacted topic cannot accept message without key; if the topic is compacted, records need a non-null key",
        c.errDetail(),
    );

    const long_message = ("x" ** 512) ++ " WITHOUT KEY";
    c.produceErr("topic", 0, .{ .code = .invalid_record, .message = long_message }, 0);
    const detail = c.errDetail();
    try std.testing.expectEqual(c.err_ctx.len - 1, detail.len);
    try std.testing.expect(std.mem.endsWith(u8, detail, "; if the topic is compacted, records need a non-null key"));
}

test "dropping a produce socket invalidates only its outstanding responses" {
    var c: Client = undefined;
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    c.cfg = &cfg;
    c.alloc = std.testing.allocator;
    c.conns = .empty;
    defer c.conns.deinit(c.alloc);
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    const server = try addr.listen(std.testing.io, .{});
    const conn = try c.alloc.create(Conn);
    conn.stream = .{ .socket = server.socket };
    conn.io = std.testing.io;
    const dropped = Client.connKey(1, 0);
    try c.conns.put(c.alloc, dropped, conn);
    defer c.dropConn(dropped);

    var pending = [_]Client.Outstanding{
        .{ .ckey = dropped, .corr = 1, .pidx = &.{}, .batches = &.{}, .counts = &.{}, .bytes = 0 },
        .{ .ckey = Client.connKey(1, 1), .corr = 2, .pidx = &.{}, .batches = &.{}, .counts = &.{}, .bytes = 0 },
        .{ .ckey = dropped, .corr = 3, .pidx = &.{}, .batches = &.{}, .counts = &.{}, .bytes = 0 },
    };
    c.outstanding = .{ .items = &pending, .capacity = pending.len };
    c.dropConn(dropped);
    try std.testing.expect(!c.conns.contains(dropped));
    try std.testing.expectEqual(@as(i32, -1), pending[0].corr);
    try std.testing.expectEqual(@as(i32, 2), pending[1].corr);
    try std.testing.expectEqual(@as(i32, -1), pending[2].corr);
}

test "bootstrap server parser rejects invalid explicit ports" {
    const defaulted = try parseBootstrapServer("localhost");
    try std.testing.expectEqualStrings("localhost", defaulted.host);
    try std.testing.expectEqual(@as(u16, 9092), defaulted.port);

    const explicit = try parseBootstrapServer("broker.example:19092");
    try std.testing.expectEqualStrings("broker.example", explicit.host);
    try std.testing.expectEqual(@as(u16, 19092), explicit.port);

    try std.testing.expectError(error.InvalidBootstrapServer, parseBootstrapServer("localhost:"));
    try std.testing.expectError(error.InvalidBootstrapServer, parseBootstrapServer("localhost:nope"));
    try std.testing.expectError(error.InvalidBootstrapServer, parseBootstrapServer("localhost:0"));
    try std.testing.expectError(error.InvalidBootstrapServer, parseBootstrapServer("localhost:65536"));
}

test "group APIs check versions lazily and coordinator cache is group scoped" {
    var c: Client = undefined;
    c.alloc = std.testing.allocator;
    c.api_ranges = .empty;
    defer c.api_ranges.deinit(c.alloc);
    c.err_ctx = std.mem.zeroes([512]u8);
    c.coordinator_group = "g";
    c.coordinator_node = 7;
    try std.testing.expectEqual(@as(i32, 7), try c.findCoordinator("g"));
    try std.testing.expectError(error.AdminFailed, c.findCoordinator("other"));
    try std.testing.expectError(error.AdminFailed, c.listGroups(c.alloc, false));
    try std.testing.expectError(error.AdminFailed, c.describeGroup(c.alloc, "g"));
    try std.testing.expectError(error.AdminFailed, c.fetchGroupOffsets(c.alloc, .{ .group = "g" }));
    try std.testing.expectError(error.AdminFailed, c.heartbeat(c.alloc, .{ .group = "g", .member_id = "m", .member_epoch = 0 }));
    try std.testing.expectError(error.AdminFailed, c.commitGroupOffsets(c.alloc, .{ .group = "g", .topics = &.{} }));
    try c.api_ranges.put(c.alloc, protocol.api_key.list_groups, .{ .min = 3, .max = 5 });
    try c.requireVersion(protocol.api_key.list_groups, 5, "ListGroups");
    try std.testing.expectError(error.AdminFailed, c.requireVersion(protocol.api_key.list_groups, 6, "ListGroups"));
    try std.testing.expect(Client.isCoordinatorError(.group_load_in_progress));
    try std.testing.expect(Client.isCoordinatorError(.not_coordinator));
    try std.testing.expect(Client.isCoordinatorError(.coordinator_not_available));
    try std.testing.expect(!Client.isCoordinatorError(.group_authorization_failed));
}

test "abort treats fenced producer as already aborted by broker while commit still fails" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const cfg: config.Config = .{ .bootstrap_servers = &.{} };
    var c = Client.init(alloc, io, &env, &cfg);
    defer c.deinit();

    // Case 1: already fenced (e.g. Produce returned INVALID_PRODUCER_EPOCH after broker timeout).
    c.txn_id = "tx-fenced";
    c.producer_inited = true;
    c.producer_id = 42;
    c.producer_epoch = 0;
    c.txn_add_attempted = true;
    try c.txn_added_parts.put(alloc, 0, {});
    c.txn_fenced = true;
    try std.testing.expect(c.abortTransactionBestEffort());
    try std.testing.expect(!c.hasActiveTxn());
    try std.testing.expectEqual(@as(u32, 0), c.txn_added_parts.count());

    // Set up a mock transaction coordinator socket for EndTxn responses.
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(io, .{});
    defer server.deinit(io);
    const conn = try transport.connect(io, alloc, &env, "127.0.0.1", server.socket.address.getPort(), null, null);
    try c.conns.put(alloc, Client.connKey(1, Client.txn_coord_slot), conn);
    c.txn_coordinator_node = 1;
    const peer = try server.accept(io);
    defer peer.close(io);

    const writeEndTxnRespWithCorr = struct {
        fn send(p: std.Io.net.Stream, a: std.mem.Allocator, corr: i32, code: protocol.ErrorCode) !void {
            var body = Encoder.init(a);
            defer body.deinit();
            try body.i32v(corr);
            try body.tagBuffer();
            try body.i32v(0); // throttle_time_ms
            try body.i16v(@intFromEnum(code));
            try body.tagBuffer();
            var frame = Encoder.init(a);
            defer frame.deinit();
            try frame.i32v(@intCast(body.written().len));
            try frame.raw(body.written());
            var wbuf: [128]u8 = undefined;
            var w = p.writer(std.testing.io, &wbuf);
            try w.interface.writeAll(frame.written());
            try w.interface.flush();
        }
    }.send;
    const writeEndTxnResp = struct {
        fn send(p: std.Io.net.Stream, a: std.mem.Allocator, code: protocol.ErrorCode) !void {
            try writeEndTxnRespWithCorr(p, a, protocol.lastCorrelationId() + 1, code);
        }
    }.send;

    // Case 2: EndTxn(abort) returns PRODUCER_FENCED -> known aborted, no error.
    c.txn_fenced = false;
    c.txn_add_attempted = true;
    try c.txn_added_parts.put(alloc, 0, {});
    try writeEndTxnResp(peer, alloc, .producer_fenced);
    try std.testing.expect(c.abortTransactionBestEffort());
    try std.testing.expect(c.txn_fenced);
    try std.testing.expect(!c.hasActiveTxn());

    // Case 3: EndTxn(abort) returns INVALID_PRODUCER_EPOCH -> known aborted, no error.
    c.txn_fenced = false;
    c.txn_add_attempted = true;
    try c.txn_added_parts.put(alloc, 0, {});
    try writeEndTxnResp(peer, alloc, .invalid_producer_epoch);
    try std.testing.expect(c.abortTransactionBestEffort());
    try std.testing.expect(c.txn_fenced);
    try std.testing.expect(!c.hasActiveTxn());

    // Case 4a: Unambiguous first EndTxn(commit) returns PRODUCER_FENCED ->
    // fails with "aborted by Kafka" context and clears active txn state.
    c.txn_fenced = false;
    c.txn_fence_code = null;
    c.txn_add_attempted = true;
    try c.txn_added_parts.put(alloc, 0, {});
    try writeEndTxnResp(peer, alloc, .producer_fenced);
    try std.testing.expectError(error.ProduceFailed, c.endTransaction(true, null));
    try std.testing.expect(std.mem.indexOf(u8, c.errDetail(), "aborted by Kafka") != null);
    try std.testing.expect(c.txn_fenced);
    try std.testing.expect(!c.txn_commit_dispatched);
    try std.testing.expect(!c.hasActiveTxn());
    try std.testing.expect(c.abortTransactionBestEffort());

    // Case 4b: Ambiguous retried EndTxn(commit) (first attempt retriable, retry
    // returns PRODUCER_FENCED) -> fails with "outcome unknown", keeps
    // txn_commit_dispatched set, and a subsequent abort must not claim aborted.
    c.txn_fenced = false;
    c.txn_fence_code = null;
    c.txn_add_attempted = true;
    try c.txn_added_parts.put(alloc, 0, {});
    const base_corr = protocol.lastCorrelationId();
    try writeEndTxnRespWithCorr(peer, alloc, base_corr + 1, .concurrent_transactions);
    try writeEndTxnRespWithCorr(peer, alloc, base_corr + 2, .producer_fenced);
    try std.testing.expectError(error.ProduceFailed, c.endTransaction(true, null));
    try std.testing.expect(std.mem.indexOf(u8, c.errDetail(), "outcome unknown") != null);
    try std.testing.expect(c.txn_fenced);
    try std.testing.expect(c.txn_commit_dispatched);
    try std.testing.expectError(error.ProduceFailed, c.endTransaction(false, null));
    try std.testing.expect(!c.abortTransactionBestEffort());
    try std.testing.expect(!c.hasActiveTxn());

    // Case 5: EndTxn(abort) returns a non-fenced fatal error -> still reports failure.
    c.txn_fenced = false;
    c.txn_fence_code = null;
    c.txn_add_attempted = true;
    try c.txn_added_parts.put(alloc, 0, {});
    try writeEndTxnResp(peer, alloc, .transactional_id_authorization_failed);
    try std.testing.expectError(error.ProduceFailed, c.endTransaction(false, null));
    c.clearTxnState();

    // Case 6: Fencing first discovered while settleProduceBeforeAbort() drains
    // an in-flight ProduceResponse skips EndTxn(abort) and succeeds cleanly.
    var prod_server = try addr.listen(io, .{});
    defer prod_server.deinit(io);
    const prod_conn = try transport.connect(io, alloc, &env, "127.0.0.1", prod_server.socket.address.getPort(), null, null);
    const prod_key = Client.connKey(2, 0);
    try c.conns.put(alloc, prod_key, prod_conn);
    const prod_peer = try prod_server.accept(io);
    defer prod_peer.close(io);

    const pidx_slice = try alloc.alloc(i32, 1);
    pidx_slice[0] = 0;
    try c.outstanding.append(alloc, .{
        .ckey = prod_key,
        .corr = 77,
        .pidx = pidx_slice,
        .batches = try alloc.alloc([]const u8, 0),
        .counts = try alloc.alloc(u32, 0),
        .bytes = 0,
        .sent = std.Io.Timestamp.now(io, .awake),
    });

    // Write a ProduceResponse v11 with INVALID_PRODUCER_EPOCH for partition 0.
    var pbody = Encoder.init(alloc);
    defer pbody.deinit();
    try pbody.i32v(77); // correlation_id
    try pbody.tagBuffer(); // response header v1 tags
    try pbody.compactArrayLen(1); // responses (1 topic)
    try pbody.compactString("t"); // topic name
    try pbody.compactArrayLen(1); // partition_responses (1 partition)
    try pbody.i32v(0); // partition index
    try pbody.i16v(@intFromEnum(protocol.ErrorCode.invalid_producer_epoch));
    try pbody.i64v(-1); // base_offset
    try pbody.i64v(-1); // log_append_time_ms
    try pbody.i64v(-1); // log_start_offset
    try pbody.compactArrayLen(0); // record_errors
    try pbody.compactString(null); // error_message
    try pbody.tagBuffer(); // partition tags
    try pbody.tagBuffer(); // topic tags
    try pbody.i32v(0); // throttle_time_ms
    try pbody.tagBuffer(); // response tags
    var pframe = Encoder.init(alloc);
    defer pframe.deinit();
    try pframe.i32v(@intCast(pbody.written().len));
    try pframe.raw(pbody.written());
    var pwbuf: [256]u8 = undefined;
    var pw = prod_peer.writer(io, &pwbuf);
    try pw.interface.writeAll(pframe.written());
    try pw.interface.flush();

    // Clear coordinator cache so any attempt to send EndTxn(abort) would fail.
    c.txn_coordinator_node = -1;
    c.txn_fenced = false;
    c.txn_fence_code = null;
    c.txn_add_attempted = true;
    try c.txn_added_parts.put(alloc, 0, {});
    try std.testing.expect(c.abortTransactionBestEffort());
    try std.testing.expect(c.txn_fenced);
    try std.testing.expectEqual(@as(?protocol.ErrorCode, .invalid_producer_epoch), c.txn_fence_code);
    try std.testing.expect(!c.hasActiveTxn());
}
