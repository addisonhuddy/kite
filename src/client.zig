//! Kafka client: bootstrap connect, ApiVersions handshake, SASL auth,
//! Metadata v12 topic resolution, Produce v11 with retries.

const std = @import("std");
const config = @import("config.zig");
const protocol = @import("protocol.zig");
const transport = @import("transport.zig");
const scram = @import("scram.zig");

const Encoder = protocol.Encoder;
const Decoder = protocol.Decoder;
const Conn = transport.Conn;

const BrokerAddr = struct { host: []const u8, port: u16 };

const Partition = struct { index: i32, leader: i32 };

const ApiRange = struct { min: i16, max: i16 };

const max_attempts = 6;

pub const Client = struct {
    alloc: std.mem.Allocator,
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
    /// partition index -> leader node_id for the target topic
    partitions: std.ArrayListUnmanaged(Partition),
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
    /// pidx -> next base sequence number (idempotent produce only).
    seqs: std.AutoHashMapUnmanaged(i32, i32),

    pub fn init(alloc: std.mem.Allocator, cfg: *const config.Config) Client {
        return .{
            .alloc = alloc,
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
        var it = c.conns.valueIterator();
        while (it.next()) |conn| {
            conn.*.close();
            c.alloc.destroy(conn.*);
        }
        c.conns.deinit(c.alloc);
        c.inflight.deinit(c.alloc);
        c.alt.deinit(c.alloc);
        c.brokers.deinit(c.alloc);
        c.partitions.deinit(c.alloc);
        c.api_ranges.deinit(c.alloc);
        for (c.outstanding.items) |*o| {
            for (o.encoders.items) |*e| e.deinit();
            o.encoders.deinit(c.alloc);
            c.alloc.free(o.pidx);
            c.alloc.free(o.batches);
        }
        c.outstanding.deinit(c.alloc);
        c.seqs.deinit(c.alloc);
        if (c.ca) |*b| b.deinit(c.alloc);
    }

    pub fn setErr(c: *Client, comptime fmt: []const u8, args: anytype) void {
        _ = std.fmt.bufPrintZ(&c.err_ctx, fmt, args) catch {};
    }
    pub fn errDetail(c: *const Client) []const u8 {
        return std.mem.sliceTo(&c.err_ctx, 0);
    }

    /// Verbose diagnostic to stderr, gated on `cfg.verbose` (-v).
    fn vlog(c: *Client, comptime fmt: []const u8, args: anytype) void {
        if (!c.cfg.verbose) return;
        var buf: [512]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, "kannon: " ++ fmt ++ "\n", args) catch return;
        _ = std.posix.write(std.posix.STDERR_FILENO, s) catch {};
    }

    fn loadCa(c: *Client) !*const std.crypto.Certificate.Bundle {
        if (c.ca) |*b| return b;
        var bundle: std.crypto.Certificate.Bundle = .{};
        if (c.cfg.ssl_truststore_location) |path| {
            const f = std.fs.cwd().openFile(path, .{}) catch {
                c.setErr("cannot open ssl.truststore.location '{s}'", .{path});
                return error.TlsCaLoadFailed;
            };
            defer f.close();
            bundle.addCertsFromFile(c.alloc, f) catch {
                c.setErr("failed parsing CA bundle '{s}'", .{path});
                return error.TlsCaLoadFailed;
            };
        } else {
            bundle.rescan(c.alloc) catch {
                c.setErr("failed loading system CA bundle", .{});
                return error.TlsCaLoadFailed;
            };
        }
        c.ca = bundle;
        return &c.ca.?;
    }

    fn connectOne(c: *Client, host: []const u8, port: u16) !*Conn {
        const ca: ?std.crypto.Certificate.Bundle = if (c.cfg.needsTls())
            (try c.loadCa()).*
        else
            null;
        const conn = transport.connect(c.alloc, host, port, ca) catch |err| {
            switch (err) {
                error.TlsHandshakeAuthFailed => c.setErr(
                    "TLS certificate verification failed for {s}:{d} (check ssl.truststore.location)",
                    .{ host, port },
                ),
                error.TlsFailed => c.setErr("TLS handshake failed for {s}:{d}", .{ host, port }),
                else => c.setErr("connect {s}:{d}: {s}", .{ host, port, @errorName(err) }),
            }
            return error.AllBootstrapFailed;
        };
        c.postConnect(conn) catch |err| {
            conn.close();
            c.alloc.destroy(conn);
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
        return conn;
    }

    /// Establish the control connection: try each bootstrap server until one
    /// fully connects (TCP + TLS + SASL + ApiVersions).
    pub fn bootstrap(c: *Client) !void {
        var first_err_ctx: [512]u8 = undefined;
        var have_err = false;
        for (c.cfg.bootstrap_servers) |server| {
            const colon = std.mem.lastIndexOfScalar(u8, server, ':');
            const host = if (colon) |i| server[0..i] else server;
            const port: u16 = if (colon) |i|
                std.fmt.parseInt(u16, server[i + 1 ..], 10) catch 9092
            else
                9092;
            const conn = c.connectOne(host, port) catch {
                if (!have_err) {
                    @memcpy(&first_err_ctx, &c.err_ctx);
                    have_err = true;
                }
                continue;
            };
            c.control = conn;
            c.vlog("bootstrap connected {s}:{d}", .{ host, port });
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
        var e = Encoder.init(c.alloc);
        defer e.deinit();
        // ApiVersions and SaslHandshake are the non-/semi-flexible oddballs:
        // ApiVersions answers header v0; SaslHandshake v1 is not flexible at
        // all (header v1 request, header v0 response, legacy string types).
        const flexible = key != protocol.api_key.sasl_handshake;
        const resp_tags = key != protocol.api_key.api_versions and key != protocol.api_key.sasl_handshake;
        try protocol.encodeRequest(&e, key, ver, flexible, "kannon", ctx, body_fn);
        try transport.send(conn, e.written());
        return try transport.recv(conn, c.alloc, protocol.lastCorrelationId(), resp_tags);
    }

    // -- ApiVersions ---------------------------------------------------------

    fn apiVersions(c: *Client, conn: *Conn) !void {
        const Ctx = struct {};
        const body = struct {
            fn f(e: *Encoder, _: Ctx) protocol.ProtoError!void {
                try e.compactString("kannon");
                try e.compactString("0.1.0");
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
        const first = try S.clientFirst(c.alloc, c.cfg.sasl_username.?);
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
        const conn = c.control orelse return error.MetadataFailed;

        const body = struct {
            fn f(e: *Encoder, t: []const u8) protocol.ProtoError!void {
                try e.compactArrayLen(1);
                try e.raw(&[_]u8{0} ** 16); // topic_id: zero uuid
                try e.compactString(t);
                try e.tagBuffer();
                try e.boolean(false); // allow_auto_topic_creation
                try e.boolean(false); // include_cluster_authorized_operations
                try e.boolean(false); // include_topic_authorized_operations
                try e.tagBuffer();
            }
        }.f;
        const resp = c.sendRequest(conn, protocol.api_key.metadata, protocol.version.metadata, topic, body) catch {
            c.setErr("metadata request failed", .{});
            return error.MetadataFailed;
        };
        defer c.alloc.free(resp.frame);

        var d = Decoder.init(resp.body);
        _ = try d.i32v(); // throttle
        const nbrokers = try d.compactArrayLen();
        c.brokers.clearRetainingCapacity();
        var i: i64 = 0;
        while (i < nbrokers) : (i += 1) {
            const node = try d.i32v();
            const host = (try d.compactString()) orelse "";
            const port = try d.i32v();
            _ = try d.compactString(); // rack
            try d.tagBuffer();
            try c.brokers.put(c.alloc, node, .{ .host = try c.alloc.dupe(u8, host), .port = @intCast(@max(0, port)) });
        }
        _ = try d.compactString(); // cluster id
        _ = try d.i32v(); // controller id

        const ntopics = try d.compactArrayLen();
        var t: i64 = 0;
        while (t < ntopics) : (t += 1) {
            const terr: protocol.ErrorCode = @enumFromInt(try d.i16v());
            _ = try d.compactString(); // name (we requested exactly one topic)
            try d.skip(16); // topic_id uuid
            _ = try d.boolean(); // is_internal
            const nparts = try d.compactArrayLen();
            var p: i64 = 0;
            c.partitions.clearRetainingCapacity();
            while (p < nparts) : (p += 1) {
                _ = try d.i16v(); // partition error
                const pidx = try d.i32v();
                const leader = try d.i32v();
                _ = try d.i32v(); // leader epoch
                try d.skipCompactI32Array(); // replicas
                try d.skipCompactI32Array(); // isr
                try d.skipCompactI32Array(); // offline replicas
                try d.tagBuffer();
                try c.partitions.append(c.alloc, .{ .index = pidx, .leader = leader });
            }
            _ = try d.i32v(); // topic_authorized_operations
            try d.tagBuffer();

            if (terr != .none) {
                c.setErr("metadata for topic '{s}': {s}", .{ topic, terr.name() });
                return switch (terr) {
                    .unknown_topic_or_partition => error.TopicNotFound,
                    .topic_authorization_failed => error.TopicAuthorizationFailed,
                    else => error.MetadataFailed,
                };
            }
        }
        if (c.partitions.items.len == 0) {
            c.setErr("topic '{s}' has no partitions", .{topic});
            return error.MetadataFailed;
        }
        c.vlog("metadata: topic '{s}' has {d} partition(s)", .{ topic, c.partitions.items.len });
    }

    pub fn partitionCount(c: *const Client) usize {
        return c.partitions.items.len;
    }

    // -- Idempotent produce: InitProducerId ----------------------------------

    /// Ask the broker for a producer id + epoch (idempotent produce). Sends
    /// InitProducerId v4 on the control connection; retries transient errors.
    fn initProducerId(c: *Client) !void {
        const conn = c.control orelse return error.MetadataFailed;
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
        var attempt: usize = 0;
        var backoff_ms: u64 = 100;
        while (attempt < 3) : (attempt += 1) {
            const resp = c.sendRequest(conn, protocol.api_key.init_producer_id, protocol.version.init_producer_id, Ctx{}, body) catch {
                c.setErr("InitProducerId request failed", .{});
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
            if (!code.retriable() or attempt == 2) {
                c.setErr("InitProducerId: {s}", .{code.name()});
                return error.ProduceFailed;
            }
            c.sleep(backoff_ms);
            backoff_ms = @min(backoff_ms * 2, 2000);
        }
    }

    /// Ensure a producer id exists when idempotence is enabled.
    fn ensureProducerId(c: *Client) !void {
        if (c.cfg.enable_idempotence and !c.producer_inited)
            try c.initProducerId();
    }

    // -- Produce -------------------------------------------------------------

    /// A sent ProduceRequest awaiting its response. Batches stay alive in the
    /// encoders so retriable partitions can be resent verbatim.
    const Outstanding = struct {
        node: i32,
        ckey: u64,
        conn: *Conn,
        corr: i32, // <0 = the send never completed; all parts retriable
        pidx: []i32,
        batches: [][]const u8,
        bytes: usize,
        encoders: std.ArrayListUnmanaged(Encoder) = .empty,
    };

    const Inflight = struct { node: i32, key: u64, n: usize };
    const conns_per_partition = 2;

    fn connKey(node: i32, slot: u32) u64 {
        return (@as(u64, @intCast(node)) << 32) | slot;
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
        for (parts, 0..) |pi, i| {
            const pidx: i32 = @intCast(pi);
            var o: Outstanding = .{
                .node = -1,
                .ckey = 0,
                .conn = undefined,
                .corr = -1,
                .pidx = try c.alloc.alloc(i32, 1),
                .batches = try c.alloc.alloc([]const u8, 1),
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
                std.time.milliTimestamp(),
                c.producer_id,
                c.producer_epoch,
                base_seq,
            ) catch {
                be.deinit();
                return error.OutOfMemory;
            };
            if (base_seq >= 0)
                try c.seqs.put(c.alloc, pidx, base_seq + @as(i32, @intCast(sets[i].len)));
            try o.encoders.append(c.alloc, be);
            o.pidx[0] = pidx;
            o.batches[0] = be.written();
            o.bytes = be.written().len;
            c.outstanding_bytes += o.bytes;
            const claim = c.claimConn(pidx) catch {
                try c.outstanding.append(c.alloc, o);
                continue;
            };
            o.node = claim.node;
            o.ckey = claim.key;
            if (c.connFor(claim.node, claim.key)) |conn| {
                o.conn = conn;
                o.corr = c.produceSend(conn, topic, o.pidx, o.batches) catch {
                    c.dropConn(claim.key);
                    try c.outstanding.append(c.alloc, o);
                    continue;
                };
            } else |_| {}
            try c.outstanding.append(c.alloc, o);
            // Idempotent produce allows at most 5 un-acked requests per
            // partition (broker dedup window); wait for a slot before the
            // next send on this partition.
            if (c.producer_inited) {
                if (c.inflight.get(pidx)) |inf| {
                    if (inf.n > max_inflight)
                        c.vlog("partition {d}: {d} in flight — draining for a slot", .{ pidx, inf.n });
                }
                try c.produceDrainStop(topic, .{ .partition_inflight = .{ .pidx = pidx, .max = max_inflight } });
            }
        }
    }

    /// Drain every outstanding ProduceResponse in send order, then retry
    /// retriable partitions (backoff + metadata refresh) up to max_attempts.
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
        const PendingPart = struct { pidx: i32, batch: []const u8 };
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

        while (done < c.outstanding.items.len and c.drainStopActive(stop)) {
            const o = &c.outstanding.items[done];
            done += 1;
            c.outstanding_bytes -= o.bytes;
            c.releaseConn(o.pidx[0]);
            if (o.corr >= 0) {
                var codes = std.AutoHashMapUnmanaged(i32, protocol.ErrorCode).empty;
                defer codes.deinit(c.alloc);
                if (c.produceRecv(o.conn, o.corr, &codes)) |_| {
                    for (o.pidx, o.batches) |pi, b| {
                        const code = codes.get(pi) orelse .none;
                        switch (code) {
                            // duplicate_sequence_number: broker already
                            // appended this batch — dedup success.
                            .none, .duplicate_sequence_number => {},
                            else => if (code.retriable()) {
                                try retry.append(c.alloc, .{ .pidx = pi, .batch = b });
                            } else {
                                c.setErr("produce to {s}[{d}]: {s}", .{ topic, pi, code.name() });
                                return error.ProduceFailed;
                            },
                        }
                    }
                } else |_| {
                    c.dropConn(o.ckey);
                    for (o.pidx, o.batches) |pi, b|
                        try retry.append(c.alloc, .{ .pidx = pi, .batch = b });
                }
            } else {
                for (o.pidx, o.batches) |pi, b|
                    try retry.append(c.alloc, .{ .pidx = pi, .batch = b });
            }
        }

        var attempt: usize = 0;
        var backoff_ms: u64 = 100;
        while (retry.items.len > 0 and attempt < max_attempts) : (attempt += 1) {
            c.vlog("retrying {d} partition(s) — attempt {d}/{d}, backoff {d}ms", .{ retry.items.len, attempt + 1, max_attempts, backoff_ms });
            c.sleep(backoff_ms);
            backoff_ms = @min(backoff_ms * 2, 3000);
            _ = c.refreshMetadata(topic) catch {};

            const items = try retry.toOwnedSlice(c.alloc);
            defer c.alloc.free(items);
            for (items) |pp| {
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
                    c.dropConn(claim.key);
                    c.releaseConn(pp.pidx);
                    try retry.append(c.alloc, pp);
                    continue;
                };
                var codes = std.AutoHashMapUnmanaged(i32, protocol.ErrorCode).empty;
                defer codes.deinit(c.alloc);
                c.produceRecv(conn, corr, &codes) catch {
                    c.dropConn(claim.key);
                    c.releaseConn(pp.pidx);
                    try retry.append(c.alloc, pp);
                    continue;
                };
                const code = codes.get(pp.pidx) orelse .none;
                switch (code) {
                    .none, .duplicate_sequence_number => c.releaseConn(pp.pidx),
                    else => if (code.retriable()) {
                        // Keep the claim: the retry stays bound to this conn.
                        try retry.append(c.alloc, pp);
                    } else {
                        c.setErr("produce to {s}[{d}]: {s}", .{ topic, pp.pidx, code.name() });
                        return error.ProduceFailed;
                    },
                }
            }
        }
        if (retry.items.len > 0) {
            c.setErr("produce to {s}: giving up after {d} attempts", .{ topic, max_attempts });
            return error.ProduceFailed;
        }
    }

    fn partitionLeader(c: *Client, pidx: i32) ?i32 {
        for (c.partitions.items) |p|
            if (p.index == pidx) return p.leader;
        return null;
    }

    fn sleep(_: *Client, ms: u64) void {
        std.Thread.sleep(ms * std.time.ns_per_ms);
    }

    fn connFor(c: *Client, node: i32, key: u64) !*Conn {
        if (c.conns.get(key)) |conn| return conn;
        const addr = c.brokers.get(node) orelse {
            c.setErr("no address for broker node {d}", .{node});
            return error.MetadataFailed;
        };
        const conn = try c.connectOne(addr.host, addr.port);
        c.vlog("connected broker {d} at {s}:{d}", .{ node, addr.host, addr.port });
        try c.conns.put(c.alloc, key, conn);
        return conn;
    }

    fn dropConn(c: *Client, key: u64) void {
        if (c.conns.fetchRemove(key)) |kv| {
            c.vlog("dropping connection to broker {d}", .{key >> 32});
            kv.value.close();
            c.alloc.destroy(kv.value);
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
        try protocol.encodeRequestHeader(&head, protocol.api_key.produce, protocol.version.produce, true, "kannon");
        try head.compactString(null); // transactional_id
        try head.i16v(-1); // acks=all
        try head.i32v(15000); // timeout_ms
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

    /// Read the ProduceResponse for correlation id `corr`; fills `codes`
    /// with partition_index -> error code.
    fn produceRecv(
        c: *Client,
        conn: *Conn,
        corr: i32,
        codes: *std.AutoHashMapUnmanaged(i32, protocol.ErrorCode),
    ) !void {
        const resp = try transport.recv(conn, c.alloc, corr, true);
        defer c.alloc.free(resp.frame);

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
                _ = try d.i64v(); // base_offset
                _ = try d.i64v(); // log_append_time_ms
                _ = try d.i64v(); // log_start_offset
                const nerrs = try d.compactArrayLen(); // record_errors
                var r: i64 = 0;
                while (r < nerrs) : (r += 1) {
                    _ = try d.i32v();
                    _ = try d.compactString();
                    try d.tagBuffer();
                }
                _ = try d.compactString(); // error_message
                try d.tagBuffer(); // partition tags
                try codes.put(c.alloc, pidx, code);
            }
            try d.tagBuffer(); // topic tags
        }
        const throttle = try d.i32v();
        if (throttle > 0) std.debug.print("kannon: broker throttled produce {d}ms\n", .{throttle});
        try d.tagBuffer();
    }
};
