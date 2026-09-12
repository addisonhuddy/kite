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
    /// node_id -> connection (broker connections opened lazily)
    conns: std.AutoHashMapUnmanaged(i32, *Conn),
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

    pub fn init(alloc: std.mem.Allocator, cfg: *const config.Config) Client {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .conns = .empty,
            .control = null,
            .brokers = .empty,
            .partitions = .empty,
            .api_ranges = .empty,
            .ca = null,
            .err_ctx = std.mem.zeroes([512]u8),
        };
    }

    pub fn deinit(c: *Client) void {
        var it = c.conns.valueIterator();
        while (it.next()) |conn| {
            conn.*.close();
            c.alloc.destroy(conn.*);
        }
        c.conns.deinit(c.alloc);
        c.brokers.deinit(c.alloc);
        c.partitions.deinit(c.alloc);
        c.api_ranges.deinit(c.alloc);
        if (c.ca) |*b| b.deinit(c.alloc);
    }

    pub fn setErr(c: *Client, comptime fmt: []const u8, args: anytype) void {
        _ = std.fmt.bufPrintZ(&c.err_ctx, fmt, args) catch {};
    }
    pub fn errDetail(c: *const Client) []const u8 {
        return std.mem.sliceTo(&c.err_ctx, 0);
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
    }

    pub fn partitionCount(c: *const Client) usize {
        return c.partitions.items.len;
    }

    // -- Produce -------------------------------------------------------------

    /// Encode `records` into one record batch and produce it to `partition`
    /// of `topic`, retrying retriable errors with backoff + metadata refresh.
    pub fn produceToPartition(
        c: *Client,
        topic: []const u8,
        partition: usize,
        records: []const protocol.Record,
    ) !void {
        var be = Encoder.init(c.alloc);
        defer be.deinit();
        protocol.encodeRecordBatch(&be, records, std.time.milliTimestamp()) catch
            return error.OutOfMemory;
        const batch = be.written();

        var attempt: usize = 0;
        var backoff_ms: u64 = 100;
        while (attempt < max_attempts) : (attempt += 1) {
            const leader = c.partitions.items[partition].leader;
            const conn = c.connForBroker(leader) catch {
                c.sleep(backoff_ms);
                backoff_ms = @min(backoff_ms * 2, 3000);
                _ = c.refreshMetadata(topic) catch {};
                continue;
            };
            const code = c.produceOnce(conn, topic, @intCast(partition), batch) catch {
                c.dropConn(leader);
                c.sleep(backoff_ms);
                backoff_ms = @min(backoff_ms * 2, 3000);
                continue;
            };
            switch (code) {
                .none => return,
                else => {
                    if (code.retriable()) {
                        c.sleep(backoff_ms);
                        backoff_ms = @min(backoff_ms * 2, 3000);
                        _ = c.refreshMetadata(topic) catch {};
                        continue;
                    }
                    c.setErr("produce to {s}[{d}]: {s}", .{ topic, partition, code.name() });
                    return error.ProduceFailed;
                },
            }
        }
        c.setErr("produce to {s}[{d}]: giving up after {d} attempts", .{ topic, partition, max_attempts });
        return error.ProduceFailed;
    }

    fn sleep(_: *Client, ms: u64) void {
        std.Thread.sleep(ms * std.time.ns_per_ms);
    }

    fn connForBroker(c: *Client, node: i32) !*Conn {
        if (c.conns.get(node)) |conn| return conn;
        const addr = c.brokers.get(node) orelse {
            c.setErr("no address for broker node {d}", .{node});
            return error.MetadataFailed;
        };
        const conn = try c.connectOne(addr.host, addr.port);
        try c.conns.put(c.alloc, node, conn);
        return conn;
    }

    fn dropConn(c: *Client, node: i32) void {
        if (c.conns.fetchRemove(node)) |kv| {
            kv.value.close();
            c.alloc.destroy(kv.value);
        }
    }

    /// One ProduceRequest holding a single topic/partition; returns the
    /// partition-level error code.
    fn produceOnce(c: *Client, conn: *Conn, topic: []const u8, pidx: i32, batch: []const u8) !protocol.ErrorCode {
        const Ctx = struct { topic: []const u8, pidx: i32, batch: []const u8 };
        const body = struct {
            fn f(e: *Encoder, x: Ctx) protocol.ProtoError!void {
                try e.compactString(null); // transactional_id
                try e.i16v(-1); // acks=all
                try e.i32v(15000); // timeout_ms
                try e.compactArrayLen(1); // one topic
                try e.compactString(x.topic);
                try e.compactArrayLen(1); // one partition
                try e.i32v(x.pidx);
                try e.compactBytes(x.batch);
                try e.tagBuffer();
                try e.tagBuffer();
                try e.tagBuffer();
            }
        }.f;
        const resp = try c.sendRequest(conn, protocol.api_key.produce, protocol.version.produce, Ctx{ .topic = topic, .pidx = pidx, .batch = batch }, body);
        defer c.alloc.free(resp.frame);

        var d = Decoder.init(resp.body);
        const ntopics = try d.compactArrayLen();
        var t: i64 = 0;
        var result: protocol.ErrorCode = .none;
        while (t < ntopics) : (t += 1) {
            _ = try d.compactString(); // name
            const nparts = try d.compactArrayLen();
            var p: i64 = 0;
            while (p < nparts) : (p += 1) {
                _ = try d.i32v(); // partition index
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
                result = code;
            }
            try d.tagBuffer(); // topic tags
        }
        return result;
    }
};
