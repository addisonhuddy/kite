//! Framed request/response transport over TCP, optionally wrapped in TLS
//! via std.crypto.tls.

const std = @import("std");
const protocol = @import("protocol.zig");

pub const TransportError = error{
    ConnectFailed,
    TlsFailed,
    TlsHandshakeAuthFailed,
    IoFailed,
    ResponseTooLarge,
    CorrelationMismatch,
    MalformedResponse,
    OutOfMemory,
};

const rbuf_len = std.crypto.tls.Client.min_buffer_len + 4096;
// Net send buffer: large enough to hold many TLS records so socket writes
// are amortized — ciphertext only reaches the wire on explicit flush.
const wbuf_len = 512 * 1024;
const tls_plain_len = 32 * 1024;
/// Kafka brokers never need a response larger than this for the requests we
/// send (metadata for one topic, produce acks).
const max_response_len = 16 << 20;

pub const Conn = struct {
    stream: std.Io.net.Stream,
    io: std.Io,
    debug: bool,
    rbuf: [rbuf_len]u8,
    wbuf: [wbuf_len]u8,
    tls_rbuf: [tls_plain_len]u8,
    tls_wbuf: [tls_plain_len]u8,
    net_reader: std.Io.net.Stream.Reader,
    net_writer: std.Io.net.Stream.Writer,
    ca_lock: std.Io.RwLock,
    tls_client: ?std.crypto.tls.Client,
    input: *std.Io.Reader,
    output: *std.Io.Writer,
    host: []const u8,
    port: u16,

    pub fn close(c: *Conn) void {
        c.stream.close(c.io);
    }
};

/// Connect TCP to host:port. host may be DNS or IP literal.
fn tcpConnect(io: std.Io, host: []const u8, port: u16) !std.Io.net.Stream {
    const hn = std.Io.net.HostName.init(host) catch return error.ConnectFailed;
    const s = hn.connect(io, port, .{ .mode = .stream }) catch return error.ConnectFailed;
    // 15s timeouts keep a hung broker from stalling the CLI forever.
    const tv = std.posix.timeval{ .sec = 15, .usec = 0 };
    std.posix.setsockopt(s.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
    std.posix.setsockopt(s.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&tv)) catch {};
    const one: c_int = 1;
    std.posix.setsockopt(s.socket.handle, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&one)) catch {};
    // NB: no SO_SNDBUF — explicit values clamp to wmem_max (~208KB here),
    // smaller than tcp autotuning's ceiling.
    return s;
}

/// Establish a connection: TCP, then TLS if `ca` is set (SNI + verification on).
pub fn connect(
    io: std.Io,
    alloc: std.mem.Allocator,
    env: *std.process.Environ.Map,
    host: []const u8,
    port: u16,
    ca: ?*std.crypto.Certificate.Bundle,
) TransportError!*Conn {
    const stream = tcpConnect(io, host, port) catch return error.ConnectFailed;
    errdefer stream.close(io);

    const c = try alloc.create(Conn);
    errdefer alloc.destroy(c);
    c.* = .{
        .stream = stream,
        .io = io,
        .debug = env.get("KANNON_DEBUG") != null,
        .rbuf = undefined,
        .wbuf = undefined,
        .tls_rbuf = undefined,
        .tls_wbuf = undefined,
        .net_reader = undefined,
        .net_writer = undefined,
        .ca_lock = .init,
        .tls_client = null,
        .input = undefined,
        .output = undefined,
        .host = host,
        .port = port,
    };
    c.net_reader = stream.reader(io, &c.rbuf);
    c.net_writer = stream.writer(io, &c.wbuf);
    c.input = &c.net_reader.interface;
    c.output = &c.net_writer.interface;

    if (ca) |bundle| {
        var entropy: [std.crypto.tls.Client.Options.entropy_len]u8 = undefined;
        io.randomSecure(&entropy) catch io.random(&entropy);
        const client = std.crypto.tls.Client.init(c.input, c.output, .{
            .host = .{ .explicit = host },
            .ca = .{ .bundle = .{ .gpa = alloc, .io = io, .lock = &c.ca_lock, .bundle = bundle } },
            .read_buffer = &c.tls_rbuf,
            .write_buffer = &c.tls_wbuf,
            .entropy = &entropy,
            .realtime_now = .now(io, .real),
        }) catch |err| {
            // errdefers above close the stream and free `c`.
            if (c.debug)
                std.debug.print("tls init: {s}\n", .{@errorName(err)});
            const failed: TransportError = switch (err) {
                error.TlsCertificateNotVerified, error.CertificateHostMismatch, error.CertificateIssuerMismatch, error.CertificateExpired, error.CertificateNotYetValid, error.CertificatePublicKeyInvalid, error.CertificateSignatureInvalid, error.UnsupportedCertificateVersion => error.TlsHandshakeAuthFailed,
                else => error.TlsFailed,
            };
            return failed;
        };
        c.tls_client = client;
        c.input = &c.tls_client.?.reader;
        c.output = &c.tls_client.?.writer;
    }
    return c;
}

/// Send a framed request (4-byte big-endian length + payload).
pub fn send(c: *Conn, payload: []const u8) TransportError!void {
    return sendv(c, &.{payload});
}

/// Send a framed request assembled from parts — the frame length is the sum
/// of part lengths; parts are written back-to-back without copying into a
/// contiguous buffer (avoids duplicating multi-MB record batches).
pub fn sendv(c: *Conn, parts: []const []const u8) TransportError!void {
    var total: usize = 0;
    for (parts) |p| total += p.len;
    if (c.debug) {
        const first = parts[0];
        std.debug.print("send {d}B: {x}\n", .{ total, first[0..@min(first.len, 200)] });
    }
    var hdr: [4]u8 = undefined;
    std.mem.writeInt(u32, &hdr, @intCast(total), .big);
    c.output.writeAll(&hdr) catch return error.IoFailed;
    for (parts) |p| c.output.writeAll(p) catch return error.IoFailed;
    c.output.flush() catch |err| {
        if (c.debug)
            std.debug.print("send flush: {s}\n", .{@errorName(err)});
        return error.IoFailed;
    };
    // The TLS writer's flush encrypts into the net writer's buffer without
    // pushing it out; the underlying flush puts ciphertext on the wire.
    if (c.tls_client != null) c.net_writer.interface.flush() catch return error.IoFailed;
}

pub const Response = struct {
    /// Full allocation backing the frame; free this, not `body`.
    frame: []u8,
    /// Response body after the v1 header (correlation id + tag buffer).
    body: []u8,
};

/// Receive one framed response and validate its correlation id.
/// `header_tags`: response header v1 carries a tag buffer — every flexible
/// response except ApiVersions, which is always header v0.
pub fn recv(c: *Conn, alloc: std.mem.Allocator, expect_corr: i32, header_tags: bool) TransportError!Response {
    const hdr = c.input.takeArray(4) catch |err| {
        if (c.debug) {
            std.debug.print("recv hdr: {s}", .{@errorName(err)});
            if (c.tls_client) |*t| std.debug.print(" read_err={s}", .{@errorName(t.read_err orelse error{NoErr}.NoErr)});
            std.debug.print("\n", .{});
        }
        return error.IoFailed;
    };
    const len = std.mem.readInt(u32, hdr, .big);
    if (len > max_response_len) return error.ResponseTooLarge;
    const frame = alloc.alloc(u8, len) catch return error.OutOfMemory;
    errdefer alloc.free(frame);
    c.input.readSliceAll(frame) catch return error.IoFailed;

    if (c.debug) {
        const n = @min(frame.len, 512);
        std.debug.print("recv {d}B: {x}\n", .{ frame.len, frame[0..n] });
    }
    var d = protocol.Decoder.init(frame);
    const corr = d.i32v() catch return error.MalformedResponse;
    if (corr != expect_corr) return error.CorrelationMismatch;
    if (header_tags) d.tagBuffer() catch return error.MalformedResponse;
    return .{ .frame = frame, .body = frame[d.pos..] };
}
