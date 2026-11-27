//! Framed request/response transport over TCP, optionally wrapped in TLS
//! via std.crypto.tls.

const std = @import("std");
const protocol = @import("protocol.zig");

pub const TransportError = error{
    ConnectFailed,
    ConnectionRefused,
    ConnectTimedOut,
    HostNotFound,
    TlsFailed,
    TlsHandshakeAuthFailed,
    IoFailed,
    IoTimedOut,
    WaitHookAborted,
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
/// Maximum Kafka response frame size (128 MiB): large enough for oversized
/// Fetch responses while bounding allocation against corrupt length prefixes.
pub const max_response_len: usize = 128 << 20;

pub const WaitHook = struct {
    ctx: *anyopaque,
    io: std.Io,
    next_due: *const fn (*anyopaque) ?std.Io.Timestamp,
    tick: *const fn (*anyopaque) bool,
};

const SocketReader = struct {
    interface: std.Io.Reader,
    fd: std.posix.socket_t,
    err: ?TransportError = null,
    cancellation: ?struct {
        stop: ?*const std.atomic.Value(bool),
        deadline: std.Io.Timestamp,
        io: std.Io,
    } = null,
    wait_hook: ?WaitHook = null,
    poll_nonblock: bool = false,

    fn readVec(r: *std.Io.Reader, data: [][]u8) std.Io.Reader.Error!usize {
        const self: *SocketReader = @alignCast(@fieldParentPtr("interface", r));
        var buffers: [16][]u8 = undefined;
        const nbufs, const data_size = try r.writableVector(&buffers, data);
        std.debug.assert(nbufs != 0 and buffers[0].len != 0);
        while (true) {
            if (self.poll_nonblock) {
                var fds = [_]std.posix.pollfd{.{ .fd = self.fd, .events = std.posix.POLL.IN, .revents = 0 }};
                const rc = std.posix.poll(&fds, 0) catch {
                    self.err = error.IoFailed;
                    return error.ReadFailed;
                };
                if (rc == 0) {
                    self.err = error.IoTimedOut;
                    return error.ReadFailed;
                }
            }
            var poll_limit: ?u64 = null;
            var hook_due = false;
            var ready = false;
            if (self.cancellation) |cancel| {
                if (cancel.stop) |flag| if (flag.load(.acquire)) return error.ReadFailed;
                const remaining = std.Io.Timestamp.now(cancel.io, .awake).durationTo(cancel.deadline).toMilliseconds();
                if (remaining <= 0) {
                    if (self.wait_hook != null) {
                        var fds = [_]std.posix.pollfd{.{ .fd = self.fd, .events = std.posix.POLL.IN, .revents = 0 }};
                        if ((std.posix.poll(&fds, 0) catch 0) > 0) ready = true;
                    }
                    if (!ready) {
                        self.err = error.IoTimedOut;
                        return error.ReadFailed;
                    }
                } else {
                    poll_limit = @intCast(if (cancel.stop != null) @min(remaining, 50) else remaining);
                }
            }
            if (!ready) {
                if (self.wait_hook) |hook| {
                    if (hook.next_due(hook.ctx)) |due| {
                        const until = std.Io.Timestamp.now(hook.io, .awake).durationTo(due).toMilliseconds();
                        if (until <= 0) {
                            hook_due = true;
                            poll_limit = 0;
                        } else {
                            const cap: u64 = @intCast(until);
                            poll_limit = if (poll_limit) |p| @min(p, cap) else cap;
                        }
                    }
                }
                if (poll_limit) |ms| {
                    var fds = [_]std.posix.pollfd{.{ .fd = self.fd, .events = std.posix.POLL.IN, .revents = 0 }};
                    if ((std.posix.poll(&fds, @intCast(@min(ms, std.math.maxInt(i32)))) catch return error.ReadFailed) == 0) {
                        if (hook_due and self.wait_hook.?.tick(self.wait_hook.?.ctx)) {
                            self.err = error.WaitHookAborted;
                            return error.ReadFailed;
                        }
                        continue;
                    }
                }
            }
            const n = std.posix.system.read(self.fd, buffers[0].ptr, buffers[0].len);
            switch (std.posix.errno(n)) {
                .SUCCESS => {
                    if (n == 0) return error.EndOfStream;
                    const size: usize = @intCast(n);
                    if (size > data_size) r.end += size - data_size;
                    return @min(size, data_size);
                },
                .INTR => continue,
                .AGAIN, .TIMEDOUT => self.err = error.IoTimedOut,
                else => self.err = error.IoFailed,
            }
            return error.ReadFailed;
        }
    }

    fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        var data = [_][]u8{limit.slice(try w.writableSliceGreedy(1))};
        const n = try readVec(r, &data);
        w.advance(n);
        return n;
    }
};

const SocketWriter = struct {
    interface: std.Io.Writer,
    fd: std.posix.socket_t,
    err: ?TransportError = null,

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *SocketWriter = @alignCast(@fieldParentPtr("interface", w));
        const bytes = bytes: {
            if (w.end != 0) break :bytes w.buffered();
            for (data[0 .. data.len - 1]) |part| if (part.len != 0) break :bytes part;
            break :bytes if (splat != 0) data[data.len - 1] else &.{};
        };
        if (bytes.len == 0) return 0;
        var iov = [_]std.posix.iovec_const{.{ .base = bytes.ptr, .len = bytes.len }};
        const msg: std.posix.msghdr_const = .{ .name = null, .namelen = 0, .iov = &iov, .iovlen = 1, .control = null, .controllen = 0, .flags = 0 };
        while (true) {
            const n = std.posix.system.sendmsg(self.fd, &msg, std.posix.MSG.NOSIGNAL);
            switch (std.posix.errno(n)) {
                .SUCCESS => {
                    if (n == 0) {
                        self.err = error.IoFailed;
                        return error.WriteFailed;
                    }
                    return w.consume(@intCast(n));
                },
                .INTR => continue,
                .AGAIN, .TIMEDOUT => self.err = error.IoTimedOut,
                else => self.err = error.IoFailed,
            }
            return error.WriteFailed;
        }
    }
};

pub const Conn = struct {
    stream: std.Io.net.Stream,
    io: std.Io,
    debug: bool,
    rbuf: [rbuf_len]u8,
    wbuf: [wbuf_len]u8,
    tls_rbuf: [tls_plain_len]u8,
    tls_wbuf: [tls_plain_len]u8,
    net_reader: SocketReader,
    net_writer: SocketWriter,
    timeout_ms: u64 = 15000,
    closed: bool = false,
    ca_lock: std.Io.RwLock,
    tls_client: ?std.crypto.tls.Client,
    /// Set when the last send carried a SASL token; the matching response
    /// dump is redacted too.
    redact_next_recv: bool,
    input: *std.Io.Reader,
    output: *std.Io.Writer,
    host: []const u8,
    port: u16,

    pub fn close(c: *Conn) void {
        if (!c.closed) c.stream.close(c.io);
        c.closed = true;
    }

    pub fn setReadCancellation(c: *Conn, stop: ?*const std.atomic.Value(bool), deadline: ?std.Io.Timestamp) void {
        c.net_reader.cancellation = if (deadline) |dl| .{
            .stop = stop,
            .deadline = dl,
            .io = c.io,
        } else null;
    }

    pub fn setReadDeadline(c: *Conn, deadline: ?std.Io.Timestamp) void {
        c.setReadCancellation(null, deadline);
    }

    fn ioError(c: *Conn) TransportError {
        if (c.net_reader.err) |err| if (err == error.IoTimedOut or err == error.WaitHookAborted) return err;
        if (c.net_writer.err) |err| if (err == error.IoTimedOut) return err;
        return error.IoFailed;
    }
};

/// Connect TCP to host:port. host may be DNS or IP literal.
fn tcpConnect(io: std.Io, host: []const u8, port: u16, timeout_ms: ?u64, wait_hook: ?WaitHook) !std.Io.net.Stream {
    const hn = std.Io.net.HostName.init(host) catch return error.HostNotFound;
    const s = if (timeout_ms) |ms| try connectBounded(io, hn, port, ms, wait_hook) else if (wait_hook != null) try connectBounded(io, hn, port, 15000, wait_hook) else hn.connect(io, port, .{ .mode = .stream }) catch |err| return switch (err) {
        error.ConnectionRefused => error.ConnectionRefused,
        error.Timeout, error.HostUnreachable, error.NetworkUnreachable => error.ConnectTimedOut,
        error.UnknownHostName, error.NameServerFailure, error.NoAddressReturned => error.HostNotFound,
        else => error.ConnectFailed,
    };
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

fn connectBounded(io: std.Io, host: std.Io.net.HostName, port: u16, timeout_ms: u64, wait_hook: ?WaitHook) !std.Io.net.Stream {
    const deadline = std.Io.Timestamp.now(io, .awake).addDuration(.fromMilliseconds(@intCast(timeout_ms)));
    var buffer: [32]std.Io.net.HostName.LookupResult = undefined;
    var addresses: std.Io.Queue(std.Io.net.HostName.LookupResult) = .init(&buffer);
    host.lookup(io, &addresses, .{ .port = port }) catch return error.HostNotFound;
    var last_error: error{ HostNotFound, ConnectTimedOut, ConnectFailed, ConnectionRefused, WaitHookAborted } = error.HostNotFound;
    while (addresses.getOne(io)) |result| switch (result) {
        .canonical_name => {},
        .address => |address| {
            const remaining = std.Io.Timestamp.now(io, .awake).durationTo(deadline).toMilliseconds();
            if (remaining <= 0) return error.ConnectTimedOut;
            return connectAddress(io, address, @intCast(remaining), wait_hook) catch |err| {
                if (err == error.WaitHookAborted) return err;
                last_error = err;
                continue;
            };
        },
    } else |_| {}
    return last_error;
}

fn connectAddress(io: std.Io, address: std.Io.net.IpAddress, timeout_ms: u64, wait_hook: ?WaitHook) !std.Io.net.Stream {
    const p = std.posix;
    const raw_fd = p.system.socket(std.Io.Threaded.posixAddressFamily(&address), p.SOCK.STREAM, p.IPPROTO.TCP);
    if (p.errno(raw_fd) != .SUCCESS) return error.ConnectFailed;
    const fd: p.fd_t = @intCast(raw_fd);
    const stream: std.Io.net.Stream = .{ .socket = .{ .handle = fd, .address = address } };
    errdefer stream.close(io);
    const flags: u32 = @bitCast(p.O{ .NONBLOCK = true });
    if (p.errno(p.system.fcntl(fd, p.F.SETFL, flags)) != .SUCCESS) return error.ConnectFailed;
    if (p.errno(p.system.fcntl(fd, p.F.SETFD, @as(u32, p.FD_CLOEXEC))) != .SUCCESS) return error.ConnectFailed;
    var storage: std.Io.Threaded.PosixAddress = undefined;
    const len = std.Io.Threaded.addressToPosix(&address, &storage);
    const code = p.errno(p.system.connect(fd, &storage.any, len));
    switch (code) {
        .SUCCESS => {},
        .CONNREFUSED => return error.ConnectionRefused,
        .INPROGRESS, .AGAIN => {
            const deadline = std.Io.Timestamp.now(io, .awake).addDuration(.fromMilliseconds(@intCast(timeout_ms)));
            while (true) {
                const remaining = std.Io.Timestamp.now(io, .awake).durationTo(deadline).toMilliseconds();
                if (remaining <= 0) return error.ConnectTimedOut;
                var poll_limit: u64 = @intCast(remaining);
                var hook_due = false;
                if (wait_hook) |hook| {
                    if (hook.next_due(hook.ctx)) |due| {
                        const until = std.Io.Timestamp.now(hook.io, .awake).durationTo(due).toMilliseconds();
                        if (until <= 0) {
                            hook_due = true;
                            poll_limit = 0;
                        } else {
                            poll_limit = @min(poll_limit, @as(u64, @intCast(until)));
                        }
                    }
                }
                var pollfds = [_]p.pollfd{.{ .fd = fd, .events = p.POLL.OUT, .revents = 0 }};
                const rc = p.poll(&pollfds, @intCast(@min(poll_limit, std.math.maxInt(i32)))) catch return error.ConnectFailed;
                if (rc > 0) break;
                if (hook_due) {
                    if (wait_hook.?.tick(wait_hook.?.ctx)) return error.WaitHookAborted;
                    continue;
                }
                if (std.Io.Timestamp.now(io, .awake).durationTo(deadline).toMilliseconds() <= 0) return error.ConnectTimedOut;
            }
            var socket_error: u32 = 0;
            var size: p.socklen_t = @sizeOf(u32);
            if (p.errno(p.system.getsockopt(fd, p.SOL.SOCKET, p.SO.ERROR, std.mem.asBytes(&socket_error), &size)) != .SUCCESS) return error.ConnectFailed;
            if (socket_error == @intFromEnum(p.E.CONNREFUSED)) return error.ConnectionRefused;
            if (socket_error != 0) return error.ConnectFailed;
        },
        else => return error.ConnectFailed,
    }
    if (p.errno(p.system.fcntl(fd, p.F.SETFL, @as(u32, 0))) != .SUCCESS) return error.ConnectFailed;
    return stream;
}

test "socket reads time out normally and discard partial frames" {
    const io = std.testing.io;
    const alloc = std.testing.allocator;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(io, .{});
    defer server.deinit(io);
    const frame = [_]u8{ 0, 0, 0, 6, 0, 0, 0, 1, 0, 'z' };
    for ([_]usize{ 0, 2, 9, frame.len }) |len| {
        const c = try connect(io, alloc, &env, "127.0.0.1", server.socket.address.getPort(), null, 10);
        defer alloc.destroy(c);
        defer c.close();
        const peer = try server.accept(io);
        defer peer.close(io);
        var buf: [32]u8 = undefined;
        var writer = peer.writer(io, &buf);
        try writer.interface.writeAll(frame[0..len]);
        try writer.interface.flush();
        if (len == frame.len) {
            const response = try recv(c, alloc, 1, true);
            defer alloc.free(response.frame);
            try std.testing.expectEqualStrings("z", response.body);
            try std.testing.expect(!c.closed);
        } else {
            try std.testing.expectError(error.IoTimedOut, recv(c, alloc, 1, true));
            try std.testing.expect(c.closed);
            try std.testing.expectError(error.IoFailed, send(c, "not reused"));
            try std.testing.expectError(error.IoFailed, recv(c, alloc, 1, true));
        }
    }
}

test "FrameAccumulator handles partial header and body without blocking" {
    const io = std.testing.io;
    const alloc = std.testing.allocator;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(io, .{});
    defer server.deinit(io);
    const c = try connect(io, alloc, &env, "127.0.0.1", server.socket.address.getPort(), null, 50);
    defer alloc.destroy(c);
    defer c.close();
    const peer = try server.accept(io);
    defer peer.close(io);
    const frame = [_]u8{ 0, 0, 0, 6, 0, 0, 0, 7, 0, 'k' };
    var buf: [32]u8 = undefined;
    var writer = peer.writer(io, &buf);
    var acc: FrameAccumulator = .{};
    defer acc.deinit(alloc);

    try std.testing.expect((try acc.step(c, alloc, 7, true)) == null);
    try writer.interface.writeAll(frame[0..2]);
    try writer.interface.flush();
    try std.testing.expect(waitReadable(c, 50));
    try std.testing.expect((try acc.step(c, alloc, 7, true)) == null);
    try std.testing.expectEqual(@as(usize, 2), acc.hdr_len);

    try writer.interface.writeAll(frame[2..7]);
    try writer.interface.flush();
    try std.testing.expect(waitReadable(c, 50));
    try std.testing.expect((try acc.step(c, alloc, 7, true)) == null);
    try std.testing.expectEqual(@as(usize, 3), acc.body_len);

    try writer.interface.writeAll(frame[7..]);
    try writer.interface.flush();
    try std.testing.expect(waitReadable(c, 50));
    const resp = (try acc.step(c, alloc, 7, true)).?;
    defer alloc.free(resp.frame);
    try std.testing.expectEqualStrings("k", resp.body);
    try std.testing.expect(!c.closed);
}

test "socket write timeouts are normal errors and close the connection" {
    const io = std.testing.io;
    const alloc = std.testing.allocator;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(io, .{});
    defer server.deinit(io);
    const c = try connect(io, alloc, &env, "127.0.0.1", server.socket.address.getPort(), null, 10);
    defer alloc.destroy(c);
    defer c.close();
    const peer = try server.accept(io);
    defer peer.close(io);
    const small: c_int = 4096;
    try std.posix.setsockopt(c.stream.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.SNDBUF, std.mem.asBytes(&small));
    const payload = try alloc.alloc(u8, 1024 * 1024);
    defer alloc.free(payload);
    @memset(payload, 0);
    try std.testing.expectError(error.IoTimedOut, send(c, payload));
    try std.testing.expect(c.closed);
}

test "bounded group connections succeed and report refused sockets" {
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(std.testing.io, .{});
    const bound = server.socket.address;
    const stream = try connectAddress(std.testing.io, bound, 100, null);
    stream.close(std.testing.io);
    server.deinit(std.testing.io);
    try std.testing.expectError(error.ConnectionRefused, connectAddress(std.testing.io, bound, 100, null));
}

test "connectWithHook aborts stalled TLS handshake when wait hook fires" {
    const io = std.testing.io;
    const alloc = std.testing.allocator;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(io, .{});
    defer server.deinit(io);
    var ca: std.crypto.Certificate.Bundle = .empty;
    defer ca.deinit(alloc);
    const HookState = struct {
        due: std.Io.Timestamp,
        fired: bool = false,
        fn nextDue(ctx: *anyopaque) ?std.Io.Timestamp {
            const s: *@This() = @ptrCast(@alignCast(ctx));
            return s.due;
        }
        fn tick(ctx: *anyopaque) bool {
            const s: *@This() = @ptrCast(@alignCast(ctx));
            s.fired = true;
            return true;
        }
    };
    const started = std.Io.Timestamp.now(io, .awake);
    var state: HookState = .{ .due = started.addDuration(.fromMilliseconds(15)) };
    const hook: WaitHook = .{ .io = io, .ctx = &state, .next_due = HookState.nextDue, .tick = HookState.tick };
    try std.testing.expectError(error.WaitHookAborted, connectWithHook(io, alloc, &env, "127.0.0.1", server.socket.address.getPort(), &ca, 1000, hook));
    try std.testing.expect(state.fired);
    try std.testing.expect(started.durationTo(std.Io.Timestamp.now(io, .awake)).toMilliseconds() < 200);
}

test "connectWithHook honors timeout when TLS handshake and wait hook both stall" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var env: std.process.Environ.Map = .init(alloc);
    defer env.deinit();
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(io, .{});
    defer server.deinit(io);
    var ca: std.crypto.Certificate.Bundle = .empty;
    defer ca.deinit(alloc);
    const HookState = struct {
        io: std.Io,
        fn nextDue(ctx: *anyopaque) ?std.Io.Timestamp {
            const s: *@This() = @ptrCast(@alignCast(ctx));
            return std.Io.Timestamp.now(s.io, .awake).addDuration(.fromMilliseconds(5));
        }
        fn tick(_: *anyopaque) bool {
            return false;
        }
    };
    var state: HookState = .{ .io = io };
    const hook: WaitHook = .{ .io = io, .ctx = &state, .next_due = HookState.nextDue, .tick = HookState.tick };
    const started = std.Io.Timestamp.now(io, .awake);
    try std.testing.expectError(error.ConnectTimedOut, connectWithHook(io, alloc, &env, "127.0.0.1", server.socket.address.getPort(), &ca, 50, hook));
    try std.testing.expect(started.durationTo(std.Io.Timestamp.now(io, .awake)).toMilliseconds() < 300);
}

/// Establish a connection: TCP, then TLS if `ca` is set (SNI + verification on).
pub fn connect(
    io: std.Io,
    alloc: std.mem.Allocator,
    env: *std.process.Environ.Map,
    host: []const u8,
    port: u16,
    ca: ?*std.crypto.Certificate.Bundle,
    timeout_ms: ?u64,
) TransportError!*Conn {
    return connectWithHook(io, alloc, env, host, port, ca, timeout_ms, null);
}

/// Establish a connection with an optional wait hook that can abort slow TCP
/// connect or TLS handshake waits.
pub fn connectWithHook(
    io: std.Io,
    alloc: std.mem.Allocator,
    env: *std.process.Environ.Map,
    host: []const u8,
    port: u16,
    ca: ?*std.crypto.Certificate.Bundle,
    timeout_ms: ?u64,
    wait_hook: ?WaitHook,
) TransportError!*Conn {
    const stream = tcpConnect(io, host, port, timeout_ms, wait_hook) catch |err| return switch (err) {
        error.ConnectionRefused, error.ConnectTimedOut, error.HostNotFound, error.WaitHookAborted => err,
        else => error.ConnectFailed,
    };
    errdefer stream.close(io);

    const c = try alloc.create(Conn);
    errdefer alloc.destroy(c);
    c.* = .{
        .stream = stream,
        .io = io,
        .debug = env.get("KITE_DEBUG") != null,
        .rbuf = undefined,
        .wbuf = undefined,
        .tls_rbuf = undefined,
        .tls_wbuf = undefined,
        .net_reader = undefined,
        .net_writer = undefined,
        .ca_lock = .init,
        .tls_client = null,
        .redact_next_recv = false,
        .input = undefined,
        .output = undefined,
        .host = host,
        .port = port,
    };
    c.net_reader = .{ .fd = stream.socket.handle, .interface = .{ .vtable = &.{ .stream = SocketReader.stream, .readVec = SocketReader.readVec }, .buffer = &c.rbuf, .seek = 0, .end = 0 } };
    c.net_writer = .{ .fd = stream.socket.handle, .interface = .{ .vtable = &.{ .drain = SocketWriter.drain }, .buffer = &c.wbuf } };
    c.input = &c.net_reader.interface;
    c.output = &c.net_writer.interface;
    if (timeout_ms) |ms| setTimeout(c, ms);

    if (ca) |bundle| {
        c.net_reader.wait_hook = wait_hook;
        if (timeout_ms) |ms| {
            c.net_reader.cancellation = .{
                .io = io,
                .deadline = std.Io.Timestamp.now(io, .awake).addDuration(.fromMilliseconds(@intCast(ms))),
                .stop = null,
            };
        }
        defer {
            c.net_reader.wait_hook = null;
            c.net_reader.cancellation = null;
        }
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
            if (c.net_reader.err) |rerr| {
                if (rerr == error.WaitHookAborted) return error.WaitHookAborted;
                if (rerr == error.IoTimedOut) return error.ConnectTimedOut;
            }
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

pub fn setTimeout(c: *Conn, ms: u64) void {
    if (c.closed or c.timeout_ms == ms) return;
    const tv = std.posix.timeval{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) };
    std.posix.setsockopt(c.stream.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
    std.posix.setsockopt(c.stream.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&tv)) catch {};
    c.timeout_ms = ms;
}

/// Wait until `c` has buffered plaintext or socket-readable bytes, or `timeout_ms` elapses.
pub fn waitReadable(c: *Conn, timeout_ms: u64) bool {
    if (c.closed) return true;
    if (c.input.seek < c.input.end) return true;
    var fds = [_]std.posix.pollfd{.{ .fd = c.net_reader.fd, .events = std.posix.POLL.IN, .revents = 0 }};
    const rc = std.posix.poll(&fds, @intCast(@min(timeout_ms, std.math.maxInt(i32)))) catch return true;
    return rc > 0;
}

/// Wait until either `a` or `b` has buffered plaintext or socket-readable bytes, or `deadline` expires.
/// Returns 0 if `a` is ready, 1 if `b` is ready, or null on timeout.
pub fn waitEitherReadable(io: std.Io, a: *Conn, b: *Conn, deadline: std.Io.Timestamp) ?u1 {
    if (a.closed or a.input.seek < a.input.end) return 0;
    if (b.closed or b.input.seek < b.input.end) return 1;
    while (true) {
        const rem = std.Io.Timestamp.now(io, .awake).durationTo(deadline).toMilliseconds();
        if (rem <= 0) return null;
        var fds = [_]std.posix.pollfd{
            .{ .fd = a.net_reader.fd, .events = std.posix.POLL.IN, .revents = 0 },
            .{ .fd = b.net_reader.fd, .events = std.posix.POLL.IN, .revents = 0 },
        };
        const rc = std.posix.poll(&fds, @intCast(@min(@as(u64, @intCast(rem)), std.math.maxInt(i32)))) catch return 0;
        if (rc == 0) return null;
        if (fds[0].revents != 0) return 0;
        if (fds[1].revents != 0) return 1;
    }
}

/// Send a framed request (4-byte big-endian length + payload).
pub fn send(c: *Conn, payload: []const u8) TransportError!void {
    return sendv(c, &.{payload});
}

/// Send a framed request assembled from parts — the frame length is the sum
/// of part lengths; parts are written back-to-back without copying into a
/// contiguous buffer (avoids duplicating multi-MB record batches).
pub fn sendv(c: *Conn, parts: []const []const u8) TransportError!void {
    if (c.closed) return error.IoFailed;
    errdefer c.close();
    var total: usize = 0;
    for (parts) |p| total += p.len;
    c.redact_next_recv = false;
    if (c.debug) {
        const first = parts[0];
        // The request header starts with the api key; never dump SASL tokens.
        const redact = first.len >= 2 and
            std.mem.readInt(i16, first[0..2], .big) == protocol.api_key.sasl_authenticate;
        if (redact) {
            c.redact_next_recv = true;
            std.debug.print("send {d}B: <SaslAuthenticate redacted>\n", .{total});
        } else {
            std.debug.print("send {d}B: {x}\n", .{ total, first[0..@min(first.len, 200)] });
        }
    }
    var hdr: [4]u8 = undefined;
    std.mem.writeInt(u32, &hdr, @intCast(total), .big);
    c.output.writeAll(&hdr) catch return c.ioError();
    for (parts) |p| c.output.writeAll(p) catch return c.ioError();
    c.output.flush() catch |err| {
        if (c.debug)
            std.debug.print("send flush: {s}\n", .{@errorName(err)});
        return c.ioError();
    };
    // The TLS writer's flush encrypts into the net writer's buffer without
    // pushing it out; the underlying flush puts ciphertext on the wire.
    if (c.tls_client != null) c.net_writer.interface.flush() catch return c.ioError();
}

pub const Response = struct {
    /// Full allocation backing the frame; free this, not `body`.
    frame: []u8,
    /// Response body after the v1 header (correlation id + tag buffer).
    body: []u8,
};

pub const FrameAccumulator = struct {
    hdr: [4]u8 = undefined,
    hdr_len: usize = 0,
    frame: ?[]u8 = null,
    body_len: usize = 0,

    pub fn deinit(f: *FrameAccumulator, alloc: std.mem.Allocator) void {
        if (f.frame) |buf| alloc.free(buf);
        f.frame = null;
        f.hdr_len = 0;
        f.body_len = 0;
    }

    pub fn hasPartial(f: *const FrameAccumulator) bool {
        return f.hdr_len > 0 or f.frame != null;
    }

    fn readAvailable(c: *Conn, dst: []u8) TransportError!usize {
        while (c.input.buffered().len == 0) {
            const before = c.net_reader.interface.seek;
            c.net_reader.err = null;
            c.net_reader.poll_nonblock = true;
            defer c.net_reader.poll_nonblock = false;
            c.input.fillMore() catch |err| {
                if (err == error.ReadFailed and c.net_reader.err != null and c.net_reader.err.? == error.IoTimedOut) {
                    c.net_reader.err = null;
                    return 0;
                }
                c.close();
                return if (err == error.EndOfStream) error.IoFailed else c.ioError();
            };
            if (c.input.buffered().len == 0 and c.net_reader.interface.seek == before) return 0;
        }
        const avail = c.input.buffered();
        const n = @min(avail.len, dst.len);
        @memcpy(dst[0..n], avail[0..n]);
        c.input.toss(n);
        return n;
    }

    /// Drain any currently available bytes from `c` without blocking.
    /// Returns a complete validated Response once the full frame arrives,
    /// `null` while the frame is incomplete, or a TransportError on failure.
    pub fn step(f: *FrameAccumulator, c: *Conn, alloc: std.mem.Allocator, expect_corr: i32, header_tags: bool) TransportError!?Response {
        if (c.closed) return error.IoFailed;
        while (f.hdr_len < 4) {
            const n = try readAvailable(c, f.hdr[f.hdr_len..4]);
            if (n == 0) return null;
            f.hdr_len += n;
        }
        if (f.frame == null) {
            const len = std.mem.readInt(u32, &f.hdr, .big);
            if (len > max_response_len) {
                c.close();
                return error.ResponseTooLarge;
            }
            f.frame = alloc.alloc(u8, len) catch return error.OutOfMemory;
        }
        const buf = f.frame.?;
        while (f.body_len < buf.len) {
            const n = try readAvailable(c, buf[f.body_len..]);
            if (n == 0) return null;
            f.body_len += n;
        }
        f.frame = null;
        f.hdr_len = 0;
        f.body_len = 0;
        errdefer alloc.free(buf);
        if (c.debug) {
            if (c.redact_next_recv) {
                c.redact_next_recv = false;
                std.debug.print("recv {d}B: <redacted>\n", .{buf.len});
            } else {
                const n = @min(buf.len, 512);
                std.debug.print("recv {d}B: {x}\n", .{ buf.len, buf[0..n] });
            }
        }
        var d = protocol.Decoder.init(buf);
        const corr = d.i32v() catch {
            c.close();
            return error.MalformedResponse;
        };
        if (corr != expect_corr) {
            c.close();
            return error.CorrelationMismatch;
        }
        if (header_tags) d.tagBuffer() catch {
            c.close();
            return error.MalformedResponse;
        };
        return .{ .frame = buf, .body = buf[d.pos..] };
    }
};

/// Receive one framed response and validate its correlation id.
/// `header_tags`: response header v1 carries a tag buffer — every flexible
/// response except ApiVersions, which is always header v0.
pub fn recv(c: *Conn, alloc: std.mem.Allocator, expect_corr: i32, header_tags: bool) TransportError!Response {
    if (c.closed) return error.IoFailed;
    errdefer c.close();
    const hdr = c.input.takeArray(4) catch |err| {
        if (c.debug) {
            std.debug.print("recv hdr: {s}", .{@errorName(err)});
            if (c.tls_client) |*t| std.debug.print(" read_err={s}", .{@errorName(t.read_err orelse error{NoErr}.NoErr)});
            std.debug.print("\n", .{});
        }
        return c.ioError();
    };
    const len = std.mem.readInt(u32, hdr, .big);
    if (len > max_response_len) return error.ResponseTooLarge;
    const frame = alloc.alloc(u8, len) catch return error.OutOfMemory;
    errdefer alloc.free(frame);
    c.input.readSliceAll(frame) catch return c.ioError();

    if (c.debug) {
        if (c.redact_next_recv) {
            c.redact_next_recv = false;
            std.debug.print("recv {d}B: <redacted>\n", .{frame.len});
        } else {
            const n = @min(frame.len, 512);
            std.debug.print("recv {d}B: {x}\n", .{ frame.len, frame[0..n] });
        }
    }
    var d = protocol.Decoder.init(frame);
    const corr = d.i32v() catch return error.MalformedResponse;
    if (corr != expect_corr) return error.CorrelationMismatch;
    if (header_tags) d.tagBuffer() catch return error.MalformedResponse;
    return .{ .frame = frame, .body = frame[d.pos..] };
}
