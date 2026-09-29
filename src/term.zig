const std = @import("std");

pub const bold = "\x1b[1m";
pub const dim = "\x1b[2m";
pub const red = "\x1b[31m";
pub const yellow = "\x1b[33m";
pub const green = "\x1b[32m";
pub const bold_blue = "\x1b[1;34m";
pub const cyan = "\x1b[36m";
pub const reset = "\x1b[0m";

pub const Color = struct {
    enabled: bool,

    pub fn wrap(self: Color, code: []const u8, text: []const u8, buf: []u8) []const u8 {
        if (!self.enabled) return text;
        return std.fmt.bufPrint(buf, "{s}{s}{s}", .{ code, text, reset }) catch text;
    }
};

pub var color: Color = .{ .enabled = false };

const reverse = "\x1b[7m";
const hide_cursor = "\x1b[?25l";
const show_cursor = "\x1b[?25h";
const clear_line = "\x1b[2K";

/// termios to restore when the picker dies on a signal (SIGINT via ISIG).
var pick_saved: ?std.posix.termios = null;

fn pickRestoreAndExit(_: std.posix.SIG) callconv(.c) void {
    if (pick_saved) |t| {
        std.posix.tcsetattr(std.posix.STDIN_FILENO, .NOW, t) catch {};
        writeAllFd(std.posix.STDERR_FILENO, "\x1b[0m" ++ show_cursor ++ "\n");
    }
    std.process.exit(130);
}

fn writeAllFd(fd: std.posix.fd_t, bytes: []const u8) void {
    var rest = bytes;
    while (rest.len > 0) {
        const n = std.os.linux.write(@intCast(fd), rest.ptr, rest.len);
        if (n == 0 or n > rest.len) return;
        rest = rest[n..];
    }
}

fn pickDraw(items: []const []const u8, sel: usize, marked: usize, up_lines: usize) void {
    var buf: [4096]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    if (up_lines > 0) w.print("\x1b[{d}A", .{up_lines}) catch unreachable;
    w.writeAll(clear_line ++ "Select a cluster (\u{2191}/\u{2193} or j/k, Enter to choose, q to quit)\n") catch unreachable;
    for (items, 0..) |item, i| {
        w.writeAll(clear_line) catch unreachable;
        if (i == sel) w.writeAll(reverse ++ "> ") catch unreachable else w.writeAll("  ") catch unreachable;
        w.writeAll(item) catch unreachable;
        if (i == marked) w.writeAll(" *") catch unreachable;
        if (i == sel) w.writeAll(reset) catch unreachable;
        w.writeAll("\n") catch unreachable;
    }
    writeAllFd(std.posix.STDERR_FILENO, w.buffered());
}

fn pickErase(lines: usize) void {
    var buf: [4096]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    w.print("\x1b[{d}A", .{lines}) catch unreachable;
    for (0..lines) |_| w.writeAll(clear_line ++ "\n") catch unreachable;
    w.print("\x1b[{d}A", .{lines}) catch unreachable;
    writeAllFd(std.posix.STDERR_FILENO, w.buffered());
}

/// Interactive single-choice menu drawn on stderr: ↑/↓ or j/k move,
/// Enter returns the index, q/Esc/^C/^D return null. `initial` is the
/// highlighted entry and also carries the ` *` current-marker.
pub fn pick(io: std.Io, alloc: std.mem.Allocator, items: []const []const u8, initial: usize) !?usize {
    _ = io;
    _ = alloc;
    const fd = std.posix.STDIN_FILENO;
    const saved = std.posix.tcgetattr(fd) catch return error.NotATerminal;
    var raw = saved;
    raw.lflag.ECHO = false;
    raw.lflag.ICANON = false;
    try std.posix.tcsetattr(fd, .NOW, raw);
    defer std.posix.tcsetattr(fd, .NOW, saved) catch {};
    // ISIG stays on: ^C raises SIGINT, which restores the tty and exits 130.
    pick_saved = saved;
    defer pick_saved = null;
    const act: std.posix.Sigaction = .{
        .handler = .{ .handler = pickRestoreAndExit },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.INT, &act, null);
    defer std.posix.sigaction(.INT, &.{ .handler = .{ .handler = std.posix.SIG.DFL }, .mask = std.posix.sigemptyset(), .flags = 0 }, null);

    writeAllFd(std.posix.STDERR_FILENO, hide_cursor);
    var sel = @min(initial, items.len - 1);
    const lines = items.len + 1;
    pickDraw(items, sel, initial, 0);
    defer {
        pickErase(lines);
        writeAllFd(std.posix.STDERR_FILENO, show_cursor);
    }

    var b: [8]u8 = undefined;
    while (true) {
        const n = std.posix.read(fd, &b) catch |err| switch (err) {
            error.WouldBlock => continue,
            else => return err,
        };
        if (n == 0) return null;
        for (b[0..n]) |ch| {
            switch (ch) {
                '\r', '\n' => return sel,
                'q', 0x03, 0x04 => return null,
                'k' => sel = if (sel == 0) items.len - 1 else sel - 1,
                'j' => sel = (sel + 1) % items.len,
                0x1b => {
                    if (n == 1) return null;
                    if (n >= 3 and b[1] == '[') switch (b[2]) {
                        'A' => sel = if (sel == 0) items.len - 1 else sel - 1,
                        'B' => sel = (sel + 1) % items.len,
                        else => {},
                    };
                },
                else => {},
            }
        }
        pickDraw(items, sel, initial, lines);
    }
}

pub fn detect(io: std.Io, file: std.Io.File, env: *const std.process.Environ.Map) bool {
    if (env.get("KITE_COLOR")) |value| {
        if (std.mem.eql(u8, value, "never")) return false;
        if (std.mem.eql(u8, value, "always")) return true;
    }
    if (env.get("NO_COLOR")) |value| if (value.len > 0) return false;
    return file.isTty(io) catch false;
}

fn appendStyled(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, enabled: bool, code: []const u8, text: []const u8) !void {
    if (enabled) try out.appendSlice(alloc, code);
    try out.appendSlice(alloc, text);
    if (enabled) try out.appendSlice(alloc, reset);
}

pub fn renderHelp(alloc: std.mem.Allocator, page: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    var lines = std.mem.splitScalar(u8, page, '\n');
    while (lines.next()) |line| {
        if (line.len > 0 and line[0] != ' ' and line[line.len - 1] == ':') {
            try appendStyled(&out, alloc, color.enabled, bold, line);
        } else if (std.mem.startsWith(u8, line, "  ")) {
            const prefix_len: usize = 2;
            const body = line[prefix_len..];
            if (body.len > 0 and (body[0] == '-' or body[0] == '@')) {
                const end = std.mem.indexOf(u8, body, "  ") orelse body.len;
                try out.appendSlice(alloc, line[0..prefix_len]);
                try appendStyled(&out, alloc, color.enabled, green, body[0..end]);
                try out.appendSlice(alloc, body[end..]);
            } else {
                const word: []const u8 = if (std.mem.startsWith(u8, body, "kite produce"))
                    "kite produce"
                else if (std.mem.startsWith(u8, body, "kite consume"))
                    "kite consume"
                else if (std.mem.startsWith(u8, body, "kite topic"))
                    "kite topic"
                else if (std.mem.startsWith(u8, body, "kite cluster"))
                    "kite cluster"
                else if (std.mem.startsWith(u8, body, "kite"))
                    "kite"
                else
                    "";
                try out.appendSlice(alloc, line[0..prefix_len]);
                if (word.len > 0) {
                    try appendStyled(&out, alloc, color.enabled, cyan, word);
                    try out.appendSlice(alloc, line[prefix_len + word.len ..]);
                } else {
                    try out.appendSlice(alloc, body);
                }
            }
        } else {
            try out.appendSlice(alloc, line);
        }
        try out.append(alloc, '\n');
    }
    return out.toOwnedSlice(alloc);
}

/// Syntax-color one JSON value: object keys bold blue, other strings
/// green, `null` dim, everything else verbatim. Escapes inside strings
/// are skipped so a \" does not end the span early.
pub fn writeJsonColored(w: *std.Io.Writer, bytes: []const u8) !void {
    var i: usize = 0;
    while (i < bytes.len) {
        if (bytes[i] == '"') {
            var j = i + 1;
            while (j < bytes.len) {
                if (bytes[j] == '\\') {
                    j += 2;
                    continue;
                }
                if (bytes[j] == '"') break;
                j += 1;
            }
            const end = @min(j + 1, bytes.len);
            var k = end;
            while (k < bytes.len and (bytes[k] == ' ' or bytes[k] == '\t')) k += 1;
            const is_key = k < bytes.len and bytes[k] == ':';
            try w.writeAll(if (is_key) bold_blue else green);
            try w.writeAll(bytes[i..end]);
            try w.writeAll(reset);
            i = end;
        } else if (i + 4 <= bytes.len and std.mem.eql(u8, bytes[i .. i + 4], "null") and
            (i + 4 == bytes.len or !std.ascii.isAlphanumeric(bytes[i + 4])))
        {
            try w.writeAll(dim);
            try w.writeAll("null");
            try w.writeAll(reset);
            i += 4;
        } else {
            try w.writeByte(bytes[i]);
            i += 1;
        }
    }
}

fn stripAnsi(alloc: std.mem.Allocator, bytes: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    var i: usize = 0;
    while (i < bytes.len) {
        if (bytes[i] == 0x1b) {
            while (i < bytes.len and bytes[i] != 'm') i += 1;
            i += 1;
        } else {
            try out.append(alloc, bytes[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice(alloc);
}

test "writeJsonColored colors keys, strings, escapes and null" {
    const alloc = std.testing.allocator;
    const input = "{\"a\":\"x\",\"b\":null,\"c\":\"he\\\"llo\\\\\",\"d\":[1,\"s\"],\"nullable\":true}";
    var buf: std.Io.Writer.Allocating = .init(alloc);
    defer buf.deinit();
    try writeJsonColored(&buf.writer, input);
    const out = buf.written();

    try std.testing.expect(std.mem.indexOf(u8, out, bold_blue ++ "\"a\"" ++ reset) != null);
    try std.testing.expect(std.mem.indexOf(u8, out, green ++ "\"x\"" ++ reset) != null);
    try std.testing.expect(std.mem.indexOf(u8, out, green ++ "\"he\\\"llo\\\\\"" ++ reset) != null);
    try std.testing.expect(std.mem.indexOf(u8, out, dim ++ "null" ++ reset) != null);
    try std.testing.expect(std.mem.indexOf(u8, out, bold_blue ++ "\"nullable\"" ++ reset) != null);
    // Keys inside the string value are not treated as object keys.
    try std.testing.expect(std.mem.indexOf(u8, out, bold_blue ++ "\"llo\"") == null);

    const plain = try stripAnsi(alloc, out);
    defer alloc.free(plain);
    try std.testing.expectEqualStrings(input, plain);
}

test "writeJsonColored leaves bare scalars unstyled" {
    const alloc = std.testing.allocator;
    var buf: std.Io.Writer.Allocating = .init(alloc);
    defer buf.deinit();
    try writeJsonColored(&buf.writer, "42, -1.5, true");
    try std.testing.expectEqualStrings("42, -1.5, true", buf.written());
}
