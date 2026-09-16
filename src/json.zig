//! Minimal JSON helpers shared by CSV conversion and --json record I/O.
//! A tiny scanner is used instead of std.json to keep the binary small and to
//! pass non-string values through to Kafka byte-for-byte.

const std = @import("std");

/// Write `s` as a JSON string literal, escaping quotes, backslashes and
/// control characters. Bytes are passed through as-is (no UTF-8 validation).
pub fn writeString(w: *std.Io.Writer, s: []const u8) !void {
    try w.writeByte('"');
    for (s) |c| {
        switch (c) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            '\t' => try w.writeAll("\\t"),
            0...8, 11, 12, 14...31 => try w.print("\\u{x:0>4}", .{c}),
            else => try w.writeByte(c),
        }
    }
    try w.writeByte('"');
}

pub const Error = error{ Malformed, OutOfMemory };

/// A scanned JSON value: strings are unescaped, everything else is the raw
/// source slice so it can be forwarded verbatim.
pub const Value = union(enum) {
    null,
    string: []const u8,
    object: []const u8,
    array: []const u8,
    other: []const u8,

    /// The value as record bytes: strings decoded, anything else verbatim.
    pub fn bytes(v: Value) ?[]const u8 {
        return switch (v) {
            .null => null,
            inline else => |s| s,
        };
    }
};

pub const Member = struct { key: []const u8, value: Value };

/// Cursor over a JSON document. Strings are unescaped into `alloc` only when
/// they contain a backslash; otherwise the source slice is returned.
pub const Scanner = struct {
    src: []const u8,
    pos: usize = 0,
    alloc: std.mem.Allocator,

    fn skipWs(s: *Scanner) void {
        while (s.pos < s.src.len and std.ascii.isWhitespace(s.src[s.pos])) s.pos += 1;
    }

    fn peek(s: *Scanner) ?u8 {
        s.skipWs();
        return if (s.pos < s.src.len) s.src[s.pos] else null;
    }

    fn expect(s: *Scanner, c: u8) Error!void {
        if (s.peek() != c) return error.Malformed;
        s.pos += 1;
    }

    /// True when the document has nothing but whitespace left.
    pub fn atEnd(s: *Scanner) bool {
        return s.peek() == null;
    }

    /// Enter an object; returns false if the value here is not an object.
    pub fn beginObject(s: *Scanner) Error!bool {
        if (s.peek() != '{') return false;
        s.pos += 1;
        return true;
    }

    /// Next `"key": value` pair, or null once the closing brace is consumed.
    pub fn nextMember(s: *Scanner, first: bool) Error!?Member {
        switch (s.peek() orelse return error.Malformed) {
            '}' => {
                s.pos += 1;
                return null;
            },
            ',' => {
                if (first) return error.Malformed;
                s.pos += 1;
            },
            else => if (!first) return error.Malformed,
        }
        const key = try s.string();
        try s.expect(':');
        return .{ .key = key, .value = try s.value() };
    }

    /// Enter an array; returns false if the value here is not an array.
    pub fn beginArray(s: *Scanner) Error!bool {
        if (s.peek() != '[') return false;
        s.pos += 1;
        return true;
    }

    /// True while another element follows; consumes separators and `]`.
    pub fn nextElement(s: *Scanner, first: bool) Error!bool {
        switch (s.peek() orelse return error.Malformed) {
            ']' => {
                s.pos += 1;
                return false;
            },
            ',' => {
                if (first) return error.Malformed;
                s.pos += 1;
            },
            else => if (!first) return error.Malformed,
        }
        return true;
    }

    pub fn value(s: *Scanner) Error!Value {
        const c = s.peek() orelse return error.Malformed;
        const start = s.pos;
        switch (c) {
            '"' => return .{ .string = try s.string() },
            '{' => {
                try s.skipNested('{', '}');
                return .{ .object = s.src[start..s.pos] };
            },
            '[' => {
                try s.skipNested('[', ']');
                return .{ .array = s.src[start..s.pos] };
            },
            else => {
                while (s.pos < s.src.len) : (s.pos += 1) {
                    switch (s.src[s.pos]) {
                        ',', '}', ']', ' ', '\t', '\n', '\r' => break,
                        else => {},
                    }
                }
                const raw = s.src[start..s.pos];
                if (std.mem.eql(u8, raw, "null")) return .null;
                if (raw.len == 0) return error.Malformed;
                return .{ .other = raw };
            },
        }
    }

    fn skipNested(s: *Scanner, open: u8, close: u8) Error!void {
        var depth: usize = 0;
        while (s.pos < s.src.len) {
            const c = s.src[s.pos];
            if (c == '"') {
                _ = try s.string();
                continue;
            }
            s.pos += 1;
            if (c == open) depth += 1;
            if (c == close) {
                depth -= 1;
                if (depth == 0) return;
            }
        }
        return error.Malformed;
    }

    fn string(s: *Scanner) Error![]const u8 {
        try s.expect('"');
        const start = s.pos;
        var escaped = false;
        while (s.pos < s.src.len) : (s.pos += 1) {
            switch (s.src[s.pos]) {
                '"' => {
                    const raw = s.src[start..s.pos];
                    s.pos += 1;
                    return if (escaped) try unescape(s.alloc, raw) else raw;
                },
                '\\' => {
                    escaped = true;
                    s.pos += 1;
                },
                else => {},
            }
        }
        return error.Malformed;
    }
};

fn unescape(alloc: std.mem.Allocator, raw: []const u8) Error![]const u8 {
    var out = try std.ArrayListUnmanaged(u8).initCapacity(alloc, raw.len);
    var i: usize = 0;
    while (i < raw.len) : (i += 1) {
        const c = raw[i];
        if (c != '\\') {
            out.appendAssumeCapacity(c);
            continue;
        }
        i += 1;
        if (i >= raw.len) return error.Malformed;
        switch (raw[i]) {
            '"', '\\', '/' => |e| out.appendAssumeCapacity(e),
            'n' => out.appendAssumeCapacity('\n'),
            'r' => out.appendAssumeCapacity('\r'),
            't' => out.appendAssumeCapacity('\t'),
            'b' => out.appendAssumeCapacity(8),
            'f' => out.appendAssumeCapacity(12),
            'u' => {
                if (i + 4 >= raw.len) return error.Malformed;
                var cp: u21 = std.fmt.parseInt(u16, raw[i + 1 .. i + 5], 16) catch return error.Malformed;
                i += 4;
                if (cp >= 0xD800 and cp < 0xDC00 and i + 6 < raw.len and raw[i + 1] == '\\' and raw[i + 2] == 'u') {
                    const lo = std.fmt.parseInt(u16, raw[i + 3 .. i + 7], 16) catch return error.Malformed;
                    if (lo >= 0xDC00 and lo < 0xE000) {
                        cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                        i += 6;
                    }
                }
                var buf: [4]u8 = undefined;
                const n = std.unicode.utf8Encode(cp, &buf) catch return error.Malformed;
                try out.appendSlice(alloc, buf[0..n]);
            },
            else => return error.Malformed,
        }
    }
    return out.items;
}

test "scanner: flat object with strings, null and nested values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var s = Scanner{ .src =
        \\ {"key": "k\"1", "value": {"a": [1, "]"]}, "n": 42, "x": null, "arr": [1,2] }
    , .alloc = arena.allocator() };
    try std.testing.expect(try s.beginObject());
    var first = true;
    var seen: usize = 0;
    while (try s.nextMember(first)) |m| : (first = false) {
        seen += 1;
        if (std.mem.eql(u8, m.key, "key")) try std.testing.expectEqualStrings("k\"1", m.value.string);
        if (std.mem.eql(u8, m.key, "value")) try std.testing.expectEqualStrings("{\"a\": [1, \"]\"]}", m.value.object);
        if (std.mem.eql(u8, m.key, "n")) try std.testing.expectEqualStrings("42", m.value.other);
        if (std.mem.eql(u8, m.key, "x")) try std.testing.expect(m.value == .null);
        if (std.mem.eql(u8, m.key, "arr")) try std.testing.expectEqualStrings("[1,2]", m.value.array);
    }
    try std.testing.expectEqual(@as(usize, 5), seen);
    try std.testing.expect(s.atEnd());
}

test "scanner: unescapes \\u sequences and rejects malformed input" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var s = Scanner{ .src = "{\"v\":\"\\u00e9\\ud83d\\ude00\\n\"}", .alloc = arena.allocator() };
    try std.testing.expect(try s.beginObject());
    const m = (try s.nextMember(true)).?;
    try std.testing.expectEqualStrings("é😀\n", m.value.string);
    try std.testing.expect((try s.nextMember(false)) == null);

    for ([_][]const u8{ "{\"a\":1,}", "{\"a\" 1}", "{\"a\":\"x}", "{\"a\":[1}", "{,}", "{\"a\":}" }) |bad| {
        var b = Scanner{ .src = bad, .alloc = arena.allocator() };
        _ = try b.beginObject();
        var first = true;
        var failed = false;
        while (true) {
            const next = b.nextMember(first) catch {
                failed = true;
                break;
            };
            if (next == null) break;
            first = false;
        }
        try std.testing.expect(failed);
    }
}
