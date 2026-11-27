//! Minimal JSON helpers shared by CSV conversion and --json record I/O.
//! A tiny scanner is used instead of std.json to keep the binary small and to
//! pass non-string values through to Kafka byte-for-byte.

const std = @import("std");

/// Write `s` as a JSON string literal, escaping quotes, backslashes and
/// control characters.
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

pub fn writeBase64String(w: *std.Io.Writer, bytes: []const u8) !void {
    try w.writeByte('"');
    try std.base64.standard.Encoder.encodeWriter(w, bytes);
    try w.writeByte('"');
}

pub fn decodeBase64(alloc: std.mem.Allocator, encoded: []const u8) error{ InvalidBase64, OutOfMemory }![]u8 {
    const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InvalidBase64;
    const decoded = alloc.alloc(u8, decoded_len) catch return error.OutOfMemory;
    errdefer alloc.free(decoded);
    std.base64.standard.Decoder.decode(decoded, encoded) catch return error.InvalidBase64;
    return decoded;
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

fn isWhitespace(c: u8) bool {
    return switch (c) {
        ' ', '\t', '\r', '\n' => true,
        else => false,
    };
}

/// Cursor over a JSON document. Strings are unescaped into `alloc` only when
/// they contain a backslash; otherwise the source slice is returned.
pub const Scanner = struct {
    src: []const u8,
    pos: usize = 0,
    alloc: std.mem.Allocator,

    fn skipWs(s: *Scanner) void {
        while (s.pos < s.src.len and isWhitespace(s.src[s.pos])) s.pos += 1;
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
        if (!try s.nextEntry(first, '}')) return null;
        const key = try s.string(true);
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
        return s.nextEntry(first, ']');
    }

    fn nextEntry(s: *Scanner, first: bool, close: u8) Error!bool {
        const c = s.peek() orelse return error.Malformed;
        if (c == close) {
            s.pos += 1;
            return false;
        }
        if ((c == ',') == first) return error.Malformed;
        if (!first) s.pos += 1;
        return true;
    }

    pub fn value(s: *Scanner) Error!Value {
        const c = s.peek() orelse return error.Malformed;
        const start = s.pos;
        switch (c) {
            '"' => return .{ .string = try s.string(true) },
            '{' => {
                try s.skipNested('}');
                return .{ .object = s.src[start..s.pos] };
            },
            '[' => {
                try s.skipNested(']');
                return .{ .array = s.src[start..s.pos] };
            },
            else => return s.primitive(),
        }
    }

    fn primitive(s: *Scanner) Error!Value {
        const start = s.pos;
        while (s.pos < s.src.len) {
            const c = s.src[s.pos];
            if (c == ',' or c == '}' or c == ']' or isWhitespace(c)) break;
            s.pos += 1;
        }
        const raw = s.src[start..s.pos];
        if (std.mem.eql(u8, raw, "null")) return .null;
        if (!std.mem.eql(u8, raw, "true") and !std.mem.eql(u8, raw, "false") and !isNumber(raw))
            return error.Malformed;
        return .{ .other = raw };
    }

    fn skipNested(s: *Scanner, initial_close: u8) Error!void {
        var scratch = std.heap.stackFallback(64, s.alloc);
        const alloc = scratch.get();
        var closes: std.ArrayList(u8) = .empty;
        defer closes.deinit(alloc);
        try closes.append(alloc, initial_close);
        s.pos += 1;
        var first = true;
        while (closes.items.len != 0) {
            const close = closes.items[closes.items.len - 1];
            if (!try s.nextEntry(first, close)) {
                _ = closes.pop();
                first = false;
                continue;
            }
            if (close == '}') {
                _ = try s.string(false);
                try s.expect(':');
            }
            first = false;
            switch (s.peek() orelse return error.Malformed) {
                '{', '[' => |open| {
                    try closes.append(alloc, if (open == '{') '}' else ']');
                    s.pos += 1;
                    first = true;
                },
                '"' => _ = try s.string(false),
                else => _ = try s.primitive(),
            }
        }
    }

    fn string(s: *Scanner, decode: bool) Error![]const u8 {
        try s.expect('"');
        const start = s.pos;
        var escaped = false;
        while (s.pos < s.src.len) : (s.pos += 1) {
            switch (s.src[s.pos]) {
                '"' => {
                    const raw = s.src[start..s.pos];
                    s.pos += 1;
                    return if (escaped) try unescape(s.alloc, raw, decode) else raw;
                },
                '\\' => {
                    escaped = true;
                    s.pos += 1;
                },
                0...31 => return error.Malformed,
                else => {},
            }
        }
        return error.Malformed;
    }
};

fn isNumber(raw: []const u8) bool {
    var i: usize = 0;
    if (i < raw.len and raw[i] == '-') i += 1;
    if (i == raw.len or !std.ascii.isDigit(raw[i])) return false;
    if (raw[i] == '0') {
        i += 1;
    } else {
        while (i < raw.len and std.ascii.isDigit(raw[i])) i += 1;
    }
    if (i < raw.len and raw[i] == '.') {
        i += 1;
        const start = i;
        while (i < raw.len and std.ascii.isDigit(raw[i])) i += 1;
        if (i == start) return false;
    }
    if (i < raw.len and (raw[i] == 'e' or raw[i] == 'E')) {
        i += 1;
        if (i < raw.len and (raw[i] == '+' or raw[i] == '-')) i += 1;
        const start = i;
        while (i < raw.len and std.ascii.isDigit(raw[i])) i += 1;
        if (i == start) return false;
    }
    return i == raw.len;
}

fn hexQuad(raw: *const [4]u8) Error!u16 {
    var cp: u16 = 0;
    for (raw) |c| cp = (cp << 4) | (std.fmt.charToDigit(c, 16) catch return error.Malformed);
    return cp;
}

fn unescape(alloc: std.mem.Allocator, raw: []const u8, decode: bool) Error![]const u8 {
    var out: std.ArrayList(u8) = if (decode) try .initCapacity(alloc, raw.len) else .empty;
    errdefer out.deinit(alloc);
    var i: usize = 0;
    while (i < raw.len) : (i += 1) {
        const c = raw[i];
        if (c != '\\') {
            if (decode) out.appendAssumeCapacity(c);
            continue;
        }
        i += 1;
        if (i >= raw.len) return error.Malformed;
        const byte = switch (raw[i]) {
            '"', '\\', '/' => |e| e,
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'b' => 8,
            'f' => 12,
            'u' => {
                if (i + 4 >= raw.len) return error.Malformed;
                var cp: u21 = try hexQuad(raw[i + 1 ..][0..4]);
                i += 4;
                if (cp >= 0xD800 and cp < 0xDC00 and i + 6 < raw.len and raw[i + 1] == '\\' and raw[i + 2] == 'u') {
                    const lo = try hexQuad(raw[i + 3 ..][0..4]);
                    if (lo >= 0xDC00 and lo < 0xE000) {
                        cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                        i += 6;
                    }
                }
                var buf: [4]u8 = undefined;
                const n = std.unicode.utf8Encode(cp, &buf) catch return error.Malformed;
                if (decode) out.appendSliceAssumeCapacity(buf[0..n]);
                continue;
            },
            else => return error.Malformed,
        };
        if (decode) out.appendAssumeCapacity(byte);
    }
    return if (decode) out.items else raw;
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

test "scanner: valid JSON values are preserved verbatim" {
    for ([_][]const u8{
        "true",  "false", "0",           "-0",                      "123456789012345678901234567890",
        "-0.25", "1e9",   "-10.5E+0002", "1e-99999999999999999999",
        "{\"é\\u0021\" : [true, false, null, {}, [], \"\\t\\ud83d\\ude00\", -0.5e+2]}",
    }) |raw| {
        var s = Scanner{ .src = raw, .alloc = std.testing.allocator };
        try std.testing.expectEqualStrings(raw, (try s.value()).bytes().?);
        try std.testing.expect(s.atEnd());
    }
}

test "scanner: rejects invalid tokens, containers and strings" {
    for ([_][]const u8{
        "tru",      "truefalse",   "False",                 "nul",          "null1",                 "undefined",
        "-",        "+1",          "01",                    "-01",          ".2",                    "1.",
        "1e",       "1e+",         "1e-",                   "0x1",          "1_000",                 "NaN",
        "Infinity", "{\"x\":}",    "{\"x\" 1}",             "{\"x\":1,}",   "{\"x\":1 \"y\":2}",     "{x:1}",
        "[,1]",     "[1,]",        "[1,,2]",                "[1 2]",        "[}",                    "{]",
        "[",        "[[[]]",       "{\"x\":[{\"y\":tru}]}", "[{\"x\":01}]", "[true\x0c]",            "\x0btrue",
        "\"a\tb\"", "\"\\u+01f\"", "\"\\u0_01\"",           "\"\\q\"",      "{\"x\":[\"\\ud800\"]}", "{\"\\q\":1}",
    }) |raw| {
        var s = Scanner{ .src = raw, .alloc = std.testing.allocator };
        try std.testing.expectError(error.Malformed, s.value());
    }
}

test "scanner: deeply nested JSON uses a growable stack" {
    var raw: [2048]u8 = undefined;
    @memset(raw[0..1024], '[');
    @memset(raw[1024..], ']');
    var s = Scanner{ .src = &raw, .alloc = std.testing.allocator };
    try std.testing.expectEqualStrings(&raw, (try s.value()).array);
    try std.testing.expect(s.atEnd());
}

test "standard padded base64 decoder preserves bytes" {
    const original = [_]u8{ 0, 1, 0x7f, 0x80, 0xff };
    const decoded = try decodeBase64(std.testing.allocator, "AAF/gP8=");
    defer std.testing.allocator.free(decoded);
    try std.testing.expectEqualSlices(u8, &original, decoded);
    try std.testing.expectError(error.InvalidBase64, decodeBase64(std.testing.allocator, "not base64"));
    try std.testing.expectError(error.InvalidBase64, decodeBase64(std.testing.allocator, "AAF/gP8"));
    try std.testing.expectError(error.InvalidBase64, decodeBase64(std.testing.allocator, "!!!!"));
}
