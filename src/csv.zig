//! RFC 4180 CSV input: quote-aware record boundaries (a quoted newline does
//! not end the row), `""` unescaping, and JSON object emission.

const std = @import("std");
const json = @import("json.zig");

/// Read one logical record's raw bytes (no trailing newline). Returns null at
/// EOF. The returned slice is arena-owned.
pub fn nextRow(r: *std.Io.Reader, alloc: std.mem.Allocator) !?[]u8 {
    var acc: std.ArrayListUnmanaged(u8) = .empty;
    var in_quotes = false;
    var eof = false;
    var need_more = false;
    while (true) {
        var buf = r.buffered();
        // A short read (a pipe's first read may deliver zero bytes without
        // signalling EOF) is not the end of the stream; only EndOfStream is.
        while ((buf.len == 0 or need_more) and !eof) {
            r.fillMore() catch |err| switch (err) {
                error.EndOfStream => eof = true,
                error.ReadFailed => return error.ReadFailed,
            };
            buf = r.buffered();
            need_more = false;
        }
        if (buf.len == 0) {
            if (acc.items.len == 0) return null;
            const row = try alloc.dupe(u8, acc.items);
            return stripCrTail(row);
        }
        var i: usize = 0;
        scan: while (i < buf.len) : (i += 1) {
            switch (buf[i]) {
                '"' => {
                    if (in_quotes and i + 1 < buf.len and buf[i + 1] == '"') {
                        i += 1; // "" escape: still inside quotes
                    } else if (in_quotes and i + 1 == buf.len and !eof) {
                        need_more = true;
                        break :scan; // need next byte to tell close vs escape
                    } else in_quotes = !in_quotes;
                },
                '\n' => {
                    if (!in_quotes) {
                        try acc.appendSlice(alloc, buf[0..i]);
                        r.toss(i + 1);
                        const row = try alloc.dupe(u8, acc.items);
                        return stripCrTail(row);
                    }
                },
                else => {},
            }
        }
        try acc.appendSlice(alloc, buf[0..i]);
        r.toss(i);
    }
}

/// Does buffered data already hold a complete row? Used to skip the linger
/// poll only when a read can never block. Only meaningful at a row boundary,
/// i.e. when no partial row has been consumed from the reader.
pub fn rowReady(r: *std.Io.Reader) bool {
    const buf = r.buffered();
    var in_quotes = false;
    var i: usize = 0;
    while (i < buf.len) : (i += 1) {
        switch (buf[i]) {
            '"' => {
                if (in_quotes and i + 1 < buf.len and buf[i + 1] == '"') {
                    i += 1;
                } else if (in_quotes and i + 1 == buf.len) {
                    return false; // ambiguous without the next byte
                } else in_quotes = !in_quotes;
            },
            '\n' => if (!in_quotes) return true,
            else => {},
        }
    }
    return false;
}

fn stripCrTail(s: []u8) []u8 {
    return if (s.len > 0 and s[s.len - 1] == '\r') s[0 .. s.len - 1] else s;
}

/// Split an owned row into fields, unescaping `""` → `"` in place. Quoted
/// fields tolerate any byte after the closing quote except `,`, which starts
/// the next field.
pub fn splitFields(alloc: std.mem.Allocator, row: []u8) ![][]const u8 {
    var fields: std.ArrayListUnmanaged([]const u8) = .empty;
    var i: usize = 0;
    while (true) {
        if (i < row.len and row[i] == '"') {
            i += 1;
            const fstart = i; // unescaped text is compacted in place
            var w = i;
            var closed = false;
            while (i < row.len) {
                if (row[i] == '"') {
                    if (i + 1 < row.len and row[i + 1] == '"') {
                        row[w] = '"';
                        w += 1;
                        i += 2;
                    } else {
                        i += 1;
                        closed = true;
                        break;
                    }
                } else {
                    row[w] = row[i];
                    w += 1;
                    i += 1;
                }
            }
            if (!closed) return error.MalformedCsv; // unterminated quote
            try fields.append(alloc, row[fstart..w]);
            if (i < row.len and row[i] == ',') {
                i += 1;
                if (i == row.len) {
                    try fields.append(alloc, ""); // trailing comma
                    break;
                }
                continue;
            }
            break;
        } else {
            const start = i;
            while (i < row.len and row[i] != ',') i += 1;
            var f = row[start..i];
            if (f.len > 0 and f[f.len - 1] == '\r') f = f[0 .. f.len - 1];
            try fields.append(alloc, f);
            if (i >= row.len) break;
            i += 1;
            if (i >= row.len) {
                try fields.append(alloc, "");
                break;
            }
        }
    }
    return fields.items;
}

/// Emit `{"col":"value",...}` for a row. All fields become JSON strings.
pub fn rowJson(w: *std.Io.Writer, cols: []const []const u8, fields: []const []const u8) !void {
    try w.writeByte('{');
    for (fields, 0..) |f, n| {
        if (n > 0) try w.writeByte(',');
        const name = if (n < cols.len) cols[n] else "";
        try json.writeString(w, name);
        try w.writeByte(':');
        try json.writeString(w, f);
    }
    try w.writeByte('}');
}

/// Strip a UTF-8 BOM from the first row if present.
pub fn stripBom(s: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, s, "\xef\xbb\xbf")) s[3..] else s;
}

const expect = std.testing.expect;
const expectEqualStrings = std.testing.expectEqualStrings;
const expectError = std.testing.expectError;

fn parseOne(input: []const u8, row_index: usize) ![][]const u8 {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var r = std.Io.Reader.fixed(input);
    var n: usize = 0;
    var last: [][]const u8 = &.{};
    while (try nextRow(&r, alloc)) |row| {
        last = try splitFields(alloc, row);
        if (n == row_index) {
            const out = try std.testing.allocator.alloc([]const u8, last.len);
            for (last, 0..) |f, i| out[i] = try std.testing.allocator.dupe(u8, f);
            return out;
        }
        n += 1;
    }
    return error.NoSuchRow;
}

fn freeFields(fields: [][]const u8) void {
    for (fields) |f| std.testing.allocator.free(f);
    std.testing.allocator.free(fields);
}

test "simple row" {
    const f = try parseOne("a,b,c\n", 0);
    defer freeFields(f);
    try expect(f.len == 3);
    try expectEqualStrings("a", f[0]);
    try expectEqualStrings("c", f[2]);
}

test "quoted newline stays inside the record" {
    const f = try parseOne("id,note\n1,\"x\ny\"\n2,z\n", 1);
    defer freeFields(f);
    try expectEqualStrings("x\ny", f[1]);
}

test "double-quote escape" {
    const f = try parseOne("k,v\n1,\"she said \"\"hi\"\"\"\n", 1);
    defer freeFields(f);
    try expectEqualStrings("she said \"hi\"", f[1]);
}

test "trailing comma yields empty field" {
    const f = try parseOne("a,b,\n", 0);
    defer freeFields(f);
    try expect(f.len == 3);
    try expectEqualStrings("", f[2]);
}

test "final row without trailing newline" {
    const f = try parseOne("a\n1,2\n3,4", 2);
    defer freeFields(f);
    try expectEqualStrings("4", f[1]);
}

test "crlf endings" {
    const f = try parseOne("a,b\r\n1,2\r\n", 1);
    defer freeFields(f);
    try expectEqualStrings("2", f[1]);
}

test "bom strip" {
    try expectEqualStrings("id", stripBom("\xef\xbb\xbfid"));
    try expectEqualStrings("id", stripBom("id"));
}

test "unterminated quote errors" {
    try expectError(error.MalformedCsv, parseOne("a\n\"never closed", 1));
}

test "rowJson escaping" {
    var jw = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer jw.deinit();
    const cols = [_][]const u8{ "a", "b" };
    const fields = [_][]const u8{ "q\"q", "tab\there" };
    try rowJson(&jw.writer, &cols, &fields);
    try expectEqualStrings("{\"a\":\"q\\\"q\",\"b\":\"tab\\there\"}", jw.written());
}

/// Test reader that hands out `chunk` bytes per read (like a slow pipe) and,
/// when `zero_first` is set, answers the first read with zero bytes without
/// signalling EOF, as `File.Reader` does when a pipe is not seekable.
const ChunkedReader = struct {
    interface: std.Io.Reader,
    src: []const u8,
    pos: usize = 0,
    chunk: usize,
    zero_first: bool,

    fn init(buf: []u8, src: []const u8, chunk: usize, zero_first: bool) ChunkedReader {
        return .{
            .interface = .{
                .vtable = &.{ .stream = stream },
                .buffer = buf,
                .seek = 0,
                .end = 0,
            },
            .src = src,
            .chunk = chunk,
            .zero_first = zero_first,
        };
    }

    fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *ChunkedReader = @alignCast(@fieldParentPtr("interface", r));
        if (self.zero_first) {
            self.zero_first = false;
            return 0;
        }
        if (self.pos == self.src.len) return error.EndOfStream;
        const n = @min(self.chunk, @intFromEnum(limit), self.src.len - self.pos);
        try w.writeAll(self.src[self.pos .. self.pos + n]);
        self.pos += n;
        return n;
    }
};

fn collectRows(alloc: std.mem.Allocator, r: *std.Io.Reader) ![]const []const []const u8 {
    var rows: std.ArrayListUnmanaged([]const []const u8) = .empty;
    while (try nextRow(r, alloc)) |row| try rows.append(alloc, try splitFields(alloc, row));
    return rows.items;
}

const stream_fixture =
    "id,name,note\r\n" ++
    "1,\"Ann \"\"A\"\" Lee\",\"line one\nline two\"\r\n" ++
    "2,Bob,\"\"\n" ++
    "3,Cy,tail";

fn expectStreamFixture(rows: []const []const []const u8) !void {
    try expect(rows.len == 4);
    try expectEqualStrings("note", rows[0][2]);
    try expectEqualStrings("Ann \"A\" Lee", rows[1][1]);
    try expectEqualStrings("line one\nline two", rows[1][2]);
    try expectEqualStrings("", rows[2][2]);
    try expectEqualStrings("3", rows[3][0]);
    try expectEqualStrings("tail", rows[3][2]);
}

test "pipe whose first read returns zero bytes is not empty" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var buf: [64]u8 = undefined;
    var cr = ChunkedReader.init(&buf, stream_fixture, 4096, true);
    try expectStreamFixture(try collectRows(arena.allocator(), &cr.interface));
}

test "chunked input: every chunk size yields the same records" {
    var chunk: usize = 1;
    while (chunk <= stream_fixture.len + 1) : (chunk += 1) {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var buf: [16]u8 = undefined;
        var cr = ChunkedReader.init(&buf, stream_fixture, chunk, false);
        try expectStreamFixture(try collectRows(arena.allocator(), &cr.interface));
    }
}

test "truly empty pipe reports EOF" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var buf: [16]u8 = undefined;
    var cr = ChunkedReader.init(&buf, "", 1, true);
    try expect((try nextRow(&cr.interface, arena.allocator())) == null);
}

test "rowReady" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var r = std.Io.Reader.fixed("x,\"y\nz\",w\nrest");
    try expect(rowReady(&r)); // boundary at '\n' after w; the quoted one doesn't count
    _ = try nextRow(&r, alloc);
    try expect(!rowReady(&r)); // "rest" has no newline left

    var r2 = std.Io.Reader.fixed("\"a\nb\"");
    try expect(!rowReady(&r2)); // newline only inside quotes
}
