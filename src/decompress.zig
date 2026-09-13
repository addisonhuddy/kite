const std = @import("std");

pub const Error = error{
    UnsupportedCompression,
    InvalidCompression,
    InvalidSnappy,
    InvalidLz4,
};

pub fn decompress(alloc: std.mem.Allocator, codec: u3, input: []const u8) ![]u8 {
    return switch (codec) {
        0 => alloc.dupe(u8, input),
        1 => streamFlate(alloc, input),
        2 => streamSnappy(alloc, input),
        3 => streamLz4(alloc, input),
        4 => streamZstd(alloc, input),
        else => error.UnsupportedCompression,
    };
}

fn streamFlate(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    var reader: std.Io.Reader = .fixed(input);
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    var d = std.compress.flate.Decompress.init(&reader, .gzip, &.{});
    _ = try d.reader.streamRemaining(&out.writer);
    return out.toOwnedSlice();
}

fn streamZstd(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    var reader: std.Io.Reader = .fixed(input);
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    var d = std.compress.zstd.Decompress.init(&reader, &.{}, .{
        .window_len = 8 * 1024 * 1024,
    });
    _ = try d.reader.streamRemaining(&out.writer);
    return out.toOwnedSlice();
}

fn readLe32(data: []const u8, pos: *usize) !u32 {
    if (data.len -| pos.* < 4) return error.InvalidCompression;
    const v = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return v;
}

fn readBe32(data: []const u8, pos: *usize) !u32 {
    if (data.len -| pos.* < 4) return error.InvalidCompression;
    const v = std.mem.readInt(u32, data[pos.*..][0..4], .big);
    pos.* += 4;
    return v;
}

fn readSnappyVarint(data: []const u8, pos: *usize) !usize {
    var value: usize = 0;
    var shift: u6 = 0;
    while (true) {
        if (pos.* >= data.len or shift >= @bitSizeOf(usize)) return error.InvalidSnappy;
        const b = data[pos.*];
        pos.* += 1;
        value |= @as(usize, b & 0x7f) << shift;
        if (b & 0x80 == 0) return value;
        shift += 7;
    }
}

fn decodeRawSnappy(alloc: std.mem.Allocator, input: []const u8, out: *std.ArrayListUnmanaged(u8)) !void {
    var pos: usize = 0;
    const expected = try readSnappyVarint(input, &pos);
    while (pos < input.len and out.items.len < expected) {
        const tag = input[pos];
        pos += 1;
        switch (tag & 3) {
            0 => {
                var len: usize = (tag >> 2) + 1;
                if (len >= 61) {
                    const nbytes = len - 59;
                    if (nbytes > 4 or input.len -| pos < nbytes) return error.InvalidSnappy;
                    len = 0;
                    for (0..nbytes) |j| len |= @as(usize, input[pos + j]) << @intCast(j * 8);
                    pos += nbytes;
                    len += 1;
                }
                if (input.len -| pos < len) return error.InvalidSnappy;
                try out.appendSlice(alloc, input[pos .. pos + len]);
                pos += len;
            },
            1 => {
                if (pos >= input.len) return error.InvalidSnappy;
                const len: usize = 4 + ((tag >> 2) & 7);
                const offset = (@as(usize, tag >> 5) << 8) | input[pos];
                pos += 1;
                try copyMatch(alloc, out, offset, len);
            },
            2 => {
                if (input.len -| pos < 2) return error.InvalidSnappy;
                const len: usize = (tag >> 2) + 1;
                const offset = @as(usize, input[pos]) | (@as(usize, input[pos + 1]) << 8);
                pos += 2;
                try copyMatch(alloc, out, offset, len);
            },
            3 => {
                if (input.len -| pos < 4) return error.InvalidSnappy;
                const len: usize = (tag >> 2) + 1;
                const offset = @as(usize, input[pos]) |
                    (@as(usize, input[pos + 1]) << 8) |
                    (@as(usize, input[pos + 2]) << 16) |
                    (@as(usize, input[pos + 3]) << 24);
                pos += 4;
                try copyMatch(alloc, out, offset, len);
            },
            else => unreachable,
        }
    }
    if (out.items.len != expected) return error.InvalidSnappy;
}

fn copyMatch(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    offset: usize,
    len: usize,
) !void {
    if (offset == 0 or offset > out.items.len) return error.InvalidSnappy;
    for (0..len) |_| try out.append(alloc, out.items[out.items.len - offset]);
}

fn streamSnappy(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    const magic = "\x82SNAPPY\x00";
    if (!std.mem.startsWith(u8, input, magic)) {
        try decodeRawSnappy(alloc, input, &out);
        return out.toOwnedSlice(alloc);
    }
    var pos = magic.len;
    if (input.len -| pos < 8) return error.InvalidSnappy;
    _ = try readBe32(input, &pos);
    _ = try readBe32(input, &pos);
    while (pos < input.len) {
        const block_len = try readBe32(input, &pos);
        if (input.len -| pos < block_len) return error.InvalidSnappy;
        try decodeRawSnappy(alloc, input[pos .. pos + block_len], &out);
        pos += block_len;
    }
    return out.toOwnedSlice(alloc);
}

fn decodeRawLz4(alloc: std.mem.Allocator, input: []const u8, out: *std.ArrayListUnmanaged(u8)) !void {
    var pos: usize = 0;
    while (pos < input.len) {
        const token = input[pos];
        pos += 1;
        var literal_len: usize = token >> 4;
        if (literal_len == 15) {
            while (true) {
                if (pos >= input.len) return error.InvalidLz4;
                const n = input[pos];
                pos += 1;
                literal_len += n;
                if (n != 255) break;
            }
        }
        if (input.len -| pos < literal_len) return error.InvalidLz4;
        try out.appendSlice(alloc, input[pos .. pos + literal_len]);
        pos += literal_len;
        if (pos == input.len) break;
        if (input.len -| pos < 2) return error.InvalidLz4;
        const offset = std.mem.readInt(u16, input[pos..][0..2], .little);
        pos += 2;
        if (offset == 0 or offset > out.items.len) return error.InvalidLz4;
        var match_len: usize = (token & 15) + 4;
        if ((token & 15) == 15) {
            while (true) {
                if (pos >= input.len) return error.InvalidLz4;
                const n = input[pos];
                pos += 1;
                match_len += n;
                if (n != 255) break;
            }
        }
        try copyLz4Match(alloc, out, offset, match_len);
    }
}

fn copyLz4Match(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    offset: usize,
    len: usize,
) !void {
    if (offset == 0 or offset > out.items.len) return error.InvalidLz4;
    for (0..len) |_| try out.append(alloc, out.items[out.items.len - offset]);
}

fn streamLz4(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    if (input.len < 7 or std.mem.readInt(u32, input[0..4], .little) != 0x184d2204)
        return error.InvalidLz4;
    const flg = input[4];
    if (flg >> 6 != 1) return error.InvalidLz4;
    var pos: usize = 6;
    if (flg & 8 != 0) {
        if (input.len -| pos < 8) return error.InvalidLz4;
        pos += 8;
    }
    if (flg & 1 != 0) {
        if (input.len -| pos < 4) return error.InvalidLz4;
        pos += 4;
    }
    if (pos >= input.len) return error.InvalidLz4;
    pos += 1; // header checksum; intentionally not verified

    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    while (true) {
        const block_size = try readLe32(input, &pos);
        if (block_size == 0) break;
        const uncompressed = block_size & 0x80000000 != 0;
        const len = block_size & 0x7fffffff;
        if (input.len -| pos < len) return error.InvalidLz4;
        if (uncompressed)
            try out.appendSlice(alloc, input[pos .. pos + len])
        else
            try decodeRawLz4(alloc, input[pos .. pos + len], &out);
        pos += len;
        if (flg & 0x10 != 0) {
            if (input.len -| pos < 4) return error.InvalidLz4;
            pos += 4;
        }
    }
    if (flg & 4 != 0) {
        if (input.len -| pos < 4) return error.InvalidLz4;
        pos += 4;
    }
    return out.toOwnedSlice(alloc);
}

test "decompresses gzip" {
    const compressed = [_]u8{
        0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
        0x4b, 0x4c, 0x4a, 0x4e, 0x44, 0x45, 0x00, 0x04, 0xc0, 0x26,
        0xdc, 0x12, 0x00, 0x00, 0x00,
    };
    const result = try decompress(std.testing.allocator, 1, &compressed);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("abcabcabcabcabcabc", result);
}

test "decompresses zstd" {
    const compressed = [_]u8{
        0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x4d, 0x00, 0x00, 0x18, 0x61,
        0x62, 0x63, 0x01, 0x00, 0x76, 0x6e, 0x08, 0xeb, 0xfe, 0x13, 0x27,
    };
    const result = try decompress(std.testing.allocator, 4, &compressed);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("abcabcabcabcabcabc", result);
}

test "decompresses raw and xerial snappy" {
    const raw = [_]u8{ 18, 8, 'a', 'b', 'c', 0x1d, 3, 0x0e, 3, 0 };
    const result = try decompress(std.testing.allocator, 2, &raw);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("abcabcabcabcabcabc", result);

    var xerial: [20 + raw.len]u8 = undefined;
    @memcpy(xerial[0..8], "\x82SNAPPY\x00");
    std.mem.writeInt(u32, xerial[8..12], 1, .big);
    std.mem.writeInt(u32, xerial[12..16], 0, .big);
    std.mem.writeInt(u32, xerial[16..20], raw.len, .big);
    @memcpy(xerial[20 .. 20 + raw.len], &raw);
    const framed = try decompress(std.testing.allocator, 2, &xerial);
    defer std.testing.allocator.free(framed);
    try std.testing.expectEqualStrings("abcabcabcabcabcabc", framed);
}

test "decompresses lz4 frame" {
    const compressed = [_]u8{
        0x04, 0x22, 0x4d, 0x18, 0x60, 0x40, 0x00,
        0x06, 0x00, 0x00, 0x00, 0x3b, 'a',  'b',
        'c',  3,    0,    0x00, 0x00, 0x00, 0x00,
    };
    const result = try decompress(std.testing.allocator, 3, &compressed);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("abcabcabcabcabcabc", result);
}
