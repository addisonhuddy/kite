const std = @import("std");

pub const Error = error{
    UnsupportedCompression,
    InvalidCompression,
    InvalidSnappy,
    InvalidLz4,
    DecompressedTooLarge,
};

/// A malicious broker could send a tiny batch that expands hugely; cap the
/// decompressed size per batch.
pub const max_decompressed_len: usize = 64 << 20;

pub fn decompress(alloc: std.mem.Allocator, codec: u3, input: []const u8) ![]u8 {
    return decompressWithLimit(alloc, codec, input, max_decompressed_len);
}

fn decompressWithLimit(alloc: std.mem.Allocator, codec: u3, input: []const u8, limit: usize) ![]u8 {
    return switch (codec) {
        0 => alloc.dupe(u8, input),
        1 => streamFlate(alloc, input, limit),
        2 => streamSnappy(alloc, input, limit),
        3 => streamLz4(alloc, input, limit),
        // zstd (4) is deliberately unsupported: its decoder costs ~50 KiB of binary.
        else => error.UnsupportedCompression,
    };
}

/// Fail `append`/`appendSlice` calls that would grow `out` past `limit`.
fn reserve(out: *std.ArrayListUnmanaged(u8), extra: usize, limit: usize) Error!void {
    if (out.items.len +| extra > limit) return error.DecompressedTooLarge;
}

fn streamFlate(alloc: std.mem.Allocator, input: []const u8, limit: usize) ![]u8 {
    var reader: std.Io.Reader = .fixed(input);
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    var flate_buf: [std.compress.flate.max_window_len + 4096]u8 = undefined;
    var d = std.compress.flate.Decompress.init(&reader, .gzip, &flate_buf);
    // Pump in chunks so `out` can never grow past limit+1.
    while (true) {
        const chunk = d.reader.peekGreedy(1) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        const take_n = @min(chunk.len, limit +| 1 -| out.written().len);
        out.writer.writeAll(chunk[0..take_n]) catch return error.OutOfMemory;
        d.reader.toss(take_n);
        if (out.written().len > limit) return error.DecompressedTooLarge;
    }
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

fn decodeRawSnappy(alloc: std.mem.Allocator, input: []const u8, out: *std.ArrayListUnmanaged(u8), limit: usize) !void {
    var pos: usize = 0;
    const start = out.items.len;
    const expected = try readSnappyVarint(input, &pos);
    if (expected > limit) return error.DecompressedTooLarge;
    while (pos < input.len and out.items.len - start < expected) {
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
                try reserve(out, len, limit);
                try out.appendSlice(alloc, input[pos .. pos + len]);
                pos += len;
            },
            1 => {
                if (pos >= input.len) return error.InvalidSnappy;
                const len: usize = 4 + ((tag >> 2) & 7);
                const offset = (@as(usize, tag >> 5) << 8) | input[pos];
                pos += 1;
                try copyMatch(alloc, out, offset, len, start, limit);
            },
            2 => {
                if (input.len -| pos < 2) return error.InvalidSnappy;
                const len: usize = (tag >> 2) + 1;
                const offset = @as(usize, input[pos]) | (@as(usize, input[pos + 1]) << 8);
                pos += 2;
                try copyMatch(alloc, out, offset, len, start, limit);
            },
            3 => {
                if (input.len -| pos < 4) return error.InvalidSnappy;
                const len: usize = (tag >> 2) + 1;
                const offset = @as(usize, input[pos]) |
                    (@as(usize, input[pos + 1]) << 8) |
                    (@as(usize, input[pos + 2]) << 16) |
                    (@as(usize, input[pos + 3]) << 24);
                pos += 4;
                try copyMatch(alloc, out, offset, len, start, limit);
            },
            else => unreachable,
        }
    }
    if (out.items.len - start != expected) return error.InvalidSnappy;
}

fn copyMatch(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    offset: usize,
    len: usize,
    start: usize,
    limit: usize,
) !void {
    if (offset == 0 or offset > out.items.len - start) return error.InvalidSnappy;
    try reserve(out, len, limit);
    for (0..len) |_| try out.append(alloc, out.items[out.items.len - offset]);
}

fn streamSnappy(alloc: std.mem.Allocator, input: []const u8, limit: usize) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    const magic = "\x82SNAPPY\x00";
    if (!std.mem.startsWith(u8, input, magic)) {
        try decodeRawSnappy(alloc, input, &out, limit);
        return out.toOwnedSlice(alloc);
    }
    var pos = magic.len;
    if (input.len -| pos < 8) return error.InvalidSnappy;
    _ = try readBe32(input, &pos);
    _ = try readBe32(input, &pos);
    while (pos < input.len) {
        const block_len = try readBe32(input, &pos);
        if (input.len -| pos < block_len) return error.InvalidSnappy;
        try decodeRawSnappy(alloc, input[pos .. pos + block_len], &out, limit);
        pos += block_len;
    }
    return out.toOwnedSlice(alloc);
}

fn decodeRawLz4(alloc: std.mem.Allocator, input: []const u8, out: *std.ArrayListUnmanaged(u8), limit: usize) !void {
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
        try reserve(out, literal_len, limit);
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
        try copyLz4Match(alloc, out, offset, match_len, limit);
    }
}

fn copyLz4Match(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    offset: usize,
    len: usize,
    limit: usize,
) !void {
    if (offset == 0 or offset > out.items.len) return error.InvalidLz4;
    try reserve(out, len, limit);
    for (0..len) |_| try out.append(alloc, out.items[out.items.len - offset]);
}

fn streamLz4(alloc: std.mem.Allocator, input: []const u8, limit: usize) ![]u8 {
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
        if (uncompressed) {
            try reserve(&out, len, limit);
            try out.appendSlice(alloc, input[pos .. pos + len]);
        } else {
            try decodeRawLz4(alloc, input[pos .. pos + len], &out, limit);
        }
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

    var multi: [16 + 2 * (4 + raw.len)]u8 = undefined;
    @memcpy(multi[0..8], "\x82SNAPPY\x00");
    std.mem.writeInt(u32, multi[8..12], 1, .big);
    std.mem.writeInt(u32, multi[12..16], 0, .big);
    std.mem.writeInt(u32, multi[16..20], raw.len, .big);
    @memcpy(multi[20 .. 20 + raw.len], &raw);
    const second_len = 20 + raw.len;
    std.mem.writeInt(u32, multi[second_len .. second_len + 4], raw.len, .big);
    @memcpy(multi[second_len + 4 .. second_len + 4 + raw.len], &raw);
    const multi_result = try decompress(std.testing.allocator, 2, &multi);
    defer std.testing.allocator.free(multi_result);
    try std.testing.expectEqualStrings(
        "abcabcabcabcabcabcabcabcabcabcabcabc",
        multi_result,
    );
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

test "decompression limit is enforced per codec" {
    const a = std.testing.allocator;
    // gzip: existing vector expands to 18 bytes; limit of 4 trips the cap.
    const gz = [_]u8{
        0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
        0x4b, 0x4c, 0x4a, 0x4e, 0x44, 0x45, 0x00, 0x04, 0xc0, 0x26,
        0xdc, 0x12, 0x00, 0x00, 0x00,
    };
    try std.testing.expectError(
        error.DecompressedTooLarge,
        decompressWithLimit(a, 1, &gz, 4),
    );
    // snappy: declared length above the limit fails before decoding.
    const snappy_hdr = [_]u8{5}; // varint: declared output length 5
    try std.testing.expectError(
        error.DecompressedTooLarge,
        decompressWithLimit(a, 2, &snappy_hdr, 4),
    );
    // snappy literal copy that would exceed the limit also fails.
    const raw = [_]u8{ 18, 8, 'a', 'b', 'c', 0x1d, 3, 0x0e, 3, 0 };
    try std.testing.expectError(
        error.DecompressedTooLarge,
        decompressWithLimit(a, 2, &raw, 4),
    );
    // lz4: same existing frame expands to 18 bytes.
    const lz4 = [_]u8{
        0x04, 0x22, 0x4d, 0x18, 0x60, 0x40, 0x00,
        0x06, 0x00, 0x00, 0x00, 0x3b, 'a',  'b',
        'c',  3,    0,    0x00, 0x00, 0x00, 0x00,
    };
    try std.testing.expectError(
        error.DecompressedTooLarge,
        decompressWithLimit(a, 3, &lz4, 4),
    );
    // and a large enough limit still succeeds.
    const ok = try decompressWithLimit(a, 3, &lz4, 64);
    defer a.free(ok);
    try std.testing.expectEqualStrings("abcabcabcabcabcabc", ok);
}
