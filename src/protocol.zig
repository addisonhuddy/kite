//! Kafka wire protocol: KIP-482 flexible-version encoding primitives,
//! request/response framing, and record batch v2 (magic 2) encoding.

const std = @import("std");
const builtin = @import("builtin");
const decompress = @import("decompress.zig");

pub const api_key = struct {
    pub const produce: i16 = 0;
    pub const fetch: i16 = 1;
    pub const list_offsets: i16 = 2;
    pub const metadata: i16 = 3;
    pub const api_versions: i16 = 18;
    pub const create_topics: i16 = 19;
    pub const sasl_handshake: i16 = 17;
    pub const init_producer_id: i16 = 22;
    pub const sasl_authenticate: i16 = 36;
};

/// Request versions this client pins, all flexible (Kafka 4.x baseline).
pub const version = struct {
    pub const api_versions: i16 = 3;
    pub const metadata: i16 = 12;
    pub const produce: i16 = 11;
    pub const fetch: i16 = 12;
    pub const list_offsets: i16 = 8;
    pub const sasl_handshake: i16 = 1;
    pub const sasl_authenticate: i16 = 2;
    pub const init_producer_id: i16 = 4;
    pub const create_topics: i16 = 7;
};

pub const ErrorCode = enum(i16) {
    none = 0,
    unknown_server_error = -1,
    offset_out_of_range = 1,
    unknown_topic_or_partition = 3,
    leader_not_available = 5,
    not_leader_or_follower = 6,
    request_timed_out = 7,
    broker_not_available = 8,
    replica_not_available = 9,
    message_too_large = 10,
    stale_metadata = 11,
    network_exception = 13,
    group_load_in_progress = 14,
    not_coordinator = 16,
    not_enough_replicas = 19,
    not_enough_replicas_after_append = 20,
    out_of_order_sequence_number = 45,
    duplicate_sequence_number = 46,
    invalid_producer_epoch = 47,
    invalid_producer_id_mapping = 49,
    unknown_producer_id = 59,
    invalid_required_ack = 21,
    topic_authorization_failed = 29,
    cluster_authorization_failed = 31,
    unsupported_sasl_mechanism = 33,
    illegal_sasl_state = 34,
    unsupported_version = 35,
    topic_already_exists = 36,
    invalid_partitions = 37,
    invalid_replication_factor = 38,
    not_controller = 41,
    sasl_authentication_failed = 58,
    fenced_leader_epoch = 74,
    unknown_leader_epoch = 75,
    unsupported_compression_type = 76,
    _,

    pub fn name(self: ErrorCode) []const u8 {
        return switch (self) {
            .none => "NONE",
            .unknown_server_error => "UNKNOWN_SERVER_ERROR",
            .offset_out_of_range => "OFFSET_OUT_OF_RANGE",
            .unknown_topic_or_partition => "UNKNOWN_TOPIC_OR_PARTITION",
            .leader_not_available => "LEADER_NOT_AVAILABLE",
            .not_leader_or_follower => "NOT_LEADER_OR_FOLLOWER",
            .request_timed_out => "REQUEST_TIMED_OUT",
            .broker_not_available => "BROKER_NOT_AVAILABLE",
            .replica_not_available => "REPLICA_NOT_AVAILABLE",
            .message_too_large => "MESSAGE_TOO_LARGE",
            .stale_metadata => "STALE_METADATA",
            .network_exception => "NETWORK_EXCEPTION",
            .group_load_in_progress => "GROUP_LOAD_IN_PROGRESS",
            .not_coordinator => "NOT_COORDINATOR",
            .not_enough_replicas => "NOT_ENOUGH_REPLICAS",
            .not_enough_replicas_after_append => "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
            .out_of_order_sequence_number => "OUT_OF_ORDER_SEQUENCE_NUMBER",
            .duplicate_sequence_number => "DUPLICATE_SEQUENCE_NUMBER",
            .invalid_producer_epoch => "INVALID_PRODUCER_EPOCH",
            .invalid_producer_id_mapping => "INVALID_PRODUCER_ID_MAPPING",
            .unknown_producer_id => "UNKNOWN_PRODUCER_ID",
            .invalid_required_ack => "INVALID_REQUIRED_ACK",
            .topic_authorization_failed => "TOPIC_AUTHORIZATION_FAILED",
            .cluster_authorization_failed => "CLUSTER_AUTHORIZATION_FAILED",
            .unsupported_sasl_mechanism => "UNSUPPORTED_SASL_MECHANISM",
            .illegal_sasl_state => "ILLEGAL_SASL_STATE",
            .unsupported_version => "UNSUPPORTED_VERSION",
            .topic_already_exists => "TOPIC_ALREADY_EXISTS",
            .invalid_partitions => "INVALID_PARTITIONS",
            .invalid_replication_factor => "INVALID_REPLICATION_FACTOR",
            .not_controller => "NOT_CONTROLLER",
            .sasl_authentication_failed => "SASL_AUTHENTICATION_FAILED",
            .fenced_leader_epoch => "FENCED_LEADER_EPOCH",
            .unknown_leader_epoch => "UNKNOWN_LEADER_EPOCH",
            .unsupported_compression_type => "UNSUPPORTED_COMPRESSION_TYPE",
            _ => "UNKNOWN_ERROR",
        };
    }

    /// Produce/broker errors worth retrying after a metadata refresh.
    pub fn retriable(self: ErrorCode) bool {
        return switch (self) {
            .leader_not_available,
            .not_leader_or_follower,
            .request_timed_out,
            .broker_not_available,
            .network_exception,
            .not_enough_replicas,
            .not_enough_replicas_after_append,
            .unknown_topic_or_partition,
            .fenced_leader_epoch,
            .unknown_leader_epoch,
            .stale_metadata,
            .group_load_in_progress,
            .not_coordinator,
            => true,
            else => false,
        };
    }
};

pub const ProtoError = error{
    Truncated,
    Overflow,
    NegativeLength,
    OutOfMemory,
    WriteFailed,
};

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

pub const Encoder = struct {
    aw: std.Io.Writer.Allocating,

    pub fn init(alloc: std.mem.Allocator) Encoder {
        return .{ .aw = std.Io.Writer.Allocating.init(alloc) };
    }

    pub fn deinit(e: *Encoder) void {
        e.aw.deinit();
    }

    pub fn written(e: *Encoder) []u8 {
        return e.aw.written();
    }

    pub fn reset(e: *Encoder) void {
        e.aw.writer.end = 0;
    }

    fn w(e: *Encoder) *std.Io.Writer {
        return &e.aw.writer;
    }

    pub fn raw(e: *Encoder, bytes: []const u8) ProtoError!void {
        e.w().writeAll(bytes) catch return error.OutOfMemory;
    }

    pub fn u8v(e: *Encoder, v: u8) ProtoError!void {
        e.w().writeByte(v) catch return error.OutOfMemory;
    }

    pub fn boolean(e: *Encoder, v: bool) ProtoError!void {
        try e.u8v(if (v) 1 else 0);
    }

    pub fn i8v(e: *Encoder, v: i8) ProtoError!void {
        try e.u8v(@bitCast(v));
    }

    pub fn i16v(e: *Encoder, v: i16) ProtoError!void {
        var b: [2]u8 = undefined;
        std.mem.writeInt(i16, &b, v, .big);
        try e.raw(&b);
    }

    pub fn i32v(e: *Encoder, v: i32) ProtoError!void {
        var b: [4]u8 = undefined;
        std.mem.writeInt(i32, &b, v, .big);
        try e.raw(&b);
    }

    pub fn i64v(e: *Encoder, v: i64) ProtoError!void {
        var b: [8]u8 = undefined;
        std.mem.writeInt(i64, &b, v, .big);
        try e.raw(&b);
    }

    pub fn u32v(e: *Encoder, v: u32) ProtoError!void {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, v, .big);
        try e.raw(&b);
    }

    pub fn uvarint(e: *Encoder, v: u64) ProtoError!void {
        var b: [10]u8 = undefined;
        var n = v;
        var i: usize = 0;
        while (true) {
            b[i] = @intCast(n & 0x7f);
            n >>= 7;
            if (n == 0) break;
            b[i] |= 0x80;
            i += 1;
        }
        try e.raw(b[0 .. i + 1]);
    }

    pub fn varint(e: *Encoder, v: i32) ProtoError!void {
        try e.uvarint(@as(u32, @bitCast(v)) << 1 ^ @as(u32, @bitCast(v >> 31)));
    }

    pub fn varlong(e: *Encoder, v: i64) ProtoError!void {
        try e.uvarint(@as(u64, @bitCast(v)) << 1 ^ @as(u64, @bitCast(v >> 63)));
    }

    pub fn compactString(e: *Encoder, s: ?[]const u8) ProtoError!void {
        if (s) |str| {
            try e.uvarint(str.len + 1);
            try e.raw(str);
        } else {
            try e.uvarint(0);
        }
    }

    /// Same on the wire as compactString; kept separate for opaque blobs like
    /// record batches and SASL tokens.
    pub fn compactBytes(e: *Encoder, s: ?[]const u8) ProtoError!void {
        try e.compactString(s);
    }

    /// Element count for a non-nullable COMPACT_ARRAY.
    pub fn compactArrayLen(e: *Encoder, n: usize) ProtoError!void {
        try e.uvarint(n + 1);
    }

    pub fn tagBuffer(e: *Encoder) ProtoError!void {
        try e.uvarint(0);
    }

    /// Legacy (non-compact) NULLABLE_STRING for request header client_id.
    pub fn nullableString(e: *Encoder, s: ?[]const u8) ProtoError!void {
        if (s) |str| {
            try e.i16v(@intCast(str.len));
            try e.raw(str);
        } else {
            try e.i16v(-1);
        }
    }

    /// Legacy (non-compact) non-nullable STRING.
    pub fn string(e: *Encoder, s: []const u8) ProtoError!void {
        try e.i16v(@intCast(s.len));
        try e.raw(s);
    }
};

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

pub const Decoder = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn init(data: []const u8) Decoder {
        return .{ .data = data };
    }

    fn need(d: *Decoder, n: usize) ProtoError!void {
        if (d.pos + n > d.data.len) return error.Truncated;
    }

    pub fn u8v(d: *Decoder) ProtoError!u8 {
        try d.need(1);
        defer d.pos += 1;
        return d.data[d.pos];
    }

    pub fn boolean(d: *Decoder) ProtoError!bool {
        return (try d.u8v()) != 0;
    }

    pub fn i8v(d: *Decoder) ProtoError!i8 {
        return @bitCast(try d.u8v());
    }

    pub fn i16v(d: *Decoder) ProtoError!i16 {
        try d.need(2);
        defer d.pos += 2;
        return std.mem.readInt(i16, d.data[d.pos..][0..2], .big);
    }

    pub fn i32v(d: *Decoder) ProtoError!i32 {
        try d.need(4);
        defer d.pos += 4;
        return std.mem.readInt(i32, d.data[d.pos..][0..4], .big);
    }

    pub fn u32v(d: *Decoder) ProtoError!u32 {
        try d.need(4);
        defer d.pos += 4;
        return std.mem.readInt(u32, d.data[d.pos..][0..4], .big);
    }

    pub fn i64v(d: *Decoder) ProtoError!i64 {
        try d.need(8);
        defer d.pos += 8;
        return std.mem.readInt(i64, d.data[d.pos..][0..8], .big);
    }

    pub fn uvarint(d: *Decoder) ProtoError!u64 {
        var result: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            const b = try d.u8v();
            result |= @as(u64, b & 0x7f) << shift;
            if (b & 0x80 == 0) return result;
            shift += 7;
            if (shift > 63) return error.Overflow;
        }
    }

    pub fn varint(d: *Decoder) ProtoError!i32 {
        const v = try d.uvarint();
        const u: u32 = @truncate(v);
        return @bitCast((u >> 1) ^ (~(u & 1) +% 1));
    }

    pub fn varlong(d: *Decoder) ProtoError!i64 {
        const v = try d.uvarint();
        return @bitCast((v >> 1) ^ (~(v & 1) +% 1));
    }

    pub fn compactBytes(d: *Decoder) ProtoError!?[]const u8 {
        const n = try d.uvarint();
        if (n == 0) return null;
        const len = n - 1;
        if (len > std.math.maxInt(usize)) return error.Overflow;
        const l: usize = @intCast(len);
        try d.need(l);
        defer d.pos += l;
        return d.data[d.pos..][0..l];
    }

    pub fn compactString(d: *Decoder) ProtoError!?[]const u8 {
        return d.compactBytes();
    }

    /// Returns -1 for a null array.
    pub fn compactArrayLen(d: *Decoder) ProtoError!i64 {
        const n = try d.uvarint();
        if (n == 0) return -1;
        return @intCast(n - 1);
    }

    pub fn tagBuffer(d: *Decoder) ProtoError!void {
        const count = try d.uvarint();
        var i: u64 = 0;
        while (i < count) : (i += 1) {
            _ = try d.uvarint(); // tag
            const size = try d.uvarint();
            if (size > std.math.maxInt(usize)) return error.Overflow;
            try d.need(@intCast(size));
            d.pos += @intCast(size);
        }
    }

    /// Skip n raw bytes.
    pub fn skip(d: *Decoder, n: usize) ProtoError!void {
        try d.need(n);
        d.pos += n;
    }

    /// Skip a COMPACT_ARRAY of fixed 4-byte elements (replica/isr node lists).
    pub fn skipCompactI32Array(d: *Decoder) ProtoError!void {
        const n = try d.compactArrayLen();
        if (n < 0) return;
        try d.need(@as(usize, @intCast(n)) * 4);
        d.pos += @as(usize, @intCast(n)) * 4;
    }
};

// ---------------------------------------------------------------------------
// Request framing
// ---------------------------------------------------------------------------

var correlation_id: i32 = 0;

/// Request header v2: api key/version, correlation id, client id, tags.
/// The tag buffer is written only when the request version is flexible.
pub fn encodeRequestHeader(
    e: *Encoder,
    key: i16,
    ver: i16,
    flexible: bool,
    client_id: ?[]const u8,
) ProtoError!void {
    correlation_id +%= 1;
    try e.i16v(key);
    try e.i16v(ver);
    try e.i32v(correlation_id);
    try e.nullableString(client_id);
    if (flexible) try e.tagBuffer();
}

/// Writes the request header + body into `e` via `body_fn`.
pub fn encodeRequest(
    e: *Encoder,
    key: i16,
    ver: i16,
    flexible: bool,
    client_id: ?[]const u8,
    ctx: anytype,
    comptime body_fn: fn (*Encoder, @TypeOf(ctx)) ProtoError!void,
) ProtoError!void {
    try encodeRequestHeader(e, key, ver, flexible, client_id);
    try body_fn(e, ctx);
}

pub fn lastCorrelationId() i32 {
    return correlation_id;
}

/// CRC-32C (Castagnoli) over `data`: hardware instructions when the target
/// CPU has them (x86 SSE4.2 crc32, Armv8 crc32c*), else the std table impl.
pub fn crc32c(data: []const u8) u32 {
    if (comptime builtin.cpu.arch == .x86_64 and
        std.Target.x86.featureSetHas(builtin.cpu.features, .sse4_2))
    {
        return crc32cX86(data);
    }
    if (comptime builtin.cpu.arch == .aarch64 and
        std.Target.aarch64.featureSetHas(builtin.cpu.features, .crc))
    {
        return crc32cArm(data);
    }
    return std.hash.crc.Crc32Iscsi.hash(data);
}

fn crc32cX86(data: []const u8) u32 {
    var crc: u64 = 0xffffffff;
    var i: usize = 0;
    while (i + 8 <= data.len) : (i += 8) {
        const v = std.mem.readInt(u64, data[i..][0..8], .little);
        asm ("crc32q %[v], %[c]"
            : [c] "+r" (crc),
            : [v] "r" (v),
        );
    }
    var crc32: u32 = @truncate(crc);
    if (i + 4 <= data.len) {
        const v = std.mem.readInt(u32, data[i..][0..4], .little);
        asm ("crc32l %[v], %[c]"
            : [c] "+r" (crc32),
            : [v] "r" (v),
        );
        i += 4;
    }
    if (i + 2 <= data.len) {
        const v = std.mem.readInt(u16, data[i..][0..2], .little);
        asm ("crc32w %[v], %[c]"
            : [c] "+r" (crc32),
            : [v] "r" (v),
        );
        i += 2;
    }
    while (i < data.len) : (i += 1) {
        asm ("crc32b %[v], %[c]"
            : [c] "+r" (crc32),
            : [v] "r" (data[i]),
        );
    }
    return ~crc32;
}

fn crc32cArm(data: []const u8) u32 {
    var crc: u32 = 0xffffffff;
    var i: usize = 0;
    while (i + 8 <= data.len) : (i += 8) {
        const v = std.mem.readInt(u64, data[i..][0..8], .little);
        asm ("crc32cx %[c:w], %[c:w], %[v:x]"
            : [c] "+r" (crc),
            : [v] "r" (v),
        );
    }
    if (i + 4 <= data.len) {
        const v = std.mem.readInt(u32, data[i..][0..4], .little);
        asm ("crc32cw %[c:w], %[c:w], %[v:w]"
            : [c] "+r" (crc),
            : [v] "r" (v),
        );
        i += 4;
    }
    if (i + 2 <= data.len) {
        const v = std.mem.readInt(u16, data[i..][0..2], .little);
        asm ("crc32ch %[c:w], %[c:w], %[v:w]"
            : [c] "+r" (crc),
            : [v] "r" (v),
        );
        i += 2;
    }
    while (i < data.len) : (i += 1) {
        asm ("crc32cb %[c:w], %[c:w], %[v:w]"
            : [c] "+r" (crc),
            : [v] "r" (data[i]),
        );
    }
    return ~crc;
}

// ---------------------------------------------------------------------------
// Record batch v2 (magic 2)
// ---------------------------------------------------------------------------

pub const Header = struct {
    key: []const u8,
    value: ?[]const u8,
};

/// One record in a batch. `key` null = unkeyed; `headers` may be empty.
pub const Record = struct {
    key: ?[]const u8 = null,
    value: []const u8,
    headers: []const Header = &.{},
};

/// Walk Kafka record batch v2 data from a Fetch partition response. The
/// callback is invoked as records are decoded, so its slices remain valid only
/// for the duration of the call.
pub fn decodeBatches(
    alloc: std.mem.Allocator,
    records: []const u8,
    ctx: anytype,
    comptime on_record: fn (@TypeOf(ctx), offset: i64, timestamp_ms: i64, rec: Record) anyerror!void,
) !?i64 {
    var pos: usize = 0;
    var next_offset: ?i64 = null;
    while (records.len -| pos >= 12) {
        const base_offset = std.mem.readInt(i64, records[pos..][0..8], .big);
        const batch_length = std.mem.readInt(i32, records[pos + 8 ..][0..4], .big);
        pos += 12;
        if (batch_length < 0 or @as(usize, @intCast(batch_length)) > records.len -| pos) break;
        const batch_end = pos + @as(usize, @intCast(batch_length));
        if (batch_length < 49) return error.Truncated;

        _ = std.mem.readInt(i32, records[pos..][0..4], .big); // leader epoch
        const magic = records[pos + 4];
        if (magic != 2) return error.UnsupportedMagic;
        const stored_crc = std.mem.readInt(u32, records[pos + 5 ..][0..4], .big);
        const crc_start = pos + 9;
        if (crc32c(records[crc_start..batch_end]) != stored_crc) return error.BadCrc;

        var d = Decoder.init(records[pos..batch_end]);
        _ = try d.i32v(); // partition leader epoch
        _ = try d.u8v(); // magic
        _ = try d.u32v();
        const attributes = try d.i16v();
        const last_offset_delta = try d.i32v();
        const base_timestamp = try d.i64v();
        _ = try d.i64v(); // max timestamp
        _ = try d.i64v(); // producer id
        _ = try d.i16v(); // producer epoch
        _ = try d.i32v(); // base sequence
        const records_count = try d.i32v();
        if (records_count < 0) return error.Truncated;
        const compressed_records = records[pos + d.pos .. batch_end];

        if (attributes & (@as(i16, 1) << 5) == 0) {
            const codec: u3 = @intCast(@as(u16, @bitCast(attributes)) & 7);
            const decoded = try decompress.decompress(alloc, codec, compressed_records);
            defer alloc.free(decoded);
            var rpos: usize = 0;
            while (rpos < decoded.len) {
                var rd = Decoder.init(decoded[rpos..]);
                const record_len = try rd.varint();
                if (record_len < 0 or rd.pos > decoded.len -| rpos or
                    @as(usize, @intCast(record_len)) > decoded.len - rpos - rd.pos)
                    return error.Truncated;
                const record_end = rpos + rd.pos + @as(usize, @intCast(record_len));
                if (record_end > decoded.len) return error.Truncated;
                const body = decoded[rpos + rd.pos .. record_end];
                var b = Decoder.init(body);
                _ = try b.i8v();
                const timestamp_delta = try b.varlong();
                const offset_delta = try b.varint();
                const key_len = try b.varint();
                const key: ?[]const u8 = if (key_len < 0)
                    null
                else blk: {
                    const n: usize = @intCast(key_len);
                    try b.skip(n);
                    break :blk body[b.pos - n .. b.pos];
                };
                const value_len = try b.varint();
                const value: []const u8 = if (value_len < 0)
                    ""
                else blk: {
                    const n: usize = @intCast(value_len);
                    try b.skip(n);
                    break :blk body[b.pos - n .. b.pos];
                };
                const header_count = try b.varint();
                if (header_count < 0) return error.Truncated;
                var headers: std.ArrayListUnmanaged(Header) = .empty;
                defer headers.deinit(alloc);
                for (0..@as(usize, @intCast(header_count))) |_| {
                    const hk_len = try b.varint();
                    if (hk_len < 0) return error.Truncated;
                    const hk_n: usize = @intCast(hk_len);
                    const hk_start = b.pos;
                    try b.skip(hk_n);
                    const hk = body[hk_start..b.pos];
                    const hv_len = try b.varint();
                    const hv: ?[]const u8 = if (hv_len < 0)
                        null
                    else blk: {
                        const hv_n: usize = @intCast(hv_len);
                        try b.skip(hv_n);
                        break :blk body[b.pos - hv_n .. b.pos];
                    };
                    try headers.append(alloc, .{ .key = hk, .value = hv });
                }
                try on_record(ctx, base_offset + offset_delta, base_timestamp + timestamp_delta, .{
                    .key = key,
                    .value = value,
                    .headers = headers.items,
                });
                rpos = record_end;
            }
        }
        const batch_next = base_offset + @as(i64, last_offset_delta) + 1;
        if (next_offset == null or batch_next > next_offset.?) next_offset = batch_next;
        pos = batch_end;
    }
    return next_offset;
}

/// Records share the batch base timestamp (delta 0 each). For idempotent
/// produce pass the InitProducerId-issued `producer_id`/`producer_epoch` and
/// the partition's next `base_sequence`; all -1 for a plain (non-idempotent)
/// batch.
pub fn encodeRecordBatch(
    e: *Encoder,
    records: []const Record,
    base_timestamp_ms: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) ProtoError!void {
    var body = Encoder.init(e.aw.allocator);
    defer body.deinit();

    try body.i16v(0); // attributes: no compression, ts=CreateTime
    try body.i32v(@intCast(records.len - 1)); // last offset delta
    try body.i64v(base_timestamp_ms);
    try body.i64v(base_timestamp_ms); // max timestamp
    try body.i64v(producer_id);
    try body.i16v(producer_epoch);
    try body.i32v(base_sequence);
    try body.i32v(@intCast(records.len)); // record count
    for (records, 0..) |rec, idx| {
        var r = Encoder.init(e.aw.allocator);
        defer r.deinit();
        try r.u8v(0); // record attributes
        try r.varlong(0); // timestamp delta
        try r.varint(@intCast(idx)); // offset delta
        if (rec.key) |k| {
            try r.varint(@intCast(k.len));
            try r.raw(k);
        } else {
            try r.varint(-1); // key: null
        }
        try r.varint(@intCast(rec.value.len));
        try r.raw(rec.value);
        try r.varint(@intCast(rec.headers.len));
        for (rec.headers) |h| {
            try r.varint(@intCast(h.key.len));
            try r.raw(h.key);
            if (h.value) |v| {
                try r.varint(@intCast(v.len));
                try r.raw(v);
            } else {
                try r.varint(-1);
            }
        }
        try body.varint(@intCast(r.written().len));
        try body.raw(r.written());
    }

    const body_bytes = body.written();
    const crc = crc32c(body_bytes);

    try e.i64v(0); // base offset (broker assigns)
    try e.i32v(@intCast(4 + 1 + 4 + body_bytes.len)); // batchLength
    try e.i32v(-1); // partition leader epoch
    try e.u8v(2); // magic
    try e.u32v(crc);
    try e.raw(body_bytes);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "uvarint/varint round trip" {
    var e = Encoder.init(std.testing.allocator);
    defer e.deinit();
    try e.uvarint(0);
    try e.uvarint(1);
    try e.uvarint(300);
    try e.uvarint(16384);
    try e.uvarint(std.math.maxInt(u64));
    try e.varint(0);
    try e.varint(-1);
    try e.varint(63);
    try e.varint(-64);
    try e.varint(std.math.minInt(i32));

    var d = Decoder.init(e.written());
    try std.testing.expectEqual(@as(u64, 0), try d.uvarint());
    try std.testing.expectEqual(@as(u64, 1), try d.uvarint());
    try std.testing.expectEqual(@as(u64, 300), try d.uvarint());
    try std.testing.expectEqual(@as(u64, 16384), try d.uvarint());
    try std.testing.expectEqual(std.math.maxInt(u64), try d.uvarint());
    try std.testing.expectEqual(@as(i32, 0), try d.varint());
    try std.testing.expectEqual(@as(i32, -1), try d.varint());
    try std.testing.expectEqual(@as(i32, 63), try d.varint());
    try std.testing.expectEqual(@as(i32, -64), try d.varint());
    try std.testing.expectEqual(std.math.minInt(i32), try d.varint());
}

const DecodeTestSink = struct {
    count: usize = 0,
    offset: i64 = 0,
    timestamp: i64 = 0,
    record: ?Record = null,
};

fn decodeTestRecord(
    sink: *DecodeTestSink,
    offset: i64,
    timestamp_ms: i64,
    rec: Record,
) !void {
    sink.count += 1;
    sink.offset = offset;
    sink.timestamp = timestamp_ms;
    sink.record = rec;
}

test "decodes record batch v2" {
    var e = Encoder.init(std.testing.allocator);
    defer e.deinit();
    const headers = [_]Header{.{ .key = "source", .value = "test" }};
    const input = [_]Record{
        .{ .key = "key", .value = "value", .headers = &headers },
        .{ .value = "empty-key" },
    };
    try encodeRecordBatch(&e, &input, 1000, -1, -1, -1);

    var sink = DecodeTestSink{};
    try std.testing.expectEqual(@as(?i64, 2), try decodeBatches(std.testing.allocator, e.written(), &sink, decodeTestRecord));
    try std.testing.expectEqual(@as(usize, 2), sink.count);
    try std.testing.expectEqual(@as(i64, 1), sink.offset);
    try std.testing.expectEqual(@as(i64, 1000), sink.timestamp);
}

test "skips control batches and returns next complete offset" {
    var control_encoder = Encoder.init(std.testing.allocator);
    defer control_encoder.deinit();
    const control_input = [_]Record{.{ .value = "control" }};
    try encodeRecordBatch(&control_encoder, &control_input, 1000, -1, -1, -1);
    const control = try std.testing.allocator.dupe(u8, control_encoder.written());
    defer std.testing.allocator.free(control);
    std.mem.writeInt(i16, control[21..23], 1 << 5, .big);
    std.mem.writeInt(u32, control[17..21], crc32c(control[21..]), .big);

    var normal_encoder = Encoder.init(std.testing.allocator);
    defer normal_encoder.deinit();
    const normal_input = [_]Record{.{ .value = "normal" }};
    try encodeRecordBatch(&normal_encoder, &normal_input, 2000, -1, -1, -1);
    const normal = try std.testing.allocator.dupe(u8, normal_encoder.written());
    defer std.testing.allocator.free(normal);
    std.mem.writeInt(i64, normal[0..8], 1, .big);

    var records: std.ArrayListUnmanaged(u8) = .empty;
    defer records.deinit(std.testing.allocator);
    try records.appendSlice(std.testing.allocator, control);
    try records.appendSlice(std.testing.allocator, normal);

    var sink = DecodeTestSink{};
    try std.testing.expectEqual(
        @as(?i64, 2),
        try decodeBatches(std.testing.allocator, records.items, &sink, decodeTestRecord),
    );
    try std.testing.expectEqual(@as(usize, 1), sink.count);
    try std.testing.expectEqual(@as(i64, 1), sink.offset);
}

test "varint byte-exact fixtures" {
    var e = Encoder.init(std.testing.allocator);
    defer e.deinit();
    try e.uvarint(300); // protocol docs example
    try std.testing.expectEqualSlices(u8, &.{ 0xAC, 0x02 }, e.written());
    e.reset();
    try e.varint(-1);
    try std.testing.expectEqualSlices(u8, &.{0x01}, e.written());
    e.reset();
    try e.varint(1);
    try std.testing.expectEqualSlices(u8, &.{0x02}, e.written());
}

test "compact string round trip" {
    var e = Encoder.init(std.testing.allocator);
    defer e.deinit();
    try e.compactString("kite");
    try e.compactString(null);
    try e.compactString("");
    try e.compactArrayLen(3);

    var d = Decoder.init(e.written());
    try std.testing.expectEqualStrings("kite", (try d.compactString()).?);
    try std.testing.expect((try d.compactString()) == null);
    try std.testing.expectEqualStrings("", (try d.compactString()).?);
    try std.testing.expectEqual(@as(i64, 3), try d.compactArrayLen());
}

test "tagBuffer skip" {
    var e = Encoder.init(std.testing.allocator);
    defer e.deinit();
    try e.tagBuffer();
    try e.i16v(42);
    var d = Decoder.init(e.written());
    try d.tagBuffer();
    try std.testing.expectEqual(@as(i16, 42), try d.i16v());
}

test "record batch smoke" {
    var e = Encoder.init(std.testing.allocator);
    defer e.deinit();
    const recs = [_]Record{ .{ .value = "a" }, .{ .value = "b" }, .{ .value = "c" } };
    try encodeRecordBatch(&e, &recs, 1700000000000, -1, -1, -1);
    const batch = e.written();
    var d = Decoder.init(batch);
    try std.testing.expectEqual(@as(i64, 0), try d.i64v());
    const blen = try d.i32v();
    try std.testing.expectEqual(@as(i32, @intCast(batch.len - 12)), blen);
    try std.testing.expectEqual(@as(i32, -1), try d.i32v());
    try std.testing.expectEqual(@as(u8, 2), try d.u8v());
    const crc = try d.i32v();
    const crc_pos = d.pos;
    const computed = std.hash.crc.Crc32Iscsi.hash(batch[crc_pos..]);
    try std.testing.expectEqual(@as(u32, @bitCast(crc)), computed);
}
