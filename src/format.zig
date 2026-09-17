const std = @import("std");

/// Record shape shared by produce (input) and consume (output).
/// `auto` keeps the historical TAB-sniffing behaviour.
pub const Format = enum { auto, value, tsv, json, csv };

/// Parse a `--format` value. `auto` is the default and cannot be spelled.
pub fn parse(s: []const u8) ?Format {
    if (std.mem.eql(u8, s, "value")) return .value;
    if (std.mem.eql(u8, s, "tsv")) return .tsv;
    if (std.mem.eql(u8, s, "json")) return .json;
    if (std.mem.eql(u8, s, "csv")) return .csv;
    return null;
}
