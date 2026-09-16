const std = @import("std");

pub const bold = "\x1b[1m";
pub const dim = "\x1b[2m";
pub const red = "\x1b[31m";
pub const yellow = "\x1b[33m";
pub const green = "\x1b[32m";
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
            const word: []const u8 = if (std.mem.startsWith(u8, line[prefix_len..], "kite -c"))
                "kite -c"
            else if (std.mem.startsWith(u8, line[prefix_len..], "kite -i"))
                "kite -i"
            else if (std.mem.startsWith(u8, line[prefix_len..], "kite"))
                "kite"
            else
                "";
            try out.appendSlice(alloc, line[0..prefix_len]);
            if (word.len > 0) {
                try appendStyled(&out, alloc, color.enabled, cyan, word);
                try out.appendSlice(alloc, line[prefix_len + word.len ..]);
            } else {
                try out.appendSlice(alloc, line[prefix_len..]);
            }
        } else {
            try out.appendSlice(alloc, line);
        }
        try out.append(alloc, '\n');
    }
    return out.toOwnedSlice(alloc);
}
