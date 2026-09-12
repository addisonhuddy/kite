const std = @import("std");

pub const SecurityProtocol = enum { plaintext, ssl, sasl_ssl, sasl_plaintext };
pub const SaslMechanism = enum { plain, scram_sha_256, scram_sha_512 };

pub const Config = struct {
    bootstrap_servers: [][]const u8,
    security_protocol: SecurityProtocol = .plaintext,
    sasl_mechanism: ?SaslMechanism = null,
    sasl_username: ?[]const u8 = null,
    sasl_password: ?[]const u8 = null,
    ssl_truststore_location: ?[]const u8 = null,
    linger_ms: u64 = 50,
    batch_size: usize = 1 << 20,
    /// Idempotent produce: InitProducerId handshake + per-partition sequence
    /// numbers so broker-side retries/dedup can never duplicate records.
    enable_idempotence: bool = true,
    /// Verbose diagnostics on stderr (-v / --verbose flag).
    verbose: bool = false,

    pub fn needsSasl(self: *const Config) bool {
        return self.security_protocol == .sasl_ssl or self.security_protocol == .sasl_plaintext;
    }

    pub fn needsTls(self: *const Config) bool {
        return self.security_protocol == .ssl or self.security_protocol == .sasl_ssl;
    }
};

pub const LoadError = error{
    ConfigNotFound,
    MissingBootstrapServers,
    InvalidSecurityProtocol,
    InvalidSaslMechanism,
    MissingSaslMechanism,
    MissingSaslCredentials,
    OutOfMemory,
};

fn warn(comptime fmt: []const u8, args: anytype) void {
    var buf: [512]u8 = undefined;
    var w = std.fs.File.stderr().writer(&buf);
    w.interface.print("kannon: warning: " ++ fmt ++ "\n", args) catch {};
    w.interface.flush() catch {};
}

const Props = std.StringHashMap([]const u8);

/// Parse Java-style `key=value` content into a map. Comments (`#`, `!`) and
/// blank lines are ignored; whitespace around keys and values is trimmed;
/// later duplicate keys win; lines without `=` are skipped with a warning.
pub fn parse(alloc: std.mem.Allocator, text: []const u8) !Props {
    var map = Props.init(alloc);
    var it = std.mem.splitScalar(u8, text, '\n');
    var lineno: usize = 0;
    while (it.next()) |raw| {
        lineno += 1;
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0 or line[0] == '#' or line[0] == '!') continue;
        const eq = std.mem.indexOfScalar(u8, line, '=') orelse {
            warn("kannon.properties line {d}: ignoring line without '=': {s}", .{ lineno, line });
            continue;
        };
        const key = std.mem.trim(u8, line[0..eq], " \t");
        const value = std.mem.trim(u8, line[eq + 1 ..], " \t");
        if (key.len == 0) {
            warn("kannon.properties line {d}: ignoring empty key", .{lineno});
            continue;
        }
        try map.put(key, value);
    }
    return map;
}

fn configPaths(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged([]const u8)) !void {
    try list.append(alloc, "./kannon.properties");
    if (std.posix.getenv("XDG_CONFIG_HOME")) |xdg| {
        if (xdg.len > 0) {
            try list.append(alloc, try std.fmt.allocPrint(alloc, "{s}/kannon/kannon.properties", .{xdg}));
            return;
        }
    }
    if (std.posix.getenv("HOME")) |home| {
        if (home.len > 0)
            try list.append(alloc, try std.fmt.allocPrint(alloc, "{s}/.config/kannon/kannon.properties", .{home}));
    }
}

/// Find and parse the first kannon.properties on the search path, then
/// validate it into a Config.
pub fn load(alloc: std.mem.Allocator) LoadError!Config {
    var paths: std.ArrayListUnmanaged([]const u8) = .empty;
    try configPaths(alloc, &paths);

    var text: ?[]u8 = null;
    for (paths.items) |p| {
        const t = std.fs.cwd().readFileAlloc(alloc, p, 1 << 20) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return error.ConfigNotFound,
        };
        text = t;
        break;
    }
    const body = text orelse return error.ConfigNotFound;

    const props = try parse(alloc, body);

    var cfg: Config = .{ .bootstrap_servers = &.{} };

    var it = props.iterator();
    while (it.next()) |e| {
        const key = e.key_ptr.*;
        const val = e.value_ptr.*;
        if (std.mem.eql(u8, key, "bootstrap.servers")) {
            var servers: std.ArrayListUnmanaged([]const u8) = .empty;
            var sit = std.mem.splitScalar(u8, val, ',');
            while (sit.next()) |s| {
                const sv = std.mem.trim(u8, s, " \t");
                if (sv.len == 0) continue;
                if (std.mem.lastIndexOfScalar(u8, sv, ':') == null)
                    warn("bootstrap.servers entry '{s}' lacks a port; using 9092", .{sv});
                try servers.append(alloc, sv);
            }
            cfg.bootstrap_servers = servers.items;
        } else if (std.mem.eql(u8, key, "security.protocol")) {
            const v = try lower(alloc, val);
            cfg.security_protocol = std.meta.stringToEnum(SecurityProtocol, v) orelse
                return error.InvalidSecurityProtocol;
        } else if (std.mem.eql(u8, key, "sasl.mechanism")) {
            cfg.sasl_mechanism = mechFromString(val) orelse return error.InvalidSaslMechanism;
        } else if (std.mem.eql(u8, key, "sasl.username")) {
            cfg.sasl_username = val;
        } else if (std.mem.eql(u8, key, "sasl.password")) {
            cfg.sasl_password = val;
        } else if (std.mem.eql(u8, key, "ssl.truststore.location")) {
            cfg.ssl_truststore_location = val;
        } else if (std.mem.eql(u8, key, "linger.ms")) {
            cfg.linger_ms = std.fmt.parseInt(u64, val, 10) catch {
                warn("invalid linger.ms '{s}' ignored", .{val});
                continue;
            };
        } else if (std.mem.eql(u8, key, "batch.size")) {
            cfg.batch_size = std.fmt.parseInt(usize, val, 10) catch {
                warn("invalid batch.size '{s}' ignored", .{val});
                continue;
            };
        } else if (std.mem.eql(u8, key, "enable.idempotence")) {
            if (std.ascii.eqlIgnoreCase(val, "true")) {
                cfg.enable_idempotence = true;
            } else if (std.ascii.eqlIgnoreCase(val, "false")) {
                cfg.enable_idempotence = false;
            } else {
                warn("invalid enable.idempotence '{s}' ignored", .{val});
            }
        } else {
            warn("unknown config key '{s}' ignored", .{key});
        }
    }

    if (cfg.bootstrap_servers.len == 0) return error.MissingBootstrapServers;

    switch (cfg.security_protocol) {
        .sasl_ssl, .sasl_plaintext => {
            if (cfg.sasl_mechanism == null) return error.MissingSaslMechanism;
        },
        else => {},
    }
    if (cfg.sasl_mechanism != null) {
        if (cfg.sasl_username == null or cfg.sasl_password == null)
            return error.MissingSaslCredentials;
    }
    return cfg;
}

fn lower(alloc: std.mem.Allocator, s: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, s.len);
    return std.ascii.lowerString(out, s);
}

fn mechFromString(s: []const u8) ?SaslMechanism {
    if (std.ascii.eqlIgnoreCase(s, "PLAIN")) return .plain;
    if (std.ascii.eqlIgnoreCase(s, "SCRAM-SHA-256")) return .scram_sha_256;
    if (std.ascii.eqlIgnoreCase(s, "SCRAM-SHA-512")) return .scram_sha_512;
    return null;
}

pub fn mechName(m: SaslMechanism) []const u8 {
    return switch (m) {
        .plain => "PLAIN",
        .scram_sha_256 => "SCRAM-SHA-256",
        .scram_sha_512 => "SCRAM-SHA-512",
    };
}

test "parse handles comments, blanks, whitespace, duplicates, missing =" {
    const gpa = std.testing.allocator;
    const text =
        "# comment\n" ++
        "! bang comment\n" ++
        "\n" ++
        "  a = 1  \n" ++
        "b=2\n" ++
        "a = last-wins\n" ++
        "no-equals-here\n" ++
        "  spaced.key = spaced value \n";
    var m = try parse(gpa, text);
    defer m.deinit();
    try std.testing.expectEqualStrings("last-wins", m.get("a").?);
    try std.testing.expectEqualStrings("2", m.get("b").?);
    try std.testing.expectEqualStrings("spaced value", m.get("spaced.key").?);
    try std.testing.expect(m.get("no-equals-here") == null);
}

test "parse empty file" {
    var m = try parse(std.testing.allocator, "");
    defer m.deinit();
    try std.testing.expectEqual(@as(usize, 0), m.count());
}
