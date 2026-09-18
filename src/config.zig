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
    fetch_max_bytes: usize = 8 << 20,
    fetch_max_wait_ms: u64 = 500,
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
    ConfigFileNotFound,
    ConfigFileUnreadable,
    MissingBootstrapServers,
    InvalidSecurityProtocol,
    InvalidSaslMechanism,
    MissingSaslMechanism,
    MissingSaslCredentials,
    OutOfMemory,
};

/// Command-line settings that take precedence over the environment and file.
pub const Overrides = struct {
    bootstrap: ?[]const u8 = null,
    config_path: ?[]const u8 = null,
};

/// Where the effective configuration came from, for `-v` and error messages.
pub const Source = struct {
    /// Properties file that was read, if any.
    file: ?[]const u8 = null,
    /// Explicit path (--config or KAFKA_PROPERTIES) that was requested.
    requested: ?[]const u8 = null,
    env: bool = false,
    flags: bool = false,
    /// Per-key provenance for --show-config.
    origins: std.EnumArray(Key, Origin) = .initFill(.default),
};

pub const Key = enum {
    bootstrap_servers,
    security_protocol,
    sasl_mechanism,
    sasl_username,
    sasl_password,
    ssl_truststore_location,
    linger_ms,
    batch_size,
    fetch_max_bytes,
    fetch_max_wait_ms,
    enable_idempotence,
};

pub const Origin = enum { default, file, env, flag };

pub const key_names = std.EnumArray(Key, []const u8).init(.{
    .bootstrap_servers = "bootstrap.servers",
    .security_protocol = "security.protocol",
    .sasl_mechanism = "sasl.mechanism",
    .sasl_username = "sasl.username",
    .sasl_password = "sasl.password",
    .ssl_truststore_location = "ssl.truststore.location",
    .linger_ms = "linger.ms",
    .batch_size = "batch.size",
    .fetch_max_bytes = "fetch.max.bytes",
    .fetch_max_wait_ms = "fetch.max.wait.ms",
    .enable_idempotence = "enable.idempotence",
});

fn keyFromName(name: []const u8) ?Key {
    for (std.enums.values(Key)) |k|
        if (std.mem.eql(u8, name, key_names.get(k))) return k;
    return null;
}

/// Effective value of `key` as display text; empty when an optional is unset.
pub fn valueString(cfg: *const Config, alloc: std.mem.Allocator, key: Key) ![]const u8 {
    switch (key) {
        .bootstrap_servers => {
            var aw = std.Io.Writer.Allocating.init(alloc);
            for (cfg.bootstrap_servers, 0..) |s, i| {
                if (i > 0) try aw.writer.writeByte(',');
                try aw.writer.writeAll(s);
            }
            return aw.toOwnedSlice();
        },
        .security_protocol => return protoName(cfg.security_protocol),
        .sasl_mechanism => return if (cfg.sasl_mechanism) |m| mechName(m) else "",
        .sasl_username => return cfg.sasl_username orelse "",
        .sasl_password => return if (cfg.sasl_password != null) "********" else "",
        .ssl_truststore_location => return cfg.ssl_truststore_location orelse "",
        .linger_ms => return fmtInt(alloc, cfg.linger_ms),
        .batch_size => return fmtInt(alloc, cfg.batch_size),
        .fetch_max_bytes => return fmtInt(alloc, cfg.fetch_max_bytes),
        .fetch_max_wait_ms => return fmtInt(alloc, cfg.fetch_max_wait_ms),
        .enable_idempotence => return if (cfg.enable_idempotence) "true" else "false",
    }
}

fn fmtInt(alloc: std.mem.Allocator, v: u64) ![]const u8 {
    return std.fmt.allocPrint(alloc, "{d}", .{v});
}

pub fn isSecret(key: Key) bool {
    return key == .sasl_password;
}

fn protoName(p: SecurityProtocol) []const u8 {
    return switch (p) {
        .plaintext => "PLAINTEXT",
        .ssl => "SSL",
        .sasl_ssl => "SASL_SSL",
        .sasl_plaintext => "SASL_PLAINTEXT",
    };
}

fn warn(comptime fmt: []const u8, args: anytype) void {
    if (@import("builtin").is_test) return; // stderr writes corrupt the 0.16 test-runner IPC
    std.debug.print("kite: warning: " ++ fmt ++ "\n", args);
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
            warn("kite.properties line {d}: ignoring line without '=': {s}", .{ lineno, line });
            continue;
        };
        const key = std.mem.trim(u8, line[0..eq], " \t");
        const value = std.mem.trim(u8, line[eq + 1 ..], " \t");
        if (key.len == 0) {
            warn("kite.properties line {d}: ignoring empty key", .{lineno});
            continue;
        }
        try map.put(key, value);
    }
    return map;
}

fn configPaths(alloc: std.mem.Allocator, env: *std.process.Environ.Map, list: *std.ArrayListUnmanaged([]const u8)) !void {
    try list.append(alloc, "./kite.properties");
    if (env.get("XDG_CONFIG_HOME")) |xdg| {
        if (xdg.len > 0)
            try list.append(alloc, try std.fmt.allocPrint(alloc, "{s}/kite/kite.properties", .{xdg}));
    }
    if (env.get("HOME")) |home| {
        if (home.len > 0) {
            const p = try std.fmt.allocPrint(alloc, "{s}/.config/kite/kite.properties", .{home});
            for (list.items) |q|
                if (std.mem.eql(u8, q, p)) return;
            try list.append(alloc, p);
        }
    }
}

const env_keys = [_][2][]const u8{
    .{ "BOOTSTRAP_SERVERS", "bootstrap.servers" },
    .{ "SECURITY_PROTOCOL", "security.protocol" },
    .{ "SASL_MECHANISM", "sasl.mechanism" },
    .{ "SASL_USERNAME", "sasl.username" },
    .{ "SASL_PASSWORD", "sasl.password" },
    .{ "SSL_TRUSTSTORE_LOCATION", "ssl.truststore.location" },
};

fn overriddenByEnv(env: *std.process.Environ.Map, key: []const u8) bool {
    for (env_keys) |pair| {
        if (!std.mem.eql(u8, key, pair[1])) continue;
        const value = env.get(pair[0]) orelse return false;
        return value.len > 0;
    }
    return false;
}

/// Build the effective Config. Precedence: command-line flags, then
/// environment variables, then the properties file (`--config`/`KAFKA_PROPERTIES`
/// or the first file on the search path). A file is optional as soon as the
/// environment or flags supply bootstrap.servers.
pub fn load(
    io: std.Io,
    alloc: std.mem.Allocator,
    env: *std.process.Environ.Map,
    overrides: Overrides,
    source: *Source,
) LoadError!Config {
    var cfg: Config = .{ .bootstrap_servers = &.{} };

    const explicit: ?[]const u8 = overrides.config_path orelse blk: {
        const from_env = env.get("KAFKA_PROPERTIES") orelse break :blk null;
        break :blk if (from_env.len > 0) from_env else null;
    };
    source.requested = explicit;

    var text: ?[]u8 = null;
    if (explicit) |path| {
        text = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(1 << 20)) catch |err| switch (err) {
            error.FileNotFound => return error.ConfigFileNotFound,
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.ConfigFileUnreadable,
        };
        source.file = path;
    } else {
        var paths: std.ArrayListUnmanaged([]const u8) = .empty;
        try configPaths(alloc, env, &paths);
        for (paths.items) |p| {
            const t = std.Io.Dir.cwd().readFileAlloc(io, p, alloc, .limited(1 << 20)) catch |err| switch (err) {
                error.FileNotFound => continue,
                error.OutOfMemory => return error.OutOfMemory,
                else => continue,
            };
            text = t;
            source.file = p;
            break;
        }
    }

    if (text) |body| {
        const props = try parse(alloc, body);
        var it = props.iterator();
        while (it.next()) |e| {
            const key = e.key_ptr.*;
            if (overrides.bootstrap != null and std.mem.eql(u8, key, "bootstrap.servers")) continue;
            if (overriddenByEnv(env, key)) continue;
            try applyKey(&cfg, alloc, source, .file, key, e.value_ptr.*);
        }
    }

    for (env_keys) |pair| {
        const val = env.get(pair[0]) orelse continue;
        if (val.len == 0) continue;
        source.env = true;
        try applyKey(&cfg, alloc, source, .env, pair[1], val);
    }

    if (overrides.bootstrap) |b| {
        source.flags = true;
        try applyKey(&cfg, alloc, source, .flag, "bootstrap.servers", b);
    }

    if (cfg.bootstrap_servers.len == 0) {
        if (source.file == null) return error.ConfigNotFound;
        return error.MissingBootstrapServers;
    }

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
    if (source.origins.get(.sasl_password) == .file)
        warnIfLoosePerms(io, source.file.?);
    return cfg;
}

/// Warn when a properties file that supplied sasl.password is readable by
/// group/other users.
fn warnIfLoosePerms(io: std.Io, path: []const u8) void {
    const stat = std.Io.Dir.cwd().statFile(io, path, .{}) catch return;
    if (stat.permissions.toMode() & 0o077 != 0)
        warn("{s} is readable by other users; run chmod 600 {s}", .{ path, path });
}

fn applyKey(cfg: *Config, alloc: std.mem.Allocator, source: *Source, origin: Origin, key: []const u8, val: []const u8) LoadError!void {
    const k = keyFromName(key) orelse {
        warn("unknown config key '{s}' ignored", .{key});
        return;
    };
    switch (k) {
        .bootstrap_servers => {
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
        },
        .security_protocol => {
            const v = try lower(alloc, val);
            cfg.security_protocol = std.meta.stringToEnum(SecurityProtocol, v) orelse
                return error.InvalidSecurityProtocol;
        },
        .sasl_mechanism => cfg.sasl_mechanism = mechFromString(val) orelse
            return error.InvalidSaslMechanism,
        .sasl_username => cfg.sasl_username = val,
        .sasl_password => cfg.sasl_password = val,
        .ssl_truststore_location => cfg.ssl_truststore_location = val,
        .linger_ms => cfg.linger_ms = std.fmt.parseInt(u64, val, 10) catch {
            warn("invalid linger.ms '{s}' ignored", .{val});
            return;
        },
        .batch_size => cfg.batch_size = std.fmt.parseInt(usize, val, 10) catch {
            warn("invalid batch.size '{s}' ignored", .{val});
            return;
        },
        .fetch_max_bytes => {
            cfg.fetch_max_bytes = std.fmt.parseInt(usize, val, 10) catch {
                warn("invalid fetch.max.bytes '{s}' ignored", .{val});
                return;
            };
            if (cfg.fetch_max_bytes >= 16 << 20) {
                warn("fetch.max.bytes '{s}' is >= transport maximum; using default", .{val});
                cfg.fetch_max_bytes = 8 << 20;
            }
        },
        .fetch_max_wait_ms => {
            cfg.fetch_max_wait_ms = std.fmt.parseInt(u64, val, 10) catch {
                warn("invalid fetch.max.wait.ms '{s}' ignored", .{val});
                return;
            };
            if (cfg.fetch_max_wait_ms >= 15_000) {
                warn("fetch.max.wait.ms '{s}' is too close to socket timeout; using default", .{val});
                cfg.fetch_max_wait_ms = 500;
            }
        },
        .enable_idempotence => if (std.ascii.eqlIgnoreCase(val, "true")) {
            cfg.enable_idempotence = true;
        } else if (std.ascii.eqlIgnoreCase(val, "false")) {
            cfg.enable_idempotence = false;
        } else {
            warn("invalid enable.idempotence '{s}' ignored", .{val});
            return;
        },
    }
    source.origins.set(k, origin);
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

test "later applyKey calls override earlier values" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var cfg: Config = .{ .bootstrap_servers = &.{} };
    var src: Source = .{};
    try applyKey(&cfg, arena.allocator(), &src, .file, "bootstrap.servers", "a:1, b:2");
    try std.testing.expectEqual(@as(usize, 2), cfg.bootstrap_servers.len);
    try std.testing.expectEqual(Origin.file, src.origins.get(.bootstrap_servers));
    try applyKey(&cfg, arena.allocator(), &src, .file, "bootstrap.servers", "c:3");
    try std.testing.expectEqual(@as(usize, 1), cfg.bootstrap_servers.len);
    try std.testing.expectEqualStrings("c:3", cfg.bootstrap_servers[0]);
    try applyKey(&cfg, arena.allocator(), &src, .file, "security.protocol", "SASL_SSL");
    try std.testing.expectEqual(SecurityProtocol.sasl_ssl, cfg.security_protocol);
    try std.testing.expectError(error.InvalidSaslMechanism, applyKey(&cfg, arena.allocator(), &src, .file, "sasl.mechanism", "nope"));
    try std.testing.expectEqual(Origin.default, src.origins.get(.sasl_mechanism));
}

test "origins track the highest-precedence writer" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var cfg: Config = .{ .bootstrap_servers = &.{} };
    var src: Source = .{};
    try applyKey(&cfg, arena.allocator(), &src, .file, "sasl.username", "from-file");
    try applyKey(&cfg, arena.allocator(), &src, .env, "sasl.username", "from-env");
    try std.testing.expectEqual(Origin.env, src.origins.get(.sasl_username));
    try std.testing.expectEqualStrings("from-env", cfg.sasl_username.?);
    try applyKey(&cfg, arena.allocator(), &src, .flag, "bootstrap.servers", "h:1");
    try std.testing.expectEqual(Origin.flag, src.origins.get(.bootstrap_servers));
}
