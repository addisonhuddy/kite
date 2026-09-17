const std = @import("std");
const consumer = @import("consumer.zig");
const protocol = @import("protocol.zig");
const format_mod = @import("format.zig");

pub const Format = format_mod.Format;

/// Options shared by produce and consume.
pub const Common = struct {
    verbose: bool = false,
    quiet: bool = false,
    format: Format = .auto,
    /// First spelling that set `format`, for conflict messages.
    format_spelling: ?[]const u8 = null,
    bootstrap: ?[]const u8 = null,
    config_path: ?[]const u8 = null,
};

pub const ProduceArgs = struct {
    topic: []const u8,
    common: Common = .{},
    key_col: ?[]const u8 = null,
    headers: []const protocol.Header,

    pub fn isCsv(a: ProduceArgs) bool {
        return a.common.format == .csv;
    }
};

pub const ConsumeArgs = struct {
    topic: []const u8,
    common: Common = .{},
    start: consumer.Options.Start = .latest,
    offset: i64 = 0,
    partition: ?i32 = null,
    max_records: ?u64 = null,
    idle_ms: ?u64 = null,
    follow: bool = false,
};

pub fn Result(comptime T: type) type {
    return union(enum) {
        help,
        ok: T,
        err: []const u8,
    };
}

pub const version = "0.1.0";

pub const Mode = enum {
    produce,
    consume,
    install,
};

pub const ModeSplit = struct {
    mode: Mode,
    rest: []const []const u8,
};

fn takesSeparateValue(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "-b") or
        std.mem.eql(u8, arg, "--bootstrap") or
        std.mem.eql(u8, arg, "--config") or
        std.mem.eql(u8, arg, "--format") or
        std.mem.eql(u8, arg, "-H") or
        std.mem.eql(u8, arg, "--key") or
        std.mem.eql(u8, arg, "--offset") or
        std.mem.eql(u8, arg, "--partition") or
        std.mem.eql(u8, arg, "-n") or
        std.mem.eql(u8, arg, "--max") or
        std.mem.eql(u8, arg, "-t") or
        std.mem.eql(u8, arg, "--idle") or
        std.mem.eql(u8, arg, "--dir");
}

pub fn splitMode(alloc: std.mem.Allocator, args: []const []const u8) Result(ModeSplit) {
    var mode: Mode = .produce;
    var rest: std.ArrayListUnmanaged([]const u8) = .empty;
    var value_follows = false;
    for (args) |arg| {
        if (value_follows) {
            rest.append(alloc, arg) catch return .{ .err = "out of memory" };
            value_follows = false;
            continue;
        }
        if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--consume")) {
            if (mode == .install)
                return .{ .err = "--consume cannot be combined with --install" };
            mode = .consume;
        } else if (std.mem.eql(u8, arg, "-i") or std.mem.eql(u8, arg, "--install")) {
            if (mode == .consume)
                return .{ .err = "--consume cannot be combined with --install" };
            mode = .install;
        } else {
            rest.append(alloc, arg) catch return .{ .err = "out of memory" };
            value_follows = takesSeparateValue(arg);
        }
    }
    return .{ .ok = .{ .mode = mode, .rest = rest.toOwnedSlice(alloc) catch return .{ .err = "out of memory" } } };
}

pub const produce_usage =
    "Usage: kite [OPTIONS] TOPIC\n" ++
    "Try 'kite --help' for examples.\n";

pub const consume_usage =
    "Usage: kite -c [OPTIONS] TOPIC\n" ++
    "Try 'kite -c --help' for examples.\n";

pub const install_usage =
    "Usage: kite -i [OPTIONS]\n" ++
    "Try 'kite -i --help' for details.\n";

const config_help =
    "Configuration:\n" ++
    "  Flags override environment variables, which override the first\n" ++
    "  kite.properties found in: ./kite.properties,\n" ++
    "  $XDG_CONFIG_HOME/kite/kite.properties, ~/.config/kite/kite.properties.\n" ++
    "  Environment: BOOTSTRAP_SERVERS, SECURITY_PROTOCOL,\n" ++
    "  SASL_MECHANISM, SASL_USERNAME, SASL_PASSWORD,\n" ++
    "  SSL_TRUSTSTORE_LOCATION, KAFKA_PROPERTIES (path to a properties file).\n" ++
    "  Templates are in examples/config/.\n";

pub const produce_help =
    "kite - Write stdin records to a Kafka topic\n" ++
    "\n" ++
    "Usage:\n" ++
    "  kite [OPTIONS] TOPIC          Produce stdin lines to TOPIC (default).\n" ++
    "  kite -c [OPTIONS] TOPIC       Consume TOPIC to stdout. See 'kite -c --help'.\n" ++
    "  kite -i [OPTIONS]             Install this executable. See 'kite -i --help'.\n" ++
    "\n" ++
    "Options:\n" ++
    "  -c, --consume         Consume instead of produce.\n" ++
    "  -i, --install         Install the executable to ~/.local/bin.\n" ++
    "  -V, --version         Print the version and exit.\n" ++
    "  -b, --bootstrap HOSTS Comma-separated host:port brokers.\n" ++
    "  --config FILE         Read this properties file instead of searching.\n" ++
    "  -H HEADER             Add a 'name: value' header (repeatable).\n" ++
    "  --csv                 Read RFC 4180 CSV (same as --format csv).\n" ++
    "  --key COL             Use CSV column COL as the record key; requires --csv.\n" ++
    "  --format FMT          Record shape: value, tsv, json (default: auto; see\n" ++
    "                        below). --json is short for --format json.\n" ++
    "  -q, --quiet           Suppress the summary and progress lines on stderr.\n" ++
    "  -v, --verbose         Write connection and retry diagnostics to stderr.\n" ++
    "  -h, --help            Show this help and exit.\n" ++
    "\n" ++
    "Input format:\n" ++
    "  auto/tsv              value | key<TAB>value | key<TAB>h: v<TAB>value\n" ++
    "  value                 The whole line is the value; TAB is not special.\n" ++
    "  json                  {\"key\":..,\"value\":..,\"headers\":{..}} per line.\n" ++
    "\n" ++
    "Examples:\n" ++
    "  kite events < examples/data/lines.txt\n" ++
    "  kite -b localhost:9092 events < examples/data/lines.txt\n" ++
    "  kite -H 'source: import' events < examples/data/headers.tsv\n" ++
    "  kite --csv --key user_id events < examples/data/events.csv\n" ++
    "  kite -c --json src | kite --json dst\n" ++
    "\n" ++
    config_help;

pub const consume_help =
    "kite -c - Read Kafka records to stdout\n" ++
    "\n" ++
    "Usage:\n" ++
    "  kite -c [OPTIONS] TOPIC\n" ++
    "\n" ++
    "Options:\n" ++
    "  -b, --bootstrap HOSTS Comma-separated host:port brokers.\n" ++
    "  --config FILE         Read this properties file instead of searching.\n" ++
    "  -B, --from-beginning  Start at the earliest available offset.\n" ++
    "  --offset N            Start at offset N in each selected partition.\n" ++
    "                        Cannot be combined with --from-beginning.\n" ++
    "  --partition P         Read partition P only (default: all partitions).\n" ++
    "  -n, --max MAX         Stop after MAX records.\n" ++
    "  --idle DUR            Stop after DUR without a record (3s, 500ms, 1m;\n" ++
    "                        a bare number is milliseconds). Also -t.\n" ++
    "  -f, --follow          Never stop on idle; wait for new records.\n" ++
    "  --format FMT          Record shape: value, tsv, json (default: auto; see\n" ++
    "                        below). --json is short for --format json.\n" ++
    "  -q, --quiet           Suppress the summary and progress lines on stderr.\n" ++
    "  -v, --verbose         Write fetch diagnostics to stderr.\n" ++
    "  -h, --help            Show this help and exit.\n" ++
    "\n" ++
    "Output format:\n" ++
    "  auto                  Value alone, or key<TAB>h: v<TAB>value when set.\n" ++
    "  value                 Only the record value.\n" ++
    "  tsv                   Always key<TAB>[h: v<TAB>]value (empty key field).\n" ++
    "  json                  One object per record with full metadata.\n" ++
    "\n" ++
    "By default, start at the latest offset. When stdout is a terminal the read\n" ++
    "follows new records until Ctrl-C; when stdout is a pipe or file and none of\n" ++
    "--max, --idle, or --follow is given, the read stops after 5s idle.\n" ++
    "\n" ++
    "Examples:\n" ++
    "  kite -c events\n" ++
    "  kite -c -B --idle 3s events\n" ++
    "  kite -c --partition 0 --offset 42 -n 10 --idle 3s events\n" ++
    "  kite -c -B --json events | jq -c .value\n" ++
    "\n" ++
    config_help;

pub const install_help =
    "kite -i - Install the current kite executable\n" ++
    "\n" ++
    "Usage:\n" ++
    "  kite -i [OPTIONS]\n" ++
    "\n" ++
    "Options:\n" ++
    "  --dir DIR             Install to DIR (default: $KITE_INSTALL_DIR or\n" ++
    "                        ~/.local/bin).\n" ++
    "  -y, --yes             Add the install directory to PATH without prompting.\n" ++
    "  -h, --help            Show this help and exit.\n";

fn errorResult(comptime T: type, alloc: std.mem.Allocator, comptime fmt: []const u8, args: anytype) Result(T) {
    return .{ .err = std.fmt.allocPrint(alloc, fmt, args) catch "out of memory" };
}

fn badNumber(comptime T: type, alloc: std.mem.Allocator, option: []const u8, value: []const u8) Result(T) {
    return errorResult(T, alloc, "{s}: '{s}' is not a non-negative integer", .{ option, value });
}

fn parseU64(comptime T: type, alloc: std.mem.Allocator, option: []const u8, value: []const u8) Result(T) {
    const parsed = std.fmt.parseInt(u64, value, 10) catch return badNumber(T, alloc, option, value);
    return .{ .ok = parsed };
}

/// Duration in milliseconds. A bare number is milliseconds; suffixes
/// ms, s, m, h are accepted.
pub fn parseDurationMs(value: []const u8) ?u64 {
    var digits_end: usize = 0;
    while (digits_end < value.len and std.ascii.isDigit(value[digits_end])) digits_end += 1;
    if (digits_end == 0) return null;
    const n = std.fmt.parseInt(u64, value[0..digits_end], 10) catch return null;
    const suffix = value[digits_end..];
    const mult: u64 = if (suffix.len == 0 or std.mem.eql(u8, suffix, "ms"))
        1
    else if (std.mem.eql(u8, suffix, "s"))
        1000
    else if (std.mem.eql(u8, suffix, "m"))
        60_000
    else if (std.mem.eql(u8, suffix, "h"))
        3_600_000
    else
        return null;
    return std.math.mul(u64, n, mult) catch null;
}

fn parseDuration(alloc: std.mem.Allocator, option: []const u8, value: []const u8) Result(u64) {
    return .{ .ok = parseDurationMs(value) orelse
        return errorResult(u64, alloc, "{s}: '{s}' is not a duration (want e.g. 3000, 3s, 500ms, 1m)", .{ option, value }) };
}

fn parseOffset(alloc: std.mem.Allocator, value: []const u8) Result(i64) {
    const parsed = std.fmt.parseInt(u64, value, 10) catch return badNumber(i64, alloc, "--offset", value);
    if (parsed > std.math.maxInt(i64)) return badNumber(i64, alloc, "--offset", value);
    return .{ .ok = @intCast(parsed) };
}

fn parsePartition(alloc: std.mem.Allocator, value: []const u8) Result(i32) {
    const parsed = std.fmt.parseInt(u64, value, 10) catch return badNumber(i32, alloc, "--partition", value);
    if (parsed > std.math.maxInt(i32)) return badNumber(i32, alloc, "--partition", value);
    return .{ .ok = @intCast(parsed) };
}

pub fn parseHeaderArg(s: []const u8) !protocol.Header {
    const colon = std.mem.indexOfScalar(u8, s, ':') orelse return error.MalformedHeader;
    const name = std.mem.trim(u8, s[0..colon], " \t");
    if (name.len == 0) return error.MalformedHeader;
    return .{ .key = name, .value = std.mem.trim(u8, s[colon + 1 ..], " \t") };
}

const produce_only = [_][]const u8{ "-H", "--csv", "--key" };
const consume_only = [_][]const u8{ "-B", "--from-beginning", "--offset", "--partition", "-n", "--max", "-t", "--idle", "-f", "--follow" };

fn optionName(arg: []const u8) []const u8 {
    if (std.mem.indexOfScalar(u8, arg, '=')) |eq| return arg[0..eq];
    if (arg.len > 2 and arg[1] != '-') return arg[0..2];
    return arg;
}

fn isOneOf(name: []const u8, list: []const []const u8) bool {
    for (list) |candidate| if (std.mem.eql(u8, name, candidate)) return true;
    return false;
}

fn unknownOption(comptime T: type, alloc: std.mem.Allocator, mode: Mode, arg: []const u8) Result(T) {
    const name = optionName(arg);
    switch (mode) {
        .produce => if (isOneOf(name, &consume_only))
            return errorResult(T, alloc, "'{s}' is a consume option; use 'kite -c [OPTIONS] TOPIC'", .{name}),
        .consume => if (isOneOf(name, &produce_only))
            return errorResult(T, alloc, "'{s}' is a produce option and is not valid with -c", .{name}),
        .install => {},
    }
    return errorResult(T, alloc, "unknown option '{s}'", .{arg});
}

/// Handle options shared by produce and consume. Returns true when `arg`
/// was consumed; `i` is advanced past any separate value.
fn parseCommon(comptime T: type, alloc: std.mem.Allocator, common: *Common, args: []const []const u8, i: *usize) ?Result(T) {
    const arg = args[i.*];
    if (std.mem.eql(u8, arg, "-v") or std.mem.eql(u8, arg, "--verbose")) {
        common.verbose = true;
    } else if (std.mem.eql(u8, arg, "-q") or std.mem.eql(u8, arg, "--quiet")) {
        common.quiet = true;
    } else if (std.mem.eql(u8, arg, "--json")) {
        if (setFormat(T, alloc, common, .json, "--json")) |r| return r;
    } else if (std.mem.eql(u8, arg, "--format")) {
        i.* += 1;
        if (i.* >= args.len) return errorResult(T, alloc, "--format requires a value", .{});
        const value = args[i.*];
        if (parseFormatArg(T, alloc, common, value, std.fmt.allocPrint(alloc, "--format {s}", .{value}) catch return .{ .err = "out of memory" })) |r| return r;
    } else if (std.mem.startsWith(u8, arg, "--format=")) {
        if (parseFormatArg(T, alloc, common, arg["--format=".len..], arg)) |r| return r;
    } else if (std.mem.eql(u8, arg, "-b") or std.mem.eql(u8, arg, "--bootstrap")) {
        i.* += 1;
        if (i.* >= args.len) return errorResult(T, alloc, "{s} requires a value", .{arg});
        common.bootstrap = args[i.*];
    } else if (std.mem.startsWith(u8, arg, "--bootstrap=")) {
        common.bootstrap = arg["--bootstrap=".len..];
    } else if (std.mem.startsWith(u8, arg, "-b") and arg.len > 2) {
        common.bootstrap = arg[2..];
    } else if (std.mem.eql(u8, arg, "--config")) {
        i.* += 1;
        if (i.* >= args.len) return errorResult(T, alloc, "--config requires a value", .{});
        common.config_path = args[i.*];
    } else if (std.mem.startsWith(u8, arg, "--config=")) {
        common.config_path = arg["--config=".len..];
    } else {
        return null;
    }
    return .{ .ok = undefined };
}

/// Record a format choice; a different non-auto format already set is a
/// conflict reported with the user's own spellings.
fn setFormat(comptime T: type, alloc: std.mem.Allocator, common: *Common, format: Format, spelling: []const u8) ?Result(T) {
    if (common.format != .auto and common.format != format)
        return errorResult(T, alloc, "{s} cannot be combined with {s}", .{ common.format_spelling.?, spelling });
    common.format = format;
    if (common.format_spelling == null) common.format_spelling = spelling;
    return null;
}

fn parseFormatArg(comptime T: type, alloc: std.mem.Allocator, common: *Common, value: []const u8, spelling: []const u8) ?Result(T) {
    const format = format_mod.parse(value) orelse
        return errorResult(T, alloc, "--format: '{s}' is not a format (want value, tsv, json, or csv)", .{value});
    return setFormat(T, alloc, common, format, spelling);
}

fn finishCommon(comptime T: type, alloc: std.mem.Allocator, common: Common) ?Result(T) {
    if (common.quiet and common.verbose) return errorResult(T, alloc, "--quiet cannot be combined with --verbose", .{});
    if (common.bootstrap) |b| if (b.len == 0) return errorResult(T, alloc, "--bootstrap must not be empty", .{});
    if (common.config_path) |p| if (p.len == 0) return errorResult(T, alloc, "--config must not be empty", .{});
    return null;
}

pub fn parseProduce(alloc: std.mem.Allocator, args: []const []const u8) Result(ProduceArgs) {
    var headers: std.ArrayListUnmanaged(protocol.Header) = .empty;
    var topic: ?[]const u8 = null;
    var common: Common = .{};
    var key_col: ?[]const u8 = null;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) return .help;
        if (parseCommon(ProduceArgs, alloc, &common, args, &i)) |r| {
            switch (r) {
                .err => return r,
                else => continue,
            }
        }
        if (std.mem.eql(u8, arg, "-H")) {
            i += 1;
            if (i >= args.len) return errorResult(ProduceArgs, alloc, "-H requires a value", .{});
            headers.append(alloc, parseHeaderArg(args[i]) catch
                return errorResult(ProduceArgs, alloc, "malformed header '{s}' (want 'name: value')", .{args[i]})) catch
                return .{ .err = "out of memory" };
        } else if (std.mem.startsWith(u8, arg, "-H") and arg.len > 2) {
            headers.append(alloc, parseHeaderArg(arg[2..]) catch
                return errorResult(ProduceArgs, alloc, "malformed header '{s}' (want 'name: value')", .{arg[2..]})) catch
                return .{ .err = "out of memory" };
        } else if (std.mem.eql(u8, arg, "--csv")) {
            if (setFormat(ProduceArgs, alloc, &common, .csv, "--csv")) |r| return r;
        } else if (std.mem.eql(u8, arg, "--key")) {
            i += 1;
            if (i >= args.len) return errorResult(ProduceArgs, alloc, "--key requires a value", .{});
            key_col = args[i];
        } else if (std.mem.startsWith(u8, arg, "--key=")) {
            key_col = arg["--key=".len..];
        } else if (arg.len > 0 and arg[0] == '-') {
            return unknownOption(ProduceArgs, alloc, .produce, arg);
        } else if (topic == null) {
            topic = arg;
        } else {
            return errorResult(ProduceArgs, alloc, "unexpected argument '{s}' (only one TOPIC is allowed)", .{arg});
        }
    }

    const topic_name = topic orelse return errorResult(ProduceArgs, alloc, "missing TOPIC", .{});
    if (topic_name.len == 0) return errorResult(ProduceArgs, alloc, "TOPIC must not be empty", .{});
    if (key_col != null and common.format != .csv) return errorResult(ProduceArgs, alloc, "--key requires --csv", .{});
    if (finishCommon(ProduceArgs, alloc, common)) |r| return r;
    return .{ .ok = .{
        .topic = topic_name,
        .common = common,
        .key_col = key_col,
        .headers = headers.items,
    } };
}

pub fn parseConsume(alloc: std.mem.Allocator, args: []const []const u8) Result(ConsumeArgs) {
    var topic: ?[]const u8 = null;
    var parsed: ConsumeArgs = .{ .topic = "" };
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) return .help;
        if (parseCommon(ConsumeArgs, alloc, &parsed.common, args, &i)) |r| {
            switch (r) {
                .err => return r,
                else => continue,
            }
        }
        if (std.mem.eql(u8, arg, "-B") or std.mem.eql(u8, arg, "--from-beginning")) {
            if (parsed.start == .offset)
                return errorResult(ConsumeArgs, alloc, "--offset cannot be combined with --from-beginning", .{});
            parsed.start = .earliest;
        } else if (std.mem.eql(u8, arg, "--offset")) {
            i += 1;
            if (i >= args.len) return errorResult(ConsumeArgs, alloc, "--offset requires a value", .{});
            if (parsed.start == .earliest)
                return errorResult(ConsumeArgs, alloc, "--offset cannot be combined with --from-beginning", .{});
            const value = parseOffset(alloc, args[i]);
            switch (value) {
                .ok => |n| {
                    parsed.offset = n;
                    parsed.start = .offset;
                },
                .err => |message| return .{ .err = message },
                .help => unreachable,
            }
        } else if (std.mem.startsWith(u8, arg, "--offset=")) {
            if (parsed.start == .earliest)
                return errorResult(ConsumeArgs, alloc, "--offset cannot be combined with --from-beginning", .{});
            const value = parseOffset(alloc, arg["--offset=".len..]);
            switch (value) {
                .ok => |n| {
                    parsed.offset = n;
                    parsed.start = .offset;
                },
                .err => |message| return .{ .err = message },
                .help => unreachable,
            }
        } else if (std.mem.eql(u8, arg, "--partition")) {
            i += 1;
            if (i >= args.len) return errorResult(ConsumeArgs, alloc, "--partition requires a value", .{});
            const value = parsePartition(alloc, args[i]);
            switch (value) {
                .ok => |n| parsed.partition = n,
                .err => |message| return .{ .err = message },
                .help => unreachable,
            }
        } else if (std.mem.startsWith(u8, arg, "--partition=")) {
            const value = parsePartition(alloc, arg["--partition=".len..]);
            switch (value) {
                .ok => |n| parsed.partition = n,
                .err => |message| return .{ .err = message },
                .help => unreachable,
            }
        } else if (std.mem.eql(u8, arg, "-n") or std.mem.eql(u8, arg, "--max")) {
            i += 1;
            if (i >= args.len) return errorResult(ConsumeArgs, alloc, "{s} requires a value", .{arg});
            const value = parseU64(u64, alloc, arg, args[i]);
            switch (value) {
                .ok => |n| parsed.max_records = n,
                .err => |message| return .{ .err = message },
                .help => unreachable,
            }
        } else if (std.mem.startsWith(u8, arg, "--max=")) {
            const value = parseU64(u64, alloc, "--max", arg["--max=".len..]);
            switch (value) {
                .ok => |n| parsed.max_records = n,
                .err => |message| return .{ .err = message },
                .help => unreachable,
            }
        } else if (std.mem.startsWith(u8, arg, "-n") and arg.len > 2) {
            const value = parseU64(u64, alloc, "-n", arg[2..]);
            switch (value) {
                .ok => |n| parsed.max_records = n,
                .err => |message| return .{ .err = message },
                .help => unreachable,
            }
        } else if (std.mem.eql(u8, arg, "-t") or std.mem.eql(u8, arg, "--idle")) {
            i += 1;
            if (i >= args.len) return errorResult(ConsumeArgs, alloc, "{s} requires a value", .{arg});
            const value = parseDuration(alloc, arg, args[i]);
            switch (value) {
                .ok => |n| parsed.idle_ms = n,
                .err => |message| return .{ .err = message },
                .help => unreachable,
            }
        } else if (std.mem.startsWith(u8, arg, "--idle=")) {
            const value = parseDuration(alloc, "--idle", arg["--idle=".len..]);
            switch (value) {
                .ok => |n| parsed.idle_ms = n,
                .err => |message| return .{ .err = message },
                .help => unreachable,
            }
        } else if (std.mem.startsWith(u8, arg, "-t") and arg.len > 2) {
            const value = parseDuration(alloc, "-t", arg[2..]);
            switch (value) {
                .ok => |n| parsed.idle_ms = n,
                .err => |message| return .{ .err = message },
                .help => unreachable,
            }
        } else if (std.mem.eql(u8, arg, "-f") or std.mem.eql(u8, arg, "--follow")) {
            parsed.follow = true;
        } else if (arg.len > 0 and arg[0] == '-') {
            return unknownOption(ConsumeArgs, alloc, .consume, arg);
        } else if (topic == null) {
            topic = arg;
        } else {
            return errorResult(ConsumeArgs, alloc, "unexpected argument '{s}' (only one TOPIC is allowed)", .{arg});
        }
    }

    const topic_name = topic orelse return errorResult(ConsumeArgs, alloc, "missing TOPIC", .{});
    if (topic_name.len == 0) return errorResult(ConsumeArgs, alloc, "TOPIC must not be empty", .{});
    if (parsed.follow and parsed.idle_ms != null)
        return errorResult(ConsumeArgs, alloc, "--follow cannot be combined with --idle", .{});
    if (parsed.common.format == .csv)
        return errorResult(ConsumeArgs, alloc, "--format csv is only valid when producing", .{});
    if (finishCommon(ConsumeArgs, alloc, parsed.common)) |r| return r;
    parsed.topic = topic_name;
    return .{ .ok = parsed };
}

fn expectErr(comptime T: type, result: Result(T), expected: []const u8) !void {
    switch (result) {
        .err => |message| try std.testing.expectEqualStrings(expected, message),
        else => return error.TestUnexpectedResult,
    }
}

test "produce parser accepts option spellings and zero-independent fields" {
    const result = parseProduce(std.heap.page_allocator, &.{
        "-H", "one: value", "-Hv: one", "--csv", "--key", "id", "-v", "-b", "h:1", "--config", "x.properties", "events",
    });
    switch (result) {
        .ok => |args| {
            try std.testing.expectEqualStrings("events", args.topic);
            try std.testing.expect(args.common.verbose);
            try std.testing.expect(args.isCsv());
            try std.testing.expectEqualStrings("id", args.key_col.?);
            try std.testing.expectEqualStrings("h:1", args.common.bootstrap.?);
            try std.testing.expectEqualStrings("x.properties", args.common.config_path.?);
            try std.testing.expectEqual(@as(usize, 2), args.headers.len);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "quiet flag parses and conflicts with verbose" {
    const alloc = std.heap.page_allocator;
    const result = parseProduce(alloc, &.{ "-q", "demo" });
    switch (result) {
        .ok => |args| try std.testing.expect(args.common.quiet),
        else => return error.TestUnexpectedResult,
    }
    const long = parseConsume(alloc, &.{ "--quiet", "demo" });
    switch (long) {
        .ok => |args| try std.testing.expect(args.common.quiet),
        else => return error.TestUnexpectedResult,
    }
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "-q", "-v", "demo" }), "--quiet cannot be combined with --verbose");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "-q", "-v", "demo" }), "--quiet cannot be combined with --verbose");
}

test "format option sets common.format" {
    const alloc = std.heap.page_allocator;
    const value = parseProduce(alloc, &.{ "--format", "value", "demo" });
    switch (value) {
        .ok => |args| try std.testing.expectEqual(Format.value, args.common.format),
        else => return error.TestUnexpectedResult,
    }
    const tsv = parseProduce(alloc, &.{ "--format=tsv", "demo" });
    switch (tsv) {
        .ok => |args| try std.testing.expectEqual(Format.tsv, args.common.format),
        else => return error.TestUnexpectedResult,
    }
    const json = parseProduce(alloc, &.{ "--json", "demo" });
    switch (json) {
        .ok => |args| try std.testing.expectEqual(Format.json, args.common.format),
        else => return error.TestUnexpectedResult,
    }
    const csv = parseProduce(alloc, &.{ "--csv", "demo" });
    switch (csv) {
        .ok => |args| {
            try std.testing.expectEqual(Format.csv, args.common.format);
            try std.testing.expect(args.isCsv());
        },
        else => return error.TestUnexpectedResult,
    }
    const repeat = parseProduce(alloc, &.{ "--format", "json", "--json", "demo" });
    switch (repeat) {
        .ok => |args| try std.testing.expectEqual(Format.json, args.common.format),
        else => return error.TestUnexpectedResult,
    }
}

test "format conflicts and bad values are errors" {
    const alloc = std.heap.page_allocator;
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--format", "x", "demo" }), "--format: 'x' is not a format (want value, tsv, json, or csv)");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--format", "demo" }), "--format: 'demo' is not a format (want value, tsv, json, or csv)");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{"--format"}), "--format requires a value");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--csv", "--json", "demo" }), "--csv cannot be combined with --json");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--json", "--format", "tsv", "demo" }), "--json cannot be combined with --format tsv");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--format", "tsv", "--json", "demo" }), "--format tsv cannot be combined with --json");
    const csv_key = parseProduce(alloc, &.{ "--format", "csv", "--key", "id", "demo" });
    switch (csv_key) {
        .ok => |args| try std.testing.expect(args.isCsv()),
        else => return error.TestUnexpectedResult,
    }
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "--format", "csv", "demo" }), "--format csv is only valid when producing");
    const value_consume = parseConsume(alloc, &.{ "--format", "value", "demo" });
    switch (value_consume) {
        .ok => |args| try std.testing.expectEqual(Format.value, args.common.format),
        else => return error.TestUnexpectedResult,
    }
}

test "mode flags are split from arguments" {
    const alloc = std.heap.page_allocator;
    const before = splitMode(alloc, &.{ "-c", "events" });
    switch (before) {
        .ok => |result| {
            try std.testing.expectEqual(Mode.consume, result.mode);
            try std.testing.expectEqualSlices([]const u8, &.{"events"}, result.rest);
        },
        else => return error.TestUnexpectedResult,
    }

    const after = splitMode(alloc, &.{ "events", "-c" });
    switch (after) {
        .ok => |result| {
            try std.testing.expectEqual(Mode.consume, result.mode);
            try std.testing.expectEqualSlices([]const u8, &.{"events"}, result.rest);
        },
        else => return error.TestUnexpectedResult,
    }

    const duplicate = splitMode(alloc, &.{ "-c", "--consume", "events" });
    switch (duplicate) {
        .ok => |result| {
            try std.testing.expectEqual(Mode.consume, result.mode);
            try std.testing.expectEqualSlices([]const u8, &.{"events"}, result.rest);
        },
        else => return error.TestUnexpectedResult,
    }

    const option_value = splitMode(alloc, &.{ "--csv", "--key", "-c", "events" });
    switch (option_value) {
        .ok => |result| {
            try std.testing.expectEqual(Mode.produce, result.mode);
            try std.testing.expectEqualSlices([]const u8, &.{ "--csv", "--key", "-c", "events" }, result.rest);
        },
        else => return error.TestUnexpectedResult,
    }

    try expectErr(ModeSplit, splitMode(alloc, &.{ "-c", "-i", "events" }), "--consume cannot be combined with --install");

    const produce = splitMode(alloc, &.{"consume"});
    switch (produce) {
        .ok => |result| {
            try std.testing.expectEqual(Mode.produce, result.mode);
            try std.testing.expectEqualSlices([]const u8, &.{"consume"}, result.rest);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "consume parser accepts separate option values" {
    const result = parseConsume(std.heap.page_allocator, &.{
        "--offset", "1", "--partition", "2", "-n", "3", "-t", "4", "--json", "-bh:1", "events",
    });
    switch (result) {
        .ok => |args| {
            try std.testing.expectEqual(@as(i64, 1), args.offset);
            try std.testing.expectEqual(@as(i32, 2), args.partition.?);
            try std.testing.expectEqual(@as(u64, 3), args.max_records.?);
            try std.testing.expectEqual(@as(u64, 4), args.idle_ms.?);
            try std.testing.expectEqual(Format.json, args.common.format);
            try std.testing.expectEqualStrings("h:1", args.common.bootstrap.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "consume parser accepts attached and equals spellings including zero" {
    const result = parseConsume(std.heap.page_allocator, &.{
        "--offset=0", "--partition=0", "-n0", "-t0", "events",
    });
    switch (result) {
        .ok => |args| {
            try std.testing.expectEqual(consumer.Options.Start.offset, args.start);
            try std.testing.expectEqual(@as(i64, 0), args.offset);
            try std.testing.expectEqual(@as(i32, 0), args.partition.?);
            try std.testing.expectEqual(@as(u64, 0), args.max_records.?);
            try std.testing.expectEqual(@as(u64, 0), args.idle_ms.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "consume parser accepts long spellings, durations, -B and --follow" {
    const result = parseConsume(std.heap.page_allocator, &.{
        "-B", "--max=7", "--idle", "3s", "events",
    });
    switch (result) {
        .ok => |args| {
            try std.testing.expectEqual(consumer.Options.Start.earliest, args.start);
            try std.testing.expectEqual(@as(u64, 7), args.max_records.?);
            try std.testing.expectEqual(@as(u64, 3000), args.idle_ms.?);
        },
        else => return error.TestUnexpectedResult,
    }
    const follow = parseConsume(std.heap.page_allocator, &.{ "--follow", "events" });
    switch (follow) {
        .ok => |args| try std.testing.expect(args.follow),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(?u64, 500), parseDurationMs("500ms"));
    try std.testing.expectEqual(@as(?u64, 60_000), parseDurationMs("1m"));
    try std.testing.expectEqual(@as(?u64, 3_600_000), parseDurationMs("1h"));
    try std.testing.expectEqual(@as(?u64, 250), parseDurationMs("250"));
    try std.testing.expectEqual(@as(?u64, null), parseDurationMs("3x"));
    try std.testing.expectEqual(@as(?u64, null), parseDurationMs("s"));
    try std.testing.expectEqual(@as(?u64, null), parseDurationMs("-5"));
}

test "parser reports exact argument errors" {
    const alloc = std.heap.page_allocator;
    try expectErr(ProduceArgs, parseProduce(alloc, &.{}), "missing TOPIC");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{""}), "TOPIC must not be empty");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{"--bogus"}), "unknown option '--bogus'");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{"-H"}), "-H requires a value");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "-H", "nocolon", "demo" }), "malformed header 'nocolon' (want 'name: value')");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{"--key"}), "--key requires a value");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--key", "id", "demo" }), "--key requires --csv");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--csv", "--json", "demo" }), "--csv cannot be combined with --json");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "a", "b" }), "unexpected argument 'b' (only one TOPIC is allowed)");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{"-b"}), "-b requires a value");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--bootstrap=", "demo" }), "--bootstrap must not be empty");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{"--config"}), "--config requires a value");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--offset", "1", "demo" }), "'--offset' is a consume option; use 'kite -c [OPTIONS] TOPIC'");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "-n5", "demo" }), "'-n' is a consume option; use 'kite -c [OPTIONS] TOPIC'");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{}), "missing TOPIC");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{"--offset"}), "--offset requires a value");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "--offset", "abc", "demo" }), "--offset: 'abc' is not a non-negative integer");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "--partition", "x", "demo" }), "--partition: 'x' is not a non-negative integer");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "-n", "x", "demo" }), "-n: 'x' is not a non-negative integer");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "--max", "x", "demo" }), "--max: 'x' is not a non-negative integer");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "-t", "-5", "demo" }), "-t: '-5' is not a duration (want e.g. 3000, 3s, 500ms, 1m)");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "--idle", "abc", "demo" }), "--idle: 'abc' is not a duration (want e.g. 3000, 3s, 500ms, 1m)");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "--from-beginning", "--offset", "1", "demo" }), "--offset cannot be combined with --from-beginning");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "--offset", "1", "--from-beginning", "demo" }), "--offset cannot be combined with --from-beginning");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "-f", "--idle", "1s", "demo" }), "--follow cannot be combined with --idle");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "-H", "a: b", "demo" }), "'-H' is a produce option and is not valid with -c");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "--csv", "demo" }), "'--csv' is a produce option and is not valid with -c");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "-q", "-v", "demo" }), "--quiet cannot be combined with --verbose");
}

test "help is detected in argument order" {
    try std.testing.expectEqual(Result(ProduceArgs).help, parseProduce(std.heap.page_allocator, &.{ "demo", "--help" }));
    try std.testing.expectEqual(Result(ConsumeArgs).help, parseConsume(std.heap.page_allocator, &.{ "demo", "--help" }));
    try expectErr(ProduceArgs, parseProduce(std.heap.page_allocator, &.{ "--bogus", "--help" }), "unknown option '--bogus'");
    try expectErr(ConsumeArgs, parseConsume(std.heap.page_allocator, &.{ "--bogus", "--help" }), "unknown option '--bogus'");
}

test "help text stays plain and narrow" {
    try std.testing.expect(produce_help.len != consume_help.len);
    try std.testing.expect(std.mem.indexOf(u8, consume_help, "-c, --consume") == null);
    for ([_][]const u8{ produce_help, consume_help, install_help }) |page| {
        try std.testing.expect(std.mem.indexOf(u8, page, "-h, --help") != null);
        var lines = std.mem.splitScalar(u8, page, '\n');
        while (lines.next()) |line| {
            try std.testing.expect(line.len <= 80);
            try std.testing.expect(std.mem.indexOfScalar(u8, line, '\t') == null);
            try std.testing.expect(std.mem.indexOfScalar(u8, line, 0x1b) == null);
        }
    }
}
