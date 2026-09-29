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
    /// Cluster selected by the @NAME positional.
    target: ?[]const u8 = null,
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

pub const version = "0.3.0";

pub const Command = enum {
    produce,
    consume,
    cluster,
    topic,
    update,
};

pub const ClusterAction = enum { pick, list, set, init };

pub const ClusterArgs = struct {
    action: ClusterAction = .pick,
    name: ?[]const u8 = null,
    common: Common = .{},
};

pub const TopicAction = enum { list, create, delete, update };

pub const TopicArgs = struct {
    action: TopicAction = .list,
    topics: []const []const u8 = &.{},
    /// null = not given (create uses the broker default, -1).
    partitions: ?i32 = null,
    /// null = not given (create uses the broker default, -1).
    replication_factor: ?i16 = null,
    set: []const protocol.ConfigEntry = &.{},
    unset: []const []const u8 = &.{},
    all: bool = false,
    yes: bool = false,
    if_exists: bool = false,
    if_not_exists: bool = false,
    common: Common = .{},
};

pub const UpdateArgs = struct {
    /// Release tag to install; null = the latest release.
    version: ?[]const u8 = null,
};

pub const CommandSplit = struct {
    command: Command,
    rest: []const []const u8,
};

fn matchCommand(arg: []const u8) ?Command {
    if (std.mem.eql(u8, arg, "produce") or std.mem.eql(u8, arg, "p")) return .produce;
    if (std.mem.eql(u8, arg, "consume") or std.mem.eql(u8, arg, "c")) return .consume;
    if (std.mem.eql(u8, arg, "cluster")) return .cluster;
    if (std.mem.eql(u8, arg, "topic")) return .topic;
    if (std.mem.eql(u8, arg, "update")) return .update;
    return null;
}

const command_names = [_][]const u8{ "produce", "consume", "cluster", "topic", "update" };

/// Closest command name when exactly one is within distance 2.
fn suggestCommand(name: []const u8) ?[]const u8 {
    if (name.len < 2 or name.len > 64) return null;
    var best: ?[]const u8 = null;
    var best_dist: usize = 3;
    var tie = false;
    for (&command_names) |candidate| {
        const dist = editDistance(name, candidate);
        if (dist == 0) continue;
        if (dist < best_dist) {
            best = candidate;
            best_dist = dist;
            tie = false;
        } else if (dist == best_dist) {
            tie = true;
        }
    }
    if (best_dist > 2 or tie) return null;
    return best;
}

/// The first argument that is not a top-level -h/--help/-V/--version flag
/// names the command; everything after it is left for the command's parser.
pub fn splitCommand(alloc: std.mem.Allocator, args: []const []const u8) Result(CommandSplit) {
    var i: usize = 0;
    var want_help = false;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            want_help = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "-V") or std.mem.eql(u8, arg, "--version")) continue;
        if (matchCommand(arg)) |command|
            return .{ .ok = .{ .command = command, .rest = args[i + 1 ..] } };
        return .{ .err = firstArgError(alloc, arg) };
    }
    if (want_help) return .help;
    return .{ .err = "missing command (want produce, consume, cluster, or topic)" };
}

/// Migration errors for the 0.1 flag-mode spellings and the removed
/// config/targets commands, then the generic diagnostics.
fn firstArgError(alloc: std.mem.Allocator, arg: []const u8) []const u8 {
    if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--consume"))
        return errMsg(alloc, "'{s}' is now a command: kite consume [@CLUSTER] TOPIC", .{arg});
    if (std.mem.eql(u8, arg, "--show-config"))
        return "'--show-config' was removed; use 'kite cluster list' to see the selected cluster";
    if (std.mem.eql(u8, arg, "config"))
        return "'kite config' was removed; use 'kite cluster list' to see the selected cluster";
    if (std.mem.eql(u8, arg, "--targets"))
        return "'--targets' is now: kite cluster list";
    if (std.mem.eql(u8, arg, "targets"))
        return "'kite targets' is now: kite cluster list";
    if (std.mem.eql(u8, arg, "--target") or std.mem.startsWith(u8, arg, "--target="))
        return "'--target NAME' was replaced by the @NAME positional (e.g. @prod)";
    if (arg.len > 0 and arg[0] == '-')
        return errMsg(alloc, "unknown option '{s}'", .{arg});
    if (suggestCommand(arg)) |candidate|
        return errMsg(alloc, "unknown command '{s}'; did you mean '{s}'?", .{ arg, candidate });
    return errMsg(alloc, "unknown command '{s}' (kite now needs a command: kite produce {s})", .{ arg, arg });
}

pub const produce_usage =
    "Usage: kite produce [OPTIONS] [@CLUSTER] TOPIC\n" ++
    "Try 'kite produce --help' for examples.\n";

pub const consume_usage =
    "Usage: kite consume [OPTIONS] [@CLUSTER] TOPIC\n" ++
    "Try 'kite consume --help' for examples.\n";

pub const overview_usage =
    "Usage: kite COMMAND [OPTIONS] [@CLUSTER] [TOPIC]\n" ++
    "Try 'kite --help' for the list of commands.\n";

const config_help =
    "Configuration:\n" ++
    "  Flags override environment variables, which override the first\n" ++
    "  kite.yaml or kite.properties found in: ./, $XDG_CONFIG_HOME/kite/,\n" ++
    "  ~/.config/kite/ (kite.yaml wins in each directory).\n" ++
    "  Environment: BOOTSTRAP_SERVERS, SECURITY_PROTOCOL,\n" ++
    "  SASL_MECHANISM, SASL_USERNAME, SASL_PASSWORD,\n" ++
    "  SSL_TRUSTSTORE_LOCATION, KAFKA_PROPERTIES (path to a config file),\n" ++
    "  KITE_TARGET (named cluster).\n" ++
    "  kite.yaml's `clusters:` maps a name to settings; `default:` picks\n" ++
    "  the cluster when @NAME/KITE_TARGET is absent.\n" ++
    "  Templates are in examples/config/.\n";

pub const overview_help =
    "kite - Ultra-lightweight Kafka CLI: produce, consume, and manage topics\n" ++
    "\n" ++
    "Usage:\n" ++
    "  kite produce [OPTIONS] [@CLUSTER] TOPIC   Write stdin records to TOPIC.\n" ++
    "  kite consume [OPTIONS] [@CLUSTER] TOPIC   Read TOPIC to stdout.\n" ++
    "  kite topic [ACTION] [OPTIONS] [@CLUSTER] [TOPIC...]\n" ++
    "                                            List, create, delete, update topics.\n" ++
    "  kite cluster [list|set NAME|init]         Pick or set the current cluster.\n" ++
    "  kite update [VERSION]                     Install the latest kite release.\n" ++
    "  kite --help                               Show this help.\n" ++
    "  kite --version                            Print the version.\n" ++
    "\n" ++
    "'p' and 'c' are short for 'produce' and 'consume'. @NAME picks a\n" ++
    "cluster from kite.yaml for this run only.\n" ++
    "\n" ++
    "Examples:\n" ++
    "  kite produce events < examples/data/lines.txt\n" ++
    "  kite consume -B -n 1 --idle 3s events\n" ++
    "  kite consume @prod events\n" ++
    "  kite topic list\n" ++
    "  kite topic create -p 6 events\n" ++
    "  kite cluster\n" ++
    "  kite cluster set prod\n" ++
    "\n" ++
    "  'kite cluster set NAME' stores the current cluster in\n" ++
    "  $XDG_CONFIG_HOME/kite/current (default ~/.config/kite/current).\n" ++
    "  Precedence: @NAME, KITE_TARGET, current, then kite.yaml `default:`.\n" ++
    "\n" ++
    config_help;

pub const produce_help =
    "kite produce - Write stdin records to a Kafka topic\n" ++
    "\n" ++
    "Usage:\n" ++
    "  kite produce [OPTIONS] [@CLUSTER] TOPIC   ('p' works too)\n" ++
    "\n" ++
    "Options:\n" ++
    "  -b, --bootstrap HOSTS\n" ++
    "                        Comma-separated host:port brokers.\n" ++
    "  --config FILE         Read this properties file instead of searching.\n" ++
    "  @NAME                 Use cluster NAME from kite.yaml for this\n" ++
    "                        run only (also $KITE_TARGET).\n" ++
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
    "  kite produce events < examples/data/lines.txt\n" ++
    "  kite produce -b localhost:9092 events < examples/data/lines.txt\n" ++
    "  kite produce -H 'source: import' events < examples/data/headers.tsv\n" ++
    "  kite produce --csv --key user_id events < examples/data/events.csv\n" ++
    "  kite produce @prod events < examples/data/lines.txt\n" ++
    "  kite consume --json src | kite produce --json dst\n" ++
    "\n" ++
    config_help;

pub const consume_help =
    "kite consume - Read Kafka records to stdout\n" ++
    "\n" ++
    "Usage:\n" ++
    "  kite consume [OPTIONS] [@CLUSTER] TOPIC   ('c' works too)\n" ++
    "\n" ++
    "Options:\n" ++
    "  -b, --bootstrap HOSTS\n" ++
    "                        Comma-separated host:port brokers.\n" ++
    "  --config FILE         Read this properties file instead of searching.\n" ++
    "  @NAME                 Use cluster NAME from kite.yaml for this\n" ++
    "                        run only (also $KITE_TARGET).\n" ++
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
    "  kite consume events\n" ++
    "  kite consume -B -n 1 --idle 3s events\n" ++
    "  kite consume --partition 0 --offset 42 -n 10 --idle 3s events\n" ++
    "  kite consume -B --json events | jq -c .value\n" ++
    "  kite consume @prod events\n" ++
    "\n" ++
    config_help;

pub const topic_usage =
    "Usage: kite topic [list|create|delete|update] [OPTIONS] [@CLUSTER] [TOPIC...]\n" ++
    "Try 'kite topic --help' for examples.\n";

pub const topic_help =
    "kite topic - List, create, delete, and update Kafka topics\n" ++
    "\n" ++
    "Usage:\n" ++
    "  kite topic [list] [OPTIONS] [@CLUSTER]   List topics (default action).\n" ++
    "  kite topic create [OPTIONS] TOPIC        Create TOPIC.\n" ++
    "  kite topic delete [OPTIONS] TOPIC...     Delete one or more topics.\n" ++
    "  kite topic update [OPTIONS] TOPIC        Grow partitions or alter configs.\n" ++
    "\n" ++
    "Options:\n" ++
    "  -b, --bootstrap HOSTS\n" ++
    "                        Comma-separated host:port brokers.\n" ++
    "  --config FILE         Read this properties file instead of searching.\n" ++
    "  @NAME                 Use cluster NAME from kite.yaml for this\n" ++
    "                        run only (also $KITE_TARGET).\n" ++
    "  -a, --all             Include internal topics (list only).\n" ++
    "  -p, --partitions N    Partition count (create/update; grow only).\n" ++
    "  -r, --replication-factor N\n" ++
    "                        Replication factor (create only).\n" ++
    "  -s, --set KEY=VALUE   Set a topic config (create/update; repeatable).\n" ++
    "  --unset KEY           Remove a topic config (update only; repeatable).\n" ++
    "  -y, --yes             Delete without the confirmation prompt.\n" ++
    "  --if-exists           A missing topic is a note, not an error (delete).\n" ++
    "  --if-not-exists       An existing topic is a note, not an error (create).\n" ++
    "  --json                One JSON object per topic, fields name,\n" ++
    "                        partitions, replication_factor, internal.\n" ++
    "  -q, --quiet           Suppress notes on stderr.\n" ++
    "  -v, --verbose         Write diagnostics to stderr.\n" ++
    "  -h, --help            Show this help and exit.\n" ++
    "\n" ++
    "'list' prints a table of names, partitions and replication factors\n" ++
    "on a terminal, or one name per line otherwise; -a includes internal\n" ++
    "topics.\n" ++
    "'delete' confirms on a terminal; scripts pass -y. 'update' grows\n" ++
    "partitions first, then applies --set/--unset (one is required).\n" ++
    "\n" ++
    "Examples:\n" ++
    "  kite topic list --json | jq -r .name\n" ++
    "  kite topic create -p 6 -s retention.ms=86400000 events\n" ++
    "  kite topic update --set cleanup.policy=compact events\n" ++
    "  kite topic delete -y events\n" ++
    "\n" ++
    config_help;

pub const cluster_usage =
    "Usage: kite cluster [list|set NAME|init] [OPTIONS]\n" ++
    "Try 'kite cluster --help' for details.\n";

pub const cluster_help =
    "kite cluster - Pick, list, or set the cluster kite uses\n" ++
    "\n" ++
    "Usage:\n" ++
    "  kite cluster                  Interactive picker (terminals only).\n" ++
    "  kite cluster list             Print the clusters in kite.yaml.\n" ++
    "  kite cluster set NAME         Make NAME the current cluster.\n" ++
    "  kite cluster init             Add a cluster by answering questions.\n" ++
    "\n" ++
    "Options:\n" ++
    "  --config FILE         Read this config file instead of searching.\n" ++
    "  --json                JSON output (list only).\n" ++
    "  -q, --quiet           Suppress notes on stderr.\n" ++
    "  -v, --verbose         Write diagnostics to stderr.\n" ++
    "  -h, --help            Show this help and exit.\n" ++
    "\n" ++
    "With no subcommand on a terminal, kite draws a picker (arrows or j/k,\n" ++
    "Enter to choose, q to quit); when stdin or stderr is not a terminal it\n" ++
    "behaves like 'kite cluster list' so scripts never hang. 'list' prints\n" ++
    "one cluster name per line, sorted, with '*' after the current one\n" ++
    "(before it on a terminal); the JSON form is\n" ++
    "{\"file\":..,\"current\":..,\"clusters\":[..]}. Only the file's\n" ++
    "`clusters:` keys are read, so it works even when a cluster is\n" ++
    "incomplete; kite never connects to a broker.\n" ++
    "\n" ++
    "'init' asks for a name, bootstrap servers and SASL credentials on\n" ++
    "stderr — a SASL username implies a Confluent Cloud-style\n" ++
    "SASL_SSL/PLAIN cluster, empty means PLAINTEXT — then splices the\n" ++
    "cluster into kite.yaml without disturbing the rest of the file (an\n" ++
    "existing name may be overwritten after confirmation) and makes it\n" ++
    "the current cluster. Other protocols, SCRAM, and CA bundle paths\n" ++
    "are set by editing kite.yaml.\n" ++
    "\n" ++
    "The current cluster is stored in $XDG_CONFIG_HOME/kite/current\n" ++
    "(default ~/.config/kite/current) and is used when neither @NAME nor\n" ++
    "$KITE_TARGET selects one; it wins over the file's `default:`.\n" ++
    "\n" ++
    "Examples:\n" ++
    "  kite cluster\n" ++
    "  kite cluster list\n" ++
    "  kite cluster list --json | jq -r '.clusters[]'\n" ++
    "  kite cluster set prod\n" ++
    "  kite cluster init\n" ++
    "\n" ++
    config_help;

pub const update_usage =
    "Usage: kite update [VERSION]\n" ++
    "Try 'kite update --help' for details.\n";

pub const update_help =
    "kite update - Install the latest kite release from GitHub\n" ++
    "\n" ++
    "Usage:\n" ++
    "  kite update              Replace this binary with the latest release.\n" ++
    "  kite update VERSION      Install a specific release (e.g. v0.2.0).\n" ++
    "\n" ++
    "Options:\n" ++
    "  -h, --help            Show this help and exit.\n" ++
    "\n" ++
    "kite asks github.com/addisonhuddy/kite for its latest release and, when\n" ++
    "that differs from this binary (" ++ version ++ "), runs the project's install.sh\n" ++
    "with --bin-dir set to the directory this binary lives in, so the new\n" ++
    "version replaces it in place. The installer verifies the download\n" ++
    "against the release's SHA256SUMS and uses sudo only when that\n" ++
    "directory is not writable. Needs /bin/sh and curl or wget.\n" ++
    "\n" ++
    "Examples:\n" ++
    "  kite update\n" ++
    "  kite update v0.2.0\n";

/// A release tag: letters, digits, '.', '-', '_'; a leading 'v' is added
/// when missing so `kite update 0.3.0` means v0.3.0.
fn releaseTag(alloc: std.mem.Allocator, arg: []const u8) ?[]const u8 {
    if (arg.len == 0 or arg.len > 64) return null;
    for (arg) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '.' and ch != '-' and ch != '_') return null;
    if (arg[0] == 'v') return arg;
    if (!std.ascii.isDigit(arg[0])) return null;
    return std.fmt.allocPrint(alloc, "v{s}", .{arg}) catch null;
}

pub fn parseUpdate(alloc: std.mem.Allocator, args: []const []const u8) Result(UpdateArgs) {
    var parsed: UpdateArgs = .{};
    for (args) |arg| {
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) return .help;
        if (arg.len > 0 and arg[0] == '-')
            return .{ .err = unknownOption(alloc, .update, arg) };
        if (parsed.version != null)
            return errorResult(UpdateArgs, alloc, "unexpected argument '{s}'", .{arg});
        if (std.mem.eql(u8, arg, "latest")) continue;
        parsed.version = releaseTag(alloc, arg) orelse
            return errorResult(UpdateArgs, alloc, "'{s}' is not a release tag (want e.g. v0.3.0)", .{arg});
    }
    return .{ .ok = parsed };
}

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

const produce_options = [_][]const u8{ "--bootstrap", "--config", "--format", "--json", "--csv", "--key", "--quiet", "--verbose", "--help" };
const consume_options = [_][]const u8{ "--bootstrap", "--config", "--from-beginning", "--offset", "--partition", "--max", "--idle", "--follow", "--format", "--json", "--quiet", "--verbose", "--help" };
const cluster_options = [_][]const u8{ "--config", "--json", "--format", "--quiet", "--verbose", "--help" };
const update_options = [_][]const u8{"--help"};
const topic_options = [_][]const u8{ "--bootstrap", "--config", "--format", "--json", "--all", "--partitions", "--replication-factor", "--set", "--unset", "--yes", "--if-exists", "--if-not-exists", "--quiet", "--verbose", "--help" };

/// Damerau-Levenshtein distance (adjacent transposition counts as one edit).
/// Inputs are capped so the DP matrix lives on the stack.
fn editDistance(a: []const u8, b: []const u8) usize {
    if (a.len > 64 or b.len > 64) return 256;
    var d: [66][66]u16 = undefined;
    for (0..a.len + 1) |i| d[i][0] = @intCast(i);
    for (0..b.len + 1) |j| d[0][j] = @intCast(j);
    for (1..a.len + 1) |i| {
        for (1..b.len + 1) |j| {
            const cost: u16 = if (a[i - 1] == b[j - 1]) 0 else 1;
            var best = @min(@min(d[i - 1][j] + 1, d[i][j - 1] + 1), d[i - 1][j - 1] + cost);
            if (i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1])
                best = @min(best, d[i - 2][j - 2] + 1);
            d[i][j] = best;
        }
    }
    return d[a.len][b.len];
}

/// Closest valid long option for `name` when exactly one is within distance 2.
fn suggestOption(command: Command, name: []const u8) ?[]const u8 {
    if (name.len < 3 or !std.mem.startsWith(u8, name, "--") or name.len > 64) return null;
    const candidates: []const []const u8 = switch (command) {
        .produce => &produce_options,
        .consume => &consume_options,
        .cluster => &cluster_options,
        .topic => &topic_options,
        .update => &update_options,
    };
    var best: ?[]const u8 = null;
    var best_dist: usize = 3;
    var tie = false;
    for (candidates) |candidate| {
        const dist = editDistance(name, candidate);
        if (dist == 0) continue;
        if (dist < best_dist) {
            best = candidate;
            best_dist = dist;
            tie = false;
        } else if (dist == best_dist) {
            tie = true;
        }
    }
    if (best_dist > 2 or tie) return null;
    return best;
}

fn optionName(arg: []const u8) []const u8 {
    if (std.mem.indexOfScalar(u8, arg, '=')) |eq| return arg[0..eq];
    if (arg.len > 2 and arg[1] != '-') return arg[0..2];
    return arg;
}

fn isOneOf(name: []const u8, list: []const []const u8) bool {
    for (list) |candidate| if (std.mem.eql(u8, name, candidate)) return true;
    return false;
}

fn errMsg(alloc: std.mem.Allocator, comptime fmt: []const u8, args: anytype) []const u8 {
    return std.fmt.allocPrint(alloc, fmt, args) catch "out of memory";
}

fn unknownOption(alloc: std.mem.Allocator, command: Command, arg: []const u8) []const u8 {
    const name = optionName(arg);
    switch (command) {
        .produce => if (isOneOf(name, &consume_only))
            return errMsg(alloc, "'{s}' is a consume option; use 'kite consume [OPTIONS] TOPIC'", .{name}),
        .consume => if (isOneOf(name, &produce_only))
            return errMsg(alloc, "'{s}' is a produce option; use 'kite produce [OPTIONS] TOPIC'", .{name}),
        .cluster, .topic, .update => {},
    }
    if (suggestOption(command, name)) |candidate|
        return errMsg(alloc, "unknown option '{s}' (did you mean '{s}'?)", .{ arg, candidate });
    return errMsg(alloc, "unknown option '{s}'", .{arg});
}

const CommonStep = union(enum) { ok, err: []const u8 };

/// Handle options shared by produce and consume. Returns null when `arg`
/// is not a shared option; `i` is advanced past any separate value.
fn parseCommon(alloc: std.mem.Allocator, common: *Common, args: []const []const u8, i: *usize) ?CommonStep {
    const arg = args[i.*];
    if (std.mem.eql(u8, arg, "-v") or std.mem.eql(u8, arg, "--verbose")) {
        common.verbose = true;
    } else if (std.mem.eql(u8, arg, "-q") or std.mem.eql(u8, arg, "--quiet")) {
        common.quiet = true;
    } else if (std.mem.eql(u8, arg, "--json")) {
        if (setFormat(alloc, common, .json, "--json")) |m| return .{ .err = m };
    } else if (std.mem.eql(u8, arg, "--format")) {
        i.* += 1;
        if (i.* >= args.len) return .{ .err = "--format requires a value" };
        const value = args[i.*];
        const spelling = std.fmt.allocPrint(alloc, "--format {s}", .{value}) catch return .{ .err = "out of memory" };
        if (parseFormatArg(alloc, common, value, spelling)) |m| return .{ .err = m };
    } else if (std.mem.startsWith(u8, arg, "--format=")) {
        if (parseFormatArg(alloc, common, arg["--format=".len..], arg)) |m| return .{ .err = m };
    } else if (std.mem.eql(u8, arg, "-b") or std.mem.eql(u8, arg, "--bootstrap")) {
        i.* += 1;
        if (i.* >= args.len) return .{ .err = errMsg(alloc, "{s} requires a value", .{arg}) };
        common.bootstrap = args[i.*];
    } else if (std.mem.startsWith(u8, arg, "--bootstrap=")) {
        common.bootstrap = arg["--bootstrap=".len..];
    } else if (std.mem.startsWith(u8, arg, "-b") and arg.len > 2) {
        common.bootstrap = arg[2..];
    } else if (std.mem.eql(u8, arg, "--config")) {
        i.* += 1;
        if (i.* >= args.len) return .{ .err = "--config requires a value" };
        common.config_path = args[i.*];
    } else if (std.mem.startsWith(u8, arg, "--config=")) {
        common.config_path = arg["--config=".len..];
    } else if (std.mem.eql(u8, arg, "--target") or std.mem.startsWith(u8, arg, "--target=")) {
        return .{ .err = "'--target NAME' was replaced by the @NAME positional (e.g. @prod)" };
    } else if (arg.len > 0 and arg[0] == '@') {
        if (arg.len == 1) return .{ .err = "'@' must be followed by a cluster name (e.g. @prod)" };
        if (setTarget(alloc, common, arg[1..])) |m| return .{ .err = m };
    } else {
        return null;
    }
    return .ok;
}

/// Record a format choice; a different non-auto format already set is a
/// conflict reported with the user's own spellings.
fn setFormat(alloc: std.mem.Allocator, common: *Common, format: Format, spelling: []const u8) ?[]const u8 {
    if (common.format != .auto and common.format != format)
        return errMsg(alloc, "{s} cannot be combined with {s}", .{ common.format_spelling.?, spelling });
    common.format = format;
    if (common.format_spelling == null) common.format_spelling = spelling;
    return null;
}

/// Record a cluster choice; a different cluster already set is a conflict
/// reported with the user's own spellings.
fn setTarget(alloc: std.mem.Allocator, common: *Common, name: []const u8) ?[]const u8 {
    if (common.target) |prev| {
        if (!std.mem.eql(u8, prev, name))
            return errMsg(alloc, "@{s} cannot be combined with @{s}", .{ prev, name });
    }
    common.target = name;
    return null;
}

fn parseFormatArg(alloc: std.mem.Allocator, common: *Common, value: []const u8, spelling: []const u8) ?[]const u8 {
    const format = format_mod.parse(value) orelse
        return errMsg(alloc, "--format: '{s}' is not a format (want value, tsv, json, or csv)", .{value});
    return setFormat(alloc, common, format, spelling);
}

fn finishCommon(common: Common) ?[]const u8 {
    if (common.quiet and common.verbose) return "--quiet cannot be combined with --verbose";
    if (common.bootstrap) |b| if (b.len == 0) return "--bootstrap must not be empty";
    if (common.config_path) |p| if (p.len == 0) return "--config must not be empty";
    return null;
}

pub fn parseCluster(alloc: std.mem.Allocator, args: []const []const u8) Result(ClusterArgs) {
    var parsed: ClusterArgs = .{};
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) return .help;
        if (arg.len > 0 and arg[0] == '@')
            return .{ .err = "use 'kite cluster set NAME' to switch clusters" };
        if (parseCommon(alloc, &parsed.common, args, &i)) |step| {
            switch (step) {
                .err => |m| return .{ .err = m },
                .ok => continue,
            }
        }
        if (arg.len > 0 and arg[0] == '-')
            return .{ .err = unknownOption(alloc, .cluster, arg) };
        switch (parsed.action) {
            .pick => {
                if (std.mem.eql(u8, arg, "list")) {
                    parsed.action = .list;
                } else if (std.mem.eql(u8, arg, "set")) {
                    parsed.action = .set;
                } else if (std.mem.eql(u8, arg, "init")) {
                    parsed.action = .init;
                } else {
                    return errorResult(ClusterArgs, alloc, "unexpected argument '{s}'", .{arg});
                }
            },
            .list, .init => return errorResult(ClusterArgs, alloc, "unexpected argument '{s}'", .{arg}),
            .set => if (parsed.name == null) {
                parsed.name = arg;
            } else {
                return errorResult(ClusterArgs, alloc, "unexpected argument '{s}'", .{arg});
            },
        }
    }
    if (parsed.common.bootstrap != null)
        return errorResult(ClusterArgs, alloc, "--bootstrap is not valid with kite cluster", .{});
    switch (parsed.action) {
        .list => if (parsed.common.format != .auto and parsed.common.format != .json)
            return errorResult(ClusterArgs, alloc, "--format: only json is valid with kite cluster list", .{}),
        else => if (parsed.common.format != .auto)
            return errorResult(ClusterArgs, alloc, "--json is only valid with kite cluster list", .{}),
    }
    if (parsed.action == .set and parsed.name == null)
        return errorResult(ClusterArgs, alloc, "missing NAME", .{});
    if (parsed.action == .set and parsed.name.?.len == 0)
        return errorResult(ClusterArgs, alloc, "NAME must not be empty", .{});
    if (finishCommon(parsed.common)) |m| return .{ .err = m };
    return .{ .ok = parsed };
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
        if (parseCommon(alloc, &common, args, &i)) |step| {
            switch (step) {
                .err => |m| return .{ .err = m },
                .ok => continue,
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
            if (setFormat(alloc, &common, .csv, "--csv")) |m| return .{ .err = m };
        } else if (std.mem.eql(u8, arg, "--key")) {
            i += 1;
            if (i >= args.len) return errorResult(ProduceArgs, alloc, "--key requires a value", .{});
            key_col = args[i];
        } else if (std.mem.startsWith(u8, arg, "--key=")) {
            key_col = arg["--key=".len..];
        } else if (arg.len > 0 and arg[0] == '-') {
            return .{ .err = unknownOption(alloc, .produce, arg) };
        } else if (topic == null) {
            topic = arg;
        } else {
            return errorResult(ProduceArgs, alloc, "unexpected argument '{s}'", .{arg});
        }
    }

    const topic_name = topic orelse return errorResult(ProduceArgs, alloc, "missing TOPIC", .{});
    if (topic_name.len == 0) return errorResult(ProduceArgs, alloc, "TOPIC must not be empty", .{});
    if (key_col != null and common.format != .csv) return errorResult(ProduceArgs, alloc, "--key requires --csv", .{});
    if (finishCommon(common)) |m| return .{ .err = m };
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
        if (parseCommon(alloc, &parsed.common, args, &i)) |step| {
            switch (step) {
                .err => |m| return .{ .err = m },
                .ok => continue,
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
            return .{ .err = unknownOption(alloc, .consume, arg) };
        } else if (topic == null) {
            topic = arg;
        } else {
            return errorResult(ConsumeArgs, alloc, "unexpected argument '{s}'", .{arg});
        }
    }

    const topic_name = topic orelse return errorResult(ConsumeArgs, alloc, "missing TOPIC", .{});
    if (topic_name.len == 0) return errorResult(ConsumeArgs, alloc, "TOPIC must not be empty", .{});
    if (parsed.follow and parsed.idle_ms != null)
        return errorResult(ConsumeArgs, alloc, "--follow cannot be combined with --idle", .{});
    if (parsed.common.format == .csv)
        return errorResult(ConsumeArgs, alloc, "--format csv is only valid when producing", .{});
    if (finishCommon(parsed.common)) |m| return .{ .err = m };
    parsed.topic = topic_name;
    return .{ .ok = parsed };
}

/// `kite topic [ACTION] [OPTIONS] [@CLUSTER] [TOPIC...]` — the first
/// non-option positional names the action (default: list).
pub fn parseTopic(alloc: std.mem.Allocator, args: []const []const u8) Result(TopicArgs) {
    var parsed: TopicArgs = .{};
    var topics: std.ArrayListUnmanaged([]const u8) = .empty;
    var sets: std.ArrayListUnmanaged(protocol.ConfigEntry) = .empty;
    var unsets: std.ArrayListUnmanaged([]const u8) = .empty;
    var action_seen = false;
    // Bit per topic option, spelling recorded on first use — the message
    // reports the spelling the user typed. Bit order is the report order.
    const OptBit = struct {
        const all: u8 = 1;
        const partitions: u8 = 2;
        const rf: u8 = 4;
        const set: u8 = 8;
        const unset: u8 = 16;
        const yes: u8 = 32;
        const if_exists: u8 = 64;
        const if_not_exists: u8 = 128;
    };
    var seen: u8 = 0;
    var spells: [8][]const u8 = undefined;

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) return .help;
        if (parseCommon(alloc, &parsed.common, args, &i)) |step| {
            switch (step) {
                .err => |m| return .{ .err = m },
                .ok => continue,
            }
        }
        var value: ?[]const u8 = null;
        if (std.mem.eql(u8, arg, "-a") or std.mem.eql(u8, arg, "--all")) {
            parsed.all = true;
            seen |= OptBit.all;
            spells[0] = arg;
        } else if (std.mem.eql(u8, arg, "-y") or std.mem.eql(u8, arg, "--yes")) {
            parsed.yes = true;
            seen |= OptBit.yes;
            spells[5] = arg;
        } else if (std.mem.startsWith(u8, arg, "--if-")) {
            if (std.mem.eql(u8, arg["--if-".len..], "exists")) {
                parsed.if_exists = true;
                seen |= OptBit.if_exists;
                spells[6] = arg;
            } else if (std.mem.eql(u8, arg["--if-".len..], "not-exists")) {
                parsed.if_not_exists = true;
                seen |= OptBit.if_not_exists;
                spells[7] = arg;
            } else return .{ .err = unknownOption(alloc, .topic, arg) };
        } else if (optionValue(args, &i, &value, arg, "-p", "--partitions")) {
            const pval = value orelse
                return .{ .err = errCat(alloc, &.{ arg, " requires a value" }) };
            const v = parsePositive(alloc, "--partitions", pval, std.math.maxInt(i32));
            switch (v) {
                .ok => |n| parsed.partitions = @intCast(n),
                .err => |m| return .{ .err = m },
                .help => unreachable,
            }
            seen |= OptBit.partitions;
            spells[1] = arg[0..if (arg[1] == '-') 12 else 2];
        } else if (optionValue(args, &i, &value, arg, "-r", "--replication-factor")) {
            const rval = value orelse
                return .{ .err = errCat(alloc, &.{ arg, " requires a value" }) };
            const v = parsePositive(alloc, "--replication-factor", rval, std.math.maxInt(i16));
            switch (v) {
                .ok => |n| parsed.replication_factor = @intCast(n),
                .err => |m| return .{ .err = m },
                .help => unreachable,
            }
            seen |= OptBit.rf;
            spells[2] = arg[0..if (arg[1] == '-') 20 else 2];
        } else if (optionValue(args, &i, &value, arg, "-s", "--set")) {
            const v = value orelse
                return .{ .err = errCat(alloc, &.{ arg, " requires a value" }) };
            const eq = std.mem.indexOfScalar(u8, v, '=') orelse 0;
            if (eq == 0)
                return .{ .err = errCat(alloc, &.{ "--set: '", v, "' is not KEY=VALUE" }) };
            sets.append(alloc, .{ .name = v[0..eq], .value = v[eq + 1 ..] }) catch return .{ .err = "out of memory" };
            seen |= OptBit.set;
            spells[3] = arg[0..if (arg[1] == '-') 5 else 2];
        } else if (optionValue(args, &i, &value, arg, "", "--unset")) {
            const v = value orelse "";
            if (v.len == 0) return .{ .err = "--unset requires a config name" };
            unsets.append(alloc, v) catch return .{ .err = "out of memory" };
            seen |= OptBit.unset;
            spells[4] = arg[0..if (arg.len > 7) 7 else arg.len];
        } else if (arg.len > 0 and arg[0] == '-') {
            return .{ .err = unknownOption(alloc, .topic, arg) };
        } else if (!action_seen) {
            action_seen = true;
            if (std.mem.eql(u8, arg, "list")) {
                parsed.action = .list;
            } else if (std.mem.eql(u8, arg, "create")) {
                parsed.action = .create;
            } else if (std.mem.eql(u8, arg, "delete")) {
                parsed.action = .delete;
            } else if (std.mem.eql(u8, arg, "update")) {
                parsed.action = .update;
            } else {
                return .{ .err = errCat(alloc, &.{ "unknown topic action '", arg, "' (want list, create, delete, or update)" }) };
            }
        } else {
            topics.append(alloc, arg) catch return .{ .err = "out of memory" };
        }
    }
    parsed.topics = topics.items;
    parsed.set = sets.items;
    parsed.unset = unsets.items;

    // Options allowed per action; the message reports the spelling used.
    const disallowed: u8 = switch (parsed.action) {
        .list => OptBit.partitions | OptBit.rf | OptBit.set | OptBit.unset | OptBit.yes | OptBit.if_exists | OptBit.if_not_exists,
        .create => OptBit.all | OptBit.unset | OptBit.yes | OptBit.if_exists,
        .delete => OptBit.all | OptBit.partitions | OptBit.rf | OptBit.set | OptBit.unset | OptBit.if_not_exists,
        .update => OptBit.all | OptBit.rf | OptBit.yes | OptBit.if_exists | OptBit.if_not_exists,
    };
    const bad = seen & disallowed;
    if (bad != 0)
        return .{ .err = errCat(alloc, &.{ "'", spells[@ctz(bad)], "' is not valid with kite topic ", @tagName(parsed.action) }) };

    const extra: ?[]const u8 = switch (parsed.action) {
        .list => if (parsed.topics.len > 0) parsed.topics[0] else null,
        .create, .update => if (parsed.topics.len > 1) parsed.topics[1] else null,
        .delete => null,
    };
    if (extra) |x|
        return .{ .err = errCat(alloc, &.{ "unexpected argument '", x, "'" }) };
    if (parsed.action != .list and parsed.topics.len == 0)
        return .{ .err = "missing TOPIC" };
    for (parsed.topics) |t|
        if (t.len == 0) return .{ .err = "TOPIC must not be empty" };
    if (parsed.action == .update and parsed.partitions == null and
        parsed.set.len == 0 and parsed.unset.len == 0)
        return .{ .err = "nothing to update (want --partitions, --set, or --unset)" };
    switch (parsed.action) {
        .list => if (parsed.common.format != .auto and parsed.common.format != .json)
            return .{ .err = "--format: only json is valid with kite topic list" },
        else => if (parsed.common.format != .auto)
            return .{ .err = "--json is only valid with kite topic list" },
    }
    if (finishCommon(parsed.common)) |m| return .{ .err = m };
    return .{ .ok = parsed };
}

/// Join message parts; one shared non-generic path keeps the parser lean.
pub fn errCat(alloc: std.mem.Allocator, parts: []const []const u8) []const u8 {
    return std.mem.concat(alloc, u8, parts) catch "out of memory";
}

/// Match an option taking a value in any of its spellings
/// (`--opt VALUE`, `--opt=VALUE`, `-o VALUE`, `-oVALUE`). On match the
/// value lands in `value` and `i` advances past a separate value.
fn optionValue(
    args: []const []const u8,
    i: *usize,
    value: *?[]const u8,
    arg: []const u8,
    short: []const u8,
    long: []const u8,
) bool {
    if (std.mem.eql(u8, arg, long) or (short.len > 0 and std.mem.eql(u8, arg, short))) {
        if (i.* + 1 >= args.len) {
            value.* = null;
            return true;
        }
        i.* += 1;
        value.* = args[i.*];
        return true;
    }
    if (std.mem.startsWith(u8, arg, long) and arg.len > long.len and arg[long.len] == '=') {
        value.* = arg[long.len + 1 ..];
        return true;
    }
    if (short.len > 0 and std.mem.startsWith(u8, arg, short) and arg.len > short.len) {
        value.* = arg[short.len..];
        return true;
    }
    return false;
}

/// A positive integer no larger than `max`; 0 and negatives reject.
fn parsePositive(alloc: std.mem.Allocator, option: []const u8, value: []const u8, max: u64) Result(u64) {
    const n = std.fmt.parseInt(u64, value, 10) catch
        return .{ .err = errCat(alloc, &.{ option, ": '", value, "' is not a positive integer" }) };
    if (n == 0 or n > max)
        return .{ .err = errCat(alloc, &.{ option, ": '", value, "' is not a positive integer" }) };
    return .{ .ok = n };
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

test "misspelled long options get a suggestion" {
    const alloc = std.heap.page_allocator;
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "--from-begining", "demo" }), "unknown option '--from-begining' (did you mean '--from-beginning'?)");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--jsno", "demo" }), "unknown option '--jsno' (did you mean '--json'?)");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--verbos", "demo" }), "unknown option '--verbos' (did you mean '--verbose'?)");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--bogus", "demo" }), "unknown option '--bogus'");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "-x", "demo" }), "unknown option '-x'");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--version", "demo" }), "unknown option '--version'");
}

test "cluster command split and parsing" {
    const alloc = std.heap.page_allocator;
    switch (splitCommand(alloc, &.{ "cluster", "list" })) {
        .ok => |result| {
            try std.testing.expectEqual(Command.cluster, result.command);
            try std.testing.expectEqualSlices([]const u8, &.{"list"}, result.rest);
        },
        else => return error.TestUnexpectedResult,
    }

    switch (parseCluster(alloc, &.{ "--config", "x.yaml", "list", "--json", "-q" })) {
        .ok => |a| {
            try std.testing.expectEqual(ClusterAction.list, a.action);
            try std.testing.expectEqualStrings("x.yaml", a.common.config_path.?);
            try std.testing.expectEqual(Format.json, a.common.format);
            try std.testing.expect(a.common.quiet);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (parseCluster(alloc, &.{ "set", "prod" })) {
        .ok => |a| {
            try std.testing.expectEqual(ClusterAction.set, a.action);
            try std.testing.expectEqualStrings("prod", a.name.?);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (parseCluster(alloc, &.{})) {
        .ok => |a| try std.testing.expectEqual(ClusterAction.pick, a.action),
        else => return error.TestUnexpectedResult,
    }
    try expectErr(ClusterArgs, parseCluster(alloc, &.{"set"}), "missing NAME");
    try expectErr(ClusterArgs, parseCluster(alloc, &.{ "set", "a", "b" }), "unexpected argument 'b'");
    switch (parseCluster(alloc, &.{"init"})) {
        .ok => |a| try std.testing.expectEqual(ClusterAction.init, a.action),
        else => return error.TestUnexpectedResult,
    }
    try expectErr(ClusterArgs, parseCluster(alloc, &.{ "init", "x" }), "unexpected argument 'x'");
    try expectErr(ClusterArgs, parseCluster(alloc, &.{ "list", "demo" }), "unexpected argument 'demo'");
    try expectErr(ClusterArgs, parseCluster(alloc, &.{"demo"}), "unexpected argument 'demo'");
    try expectErr(ClusterArgs, parseCluster(alloc, &.{"@prod"}), "use 'kite cluster set NAME' to switch clusters");
    try expectErr(ClusterArgs, parseCluster(alloc, &.{ "-b", "h:1", "list" }), "--bootstrap is not valid with kite cluster");
    try expectErr(ClusterArgs, parseCluster(alloc, &.{ "list", "--format", "tsv" }), "--format: only json is valid with kite cluster list");
    try expectErr(ClusterArgs, parseCluster(alloc, &.{ "set", "prod", "--json" }), "--json is only valid with kite cluster list");
    try expectErr(ClusterArgs, parseCluster(alloc, &.{"--json"}), "--json is only valid with kite cluster list");
    try expectErr(ClusterArgs, parseCluster(alloc, &.{"--csv"}), "unknown option '--csv'");
}

test "@NAME positional selects the cluster" {
    const alloc = std.heap.page_allocator;
    switch (parseProduce(alloc, &.{ "@local-b", "events" })) {
        .ok => |a| {
            try std.testing.expectEqualStrings("local-b", a.common.target.?);
            try std.testing.expectEqualStrings("events", a.topic);
        },
        else => return error.TestUnexpectedResult,
    }
    // @NAME may appear before or after TOPIC; repeating the same name is fine.
    switch (parseConsume(alloc, &.{ "events", "@prod", "@prod" })) {
        .ok => |a| {
            try std.testing.expectEqualStrings("prod", a.common.target.?);
            try std.testing.expectEqualStrings("events", a.topic);
        },
        else => return error.TestUnexpectedResult,
    }
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "@", "events" }), "'@' must be followed by a cluster name (e.g. @prod)");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "@prod", "@dev", "events" }), "@prod cannot be combined with @dev");
}

test "the first argument names the command" {
    const alloc = std.heap.page_allocator;
    const cases = [_]struct { args: []const []const u8, command: Command, rest: []const []const u8 }{
        .{ .args = &.{ "produce", "events" }, .command = .produce, .rest = &.{"events"} },
        .{ .args = &.{ "p", "events" }, .command = .produce, .rest = &.{"events"} },
        .{ .args = &.{ "consume", "-B", "events" }, .command = .consume, .rest = &.{ "-B", "events" } },
        .{ .args = &.{ "c", "events" }, .command = .consume, .rest = &.{"events"} },
        .{ .args = &.{ "cluster", "set", "prod" }, .command = .cluster, .rest = &.{ "set", "prod" } },
        .{ .args = &.{ "produce", "@prod", "events" }, .command = .produce, .rest = &.{ "@prod", "events" } },
    };
    for (cases) |case| {
        switch (splitCommand(alloc, case.args)) {
            .ok => |result| {
                try std.testing.expectEqual(case.command, result.command);
                try std.testing.expectEqualSlices([]const u8, case.rest, result.rest);
            },
            else => return error.TestUnexpectedResult,
        }
    }
    // -h / --help with no command asks for the top-level overview.
    try std.testing.expectEqual(Result(CommandSplit).help, splitCommand(alloc, &.{"--help"}));
    try std.testing.expectEqual(Result(CommandSplit).help, splitCommand(alloc, &.{"-h"}));
    try expectErr(CommandSplit, splitCommand(alloc, &.{}), "missing command (want produce, consume, cluster, or topic)");
}

test "update takes an optional release tag" {
    const alloc = std.heap.page_allocator;
    const cases = [_]struct { args: []const []const u8, version: ?[]const u8 }{
        .{ .args = &.{}, .version = null },
        .{ .args = &.{"latest"}, .version = null },
        .{ .args = &.{"v0.2.0"}, .version = "v0.2.0" },
        .{ .args = &.{"0.2.0"}, .version = "v0.2.0" },
        .{ .args = &.{"v1.0.0-rc.1"}, .version = "v1.0.0-rc.1" },
    };
    for (cases) |case| {
        switch (parseUpdate(alloc, case.args)) {
            .ok => |r| if (case.version) |want|
                try std.testing.expectEqualStrings(want, r.version.?)
            else
                try std.testing.expect(r.version == null),
            else => return error.TestUnexpectedResult,
        }
    }
    switch (splitCommand(alloc, &.{ "update", "v0.2.0" })) {
        .ok => |r| try std.testing.expectEqual(Command.update, r.command),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(Result(UpdateArgs).help, parseUpdate(alloc, &.{"-h"}));
    try expectErr(UpdateArgs, parseUpdate(alloc, &.{ "v1", "v2" }), "unexpected argument 'v2'");
    try expectErr(UpdateArgs, parseUpdate(alloc, &.{"--bogus"}), "unknown option '--bogus'");
    try expectErr(UpdateArgs, parseUpdate(alloc, &.{"main;rm"}), "'main;rm' is not a release tag (want e.g. v0.3.0)");
    try expectErr(UpdateArgs, parseUpdate(alloc, &.{"main"}), "'main' is not a release tag (want e.g. v0.3.0)");
    try expectErr(CommandSplit, splitCommand(alloc, &.{"updte"}), "unknown command 'updte'; did you mean 'update'?");
}

test "0.1 spellings report migration errors" {
    const alloc = std.heap.page_allocator;
    try expectErr(CommandSplit, splitCommand(alloc, &.{ "-c", "events" }), "'-c' is now a command: kite consume [@CLUSTER] TOPIC");
    try expectErr(CommandSplit, splitCommand(alloc, &.{ "--consume", "events" }), "'--consume' is now a command: kite consume [@CLUSTER] TOPIC");
    try expectErr(CommandSplit, splitCommand(alloc, &.{"--show-config"}), "'--show-config' was removed; use 'kite cluster list' to see the selected cluster");
    try expectErr(CommandSplit, splitCommand(alloc, &.{"config"}), "'kite config' was removed; use 'kite cluster list' to see the selected cluster");
    try expectErr(CommandSplit, splitCommand(alloc, &.{"--targets"}), "'--targets' is now: kite cluster list");
    try expectErr(CommandSplit, splitCommand(alloc, &.{"targets"}), "'kite targets' is now: kite cluster list");
    try expectErr(CommandSplit, splitCommand(alloc, &.{ "--target", "prod", "events" }), "'--target NAME' was replaced by the @NAME positional (e.g. @prod)");
    try expectErr(CommandSplit, splitCommand(alloc, &.{"--target=prod"}), "'--target NAME' was replaced by the @NAME positional (e.g. @prod)");
    try expectErr(CommandSplit, splitCommand(alloc, &.{ "--bogus", "events" }), "unknown option '--bogus'");
    // --target is a migration error mid-command too, wherever it appears.
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--target", "prod", "events" }), "'--target NAME' was replaced by the @NAME positional (e.g. @prod)");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "events", "--target=prod" }), "'--target NAME' was replaced by the @NAME positional (e.g. @prod)");
    try expectErr(ClusterArgs, parseCluster(alloc, &.{"--target=x"}), "'--target NAME' was replaced by the @NAME positional (e.g. @prod)");
}

test "unknown first argument names a command or gets a hint" {
    const alloc = std.heap.page_allocator;
    try expectErr(CommandSplit, splitCommand(alloc, &.{"events"}), "unknown command 'events' (kite now needs a command: kite produce events)");
    try expectErr(CommandSplit, splitCommand(alloc, &.{"produse"}), "unknown command 'produse'; did you mean 'produce'?");
    try expectErr(CommandSplit, splitCommand(alloc, &.{"produe"}), "unknown command 'produe'; did you mean 'produce'?");
}

test "topic parser: default list, actions, and options" {
    const alloc = std.heap.page_allocator;
    switch (parseTopic(alloc, &.{})) {
        .ok => |a| try std.testing.expectEqual(TopicAction.list, a.action),
        else => return error.TestUnexpectedResult,
    }
    switch (parseTopic(alloc, &.{ "list", "--json", "-a", "-q" })) {
        .ok => |a| {
            try std.testing.expectEqual(TopicAction.list, a.action);
            try std.testing.expectEqual(Format.json, a.common.format);
            try std.testing.expect(a.all);
            try std.testing.expect(a.common.quiet);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (parseTopic(alloc, &.{ "create", "-p", "6", "-r", "2", "-s", "a=1", "--set=b=2", "--set", "c=3", "--if-not-exists", "events" })) {
        .ok => |a| {
            try std.testing.expectEqual(TopicAction.create, a.action);
            try std.testing.expectEqual(@as(?i32, 6), a.partitions);
            try std.testing.expectEqual(@as(?i16, 2), a.replication_factor);
            try std.testing.expectEqual(@as(usize, 3), a.set.len);
            try std.testing.expectEqualStrings("a", a.set[0].name);
            try std.testing.expectEqualStrings("1", a.set[0].value);
            try std.testing.expectEqualStrings("b", a.set[1].name);
            try std.testing.expectEqualStrings("2", a.set[1].value);
            try std.testing.expect(a.if_not_exists);
            try std.testing.expectEqualStrings("events", a.topics[0]);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (parseTopic(alloc, &.{ "delete", "-y", "--if-exists", "a", "b", "c" })) {
        .ok => |a| {
            try std.testing.expectEqual(TopicAction.delete, a.action);
            try std.testing.expect(a.yes);
            try std.testing.expect(a.if_exists);
            try std.testing.expectEqual(@as(usize, 3), a.topics.len);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (parseTopic(alloc, &.{ "update", "--partitions=12", "-s", "x=y", "--unset", "retention.ms", "--unset=cleanup.policy", "events" })) {
        .ok => |a| {
            try std.testing.expectEqual(TopicAction.update, a.action);
            try std.testing.expectEqual(@as(?i32, 12), a.partitions);
            try std.testing.expectEqual(@as(usize, 1), a.set.len);
            try std.testing.expectEqual(@as(usize, 2), a.unset.len);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (parseTopic(alloc, &.{ "@prod", "list" })) {
        .ok => |a| try std.testing.expectEqualStrings("prod", a.common.target.?),
        else => return error.TestUnexpectedResult,
    }
    switch (splitCommand(alloc, &.{ "topic", "list" })) {
        .ok => |r| {
            try std.testing.expectEqual(Command.topic, r.command);
            try std.testing.expectEqualSlices([]const u8, &.{"list"}, r.rest);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "topic parser reports exact argument errors" {
    const alloc = std.heap.page_allocator;
    try expectErr(TopicArgs, parseTopic(alloc, &.{"bogus"}), "unknown topic action 'bogus' (want list, create, delete, or update)");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "list", "x" }), "unexpected argument 'x'");
    try expectErr(TopicArgs, parseTopic(alloc, &.{"create"}), "missing TOPIC");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "a", "b" }), "unexpected argument 'b'");
    try expectErr(TopicArgs, parseTopic(alloc, &.{"update"}), "missing TOPIC");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "update", "a", "b" }), "unexpected argument 'b'");
    try expectErr(TopicArgs, parseTopic(alloc, &.{"delete"}), "missing TOPIC");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "--all", "x" }), "'--all' is not valid with kite topic create");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "-a", "x" }), "'-a' is not valid with kite topic create");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "--unset", "k", "x" }), "'--unset' is not valid with kite topic create");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "update", "-r", "2", "x" }), "'-r' is not valid with kite topic update");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "list", "-y" }), "'-y' is not valid with kite topic list");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "delete", "--if-not-exists", "x" }), "'--if-not-exists' is not valid with kite topic delete");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "update", "events" }), "nothing to update (want --partitions, --set, or --unset)");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "--set", "nokey", "x" }), "--set: 'nokey' is not KEY=VALUE");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "--set", "=v", "x" }), "--set: '=v' is not KEY=VALUE");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "update", "--unset=", "x" }), "--unset requires a config name");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "--json", "x" }), "--json is only valid with kite topic list");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "list", "--format", "tsv" }), "--format: only json is valid with kite topic list");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "-p", "0", "x" }), "--partitions: '0' is not a positive integer");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "--partitions", "abc", "x" }), "--partitions: 'abc' is not a positive integer");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "-r", "x", "t" }), "--replication-factor: 'x' is not a positive integer");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "--replication-factor=99999", "t" }), "--replication-factor: '99999' is not a positive integer");
    try expectErr(TopicArgs, parseTopic(alloc, &.{"--bogus"}), "unknown option '--bogus'");
    try expectErr(TopicArgs, parseTopic(alloc, &.{ "create", "-q", "-v", "x" }), "--quiet cannot be combined with --verbose");
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

test "@NAME parses into common.target" {
    const alloc = std.heap.page_allocator;
    const produce = parseProduce(alloc, &.{ "@prod", "events" });
    switch (produce) {
        .ok => |args| try std.testing.expectEqualStrings("prod", args.common.target.?),
        else => return error.TestUnexpectedResult,
    }
    const consume = parseConsume(alloc, &.{ "events", "@prod" });
    switch (consume) {
        .ok => |args| try std.testing.expectEqualStrings("prod", args.common.target.?),
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
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "a", "b" }), "unexpected argument 'b'");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "a", "b" }), "unexpected argument 'b'");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{"-b"}), "-b requires a value");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--bootstrap=", "demo" }), "--bootstrap must not be empty");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{"--config"}), "--config requires a value");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "--offset", "1", "demo" }), "'--offset' is a consume option; use 'kite consume [OPTIONS] TOPIC'");
    try expectErr(ProduceArgs, parseProduce(alloc, &.{ "-n5", "demo" }), "'-n' is a consume option; use 'kite consume [OPTIONS] TOPIC'");
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
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "-H", "a: b", "demo" }), "'-H' is a produce option; use 'kite produce [OPTIONS] TOPIC'");
    try expectErr(ConsumeArgs, parseConsume(alloc, &.{ "--csv", "demo" }), "'--csv' is a produce option; use 'kite produce [OPTIONS] TOPIC'");
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
    for ([_][]const u8{ produce_help, consume_help, cluster_help, topic_help, update_help }) |page|
        try std.testing.expect(std.mem.indexOf(u8, page, "-h, --help") != null);
    for ([_][]const u8{ overview_help, produce_help, consume_help, cluster_help, topic_help, update_help }) |page| {
        var lines = std.mem.splitScalar(u8, page, '\n');
        while (lines.next()) |line| {
            try std.testing.expect(line.len <= 80);
            try std.testing.expect(std.mem.indexOfScalar(u8, line, '\t') == null);
            try std.testing.expect(std.mem.indexOfScalar(u8, line, 0x1b) == null);
        }
    }
}
