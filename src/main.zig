//! kite — ultra-lightweight Kafka CLI.
//! `kite produce <topic> < file` sends each stdin line as one record value.

const std = @import("std");
const config = @import("config.zig");
const client = @import("client.zig");
const cli_args = @import("cli.zig");
const yaml = @import("yaml.zig");
const protocol = @import("protocol.zig");
const transport = @import("transport.zig");
const scram = @import("scram.zig");
const csv = @import("csv.zig");
const consumer = @import("consumer.zig");
const term = @import("term.zig");
const stats_mod = @import("stats.zig");
const json = @import("json.zig");

// unused-import anchors so `zig build test` covers every module
comptime {
    _ = config;
    _ = client;
    _ = cli_args;
    _ = yaml;
    _ = protocol;
    _ = transport;
    _ = scram;
    _ = csv;
    _ = consumer;
    _ = term;
    _ = stats_mod;
    _ = json;
}

// Panics print just the message — pulls in no DWARF/stack-trace machinery.
pub const panic = std.debug.simple_panic;

fn out(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

/// Live stderr status line to erase before printing a fatal error.
var live_stats: ?*stats_mod.Stats = null;

fn fatal(comptime fmt: []const u8, args: anytype) noreturn {
    if (live_stats) |s| s.clearLine();
    if (term.color.enabled)
        out(term.red ++ "kite:" ++ term.reset ++ " " ++ fmt ++ "\n", args)
    else
        out("kite: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

fn fatalErr(c: *const client.Client, comptime fmt: []const u8) noreturn {
    const detail = c.errDetail();
    if (detail.len > 0)
        fatal(fmt ++ ": {s}", .{detail})
    else
        fatal(fmt, .{});
}

fn writeText(init: std.process.Init, file: std.Io.File, text: []const u8) void {
    var buf: [4096]u8 = undefined;
    var w = file.writer(init.io, &buf);
    w.interface.writeAll(text) catch {};
    w.interface.flush() catch {};
}

fn parseFatal(init: std.process.Init, message: []const u8, usage_text: []const u8) noreturn {
    var buf: [4096]u8 = undefined;
    var w = std.Io.File.stderr().writer(init.io, &buf);
    if (term.color.enabled)
        w.interface.print("{s}kite:{s} {s}\n{s}", .{ term.red, term.reset, message, usage_text }) catch {}
    else
        w.interface.print("kite: {s}\n{s}", .{ message, usage_text }) catch {};
    w.interface.flush() catch {};
    std.process.exit(1);
}

/// Parse one stdin line into a record. First TAB-field = key (empty = null),
/// last = value, any middle fields = 'name: value' headers.
fn parseLine(
    alloc: std.mem.Allocator,
    line: []const u8,
    static_headers: []const protocol.Header,
    lineno: u64,
) protocol.Record {
    if (std.mem.indexOfScalar(u8, line, '\t') == null)
        return .{ .value = line, .headers = static_headers };

    var it = std.mem.splitScalar(u8, line, '\t');
    const keyf = it.next().?;
    var headers: std.ArrayListUnmanaged(protocol.Header) = .empty;
    headers.appendSlice(alloc, static_headers) catch fatal("out of memory", .{});
    var value: []const u8 = "";
    while (it.next()) |f| {
        if (it.peek() == null) {
            value = f;
        } else {
            if (std.mem.indexOfScalar(u8, f, ':') == null)
                fatal("line {d}: malformed header '{s}' (want 'name: value')", .{ lineno, f });
            headers.append(alloc, cli_args.parseHeaderArg(f) catch
                fatal("line {d}: malformed header '{s}' (want 'name: value')", .{ lineno, f })) catch
                fatal("out of memory", .{});
        }
    }
    return .{
        .key = if (keyf.len == 0) null else keyf,
        .value = value,
        .headers = headers.items,
    };
}

/// Per-partition pending record buffer.
const Pending = struct {
    records: std.ArrayListUnmanaged(protocol.Record) = .empty,
    bytes: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const alloc = init.arena.allocator();
    term.color = .{ .enabled = term.detect(io, std.Io.File.stderr(), init.environ_map) };

    const args = init.minimal.args.toSlice(alloc) catch fatal("out of memory", .{});
    if (args.len == 2 and (std.mem.eql(u8, args[1], "-V") or std.mem.eql(u8, args[1], "--version"))) {
        writeText(init, std.Io.File.stdout(), "kite " ++ cli_args.version ++ "\n");
        return;
    }
    const split = cli_args.splitCommand(alloc, args[1..]);
    const command_args = switch (split) {
        .err => |message| parseFatal(init, message, cli_args.overview_usage),
        .ok => |value| value,
        .help => {
            if (term.detect(io, std.Io.File.stdout(), init.environ_map)) {
                term.color.enabled = true;
                const page = term.renderHelp(alloc, cli_args.overview_help) catch fatal("out of memory", .{});
                writeText(init, std.Io.File.stdout(), page);
            } else writeText(init, std.Io.File.stdout(), cli_args.overview_help);
            return;
        },
    };
    switch (command_args.command) {
        .consume => {
            runConsume(init, command_args.rest, alloc);
            return;
        },
        .cluster => {
            runCluster(init, command_args.rest, alloc);
            return;
        },
        .produce => {},
    }
    const parsed = cli_args.parseProduce(alloc, command_args.rest);
    const produce = switch (parsed) {
        .help => {
            if (term.detect(io, std.Io.File.stdout(), init.environ_map)) {
                term.color.enabled = true;
                const page = term.renderHelp(alloc, cli_args.produce_help) catch fatal("out of memory", .{});
                writeText(init, std.Io.File.stdout(), page);
            } else writeText(init, std.Io.File.stdout(), cli_args.produce_help);
            return;
        },
        .err => |message| parseFatal(init, message, cli_args.produce_usage),
        .ok => |value| value,
    };
    const topic = produce.topic;
    const static_headers = produce.headers;
    const verbose = produce.common.verbose;
    const quiet = produce.common.quiet;
    const csv_mode = produce.isCsv();
    const json_mode = produce.common.format == .json;
    const value_mode = produce.common.format == .value;
    const csv_key_col = produce.key_col;

    var cfg = loadConfig(init, alloc, produce.common, &dummy_source);
    var cli = client.Client.init(alloc, io, init.environ_map, &cfg);
    connectAndResolve(&cli, topic, "write", quiet, true);

    const nparts = cli.partitionCount();
    var pend = alloc.alloc(Pending, nparts) catch fatal("out of memory", .{});
    for (pend) |*p| p.* = .{};

    var stdin_buf: [64 * 1024]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buf);
    const r = &stdin_reader.interface;
    const stdin_fd = std.Io.File.stdin().handle;
    const stderr_tty = std.Io.File.stderr().isTty(io) catch false;
    if (!quiet and stderr_tty and (std.Io.File.stdin().isTty(io) catch false))
        note("reading records from the terminal, one per line (Ctrl-D to finish)", .{});

    var cols: [][]const u8 = &.{};
    var key_idx: ?usize = null;
    if (csv_mode) {
        const hdr = csv.nextRow(r, alloc) catch fatal("failed reading stdin", .{}) orelse
            fatal("empty csv input (no header row)", .{});
        cols = csv.splitFields(alloc, hdr) catch fatal("malformed csv header row", .{});
        if (cols.len > 0) cols[0] = csv.stripBom(cols[0]);
        if (csv_key_col) |kc| {
            for (cols, 0..) |c, i| {
                if (std.mem.eql(u8, c, kc)) key_idx = i;
            }
            if (key_idx == null) fatal("--key '{s}': no such csv column", .{kc});
        }
    }

    var rr: usize = 0; // round-robin cursor for unkeyed records
    var total: u64 = 0;
    const timing = init.environ_map.get("KITE_TIME") != null;
    var t_read: u64 = 0;
    var t_flush: u64 = 0;
    var t_drain: u64 = 0;
    var timer = Lap.init(io);
    var stats = stats_mod.Stats.init(io, topic, stderr_tty and !verbose and !quiet);
    defer stats.deinit();
    live_stats = &stats;
    read_loop: while (true) {
        // Linger: with pending records and no stdin data within linger_ms,
        // flush rather than block indefinitely on a slow producer. Skip the
        // poll when a full line is already buffered — no read() can block.
        const ready = if (csv_mode)
            csv.rowReady(r)
        else
            std.mem.indexOfScalar(u8, r.buffered(), '\n') != null;
        if (pendingBytes(pend) > 0 and !ready) {
            var fds = [_]std.posix.pollfd{.{ .fd = stdin_fd, .events = std.posix.POLL.IN, .revents = 0 }};
            const nready = std.posix.poll(&fds, @intCast(@min(cfg.linger_ms, std.math.maxInt(i32)))) catch 1;
            if (nready == 0) {
                flushAll(&cli, topic, pend) catch |err| produceFatal(&cli, err);
                stats.maybeRender();
                continue;
            }
        }

        const rec = if (csv_mode) blk: {
            const row = csv.nextRow(r, alloc) catch fatal("failed reading stdin", .{}) orelse
                break :read_loop;
            break :blk csvRecord(alloc, row, cols, key_idx, static_headers, total + 1);
        } else blk: {
            const owned = nextLine(r, alloc) catch fatal("failed reading stdin", .{}) orelse
                break :read_loop;
            if (json_mode) {
                if (std.mem.trim(u8, owned, " \t").len == 0) continue;
                break :blk jsonRecord(alloc, owned, static_headers, total + 1);
            }
            if (value_mode)
                break :blk protocol.Record{ .value = owned, .headers = static_headers };
            break :blk parseLine(alloc, owned, static_headers, total + 1);
        };
        t_read += timer.lap();
        total += 1;
        stats.add(1, recordSize(rec));
        stats.maybeRender();
        // Keyed records partition by murmur2 like Kafka's default partitioner;
        // unkeyed records round-robin so every partition fills together.
        const target: usize = if (rec.key) |k| blk: {
            const h = std.hash.murmur.Murmur2_32.hashWithSeed(k, 0x9747b28c);
            break :blk (h & 0x7fffffff) % nparts;
        } else blk: {
            const t = rr;
            rr = (rr + 1) % nparts;
            break :blk t;
        };
        const p = &pend[target];
        p.records.append(alloc, rec) catch fatal("out of memory", .{});
        p.bytes += recordSize(rec);
        if (p.bytes + batch_overhead >= cfg.batch_size) {
            // Cap hit: flush every partition's pending buffer in one pipelined
            // round so all leader conns go in flight together.
            flushAll(&cli, topic, pend) catch |err| produceFatal(&cli, err);
            t_flush += timer.lap();
            if (cli.outstanding_bytes >= 96 << 20) {
                cli.produceDrainUntil(topic, 96 << 20) catch |err| produceFatal(&cli, err);
                stats.maybeRender();
                t_drain += timer.lap();
            }
        }
    }

    flushAll(&cli, topic, pend) catch |err| produceFatal(&cli, err);
    cli.produceDrain(topic) catch |err| produceFatal(&cli, err);
    for (cli.last_offsets.items) |po| stats.noteOffset(po.pidx, po.offset);
    if (timing) std.debug.print("read {d}ms send {d}ms drain {d}ms conns {d}\n", .{ t_read / 1_000_000, t_flush / 1_000_000, (t_drain + timer.lap()) / 1_000_000, cli.conns.count() });

    produceSummary(&cli, &stats, total, nparts, quiet);
    cli.deinit();
}

/// Post-produce report on stderr; stdout stays free for pipeline data.
fn produceSummary(cli: *client.Client, stats: *stats_mod.Stats, total: u64, nparts: usize, quiet: bool) void {
    const parts_used = cli.last_offsets.items.len;
    stats.clearLine();
    if (!quiet) {
        if (term.color.enabled)
            std.debug.print("{s}{d}{s} record(s) produced to '{s}{s}{s}' across {d} of {d} partition(s)\n", .{
                term.bold, total, term.reset, term.cyan, stats.topic, term.reset, parts_used, nparts,
            })
        else
            std.debug.print("{d} record(s) produced to '{s}' across {d} of {d} partition(s)\n", .{
                total, stats.topic, parts_used, nparts,
            });
        stats.finish();
    }
    if (total > 0 and !quiet) {
        var lat_buf: [32]u8 = undefined;
        const per_req = if (cli.avgAckMs()) |ms|
            std.fmt.bufPrint(&lat_buf, "{d:.1}ms", .{ms}) catch "?"
        else
            "n/a";
        std.debug.print("{d} produce request(s), {d} retried batch(es), {s} avg ack latency, {d} connection(s)\n", .{
            cli.produce_requests, cli.produce_retries, per_req, cli.conns.count(),
        });
    }
    if (cli.acked_records != total)
        std.debug.print("warning: broker acknowledged {d} of {d} record(s)\n", .{ cli.acked_records, total });
}

/// Parse a `--json` input line: {"key":..,"value":..,"headers":{..}}. A
/// non-string value is forwarded verbatim so JSON objects can be sent
/// directly; headers may be an object or an array of {"key","value"}.
fn jsonRecord(
    alloc: std.mem.Allocator,
    line: []const u8,
    static_headers: []const protocol.Header,
    lineno: u64,
) protocol.Record {
    var sc = json.Scanner{ .src = line, .alloc = alloc };
    var key: ?[]const u8 = null;
    var value: ?[]const u8 = null;
    var headers: std.ArrayListUnmanaged(protocol.Header) = .empty;
    headers.appendSlice(alloc, static_headers) catch fatal("out of memory", .{});

    const shape = "want {\"key\":..,\"value\":..,\"headers\":{..}}";
    const is_obj = sc.beginObject() catch jsonFatal(lineno, shape);
    if (!is_obj) fatal("line {d}: expected a JSON object ({s})", .{ lineno, shape });
    var first = true;
    while (sc.nextMember(first) catch jsonFatal(lineno, shape)) |m| : (first = false) {
        if (std.mem.eql(u8, m.key, "value")) {
            value = m.value.bytes() orelse fatal("line {d}: \"value\" must not be null", .{lineno});
        } else if (std.mem.eql(u8, m.key, "key")) {
            key = switch (m.value) {
                .null => null,
                .string => |v| v,
                else => fatal("line {d}: \"key\" must be a string or null", .{lineno}),
            };
        } else if (std.mem.eql(u8, m.key, "headers")) {
            switch (m.value) {
                .null => {},
                .object => |raw| {
                    var hs = json.Scanner{ .src = raw, .alloc = alloc };
                    _ = hs.beginObject() catch unreachable;
                    var hfirst = true;
                    while (hs.nextMember(hfirst) catch jsonFatal(lineno, shape)) |h| : (hfirst = false) {
                        headers.append(alloc, .{ .key = h.key, .value = jsonHeaderValue(h.value, lineno) }) catch
                            fatal("out of memory", .{});
                    }
                },
                .array => |raw| {
                    var hs = json.Scanner{ .src = raw, .alloc = alloc };
                    _ = hs.beginArray() catch unreachable;
                    var hfirst = true;
                    while (hs.nextElement(hfirst) catch jsonFatal(lineno, shape)) : (hfirst = false) {
                        const is_entry = hs.beginObject() catch jsonFatal(lineno, shape);
                        if (!is_entry) fatal("line {d}: header entries must be {{\"key\",\"value\"}} objects", .{lineno});
                        var hk: ?[]const u8 = null;
                        var hv: ?[]const u8 = null;
                        var efirst = true;
                        while (hs.nextMember(efirst) catch jsonFatal(lineno, shape)) |e| : (efirst = false) {
                            if (std.mem.eql(u8, e.key, "key")) {
                                hk = switch (e.value) {
                                    .string => |v| v,
                                    else => fatal("line {d}: header \"key\" must be a string", .{lineno}),
                                };
                            } else if (std.mem.eql(u8, e.key, "value")) {
                                hv = jsonHeaderValue(e.value, lineno);
                            }
                        }
                        headers.append(alloc, .{
                            .key = hk orelse fatal("line {d}: header entry has no \"key\"", .{lineno}),
                            .value = hv,
                        }) catch fatal("out of memory", .{});
                    }
                },
                else => fatal("line {d}: \"headers\" must be an object or array", .{lineno}),
            }
        }
    }
    if (!sc.atEnd()) jsonFatal(lineno, shape);
    return .{
        .key = key,
        .value = value orelse fatal("line {d}: JSON object has no \"value\" field", .{lineno}),
        .headers = headers.items,
    };
}

fn jsonFatal(lineno: u64, shape: []const u8) noreturn {
    fatal("line {d}: invalid JSON ({s})", .{ lineno, shape });
}

fn jsonHeaderValue(v: json.Value, lineno: u64) ?[]const u8 {
    return switch (v) {
        .null => null,
        .string, .object, .array => v.bytes(),
        .other => fatal("line {d}: header values must be strings or null", .{lineno}),
    };
}

fn note(comptime fmt: []const u8, args: anytype) void {
    if (term.color.enabled)
        out(term.dim ++ "kite: " ++ fmt ++ term.reset ++ "\n", args)
    else
        out("kite: " ++ fmt ++ "\n", args);
}

const search_path_hint = "./kite.yaml, ./kite.properties, $XDG_CONFIG_HOME/kite/, ~/.config/kite/";

/// Resolve configuration from flags, environment and properties file.
var dummy_source: config.Source = .{};

fn loadConfig(init: std.process.Init, alloc: std.mem.Allocator, common: cli_args.Common, source: *config.Source) config.Config {
    var cfg = config.load(init.io, alloc, init.environ_map, .{
        .bootstrap = common.bootstrap,
        .config_path = common.config_path,
        .target = common.target,
    }, source) catch |err| switch (err) {
        error.ConfigNotFound => fatal(
            "no broker configured. Pass -b HOST:PORT, set BOOTSTRAP_SERVERS, or create kite.properties (searched {s}); see 'kite --help' for the file format",
            .{search_path_hint},
        ),
        error.ConfigFileNotFound => fatal("config file '{s}' not found", .{source.requested.?}),
        error.ConfigFileUnreadable => fatal("cannot read config file '{s}'", .{source.requested.?}),
        error.MissingBootstrapServers => fatal(
            "'{s}' has no bootstrap.servers; add it, or pass -b HOST:PORT / set BOOTSTRAP_SERVERS",
            .{source.file.?},
        ),
        error.InvalidSecurityProtocol => fatal("invalid security.protocol (want PLAINTEXT, SSL, SASL_SSL, or SASL_PLAINTEXT)", .{}),
        error.InvalidSaslMechanism => fatal("invalid sasl.mechanism (want PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)", .{}),
        error.MissingSaslMechanism => fatal("security.protocol=SASL_* requires sasl.mechanism", .{}),
        error.MissingSaslCredentials => fatal("sasl.mechanism set but sasl.username/sasl.password missing", .{}),
        error.TargetWithoutFile => fatal(
            "@NAME requires a properties file (searched {s})",
            .{search_path_hint},
        ),
        error.ConfigSyntax => fatal("{s}:{d}: {s}", .{ source.file.?, source.diag.line, source.diag.msg }),
        error.UnknownTarget => unknownTargetFatal(alloc, source),
        error.OutOfMemory => fatal("out of memory", .{}),
    };
    cfg.verbose = common.verbose;
    if (common.verbose) {
        std.debug.print("kite: config from {s}{s}{s}{s}\n", .{
            if (source.flags) "flags" else "",
            if (source.flags and (source.env or source.file != null)) " > " else "",
            if (source.env) "environment" else "",
            if (source.file) |f| f else if (!source.env and !source.flags) "(nothing)" else "",
        });
        if (source.env and source.file != null) std.debug.print("kite: (env overrides file)\n", .{});
        if (source.target) |t| std.debug.print("kite: cluster '{s}'\n", .{t});
    }
    return cfg;
}

fn unknownTargetFatal(alloc: std.mem.Allocator, source: *config.Source) noreturn {
    if (source.file_kind == .properties)
        fatal("no cluster '{s}' in {s} (properties files define a single cluster; use kite.yaml for several)", .{ source.target.?, source.file.? });
    if (source.targets.len == 0)
        fatal("no cluster '{s}' in {s} (file defines no clusters)", .{ source.target.?, source.file.? });
    const names = std.mem.join(alloc, ", ", source.targets) catch fatal("out of memory", .{});
    fatal("no cluster '{s}' in {s} (available: {s})", .{ source.target.?, source.file.?, names });
}

/// True when a missing topic should be created: automatically when stderr
/// is not a terminal (scripts, pipes) or /dev/tty cannot be opened, else
/// only when the user confirms at the /dev/tty prompt. The prompt reads
/// /dev/tty rather than stdin, which may be the record pipe for produce.
fn shouldCreateTopic(io: std.Io, topic: []const u8) bool {
    const stderr_tty = std.Io.File.stderr().isTty(io) catch false;
    const tty: ?std.Io.File = if (stderr_tty)
        std.Io.Dir.cwd().openFile(io, "/dev/tty", .{}) catch null
    else
        null;
    const f = tty orelse return true;
    defer f.close(io);
    if (term.color.enabled)
        out(term.dim ++ "kite: topic '{s}' does not exist. Create it? [y/N] " ++ term.reset, .{topic})
    else
        out("kite: topic '{s}' does not exist. Create it? [y/N] ", .{topic});
    var buf: [256]u8 = undefined;
    var fr = f.reader(io, &buf);
    const line = fr.interface.takeDelimiter('\n') catch return false;
    const answer = std.mem.trim(u8, line orelse return false, " \t\r");
    return std.ascii.eqlIgnoreCase(answer, "y") or std.ascii.eqlIgnoreCase(answer, "yes");
}

/// Create the missing topic, then poll metadata until the brokers agree on
/// its partition->leader table (a fresh topic reports no leader briefly).
fn createAndResolve(cli: *client.Client, topic: []const u8, access: []const u8, quiet: bool) void {
    if (!shouldCreateTopic(cli.io, topic))
        fatal("topic '{s}' does not exist and was not created (answer y to create it, or create it with your admin tooling)", .{topic});
    cli.createTopic(topic) catch |err| switch (err) {
        error.TopicAuthorizationFailed => fatal(
            "not authorized to create topic '{s}' (check ACLs for this principal)",
            .{topic},
        ),
        else => fatalErr(cli, "could not create topic"),
    };
    if (!quiet) note("created topic '{s}'", .{topic});
    const max_attempts = 10;
    const retry_ms = 200;
    var attempt: usize = 0;
    while (true) {
        if (cli.refreshMetadata(topic)) |_| {
            if (attempt > 0 and !quiet) note("topic '{s}' is ready", .{topic});
            return;
        } else |err| switch (err) {
            error.TopicNotFound, error.MetadataFailed => {
                attempt += 1;
                if (attempt >= max_attempts)
                    fatalErr(cli, "topic was created but its metadata did not appear in time; retry the command in a moment");
                if (attempt == 1 and !quiet)
                    note("waiting for the cluster to publish metadata for '{s}' (up to {d} ms) ...", .{ topic, max_attempts * retry_ms });
                cli.sleep(retry_ms);
            },
            error.TopicAuthorizationFailed => fatal(
                "not authorized to {s} topic '{s}' (check ACLs for this principal; on managed clusters this is also what a missing topic looks like)",
                .{ access, topic },
            ),
            else => fatalErr(cli, "metadata lookup failed"),
        }
    }
}

/// Bootstrap and fetch topic metadata, exiting with a mode-aware message.
/// When `create_missing` (produce only) a missing topic is created on the
/// spot — after confirmation on a terminal, silently otherwise; consume
/// treats it as a plain error.
fn connectAndResolve(cli: *client.Client, topic: []const u8, access: []const u8, quiet: bool, create_missing: bool) void {
    cli.bootstrap() catch fatalErr(cli, "could not reach any bootstrap server");
    cli.refreshMetadata(topic) catch |err| switch (err) {
        error.TopicNotFound => if (create_missing)
            createAndResolve(cli, topic, access, quiet)
        else
            fatal("topic '{s}' does not exist", .{topic}),
        error.TopicAuthorizationFailed => fatal(
            "not authorized to {s} topic '{s}' (check ACLs for this principal; on managed clusters this is also what a missing topic looks like)",
            .{ access, topic },
        ),
        else => fatalErr(cli, "metadata lookup failed"),
    };
}

fn runConsume(init: std.process.Init, args: []const []const u8, alloc: std.mem.Allocator) noreturn {
    const parsed = cli_args.parseConsume(alloc, args);
    const consume = switch (parsed) {
        .help => {
            if (term.detect(init.io, std.Io.File.stdout(), init.environ_map)) {
                term.color.enabled = true;
                const page = term.renderHelp(alloc, cli_args.consume_help) catch fatal("out of memory", .{});
                writeText(init, std.Io.File.stdout(), page);
            } else writeText(init, std.Io.File.stdout(), cli_args.consume_help);
            std.process.exit(0);
        },
        .err => |message| parseFatal(init, message, cli_args.consume_usage),
        .ok => |value| value,
    };
    const topic_name = consume.topic;
    const verbose = consume.common.verbose;
    const quiet = consume.common.quiet;

    var cfg = loadConfig(init, alloc, consume.common, &dummy_source);
    var cli = client.Client.init(alloc, init.io, init.environ_map, &cfg);
    connectAndResolve(&cli, topic_name, "read", quiet, false);

    const stdout_tty = std.Io.File.stdout().isTty(init.io) catch false;
    const stderr_tty = std.Io.File.stderr().isTty(init.io) catch false;
    // Unbounded reads only make sense on a terminal (or with --follow); a
    // pipe/file consumer without a bound stops after a short idle so scripts
    // and agents never hang.
    const idle_ms: ?u64 = if (consume.follow)
        null
    else if (consume.idle_ms) |ms|
        ms
    else if (consume.max_records == null and !stdout_tty)
        default_idle_ms
    else
        null;

    var stdout_buf: [64 * 1024]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(init.io, &stdout_buf);
    var stats = stats_mod.Stats.init(init.io, topic_name, stderr_tty and !verbose and !quiet);
    defer stats.deinit();
    live_stats = &stats;
    stats.clear_before_output = stdout_tty;
    stats.waiting_hint = switch (consume.start) {
        .latest => " (new records only; use -B for history)",
        .earliest => " from the beginning",
        .offset => " from the given offset",
    };
    if (verbose) {
        if (idle_ms) |ms|
            std.debug.print("kite: consuming '{s}', stop after {d} idle ms{s}\n", .{
                topic_name, ms, if (consume.idle_ms == null) " (default for non-terminal stdout)" else "",
            })
        else
            std.debug.print("kite: consuming '{s}' until Ctrl-C\n", .{topic_name});
    }
    installSignalHandlers();
    const consumed = consumer.run(&cli, .{
        .topic = topic_name,
        .start = consume.start,
        .offset = consume.offset,
        .partition = consume.partition,
        .max_records = consume.max_records,
        .idle_ms = idle_ms,
        .format = consume.common.format,
        .stats = &stats,
        .stop = &interrupted,
        .sink_closed = stdoutClosed,
    }, &stdout.interface) catch |err| switch (err) {
        error.PartitionNotFound => fatal("partition {d} not found in topic '{s}'", .{ consume.partition orelse -1, topic_name }),
        error.OffsetOutOfRange => fatalErr(&cli, "cannot start consuming"),
        error.WriteFailed => {
            const write_err = stdout.err orelse error.Unexpected;
            if (write_err == error.BrokenPipe or stdoutClosed()) std.process.exit(0);
            fatal("cannot write stdout: {s}", .{@errorName(write_err)});
        },
        error.FetchFailed => fatalErr(&cli, "consume failed"),
        else => fatalErr(&cli, "consume failed"),
    };
    stdout.flush() catch |err| switch (err) {
        error.BrokenPipe => std.process.exit(0),
        else => fatal("cannot write stdout: {s}", .{@errorName(err)}),
    };
    const stopped_by_signal = interrupted.load(.acquire);
    const reason: []const u8 = if (stopped_by_signal)
        " (interrupted)"
    else if (consume.max_records != null and consumed >= consume.max_records.?)
        ""
    else if (idle_ms != null)
        " (idle timeout)"
    else
        "";
    stats.clearLine();
    if (!quiet) {
        if (term.color.enabled)
            std.debug.print("{s}{d}{s} record(s) consumed from '{s}{s}{s}'{s}\n", .{
                term.bold, consumed, term.reset, term.cyan, topic_name, term.reset, reason,
            })
        else
            std.debug.print("{d} record(s) consumed from '{s}'{s}\n", .{ consumed, topic_name, reason });
        stats.finish();
    }
    if (consumed == 0 and consume.start == .latest and !stats.live and !quiet)
        note("no records arrived; consume starts at the latest offset, use -B to read existing records", .{});
    cli.deinit();
    std.process.exit(if (stopped_by_signal) 130 else 0);
}

/// `kite cluster [list|set NAME]`: pick, list, or persistently select the
/// cluster other commands talk to. Lists without resolving a full Config
/// so incomplete clusters still show.
fn runCluster(init: std.process.Init, args: []const []const u8, alloc: std.mem.Allocator) noreturn {
    const parsed = cli_args.parseCluster(alloc, args);
    const cluster = switch (parsed) {
        .help => {
            if (term.detect(init.io, std.Io.File.stdout(), init.environ_map)) {
                term.color.enabled = true;
                const page = term.renderHelp(alloc, cli_args.cluster_help) catch fatal("out of memory", .{});
                writeText(init, std.Io.File.stdout(), page);
            } else writeText(init, std.Io.File.stdout(), cli_args.cluster_help);
            std.process.exit(0);
        },
        .err => |message| parseFatal(init, message, cli_args.cluster_usage),
        .ok => |value| value,
    };
    const common = cluster.common;
    if (cluster.action == .init) runClusterInit(init, alloc, common);

    var source: config.Source = .{};
    const targets = config.listTargets(init.io, alloc, init.environ_map, .{
        .config_path = common.config_path,
    }, &source) catch |err| switch (err) {
        error.ConfigNotFound => fatal("no config file found (searched {s}); see 'kite --help' for the file format", .{search_path_hint}),
        error.ConfigFileNotFound => fatal("config file '{s}' not found", .{source.requested.?}),
        error.ConfigFileUnreadable => fatal("cannot read config file '{s}'", .{source.requested.?}),
        error.ConfigSyntax => fatal("{s}:{d}: {s}", .{ source.file.?, source.diag.line, source.diag.msg }),
        error.UnknownTarget => unknownTargetFatal(alloc, &source),
        error.OutOfMemory => fatal("out of memory", .{}),
        else => unreachable,
    };

    switch (cluster.action) {
        .init => unreachable, // handled above, before listTargets
        .list => {},
        .set => {
            setCurrentCluster(init, alloc, common, cluster.name.?, &source, targets.names);
            std.process.exit(0);
        },
        .pick => {
            if (targets.names.len == 0)
                fatal("{s} defines no clusters", .{source.file.?});
            const stdin_tty = std.Io.File.stdin().isTty(init.io) catch false;
            const stderr_tty = std.Io.File.stderr().isTty(init.io) catch false;
            if (stdin_tty and stderr_tty) {
                var initial: usize = 0;
                if (targets.selected) |sel| {
                    for (targets.names, 0..) |t, i| {
                        if (std.mem.eql(u8, t, sel)) {
                            initial = i;
                            break;
                        }
                    }
                }
                const choice = term.pick(init.io, alloc, targets.names, initial) catch
                    fatal("cannot read terminal", .{});
                const idx = choice orelse fatal("no cluster selected", .{});
                setCurrentCluster(init, alloc, common, targets.names[idx], &source, targets.names);
                std.process.exit(0);
            }
            // Non-interactive (piped) stdin/stderr: list instead of picking.
        },
    }

    var stdout_buf: [4096]u8 = undefined;
    var w = std.Io.File.stdout().writer(init.io, &stdout_buf);
    const outw = &w.interface;
    if (common.format == .json) {
        outw.writeAll("{\"file\":") catch {};
        json.writeString(outw, source.file.?) catch {};
        outw.writeAll(",\"current\":") catch {};
        if (targets.selected) |t| json.writeString(outw, t) catch {} else outw.writeAll("null") catch {};
        outw.writeAll(",\"clusters\":[") catch {};
        for (targets.names, 0..) |t, i| {
            if (i > 0) outw.writeByte(',') catch {};
            json.writeString(outw, t) catch {};
        }
        outw.writeAll("]}\n") catch {};
    } else {
        for (targets.names) |t| {
            outw.writeAll(t) catch {};
            if (targets.selected) |sel| {
                if (std.mem.eql(u8, sel, t)) outw.writeAll(" *") catch {};
            }
            outw.writeByte('\n') catch {};
        }
        if (!common.quiet) {
            term.color.enabled = std.Io.File.stderr().isTty(init.io) catch false;
            if (targets.names.len == 0)
                note("{s} defines no clusters", .{source.file.?})
            else
                note("clusters from {s}", .{source.file.?});
        }
    }
    outw.flush() catch {};
    std.process.exit(0);
}

/// `kite cluster init`: interactive wizard that asks questions on stderr,
/// reads answers line-by-line from stdin (TTY or piped), and splices the
/// resulting cluster into kite.yaml. Does not need an existing config file.
fn runClusterInit(init: std.process.Init, alloc: std.mem.Allocator, common: cli_args.Common) noreturn {
    const io = init.io;
    const env = init.environ_map;
    term.color.enabled = term.detect(io, std.Io.File.stderr(), env);

    const path = (config.findConfigFile(io, alloc, env, .{ .config_path = common.config_path }) catch
        oom()) orelse
        config.defaultYamlPath(alloc, env) catch
        fatal("cannot pick a config file location (HOME is unset)", .{});
    if (!config.isYamlPath(path))
        fatal("kite cluster init writes kite.yaml; {s} is a properties file (use --config kite.yaml)", .{path});

    const existing: []const u8 = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(1 << 20)) catch |err| switch (err) {
        error.FileNotFound => "clusters:\n",
        error.OutOfMemory => oom(),
        else => fatal("cannot read {s}", .{path}),
    };
    // Fail early on syntax errors before asking anything.
    var diag: yaml.Diag = .{};
    const top = yaml.parse(alloc, existing, &diag) catch
        fatal("{s}:{d}: {s}", .{ path, diag.line, diag.msg });
    var known: std.ArrayListUnmanaged([]const u8) = .empty;
    if (top.get("clusters")) |node| switch (node) {
        .scalar => {},
        .map => |m| {
            var it = m.iterator();
            while (it.next()) |e|
                known.append(alloc, e.key_ptr.*) catch oom();
        },
    };

    var stdin_buf: [4096]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buf);
    const r = &stdin_reader.interface;

    const name = name_blk: {
        var n: usize = 2;
        var default_buf: [64]u8 = undefined;
        var default: []const u8 = "local";
        while (hasName(known.items, default)) {
            default = std.fmt.bufPrint(&default_buf, "local-{d}", .{n}) catch unreachable;
            n += 1;
        }
        while (true) {
            const answer = ask(r, alloc, "Cluster name ({s}): ", .{default}) orelse fatal("aborted", .{});
            const chosen = if (answer.len == 0) default else answer;
            if (!config.validTargetName(chosen)) {
                out("kite: '{s}' is not a valid cluster name (letters, digits, '-' and '_' only)\n", .{chosen});
                continue;
            }
            if (hasName(known.items, chosen)) {
                const ow = ask(r, alloc, "Cluster '{s}' exists; overwrite? [y/N] ", .{chosen}) orelse fatal("aborted", .{});
                if (!answeredYes(ow)) continue;
            }
            break :name_blk chosen;
        }
    };

    const bootstrap = bootstrap_blk: {
        while (true) {
            const answer = ask(r, alloc, "Bootstrap servers (localhost:9092): ", .{}) orelse fatal("aborted", .{});
            if (answer.len == 0) break :bootstrap_blk @as([]const u8, "localhost:9092");
            break :bootstrap_blk answer;
        }
    };

    // A SASL username means a Confluent Cloud-style SASL_SSL/PLAIN
    // cluster; anything else is edited into kite.yaml by hand.
    const username = ask(r, alloc, "SASL username (empty for a PLAINTEXT cluster): ", .{}) orelse fatal("aborted", .{});
    var password: ?[]const u8 = null;
    if (username.len > 0) {
        const secret = askSecret(r, alloc, "SASL password (empty to leave it out and use $SASL_PASSWORD): ", .{}) orelse
            fatal("aborted", .{});
        password = if (secret.len == 0) null else secret;
        if (password == null and !common.quiet)
            note("sasl.password left unset; set SASL_PASSWORD when using '{s}'", .{name});
    }

    var block = std.Io.Writer.Allocating.init(alloc);
    const bw = &block.writer;
    bw.print("  {s}:\n", .{name}) catch oom();
    bw.print("    bootstrap.servers: {s}\n", .{yamlScalar(alloc, bootstrap)}) catch oom();
    if (username.len > 0) {
        bw.print("    security.protocol: SASL_SSL\n", .{}) catch oom();
        bw.print("    sasl.mechanism: PLAIN\n", .{}) catch oom();
        bw.print("    sasl.username: {s}\n", .{yamlScalar(alloc, username)}) catch oom();
        if (password) |p| bw.print("    sasl.password: {s}\n", .{yamlScalar(alloc, p)}) catch oom();
    }

    const new_text = spliceCluster(alloc, existing, name, block.written()) catch oom();
    if (std.fs.path.dirname(path)) |dir|
        std.Io.Dir.cwd().createDirPath(io, dir) catch |err|
            fatal("cannot create {s} ({s})", .{ dir, @errorName(err) });
    std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = new_text }) catch |err|
        fatal("cannot write {s} ({s})", .{ path, @errorName(err) });
    if (password != null)
        std.Io.Dir.cwd().setFilePermissions(io, path, .fromMode(0o600), .{}) catch |err|
            fatal("cannot set permissions on {s} ({s})", .{ path, @errorName(err) });

    // Defensive: the file we just wrote must parse and list cleanly.
    var src2: config.Source = .{};
    _ = config.listTargets(io, alloc, env, .{ .config_path = path }, &src2) catch |err| switch (err) {
        error.ConfigSyntax => fatal("{s}:{d}: {s}", .{ path, src2.diag.line, src2.diag.msg }),
        error.OutOfMemory => oom(),
        else => fatal("wrote {s} but it did not re-parse cleanly ({s})", .{ path, @errorName(err) }),
    };

    if (!common.quiet) note("wrote cluster '{s}' to {s}", .{ name, path });
    config.writeCurrentCluster(io, alloc, env, name) catch |err|
        fatal("cannot store current cluster ({s})", .{@errorName(err)});
    if (!common.quiet) {
        note("cluster set to '{s}'", .{name});
        note("try: kite consume -B TOPIC", .{});
    }
    std.process.exit(0);
}

fn oom() noreturn {
    fatal("out of memory", .{});
}

fn hasName(names: []const []const u8, name: []const u8) bool {
    for (names) |n|
        if (std.mem.eql(u8, n, name)) return true;
    return false;
}

fn answeredYes(answer: []const u8) bool {
    return std.ascii.eqlIgnoreCase(answer, "y") or std.ascii.eqlIgnoreCase(answer, "yes");
}

/// Print a prompt on stderr (bold on a terminal).
fn printPrompt(comptime fmt: []const u8, args: anytype) void {
    if (term.color.enabled)
        out(term.bold ++ fmt ++ term.reset, args)
    else
        out(fmt, args);
}

/// Prompt and return the trimmed answer line; null at EOF.
fn ask(r: *std.Io.Reader, alloc: std.mem.Allocator, comptime fmt: []const u8, args: anytype) ?[]const u8 {
    printPrompt(fmt, args);
    const line = nextLine(r, alloc) catch fatal("failed reading stdin", .{}) orelse return null;
    return std.mem.trim(u8, line, " \t");
}

/// Like `ask` but echoes '*' per character on a terminal.
fn askSecret(r: *std.Io.Reader, alloc: std.mem.Allocator, comptime fmt: []const u8, args: anytype) ?[]const u8 {
    printPrompt(fmt, args);
    const fd = std.posix.STDIN_FILENO;
    const saved = std.posix.tcgetattr(fd) catch {
        // Not a terminal (piped): plain line read, nothing echoed.
        const line = nextLine(r, alloc) catch fatal("failed reading stdin", .{}) orelse return null;
        return std.mem.trim(u8, line, " \t");
    };
    var raw = saved;
    raw.lflag.ECHO = false;
    raw.lflag.ICANON = false;
    raw.cc[@intFromEnum(std.posix.V.MIN)] = 1;
    raw.cc[@intFromEnum(std.posix.V.TIME)] = 0;
    std.posix.tcsetattr(fd, .NOW, raw) catch {};
    defer std.posix.tcsetattr(fd, .NOW, saved) catch {};

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    var ch: [1]u8 = undefined;
    while (true) {
        const n = std.posix.read(fd, &ch) catch {
            std.posix.tcsetattr(fd, .NOW, saved) catch {};
            fatal("failed reading stdin", .{});
        };
        const b = if (n == 0) 0x04 else ch[0];
        switch (b) {
            '\n', '\r' => {
                out("\n", .{});
                return std.mem.trim(u8, buf.items, " \t");
            },
            0x04 => if (buf.items.len == 0) {
                out("\n", .{});
                return null;
            },
            0x03 => {
                std.posix.tcsetattr(fd, .NOW, saved) catch {};
                out("\n", .{});
                fatal("aborted", .{});
            },
            0x7f, 0x08 => {
                // Drop one UTF-8 character: continuation bytes plus the lead.
                var erased = false;
                while (buf.items.len > 0) {
                    const popped = buf.pop().?;
                    erased = true;
                    if (popped < 0x80 or popped > 0xBF) break;
                }
                if (erased) out("\x08 \x08", .{});
            },
            else => {
                if (b < 0x20) continue;
                buf.append(alloc, b) catch oom();
                // One '*' per character: continuation bytes add none.
                if (b < 0x80 or b > 0xBF) out("*", .{});
            },
        }
    }
}

/// The value spelled so the yaml parser reads it back unchanged: double
/// quotes (with \" and \\ escaped) whenever a plain scalar would not
/// round-trip — a '#', a ':' before a space, edge whitespace, a leading
/// quote, or a leading indicator character.
fn yamlScalar(alloc: std.mem.Allocator, v: []const u8) []const u8 {
    var need = v.len == 0 or
        std.mem.indexOfScalar(u8, v, '#') != null or
        std.mem.indexOf(u8, v, ": ") != null or
        v[v.len - 1] == ':' or
        !std.mem.eql(u8, v, std.mem.trim(u8, v, " \t"));
    if (!need) switch (v[0]) {
        '"', '\'', '[', ']', '{', '}', '&', '*', '|', '>', '-', '!', '%', '@', '`', '#', '?' => need = true,
        else => {},
    };
    if (!need) return v;
    var outb: std.ArrayListUnmanaged(u8) = .empty;
    outb.append(alloc, '"') catch oom();
    for (v) |c| {
        if (c == '"' or c == '\\') outb.append(alloc, '\\') catch oom();
        outb.append(alloc, c) catch oom();
    }
    outb.append(alloc, '"') catch oom();
    return outb.items;
}

/// Textual splice of `block` ("  NAME:\n    key: v\n...") into `text`
/// under the top-level `clusters:` key, preserving all other content:
/// replaces NAME's block when it exists, appends to the section, or adds
/// the whole `clusters:` section at the end.
fn spliceCluster(alloc: std.mem.Allocator, text: []const u8, name: []const u8, block: []const u8) error{OutOfMemory}![]const u8 {
    const blank = 0;
    const comment = 1;
    const content = 2;
    const Kind = struct {
        fn of(line: []const u8) usize {
            const t = std.mem.trimStart(u8, line, " ");
            if (t.len == 0) return blank;
            if (t[0] == '#') return comment;
            return content;
        }
        fn indent(line: []const u8) usize {
            var i: usize = 0;
            while (i < line.len and line[i] == ' ') i += 1;
            return i;
        }
    };

    var lines: std.ArrayListUnmanaged([]const u8) = .empty;
    var lit = std.mem.splitScalar(u8, text, '\n');
    while (lit.next()) |l| try lines.append(alloc, l);
    if (lines.items.len > 0 and lines.items[lines.items.len - 1].len == 0)
        lines.items.len -= 1;

    var block_lines: std.ArrayListUnmanaged([]const u8) = .empty;
    var bit = std.mem.splitScalar(u8, block, '\n');
    while (bit.next()) |l| try block_lines.append(alloc, l);
    if (block_lines.items.len > 0 and block_lines.items[block_lines.items.len - 1].len == 0)
        block_lines.items.len -= 1;

    // Top-level `clusters:` line: indent 0, key spelled plainly.
    var clusters_idx: ?usize = null;
    for (lines.items, 0..) |line, i| {
        if (Kind.of(line) != content or Kind.indent(line) != 0) continue;
        const body = line[Kind.indent(line)..];
        if (std.mem.startsWith(u8, body, "clusters:") or std.mem.startsWith(u8, body, "clusters :")) {
            clusters_idx = i;
            break;
        }
    }
    const ci = clusters_idx orelse {
        // No section: a blank line (when the file has content), then
        // `clusters:` and the new block at the end.
        if (lines.items.len > 0) try lines.append(alloc, "");
        try lines.append(alloc, "clusters:");
        try lines.appendSlice(alloc, block_lines.items);
        return joinLines(alloc, lines.items);
    };

    // The section runs until the next indent-0 key or EOF.
    var section_end = lines.items.len;
    for (lines.items[ci + 1 ..], ci + 1..) |line, i| {
        if (Kind.of(line) == content and Kind.indent(line) == 0) {
            section_end = i;
            break;
        }
    }

    // NAME's existing block: a `  NAME:` line, through the next line at
    // indent <=2 (the next cluster or a dedent).
    var replace_start: ?usize = null;
    var replace_end: usize = 0;
    for (lines.items[ci + 1 .. section_end], ci + 1..) |line, i| {
        if (Kind.of(line) != content or Kind.indent(line) != 2) continue;
        const body = line[2..];
        if (body.len > name.len and std.mem.startsWith(u8, body, name) and body[name.len] == ':') {
            replace_start = i;
            replace_end = i + 1;
            while (replace_end < section_end and
                !(Kind.of(lines.items[replace_end]) == content and Kind.indent(lines.items[replace_end]) <= 2))
                replace_end += 1;
            break;
        }
    }

    var out_lines: std.ArrayListUnmanaged([]const u8) = .empty;
    if (replace_start) |rs| {
        try out_lines.appendSlice(alloc, lines.items[0..rs]);
        try out_lines.appendSlice(alloc, block_lines.items);
        try out_lines.appendSlice(alloc, lines.items[replace_end..]);
    } else {
        // After the last section line, before any trailing blank lines.
        var at = section_end;
        while (at > ci + 1 and Kind.of(lines.items[at - 1]) == blank) at -= 1;
        try out_lines.appendSlice(alloc, lines.items[0..at]);
        try out_lines.appendSlice(alloc, block_lines.items);
        try out_lines.appendSlice(alloc, lines.items[at..]);
    }
    return joinLines(alloc, out_lines.items);
}

fn joinLines(alloc: std.mem.Allocator, lines: []const []const u8) ![]const u8 {
    var outb: std.ArrayListUnmanaged(u8) = .empty;
    for (lines) |l| {
        try outb.appendSlice(alloc, l);
        try outb.append(alloc, '\n');
    }
    return outb.items;
}

/// Store NAME as the current cluster; NAME must exist in the loaded doc.
fn setCurrentCluster(
    init: std.process.Init,
    alloc: std.mem.Allocator,
    common: cli_args.Common,
    name: []const u8,
    source: *config.Source,
    names: []const []const u8,
) void {
    var known = false;
    for (names) |t| {
        if (std.mem.eql(u8, t, name)) known = true;
    }
    if (!known) {
        source.target = name;
        unknownTargetFatal(alloc, source);
    }
    config.writeCurrentCluster(init.io, alloc, init.environ_map, name) catch |err|
        fatal("cannot store current cluster ({s})", .{@errorName(err)});
    if (!common.quiet) note("cluster set to '{s}'", .{name});
}

/// Idle bound applied to `kite consume` when stdout is not a terminal and no
/// --max/--idle/--follow was given.
const default_idle_ms: u64 = 5000;

var interrupted = std.atomic.Value(bool).init(false);

/// True once the reader of stdout has gone away (e.g. `| head` exited).
fn stdoutClosed() bool {
    var fds = [_]std.posix.pollfd{.{ .fd = std.posix.STDOUT_FILENO, .events = 0, .revents = 0 }};
    const n = std.posix.poll(&fds, 0) catch return false;
    return n > 0 and (fds[0].revents & (std.posix.POLL.ERR | std.posix.POLL.HUP)) != 0;
}

fn onInterrupt(_: std.posix.SIG) callconv(.c) void {
    interrupted.store(true, .release);
}

/// SIGINT/SIGTERM request a graceful stop (summary still printed); SIGPIPE
/// is ignored so a closed downstream surfaces as a write error instead of
/// killing the process with status 141.
fn installSignalHandlers() void {
    const stop: std.posix.Sigaction = .{
        .handler = .{ .handler = onInterrupt },
        .mask = std.posix.sigemptyset(),
        .flags = std.posix.SA.RESTART,
    };
    std.posix.sigaction(.INT, &stop, null);
    std.posix.sigaction(.TERM, &stop, null);
    const ignore: std.posix.Sigaction = .{
        .handler = .{ .handler = std.posix.SIG.IGN },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.PIPE, &ignore, null);
}

/// Lap timer over the monotonic `Io` clock (replaces std.time.Timer).
const Lap = struct {
    io: std.Io,
    last: std.Io.Timestamp,
    fn init(io: std.Io) Lap {
        return .{ .io = io, .last = .now(io, .awake) };
    }
    fn lap(l: *Lap) u64 {
        const n = std.Io.Timestamp.now(l.io, .awake);
        const d = l.last.durationTo(n);
        l.last = n;
        return @intCast(@max(0, d.toNanoseconds()));
    }
};

/// Turn a CSV row into a record: JSON object value, optional column key.
fn csvRecord(
    alloc: std.mem.Allocator,
    row: []u8,
    cols: []const []const u8,
    key_idx: ?usize,
    static_headers: []const protocol.Header,
    rowno: u64,
) protocol.Record {
    const fields = csv.splitFields(alloc, row) catch
        fatal("csv row {d}: unterminated quoted field", .{rowno});
    if (fields.len != cols.len)
        fatal("csv row {d}: expected {d} field(s), got {d}", .{ rowno, cols.len, fields.len });
    var jw = std.Io.Writer.Allocating.init(alloc);
    csv.rowJson(&jw.writer, cols, fields) catch fatal("out of memory", .{});
    return .{
        .key = if (key_idx) |ki| fields[ki] else null,
        .value = jw.written(),
        .headers = static_headers,
    };
}

fn pendingBytes(pend: []Pending) usize {
    var n: usize = 0;
    for (pend) |p| n += p.bytes;
    return n;
}

/// Estimated encoded size of a record: key + value + header bytes plus
/// varint framing (~16B/record, ~8B/header). Charged against batch_size so
/// encoded batches stay under the broker's ~1MiB message.max.bytes.
fn recordSize(rec: protocol.Record) usize {
    var n: usize = 16 + rec.value.len;
    if (rec.key) |k| n += k.len;
    for (rec.headers) |h| {
        n += h.key.len + 8;
        if (h.value) |v| n += v.len;
    }
    return n;
}

/// Record-batch header (61B) plus slack, charged against batch_size on
/// every flush check.
const batch_overhead = 96;

fn stripCr(s: []const u8) []const u8 {
    return if (s.len > 0 and s[s.len - 1] == '\r') s[0 .. s.len - 1] else s;
}

/// Read one line (without the trailing newline). Lines longer than the
/// reader's buffer spill into the arena. Returns null at EOF.
fn nextLine(r: *std.Io.Reader, alloc: std.mem.Allocator) !?[]const u8 {
    const maybe = r.takeDelimiter('\n') catch |err| switch (err) {
        error.ReadFailed => return error.ReadFailed,
        error.StreamTooLong => {
            var lw = std.Io.Writer.Allocating.init(alloc);
            _ = r.streamDelimiterEnding(&lw.writer, '\n') catch |e2| switch (e2) {
                error.WriteFailed => return error.OutOfMemory,
                error.ReadFailed => return error.ReadFailed,
            };
            // Consume the newline if the line was newline-terminated.
            if (r.peekByte()) |b| {
                if (b == '\n') r.toss(1);
            } else |_| {}
            return stripCr(lw.written());
        },
    };
    const line = maybe orelse return null;
    const owned = try alloc.dupe(u8, stripCr(line));
    return owned;
}

fn flushAll(c: *client.Client, topic: []const u8, pend: []Pending) !void {
    var parts: std.ArrayListUnmanaged(usize) = .empty;
    var sets: std.ArrayListUnmanaged([]const protocol.Record) = .empty;
    defer parts.deinit(c.alloc);
    defer sets.deinit(c.alloc);
    for (pend, 0..) |*p, i| {
        if (p.records.items.len == 0) continue;
        try parts.append(c.alloc, i);
        try sets.append(c.alloc, p.records.items);
    }
    if (parts.items.len == 0) return;
    try c.produceEnqueue(topic, parts.items, sets.items);
    for (parts.items) |i| {
        pend[i].records.clearRetainingCapacity();
        pend[i].bytes = 0;
    }
}

fn produceFatal(c: *client.Client, err: anyerror) noreturn {
    switch (err) {
        error.ProduceFailed, error.MetadataFailed => {
            const detail = c.errDetail();
            if (detail.len > 0) fatal("{s}", .{detail});
            fatal("produce failed", .{});
        },
        else => fatal("produce failed: {s}", .{@errorName(err)}),
    }
}
