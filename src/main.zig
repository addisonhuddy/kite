//! kite — ultra-lightweight Kafka CLI.
//! `kite <topic> < file` sends each stdin line as one record value.

const std = @import("std");
const config = @import("config.zig");
const client = @import("client.zig");
const cli_args = @import("cli.zig");
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
    const split = cli_args.splitMode(alloc, args[1..]);
    const mode_args = switch (split) {
        .err => |message| parseFatal(init, message, cli_args.produce_usage),
        .ok => |value| value,
        .help => unreachable,
    };
    switch (mode_args.mode) {
        .consume => {
            runConsume(init, mode_args.rest, alloc);
            return;
        },
        .show_config => {
            runShowConfig(init, mode_args.rest, alloc);
            return;
        },
        .produce => {},
    }
    const parsed = cli_args.parseProduce(alloc, mode_args.rest);
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
    connectAndResolve(&cli, topic, "write", quiet);

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

const search_path_hint = "./kite.properties, $XDG_CONFIG_HOME/kite/kite.properties, ~/.config/kite/kite.properties";

/// Resolve configuration from flags, environment and properties file.
var dummy_source: config.Source = .{};

fn loadConfig(init: std.process.Init, alloc: std.mem.Allocator, common: cli_args.Common, source: *config.Source) config.Config {
    var cfg = config.load(init.io, alloc, init.environ_map, .{
        .bootstrap = common.bootstrap,
        .config_path = common.config_path,
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
    }
    return cfg;
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
        fatal("topic '{s}' does not exist", .{topic});
    cli.createTopic(topic) catch |err| switch (err) {
        error.TopicAuthorizationFailed => fatal(
            "not authorized to create topic '{s}' (check ACLs for this principal)",
            .{topic},
        ),
        else => fatalErr(cli, "could not create topic"),
    };
    if (!quiet) note("created topic '{s}'", .{topic});
    var attempt: usize = 0;
    while (true) {
        if (cli.refreshMetadata(topic)) |_| {
            return;
        } else |err| switch (err) {
            error.TopicNotFound, error.MetadataFailed => {
                attempt += 1;
                if (attempt >= 10)
                    fatalErr(cli, "metadata lookup failed after creating topic");
                cli.sleep(200);
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
/// A missing topic is created on the spot — after confirmation on a
/// terminal, silently otherwise.
fn connectAndResolve(cli: *client.Client, topic: []const u8, access: []const u8, quiet: bool) void {
    cli.bootstrap() catch fatalErr(cli, "could not reach any bootstrap server");
    cli.refreshMetadata(topic) catch |err| switch (err) {
        error.TopicNotFound => createAndResolve(cli, topic, access, quiet),
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
    connectAndResolve(&cli, topic_name, "read", quiet);

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
    cli.deinit();
    std.process.exit(if (stopped_by_signal) 130 else 0);
}

fn runShowConfig(init: std.process.Init, args: []const []const u8, alloc: std.mem.Allocator) noreturn {
    const parsed = cli_args.parseShowConfig(alloc, args);
    const common = switch (parsed) {
        .help => {
            if (term.detect(init.io, std.Io.File.stdout(), init.environ_map)) {
                term.color.enabled = true;
                const page = term.renderHelp(alloc, cli_args.show_config_help) catch fatal("out of memory", .{});
                writeText(init, std.Io.File.stdout(), page);
            } else writeText(init, std.Io.File.stdout(), cli_args.show_config_help);
            std.process.exit(0);
        },
        .err => |message| parseFatal(init, message, cli_args.show_config_usage),
        .ok => |value| value.common,
    };

    var source: config.Source = .{};
    const cfg = loadConfig(init, alloc, common, &source);

    var stdout_buf: [4096]u8 = undefined;
    var w = std.Io.File.stdout().writer(init.io, &stdout_buf);
    const outw = &w.interface;
    const json_mode = common.format == .json;
    if (json_mode) {
        outw.writeAll("{\"file\":") catch {};
        if (source.file) |f| json.writeString(outw, f) catch {} else outw.writeAll("null") catch {};
        outw.writeAll(",\"settings\":{") catch {};
    } else {
        outw.writeAll("config file: ") catch {};
        outw.writeAll(source.file orelse "(none)") catch {};
        outw.writeByte('\n') catch {};
    }
    var first = true;
    for (std.enums.values(config.Key)) |key| {
        const name = config.key_names.get(key);
        const value = config.valueString(&cfg, alloc, key) catch fatal("out of memory", .{});
        if (json_mode) {
            if (!first) outw.writeByte(',') catch {};
            first = false;
            json.writeString(outw, name) catch {};
            outw.writeAll(":{\"value\":") catch {};
            if (value.len == 0) outw.writeAll("null") catch {} else json.writeString(outw, value) catch {};
            outw.writeAll(",\"source\":\"") catch {};
            outw.writeAll(@tagName(source.origins.get(key))) catch {};
            outw.writeByte('"') catch {};
            if (config.isSecret(key)) outw.writeAll(",\"redacted\":true") catch {};
            outw.writeByte('}') catch {};
        } else {
            const shown = if (value.len == 0) "(unset)" else value;
            outw.writeAll(name) catch {};
            outw.writeAll("                        "[0 .. 24 - @min(name.len, 23)]) catch {};
            outw.writeAll(shown) catch {};
            outw.writeAll("                        "[0 .. 24 - @min(shown.len, 23)]) catch {};
            outw.writeAll(@tagName(source.origins.get(key))) catch {};
            outw.writeByte('\n') catch {};
        }
    }
    if (json_mode) outw.writeAll("}}\n") catch {};
    outw.flush() catch {};
    std.process.exit(0);
}

/// Idle bound applied to `kite -c` when stdout is not a terminal and no
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
