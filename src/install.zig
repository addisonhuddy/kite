const builtin = @import("builtin");
const std = @import("std");
const cli = @import("cli.zig");

const InstallArgs = struct {
    dir: ?[]const u8 = null,
    yes: bool = false,
};

pub fn run(init: std.process.Init, args: []const []const u8, alloc: std.mem.Allocator) noreturn {
    const parsed = parse(alloc, args);
    const options = switch (parsed) {
        .help => {
            writeText(init, std.Io.File.stdout(), cli.install_help);
            std.process.exit(0);
        },
        .err => |message| parseFatal(init, message),
        .ok => |value| value,
    };

    const home = init.environ_map.get("HOME") orelse
        fatal(init, "HOME is not set", .{});
    const raw_dest = options.dir orelse init.environ_map.get("KITE_INSTALL_DIR") orelse
        std.fs.path.join(alloc, &.{ home, ".local", "bin" }) catch
        fatal(init, "out of memory", .{});
    const dest = if (std.fs.path.isAbsolute(raw_dest))
        raw_dest
    else blk: {
        const cwd = std.process.currentPathAlloc(init.io, alloc) catch |err|
            fatal(init, "could not determine current directory: {s}", .{@errorName(err)});
        break :blk std.fs.path.resolve(alloc, &.{ cwd, raw_dest }) catch
            fatal(init, "out of memory", .{});
    };
    const installed = std.fs.path.join(alloc, &.{ dest, "kite" }) catch
        fatal(init, "out of memory", .{});
    const executable = std.process.executablePathAlloc(init.io, alloc) catch |err|
        fatal(init, "could not determine executable path: {s}", .{@errorName(err)});

    std.Io.Dir.copyFileAbsolute(executable, installed, init.io, .{
        .permissions = std.Io.File.Permissions.fromMode(0o755),
        .make_path = true,
        .replace = true,
    }) catch |err| fatal(init, "could not install binary: {s}", .{@errorName(err)});
    writeFmt(init, std.Io.File.stdout(), "installed {s}\n", .{installed});

    const path = init.environ_map.get("PATH") orelse "";
    if (pathContains(path, dest)) {
        writeText(init, std.Io.File.stdout(), "kite is on your PATH — try: kite --help\n");
        std.process.exit(0);
    }

    const shell = init.environ_map.get("SHELL") orelse "";
    const shell_name = std.fs.path.basename(shell);
    const rc = rcPath(alloc, home, shell_name) catch
        fatal(init, "out of memory", .{});
    const line = makePathLine(alloc, dest, home, std.mem.eql(u8, shell_name, "fish")) catch
        fatal(init, "out of memory", .{});

    const interactive = std.Io.File.stdin().isTty(init.io) catch false;
    if (rc != null and (options.yes or interactive)) {
        if (!options.yes) {
            writeFmt(init, std.Io.File.stdout(), "Add {s} to your PATH in {s}? [Y/n] ", .{ dest, rc.? });
            var input_buf: [4096]u8 = undefined;
            var reader = std.Io.File.stdin().reader(init.io, &input_buf);
            const answer = reader.interface.takeDelimiterExclusive('\n') catch |err| switch (err) {
                error.EndOfStream => "",
                else => fatal(init, "could not read response: {s}", .{@errorName(err)}),
            };
            const trimmed = std.mem.trim(u8, answer, " \t\r");
            if (!(trimmed.len == 0 or std.ascii.eqlIgnoreCase(trimmed, "y") or
                std.ascii.eqlIgnoreCase(trimmed, "yes")))
            {
                printPathHint(init, line);
                std.process.exit(0);
            }
        }

        addPathLine(init, alloc, rc.?, line);
        writeFmt(init, std.Io.File.stdout(), "added — restart your shell or run: source {s}\n", .{rc.?});
        std.process.exit(0);
    }

    printPathHint(init, line);
    std.process.exit(0);
}

fn parse(alloc: std.mem.Allocator, args: []const []const u8) cli.Result(InstallArgs) {
    var parsed: InstallArgs = .{};
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            return .help;
        } else if (std.mem.eql(u8, arg, "-y") or std.mem.eql(u8, arg, "--yes")) {
            parsed.yes = true;
        } else if (std.mem.eql(u8, arg, "--dir")) {
            i += 1;
            if (i == args.len) return .{ .err = "--dir requires a value" };
            parsed.dir = args[i];
        } else if (std.mem.startsWith(u8, arg, "--dir=")) {
            parsed.dir = arg["--dir=".len..];
        } else if (arg.len > 0 and arg[0] == '-') {
            return .{ .err = std.fmt.allocPrint(alloc, "unknown option '{s}'", .{arg}) catch "out of memory" };
        } else {
            return .{ .err = std.fmt.allocPrint(alloc, "unexpected argument '{s}'", .{arg}) catch "out of memory" };
        }
    }
    return .{ .ok = parsed };
}

pub fn pathContains(path: []const u8, dest: []const u8) bool {
    var entries = std.mem.splitScalar(u8, path, ':');
    while (entries.next()) |entry| {
        if (samePath(entry, dest)) return true;
    }
    return false;
}

fn samePath(a: []const u8, b: []const u8) bool {
    if (std.mem.eql(u8, a, b)) return true;
    const a_trimmed = withoutTrailingSlashes(a);
    const b_trimmed = withoutTrailingSlashes(b);
    return a_trimmed.len > 0 and std.mem.eql(u8, a_trimmed, b_trimmed);
}

fn withoutTrailingSlashes(path: []const u8) []const u8 {
    var end = path.len;
    while (end > 0 and path[end - 1] == '/') : (end -= 1) {}
    return path[0..end];
}

pub fn makePathLine(alloc: std.mem.Allocator, dest: []const u8, home: []const u8, fish: bool) ![]u8 {
    if (fish) return std.fmt.allocPrint(alloc, "fish_add_path \"{s}\"", .{dest});

    if (std.mem.eql(u8, dest, home)) {
        return std.fmt.allocPrint(alloc, "export PATH=\"$HOME:$PATH\"", .{});
    } else if (home.len > 0 and std.mem.startsWith(u8, dest, home) and
        dest.len > home.len and dest[home.len] == '/')
    {
        return std.fmt.allocPrint(alloc, "export PATH=\"$HOME{s}:$PATH\"", .{dest[home.len..]});
    }
    return std.fmt.allocPrint(alloc, "export PATH=\"{s}:$PATH\"", .{dest});
}

fn rcPath(alloc: std.mem.Allocator, home: []const u8, shell: []const u8) !?[]u8 {
    const name = if (std.mem.eql(u8, shell, "zsh"))
        ".zshrc"
    else if (std.mem.eql(u8, shell, "bash"))
        if (builtin.os.tag == .macos) ".bash_profile" else ".bashrc"
    else if (std.mem.eql(u8, shell, "fish"))
        ".config/fish/config.fish"
    else
        return null;
    return try std.fs.path.join(alloc, &.{ home, name });
}

fn addPathLine(init: std.process.Init, alloc: std.mem.Allocator, rc: []const u8, line: []const u8) void {
    const existing = readFile(init, alloc, rc) catch |err|
        fatal(init, "could not read {s}: {s}", .{ rc, @errorName(err) });
    if (existing) |contents| {
        if (std.mem.indexOf(u8, contents, line) != null) return;
    }

    const parent = std.fs.path.dirname(rc) orelse
        fatal(init, "could not determine parent directory for {s}", .{rc});
    std.Io.Dir.cwd().createDirPath(init.io, parent) catch |err|
        fatal(init, "could not create parent directory for {s}: {s}", .{ rc, @errorName(err) });
    const addition = std.fmt.allocPrint(alloc, "\n# added by kite install\n{s}\n", .{line}) catch
        fatal(init, "out of memory", .{});
    var file = std.Io.Dir.openFileAbsolute(init.io, rc, .{ .mode = .read_write }) catch |err| switch (err) {
        error.FileNotFound => std.Io.Dir.createFileAbsolute(init.io, rc, .{
            .permissions = std.Io.File.Permissions.fromMode(0o644),
            .truncate = false,
        }) catch |create_err| fatal(init, "could not update {s}: {s}", .{ rc, @errorName(create_err) }),
        else => fatal(init, "could not update {s}: {s}", .{ rc, @errorName(err) }),
    };
    defer file.close(init.io);
    const stat = file.stat(init.io) catch |err|
        fatal(init, "could not update {s}: {s}", .{ rc, @errorName(err) });
    file.writePositionalAll(init.io, addition, stat.size) catch |err|
        fatal(init, "could not update {s}: {s}", .{ rc, @errorName(err) });
}

fn readFile(init: std.process.Init, alloc: std.mem.Allocator, path: []const u8) !?[]u8 {
    var file = std.Io.Dir.openFileAbsolute(init.io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(init.io);
    const stat = try file.stat(init.io);
    var input_buf: [4096]u8 = undefined;
    var reader = file.reader(init.io, &input_buf);
    return try reader.interface.readAlloc(alloc, @intCast(stat.size));
}

fn printPathHint(init: std.process.Init, line: []const u8) void {
    writeFmt(init, std.Io.File.stderr(), "{s}\nAdd that line to your shell startup file and restart your shell.\n", .{line});
}

fn writeText(init: std.process.Init, file: std.Io.File, text: []const u8) void {
    writeFmt(init, file, "{s}", .{text});
}

fn writeFmt(init: std.process.Init, file: std.Io.File, comptime fmt: []const u8, args: anytype) void {
    var buf: [4096]u8 = undefined;
    var writer = file.writer(init.io, &buf);
    writer.interface.print(fmt, args) catch {};
    writer.interface.flush() catch {};
}

fn parseFatal(init: std.process.Init, message: []const u8) noreturn {
    writeFmt(init, std.Io.File.stderr(), "kite: {s}\n{s}", .{ message, cli.install_usage });
    std.process.exit(1);
}

fn fatal(init: std.process.Init, comptime fmt: []const u8, args: anytype) noreturn {
    writeFmt(init, std.Io.File.stderr(), "kite: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

test "path lines substitute HOME and support fish" {
    const alloc = std.testing.allocator;
    const bash_line = try makePathLine(alloc, "/home/test/.local/bin", "/home/test", false);
    defer alloc.free(bash_line);
    try std.testing.expectEqualStrings("export PATH=\"$HOME/.local/bin:$PATH\"", bash_line);

    const fish_line = try makePathLine(alloc, "/tmp/bin", "/home/test", true);
    defer alloc.free(fish_line);
    try std.testing.expectEqualStrings("fish_add_path \"/tmp/bin\"", fish_line);
}

test "PATH contains exact entries and trailing slash variants" {
    try std.testing.expect(pathContains("/usr/bin:/home/test/.local/bin", "/home/test/.local/bin"));
    try std.testing.expect(pathContains("/usr/bin:/home/test/.local/bin/", "/home/test/.local/bin"));
    try std.testing.expect(!pathContains("/usr/bin:/home/test/bin", "/home/test/.local/bin"));
}
