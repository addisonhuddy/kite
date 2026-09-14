const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    // ReleaseSmall by default (the tool's whole point is a tiny binary);
    // -Doptimize=Debug etc. still available.
    const optimize = b.option(
        std.builtin.OptimizeMode,
        "optimize",
        "Prioritize performance, safety, or binary size",
    ) orelse .ReleaseSmall;

    const strip = optimize != .Debug;

    const mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .strip = strip,
        .single_threaded = true,
        // Errors are one-line messages; no stack traces → drop the machinery.
        .unwind_tables = .none,
        .omit_frame_pointer = true,
        .stack_protector = false,
        .error_tracing = false,
    });

    const exe = b.addExecutable(.{ .name = "kite", .root_module = mod });
    b.installArtifact(exe);

    const test_step = b.step("test", "Run unit tests");
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(unit_tests).step);
}
