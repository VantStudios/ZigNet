const std = @import("std");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const mod = b.addModule("Raknet", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    // Todo! Make this a direct import instead
    const binarystream_dep = b.dependency("BinaryStream", .{});
    mod.addImport("BinaryStream", binarystream_dep.module("BinaryStream"));

    const exe = b.addExecutable(.{
        .name = "Raknet",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "Raknet", .module = mod },
            },
        }),
    });
    // exe.addModule("network", b.dependency("network", .{}).module("network"));

    if (target.result.os.tag == .windows) {
        exe.root_module.linkSystemLibrary("ws2_32", .{});
        mod.linkSystemLibrary("ws2_32", .{});
    }
    b.installArtifact(exe);
    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    // Shared library for bun:ffi
    const ffi_mod = b.createModule(.{
        .root_source_file = b.path("src/ffi.zig"),
        .target = target,
        .optimize = optimize,
    });
    ffi_mod.addImport("BinaryStream", binarystream_dep.module("BinaryStream"));
    const ffi_lib = b.addLibrary(.{
        .name = "zignet",
        .linkage = .dynamic,
        .root_module = ffi_mod,
    });
    if (target.result.os.tag == .windows) {
        ffi_lib.root_module.linkSystemLibrary("ws2_32", .{});
    }
    const ffi_step = b.step("ffi", "Build shared library for bun:ffi");
    const ffi_install = b.addInstallArtifact(ffi_lib, .{});
    ffi_step.dependOn(&ffi_install.step);

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });
    const run_mod_tests = b.addRunArtifact(mod_tests);
    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });
    const run_exe_tests = b.addRunArtifact(exe_tests);
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    const bench_connection = b.addExecutable(.{
        .name = "connection_bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/connection_bench.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "Raknet", .module = mod },
                .{ .name = "BinaryStream", .module = binarystream_dep.module("BinaryStream") },
            },
        }),
    });
    if (target.result.os.tag == .windows) {
        bench_connection.root_module.linkSystemLibrary("ws2_32", .{});
    }
    const bench_cmd = b.addRunArtifact(bench_connection);
    if (b.args) |args| bench_cmd.addArgs(args);
    const bench_step = b.step("bench", "Run connection benchmark");
    bench_step.dependOn(&bench_cmd.step);
}
