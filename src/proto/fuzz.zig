//! Wire-format parsers under mutated input: never panic, never leak.
//!
//! Chaos tests run 50k PRNG-mutated packets on every `zig build test`.
//! The `std.testing.fuzz` targets below need `zig build test --fuzz`, which
//! is broken upstream in zig 0.16.0 (test_runner StackTrace mismatch).

const std = @import("std");
const Proto = @import("root.zig");
const BinaryStream = @import("BinaryStream").BinaryStream;

fn mutate(valid: []const u8, smith: *std.testing.Smith, buf: []u8) []const u8 {
    std.debug.assert(buf.len >= valid.len + 32);

    const len = smith.index(valid.len + 33); // 0 .. valid.len + 32
    smith.bytes(buf[0..len]);
    const copy_len = @min(len, valid.len);
    @memcpy(buf[0..copy_len], valid[0..copy_len]);

    const flips = smith.index(valid.len + 1);
    var i: usize = 0;
    while (i < flips) : (i += 1) {
        const pos = smith.index(valid.len);
        smith.bytes(buf[pos .. pos + 1]);
    }
    return buf[0..len];
}

fn chaosMutate(valid: []const u8, rng: std.Random, buf: []u8) []const u8 {
    std.debug.assert(buf.len >= valid.len + 32);

    const len = rng.uintLessThan(usize, valid.len + 33); // 0 .. valid.len + 32
    rng.bytes(buf[0..len]);
    const copy_len = @min(len, valid.len);
    @memcpy(buf[0..copy_len], valid[0..copy_len]);

    const flips = rng.uintLessThan(usize, valid.len + 1);
    var i: usize = 0;
    while (i < flips) : (i += 1) {
        buf[rng.uintLessThan(usize, valid.len)] = rng.int(u8);
    }
    return buf[0..len];
}

fn readStreamOver(data: []const u8) BinaryStream {
    return BinaryStream{
        .payload = @constCast(data),
        .written = data.len,
        .offset = 0,
        .allocator = undefined,
        .owns_buffer = false,
    };
}

fn validFrameSet(buf: []u8) ![]const u8 {
    var frames: [3]Proto.Frame = undefined;
    frames[0] = Proto.Frame.init(1, null, null, null, .Reliable, "hello world", null, null, null, null);
    frames[1] = Proto.Frame.init(2, null, 7, 1, .ReliableOrdered, "ordered payload!", null, null, null, null);
    frames[2] = Proto.Frame.init(3, null, null, null, .Reliable, "split", 0, 9, 2, null);
    return Proto.FrameSet.serializeInto(1234, &frames, buf);
}

fn fuzzFrameSet(_: void, smith: *std.testing.Smith) anyerror!void {
    const allocator = std.testing.allocator;

    var seed_buf: [256]u8 = undefined;
    const valid = try validFrameSet(&seed_buf);

    var buf: [512]u8 = undefined;
    const input = mutate(valid, smith, &buf);

    var frameset = Proto.FrameSet.deserialize(input, allocator) catch return;
    defer frameset.deinit(allocator);
}

test "fuzz FrameSet.parse" {
    try std.testing.fuzz({}, fuzzFrameSet, .{});
}

fn fuzzAck(_: void, smith: *std.testing.Smith) anyerror!void {
    const allocator = std.testing.allocator;

    const sequences = [_]u32{ 1, 2, 3, 5, 6, 10, 0xFFFFFF };
    var seed_buf: [64]u8 = undefined;
    const valid = try Proto.Ack.serializeInto(&sequences, Proto.Packets.Ack, &seed_buf);

    var buf: [256]u8 = undefined;
    const input = mutate(valid, smith, &buf);

    var ack = Proto.Ack.deserialize(input, allocator) catch return;
    defer ack.deinit();

    // round-trip oracle; skip oversized sets (stack buffer)
    if (ack.sequences.len == 0 or ack.sequences.len > 512) return;
    std.mem.sort(u32, ack.sequences, {}, comptime std.sort.asc(u32));

    var out: [3 + 512 * 4]u8 = undefined;
    const serialized = Proto.Ack.serializeInto(ack.sequences, Proto.Packets.Ack, &out) catch return error.RoundTripSerializeFailed;

    var ack2 = Proto.Ack.deserialize(serialized, allocator) catch return error.RoundTripParseFailed;
    defer ack2.deinit();

    try std.testing.expectEqualSlices(u32, ack.sequences, ack2.sequences);
}

test "fuzz Ack.parse with round-trip oracle" {
    try std.testing.fuzz({}, fuzzAck, .{});
}

fn fuzzAddress(_: void, smith: *std.testing.Smith) anyerror!void {
    const allocator = std.testing.allocator;

    var seed_buf: [16]u8 = undefined;
    const valid = try Proto.Address.init(4, "127.0.0.1", 19132).writeTo(&seed_buf);

    var buf: [128]u8 = undefined;
    const input = mutate(valid, smith, &buf);

    var stream = readStreamOver(input);
    const parsed = Proto.Address.read(&stream, allocator) catch return;
    defer parsed.deinit(allocator);
}

test "fuzz Address.read" {
    try std.testing.fuzz({}, fuzzAddress, .{});
}

fn fuzzConnectionRequestAccepted(_: void, smith: *std.testing.Smith) anyerror!void {
    const allocator = std.testing.allocator;

    const client_addr = Proto.Address.init(4, "127.0.0.1", 19132);
    const system_addr = Proto.Address.init(4, "192.168.1.1", 19133);
    const cra = Proto.ConnectionRequestAccepted.init(client_addr, 0, system_addr, 123456789, 987654321);

    var seed_buf: [Proto.ConnectionRequestAccepted.MAX_SERIALIZED_SIZE]u8 = undefined;
    const valid = try cra.serializeInto(&seed_buf);

    var buf: [Proto.ConnectionRequestAccepted.MAX_SERIALIZED_SIZE + 32]u8 = undefined;
    const input = mutate(valid, smith, &buf);

    var parsed = Proto.ConnectionRequestAccepted.deserialize(input, allocator) catch return;
    defer parsed.deinit(allocator);
}

test "fuzz ConnectionRequestAccepted.parse" {
    try std.testing.fuzz({}, fuzzConnectionRequestAccepted, .{});
}

fn fuzzConnectionRequest2(_: void, smith: *std.testing.Smith) anyerror!void {
    const allocator = std.testing.allocator;

    const request = Proto.ConnectionRequest2.init(
        Proto.Address.init(4, "127.0.0.1", 19132),
        1400,
        0x0102030405060708,
    );

    var seed_buf: [Proto.ConnectionRequest2.MAX_SERIALIZED_SIZE]u8 = undefined;
    const valid = try request.serializeInto(&seed_buf);

    var buf: [Proto.ConnectionRequest2.MAX_SERIALIZED_SIZE + 32]u8 = undefined;
    const input = mutate(valid, smith, &buf);

    var parsed = Proto.ConnectionRequest2.deserialize(input, allocator) catch return;
    defer parsed.deinit(allocator);
}

test "fuzz ConnectionRequest2.parse" {
    try std.testing.fuzz({}, fuzzConnectionRequest2, .{});
}

fn validReply1WithSecurity(buf: []u8) ![]const u8 {
    // serializeInto skips the security extension; craft it by hand
    var s = BinaryStream{
        .payload = buf,
        .written = 0,
        .offset = 0,
        .allocator = undefined,
        .owns_buffer = false,
    };

    try s.writeUint8(Proto.Packets.OpenConnectionReply1);
    try Proto.Magic.write(&s);
    try s.writeInt64(0x0102030405060708, .Big);
    try s.writeBool(true);
    try s.writeUint32(0xDEADBEEF, .Big);

    var key: [294]u8 = undefined;
    for (&key, 0..) |*b, i| b.* = @truncate(i);
    try s.write(&key);

    try s.writeUint16(1400, .Big);
    return s.getBuffer();
}

fn fuzzConnectionReply1(_: void, smith: *std.testing.Smith) anyerror!void {
    var seed_buf: [384]u8 = undefined;
    const valid = try validReply1WithSecurity(&seed_buf);

    var buf: [512]u8 = undefined;
    const input = mutate(valid, smith, &buf);

    var parsed = Proto.ConnectionReply1.deserialize(input) catch return;
    defer parsed.deinit();
}

test "fuzz ConnectionReply1.parse with security" {
    try std.testing.fuzz({}, fuzzConnectionReply1, .{});
}

fn fuzzUnconnectedPong(_: void, smith: *std.testing.Smith) anyerror!void {
    const allocator = std.testing.allocator;

    const pong = Proto.UnconnectedPong.init(123456789, 987654321, "Hello World!");

    var seed_buf: [Proto.UnconnectedPong.SERIALIZED_BASE_SIZE + 12]u8 = undefined;
    const valid = try pong.serializeInto(&seed_buf);

    var buf: [Proto.UnconnectedPong.SERIALIZED_BASE_SIZE + 64]u8 = undefined;
    const input = mutate(valid, smith, &buf);

    var parsed = Proto.UnconnectedPong.deserialize(input, allocator) catch return;
    defer parsed.deinit(allocator);
}

test "fuzz UnconnectedPong.parse" {
    try std.testing.fuzz({}, fuzzUnconnectedPong, .{});
}

// ---------------------------------------------------------------------------
// Chaos layer: the same parsers under a seeded PRNG mutation loop that runs on
// every `zig build test`. std.testing.allocator makes any leak a failure.
// ---------------------------------------------------------------------------

const CHAOS_ITERATIONS = 50_000;

fn chaosFrameSet(rng: std.Random, allocator: std.mem.Allocator) !void {
    var seed_buf: [256]u8 = undefined;
    const valid = try validFrameSet(&seed_buf);
    var buf: [512]u8 = undefined;
    const input = chaosMutate(valid, rng, &buf);

    var frameset = Proto.FrameSet.deserialize(input, allocator) catch return;
    defer frameset.deinit(allocator);
}

fn chaosAck(rng: std.Random, allocator: std.mem.Allocator) !void {
    const sequences = [_]u32{ 1, 2, 3, 5, 6, 10, 0xFFFFFF };
    var seed_buf: [64]u8 = undefined;
    const valid = try Proto.Ack.serializeInto(&sequences, Proto.Packets.Ack, &seed_buf);
    var buf: [256]u8 = undefined;
    const input = chaosMutate(valid, rng, &buf);

    var ack = Proto.Ack.deserialize(input, allocator) catch return;
    defer ack.deinit();

    // Round-trip oracle for the surviving (parseable) mutations.
    if (ack.sequences.len == 0 or ack.sequences.len > 512) return;
    std.mem.sort(u32, ack.sequences, {}, comptime std.sort.asc(u32));

    var out: [3 + 512 * 4]u8 = undefined;
    const serialized = Proto.Ack.serializeInto(ack.sequences, Proto.Packets.Ack, &out) catch return error.RoundTripSerializeFailed;

    var ack2 = Proto.Ack.deserialize(serialized, allocator) catch return error.RoundTripParseFailed;
    defer ack2.deinit();

    try std.testing.expectEqualSlices(u32, ack.sequences, ack2.sequences);
}

fn chaosAddress(rng: std.Random, allocator: std.mem.Allocator) !void {
    var seed_buf: [16]u8 = undefined;
    const valid = try Proto.Address.init(4, "127.0.0.1", 19132).writeTo(&seed_buf);
    var buf: [128]u8 = undefined;
    const input = chaosMutate(valid, rng, &buf);

    var stream = readStreamOver(input);
    const parsed = Proto.Address.read(&stream, allocator) catch return;
    defer parsed.deinit(allocator);
}

fn chaosConnectionRequestAccepted(rng: std.Random, allocator: std.mem.Allocator) !void {
    const cra = Proto.ConnectionRequestAccepted.init(
        Proto.Address.init(4, "127.0.0.1", 19132),
        0,
        Proto.Address.init(4, "192.168.1.1", 19133),
        123456789,
        987654321,
    );
    var seed_buf: [Proto.ConnectionRequestAccepted.MAX_SERIALIZED_SIZE]u8 = undefined;
    const valid = try cra.serializeInto(&seed_buf);
    var buf: [Proto.ConnectionRequestAccepted.MAX_SERIALIZED_SIZE + 32]u8 = undefined;
    const input = chaosMutate(valid, rng, &buf);

    var parsed = Proto.ConnectionRequestAccepted.deserialize(input, allocator) catch return;
    defer parsed.deinit(allocator);
}

fn chaosConnectionRequest2(rng: std.Random, allocator: std.mem.Allocator) !void {
    const request = Proto.ConnectionRequest2.init(
        Proto.Address.init(4, "127.0.0.1", 19132),
        1400,
        0x0102030405060708,
    );
    var seed_buf: [Proto.ConnectionRequest2.MAX_SERIALIZED_SIZE]u8 = undefined;
    const valid = try request.serializeInto(&seed_buf);
    var buf: [Proto.ConnectionRequest2.MAX_SERIALIZED_SIZE + 32]u8 = undefined;
    const input = chaosMutate(valid, rng, &buf);

    var parsed = Proto.ConnectionRequest2.deserialize(input, allocator) catch return;
    defer parsed.deinit(allocator);
}

fn chaosConnectionReply1(rng: std.Random, allocator: std.mem.Allocator) !void {
    _ = allocator;
    var seed_buf: [384]u8 = undefined;
    const valid = try validReply1WithSecurity(&seed_buf);
    var buf: [512]u8 = undefined;
    const input = chaosMutate(valid, rng, &buf);

    var parsed = Proto.ConnectionReply1.deserialize(input) catch return;
    defer parsed.deinit();
}

fn chaosUnconnectedPong(rng: std.Random, allocator: std.mem.Allocator) !void {
    const pong = Proto.UnconnectedPong.init(123456789, 987654321, "Hello World!");
    var seed_buf: [Proto.UnconnectedPong.SERIALIZED_BASE_SIZE + 12]u8 = undefined;
    const valid = try pong.serializeInto(&seed_buf);
    var buf: [Proto.UnconnectedPong.SERIALIZED_BASE_SIZE + 64]u8 = undefined;
    const input = chaosMutate(valid, rng, &buf);

    var parsed = Proto.UnconnectedPong.deserialize(input, allocator) catch return;
    defer parsed.deinit(allocator);
}

test "chaos: wire parsers survive mutated packets" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5EED_2026_0906);
    const rng = prng.random();

    var iter: usize = 0;
    while (iter < CHAOS_ITERATIONS) : (iter += 1) {
        switch (iter % 7) {
            0 => try chaosFrameSet(rng, allocator),
            1 => try chaosAck(rng, allocator),
            2 => try chaosAddress(rng, allocator),
            3 => try chaosConnectionRequestAccepted(rng, allocator),
            4 => try chaosConnectionRequest2(rng, allocator),
            5 => try chaosConnectionReply1(rng, allocator),
            6 => try chaosUnconnectedPong(rng, allocator),
            else => unreachable, // x % 7 is always in 0..6
        }
    }
}
