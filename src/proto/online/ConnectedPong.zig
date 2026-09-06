const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;
const Packets = @import("../Packets.zig").Packets;

pub const ConnectedPong = struct {
    pub const MAX_SERIALIZED_SIZE = 17; // id(1) + timestamp(8) + pong_timestamp(8)
    timestamp: i64,
    pong_timestamp: i64,

    pub fn init(timestamp: i64, pong_timestamp: i64) ConnectedPong {
        return .{ .timestamp = timestamp, .pong_timestamp = pong_timestamp };
    }

    pub fn deinit(self: *ConnectedPong) void {
        _ = self;
    }

    /// Writes into `out` (zero allocs); returns a slice of it.
    pub fn serializeInto(self: *const ConnectedPong, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try s.writeUint8(Packets.ConnectedPong);
        try s.writeInt64(self.timestamp, .Big);
        try s.writeInt64(self.pong_timestamp, .Big);
        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const ConnectedPong, allocator: std.mem.Allocator) ![]const u8 {
        var buf: [ConnectedPong.MAX_SERIALIZED_SIZE]u8 = undefined;
        return allocator.dupe(u8, try self.serializeInto(&buf));
    }

    pub fn deserialize(data: []const u8) !ConnectedPong {
        var stream = BinaryStream{ .payload = @constCast(data), .written = data.len, .offset = 0, .allocator = undefined, .owns_buffer = false };

        _ = try stream.readUint8();
        const timestamp = try stream.readInt64(.Big);
        const pong_timestamp = try stream.readInt64(.Big);

        return .{ .timestamp = timestamp, .pong_timestamp = pong_timestamp };
    }
};

test "ConnectedPong" {
    var pong = ConnectedPong.init(123456789, 987654321);

    var buf: [ConnectedPong.MAX_SERIALIZED_SIZE]u8 = undefined;
    const serialized = try pong.serializeInto(&buf);

    var deserialized = try ConnectedPong.deserialize(serialized);
    defer deserialized.deinit();

    try std.testing.expectEqual(pong.timestamp, deserialized.timestamp);
    try std.testing.expectEqual(pong.pong_timestamp, deserialized.pong_timestamp);
}
