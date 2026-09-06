const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;
const Packets = @import("../Packets.zig").Packets;

pub const ConnectedPing = struct {
    pub const MAX_SERIALIZED_SIZE = 9; // id(1) + timestamp(8)
    timestamp: i64,

    pub fn init(timestamp: i64) ConnectedPing {
        return .{ .timestamp = timestamp };
    }

    pub fn deinit(self: *ConnectedPing) void {
        _ = self;
    }

    /// Writes into `out` (zero allocs); returns a slice of it.
    pub fn serializeInto(self: *const ConnectedPing, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try s.writeUint8(Packets.ConnectedPing);
        try s.writeInt64(self.timestamp, .Big);
        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const ConnectedPing, allocator: std.mem.Allocator) ![]const u8 {
        var buf: [ConnectedPing.MAX_SERIALIZED_SIZE]u8 = undefined;
        return allocator.dupe(u8, try self.serializeInto(&buf));
    }

    pub fn deserialize(data: []const u8) !ConnectedPing {
        var stream = BinaryStream{ .payload = @constCast(data), .written = data.len, .offset = 0, .allocator = undefined, .owns_buffer = false };

        _ = try stream.readUint8();
        const timestamp = try stream.readInt64(.Big);

        return .{ .timestamp = timestamp };
    }
};

test "ConnectedPing" {
    var connected_ping = ConnectedPing.init(123456789);

    var buf: [ConnectedPing.MAX_SERIALIZED_SIZE]u8 = undefined;
    const serialized = try connected_ping.serializeInto(&buf);

    var deserialized = try ConnectedPing.deserialize(serialized);
    defer deserialized.deinit();

    try std.testing.expectEqual(connected_ping.timestamp, deserialized.timestamp);
}
