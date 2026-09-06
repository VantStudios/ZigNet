const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;
const Magic = @import("../Magic.zig").Magic;
const Packets = @import("../Packets.zig").Packets;

pub const UnconnectedPing = struct {
    pub const MAX_SERIALIZED_SIZE = 33; // id(1) + timestamp(8) + magic(16) + guid(8)
    timestamp: i64,
    guid: i64,

    pub fn init(timestamp: i64, guid: i64) UnconnectedPing {
        return .{ .timestamp = timestamp, .guid = guid };
    }

    pub fn deinit(self: *UnconnectedPing) void {
        _ = self;
    }

    /// Writes into `out` (zero allocs); returns a slice of it.
    pub fn serializeInto(self: *const UnconnectedPing, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try s.writeUint8(Packets.UnconnectedPing);
        try s.writeInt64(self.timestamp, .Big);
        try Magic.write(&s);
        try s.writeInt64(self.guid, .Big);
        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const UnconnectedPing, allocator: std.mem.Allocator) ![]const u8 {
        var buf: [UnconnectedPing.MAX_SERIALIZED_SIZE]u8 = undefined;
        return allocator.dupe(u8, try self.serializeInto(&buf));
    }

    pub fn deserialize(data: []const u8) !UnconnectedPing {
        var stream = BinaryStream{ .payload = @constCast(data), .written = data.len, .offset = 0, .allocator = undefined, .owns_buffer = false };

        _ = try stream.readUint8();
        const timestamp = try stream.readInt64(.Big);
        try Magic.read(&stream);
        const guid = try stream.readInt64(.Big);

        return .{ .timestamp = timestamp, .guid = guid };
    }
};

test "Unconnected Ping" {
    var ping = UnconnectedPing.init(123456789, 987654321);

    var buf: [UnconnectedPing.MAX_SERIALIZED_SIZE]u8 = undefined;
    const serialized = try ping.serializeInto(&buf);

    var deserialized = try UnconnectedPing.deserialize(serialized);
    defer deserialized.deinit();

    try std.testing.expectEqual(ping.timestamp, deserialized.timestamp);
    try std.testing.expectEqual(ping.guid, deserialized.guid);
}
