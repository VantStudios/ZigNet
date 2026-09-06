const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;
const Magic = @import("../Magic.zig").Magic;
const Packets = @import("../Packets.zig").Packets;

/// id(1) + timestamp(8) + guid(8) + magic(16) + string16(2) + message
pub const UnconnectedPong = struct {
    pub const SERIALIZED_BASE_SIZE = 35;
    timestamp: i64,
    guid: i64,
    message: []const u8,
    owns_message: bool,

    pub fn init(timestamp: i64, guid: i64, message: []const u8) UnconnectedPong {
        return .{
            .timestamp = timestamp,
            .guid = guid,
            .message = message,
            .owns_message = false,
        };
    }

    pub fn deinit(self: *UnconnectedPong, allocator: std.mem.Allocator) void {
        if (self.owns_message) {
            allocator.free(self.message);
        }
    }

    /// Writes into `out` (zero allocs); returns a slice of it.
    /// `out` must hold `SERIALIZED_BASE_SIZE + message.len`.
    pub fn serializeInto(self: *const UnconnectedPong, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try s.writeUint8(Packets.UnconnectedPong);
        try s.writeInt64(self.timestamp, .Big);
        try s.writeInt64(self.guid, .Big);
        try Magic.write(&s);
        try s.writeString16(self.message, .Big);
        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const UnconnectedPong, allocator: std.mem.Allocator) ![]const u8 {
        const buffer = try allocator.alloc(u8, SERIALIZED_BASE_SIZE + self.message.len);
        defer allocator.free(buffer);
        return allocator.dupe(u8, try self.serializeInto(buffer));
    }

    pub fn deserialize(data: []const u8, allocator: std.mem.Allocator) !UnconnectedPong {
        var stream = BinaryStream{ .payload = @constCast(data), .written = data.len, .offset = 0, .allocator = undefined, .owns_buffer = false };
        errdefer stream.deinit();

        _ = try stream.readUint8();
        const timestamp = try stream.readInt64(.Big);
        const guid = try stream.readInt64(.Big);
        try Magic.read(&stream);
        const message = try stream.readString16(.Big);
        const owned_message = try allocator.dupe(u8, message);

        return .{
            .timestamp = timestamp,
            .guid = guid,
            .message = owned_message,
            .owns_message = true,
        };
    }
};

test "Unconnected Pong" {
    const allocator = std.heap.page_allocator;
    var pong = UnconnectedPong.init(123456789, 987654321, "Hello World!");

    var buf: [UnconnectedPong.SERIALIZED_BASE_SIZE + 12]u8 = undefined;
    const serialized = try pong.serializeInto(&buf);

    var deserialized = try UnconnectedPong.deserialize(serialized, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(pong.timestamp, deserialized.timestamp);
    try std.testing.expectEqual(pong.guid, deserialized.guid);
    try std.testing.expectEqualStrings(pong.message, deserialized.message);
}
