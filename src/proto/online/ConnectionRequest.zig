const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;
const Packets = @import("../Packets.zig").Packets;

pub const ConnectionRequest = struct {
    pub const MAX_SERIALIZED_SIZE = 18; // id(1) + guid(8) + timestamp(8) + security(1)
    guid: i64,
    timestamp: i64,
    use_security: bool,

    pub fn init(guid: i64, timestamp: i64, use_security: bool) ConnectionRequest {
        return .{ .guid = guid, .timestamp = timestamp, .use_security = use_security };
    }

    pub fn deinit(self: *ConnectionRequest) void {
        _ = self;
    }

    /// Writes into `out` (zero allocs); returns a slice of it.
    pub fn serializeInto(self: *const ConnectionRequest, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try s.writeUint8(Packets.ConnectionRequest);
        try s.writeInt64(self.guid, .Big);
        try s.writeInt64(self.timestamp, .Big);
        try s.writeBool(self.use_security);
        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const ConnectionRequest, allocator: std.mem.Allocator) ![]const u8 {
        var buf: [ConnectionRequest.MAX_SERIALIZED_SIZE]u8 = undefined;
        return allocator.dupe(u8, try self.serializeInto(&buf));
    }

    pub fn deserialize(data: []const u8) !ConnectionRequest {
        var stream = BinaryStream{ .payload = @constCast(data), .written = data.len, .offset = 0, .allocator = undefined, .owns_buffer = false };

        _ = try stream.readUint8();
        const guid = try stream.readInt64(.Big);
        const timestamp = try stream.readInt64(.Big);
        const use_security = try stream.readBool();

        return .{ .guid = guid, .timestamp = timestamp, .use_security = use_security };
    }
};

test "ConnectionRequest" {
    var request = ConnectionRequest.init(123456789, 987654321, true);

    var buf: [ConnectionRequest.MAX_SERIALIZED_SIZE]u8 = undefined;
    const serialized = try request.serializeInto(&buf);

    var deserialized = try ConnectionRequest.deserialize(serialized);
    defer deserialized.deinit();

    try std.testing.expectEqual(request.guid, deserialized.guid);
    try std.testing.expectEqual(request.timestamp, deserialized.timestamp);
    try std.testing.expectEqual(request.use_security, deserialized.use_security);
}
