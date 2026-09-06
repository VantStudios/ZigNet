const std = @import("std");
const Packets = @import("../Packets.zig").Packets;
const BinaryStream = @import("BinaryStream").BinaryStream;
const Magic = @import("../Magic.zig").Magic;

pub const ConnectionReply1 = struct {
    pub const MAX_SERIALIZED_SIZE = 28; // id(1) + magic(16) + guid(8) + hasSecurity(1) + mtu(2)
    guid: i64,
    hasSecurity: bool,
    mtu_size: u16,
    has_cookie: bool = false,
    cookie: ?u32 = null,
    server_public_key: ?[294]u8 = null,

    pub fn init(guid: i64, hasSecurity: bool, mtu_size: u16) ConnectionReply1 {
        return .{
            .guid = guid,
            .hasSecurity = hasSecurity,
            .mtu_size = mtu_size,
        };
    }

    pub fn initWithSecurity(guid: i64, mtu_size: u16, has_cookie: bool, cookie: u32, server_public_key: ?[294]u8) ConnectionReply1 {
        return .{
            .guid = guid,
            .hasSecurity = true,
            .mtu_size = mtu_size,
            .has_cookie = has_cookie,
            .cookie = cookie,
            .server_public_key = server_public_key,
        };
    }

    pub fn deinit(self: *ConnectionReply1) void {
        _ = self;
    }

    /// Writes into `out` (zero allocs); returns a slice of it.
    /// The security extension (cookie + public key) is not written yet; the
    /// server never enables it today.
    pub fn serializeInto(self: *const ConnectionReply1, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try s.writeUint8(Packets.OpenConnectionReply1);
        try Magic.write(&s);
        try s.writeInt64(self.guid, .Big);
        try s.writeBool(self.hasSecurity);
        try s.writeUint16(self.mtu_size, .Big);
        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const ConnectionReply1, allocator: std.mem.Allocator) ![]const u8 {
        var buf: [ConnectionReply1.MAX_SERIALIZED_SIZE]u8 = undefined;
        return allocator.dupe(u8, try self.serializeInto(&buf));
    }

    pub fn deserialize(data: []const u8) !ConnectionReply1 {
        var stream = BinaryStream{ .payload = @constCast(data), .written = data.len, .offset = 0, .allocator = undefined, .owns_buffer = false };

        _ = try stream.readUint8();
        try Magic.read(&stream);
        const guid = try stream.readInt64(.Big);
        const hasSecurity = try stream.readBool();

        var result = ConnectionReply1{
            .guid = guid,
            .hasSecurity = hasSecurity,
            .mtu_size = 0,
        };

        if (hasSecurity) {
            const remaining = stream.written - stream.offset;

            if (remaining >= 1 + 4 + 294 + 2) {
                result.has_cookie = try stream.readBool();
                result.cookie = try stream.readUint32(.Big);
                var public_key: [294]u8 = undefined;
                const key_data = stream.read(294);
                @memcpy(&public_key, key_data);
                result.server_public_key = public_key;
            } else if (remaining >= 4 + 2) {
                result.cookie = try stream.readUint32(.Big);
            }
        }

        result.mtu_size = try stream.readUint16(.Big);
        return result;
    }
};

test "ConnectionReply1" {
    var connection_reply1 = ConnectionReply1.init(123456789, true, 1492);

    var buf: [ConnectionReply1.MAX_SERIALIZED_SIZE]u8 = undefined;
    const serialized = try connection_reply1.serializeInto(&buf);

    var deserialized = try ConnectionReply1.deserialize(serialized);
    defer deserialized.deinit();

    try std.testing.expectEqual(connection_reply1.guid, deserialized.guid);
    try std.testing.expectEqual(connection_reply1.hasSecurity, deserialized.hasSecurity);
    try std.testing.expectEqual(connection_reply1.mtu_size, deserialized.mtu_size);
}
