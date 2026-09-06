const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;
const Address = @import("../Address.zig").Address;
const Magic = @import("../Magic.zig").Magic;
const Packets = @import("../Packets.zig").Packets;

pub const ConnectionRequest2 = struct {
    pub const MAX_SERIALIZED_SIZE = 39; // id(1)+magic(16)+cookie(4)+sec(1)+address(7)+mtu(2)+guid(8)
    address: Address,
    mtu_size: u16,
    guid: i64,
    cookie: ?u32 = null,
    client_supports_security: bool = false,
    owns_address: bool,

    pub fn init(address: Address, mtu_size: u16, guid: i64) ConnectionRequest2 {
        return .{
            .address = address,
            .mtu_size = mtu_size,
            .guid = guid,
            .owns_address = false,
        };
    }

    pub fn initWithSecurity(address: Address, mtu_size: u16, guid: i64, cookie: u32, client_supports_security: bool) ConnectionRequest2 {
        return .{
            .address = address,
            .mtu_size = mtu_size,
            .guid = guid,
            .cookie = cookie,
            .client_supports_security = client_supports_security,
            .owns_address = false,
        };
    }

    pub fn deinit(self: *ConnectionRequest2, allocator: std.mem.Allocator) void {
        if (self.owns_address) {
            self.address.deinit(allocator);
        }
    }

    /// Writes into `out` (zero allocs); returns a slice of it.
    pub fn serializeInto(self: *const ConnectionRequest2, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try s.writeUint8(Packets.OpenConnectionRequest2);
        try Magic.write(&s);

        if (self.cookie) |cookie| {
            try s.writeUint32(cookie, .Big);
            try s.writeBool(self.client_supports_security);
        }

        var addr_buf: [16]u8 = undefined;
        try s.write(try self.address.writeTo(&addr_buf));

        try s.writeUint16(self.mtu_size, .Big);
        try s.writeInt64(self.guid, .Big);

        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const ConnectionRequest2, allocator: std.mem.Allocator) ![]const u8 {
        var buf: [ConnectionRequest2.MAX_SERIALIZED_SIZE]u8 = undefined;
        return allocator.dupe(u8, try self.serializeInto(&buf));
    }

    pub fn deserialize(data: []const u8, allocator: std.mem.Allocator) !ConnectionRequest2 {
        var stream = BinaryStream{ .payload = @constCast(data), .written = data.len, .offset = 0, .allocator = undefined, .owns_buffer = false };
        errdefer stream.deinit();

        _ = try stream.readUint8();
        try Magic.read(&stream);
        const address = try Address.read(&stream, allocator);
        errdefer address.deinit(allocator);
        const mtu_size = try stream.readUint16(.Big);
        const guid = try stream.readInt64(.Big);

        return .{
            .address = address,
            .mtu_size = mtu_size,
            .guid = guid,
            .owns_address = true,
        };
    }
};

test "ConnectionRequest2" {
    const test_address = Address.init(4, "127.0.0.1", 19132);
    var connection_request2 = ConnectionRequest2.init(test_address, 1492, 987654321);

    var buf: [ConnectionRequest2.MAX_SERIALIZED_SIZE]u8 = undefined;
    const serialized = try connection_request2.serializeInto(&buf);

    var deserialized = try ConnectionRequest2.deserialize(serialized, std.heap.page_allocator);
    defer deserialized.deinit(std.heap.page_allocator);

    try std.testing.expectEqual(connection_request2.mtu_size, deserialized.mtu_size);
    try std.testing.expectEqual(connection_request2.guid, deserialized.guid);
}
