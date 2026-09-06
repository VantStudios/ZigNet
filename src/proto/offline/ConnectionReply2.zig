const std = @import("std");
const Packets = @import("../Packets.zig").Packets;
const BinaryStream = @import("BinaryStream").BinaryStream;
const Address = @import("../Address.zig").Address;
const Magic = @import("../Magic.zig").Magic;

pub const ConnectionReply2 = struct {
    pub const MAX_SERIALIZED_SIZE = 35; // id(1) + magic(16) + guid(8) + address(7) + mtu(2) + encryption(1)
    guid: i64,
    address: Address,
    mtu: u16,
    encryption_enabled: bool,
    owns_address: bool,

    pub fn init(guid: i64, address: Address, mtu: u16, encryption_enabled: bool) ConnectionReply2 {
        return .{
            .guid = guid,
            .address = address,
            .mtu = mtu,
            .encryption_enabled = encryption_enabled,
            .owns_address = false,
        };
    }

    pub fn deinit(self: *ConnectionReply2, allocator: std.mem.Allocator) void {
        if (self.owns_address) {
            self.address.deinit(allocator);
        }
    }

    /// Writes into `out` (zero allocs); returns a slice of it.
    pub fn serializeInto(self: *const ConnectionReply2, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try s.writeUint8(Packets.OpenConnectionReply2);
        try Magic.write(&s);
        try s.writeInt64(self.guid, .Big);

        var addr_buf: [16]u8 = undefined;
        try s.write(try self.address.writeTo(&addr_buf));

        try s.writeUint16(self.mtu, .Big);
        try s.writeBool(self.encryption_enabled);
        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const ConnectionReply2, allocator: std.mem.Allocator) ![]const u8 {
        var buf: [ConnectionReply2.MAX_SERIALIZED_SIZE]u8 = undefined;
        return allocator.dupe(u8, try self.serializeInto(&buf));
    }

    pub fn deserialize(data: []const u8, allocator: std.mem.Allocator) !ConnectionReply2 {
        var stream = BinaryStream{ .payload = @constCast(data), .written = data.len, .offset = 0, .allocator = undefined, .owns_buffer = false };
        errdefer stream.deinit();

        _ = try stream.readUint8();
        try Magic.read(&stream);
        const guid = try stream.readInt64(.Big);
        const address = try Address.read(&stream, allocator);
        errdefer address.deinit(allocator);
        const mtu = try stream.readUint16(.Big);
        const encryption_enabled = try stream.readBool();

        return .{
            .guid = guid,
            .address = address,
            .mtu = mtu,
            .encryption_enabled = encryption_enabled,
            .owns_address = true,
        };
    }
};

test "ConnectionReply2" {
    const test_address = Address.init(4, "1.1.1.1", 19132);
    var connection_reply2 = ConnectionReply2.init(987654321, test_address, 1492, false);

    var buf: [ConnectionReply2.MAX_SERIALIZED_SIZE]u8 = undefined;
    const serialized = try connection_reply2.serializeInto(&buf);

    var deserialized = try ConnectionReply2.deserialize(serialized, std.heap.page_allocator);
    defer deserialized.deinit(std.heap.page_allocator);

    try std.testing.expectEqual(connection_reply2.guid, deserialized.guid);
    try std.testing.expectEqual(connection_reply2.mtu, deserialized.mtu);
    try std.testing.expectEqual(connection_reply2.encryption_enabled, deserialized.encryption_enabled);
}
