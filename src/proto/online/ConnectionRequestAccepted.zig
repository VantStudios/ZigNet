const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;
const Address = @import("../Address.zig").Address;
const Packets = @import("../Packets.zig").Packets;

/// 1 + address(7) + system_index(2) + 20 * address(7) + 2 timestamps(8)
pub const ConnectionRequestAccepted = struct {
    pub const MAX_SERIALIZED_SIZE = 166;
    address: Address,
    system_index: u16,
    addresses: Address,
    request_timestamp: i64,
    timestamp: i64,
    owns_addresses: bool,

    pub fn init(address: Address, system_index: u16, addresses: Address, request_timestamp: i64, timestamp: i64) ConnectionRequestAccepted {
        return .{
            .address = address,
            .system_index = system_index,
            .addresses = addresses,
            .request_timestamp = request_timestamp,
            .timestamp = timestamp,
            .owns_addresses = false,
        };
    }

    /// Writes into `out` (zero allocs); returns a slice of it.
    pub fn serializeInto(self: *const ConnectionRequestAccepted, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try s.writeUint8(Packets.ConnectionRequestAccepted);

        var addr_buf: [16]u8 = undefined;
        try s.write(try self.address.writeTo(&addr_buf));
        try s.writeUint16(self.system_index, .Big);

        var internal_buf: [16]u8 = undefined;
        const internal_encoded = try self.addresses.writeTo(&internal_buf);
        var i: u8 = 0;
        while (i < 20) : (i += 1) {
            try s.write(internal_encoded);
        }

        try s.writeInt64(self.request_timestamp, .Big);
        try s.writeInt64(self.timestamp, .Big);

        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const ConnectionRequestAccepted, allocator: std.mem.Allocator) ![]const u8 {
        var buf: [ConnectionRequestAccepted.MAX_SERIALIZED_SIZE]u8 = undefined;
        return allocator.dupe(u8, try self.serializeInto(&buf));
    }

    pub fn deserialize(data: []const u8, allocator: std.mem.Allocator) !ConnectionRequestAccepted {
        var stream = BinaryStream.init(allocator, data, null);
        errdefer stream.deinit();

        _ = try stream.readUint8();

        const client_address = try Address.read(&stream, allocator);
        errdefer client_address.deinit(allocator);

        const system_idx = try stream.readUint16(.Big);

        const timestamps_size: usize = 16;
        if (stream.offset + timestamps_size >= data.len) {
            return error.PacketTooShort;
        }
        const remaining_for_addresses = data.len - stream.offset - timestamps_size;

        var first_system_address: ?Address = null;
        const addresses_start = stream.offset;

        if (remaining_for_addresses > 0) {
            first_system_address = Address.read(&stream, allocator) catch |err| {
                return err;
            };
            stream.offset = addresses_start + remaining_for_addresses;
        }

        const system_address = first_system_address orelse blk: {
            const dummy_str = try allocator.dupe(u8, "0.0.0.0");
            break :blk Address.initOwned(4, dummy_str, 0);
        };

        const req_timestamp = try stream.readInt64(.Big);
        const server_timestamp = try stream.readInt64(.Big);

        return .{
            .address = client_address,
            .system_index = system_idx,
            .addresses = system_address,
            .request_timestamp = req_timestamp,
            .timestamp = server_timestamp,
            .owns_addresses = true,
        };
    }

    pub fn deinit(self: *ConnectionRequestAccepted, allocator: std.mem.Allocator) void {
        if (self.owns_addresses) {
            self.address.deinit(allocator);
            self.addresses.deinit(allocator);
        }
    }
};

test "ConnectionRequestAccepted" {
    const allocator = std.heap.page_allocator;
    const client_address = Address.init(4, "127.0.0.1", 19132);
    const system_address = Address.init(4, "192.168.1.1", 19133);

    var cra = ConnectionRequestAccepted.init(client_address, 0, system_address, 123456789, 987654321);

    var buf: [ConnectionRequestAccepted.MAX_SERIALIZED_SIZE]u8 = undefined;
    const serialized = try cra.serializeInto(&buf);

    var deserialized = try ConnectionRequestAccepted.deserialize(serialized, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(cra.system_index, deserialized.system_index);
    try std.testing.expectEqual(cra.request_timestamp, deserialized.request_timestamp);
    try std.testing.expectEqual(cra.timestamp, deserialized.timestamp);
}

test "ConnectionRequestAccepted rejects short packets" {
    const allocator = std.heap.page_allocator;
    const short_packet = [_]u8{ Packets.ConnectionRequestAccepted, 4, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x50, 0x00, 0x00 };
    try std.testing.expectError(error.PacketTooShort, ConnectionRequestAccepted.deserialize(&short_packet, allocator));
}
