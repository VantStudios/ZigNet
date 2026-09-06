const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;
const Address = @import("../Address.zig").Address;
const Packets = @import("../Packets.zig").Packets;

pub const NewIncomingConnection = struct {
    pub const MAX_SERIALIZED_SIZE = 94; // id(1) + address(7) + 10*address(7) + 2 timestamps(8)
    address: Address,
    internal_address: Address,
    incoming_timestamp: i64,
    server_timestamp: i64,
    owns_addresses: bool,

    pub fn init(address: Address, internal_address: Address, incoming_timestamp: i64, server_timestamp: i64) NewIncomingConnection {
        return .{
            .address = address,
            .internal_address = internal_address,
            .incoming_timestamp = incoming_timestamp,
            .server_timestamp = server_timestamp,
            .owns_addresses = false,
        };
    }

    pub fn deinit(self: *NewIncomingConnection, allocator: std.mem.Allocator) void {
        if (self.owns_addresses) {
            self.address.deinit(allocator);
            self.internal_address.deinit(allocator);
        }
    }

    /// Writes into `out` (zero allocs); returns a slice of it.
    pub fn serializeInto(self: *const NewIncomingConnection, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try s.writeUint8(Packets.NewIncomingConnection);

        var addr_buf: [16]u8 = undefined;
        try s.write(try self.address.writeTo(&addr_buf));

        var internal_buf: [16]u8 = undefined;
        const internal_encoded = try self.internal_address.writeTo(&internal_buf);
        for (0..10) |_| {
            try s.write(internal_encoded);
        }

        try s.writeInt64(self.incoming_timestamp, .Big);
        try s.writeInt64(self.server_timestamp, .Big);

        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const NewIncomingConnection, allocator: std.mem.Allocator) ![]const u8 {
        var buf: [NewIncomingConnection.MAX_SERIALIZED_SIZE]u8 = undefined;
        return allocator.dupe(u8, try self.serializeInto(&buf));
    }

    pub fn deserialize(data: []const u8, allocator: std.mem.Allocator) !NewIncomingConnection {
        var stream = BinaryStream{ .payload = @constCast(data), .written = data.len, .offset = 0, .allocator = undefined, .owns_buffer = false };
        errdefer stream.deinit();

        _ = try stream.readUint8();

        const address = try Address.read(&stream, allocator);
        errdefer address.deinit(allocator);

        var internal_address: ?Address = null;
        var address_count: usize = 0;
        var last_valid_pos: usize = stream.offset;

        while (address_count < 20) : (address_count += 1) {
            last_valid_pos = stream.offset;
            const addr = Address.read(&stream, allocator) catch |err| {
                if (err == error.InvalidAddressVersion) {
                    stream.offset = last_valid_pos;
                    break;
                }
                if (internal_address) |ia| {
                    ia.deinit(allocator);
                }
                address.deinit(allocator);
                return err;
            };

            if (internal_address) |ia| {
                ia.deinit(allocator);
            }
            internal_address = addr;
        }

        const final_internal_address = internal_address orelse blk: {
            const dummy_str = try allocator.dupe(u8, "0.0.0.0");
            break :blk Address.initOwned(4, dummy_str, 0);
        };

        const incoming_timestamp = try stream.readInt64(.Big);
        const server_timestamp = try stream.readInt64(.Big);

        return .{
            .address = address,
            .internal_address = final_internal_address,
            .incoming_timestamp = incoming_timestamp,
            .server_timestamp = server_timestamp,
            .owns_addresses = true,
        };
    }
};

test "NewIncomingConnection" {
    const allocator = std.testing.allocator;

    const address_str = try allocator.dupe(u8, "127.0.0.1");
    defer allocator.free(address_str);
    const internal_address_str = try allocator.dupe(u8, "192.168.1.100");
    defer allocator.free(internal_address_str);

    const address = Address.init(4, address_str, 19132);
    const internal_address = Address.init(4, internal_address_str, 19132);

    var nic = NewIncomingConnection.init(address, internal_address, 123456789, 987654321);

    var buf: [NewIncomingConnection.MAX_SERIALIZED_SIZE]u8 = undefined;
    const serialized = try nic.serializeInto(&buf);

    var deserialized = try NewIncomingConnection.deserialize(serialized, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(nic.incoming_timestamp, deserialized.incoming_timestamp);
    try std.testing.expectEqual(nic.server_timestamp, deserialized.server_timestamp);
}
