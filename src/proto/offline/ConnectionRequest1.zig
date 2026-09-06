const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;
const Magic = @import("../Magic.zig").Magic;
const Int8 = @import("BinaryStream").Int8;
const Packets = @import("../Packets.zig").Packets;
const Server = @import("../../server/Server.zig");

pub const ConnectionRequest1 = struct {
    protocol: u16,
    mtu_size: u16,

    pub fn init(protocol: u16, mtu_size: u16) ConnectionRequest1 {
        return .{
            .protocol = protocol,
            .mtu_size = mtu_size,
        };
    }

    pub fn deinit(self: *ConnectionRequest1) void {
        _ = self;
    }

    /// Zero-pads `out` (the MTU probe); returns the full slice.
    pub fn serializeInto(self: *const ConnectionRequest1, out: []u8) ![]const u8 {
        var s = BinaryStream{
            .payload = out,
            .written = 0,
            .offset = 0,
            .allocator = undefined,
            .owns_buffer = false,
        };

        try Int8.write(&s, Packets.OpenConnectionRequest1);
        try Magic.write(&s);
        try s.writeUint8(@as(u8, @intCast(self.protocol)));

        if (out.len < s.written) return error.MtuTooSmall;

        @memset(out[s.written..], 0);
        s.written = out.len;
        return s.getBuffer();
    }

    /// Allocating wrapper; caller owns the result.
    pub fn serialize(self: *const ConnectionRequest1, allocator: std.mem.Allocator) ![]const u8 {
        const size = self.mtu_size - Server.UDP_HEADER_SIZE;
        const buffer = try allocator.alloc(u8, size);
        defer allocator.free(buffer);
        return allocator.dupe(u8, try self.serializeInto(buffer));
    }

    pub fn deserialize(data: []const u8) !ConnectionRequest1 {
        var stream = BinaryStream{ .payload = @constCast(data), .written = data.len, .offset = 0, .allocator = undefined, .owns_buffer = false };
        errdefer stream.deinit();

        _ = try Int8.read(&stream);
        try Magic.read(&stream);
        const protocol = try stream.readUint8();
        // The MTU is inferred from the datagram size (RakNet convention).
        var mtu_size: u16 = @intCast(stream.getBuffer().len);
        if (mtu_size + Server.UDP_HEADER_SIZE <= Server.MAX_MTU_SIZE) {
            mtu_size = mtu_size + Server.UDP_HEADER_SIZE;
        } else {
            mtu_size = Server.MAX_MTU_SIZE;
        }

        return .{
            .protocol = protocol,
            .mtu_size = mtu_size,
        };
    }
};

test "ConnectionRequest1" {
    var connection_request1 = ConnectionRequest1.init(11, 1400);

    var buf: [1400 - 28]u8 = undefined;
    const serialized = try connection_request1.serializeInto(&buf);
    try std.testing.expectEqual(@as(usize, 1400 - 28), serialized.len);

    var deserialized = try ConnectionRequest1.deserialize(serialized);
    defer deserialized.deinit();

    try std.testing.expectEqual(connection_request1.protocol, deserialized.protocol);
    try std.testing.expectEqual(connection_request1.mtu_size, deserialized.mtu_size);
}
