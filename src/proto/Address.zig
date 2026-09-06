const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;

pub const AddressError = error{
    InvalidAddressVersion,
    InvalidAddress,
};

pub const Address = struct {
    version: u8,
    address: []const u8,
    port: u16,
    owned: bool = false, // deinit frees `address` only when true

    pub fn init(version: u8, address: []const u8, port: u16) Address {
        return .{ .version = version, .address = address, .port = port, .owned = false };
    }

    pub fn initOwned(version: u8, address: []const u8, port: u16) Address {
        return .{ .version = version, .address = address, .port = port, .owned = true };
    }

    pub fn deinit(self: Address, allocator: std.mem.Allocator) void {
        if (self.owned) {
            allocator.free(self.address);
        }
    }

    fn parseIPv4(address: []const u8) ![4]u8 {
        var parts = std.mem.splitAny(u8, address, ".");
        var bytes: [4]u8 = undefined;
        var i: usize = 0;

        while (parts.next()) |part| {
            if (i >= 4) return AddressError.InvalidAddress;
            bytes[i] = std.fmt.parseInt(u8, part, 10) catch return AddressError.InvalidAddress;
            i += 1;
        }

        if (i != 4) return AddressError.InvalidAddress;
        return bytes;
    }

    fn parseIPv6(address: []const u8) ![16]u8 {
        var result: [16]u8 = [_]u8{0} ** 16;

        // Handle :: shorthand by finding it and expanding
        const double_colon_pos = std.mem.indexOf(u8, address, "::");

        if (double_colon_pos) |dc_pos| {
            // Split into before and after ::
            const before = address[0..dc_pos];
            const after = if (dc_pos + 2 < address.len) address[dc_pos + 2 ..] else "";

            // Count parts before ::
            var before_count: usize = 0;
            if (before.len > 0) {
                var before_parts = std.mem.splitAny(u8, before, ":");
                while (before_parts.next()) |part| {
                    if (part.len == 0) continue;
                    const value = std.fmt.parseInt(u16, part, 16) catch return AddressError.InvalidAddress;
                    std.mem.writeInt(u16, result[before_count * 2 ..][0..2], value, .big);
                    before_count += 1;
                }
            }

            // Count parts after ::
            var after_count: usize = 0;
            var after_values: [8]u16 = undefined;
            if (after.len > 0) {
                var after_parts = std.mem.splitAny(u8, after, ":");
                while (after_parts.next()) |part| {
                    if (part.len == 0) continue;
                    const value = std.fmt.parseInt(u16, part, 16) catch return AddressError.InvalidAddress;
                    after_values[after_count] = value;
                    after_count += 1;
                }
            }

            // Write after values at the end
            const after_start = 8 - after_count;
            for (0..after_count) |i| {
                std.mem.writeInt(u16, result[(after_start + i) * 2 ..][0..2], after_values[i], .big);
            }

            return result;
        } else {
            // Parse full IPv6 address
            var parts = std.mem.splitAny(u8, address, ":");
            var i: usize = 0;

            while (parts.next()) |part| {
                if (i >= 8) return AddressError.InvalidAddress;
                const value = std.fmt.parseInt(u16, part, 16) catch return AddressError.InvalidAddress;
                std.mem.writeInt(u16, result[i * 2 ..][0..2], value, .big);
                i += 1;
            }

            if (i != 8) return AddressError.InvalidAddress;
            return result;
        }
    }

    pub fn wireSize(self: Address) !usize {
        return switch (self.version) {
            4 => 1 + 4 + 2,
            6 => 1 + 2 + 2 + 4 + 16 + 4,
            else => AddressError.InvalidAddressVersion,
        };
    }

    /// Encodes into `out`; returns a slice of it.
    pub fn writeTo(self: Address, out: []u8) ![]const u8 {
        var pos: usize = 0;

        switch (self.version) {
            4 => {
                if (out.len < 1 + 4 + 2) return AddressError.InvalidAddress;
                out[pos] = self.version;
                pos += 1;
                const ipv4_bytes = try parseIPv4(self.address);
                for (ipv4_bytes) |byte| {
                    out[pos] = (~byte) & 0xff;
                    pos += 1;
                }
                std.mem.writeInt(u16, out[pos..][0..2], self.port, .big);
                pos += 2;
            },

            6 => {
                if (out.len < 1 + 2 + 2 + 4 + 16 + 4) return AddressError.InvalidAddress;
                out[pos] = self.version;
                pos += 1;
                std.mem.writeInt(u16, out[pos..][0..2], 23, .big);
                pos += 2;
                std.mem.writeInt(u16, out[pos..][0..2], self.port, .big);
                pos += 2;
                std.mem.writeInt(u32, out[pos..][0..4], 0, .big);
                pos += 4;

                const ipv6_bytes = try parseIPv6(self.address);
                for (0..8) |i| {
                    const word = std.mem.readInt(u16, ipv6_bytes[i * 2 ..][0..2], .big);
                    std.mem.writeInt(u16, out[pos..][0..2], word ^ 0xffff, .big);
                    pos += 2;
                }
                std.mem.writeInt(u32, out[pos..][0..4], 0, .big);
                pos += 4;
            },
            else => return AddressError.InvalidAddressVersion,
        }
        return out[0..pos];
    }

    /// Allocating wrapper; caller owns the result.
    pub fn write(self: Address, allocator: std.mem.Allocator) ![]u8 {
        const size = try self.wireSize();
        const buffer = try allocator.alloc(u8, size);
        errdefer allocator.free(buffer);
        _ = try self.writeTo(buffer);
        return buffer;
    }

    pub fn read(stream: *BinaryStream, allocator: std.mem.Allocator) !Address {
        const version = try stream.readUint8();
        return switch (version) {
            4 => {
                var ipv4_bytes: [4]u8 = undefined;
                for (0..4) |i| {
                    const byte = try stream.readUint8();
                    ipv4_bytes[i] = ~byte & 0xff;
                }
                const port = try stream.readUint16(.Big);
                const address_str = try std.fmt.allocPrint(allocator, "{d}.{d}.{d}.{d}", .{ ipv4_bytes[0], ipv4_bytes[1], ipv4_bytes[2], ipv4_bytes[3] });
                return Address.initOwned(version, address_str, port);
            },

            6 => {
                _ = try stream.readUint16(.Big); // Skip IPv6 header (23)
                const port = try stream.readUint16(.Big);
                _ = try stream.readUint32(.Big); // Skip padding

                var address_parts: [8]u16 = undefined;

                for (0..8) |i| {
                    const word = try stream.readUint16(.Big);
                    address_parts[i] = word ^ 0xffff;
                }

                _ = try stream.readUint32(.Big); // Skip final padding

                const address_str = try std.fmt.allocPrint(allocator, "{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}", .{
                    address_parts[0], address_parts[1], address_parts[2], address_parts[3],
                    address_parts[4], address_parts[5], address_parts[6], address_parts[7],
                });

                return Address.initOwned(version, address_str, port);
            },

            else => return AddressError.InvalidAddressVersion,
        };
    }
};

test "Address v4 write and deinit borrow semantics" {
    const borrowed = Address.init(4, "127.0.0.1", 19132);
    var buf: [16]u8 = undefined;
    const encoded = try borrowed.writeTo(&buf);
    try std.testing.expectEqual(@as(usize, 7), encoded.len);
    borrowed.deinit(std.testing.allocator);
}
