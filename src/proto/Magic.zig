const std = @import("std");
const BinaryStream = @import("BinaryStream").BinaryStream;

pub const MagicBytes: [16]u8 = [16]u8{
    0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe,
    0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78,
};

pub const Magic = struct {
    pub const bytes = MagicBytes;

    pub fn read(stream: *BinaryStream) !void {
        const got = stream.read(MagicBytes.len);
        if (got.len != MagicBytes.len) return error.PacketTooShort;
        if (!std.mem.eql(u8, got, &MagicBytes)) return error.InvalidMagic;
    }

    pub fn write(stream: *BinaryStream) !void {
        try stream.write(&MagicBytes);
    }
};

test "Magic read rejects invalid bytes" {
    var bad: [20]u8 = undefined;
    @memset(&bad, 0xAA);
    var stream = BinaryStream{ .payload = &bad, .written = bad.len, .offset = 0, .allocator = undefined, .owns_buffer = false };
    try std.testing.expectError(error.InvalidMagic, Magic.read(&stream));

    var good: [20]u8 = undefined;
    @memcpy(good[0..16], &MagicBytes);
    @memset(good[16..], 0);
    var stream2 = BinaryStream{ .payload = &good, .written = good.len, .offset = 0, .allocator = undefined, .owns_buffer = false };
    try Magic.read(&stream2);
}
