const std = @import("std");
const Timestamp = std.Io.Timestamp;
const Duration = std.Io.Duration;
const Thread = std.Thread;

const Raknet = @import("Raknet");
const Client = Raknet.Client;
const Logger = Raknet.Logger;

// ============ CONFIGURATION ============
const TARGET_HOST = "127.0.0.1";
const TARGET_PORT: u16 = 19132;

const CONNECTION_LEVELS = [_]usize{ 10, 50, 100, 250, 500, 1000 }; // concurrent "players"
const CONNECT_TIMEOUT_SECONDS: u64 = 5; // how long we wait for each client to complete the handshake
const HOLD_DURATION_SECONDS: u64 = 3; // how long we keep the connection open (simulates an active player)
const CONNECTION_FAIL_ABORT_PCT: f64 = 10.0; // if more than this fails to connect, we stop the ramp
// ========================================

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = std.heap.page_allocator;

    Logger.init(allocator);
    Logger.INFO("=== ZigNet RakNet Connection Benchmark ===", .{});
    Logger.INFO("Target: {s}:{d}", .{ TARGET_HOST, TARGET_PORT });

    for (CONNECTION_LEVELS) |level| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const fail_pct = try runConnectionLevel(io, arena.allocator(), level);

        if (fail_pct > CONNECTION_FAIL_ABORT_PCT) {
            Logger.INFO(
                "Connection failure > {d:.0}% at {d} concurrent clients — ceiling reached. Stopping ramp.",
                .{ CONNECTION_FAIL_ABORT_PCT, level },
            );
            return;
        }
    }

    Logger.INFO("Ramp complete without exceeding failure threshold ({d:.0}%).", .{CONNECTION_FAIL_ABORT_PCT});
}

const ClientResult = struct {
    allocator: std.mem.Allocator,
    connected: std.atomic.Value(bool) = .init(false),
    connect_ns: std.atomic.Value(u64) = .init(0),
    start_ts: Timestamp = undefined,
};

const BinaryStream = @import("BinaryStream").BinaryStream;

pub const Login = struct {
    protocol: i32,
    identity: []const u8,
    client: []const u8,

    pub fn serialize(self: *Login, stream: *BinaryStream) ![]const u8 {
        try stream.writeVarInt(0x01);
        try stream.writeInt32(self.protocol, .Big);
        try stream.writeVarInt(@intCast(self.client.len + self.identity.len + 8));
        try stream.writeString32(self.identity, .Little);
        try stream.writeString32(self.client, .Little);
        return stream.getBuffer();
    }

    pub fn deserialize(stream: *BinaryStream) !Login {
        _ = try stream.readVarInt();
        const protocol = try stream.readInt32(.Big);
        _ = try stream.readVarInt();
        const identity = try stream.readString32(.Little);
        const client = try stream.readString32(.Little);

        return Login{
            .protocol = protocol,
            .identity = identity,
            .client = client,
        };
    }
};

fn onConnect(client: *Client, context: ?*anyopaque) void {
    const result: *ClientResult = @ptrCast(@alignCast(context.?));
    const elapsed = result.start_ts.untilNow(client.options.io, .awake);
    result.connect_ns.store(@intCast(elapsed.nanoseconds), .release);
    result.connected.store(true, .release);

    var stream = BinaryStream.init(result.allocator, null, 0);
    defer stream.deinit();

    var packet = Login{
        .protocol = 1001,
        .identity = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3N1ZXIiOiJkZXZlbG9wZXIifQ.signature",
        .client = "{\"ClientRandomId\": 123456789, \"DeviceModel\": \"ZigNet-Benchmark\", \"LanguageCode\": \"en_US\"}",
    };

    if (packet.serialize(&stream)) |serialized| {
        client.sendReliableMessage(serialized, .Immediate);
    } else |err| {
        Logger.ERROR("failed to serialize login packet: {any}", .{err});
    }
}

fn onDisconnect(client: *Client, context: ?*anyopaque) void {
    _ = client;
    _ = context;
}

fn onGamePacket(client: *Client, payload: []const u8, context: ?*anyopaque) void {
    _ = client;
    _ = payload;
    _ = context;
}

fn clientWorker(io: std.Io, allocator: std.mem.Allocator, result: *ClientResult) void {
    var client = Client.init(.{
        .io = io,
        .allocator = allocator,
        .address = TARGET_HOST,
        .port = TARGET_PORT,
    }) catch |err| {
        Logger.ERROR("could not create client: {any}", .{err});
        return;
    };
    defer client.deinit();

    client.setConnectionCallback(onConnect, result);
    client.setGamePacketCallback(onGamePacket, null);
    client.setDisconnectionCallback(onDisconnect, null);

    result.start_ts = Timestamp.now(io, .awake);
    client.connect() catch |err| {
        Logger.ERROR("connect() failed: {any}", .{err});
        return;
    };

    const wait_start = Timestamp.now(io, .awake);
    while (!result.connected.load(.acquire)) {
        if (wait_start.untilNow(io, .awake).toMilliseconds() > CONNECT_TIMEOUT_SECONDS * 1000) {
            return; // timeout: remains marked as "not connected"
        }
        io.sleep(Duration.fromNanoseconds(10 * std.time.ns_per_ms), .awake) catch return;
    }

    // Simulates a player connected and active for a while before disconnecting.
    io.sleep(Duration.fromSeconds(@intCast(HOLD_DURATION_SECONDS)), .awake) catch {};
}

fn runConnectionLevel(io: std.Io, allocator: std.mem.Allocator, concurrency: usize) !f64 {
    Logger.INFO("--- Connection test: {d} concurrent clients ---", .{concurrency});

    var results = try allocator.alloc(ClientResult, concurrency);
    defer allocator.free(results);
    for (results) |*r| r.* = .{ .allocator = allocator };

    var threads = try allocator.alloc(Thread, concurrency);
    defer allocator.free(threads);

    const level_start = Timestamp.now(io, .awake);

    for (0..concurrency) |idx| {
        threads[idx] = try Thread.spawn(.{}, clientWorker, .{ io, allocator, &results[idx] });
    }

    for (threads) |t| t.join();

    const level_elapsed = level_start.untilNow(io, .awake);

    var connected_count: usize = 0;
    var latencies = try std.ArrayList(u64).initCapacity(allocator, concurrency);
    defer latencies.deinit(allocator);

    for (results) |*r| {
        if (r.connected.load(.acquire)) {
            connected_count += 1;
            try latencies.append(allocator, r.connect_ns.load(.acquire));
        }
    }

    const fail_count = concurrency - connected_count;
    const fail_pct = @as(f64, @floatFromInt(fail_count)) / @as(f64, @floatFromInt(concurrency)) * 100.0;

    Logger.INFO("Connected: {d}/{d}  Failed: {d}  ({d:.2}% fail)  total level time: {d:.1}s", .{
        connected_count, concurrency, fail_count, fail_pct, @as(f64, @floatFromInt(level_elapsed.nanoseconds)) / @as(f64, std.time.ns_per_s),
    });

    if (latencies.items.len > 0) {
        std.mem.sort(u64, latencies.items, {}, std.sort.asc(u64));
        const n = latencies.items.len;
        const min = latencies.items[0];
        const max = latencies.items[n - 1];
        const p50 = latencies.items[n * 50 / 100];
        const p95 = latencies.items[@min(n - 1, n * 95 / 100)];

        var sum: u128 = 0;
        for (latencies.items) |v| sum += v;
        const avg: u64 = @intCast(sum / n);

        Logger.INFO("Handshake latency -> min: {d:.1}ms  avg: {d:.1}ms  p50: {d:.1}ms  p95: {d:.1}ms  max: {d:.1}ms", .{
            nsToMs(min), nsToMs(avg), nsToMs(p50), nsToMs(p95), nsToMs(max),
        });
    }

    return fail_pct;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_ms);
}
