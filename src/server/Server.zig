const std = @import("std");
const Thread = std.Thread;
const Timestamp = std.Io.Timestamp;
const Duration = std.Io.Duration;
const Mutex = std.Io.Mutex;
const builtin = @import("builtin");

const Logger = @import("../misc/Logger.zig").Logger;
const Proto = @import("../proto/root.zig");
const Packets = Proto.Packets;
const Socket = @import("../socket/socket.zig").Socket;
const Connection = @import("./Connection.zig").Connection;

const is_windows = builtin.os.tag == .windows;

const PERFORM_TIME_CHECKS = false;

pub const UDP_HEADER_SIZE = 28;
pub const MAX_MTU_SIZE = 1400;
pub const ConnectCallback = *const fn (connection: *Connection, context: ?*anyopaque) void;
pub const DisconnectCallback = *const fn (connection: *Connection, context: ?*anyopaque) void;
pub const TickCallback = *const fn (context: ?*anyopaque) void;

// per-IP offline rate limit: floods get capped ~10x+, legit traffic never does
const MAX_RATE_TRACKED = 256;
const RATE_CAPACITY_TOKENS: i32 = 150;
const RATE_TOKENS_PER_S: i64 = 100;

const RateEntry = struct {
    key: i64,
    tokens: i32,
    last_ns: i64,
};

pub const Server = struct {
    const Self = @This();
    io: std.Io,
    options: ServerOptions,
    socket: Socket,
    running: std.atomic.Value(bool) = .init(false),
    connections: std.AutoHashMap(i64, *Connection),
    connections_mutex: Mutex,
    tick_thread: ?std.Io.Future(void) = null,
    connect_callback: ?ConnectCallback,
    connect_context: ?*anyopaque,
    disconnect_callback: ?DisconnectCallback,
    disconnect_context: ?*anyopaque,
    tick_callback: ?TickCallback,
    tick_context: ?*anyopaque,
    last_connection_tick_ns: Duration = .zero,
    last_callback_tick_ns: Duration = .zero,
    last_total_tick_ns: Duration = .zero,
    rate_entries: [MAX_RATE_TRACKED]RateEntry = undefined,
    rate_count: usize = 0,

    /// Fast hash for address -> i64 key (no allocation)
    /// IPv4: packs ip (4 bytes) + port (2 bytes) into lower 48 bits
    /// IPv6: uses FNV-1a hash of the 16-byte address + port
    pub inline fn addressToKey(address: std.Io.net.IpAddress) i64 {
        return switch (address) {
            .ip4 => |ip4| blk: {
                const ip: u32 = @bitCast(ip4.bytes);
                const port = ip4.port;
                break :blk @as(i64, ip) | (@as(i64, port) << 32);
            },
            .ip6 => |ip6| blk: {
                // FNV-1a hash for IPv6
                var hash: u64 = 0xcbf29ce484222325;
                for (ip6.bytes) |byte| {
                    hash ^= byte;
                    hash *%= 0x100000001b3;
                }
                hash ^= ip6.port;
                hash *%= 0x100000001b3;
                break :blk @bitCast(hash);
            },
        };
    }

    /// Please provide a proper allocator.
    ///
    /// It uses std.heap.page_allocator by default but Arena or DebugAllocator is preffered.
    pub fn init(io: std.Io, options: ServerOptions) !Server {
        var server = Server{
            .io = io,
            .options = options,
            .socket = try Socket.init(
                io,
                options.allocator,
                options.address,
                options.port,
            ),
            .connections = std.AutoHashMap(i64, *Connection).init(options.allocator),
            .connections_mutex = Mutex.init,
            .connect_callback = options.connect_callback,
            .connect_context = options.connect_context,
            .disconnect_callback = options.disconnect_callback,
            .disconnect_context = options.disconnect_context,
            .tick_callback = options.tick_callback,
            .tick_context = options.tick_context,
        };
        server.resetRateLimiter();
        return server;
    }

    fn resetRateLimiter(self: *Self) void {
        self.rate_count = 0;
    }

    // socket thread only: no locking
    fn allowOfflinePacket(self: *Self, key: i64, now: Timestamp) bool {
        const now_ns: i64 = @intCast(now.nanoseconds);

        for (self.rate_entries[0..self.rate_count]) |*entry| {
            if (entry.key == key) {
                const elapsed_ns = now_ns - entry.last_ns;
                const refill: i64 = @divTrunc(elapsed_ns * RATE_TOKENS_PER_S, std.time.ns_per_s);
                entry.tokens = @min(RATE_CAPACITY_TOKENS, entry.tokens + @as(i32, @intCast(refill)));
                entry.last_ns = now_ns;
                if (entry.tokens <= 0) return false;
                entry.tokens -= 1;
                return true;
            }
        }

        if (self.rate_count < MAX_RATE_TRACKED) {
            self.rate_entries[self.rate_count] = .{ .key = key, .tokens = RATE_CAPACITY_TOKENS - 1, .last_ns = now_ns };
            self.rate_count += 1;
            return true;
        }

        // Table full: evict the entry with the fewest tokens (least active).
        var victim: usize = 0;
        for (self.rate_entries[0..self.rate_count], 0..) |*entry, i| {
            if (entry.tokens < self.rate_entries[victim].tokens) victim = i;
        }
        self.rate_entries[victim] = .{ .key = key, .tokens = RATE_CAPACITY_TOKENS - 1, .last_ns = now_ns };
        return true;
    }

    fn tickLoop(self: *Self) void {
        const tick_interval = Duration.fromNanoseconds(
            @divTrunc(std.time.ns_per_s, @as(i96, self.options.tick_rate)),
        );

        var next_tick_deadline = Timestamp.now(self.io, .awake).addDuration(tick_interval);

        while (self.running.load(.acquire)) {
            waitUntil(self.io, next_tick_deadline);
            if (!self.running.load(.acquire)) break;

            const tick_start = Timestamp.now(self.io, .awake);
            next_tick_deadline = advanceTickDeadline(next_tick_deadline, tick_start, tick_interval);

            var to_remove: [64]i64 = undefined;
            var removed: [64]*Connection = undefined;
            var remove_count: usize = 0;
            var removed_count: usize = 0;

            self.connections_mutex.lock(self.io) catch |err| {
                Logger.WARN("mutex lock failed: {}", .{err});
                return;
            };

            {
                var itter = self.connections.iterator();
                while (itter.next()) |val| {
                    const conn = val.value_ptr.*;
                    if (conn.active) {
                        conn.tick();
                    } else if (remove_count < to_remove.len) {
                        to_remove[remove_count] = val.key_ptr.*;
                        remove_count += 1;
                    }
                }
            }

            for (to_remove[0..remove_count]) |key| {
                if (self.connections.fetchRemove(key)) |entry| {
                    removed[removed_count] = entry.value;
                    removed_count += 1;
                }
            }

            self.connections_mutex.unlock(self.io);

            // callbacks fire unlocked: user code may re-enter the Server
            for (removed[0..removed_count]) |conn| {
                if (self.disconnect_callback) |callback| {
                    callback(conn, self.disconnect_context);
                }
                conn.deinit();
                self.options.allocator.destroy(conn);
            }

            const after_connections = Timestamp.now(self.io, .awake);
            self.last_connection_tick_ns = tick_start.durationTo(after_connections);

            if (self.tick_callback) |cb| cb(self.tick_context);

            const after_callback = Timestamp.now(self.io, .awake);
            self.last_total_tick_ns = tick_start.durationTo(after_callback);
            self.last_callback_tick_ns = after_connections.durationTo(after_callback);
        }
    }

    fn advanceTickDeadline(
        current_deadline: Timestamp,
        tick_start: Timestamp,
        tick_interval: Duration,
    ) Timestamp {
        // re-anchor after a long stall instead of looping every missed tick
        const missed = tick_start.nanoseconds - current_deadline.nanoseconds;
        if (missed > 64 * tick_interval.nanoseconds) {
            return tick_start.addDuration(tick_interval);
        }

        var next_deadline = current_deadline.addDuration(tick_interval);
        while (next_deadline.nanoseconds <= tick_start.nanoseconds) {
            next_deadline = next_deadline.addDuration(tick_interval);
        }
        return next_deadline;
    }

    fn waitUntil(io: std.Io, deadline: Timestamp) void {
        const coarse_sleep_guard_ns: u64 = if (is_windows) 2_000_000 else 500_000;
        const yield_guard_ns: u64 = if (is_windows) 200_000 else 100_000;

        while (true) {
            const now = Timestamp.now(io, .awake);
            const remaining = deadline.subDuration(Duration.fromNanoseconds(now.nanoseconds));
            if (remaining.toMilliseconds() <= 0) return;

            const remaining_ns: u64 = @intCast(remaining.nanoseconds);
            if (remaining_ns > coarse_sleep_guard_ns) {
                io.sleep(
                    Duration.fromNanoseconds(@intCast(remaining_ns - coarse_sleep_guard_ns)),
                    .awake,
                ) catch return;
                continue;
            }

            if (remaining_ns > yield_guard_ns) {
                std.Thread.yield() catch {};
            } else {
                std.atomic.spinLoopHint();
            }
        }
    }

    pub fn start(self: *Self) !void {
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.io, .awake) else null;

        Logger.INFO("Starting server on {s}:{d}", .{ self.options.address, self.options.port });
        self.resetRateLimiter();
        self.running.store(true, .release);
        self.tick_thread = try self.io.concurrent(tickLoop, .{self});

        var seed: u64 = undefined;
        self.io.random(std.mem.asBytes(&seed));
        var prng = std.Random.DefaultPrng.init(seed);

        self.options.advertisement.guid = prng.random().int(i64);
        self.socket.setCallback(packet_callback, self);

        try self.socket.listen();

        if (start_time) |s_time| {
            const elapsed = s_time.untilNow(self.io, .awake);
            Logger.DEBUG("PERF: server start took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    pub fn packet_callback(
        data: []const u8,
        from_addr: std.Io.net.IpAddress,
        context: ?*anyopaque,
        allocator: std.mem.Allocator,
    ) void {
        const self = @as(*Self, @ptrCast(@alignCast(context)));
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.io, .awake) else null;

        if (data.len == 0) return;

        var ID: u8 = data[0];
        if (ID & 0xF0 == 0x80) ID = 0x80;
        const key = addressToKey(from_addr);

        switch (ID) {
            Packets.UnconnectedPing => {
                if (data.len < 33 or !std.mem.eql(u8, data[9..25], &Proto.Magic.bytes)) return;
                if (!self.allowOfflinePacket(key, Timestamp.now(self.io, .awake))) return;

                const string = self.options.advertisement.toString(self.options.allocator);
                defer self.options.allocator.free(string);

                var pong_buf: [1024]u8 = undefined;
                var pong = Proto.UnconnectedPong.init(
                    Timestamp.now(self.io, .real).toMilliseconds(),
                    self.options.advertisement.guid,
                    string,
                );

                defer pong.deinit(allocator);
                const pong_data = Proto.UnconnectedPong.serializeInto(&pong, &pong_buf) catch |err| {
                    Logger.ERROR("Failed to serialize unconnected pong: {s}", .{@errorName(err)});
                    return;
                };

                self.send(pong_data, from_addr);
            },
            Packets.OpenConnectionRequest1 => {
                if (data.len < 18 or !std.mem.eql(u8, data[1..17], &Proto.Magic.bytes)) return;
                if (!self.allowOfflinePacket(key, Timestamp.now(self.io, .awake))) return;

                var reply = Proto.ConnectionReply1.init(
                    self.options.advertisement.guid,
                    false,
                    self.options.max_mtu,
                );

                defer reply.deinit();

                var reply_buf: [Proto.ConnectionReply1.MAX_SERIALIZED_SIZE]u8 = undefined;
                const reply_data = reply.serializeInto(&reply_buf) catch |err| {
                    Logger.ERROR("Failed to serialize connection reply 1: {s}", .{@errorName(err)});
                    return;
                };

                self.send(reply_data, from_addr);
            },
            Packets.OpenConnectionRequest2 => {
                if (!self.allowOfflinePacket(key, Timestamp.now(self.io, .awake))) return;

                var request = Proto.ConnectionRequest2.deserialize(data, self.options.allocator) catch |err| {
                    Logger.ERROR("Failed to deserialize connection request 2: {s}", .{@errorName(err)});
                    return;
                };

                defer request.deinit(allocator);

                const mtu = @min(request.mtu_size, self.options.max_mtu);
                const address = Proto.Address.init(4, "0.0.0.0", 0);

                var reply = Proto.ConnectionReply2.init(
                    self.options.advertisement.guid,
                    address,
                    mtu,
                    false,
                );

                defer reply.deinit(allocator);

                var reply_buf: [Proto.ConnectionReply2.MAX_SERIALIZED_SIZE]u8 = undefined;
                const reply_data = reply.serializeInto(&reply_buf) catch |err| {
                    Logger.ERROR("Failed to serialize connection reply 2: {s}", .{@errorName(err)});
                    return;
                };

                self.send(reply_data, from_addr);

                self.connections_mutex.lock(self.io) catch |err| {
                    Logger.WARN("mutex lock failed: {}", .{err});
                    return;
                };

                defer self.connections_mutex.unlock(self.io);

                if (self.connections.contains(key)) {
                    // retransmitted handshake: the reply above is enough
                } else {
                    const conn = self.options.allocator.create(Connection) catch |err| {
                        Logger.ERROR("Failed to allocate connection: {s}", .{@errorName(err)});
                        return;
                    };

                    conn.* = Connection.init(self, from_addr, mtu, request.guid);

                    self.connections.put(key, conn) catch |err| {
                        Logger.ERROR("Failed to add connection to list: {s}", .{@errorName(err)});
                        conn.deinit();
                        self.options.allocator.destroy(conn);
                        return;
                    };
                }
            },
            Packets.FrameSet => {
                var connect_event: ?*Connection = null;

                {
                    self.connections_mutex.lock(self.io) catch |err| {
                        Logger.WARN("mutex lock failed: {}", .{err});
                        return;
                    };
                    defer self.connections_mutex.unlock(self.io);

                    if (self.connections.get(key)) |conn| {
                        if (!conn.active) return;
                        conn.onFrameSet(data) catch |err| {
                            Logger.ERROR("Failed to handle frame set: {s}", .{@errorName(err)});
                            return;
                        };
                        // callback fires after unlock
                        if (conn.takePendingConnect()) connect_event = conn;
                    }
                }

                if (connect_event) |conn| {
                    if (self.connect_callback) |callback| {
                        callback(conn, self.connect_context);
                    }
                }
            },
            Packets.Ack => {
                self.connections_mutex.lock(self.io) catch |err| {
                    Logger.WARN("mutex lock failed: {}", .{err});
                    return;
                };

                defer self.connections_mutex.unlock(self.io);

                if (self.connections.get(key)) |conn| {
                    if (!conn.active) return;
                    conn.handleAck(data) catch |err| {
                        Logger.ERROR("Failed to handle ack: {s}", .{@errorName(err)});
                        return;
                    };
                }
            },
            Packets.Nack => {
                self.connections_mutex.lock(self.io) catch |err| {
                    Logger.WARN("mutex lock failed: {}", .{err});
                    return;
                };

                defer self.connections_mutex.unlock(self.io);

                if (self.connections.get(key)) |conn| {
                    if (!conn.active) return;
                    conn.handleNack(data) catch |err| {
                        Logger.ERROR("Failed to handle nack: {s}", .{@errorName(err)});
                        return;
                    };
                }
            },
            else => {
                self.connections_mutex.lock(self.io) catch |err| {
                    Logger.WARN("mutex lock failed: {}", .{err});
                    return;
                };

                defer self.connections_mutex.unlock(self.io);

                Logger.WARN("Unknown ID {d}", .{ID});
                if (self.connections.contains(key)) {
                    Logger.DEBUG("Connection already exists", .{});
                }
            },
        }

        if (start_time) |s_time| {
            const elapsed = s_time.untilNow(self.io, .awake);
            Logger.DEBUG("PERF: packet_callback took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    pub fn send(self: *Self, data: []const u8, to_addr: std.Io.net.IpAddress) void {
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.io, .awake) else null;

        self.socket.send(data, to_addr) catch |err| {
            Logger.ERROR("Failed to send: {s}", .{@errorName(err)});
            return;
        };

        if (start_time) |s_start| {
            const elapsed = s_start.untilNow(self.io, .awake);
            Logger.DEBUG("PERF: send took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    pub fn disconnect(self: *Self, address: std.Io.net.IpAddress) void {
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.io, .awake) else null;

        const key = addressToKey(address);
        Logger.INFO("Disconnecting connection with key: {d}", .{key});

        const dc = [_]u8{Packets.DisconnectNotification};
        self.send(&dc, address);

        self.connections_mutex.lock(self.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return;
        };

        defer self.connections_mutex.unlock(self.io);

        if (self.connections.get(key)) |conn| {
            conn.active = false;
            conn.connected = false;
        } else {
            Logger.WARN("Attempted to disconnect non-existent connection: {d}", .{key});
        }

        if (start_time) |s_time| {
            const elapsed = s_time.untilNow(self.io, .awake);
            Logger.DEBUG("PERF: disconnect took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    /// Set server connect callback
    pub fn setConnectCallback(self: *Self, callback: ?ConnectCallback, context: ?*anyopaque) void {
        self.connect_callback = callback;
        self.connect_context = context;
    }

    /// Set server disconnect callback
    pub fn setDisconnectCallback(self: *Self, callback: ?DisconnectCallback, context: ?*anyopaque) void {
        self.disconnect_callback = callback;
        self.disconnect_context = context;
    }

    pub fn setTickCallback(self: *Self, callback: ?TickCallback, context: ?*anyopaque) void {
        self.tick_callback = callback;
        self.tick_context = context;
    }

    /// Get a connection by address (thread-safe)
    pub fn getConnection(self: *Self, address: std.Io.net.IpAddress) ?*Connection {
        const key = addressToKey(address);

        self.connections_mutex.lock(self.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return null;
        };

        defer self.connections_mutex.unlock(self.io);

        return self.connections.get(key);
    }

    /// Get all active connections (returns a copy for thread safety)
    pub fn getActiveConnections(self: *Self, allocator: std.mem.Allocator) !std.ArrayList(*Connection) {
        var active_connections = std.ArrayList(*Connection).empty;

        self.connections_mutex.lock(self.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return err;
        };

        defer self.connections_mutex.unlock(self.io);

        var iterator = self.connections.valueIterator();
        while (iterator.next()) |entry| {
            if (entry.*.active) {
                try active_connections.append(allocator, entry.*);
            }
        }

        return active_connections;
    }

    pub fn deinit(self: *Self) void {
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.io, .awake) else null;

        // Future.cancel awaits: after this no other thread touches connections
        self.running.store(false, .release);
        if (self.tick_thread) |*thread| {
            _ = thread.cancel(self.io);
            self.tick_thread = null;
        }

        self.socket.stop();

        self.connections_mutex.lock(self.io) catch |err| {
            Logger.DEBUG("WARNING: mutex lock failed during deinit: {}", .{err});
            self.connections.deinit();
            self.socket.deinit();
            return;
        };

        var it = self.connections.valueIterator();
        while (it.next()) |entry| {
            entry.*.deinit();
            self.options.allocator.destroy(entry.*);
        }

        self.connections.deinit();
        self.connections_mutex.unlock(self.io);

        // Finish socket cleanup
        self.socket.deinit();

        if (start_time) |s_time| {
            const elapsed = s_time.untilNow(self.io, .awake);
            Logger.DEBUG("PERF: deinit took {d} ms", .{elapsed.toMilliseconds()});
        }
    }
};

pub const ServerOptions = struct {
    address: []const u8 = "0.0.0.0",
    port: u16 = 19132,
    max_mtu: u16 = 1400,
    allocator: std.mem.Allocator = std.heap.page_allocator,
    tick_rate: u16 = 20,
    advertisement: Proto.Advertisement = Proto.Advertisement.init(
        .MCPE,
        "Conduit Server",
        800,
        "1.21.80",
        0,
        10,
        0,
        "Conduit Server",
        "Survival",
    ),
    // Server callbacks
    connect_callback: ?ConnectCallback = null,
    connect_context: ?*anyopaque = null,
    disconnect_callback: ?DisconnectCallback = null,
    disconnect_context: ?*anyopaque = null,
    tick_callback: ?TickCallback = null,
    tick_context: ?*anyopaque = null,
};

test "addressToKey IPv4 produces unique keys" {
    const addr1: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 19132 } };
    const addr2: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 19133 } };
    const addr3: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 192, 168, 1, 1 }, .port = 19132 } };

    const key1 = Server.addressToKey(addr1);
    const key2 = Server.addressToKey(addr2);
    const key3 = Server.addressToKey(addr3);

    // Same IP different port -> different key
    try std.testing.expect(key1 != key2);
    // Different IP same port -> different key
    try std.testing.expect(key1 != key3);
    // Same address -> same key
    try std.testing.expectEqual(key1, Server.addressToKey(addr1));
}

test "addressToKey IPv6 produces unique keys" {
    const addr1: std.Io.net.IpAddress = .{ .ip6 = .{ .bytes = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }, .port = 19132 } };
    const addr2: std.Io.net.IpAddress = .{ .ip6 = .{ .bytes = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }, .port = 19133 } };
    const addr3: std.Io.net.IpAddress = .{ .ip6 = .{ .bytes = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2 }, .port = 19132 } };

    const key1 = Server.addressToKey(addr1);
    const key2 = Server.addressToKey(addr2);
    const key3 = Server.addressToKey(addr3);

    try std.testing.expect(key1 != key2);
    try std.testing.expect(key1 != key3);
    try std.testing.expectEqual(key1, Server.addressToKey(addr1));
}

test "Server init and deinit" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var server = try Server.init(io, .{
        .allocator = allocator,
        .port = 0, // Use ephemeral port for testing
    });
    defer server.deinit();

    try std.testing.expect(!server.running.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.connections.count());
}

test "advanceTickDeadline keeps fixed cadence after a late wake" {
    const tick_interval: Duration = .fromNanoseconds(50_000_000);
    const previous_deadline: Timestamp = .fromNanoseconds(1_000_000_000);
    const late_tick_start: Timestamp = previous_deadline.addDuration(.fromNanoseconds(12_000_000));

    try std.testing.expectEqual(
        previous_deadline.addDuration(tick_interval),
        Server.advanceTickDeadline(previous_deadline, late_tick_start, tick_interval),
    );
}

test "advanceTickDeadline skips missed intervals when badly behind" {
    const tick_interval: Duration = .fromNanoseconds(50_000_000);
    const previous_deadline: Timestamp = .fromNanoseconds(1_000_000_000);
    const late_tick_start: Timestamp = previous_deadline.addDuration(.fromNanoseconds(125_000_000));

    try std.testing.expectEqual(
        previous_deadline.addDuration(.fromNanoseconds(3 * 50_000_000)),
        Server.advanceTickDeadline(previous_deadline, late_tick_start, tick_interval),
    );
}

test "advanceTickDeadline re-anchors after a long stall" {
    const tick_interval: Duration = .fromNanoseconds(50_000_000);
    const previous_deadline: Timestamp = .fromNanoseconds(1_000_000_000);
    const stalled_tick_start: Timestamp = previous_deadline.addDuration(.fromNanoseconds(3_600 * std.time.ns_per_s));

    try std.testing.expectEqual(
        stalled_tick_start.addDuration(tick_interval),
        Server.advanceTickDeadline(previous_deadline, stalled_tick_start, tick_interval),
    );
}
