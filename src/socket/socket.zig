const std = @import("std");
const posix = std.posix;
const net = std.Io.net;
const Allocator = std.mem.Allocator;
const Thread = std.Thread;
const Mutex = std.Io.Mutex;
const Atomic = std.atomic.Value;
const builtin = @import("builtin");

const Logger = @import("../misc/Logger.zig").Logger;

pub const SocketError = error{
    WinsockInitFailed,
    SocketCreationFailed,
    BindFailed,
    SendFailed,
    AlreadyListening,
    NotListening,
    ThreadSpawnFailed,
    AddressParseError,
    SocketClosed,
    OutOfMemory,
} || net.IpAddress.ListenError || net.IpAddress.BindError || net.IpAddress.ConnectError || net.Socket.SendError || net.Socket.ReceiveError;

pub const CallbackFn = *const fn (
    data: []u8,
    from_addr: net.IpAddress,
    context: ?*anyopaque,
    allocator: Allocator,
) void;

// Configuration constants
const Config = struct {
    const BUFFER_SIZE = 8192;
    const MAX_PACKETS_PER_BATCH = 128;
    // Sleep times for different states
    const BASE_SLEEP_NS = 10_000; // 0.01ms - responsive
    const IDLE_SLEEP_NS = 500_000; // 0.5ms - light idle
    const MAX_IDLE_SLEEP_NS = 2_000_000; // 2ms - max idle
    const IDLE_THRESHOLD = 20;
    const DEEP_IDLE_THRESHOLD = 200;
    const MAX_CONSECUTIVE_ERRORS = 15;
    const SOCKET_RECV_TIMEOUT_MS = 10;
};

const PacketBuffer = struct {
    data: [Config.BUFFER_SIZE]u8,
    used: bool,
};

const BufferPool = struct {
    io: std.Io,
    buffers: []PacketBuffer,
    mutex: Mutex,
    allocator: Allocator,

    fn init(io: std.Io, allocator: Allocator, pool_size: usize) !BufferPool {
        const buffers = try allocator.alloc(PacketBuffer, pool_size);
        for (buffers) |*buffer| {
            buffer.used = false;
        }

        return BufferPool{
            .io = io,
            .buffers = buffers,
            .mutex = Mutex.init,
            .allocator = allocator,
        };
    }

    fn deinit(self: *BufferPool) void {
        self.allocator.free(self.buffers);
    }

    fn acquire(self: *BufferPool) ?*PacketBuffer {
        self.mutex.lock(self.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return null;
        };
        defer self.mutex.unlock(self.io);

        for (self.buffers) |*buffer| {
            if (!buffer.used) {
                buffer.used = true;
                return buffer;
            }
        }
        return null;
    }

    fn release(self: *BufferPool, buffer: *PacketBuffer) void {
        self.mutex.lock(self.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return;
        };
        defer self.mutex.unlock(self.io);

        buffer.used = false;
    }
};

pub const Socket = struct {
    const Self = @This();

    // Core socket data
    io: std.Io,
    allocator: Allocator,
    bind_address: net.IpAddress,
    _socket: net.Socket = undefined,

    // Threading
    thread: ?Thread,
    should_stop: Atomic(bool),
    is_listening: Atomic(bool),

    // Callback system
    callback: ?CallbackFn,
    context: ?*anyopaque,
    callback_mutex: Mutex,

    // Buffer management
    buffer_pool: BufferPool,

    // Error handling
    consecutive_errors: Atomic(u32),

    // Platform-specific
    winsock_initialized: if (builtin.os.tag == .windows) bool else void,

    pub fn init(io: std.Io, allocator: Allocator, host: []const u8, port: u16) SocketError!Self {
        const bind_address = parseAddress(host, port) catch |err| {
            std.log.err("Failed to parse address {s}:{d}: {any}", .{ host, port, err });
            return SocketError.AddressParseError;
        };

        var buffer_pool = BufferPool.init(io, allocator, 64) catch {
            return SocketError.OutOfMemory;
        };

        var self = Self{
            .io = io,
            .allocator = allocator,
            .bind_address = bind_address,
            .thread = null,
            .should_stop = Atomic(bool).init(false),
            .is_listening = Atomic(bool).init(false),
            .callback = null,
            .context = null,
            .callback_mutex = Mutex.init,
            .buffer_pool = buffer_pool,
            .consecutive_errors = Atomic(u32).init(0),
            .winsock_initialized = if (builtin.os.tag == .windows) false else {},
        };

        self.createSocket() catch |err| {
            buffer_pool.deinit();
            return err;
        };

        return self;
    }

    fn parseAddress(host: []const u8, port: u16) !net.IpAddress {
        if (std.mem.eql(u8, host, "0.0.0.0") or host.len == 0) {
            return net.IpAddress.parseIp4("0.0.0.0", port);
        }
        return net.IpAddress.parseIp4(host, port);
    }

    fn createSocket(self: *Self) SocketError!void {
        self._socket = try self.bind_address.bind(self.io, .{
            .allow_broadcast = true,
            .mode = .dgram,
            .protocol = .udp,
        });

        if (builtin.os.tag == .windows) {
            const ws2 = std.os.windows.ws2_32;
            const sol_socket = ws2.SOL.SOCKET;
            const so_reuseaddr = ws2.SO.REUSEADDR;
            const so_rcvbuf = ws2.SO.RCVBUF;
            const so_sndbuf = ws2.SO.SNDBUF;

            const setsockopt = struct {
                pub extern fn setsockopt(s: usize, level: i32, optname: u32, optval: ?*const anyopaque, optlen: u32) c_int;
            }.setsockopt;

            const enable: c_int = 1;
            _ = setsockopt(@intFromPtr(self._socket.handle), sol_socket, so_reuseaddr, &enable, @sizeOf(c_int));

            const recv_buf_size: c_int = 4 * 1024 * 1024;
            _ = setsockopt(@intFromPtr(self._socket.handle), sol_socket, so_rcvbuf, &recv_buf_size, @sizeOf(c_int));

            const send_buf_size: c_int = 4 * 1024 * 1024;
            _ = setsockopt(@intFromPtr(self._socket.handle), sol_socket, so_sndbuf, &send_buf_size, @sizeOf(c_int));
        } else {
            const enable: c_int = 1;
            _ = posix.setsockopt(self._socket.handle, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&enable)) catch {};

            const recv_buf_size: c_int = 4 * 1024 * 1024;
            _ = posix.setsockopt(self._socket.handle, posix.SOL.SOCKET, posix.SO.RCVBUF, std.mem.asBytes(&recv_buf_size)) catch {};

            const send_buf_size: c_int = 4 * 1024 * 1024;
            _ = posix.setsockopt(self._socket.handle, posix.SOL.SOCKET, posix.SO.SNDBUF, std.mem.asBytes(&send_buf_size)) catch {};
        }
    }

    pub fn listen(self: *Self) SocketError!void {
        if (self.is_listening.load(.acquire)) {
            return SocketError.AlreadyListening;
        }

        self.should_stop.store(false, .release);
        self.consecutive_errors.store(0, .release);

        self.thread = Thread.spawn(.{}, receiveLoop, .{self}) catch |err| {
            std.log.err("Failed to spawn receive thread: {any}", .{err});
            return SocketError.ThreadSpawnFailed;
        };

        self.is_listening.store(true, .release);
        // std.log.info("Socket listening on {any}", .{self.bind_address});
    }

    // Improved receive loop with CPU-efficient adaptive sleeping
    fn receiveLoop(self: *Self) void {
        var packets_processed: u32 = 0;
        var consecutive_no_data: u32 = 0;
        var current_sleep_ns: u64 = Config.BASE_SLEEP_NS;

        while (!self.should_stop.load(.acquire)) {
            packets_processed = 0;
            var buffer_shortage = false;

            // Process multiple packets in a batch
            while (packets_processed < Config.MAX_PACKETS_PER_BATCH) {
                const buffer = self.buffer_pool.acquire() orelse {
                    buffer_shortage = true;
                    break;
                };

                defer self.buffer_pool.release(buffer);

                const result = self.receivePacket(buffer.data[0..]);

                switch (result) {
                    .success => |packet_info| {
                        self.consecutive_errors.store(0, .release);
                        consecutive_no_data = 0;
                        // Reset sleep time when data arrives - be responsive
                        current_sleep_ns = Config.BASE_SLEEP_NS;

                        if (packet_info) |info| {
                            self.handlePacket(info.data, info.from_addr);
                            packets_processed += 1;
                        }
                    },
                    .would_block => {
                        consecutive_no_data += 1;
                        // Gradually increase sleep time as we detect inactivity
                        if (consecutive_no_data > Config.DEEP_IDLE_THRESHOLD) {
                            // Cap the maximum sleep time to avoid becoming too unresponsive
                            current_sleep_ns = @min(Config.MAX_IDLE_SLEEP_NS, current_sleep_ns + (current_sleep_ns / 8) // Increase by 12.5% (was 25%)
                            );
                        } else if (consecutive_no_data > Config.IDLE_THRESHOLD) {
                            // Medium idle - slowly increase sleep time
                            current_sleep_ns = @min(Config.IDLE_SLEEP_NS, current_sleep_ns + 5_000 // Increase by 0.005ms (was 0.01ms)
                            );
                        }
                        break; // No more data available
                    },
                    .error_recoverable => |err| {
                        self.handleRecoverableError(err);
                        break;
                    },
                    .error_fatal => |err| {
                        std.log.err("Fatal socket error: {any}", .{err});
                        return;
                    },
                }
            }

            // Adaptive sleep strategy based on system activity
            if (buffer_shortage) {
                // Sleep briefly to let buffers free up
                self.io.sleep(std.Io.Duration.fromNanoseconds(@intCast(current_sleep_ns / 2)), .awake) catch |err| {
                    Logger.WARN("sleep interrupted: {}", .{err});
                    return;
                };
            } else if (packets_processed == 0) {
                // No packets processed, use adaptive sleep
                self.io.sleep(.fromNanoseconds(@intCast(current_sleep_ns)), .awake) catch |err| {
                    Logger.WARN("sleep interrupted: {}", .{err});
                    return;
                };
            }
            // When packets were processed, loop immediately without sleeping
        }
    }

    // More efficient packet handling with better memory management
    fn handlePacket(self: *Self, data: []const u8, from_addr: net.IpAddress) void {
        self.callback_mutex.lock(self.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return;
        };
        const callback = self.callback;
        const context = self.context;
        self.callback_mutex.unlock(self.io);

        if (callback) |cb| {
            // Create a copy of the data that will be freed by the callback
            const data_copy = self.allocator.dupe(u8, data) catch |err| {
                std.log.err("Failed to copy packet data: {any}", .{err});
                return;
            };

            // Call the callback - the callback MUST free the data_copy when done
            cb(data_copy, from_addr, context, self.allocator);
        }
    }

    fn handleRecoverableError(self: *Self, err: anyerror) void {
        const error_count = self.consecutive_errors.fetchAdd(1, .acq_rel) + 1;

        // Only log errors for the first few occurrences to avoid spamming
        if (error_count <= 3) {
            std.log.warn("Socket error ({any}): {any}", .{ error_count, err });
        } else if (error_count == 5 or error_count % 100 == 0) {
            // Only log periodically after many errors
            std.log.err("Network errors continuing (count: {d}), client likely disconnected", .{error_count});
            self.io.sleep(.fromNanoseconds(@intCast(Config.IDLE_SLEEP_NS)), .awake) catch return; // Longer backoff
        } else {
            // Just back off without logging
            self.io.sleep(.fromNanoseconds(@intCast(Config.BASE_SLEEP_NS * 5)), .awake) catch return;
        }
    }

    const ReceiveResult = union(enum) {
        success: ?PacketInfo,
        would_block: void,
        error_recoverable: anyerror,
        error_fatal: anyerror,
    };

    const PacketInfo = struct {
        data: []const u8,
        from_addr: net.IpAddress,
    };

    fn receivePacket(self: *Self, buffer: []u8) ReceiveResult {
        const timeout: std.Io.Timeout = .{
            .duration = .{
                .raw = std.Io.Duration.fromNanoseconds(Config.SOCKET_RECV_TIMEOUT_MS * std.time.ns_per_ms),
                .clock = .awake,
            },
        };

        const msg = self._socket.receiveTimeout(self.io, buffer, timeout) catch |err| switch (err) {
            error.Timeout => return .{ .would_block = {} },
            else => return .{ .error_fatal = err },
        };

        return .{ .success = .{ .data = buffer[0..msg.data.len], .from_addr = msg.from } };
    }

    pub fn stop(self: *Self) void {
        if (!self.is_listening.load(.acquire)) return;

        self.should_stop.store(true, .release);

        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }

        self.is_listening.store(false, .release);
        std.log.info("Socket stopped listening", .{});
    }

    pub fn deinit(self: *Self) void {
        self.stop();
        self._socket.close(self.io);
        self.buffer_pool.deinit();
    }

    pub fn setCallback(self: *Self, callback: CallbackFn, context: ?*anyopaque) void {
        self.callback_mutex.lock(self.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return;
        };
        defer self.callback_mutex.unlock(self.io);
        self.callback = callback;
        self.context = context;
    }

    pub fn send(self: *Self, data: []const u8, to_addr: net.IpAddress) SocketError!void {
        try self._socket.send(self.io, &to_addr, data);
    }

    pub fn sendTo(self: *Self, data: []const u8, host: []const u8, port: u16) SocketError!void {
        const addr = parseAddress(host, port) catch |err| {
            std.log.err("Failed to parse destination address {s}:{d}: {any}", .{ host, port, err });
            return SocketError.AddressParseError;
        };
        try self.send(data, addr);
    }

    pub fn getLocalAddress(self: *Self) net.IpAddress {
        return self._socket.address;
    }

    pub fn isListening(self: *Self) bool {
        return self.is_listening.load(.acquire);
    }
};
