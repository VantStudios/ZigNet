const std = @import("std");
const posix = std.posix;
const net = std.Io.net;
const Allocator = std.mem.Allocator;
const Thread = std.Thread;
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
    const SOCKET_RECV_TIMEOUT_MS = 10;
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
    should_stop: std.atomic.Value(bool),
    is_listening: std.atomic.Value(bool),

    // Callback system
    callback: ?CallbackFn,
    context: ?*anyopaque,
    callback_mutex: std.Io.Mutex,

    // Error handling
    consecutive_errors: std.atomic.Value(u32),

    // Platform-specific
    winsock_initialized: if (builtin.os.tag == .windows) bool else void,

    pub fn init(io: std.Io, allocator: Allocator, host: []const u8, port: u16) SocketError!Self {
        const bind_address = parseAddress(host, port) catch |err| {
            std.log.err("Failed to parse address {s}:{d}: {any}", .{ host, port, err });
            return SocketError.AddressParseError;
        };

        var self = Self{
            .io = io,
            .allocator = allocator,
            .bind_address = bind_address,
            .thread = null,
            .should_stop = std.atomic.Value(bool).init(false),
            .is_listening = std.atomic.Value(bool).init(false),
            .callback = null,
            .context = null,
            .callback_mutex = std.Io.Mutex.init,
            .consecutive_errors = std.atomic.Value(u32).init(0),
            .winsock_initialized = if (builtin.os.tag == .windows) false else {},
        };

        self.createSocket() catch |err| {
            return err;
        };

        return self;
    }

    fn parseAddress(host: []const u8, port: u16) !net.IpAddress {
        if (std.mem.indexOfScalar(u8, host, ':') != null) {
            return net.IpAddress.parseIp6(host, port);
        }
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
            const so_rcvbuf = ws2.SO.RCVBUF;
            const so_sndbuf = ws2.SO.SNDBUF;

            const setsockopt = struct {
                pub extern fn setsockopt(s: usize, level: i32, optname: u32, optval: ?*const anyopaque, optlen: u32) c_int;
            }.setsockopt;

            const recv_buf_size: c_int = 4 * 1024 * 1024;
            _ = setsockopt(@intFromPtr(self._socket.handle), sol_socket, so_rcvbuf, &recv_buf_size, @sizeOf(c_int));

            const send_buf_size: c_int = 4 * 1024 * 1024;
            _ = setsockopt(@intFromPtr(self._socket.handle), sol_socket, so_sndbuf, &send_buf_size, @sizeOf(c_int));
        } else {
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
    }

    // recv with a timeout already yields the CPU; extra sleeps add latency
    fn receiveLoop(self: *Self) void {
        var recv_buffer: [Config.BUFFER_SIZE]u8 = undefined;
        var packets_processed: u32 = 0;

        while (!self.should_stop.load(.acquire)) {
            packets_processed = 0;

            // Process multiple packets in a batch
            while (packets_processed < Config.MAX_PACKETS_PER_BATCH) {
                const result = self.receivePacket(&recv_buffer);

                switch (result) {
                    .success => |packet_info| {
                        self.consecutive_errors.store(0, .release);
                        if (packet_info) |info| {
                            self.handlePacket(info.data, info.from_addr);
                            packets_processed += 1;
                        }
                    },
                    .would_block => {
                        break; // No more data available right now
                    },
                    .error_fatal => |err| {
                        std.log.err("Fatal socket error: {any}", .{err});
                        self.is_listening.store(false, .release);
                        return;
                    },
                }
            }
        }
    }

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

    const ReceiveResult = union(enum) {
        success: ?PacketInfo,
        would_block: void,
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
            error.Canceled => return .{ .error_fatal = err },
            // transient (EINTR, ENOBUFS, ...): back off and keep receiving
            else => {
                const error_count = self.consecutive_errors.fetchAdd(1, .acq_rel) + 1;
                if (error_count <= 3 or error_count % 100 == 0) {
                    std.log.warn("Socket receive error (count: {d}): {any}", .{ error_count, err });
                }
                self.io.sleep(.fromNanoseconds(10_000), .awake) catch return .{ .error_fatal = err };
                return .{ .would_block = {} };
            },
        };

        if (msg.data.len == 0) return .{ .success = null };

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
