const std = @import("std");
const Io = std.Io;
const Timestamp = Io.Timestamp;
const Duration = Io.Duration;

const Logger = @import("../misc/Logger.zig").Logger;
const Address = @import("../proto/Address.zig").Address;
const OpenConnectionReply1 = @import("../proto/offline/ConnectionReply1.zig").ConnectionReply1;
const ConnectionReply2 = @import("../proto/offline/ConnectionReply2.zig").ConnectionReply2;
const OpenConnectionRequest1 = @import("../proto/offline/ConnectionRequest1.zig").ConnectionRequest1;
const OpenConnectionRequest2 = @import("../proto/offline/ConnectionRequest2.zig").ConnectionRequest2;
const ConnectedPing = @import("../proto/online/ConnectedPing.zig").ConnectedPing;
const ConnectedPong = @import("../proto/online/ConnectedPong.zig").ConnectedPong;
const ConnectionRequest = @import("../proto/online/ConnectionRequest.zig").ConnectionRequest;
const ConnectionRequestAccepted = @import("../proto/online/ConnectionRequestAccepted.zig").ConnectionRequestAccepted;
const NewIncomingConnection = @import("../proto/online/NewIncomingConnection.zig").NewIncomingConnection;
const Packets = @import("../proto/Packets.zig").Packets;
const Proto = @import("../proto/root.zig");
const Frame = Proto.Frame;
const Reliability = Proto.Reliability;
const Socket = @import("../socket/socket.zig").Socket;

const MAX_CHANNELS = 32;
const MAX_ORDERING_QUEUE_SIZE = 64;
const MAX_SPLIT_SIZE: u32 = 1024;
const MAX_FRAGMENT_SETS = 256;
const MAX_LOST_GAP: u32 = 4096;
const DATAGRAM_SCRATCH_SIZE = 1600;

pub const GamePacketCallback = *const fn (connection: *Client, payload: []const u8, context: ?*anyopaque) void;
pub const ConnectionCallback = *const fn (connection: *Client, context: ?*anyopaque) void;
pub const DisconnectionCallback = *const fn (connection: *Client, context: ?*anyopaque) void;

pub const Client = struct {
    pub const Self = @This();
    tick_thread: ?std.Thread,
    options: ClientOptions,
    socket: Socket,
    status: Status = .Disconnected,
    comm_data: CommData,
    last_receive: Timestamp,

    game_callback: ?GamePacketCallback = null,
    connection_callback: ?ConnectionCallback = null,

    game_callback_ctx: ?*anyopaque = null,
    connection_callback_ctx: ?*anyopaque = null,

    disconnection_callback: ?DisconnectionCallback = null,
    disconnection_callback_ctx: ?*anyopaque = null,

    connect_called: bool = false,

    // sendFrame runs on the recv, tick, and user threads
    send_mutex: Io.Mutex = .init,

    // Security fields from OpenConnectionReply1
    server_has_security: bool = false,
    security_cookie: ?u32 = null,
    server_public_key: ?[294]u8 = null,

    // Flags to prevent duplicate offline packet processing
    received_reply1: bool = false,
    received_reply2: bool = false,

    send_scratch: [DATAGRAM_SCRATCH_SIZE]u8 = undefined,

    pub fn init(options: ClientOptions) !Client {
        var self = Client{
            .options = options,
            .socket = try Socket.init(options.io, options.allocator, "0.0.0.0", 0),
            .tick_thread = null,
            .comm_data = .{
                .received_sequences = initMapCapacity(u24, void, options.allocator, 16),
                .lost_sequences = initMapCapacity(u24, void, options.allocator, 16),
                .input_order_index = [_]u32{0} ** MAX_CHANNELS,
                .input_highest_sequence_index = [_]u32{0} ** MAX_CHANNELS,
                .input_ordering_channels = [_]?ChannelQueue{null} ** MAX_CHANNELS,
                .output_reliable_index = 0,
                .output_sequence = 0,
                .output_frame_queue = try std.ArrayList(Frame).initCapacity(options.allocator, 0),
                .output_backup = initMapCapacity(u24, []u8, options.allocator, 64),
                .output_order_index = [_]u32{0} ** MAX_CHANNELS,
                .output_sequence_index = [_]u32{0} ** MAX_CHANNELS,
                .output_split_index = 0,
                .fragments_queue = initMapCapacity(u16, FragmentSet, options.allocator, 8),
                .fragments_activity = initMapCapacity(u16, Timestamp, options.allocator, 8),
            },
            .last_receive = Timestamp.now(options.io, .awake),
        };

        var seed: u64 = undefined;
        options.io.random(std.mem.asBytes(&seed));
        var prng = std.Random.DefaultPrng.init(seed);
        self.options.guid = prng.random().int(i64);
        return self;
    }

    pub fn connect(self: *Client) !void {
        // callback must be live before the recv thread starts
        self.socket.setCallback(Client._on, self);
        try self.socket.listen();

        self.status = .Connecting;
        self.connect_called = true;
        self.last_receive = Timestamp.now(self.options.io, .awake);

        self.tick_thread = try std.Thread.spawn(.{}, tickLoop, .{self});
        var request = OpenConnectionRequest1.init(11, self.options.mtu_size);
        defer request.deinit();

        const datagram_size = self.options.mtu_size - 28;
        var request_buf: [1464]u8 = undefined;
        if (datagram_size > request_buf.len) return error.MtuTooLarge;
        const payload = try request.serializeInto(request_buf[0..datagram_size]);
        try self.send(payload);
    }

    pub fn setGamePacketCallback(self: *Client, callback: ?GamePacketCallback, context: ?*anyopaque) void {
        self.game_callback = callback;
        self.game_callback_ctx = context;
    }

    pub fn setConnectionCallback(self: *Client, callback: ?ConnectionCallback, context: ?*anyopaque) void {
        self.connection_callback = callback;
        self.connection_callback_ctx = context;
    }

    pub fn setDisconnectionCallback(self: *Client, callback: ?DisconnectionCallback, context: ?*anyopaque) void {
        self.disconnection_callback = callback;
        self.disconnection_callback_ctx = context;
    }

    fn tickLoop(self: *Self) void {
        const tick_rate = if (self.options.tick_rate > 0) self.options.tick_rate else 20;
        const ns_per_tick = std.time.ns_per_s / tick_rate;

        while (self.status != .Disconnected) {
            const start_time = Timestamp.now(self.options.io, .awake);

            self.tick();

            const elapsed = start_time.untilNow(self.options.io, .awake).toNanoseconds();

            if (elapsed < ns_per_tick) {
                const sleep_time = ns_per_tick - elapsed;

                self.options.io.sleep(Duration.fromNanoseconds(sleep_time), .awake) catch |err| {
                    std.debug.print("Error crítico en el bucle de sleep: {}\n", .{err});
                    break;
                };
            }
        }
    }

    pub fn onReceive(
        self: *Client,
        payload: []u8,
        from_addr: Io.net.IpAddress,
        allocator: std.mem.Allocator,
    ) !void {
        _ = from_addr;
        defer allocator.free(payload);
        if (payload.len == 0) return;
        var ID: u8 = payload[0];
        if (ID & 0xF0 == 0x80) ID = 0x80;

        self.last_receive = Timestamp.now(self.options.io, .awake);
        switch (ID) {
            Packets.OpenConnectionReply1 => {
                // Only process if we haven't received Reply1 yet
                if (self.received_reply1) return;
                self.received_reply1 = true;

                var packet = try OpenConnectionReply1.deserialize(payload);
                defer packet.deinit();

                // Store security info from the server
                self.server_has_security = packet.hasSecurity;
                self.security_cookie = packet.cookie;
                self.server_public_key = packet.server_public_key;

                const address = Address.init(4, self.options.address, self.options.port);

                // Use the server's mtu_size (from Reply1), not the client's requested size
                const mtu_to_use = packet.mtu_size;

                // If server has security with cookie, send it back with client_supports_security=false
                var r2 = if (packet.hasSecurity and packet.cookie != null)
                    OpenConnectionRequest2.initWithSecurity(address, mtu_to_use, self.options.guid, packet.cookie.?, false)
                else
                    OpenConnectionRequest2.init(address, mtu_to_use, self.options.guid);
                defer r2.deinit(allocator);

                var r2_buf: [OpenConnectionRequest2.MAX_SERIALIZED_SIZE]u8 = undefined;
                const ser = try r2.serializeInto(&r2_buf);
                try self.send(ser);
            },
            Packets.OpenConnectionReply2 => {
                // Only process if we haven't received Reply2 yet
                if (self.received_reply2) return;
                self.received_reply2 = true;

                var reply = try ConnectionReply2.deserialize(payload, allocator);
                defer reply.deinit(allocator);

                var request = ConnectionRequest.init(
                    self.options.guid,
                    Timestamp.now(self.options.io, .real).toMilliseconds(),
                    false,
                );
                defer request.deinit();

                var request_buf: [ConnectionRequest.MAX_SERIALIZED_SIZE]u8 = undefined;
                const serialized = try request.serializeInto(&request_buf);

                var frame = try Client.frameIn(serialized, allocator);
                self.sendFrame(&frame, .Immediate);
                frame.deinit();
            },
            Packets.FrameSet => {
                try self.onFrameSet(payload);
            },
            Packets.Ack => {
                try self.handleAck(payload);
            },
            Packets.Nack => {
                try self.handleNack(payload);
            },
            else => {},
        }
    }

    pub fn send(self: *Client, payload: []const u8) !void {
        try self.socket.sendTo(payload, self.options.address, self.options.port);
    }

    /// Disconnect from the server by sending DisconnectNotification
    pub fn disconnect(self: *Client) void {
        if (self.status == .Disconnected) return;

        // Send DisconnectNotification packet (0x15)
        var disconnect_payload = [_]u8{Proto.Packets.DisconnectNotification};
        var frame = Client.frameIn(&disconnect_payload, self.options.allocator) catch return;
        frame.reliability = Reliability.ReliableOrdered;
        self.sendFrame(&frame, .Immediate);
        frame.deinit();

        self.status = .Disconnected;
    }

    pub fn deinit(self: *Client) void {
        // Ensure thread stops before cleanup
        self.status = .Disconnected;
        if (self.tick_thread) |thread| {
            thread.join();
        }

        self.comm_data.deinit(self.options.allocator);
        self.socket.deinit();
    }

    pub fn _on(
        payload: []u8,
        from_addr: Io.net.IpAddress,
        context: ?*anyopaque,
        allocator: std.mem.Allocator,
    ) void {
        const self = @as(*Self, @ptrCast(@alignCast(context)));
        self.onReceive(payload, from_addr, allocator) catch |err| {
            std.debug.print("Error in client onReceive: {any}\n", .{err});
        };
    }

    pub fn onFrameSet(self: *Self, buffer: []const u8) !void {
        if (self.status == .Disconnected) return;

        self.last_receive = Timestamp.now(self.options.io, .awake);

        var frameSet = try Proto.FrameSet.deserialize(buffer, self.options.allocator);
        defer frameSet.deinit(self.options.allocator);

        const sequence = frameSet.sequence_number;

        {
            self.comm_data.input_mutex.lock(self.options.io) catch |err| {
                std.debug.print("failed to acquire lock: {}\n", .{err});
                return;
            };
            defer self.comm_data.input_mutex.unlock(self.options.io);

            const is_duplicate = (self.comm_data.last_input_sequence != -1 and sequence <= @as(u24, @intCast(@max(0, self.comm_data.last_input_sequence)))) or self.comm_data.received_sequences.contains(sequence);
            if (is_duplicate) {
                return;
            }

            if (self.comm_data.received_sequences.count() >= MAX_PENDING_SEQUENCES) {
                self.comm_data.received_sequences.clearRetainingCapacity();
            }
            self.comm_data.received_sequences.put(sequence, {}) catch {
                return;
            };
            _ = self.comm_data.lost_sequences.remove(sequence);

            const last = self.comm_data.last_input_sequence;
            if (last >= 0 and sequence > last) {
                const last_u32: u32 = @intCast(last);
                const gap: u32 = @as(u32, sequence) - last_u32 - 1;
                // big jumps are client restarts, not real loss
                if (gap > 0 and gap <= MAX_LOST_GAP) {
                    var i: u32 = last_u32 + 1;
                    while (i < sequence) : (i += 1) {
                        if (self.comm_data.lost_sequences.count() >= MAX_PENDING_SEQUENCES) break;
                        self.comm_data.lost_sequences.put(@truncate(i), {}) catch break;
                    }
                }
            }
            self.comm_data.last_input_sequence = @as(i32, @intCast(sequence));
        }

        for (frameSet.frames) |frame| {
            try self.handleFrame(frame);
        }
    }

    pub fn handleFrame(self: *Client, frame: Frame) !void {
        if (self.status == .Disconnected) return;

        if (frame.payload.len == 0) {
            Logger.WARN("Frame has empty payload - skipping in handleFrame", .{});
            return;
        }

        if (frame.isSplit()) {
            try self.handleSplitFrame(frame);
        } else if (frame.isSequenced()) {
            self.handleSequencedFrame(frame);
        } else if (frame.isOrdered()) {
            self.handleOrderedFrame(frame);
        } else {
            self.handlePacket(frame.payload) catch |err| {
                Logger.ERROR("Error handling packet: {any}", .{err});
            };
        }
    }

    pub fn handleOrderedFrame(self: *Client, frame: Frame) void {
        if (self.status == .Disconnected) return;

        const channel = frame.order_channel orelse {
            Logger.ERROR("Ordered frame missing order_channel", .{});
            return;
        };
        if (channel >= MAX_CHANNELS) {
            Logger.WARN("Ordered frame with invalid channel {d} dropped", .{channel});
            return;
        }
        const frame_index = frame.ordered_frame_index orelse {
            Logger.ERROR("Ordered frame missing ordered_frame_index", .{});
            return;
        };

        if (frame_index == self.comm_data.input_order_index[channel]) {
            self.comm_data.input_highest_sequence_index[channel] = 0;
            self.comm_data.input_order_index[channel] = frame_index + 1;
            self.handlePacket(frame.payload) catch |err| {
                Logger.ERROR("Error handling ordered packet: {any}", .{err});
            };
            var index = self.comm_data.input_order_index[channel];
            if (self.orderingChannel(channel)) |queue| {
                while (queue.contains(index)) {
                    var ordered_frame = queue.get(index).?;
                    _ = queue.remove(index);
                    self.handlePacket(ordered_frame.payload) catch |err| {
                        Logger.ERROR("Error handling ordered queued packet: {any}", .{err});
                        ordered_frame.deinit();
                        return;
                    };
                    ordered_frame.deinit();
                    index += 1;
                }
                self.comm_data.input_order_index[channel] = index;
            }
        } else if (frame_index > self.comm_data.input_order_index[channel]) {
            if (self.orderingChannel(channel)) |queue| {
                if (queue.count() >= MAX_ORDERING_QUEUE_SIZE) {
                    Logger.WARN("Ordering queue full on channel {d}, dropping frame", .{channel});
                    return;
                }
                const payload_copy = self.options.allocator.dupe(u8, frame.payload) catch return;
                var frame_copy = frame;
                frame_copy.payload = payload_copy;
                frame_copy.allocator = self.options.allocator;
                queue.put(frame_index, frame_copy) catch {
                    Logger.ERROR("Failed to put out-of-order frame into order queue", .{});
                    frame_copy.deinit();
                    return;
                };
            }
        } else {
            self.handlePacket(frame.payload) catch |err| {
                Logger.ERROR("Error handling out-of-order packet: {any}", .{err});
            };
        }
    }

    pub fn handleSequencedFrame(self: *Self, frame: Frame) void {
        const channel = frame.order_channel orelse 0;
        if (channel >= MAX_CHANNELS) {
            Logger.WARN("Sequenced frame with invalid channel {d} dropped", .{channel});
            return;
        }
        const frame_index = frame.sequence_frame_index orelse {
            Logger.ERROR("Sequenced frame missing sequence_frame_index", .{});
            return;
        };
        const order_index = frame.ordered_frame_index orelse {
            Logger.ERROR("Sequenced frame missing ordered_frame_index", .{});
            return;
        };
        const current_highest = self.comm_data.input_highest_sequence_index[channel];
        if (frame_index >= current_highest and order_index >= self.comm_data.input_order_index[channel]) {
            self.comm_data.input_highest_sequence_index[channel] = frame_index + 1;
            self.handlePacket(frame.payload) catch |err| {
                Logger.ERROR("Error handling sequenced packet: {any}", .{err});
            };
        }
    }

    pub fn handleSplitFrame(self: *Self, frame: Frame) !void {
        const split_id = frame.split_id orelse {
            Logger.ERROR("Split frame missing split_id", .{});
            return;
        };

        const split_index = frame.split_frame_index orelse {
            Logger.ERROR("Split frame missing split_frame_index", .{});
            return;
        };

        const split_size = frame.split_size orelse {
            Logger.ERROR("Split frame missing split_size", .{});
            return;
        };

        if (split_size == 0 or split_size > MAX_SPLIT_SIZE) {
            Logger.WARN("Split frame with invalid split_size {d} dropped", .{split_size});
            return;
        }
        if (split_index >= split_size) {
            Logger.WARN("Split frame with invalid split_index {d} dropped", .{split_index});
            return;
        }

        const allocator = self.options.allocator;
        const index_u16: u16 = @intCast(split_index);

        if (self.comm_data.fragments_queue.getPtr(split_id)) |fragment| {
            if (fragment.contains(index_u16)) {
                // Duplicate fragment, ignore
                return;
            }
            const payload_copy = allocator.dupe(u8, frame.payload) catch {
                Logger.ERROR("Failed to duplicate payload for split frame", .{});
                return;
            };
            var frame_copy = Frame.init(
                frame.reliable_frame_index,
                frame.sequence_frame_index,
                frame.ordered_frame_index,
                frame.order_channel,
                frame.reliability,
                payload_copy,
                frame.split_frame_index,
                frame.split_id,
                frame.split_size,
                allocator,
            );

            fragment.put(index_u16, frame_copy) catch {
                Logger.ERROR("Failed to put split frame", .{});
                frame_copy.deinit();
                return;
            };
            self.comm_data.fragments_activity.put(split_id, Timestamp.now(self.options.io, .awake)) catch {};

            // Check if we have all the fragments
            if (fragment.count() == split_size) {
                var total_length: usize = 0;
                var complete = true;
                var i: u16 = 0;
                while (i < split_size) : (i += 1) {
                    const f = fragment.get(i) orelse {
                        complete = false;
                        break;
                    };
                    total_length += f.payload.len;
                }

                if (!complete) {
                    Logger.WARN("Fragment set {d} incomplete despite count match", .{split_id});
                    return;
                }

                const combined_payload = allocator.alloc(u8, total_length) catch {
                    Logger.ERROR("Failed to allocate combined payload", .{});
                    return;
                };

                var offset: usize = 0;
                i = 0;
                while (i < split_size) : (i += 1) {
                    const f = fragment.get(i).?;
                    @memcpy(combined_payload[offset .. offset + f.payload.len], f.payload);
                    offset += f.payload.len;
                }

                var combined_frame = Frame.init(
                    frame.reliable_frame_index,
                    frame.sequence_frame_index,
                    frame.ordered_frame_index,
                    frame.order_channel,
                    frame.reliability,
                    combined_payload,
                    null,
                    null,
                    null,
                    allocator,
                );

                // fragments are consumed; nframe owns its payload now
                self.removeFragmentSet(split_id);

                if (combined_frame.isSequenced()) {
                    self.handleSequencedFrame(combined_frame);
                } else if (combined_frame.isOrdered()) {
                    self.handleOrderedFrame(combined_frame);
                } else {
                    self.handlePacket(combined_frame.payload) catch |err| {
                        Logger.ERROR("Error handling combined packet: {any}", .{err});
                    };
                }
                combined_frame.deinit();
            }
        } else {
            if (self.comm_data.fragments_queue.count() >= MAX_FRAGMENT_SETS) {
                Logger.WARN("Too many incomplete fragment sets, dropping split_id {d}", .{split_id});
                return;
            }

            var new_fragment = FragmentSet.init(allocator);
            errdefer new_fragment.deinit();

            const payload_copy = allocator.dupe(u8, frame.payload) catch {
                Logger.ERROR("Failed to duplicate payload for new split frame", .{});
                return;
            };
            var frame_copy = Frame.init(
                frame.reliable_frame_index,
                frame.sequence_frame_index,
                frame.ordered_frame_index,
                frame.order_channel,
                frame.reliability,
                payload_copy,
                frame.split_frame_index,
                frame.split_id,
                frame.split_size,
                allocator,
            );

            new_fragment.put(index_u16, frame_copy) catch {
                Logger.ERROR("Failed to put new split frame", .{});
                frame_copy.deinit();
                return;
            };
            self.comm_data.fragments_queue.put(split_id, new_fragment) catch {
                Logger.ERROR("Failed to put new fragment queue", .{});
                frame_copy.deinit();
                new_fragment.deinit();
                return;
            };
            self.comm_data.fragments_activity.put(split_id, Timestamp.now(self.options.io, .awake)) catch {};
        }
    }

    fn removeFragmentSet(self: *Self, split_id: u16) void {
        if (self.comm_data.fragments_queue.fetchRemove(split_id)) |entry| {
            var fragment = entry.value;
            var iter = fragment.iterator();
            while (iter.next()) |frag_entry| {
                frag_entry.value_ptr.deinit();
            }
            fragment.deinit();
        }
        _ = self.comm_data.fragments_activity.remove(split_id);
    }

    fn purgeStaleFragments(self: *Self, now: Timestamp) void {
        var stale: [64]u16 = undefined;
        var stale_count: usize = 0;

        var iter = self.comm_data.fragments_activity.iterator();
        while (iter.next()) |entry| {
            const age_ns: i64 = @intCast(now.nanoseconds - entry.value_ptr.nanoseconds);
            if (age_ns > FRAGMENT_TIMEOUT_NS and stale_count < stale.len) {
                stale[stale_count] = entry.key_ptr.*;
                stale_count += 1;
            }
        }

        for (stale[0..stale_count]) |id| {
            Logger.WARN("Fragment set {d} timed out incomplete, dropping", .{id});
            self.removeFragmentSet(id);
        }
    }

    fn orderingChannel(self: *Self, channel: usize) ?*ChannelQueue {
        const slot = &self.comm_data.input_ordering_channels[channel];
        if (slot.*) |*existing| {
            return existing;
        }
        slot.* = ChannelQueue.init(self.options.allocator);
        return &(slot.*.?);
    }

    pub fn handlePacket(self: *Self, payload: []const u8) !void {
        if (payload.len == 0) return;
        const ID = payload[0];

        const allocator = self.options.allocator;

        switch (ID) {
            Proto.Packets.ConnectionRequestAccepted => {
                var pak = try ConnectionRequestAccepted.deserialize(payload, allocator);
                defer pak.deinit(allocator);

                var nic = NewIncomingConnection.init(
                    Address.init(4, self.options.address, self.options.port),
                    Address.init(4, "0.0.0.0", 0),
                    Timestamp.now(self.options.io, .real).toMilliseconds(),
                    pak.timestamp,
                );
                defer nic.deinit(allocator);

                var nic_buf: [NewIncomingConnection.MAX_SERIALIZED_SIZE]u8 = undefined;
                const serialized = try nic.serializeInto(&nic_buf);

                var frame = try Client.frameIn(serialized, allocator);
                self.sendFrame(&frame, .Immediate);
                frame.deinit();
                self.status = .Connected;

                if (self.connection_callback) |callback| {
                    callback(self, self.connection_callback_ctx);
                }
            },
            Proto.Packets.DisconnectNotification => {
                self.status = .Disconnected;

                if (self.disconnection_callback) |callback| {
                    callback(self, self.disconnection_callback_ctx);
                } else {
                    std.debug.print("Client disconnected by server\n", .{});
                }
            },
            254 => {
                if (self.game_callback) |callback| {
                    callback(self, payload, self.game_callback_ctx);
                }
            },
            Packets.ConnectedPing => {
                var ping = try ConnectedPing.deserialize(payload);
                defer ping.deinit();
                var pong = ConnectedPong.init(
                    ping.timestamp,
                    Timestamp.now(self.options.io, .real).toMilliseconds(),
                );
                defer pong.deinit();

                var pong_buf: [ConnectedPong.MAX_SERIALIZED_SIZE]u8 = undefined;
                const ser = try pong.serializeInto(&pong_buf);
                var frame = try Client.frameIn(ser, allocator);
                self.sendFrame(&frame, .Immediate);
                frame.deinit();
            },
            else => {
                Logger.WARN("Unhandeled inner packet {d}", .{ID});
            },
        }
    }

    pub fn handleAck(self: *Self, payload: []const u8) !void {
        if (self.status == .Disconnected) return;

        var ack = try Proto.Ack.deserialize(payload, self.options.allocator);
        defer ack.deinit();

        try self.comm_data.output_queue_mutex.lock(self.options.io);
        defer self.comm_data.output_queue_mutex.unlock(self.options.io);

        for (ack.sequences) |seq| {
            const key: u24 = @truncate(seq);
            if (self.comm_data.output_backup.fetchRemove(key)) |entry| {
                self.options.allocator.free(entry.value);
            }
        }
    }

    pub fn handleNack(self: *Self, payload: []const u8) !void {
        if (self.status == .Disconnected) return;

        var nack = try Proto.Ack.deserialize(payload, self.options.allocator);
        defer nack.deinit();

        try self.comm_data.output_queue_mutex.lock(self.options.io);
        defer self.comm_data.output_queue_mutex.unlock(self.options.io);

        for (nack.sequences) |seq| {
            const key: u24 = @truncate(seq);
            if (self.comm_data.output_backup.get(key)) |bytes| {
                self.socket.sendTo(bytes, self.options.address, self.options.port) catch |err| {
                    Logger.ERROR("Failed to send nack response: {any}", .{err});
                };
            }
        }
    }

    pub fn sendReliableMessage(self: *Client, msg: []const u8, priority: Priority) void {
        var frame = Client.frameIn(msg, self.options.allocator) catch |err| {
            Logger.ERROR("Failed to allocate reliable message: {any}", .{err});
            return;
        };
        defer frame.deinit();
        frame.reliability = Reliability.ReliableOrdered;
        self.sendFrame(&frame, priority);
    }

    pub fn frameIn(msg: []const u8, allocator: std.mem.Allocator) !Frame {
        const payload_copy = try allocator.dupe(u8, msg);
        return Frame.init(null, null, null, 0, Reliability.ReliableOrdered, payload_copy, null, null, null, allocator);
    }

    pub fn sendFrame(self: *Client, frame: *Frame, priority: Priority) void {
        self.send_mutex.lock(self.options.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return;
        };
        defer self.send_mutex.unlock(self.options.io);

        const channel = frame.order_channel orelse 0;
        if (channel >= MAX_CHANNELS) {
            Logger.WARN("sendFrame with invalid channel {d} dropped", .{channel});
            return;
        }
        var mutable_frame = frame.*;

        if (mutable_frame.isSequenced()) {
            mutable_frame.ordered_frame_index = self.comm_data.output_order_index[channel];
            mutable_frame.sequence_frame_index = self.comm_data.output_sequence_index[channel];
            self.comm_data.output_sequence_index[channel] += 1;
        } else if (mutable_frame.isOrdered()) {
            mutable_frame.ordered_frame_index = self.comm_data.output_order_index[channel];
            self.comm_data.output_order_index[channel] += 1;
            self.comm_data.output_sequence_index[channel] = 0;
        }

        const payload_size = mutable_frame.payload.len;
        const max_size = self.options.mtu_size - 36;
        if (payload_size <= max_size) {
            if (mutable_frame.isReliable()) {
                mutable_frame.reliable_frame_index = self.comm_data.output_reliable_index;
                self.comm_data.output_reliable_index += 1;
            }
            // Clear original frame's payload BEFORE queueFrame, since queueFrame with
            // Immediate priority may call sendQueueLocked which frees
            // the payload. This prevents double-free when caller calls frame.deinit().
            frame.payload = &[_]u8{};
            self.queueFrameLocked(mutable_frame, priority);
            return;
        } else {
            const split_size: usize = (payload_size + max_size - 1) / max_size;
            // Assign reliable index for the first fragment before splitting
            if (mutable_frame.isReliable()) {
                mutable_frame.reliable_frame_index = self.comm_data.output_reliable_index;
                self.comm_data.output_reliable_index += 1;
            }
            frame.payload = &[_]u8{};
            defer mutable_frame.deinit();
            self.handleLargePayload(mutable_frame, max_size, split_size, priority);
        }
    }

    pub fn handleLargePayload(self: *Client, frame: Frame, max_size: usize, split_size: usize, priority: Priority) void {
        const allocator = self.options.allocator;
        const split_id = self.comm_data.output_split_index & 0xFFFF;
        self.comm_data.output_split_index = (self.comm_data.output_split_index +% 1);

        var index: usize = 0;
        while (index < frame.payload.len) : (index += max_size) {
            const end_index = @min(index + max_size, frame.payload.len);
            const fragment_payload = frame.payload[index..end_index];

            const payload_copy = allocator.dupe(u8, fragment_payload) catch {
                Logger.ERROR("Failed to duplicate fragment payload", .{});
                return;
            };

            var new_frame = Frame.init(
                frame.reliable_frame_index,
                frame.sequence_frame_index,
                frame.ordered_frame_index,
                frame.order_channel,
                frame.reliability,
                payload_copy,
                @as(u32, @intCast(index / max_size)),
                split_id,
                @as(u32, @intCast(split_size)),
                allocator,
            );

            if (index != 0) {
                new_frame.reliable_frame_index = self.comm_data.output_reliable_index;
                self.comm_data.output_reliable_index += 1;
            }

            self.queueFrameLocked(new_frame, priority);
        }
    }

    // caller holds send_mutex
    fn queueFrameLocked(self: *Client, frame: Frame, priority: Priority) void {
        self.comm_data.output_queue_mutex.lock(self.options.io) catch |err| {
            std.debug.print("failed to acquire lock: {}\n", .{err});
            var mutable_frame = frame;
            mutable_frame.deinit();
            return;
        };
        defer self.comm_data.output_queue_mutex.unlock(self.options.io);

        self.comm_data.output_frame_queue.append(self.options.allocator, frame) catch {
            Logger.ERROR("Failed to queue frame", .{});
            var mutable_frame = frame;
            mutable_frame.deinit();
            return;
        };

        const should_send_immediately = priority == Priority.Immediate;
        if (should_send_immediately) {
            self.sendQueueLocked(self.queuedFrameCount());
        }
    }

    pub fn sendQueue(self: *Client, amount: usize) void {
        self.comm_data.output_queue_mutex.lock(self.options.io) catch |err| {
            std.debug.print("failed to acquire lock: {}\n", .{err});
            return;
        };
        defer self.comm_data.output_queue_mutex.unlock(self.options.io);
        self.sendQueueLocked(amount);
    }

    fn queuedFrameCount(self: *Client) usize {
        return self.comm_data.output_frame_queue.items.len - self.comm_data.output_queue_head;
    }

    fn advanceQueueHead(self: *Client, count: usize) void {
        const c = &self.comm_data;
        c.output_queue_head += count;

        if (c.output_queue_head == c.output_frame_queue.items.len) {
            c.output_frame_queue.clearRetainingCapacity();
            c.output_queue_head = 0;
        } else if (c.output_queue_head >= 64 and c.output_queue_head * 2 >= c.output_frame_queue.items.len) {
            c.output_frame_queue.replaceRange(self.options.allocator, 0, c.output_queue_head, &[_]Frame{}) catch {
                return;
            };
            c.output_queue_head = 0;
        }
    }

    // caller holds output_queue_mutex
    fn sendQueueLocked(self: *Client, amount: usize) void {
        if (self.queuedFrameCount() == 0) return;
        const allocator = self.options.allocator;

        const max_frameset_size = self.options.mtu_size - 28;

        var processed: usize = 0;
        while (processed < amount) {
            const available = self.queuedFrameCount();
            if (available == 0) break;

            const head = self.comm_data.output_queue_head;
            const candidate = @min(available, amount - processed);

            var fit: usize = 0;
            var current_size: usize = 4; // Frameset header size
            for (self.comm_data.output_frame_queue.items[head .. head + candidate]) |frame| {
                const frame_size = frame.getByteLength();
                if (current_size + frame_size > max_frameset_size and fit > 0) {
                    break;
                }
                current_size += frame_size;
                fit += 1;
            }
            if (fit == 0) fit = 1;

            const frames = self.comm_data.output_frame_queue.items[head .. head + fit];

            const sequence: u24 = @truncate(self.comm_data.output_sequence);
            self.comm_data.output_sequence += 1;

            const serialized = Proto.FrameSet.serializeInto(sequence, frames, self.send_scratch[0..]) catch |err| {
                Logger.ERROR("Failed to serialize frameset: {any}", .{err});
                for (frames) |*frame| frame.deinit();
                self.advanceQueueHead(fit);
                processed += fit;
                continue;
            };

            // Store serialized bytes for potential retransmission
            const backup_bytes = allocator.dupe(u8, serialized) catch |err| {
                Logger.ERROR("Failed to dupe serialized bytes: {any}", .{err});
                for (frames) |*frame| frame.deinit();
                self.advanceQueueHead(fit);
                processed += fit;
                continue;
            };

            if (self.comm_data.output_backup.fetchRemove(sequence)) |old| {
                allocator.free(old.value);
            }

            self.comm_data.output_backup.put(sequence, backup_bytes) catch |err| {
                Logger.WARN("Backup store failed, sending without reliability: {any}", .{err});
                allocator.free(backup_bytes);
            };

            self.send(serialized) catch |err| {
                Logger.ERROR("Failed to send frameset: {any}", .{err});
            };

            for (frames) |*frame| frame.deinit();
            self.advanceQueueHead(fit);
            processed += fit;
        }
    }

    pub fn tick(self: *Client) void {
        if (self.status == .Disconnected or !self.connect_called) return;

        // Only check for timeout after connect() has been called
        const now = Timestamp.now(self.options.io, .awake);
        const elapsed = self.last_receive.untilNow(self.options.io, .awake).toMilliseconds();
        if (self.connect_called and elapsed >= 15000) {
            Logger.WARN("Client has not received any packets in 15000ms (15s)", .{});
            self.status = .Disconnected;
            return;
        }

        const allocator = self.options.allocator;

        self.purgeStaleFragments(now);

        // Check queue length under lock to avoid race condition
        {
            self.comm_data.output_queue_mutex.lock(self.options.io) catch |err| {
                std.debug.print("failed to acquire lock: {}\n", .{err});
                return;
            };
            if (self.queuedFrameCount() > 0) {
                self.sendQueueLocked(MAX_FRAMES_PER_TICK);
            }
            self.comm_data.output_queue_mutex.unlock(self.options.io);
        }

        self.comm_data.input_mutex.lock(self.options.io) catch |err| {
            std.debug.print("failed to acquire lock: {}\n", .{err});
            return;
        };
        defer self.comm_data.input_mutex.unlock(self.options.io);

        if (self.comm_data.received_sequences.count() > 0) {
            var sequences_list = std.ArrayList(u32).empty;
            defer sequences_list.deinit(allocator);
            var iter = self.comm_data.received_sequences.keyIterator();
            while (iter.next()) |key| {
                sequences_list.append(allocator, key.*) catch continue;
            }
            if (sequences_list.items.len > 0) {
                std.mem.sort(u32, sequences_list.items, {}, comptime std.sort.asc(u32));

                const batch = sequences_list.items[0..@min(sequences_list.items.len, MAX_ACK_BATCH)];

                var ack_buf: [DATAGRAM_SCRATCH_SIZE]u8 = undefined;
                const serialized = Proto.Ack.serializeInto(batch, Proto.Packets.Ack, &ack_buf) catch |err| {
                    Logger.ERROR("Failed to serialize ack: {any}", .{err});
                    return;
                };

                for (batch) |seq| {
                    _ = self.comm_data.received_sequences.remove(@truncate(seq));
                }

                self.socket.sendTo(serialized, self.options.address, self.options.port) catch |err| {
                    Logger.ERROR("Failed to send ack: {any}", .{err});
                };
            }
        }
        if (self.comm_data.lost_sequences.count() > 0) {
            var sequences_list = std.ArrayList(u32).empty;
            defer sequences_list.deinit(allocator);
            var iter = self.comm_data.lost_sequences.keyIterator();
            while (iter.next()) |key| {
                sequences_list.append(allocator, key.*) catch continue;
            }
            if (sequences_list.items.len > 0) {
                std.mem.sort(u32, sequences_list.items, {}, comptime std.sort.asc(u32));

                const batch = sequences_list.items[0..@min(sequences_list.items.len, MAX_ACK_BATCH)];

                var nack_buf: [DATAGRAM_SCRATCH_SIZE]u8 = undefined;
                const serialized = Proto.Ack.serializeInto(batch, Proto.Packets.Nack, &nack_buf) catch |err| {
                    Logger.ERROR("Failed to serialize nack: {any}", .{err});
                    return;
                };

                for (batch) |seq| {
                    _ = self.comm_data.lost_sequences.remove(@truncate(seq));
                }

                self.socket.sendTo(serialized, self.options.address, self.options.port) catch |err| {
                    Logger.ERROR("Failed to send nack: {any}", .{err});
                };
            }
        }
    }
};

pub const ClientOptions = struct {
    io: Io,
    allocator: std.mem.Allocator = std.heap.page_allocator,
    address: []const u8 = "127.0.0.1",
    port: u16 = 19132,
    mtu_size: u16 = 1492,
    guid: i64 = 0,
    tick_rate: u64 = 20,
};

pub const Status = enum {
    Disconnected,
    Connecting,
    Connected,
    Disconnecting,
};

pub const Priority = enum(u8) {
    Immediate = 0,
    Normal = 1,
};

const ChannelQueue = std.AutoHashMap(u32, Frame);
const FragmentSet = std.AutoHashMap(u16, Frame);
const MAX_PENDING_SEQUENCES = 8192;
const MAX_FRAMES_PER_TICK: usize = 128;
const FRAGMENT_TIMEOUT_NS: i64 = 10 * std.time.ns_per_s;
// worst case 3 + 384*4 = 1539 fits the scratch buffer; bigger batches would
// fail to serialize every tick and never ack
const MAX_ACK_BATCH: usize = 384;

comptime {
    std.debug.assert(3 + MAX_ACK_BATCH * 4 <= DATAGRAM_SCRATCH_SIZE);
    std.debug.assert(DATAGRAM_SCRATCH_SIZE >= 1492); // >= default client MTU
}

fn initMapCapacity(comptime K: type, comptime V: type, allocator: std.mem.Allocator, capacity: u32) std.AutoHashMap(K, V) {
    var map = std.AutoHashMap(K, V).init(allocator);
    map.ensureTotalCapacity(capacity) catch {}; // best effort: grows on demand
    return map;
}

pub const CommData = struct {
    last_input_sequence: i32 = -1,
    received_sequences: std.AutoHashMap(u24, void),
    lost_sequences: std.AutoHashMap(u24, void),
    input_mutex: Io.Mutex = .init,
    input_order_index: [MAX_CHANNELS]u32,
    input_highest_sequence_index: [MAX_CHANNELS]u32,
    input_ordering_channels: [MAX_CHANNELS]?ChannelQueue,

    output_reliable_index: u32,
    output_sequence: u32,
    output_frame_queue: std.ArrayList(Frame),
    output_queue_head: usize = 0,
    output_queue_mutex: Io.Mutex = .init,
    /// Serialized datagrams kept for retransmission.
    output_backup: std.AutoHashMap(u24, []u8),
    output_order_index: [MAX_CHANNELS]u32,
    output_sequence_index: [MAX_CHANNELS]u32,
    output_split_index: u16,

    fragments_queue: std.AutoHashMap(u16, FragmentSet),
    fragments_activity: std.AutoHashMap(u16, Timestamp),

    pub fn deinit(self: *CommData, allocator: std.mem.Allocator) void {
        self.received_sequences.deinit();
        self.lost_sequences.deinit();

        // frames below the head were freed on send
        for (self.output_frame_queue.items[self.output_queue_head..]) |*frame| {
            frame.deinit();
        }
        self.output_frame_queue.deinit(allocator);

        for (&self.input_ordering_channels) |*maybe_queue| {
            if (maybe_queue.*) |*queue| {
                var inner_iterator = queue.iterator();
                while (inner_iterator.next()) |inner_entry| {
                    inner_entry.value_ptr.deinit();
                }
                queue.deinit();
            }
        }

        var backup_iter = self.output_backup.iterator();
        while (backup_iter.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        self.output_backup.deinit();

        var fragments_iter = self.fragments_queue.iterator();
        while (fragments_iter.next()) |outer_entry| {
            var inner_fragments_iter = outer_entry.value_ptr.iterator();
            while (inner_fragments_iter.next()) |inner_entry| {
                inner_entry.value_ptr.deinit();
            }
            outer_entry.value_ptr.deinit();
        }
        self.fragments_queue.deinit();
        self.fragments_activity.deinit();
    }
};
