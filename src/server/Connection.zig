const std = @import("std");
const Timestamp = std.Io.Timestamp;

const Logger = @import("../misc/Logger.zig").Logger;
const Proto = @import("../proto/root.zig");
const Frame = Proto.Frame;
const Reliability = Proto.Reliability;
const ServerModule = @import("./Server.zig");
const Server = ServerModule.Server;

const MAX_CHANNELS = 32;
const MAX_ORDERING_QUEUE_SIZE = 64;
const MAX_SPLIT_SIZE: u32 = 1024;
const MAX_FRAGMENT_SETS = 256;
const FRAGMENT_TIMEOUT_NS: i64 = 10 * std.time.ns_per_s;
const MAX_LOST_GAP: u32 = 4096;
const MAX_PENDING_SEQUENCES = 8192;
const RETRANSMIT_TIMEOUT_NS: i64 = 200 * std.time.ns_per_ms;
const MAX_RETRANSMITS: u8 = 10;
const MAX_RETRANSMITS_PER_TICK: usize = 32;
// 3 + 384*4 = 1539: worst-case ACK datagram; a bigger batch would fail to
// serialize every tick and never ack.
const MAX_ACK_BATCH: usize = 384;
const MAX_FRAMES_PER_TICK: usize = 512;
const MAX_SEND_NS: i64 = 5 * std.time.ns_per_ms;
const DATAGRAM_SCRATCH_SIZE = 1600;

comptime {
    std.debug.assert(3 + MAX_ACK_BATCH * 4 <= DATAGRAM_SCRATCH_SIZE);
    std.debug.assert(DATAGRAM_SCRATCH_SIZE >= ServerModule.MAX_MTU_SIZE);
}

const PERFORM_TIME_CHECKS = false;
const DEBUG = false;

// Game packet callback for Connection (packet ID 254)
pub const GamePacketCallback = *const fn (connection: *Connection, payload: []const u8, context: ?*anyopaque) void;

pub const BackupEntry = struct {
    bytes: []u8,
    sent_ns: i64,
    retries: u8,
};

const ChannelQueue = std.AutoHashMap(u32, Frame);
const FragmentSet = std.AutoHashMap(u16, Frame);

fn initMapCapacity(comptime K: type, comptime V: type, allocator: std.mem.Allocator, capacity: u32) std.AutoHashMap(K, V) {
    var map = std.AutoHashMap(K, V).init(allocator);
    map.ensureTotalCapacity(capacity) catch {}; // best effort: grows on demand
    return map;
}

pub const Connection = struct {
    const Self = @This();
    server: *Server,
    address: std.Io.net.IpAddress,
    key: i64,
    mtu_size: u16,
    guid: i64,
    connected: bool,
    active: bool,
    comm_data: CommData,
    last_receive: Timestamp,
    created_at: Timestamp,
    game_packet_callback: ?GamePacketCallback = null,
    game_packet_context: ?*anyopaque = null,
    tick_counter: u64 = 0,
    last_ping_time: std.Io.Timestamp = .zero,
    ping_interval: std.Io.Duration = .fromMilliseconds(5000),
    send_mutex: std.Io.Mutex = .init,
    pending_connect_event: bool = false,
    send_scratch: [DATAGRAM_SCRATCH_SIZE]u8 = undefined,
    pending_movement: ?[]u8 = null,

    pub fn init(server: *Server, address: std.Io.net.IpAddress, mtu_size: u16, guid: i64) Self {
        return Self{
            .server = server,
            .address = address,
            .key = Server.addressToKey(address),
            .mtu_size = mtu_size,
            .guid = guid,
            .connected = false,
            .active = true,
            .comm_data = .{
                .received_sequences = initMapCapacity(u24, void, server.options.allocator, 16),
                .lost_sequences = initMapCapacity(u24, void, server.options.allocator, 16),
                .input_order_index = [_]u32{0} ** MAX_CHANNELS,
                .input_highest_sequence_index = [_]u32{0} ** MAX_CHANNELS,
                .input_ordering_channels = [_]?ChannelQueue{null} ** MAX_CHANNELS,
                .output_reliable_index = 0,
                .output_sequence = 0,
                .output_frame_queue = std.ArrayList(Frame).initBuffer(&[_]Frame{}),
                .output_backup = initMapCapacity(u24, BackupEntry, server.options.allocator, 64),
                .output_order_index = [_]u32{0} ** MAX_CHANNELS,
                .output_sequence_index = [_]u32{0} ** MAX_CHANNELS,
                .output_split_index = 0,
                .fragments_queue = initMapCapacity(u16, FragmentSet, server.options.allocator, 8),
                .fragments_activity = initMapCapacity(u16, Timestamp, server.options.allocator, 8),
            },
            .last_receive = Timestamp.now(server.io, .awake),
            .created_at = Timestamp.now(server.io, .awake),
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.pending_movement) |pending| {
            self.server.options.allocator.free(pending);
            self.pending_movement = null;
        }
        self.comm_data.deinit(self.server.options.allocator);
    }

    pub fn handlePacket(self: *Self, payload: []const u8) !void {
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;
        if (payload.len == 0) return;
        const ID = payload[0];

        const allocator = self.server.options.allocator;

        switch (ID) {
            Proto.Packets.ConnectionRequest => {
                var request = try Proto.ConnectionRequest.deserialize(payload);
                defer request.deinit();

                const empty_address = Proto.Address.init(4, "0.0.0.0", 0);
                var accepted = Proto.ConnectionRequestAccepted.init(
                    empty_address,
                    0,
                    empty_address,
                    request.timestamp,
                    Timestamp.now(self.server.io, .real).toMilliseconds(),
                );

                defer accepted.deinit(allocator);

                var accepted_buf: [Proto.ConnectionRequestAccepted.MAX_SERIALIZED_SIZE]u8 = undefined;
                const serialized = try accepted.serializeInto(&accepted_buf);
                const frame = try frameIn(serialized, allocator);

                self.sendFrame(frame, .Immediate);
            },
            Proto.Packets.NewIncomingConnection => {
                self.connected = true;
                const elapsed = self.created_at.untilNow(self.server.io, .awake);

                if (DEBUG)
                    Logger.DEBUG("Connection established in {d}ms", .{elapsed.toMilliseconds()});

                // fired by Server after it releases connections_mutex
                self.pending_connect_event = true;
            },
            Proto.Packets.DisconnectNotification => {
                self.connected = false;
                self.active = false;
            },
            254 => {
                // Game packet - trigger connection game packet callback
                if (self.game_packet_callback) |callback| {
                    callback(self, payload, self.game_packet_context);
                }
            },
            Proto.Packets.ConnectedPing => {
                var ping = Proto.ConnectedPing.deserialize(payload) catch |err| {
                    Logger.ERROR("Failed to deserialize ConnectedPing: {any}", .{err});
                    return;
                };

                defer ping.deinit();

                const current_time_ms = Timestamp.now(self.server.io, .real).toMilliseconds();
                var pong = Proto.ConnectedPong.init(ping.timestamp, current_time_ms);
                defer pong.deinit();

                var pong_buf: [Proto.ConnectedPong.MAX_SERIALIZED_SIZE]u8 = undefined;
                const serialized = pong.serializeInto(&pong_buf) catch |err| {
                    Logger.ERROR("Failed to serialize ConnectedPong: {any}", .{err});
                    return;
                };

                const frame = try frameIn(serialized, allocator);
                self.sendFrame(frame, .Immediate);
            },
            Proto.Packets.ConnectedPong => {
                var pong = Proto.ConnectedPong.deserialize(payload) catch |err| {
                    Logger.ERROR("Failed to deserialize ConnectedPong: {any}", .{err});
                    return;
                };

                defer pong.deinit();

                const current_time_ms = Timestamp.now(self.server.io, .real).toMilliseconds();
                const rtt = current_time_ms - pong.timestamp; // Round trip time

                if (DEBUG)
                    Logger.DEBUG("Received ConnectedPong - RTT: {d}ms", .{rtt});
            },
            else => {
                Logger.WARN("Unhandeled Packet {d}", .{ID});
            },
        }
        if (start_time) |start| {
            const elapsed = start.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: handlePacket took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    pub fn tick(self: *Connection) void {
        if (!self.active) return;

        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;
        const elapsed = self.last_receive.untilNow(self.server.io, .awake);

        if (elapsed.toMilliseconds() > 15000) {
            Logger.WARN("Connection {any} has not received any packets in 15000ms", .{self.address});
            self.active = false;
            return;
        }

        const now = Timestamp.now(self.server.io, .awake);

        self.purgeStaleFragments(now);
        self.flushPendingMovement();

        self.send_mutex.lock(self.server.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return;
        };

        if (self.queuedFrameCount() > 0) {
            self.sendQueueLocked(MAX_FRAMES_PER_TICK, MAX_SEND_NS);
        }

        self.retransmitTimedOut(now);

        self.send_mutex.unlock(self.server.io);

        if (self.comm_data.received_sequences.count() > 0) {
            var batch_storage: [MAX_ACK_BATCH]u32 = undefined;
            var batch_len: usize = 0;
            var iter = self.comm_data.received_sequences.keyIterator();
            while (iter.next()) |key| {
                if (batch_len == batch_storage.len) break;
                batch_storage[batch_len] = key.*;
                batch_len += 1;
            }
            if (batch_len > 0) {
                const batch = batch_storage[0..batch_len];
                std.mem.sort(u32, batch, {}, comptime std.sort.asc(u32));
                var ack_buf: [DATAGRAM_SCRATCH_SIZE]u8 = undefined;
                const serialized = Proto.Ack.serializeInto(batch, Proto.Packets.Ack, &ack_buf) catch |err| {
                    Logger.ERROR("Failed to serialize ack: {any}", .{err});
                    return;
                };
                for (batch) |seq| _ = self.comm_data.received_sequences.remove(@truncate(seq));
                self.send(serialized);
            }
        }
        if (self.comm_data.lost_sequences.count() > 0) {
            var batch_storage: [MAX_ACK_BATCH]u32 = undefined;
            var batch_len: usize = 0;
            var iter = self.comm_data.lost_sequences.keyIterator();
            while (iter.next()) |key| {
                if (batch_len == batch_storage.len) break;
                batch_storage[batch_len] = key.*;
                batch_len += 1;
            }
            if (batch_len > 0) {
                const batch = batch_storage[0..batch_len];
                std.mem.sort(u32, batch, {}, comptime std.sort.asc(u32));
                var nack_buf: [DATAGRAM_SCRATCH_SIZE]u8 = undefined;
                const serialized = Proto.Ack.serializeInto(batch, Proto.Packets.Nack, &nack_buf) catch |err| {
                    Logger.ERROR("Failed to serialize nack: {any}", .{err});
                    return;
                };
                for (batch) |seq| _ = self.comm_data.lost_sequences.remove(@truncate(seq));
                self.send(serialized);
            }
        }

        // Send ping every ping_interval milliseconds if connected
        if (self.connected) {
            const since_last_ping = self.last_ping_time.durationTo(now);

            if (since_last_ping.nanoseconds >= self.ping_interval.nanoseconds) {
                self.sendPing();
                self.last_ping_time = now;
            }
        }

        self.tick_counter += 1;

        if (start_time) |start| {
            const tick_elapsed = start.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: tick took {d} ms", .{tick_elapsed.toMilliseconds()});
        }
    }

    pub fn handleAck(self: *Self, payload: []const u8) !void {
        if (!self.active) return;

        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;

        var ack = try Proto.Ack.deserialize(payload, self.server.options.allocator);
        defer ack.deinit();

        // output_backup belongs to send_mutex
        self.send_mutex.lock(self.server.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return;
        };
        defer self.send_mutex.unlock(self.server.io);

        for (ack.sequences) |seq| {
            const key: u24 = @truncate(seq);
            if (self.comm_data.output_backup.fetchRemove(key)) |entry| {
                self.server.options.allocator.free(entry.value.bytes);
            }
        }

        if (start_time) |start| {
            const elapsed = start.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: handleAck took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    pub fn handleNack(self: *Self, payload: []const u8) !void {
        if (!self.active) return;

        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;

        var nack = try Proto.Ack.deserialize(payload, self.server.options.allocator);
        defer nack.deinit();

        self.send_mutex.lock(self.server.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            return;
        };
        defer self.send_mutex.unlock(self.server.io);

        const now = Timestamp.now(self.server.io, .awake);
        for (nack.sequences) |seq| {
            const key: u24 = @truncate(seq);
            if (self.comm_data.output_backup.getPtr(key)) |entry| {
                self.send(entry.bytes);
                entry.sent_ns = @intCast(now.nanoseconds);
                entry.retries += 1;
            }
        }

        if (start_time) |start| {
            const elapsed = start.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: handleNack took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    pub fn onFrameSet(self: *Self, buffer: []const u8) !void {
        if (!self.active) return;

        self.last_receive = Timestamp.now(self.server.io, .awake);
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;

        var frameSet = try Proto.FrameSet.deserialize(buffer, self.server.options.allocator);
        defer frameSet.deinit(self.server.options.allocator);

        const sequence = frameSet.sequence_number;
        const last = self.comm_data.last_input_sequence;
        const is_older_than_last = last != -1 and sequence <= @as(u24, @intCast(@max(0, last)));
        const is_already_received = self.comm_data.received_sequences.contains(sequence);

        if (is_older_than_last or is_already_received) {
            return;
        }

        if (self.comm_data.received_sequences.count() >= MAX_PENDING_SEQUENCES) {
            self.comm_data.received_sequences.clearRetainingCapacity();
        }

        self.comm_data.received_sequences.put(sequence, {}) catch {
            return;
        };

        _ = self.comm_data.lost_sequences.remove(sequence);

        // big jumps are client restarts, not real loss
        if (last >= 0 and sequence > last) {
            const last_u32: u32 = @intCast(last);
            if (sequence > last_u32) {
                const gap: u32 = @as(u32, sequence) - last_u32 - 1;
                if (gap > 0 and gap <= MAX_LOST_GAP) {
                    var i: u32 = last_u32 + 1;
                    while (i < sequence) : (i += 1) {
                        if (self.comm_data.lost_sequences.count() >= MAX_PENDING_SEQUENCES) break;
                        self.comm_data.lost_sequences.put(@truncate(i), {}) catch break;
                    }
                }
            }
        }

        self.comm_data.last_input_sequence = @as(i32, @intCast(sequence));
        for (frameSet.frames) |frame| {
            try self.handleFrame(frame);
        }

        if (start_time) |s_time| {
            const elapsed = s_time.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: onFrameSet took {d} ms", .{elapsed.toMicroseconds()});
        }
    }

    pub fn handleFrame(self: *Connection, frame: Frame) !void {
        if (!self.active) return;

        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;

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
            self.handlePacket(frame.payload) catch {
                Logger.ERROR("Failed to handle packet", .{});
                return;
            };
        }

        if (start_time) |start| {
            const elapsed = start.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: handleFrame took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    pub fn handleOrderedFrame(self: *Connection, frame: Frame) void {
        if (!self.active) return;

        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;

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

            self.handlePacket(frame.payload) catch {
                Logger.ERROR("Failed to handle packet", .{});
                return;
            };

            var index = self.comm_data.input_order_index[channel];
            if (self.orderingChannel(channel)) |queue| {
                while (queue.contains(index)) {
                    var iframe = queue.get(index).?;
                    _ = queue.remove(index);
                    self.handlePacket(iframe.payload) catch |err| {
                        Logger.ERROR("Failed to handle ordered queued packet: {any}", .{err});
                        iframe.deinit();
                        return;
                    };
                    iframe.deinit();
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
                const allocator = self.server.options.allocator;
                const payload_copy = allocator.dupe(u8, frame.payload) catch {
                    Logger.ERROR("Failed to dupe payload for ordering queue", .{});
                    return;
                };

                var frame_copy = frame;
                frame_copy.payload = payload_copy;
                frame_copy.allocator = allocator;

                queue.put(frame_index, frame_copy) catch |err| {
                    Logger.ERROR("Failed to put frame in ordering queue: {any}", .{err});
                    allocator.free(payload_copy);
                    return;
                };
            }
        } else {
            self.handlePacket(frame.payload) catch {
                Logger.ERROR("Failed to handle packet", .{});
                return;
            };
        }

        if (start_time) |start| {
            const elapsed = start.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: handleOrderedFrame took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    pub fn handleSequencedFrame(self: *Self, frame: Frame) void {
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;

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
                Logger.ERROR("Failed to handle packet: {any}", .{err});
                return;
            };
        }

        if (start_time) |start| {
            const elapsed = start.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: handleSequencedFrame took {d} ms", .{elapsed.toMilliseconds()});
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

        const allocator = self.server.options.allocator;
        const index_u16: u16 = @intCast(split_index);

        if (self.comm_data.fragments_queue.getPtr(split_id)) |fragment| {
            // Duplicate fragment, ignore
            if (fragment.contains(index_u16)) {
                return;
            }

            const payload_copy = allocator.dupe(u8, frame.payload) catch {
                Logger.ERROR("Failed to duplicate frame payload", .{});
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
                Logger.ERROR("Failed to put frame in fragment queue", .{});
                frame_copy.deinit();
                return;
            };
            self.comm_data.fragments_activity.put(split_id, Timestamp.now(self.server.io, .awake)) catch {};

            if (fragment.count() == split_size) {
                var total_length: usize = 0;
                var complete = true;
                var index: u16 = 0;
                while (index < split_size) : (index += 1) {
                    const sframe = fragment.get(index) orelse {
                        complete = false;
                        break;
                    };
                    total_length += sframe.payload.len;
                }

                if (!complete) {
                    Logger.WARN("Fragment set {d} incomplete despite count match", .{split_id});
                    return;
                }

                const reconstructed = allocator.alloc(u8, total_length) catch {
                    Logger.ERROR("Failed to allocate reconstructed payload", .{});
                    return;
                };

                var offset: usize = 0;
                index = 0;
                while (index < split_size) : (index += 1) {
                    const sframe = fragment.get(index).?;
                    @memcpy(reconstructed[offset .. offset + sframe.payload.len], sframe.payload);
                    offset += sframe.payload.len;
                }

                var nframe = Frame.init(
                    frame.reliable_frame_index,
                    frame.sequence_frame_index,
                    frame.ordered_frame_index,
                    frame.order_channel,
                    frame.reliability,
                    reconstructed,
                    null, // split_frame_index - not split anymore
                    null, // split_id - not split anymore
                    null, // split_size - not split anymore
                    allocator,
                );

                // fragments are consumed; nframe owns its payload now
                self.removeFragmentSet(split_id);

                if (nframe.isSequenced()) {
                    self.handleSequencedFrame(nframe);
                } else if (nframe.isOrdered()) {
                    self.handleOrderedFrame(nframe);
                } else {
                    self.handlePacket(nframe.payload) catch {
                        Logger.ERROR("Failed to handle reconstructed packet", .{});
                    };
                }

                nframe.deinit();
            }
        } else {
            if (self.comm_data.fragments_queue.count() >= MAX_FRAGMENT_SETS) {
                Logger.WARN("Too many incomplete fragment sets, dropping split_id {d}", .{split_id});
                return;
            }

            var new_fragment = FragmentSet.init(allocator);
            errdefer new_fragment.deinit();

            const payload_copy = allocator.dupe(u8, frame.payload) catch {
                Logger.ERROR("Failed to duplicate frame payload", .{});
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
                Logger.ERROR("Failed to create new fragment queue", .{});
                frame_copy.deinit();
                return;
            };
            self.comm_data.fragments_queue.put(split_id, new_fragment) catch {
                Logger.ERROR("Failed to add fragment to queue", .{});
                frame_copy.deinit();
                new_fragment.deinit();
                return;
            };
            self.comm_data.fragments_activity.put(split_id, Timestamp.now(self.server.io, .awake)) catch {};
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
        slot.* = ChannelQueue.init(self.server.options.allocator);
        return &(slot.*.?);
    }

    pub fn frameIn(msg: []const u8, allocator: std.mem.Allocator) !Frame {
        const payload_copy = try allocator.dupe(u8, msg);
        return Frame.init(null, null, null, 0, Reliability.ReliableOrdered, payload_copy, null, null, null, allocator);
    }

    pub fn sendReliableMessage(self: *Connection, msg: []const u8, priority: Priority) void {
        if (!self.active) return;

        var frame = frameIn(msg, self.server.options.allocator) catch |err| {
            Logger.ERROR("Failed to allocate reliable message: {any}", .{err});
            return;
        };
        frame.reliability = Reliability.ReliableOrdered;

        self.sendFrame(frame, priority);
    }

    pub fn sendMovementReliableMessage(self: *Connection, msg: []const u8) void {
        if (!self.active) return;
        const allocator = self.server.options.allocator;
        const copy = allocator.dupe(u8, msg) catch {
            self.comm_data.movement_dropped += 1;
            return;
        };
        self.send_mutex.lock(self.server.io) catch {
            allocator.free(copy);
            self.comm_data.movement_dropped += 1;
            return;
        };
        if (self.pending_movement) |previous| {
            allocator.free(previous);
            self.comm_data.movement_coalesced += 1;
        }
        self.pending_movement = copy;
        self.send_mutex.unlock(self.server.io);
    }

    fn flushPendingMovement(self: *Connection) void {
        self.send_mutex.lock(self.server.io) catch return;
        const pending = self.pending_movement orelse {
            self.send_mutex.unlock(self.server.io);
            return;
        };
        self.pending_movement = null;
        self.send_mutex.unlock(self.server.io);

        const frame = Frame.init(null, null, null, 0, Reliability.ReliableOrdered, pending, null, null, null, self.server.options.allocator);
        self.sendFrame(frame, .Normal);
    }

    pub fn sendFrame(self: *Connection, frame: Frame, priority: Priority) void {
        if (!self.active) {
            var f = frame;
            f.deinit();
            return;
        }

        self.send_mutex.lock(self.server.io) catch |err| {
            Logger.WARN("mutex lock failed: {}", .{err});
            var f = frame;
            f.deinit();
            return;
        };

        defer self.send_mutex.unlock(self.server.io);

        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;

        const channel = frame.order_channel orelse 0;
        if (channel >= MAX_CHANNELS) {
            Logger.WARN("sendFrame with invalid channel {d} dropped", .{channel});
            var f = frame;
            f.deinit();
            return;
        }

        var mutable_frame = frame;

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
        const max_size = self.mtu_size - 36;

        if (payload_size <= max_size) {
            if (mutable_frame.isReliable()) {
                mutable_frame.reliable_frame_index = self.comm_data.output_reliable_index;
                self.comm_data.output_reliable_index += 1;
            }
            self.queueFrameLocked(mutable_frame, priority);
        } else {
            const split_size = (payload_size + max_size - 1) / max_size;
            self.handleLargePayload(&mutable_frame, max_size, split_size, priority);
        }

        if (start_time) |start| {
            const elapsed = start.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: sendFrame took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    pub fn handleLargePayload(
        self: *Connection,
        frame: *Frame,
        max_size: usize,
        split_size: usize,
        priority: Priority,
    ) void {
        const allocator = self.server.options.allocator;
        const split_id = self.comm_data.output_split_index;

        self.comm_data.output_split_index = (self.comm_data.output_split_index +% 1);

        // Store original payload reference before we start fragmenting
        const original_payload = frame.payload;
        defer allocator.free(original_payload); // Free the original frame's payload after fragmenting

        var index: usize = 0;
        while (index < original_payload.len) {
            const end_index = @min(index + max_size, original_payload.len);
            const fragment_payload = original_payload[index..end_index];

            const payload_copy = allocator.dupe(u8, fragment_payload) catch {
                Logger.ERROR("Failed to duplicate fragment payload", .{});
                return;
            };

            var new_frame = Frame.init(frame.reliable_frame_index, frame.sequence_frame_index, frame.ordered_frame_index, frame.order_channel, frame.reliability, payload_copy, @as(u32, @intCast(index / max_size)), // split_frame_index
                split_id, // split_id
                @as(u32, @intCast(split_size)), // split_size
                allocator);

            if (new_frame.isReliable()) {
                new_frame.reliable_frame_index = self.comm_data.output_reliable_index;
                self.comm_data.output_reliable_index += 1;
            }

            self.queueFrameLocked(new_frame, priority);
            index += max_size;
        }
    }

    // caller holds send_mutex
    fn queueFrameLocked(self: *Connection, frame: Frame, priority: Priority) void {
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;

        // Don't queue frames if connection is not active - prevents leaks during shutdown
        if (!self.active) {
            var f = frame;
            f.deinit();
            return;
        }

        self.comm_data.output_frame_queue.append(self.server.options.allocator, frame) catch {
            Logger.ERROR("Failed to queue frame", .{});
            var f = frame;
            f.deinit();
            return;
        };
        self.comm_data.output_frames_queued += 1;
        self.comm_data.output_queue_peak = @max(self.comm_data.output_queue_peak, self.queuedFrameCount());

        const should_send_immediately = priority == Priority.Immediate;
        if (should_send_immediately) {
            self.sendQueueLocked(self.queuedFrameCount(), 0);
        }

        if (start_time) |start| {
            const elapsed = start.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: queueFrame took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    fn queuedFrameCount(self: *Connection) usize {
        return self.comm_data.output_frame_queue.items.len - self.comm_data.output_queue_head;
    }

    fn advanceQueueHead(self: *Connection, count: usize) void {
        const c = &self.comm_data;
        c.output_queue_head += count;

        // memmove only once the dead prefix is large (amortized O(1))
        if (c.output_queue_head == c.output_frame_queue.items.len) {
            c.output_frame_queue.clearRetainingCapacity();
            c.output_queue_head = 0;
        } else if (c.output_queue_head >= 64 and c.output_queue_head * 2 >= c.output_frame_queue.items.len) {
            c.output_frame_queue.replaceRange(self.server.options.allocator, 0, c.output_queue_head, &[_]Frame{}) catch {
                return;
            };
            c.output_queue_head = 0;
        }
    }

    // caller holds send_mutex
    fn sendQueueLocked(self: *Connection, amount: usize, budget_ns: i64) void {
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;
        const allocator = self.server.options.allocator;
        const now = Timestamp.now(self.server.io, .awake);
        const budget_started = now;

        const max_frameset_size = self.mtu_size - 28; // Leave room for UDP/IP headers

        var processed: usize = 0;
        while (processed < amount) {
            if (budget_ns > 0 and processed > 0) {
                const elapsed_ns = budget_started.untilNow(self.server.io, .awake).nanoseconds;
                if (elapsed_ns >= budget_ns) break;
            }
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

            const backup_bytes = allocator.dupe(u8, serialized) catch |err| {
                Logger.ERROR("Backup alloc failed: {any}", .{err});
                for (frames) |*frame| frame.deinit();
                self.advanceQueueHead(fit);
                processed += fit;
                continue;
            };

            // retire whatever previously occupied this sequence slot (u24 wrap)
            if (self.comm_data.output_backup.fetchRemove(sequence)) |old| {
                allocator.free(old.value.bytes);
            }

            self.comm_data.output_backup.put(sequence, .{
                .bytes = backup_bytes,
                .sent_ns = @intCast(now.nanoseconds),
                .retries = 0,
            }) catch |err| {
                Logger.WARN("Backup store failed, sending without reliability: {any}", .{err});
                allocator.free(backup_bytes);
            };

            self.send(serialized);
            self.comm_data.output_frames_sent += fit;

            for (frames) |*frame| frame.deinit();
            self.advanceQueueHead(fit);
            processed += fit;
        }

        if (start_time) |start| {
            const end_time = Timestamp.now(self.server.io, .awake);
            const elapsed = start.durationTo(end_time);
            Logger.DEBUG("PERF: sendQueue took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    fn retransmitTimedOut(self: *Connection, now: Timestamp) void {
        var resent: usize = 0;
        var dropped_key: ?u24 = null;

        var iter = self.comm_data.output_backup.iterator();
        while (iter.next()) |entry| {
            if (resent >= MAX_RETRANSMITS_PER_TICK) break;

            const age_ns: i64 = @intCast(now.nanoseconds - entry.value_ptr.sent_ns);
            if (age_ns < RETRANSMIT_TIMEOUT_NS) continue;

            if (entry.value_ptr.retries >= MAX_RETRANSMITS) {
                Logger.WARN("Connection {any} unresponsive (sequence {d} unacked after {d} retries)", .{ self.address, entry.key_ptr.*, entry.value_ptr.retries });
                dropped_key = entry.key_ptr.*;
                self.active = false;
                break;
            }

            self.send(entry.value_ptr.bytes);
            entry.value_ptr.sent_ns = @intCast(now.nanoseconds);
            entry.value_ptr.retries += 1;
            resent += 1;
        }

        if (dropped_key) |key| {
            if (self.comm_data.output_backup.fetchRemove(key)) |entry| {
                self.server.options.allocator.free(entry.value.bytes);
            }
        }
    }

    pub fn send(self: *Connection, data: []const u8) void {
        const start_time: ?Timestamp = if (PERFORM_TIME_CHECKS) .now(self.server.io, .awake) else null;

        self.server.send(data, self.address);
        if (start_time) |start| {
            const elapsed = start.untilNow(self.server.io, .awake);
            Logger.DEBUG("PERF: send took {d} ms", .{elapsed.toMilliseconds()});
        }
    }

    pub fn takePendingConnect(self: *Self) bool {
        const was_pending = self.pending_connect_event;
        self.pending_connect_event = false;
        return was_pending;
    }

    /// Set game packet callback (for packet ID 254)
    pub fn setGamePacketCallback(self: *Connection, callback: ?GamePacketCallback, context: ?*anyopaque) void {
        self.game_packet_callback = callback;
        self.game_packet_context = context;
    }

    pub fn getAddress(self: *const Connection) std.Io.net.IpAddress {
        return self.address;
    }

    pub fn isConnected(self: *const Connection) bool {
        return self.connected;
    }

    pub fn isActive(self: *const Connection) bool {
        return self.active;
    }

    /// Send a ConnectedPing packet to the client
    pub fn sendPing(self: *Connection) void {
        const allocator = self.server.options.allocator;
        const timestamp = Timestamp.now(self.server.io, .real).toMilliseconds();

        var ping = Proto.ConnectedPing.init(timestamp);
        defer ping.deinit();

        var ping_buf: [Proto.ConnectedPing.MAX_SERIALIZED_SIZE]u8 = undefined;
        const serialized = ping.serializeInto(&ping_buf) catch |err| {
            Logger.ERROR("Failed to serialize ConnectedPing: {any}", .{err});
            return;
        };

        const frame = frameIn(serialized, allocator) catch |err| {
            Logger.ERROR("Failed to allocate ping frame: {any}", .{err});
            return;
        };
        self.sendFrame(frame, .Normal);
    }
};

pub const CommData = struct {
    last_input_sequence: i32 = -1,
    received_sequences: std.AutoHashMap(u24, void),
    lost_sequences: std.AutoHashMap(u24, void),
    input_order_index: [MAX_CHANNELS]u32,
    input_highest_sequence_index: [MAX_CHANNELS]u32,
    input_ordering_channels: [MAX_CHANNELS]?ChannelQueue,

    output_reliable_index: u32,
    output_sequence: u32,
    output_frame_queue: std.ArrayList(Frame),
    output_queue_head: usize = 0,
    output_frames_queued: u64 = 0,
    output_frames_sent: u64 = 0,
    output_queue_peak: usize = 0,
    output_backup: std.AutoHashMap(u24, BackupEntry),
    output_order_index: [MAX_CHANNELS]u32,
    output_sequence_index: [MAX_CHANNELS]u32,
    output_split_index: u16,

    fragments_queue: std.AutoHashMap(u16, FragmentSet),
    fragments_activity: std.AutoHashMap(u16, Timestamp),
    movement_coalesced: u64 = 0,
    movement_dropped: u64 = 0,

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
            allocator.free(entry.value_ptr.bytes);
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

pub const Priority = enum(u8) {
    Immediate = 0,
    Normal = 1,
};
