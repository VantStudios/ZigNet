const std = @import("std");
const Timestamp = std.Io.Timestamp;
const Duration = std.Io.Duration;

const Raknet = @import("Raknet");
const Server = Raknet.Server;
const Client = Raknet.Client;
const Connection = Raknet.Connection;
const Logger = Raknet.Logger;

const SERVER = true; // true = run server, false = run client

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    const allocator = gpa.allocator();
    Logger.init(allocator);
    defer {
        if (gpa.detectLeaks() > 0) {
            Logger.ERROR("Leaks detected", .{});
        } else {
            Logger.INFO("No leaks detected", .{});
        }
    }
    if (SERVER) {
        try runServer(io, allocator);
    } else {
        try runClient(io, allocator);
    }
}

fn runServer(io: std.Io, allocator: std.mem.Allocator) !void {
    Logger.INFO("Running ZigNet Server", .{});
    var server = try Server.init(io, .{
        .allocator = allocator,
    });
    defer server.deinit();

    server.setConnectCallback(onServerConnect, null);
    server.setDisconnectCallback(onServerDisconnect, null);
    try server.start();

    try io.sleep(Duration.fromSeconds(300), .awake);
}

fn runClient(io: std.Io, allocator: std.mem.Allocator) !void {
    Logger.INFO("Running ZigNet Client", .{});
    var client = try Client.init(.{
        .io = io,
        .allocator = allocator,
        .address = "127.0.0.1",
        .port = 19132,
    });
    defer client.deinit();

    connect_start_time = Timestamp.now(io, .awake);
    client.setConnectionCallback(onClientConnect, null);
    client.setGamePacketCallback(onGamePacket, null);
    client.setDisconnectionCallback(onClientDisconnect, null);
    try client.connect();

    const time_start = Timestamp.now(io, .awake);
    while (time_start.untilNow(io, .awake).toMilliseconds() < 120000 and client.status != .Disconnected) {
        try io.sleep(Duration.fromNanoseconds(std.time.ns_per_s), .awake);
    }
    try io.sleep(Duration.fromNanoseconds(std.time.ns_per_s), .awake);
    client.status = .Disconnected;
}

var connect_start_time: std.Io.Timestamp = undefined;

fn onServerConnect(connection: *Connection, context: ?*anyopaque) void {
    _ = context;
    Logger.INFO("Server: Client connected from {any}", .{connection.address});
}

fn onServerDisconnect(connection: *Connection, context: ?*anyopaque) void {
    _ = context;
    Logger.INFO("Server: Client disconnected from {any}", .{connection.address});
}

fn onClientConnect(client: *Client, context: ?*anyopaque) void {
    _ = context;
    const elapsed = connect_start_time.untilNow(client.options.io, .awake).toMilliseconds();
    Logger.INFO("Client: Connected in {d}ms", .{elapsed});
    client.setGamePacketCallback(onGamePacket, null);
}

fn onClientDisconnect(client: *Client, context: ?*anyopaque) void {
    _ = context;
    _ = client;
    Logger.INFO("Client: Disconnected", .{});
}

fn onGamePacket(client: *Client, payload: []const u8, context: ?*anyopaque) void {
    _ = client;
    _ = context;
    Logger.INFO("Payload received: {any}", .{payload});
}
