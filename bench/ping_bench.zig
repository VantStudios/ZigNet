const std = @import("std");
const net = std.Io.net;
const Timestamp = std.Io.Timestamp;
const Duration = std.Io.Duration;
const Thread = std.Thread;

const Raknet = @import("Raknet");

const Proto = Raknet.Proto;
const Logger = Raknet.Logger;

const TARGET_HOST = "127.0.0.1";
const TARGET_PORT: u16 = 19132;

const LATENCY_SAMPLE_COUNT: usize = 500;
const LATENCY_WARMUP_COUNT: usize = 20;
const LATENCY_TIMEOUT_MS: u64 = 500;
const LATENCY_PROGRESS_EVERY: usize = 50;

const STRESS_LEVELS = [_]usize{ 100, 500, 1000, 2000, 5000, 10000 };
const STRESS_PINGS_PER_SEC_PER_SENDER: u64 = 20;
const STRESS_DURATION_SECONDS: u64 = 5;
const STRESS_DRAIN_MS: u64 = 300;
const STRESS_LOSS_ABORT_PCT: f64 = 5.0;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = std.heap.page_allocator;

    Logger.init(allocator);
    Logger.INFO("=== ZigNet RakNet Benchmark ===", .{});
    Logger.INFO("Target: {s}:{d}", .{ TARGET_HOST, TARGET_PORT });

    try runLatencyBenchmark(io, allocator);
    Logger.INFO("", .{});
    try runThroughputBenchmark(io, allocator);
}

fn makeSocket(io: std.Io) !net.Socket {
    const bind_addr = try net.IpAddress.parseIp4("0.0.0.0", 0); // ephemeral port
    return bind_addr.bind(io, .{
        .allow_broadcast = false,
        .mode = .dgram,
        .protocol = .udp,
    });
}

fn makeTimeout(io: std.Io, ms: u64) std.Io.Timeout {
    _ = io;
    return .{
        .duration = .{
            .raw = .fromNanoseconds(ms * std.time.ns_per_ms),
            .clock = .awake,
        },
    };
}

fn pingOnce(
    io: std.Io,
    sock: *const net.Socket,
    target: net.IpAddress,
    allocator: std.mem.Allocator,
    guid: i64,
    timeout_ms: u64,
) !?u64 {
    _ = allocator;
    var ping = Proto.UnconnectedPing.init(
        Timestamp.now(io, .real).toMilliseconds(),
        guid,
    );
    defer ping.deinit();

    var ping_buf: [Proto.UnconnectedPing.MAX_SERIALIZED_SIZE]u8 = undefined;
    const ping_data = try ping.serializeInto(&ping_buf);

    const start = Timestamp.now(io, .awake);
    try sock.send(io, &target, ping_data);

    var recv_buf: [2048]u8 = undefined;
    const timeout = makeTimeout(io, timeout_ms);

    const msg = sock.receiveTimeout(io, &recv_buf, timeout) catch |err| switch (err) {
        error.Timeout => return null,
        else => return err,
    };

    // Validate that this is an UnconnectedPong and not garbage/other traffic
    if (msg.data.len == 0 or msg.data[0] != Proto.Packets.UnconnectedPong) {
        return null;
    }

    const elapsed = start.untilNow(io, .awake);
    return @intCast(elapsed.nanoseconds);
}

fn runLatencyBenchmark(io: std.Io, allocator: std.mem.Allocator) !void {
    Logger.INFO("--- Latency benchmark ({d} samples) ---", .{LATENCY_SAMPLE_COUNT});

    var sock = try makeSocket(io);
    defer sock.close(io);

    const target = try net.IpAddress.parseIp4(TARGET_HOST, TARGET_PORT);

    var warmup_ok = false;
    var i: usize = 0;
    while (i < LATENCY_WARMUP_COUNT) : (i += 1) {
        if (try pingOnce(io, &sock, target, allocator, 1, LATENCY_TIMEOUT_MS)) |_| {
            warmup_ok = true;
        }
    }

    if (!warmup_ok) {
        Logger.ERROR("Warmup: no response from server at {s}:{d}. Is it running? Aborting.", .{ TARGET_HOST, TARGET_PORT });
        return;
    }

    var samples = try std.ArrayList(u64).initCapacity(allocator, LATENCY_SAMPLE_COUNT);
    defer samples.deinit(allocator);

    var timeouts: usize = 0;
    i = 0;
    while (i < LATENCY_SAMPLE_COUNT) : (i += 1) {
        const rtt = try pingOnce(io, &sock, target, allocator, @intCast(i), LATENCY_TIMEOUT_MS);
        if (rtt) |ns| {
            try samples.append(allocator, ns);
        } else {
            timeouts += 1;
        }

        if ((i + 1) % LATENCY_PROGRESS_EVERY == 0) {
            Logger.INFO("  progress: {d}/{d} ({d} timeouts so far)", .{ i + 1, LATENCY_SAMPLE_COUNT, timeouts });
        }
    }

    if (samples.items.len == 0) {
        Logger.ERROR("No responses received during the measurement.", .{});
        return;
    }

    std.mem.sort(u64, samples.items, {}, std.sort.asc(u64));

    const n = samples.items.len;
    const min = samples.items[0];
    const max = samples.items[n - 1];
    const p50 = samples.items[n * 50 / 100];
    const p95 = samples.items[@min(n - 1, n * 95 / 100)];
    const p99 = samples.items[@min(n - 1, n * 99 / 100)];

    var sum: u128 = 0;
    for (samples.items) |v| sum += v;
    const avg: u64 = @intCast(sum / n);

    Logger.INFO("Responses: {d}/{d} (timeouts: {d})", .{ n, LATENCY_SAMPLE_COUNT, timeouts });
    Logger.INFO("min: {d:.3}ms  avg: {d:.3}ms  p50: {d:.3}ms  p95: {d:.3}ms  p99: {d:.3}ms  max: {d:.3}ms", .{
        nsToMs(min), nsToMs(avg), nsToMs(p50), nsToMs(p95), nsToMs(p99), nsToMs(max),
    });
}

const StressResult = struct {
    sent: std.atomic.Value(u64) = .init(0),
    received: std.atomic.Value(u64) = .init(0),
};

/// Sends the same pre-serialized packet at a fixed rate (STRESS_PINGS_PER_SEC_PER_SENDER),
/// simulating a real client instead of an uncontrolled flood. Does not wait for individual responses.
fn stressSender(
    io: std.Io,
    sock: *const net.Socket,
    target: net.IpAddress,
    ping_data: []const u8,
    result: *StressResult,
    stop_flag: *std.atomic.Value(bool),
) void {
    const interval_ns: u64 = std.time.ns_per_s / STRESS_PINGS_PER_SEC_PER_SENDER;

    while (!stop_flag.load(.acquire)) {
        sock.send(io, &target, ping_data) catch continue;
        _ = result.sent.fetchAdd(1, .monotonic);

        io.sleep(Duration.fromNanoseconds(@intCast(interval_ns)), .awake) catch return;
    }
}

/// Single thread that drains every response on the shared socket and counts
/// how many valid UnconnectedPong packets arrive.
fn stressReceiver(
    io: std.Io,
    sock: *const net.Socket,
    result: *StressResult,
    stop_flag: *std.atomic.Value(bool),
) void {
    var recv_buf: [4096]u8 = undefined;
    while (!stop_flag.load(.acquire)) {
        const timeout = makeTimeout(io, 100); // short: polls stop_flag frequently
        const msg = sock.receiveTimeout(io, &recv_buf, timeout) catch |err| switch (err) {
            error.Timeout => continue,
            else => continue,
        };
        if (msg.data.len > 0 and msg.data[0] == Proto.Packets.UnconnectedPong) {
            _ = result.received.fetchAdd(1, .monotonic);
        }
    }
}

fn runStressLevel(io: std.Io, allocator: std.mem.Allocator, concurrency: usize) !struct { pps: f64, loss_pct: f64 } {
    Logger.INFO("--- Stress test: {d} concurrent senders, {d}s ---", .{ concurrency, STRESS_DURATION_SECONDS });

    var sock = try makeSocket(io);
    defer sock.close(io);

    const target = try net.IpAddress.parseIp4(TARGET_HOST, TARGET_PORT);

    // Serialize ONCE: we do not want to measure client-side allocation
    // cost, only the real work done by the server.
    var ping = Proto.UnconnectedPing.init(
        Timestamp.now(io, .real).toMilliseconds(),
        42,
    );
    defer ping.deinit();
    var ping_buf: [Proto.UnconnectedPing.MAX_SERIALIZED_SIZE]u8 = undefined;
    const ping_data = try ping.serializeInto(&ping_buf);

    var result: StressResult = .{};
    var stop_flag = std.atomic.Value(bool).init(false);

    var receiver_thread = try Thread.spawn(.{}, stressReceiver, .{ io, &sock, &result, &stop_flag });

    var sender_threads = try allocator.alloc(Thread, concurrency);
    defer allocator.free(sender_threads);

    for (0..concurrency) |idx| {
        sender_threads[idx] = try Thread.spawn(.{}, stressSender, .{ io, &sock, target, ping_data, &result, &stop_flag });
    }

    try io.sleep(Duration.fromSeconds(@intCast(STRESS_DURATION_SECONDS)), .awake);

    // Senders stop on their own once they see stop_flag set to true.
    stop_flag.store(true, .release);
    for (sender_threads) |t| t.join();

    // Open a short drain window: stop_flag was already set above, so use a
    // second flag for the receiver alone.
    var drain_flag = std.atomic.Value(bool).init(false);
    var drain_receiver = try Thread.spawn(.{}, stressReceiver, .{ io, &sock, &result, &drain_flag });
    try io.sleep(Duration.fromNanoseconds(STRESS_DRAIN_MS * std.time.ns_per_ms), .awake);
    drain_flag.store(true, .release);
    drain_receiver.join();
    receiver_thread.join();

    const sent = result.sent.load(.monotonic);
    const received = result.received.load(.monotonic);
    const pps = @as(f64, @floatFromInt(received)) / @as(f64, @floatFromInt(STRESS_DURATION_SECONDS));
    const loss_pct = if (sent > 0)
        (1.0 - @as(f64, @floatFromInt(received)) / @as(f64, @floatFromInt(sent))) * 100.0
    else
        0.0;

    Logger.INFO("Sent: {d}  Received: {d}  Actual throughput: {d:.1} pings/s  Loss: {d:.2}%", .{
        sent, received, pps, loss_pct,
    });

    return .{ .pps = pps, .loss_pct = loss_pct };
}

fn runThroughputBenchmark(io: std.Io, allocator: std.mem.Allocator) !void {
    Logger.INFO("--- Stress ramp (fire-and-forget) ---", .{});

    for (STRESS_LEVELS) |level| {
        const r = try runStressLevel(io, allocator, level);

        if (r.loss_pct > STRESS_LOSS_ABORT_PCT) {
            Logger.INFO(
                "Loss > {d:.0}% at {d} senders — server ceiling reached. Stopping ramp.",
                .{ STRESS_LOSS_ABORT_PCT, level },
            );
            return;
        }
    }

    Logger.INFO("Ramp complete without exceeding the loss threshold ({d:.0}%). The server held every tested level.", .{STRESS_LOSS_ABORT_PCT});
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_ms);
}
