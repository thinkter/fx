const std = @import("std");
const io_mod = @import("../core/shared/io.zig");
const gateway_client = @import("client.zig");
const websocket_transport = @import("websocket_transport.zig");

const Allocator = std.mem.Allocator;
const pool_alloc = std.heap.c_allocator;

pub const health_budget: u8 = 3;
pub const default_max_connection_age_ms: i64 = 55 * 60 * 1000;
const default_max_lanes: usize = 4;
const max_connection_age_env = "FX_CODEX_WEBSOCKET_MAX_CONNECTION_AGE_MS";
const max_lanes_env = "FX_CODEX_WEBSOCKET_MAX_LANES";

const Slot = struct {
    session_id: []u8,
    account_id: []u8,
    model: []u8,
    endpoint: []u8,
    authorization_fingerprint: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    connection: ?*websocket_transport.Connection,
    busy: bool,
    health_failures: u8,
    opened_at_ms: i64,
    continuation_response_id: ?[]u8,
    continuation_baseline: ?[]u8,
    continuation_durable_baseline: ?[]u8,
    continuation_shape: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    continuation_valid: bool,

    fn clearContinuation(self: *Slot) void {
        if (self.continuation_response_id) |value| pool_alloc.free(value);
        if (self.continuation_baseline) |value| pool_alloc.free(value);
        if (self.continuation_durable_baseline) |value| pool_alloc.free(value);
        self.continuation_response_id = null;
        self.continuation_baseline = null;
        self.continuation_durable_baseline = null;
        self.continuation_valid = false;
    }

    fn deinit(self: *Slot) void {
        if (self.connection) |connection| websocket_transport.close(connection, pool_alloc);
        self.clearContinuation();
        pool_alloc.free(self.session_id);
        pool_alloc.free(self.account_id);
        pool_alloc.free(self.model);
        pool_alloc.free(self.endpoint);
        self.* = undefined;
    }
};

var pool_mutex: std.Io.Mutex = .init;
var slots: std.ArrayList(Slot) = .empty;

pub const AcquireArgs = struct {
    session_id: ?[]const u8,
    account_id: []const u8,
    model: []const u8,
    endpoint: []const u8,
    authorization: []const u8,
    deadline: ?std.Io.Clock.Timestamp,
    cancel_flag: *std.atomic.Value(bool),
    delivery: *gateway_client.DeliveryCertainty,
    continuation_input: ?[]const u8 = null,
    continuation_shape: ?[std.crypto.hash.sha2.Sha256.digest_length]u8 = null,
};

pub const Checkout = struct {
    slot: usize,
    connection: *websocket_transport.Connection,
    reused: bool,
    handshake_ms: i64,
    health_failures: u8,
};

pub const Continuation = struct {
    previous_response_id: []const u8,
    delta_input: []const u8,
};

fn continuationDelta(full_input: []const u8, baseline: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, full_input, baseline)) return null;
    if (full_input.len == baseline.len) return "";
    if (full_input[baseline.len] != ',') return null;
    return full_input[baseline.len + 1 ..];
}

pub const Outcome = enum { completed, failed };

fn sessionKey(session_id: ?[]const u8) []const u8 {
    const value = session_id orelse return "";
    return if (value.len == 0) "" else value;
}

fn authorizationFingerprint(authorization: []const u8) [std.crypto.hash.sha2.Sha256.digest_length]u8 {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(authorization, &digest, .{});
    return digest;
}

fn matches(slot: *const Slot, args: AcquireArgs) bool {
    const fingerprint = authorizationFingerprint(args.authorization);
    return std.mem.eql(u8, slot.session_id, sessionKey(args.session_id)) and
        std.mem.eql(u8, slot.account_id, args.account_id) and
        std.mem.eql(u8, slot.model, args.model) and
        std.mem.eql(u8, slot.endpoint, args.endpoint) and
        std.mem.eql(u8, &slot.authorization_fingerprint, &fingerprint);
}

fn maxConnectionAgeMs() !i64 {
    const value = io_mod.getenv(max_connection_age_env) orelse return default_max_connection_age_ms;
    const parsed = std.fmt.parseInt(i64, value, 10) catch return error.InvalidOpenAICodexTransport;
    if (parsed < 0) return error.InvalidOpenAICodexTransport;
    return parsed;
}

fn maxLanes() !usize {
    const value = io_mod.getenv(max_lanes_env) orelse return default_max_lanes;
    const parsed = std.fmt.parseInt(usize, value, 10) catch return error.InvalidOpenAICodexTransport;
    if (parsed == 0) return error.InvalidOpenAICodexTransport;
    return parsed;
}

fn incompatibleIdentity(slot: *const Slot, args: AcquireArgs) bool {
    const fingerprint = authorizationFingerprint(args.authorization);
    return std.mem.eql(u8, slot.session_id, sessionKey(args.session_id)) and
        (!std.mem.eql(u8, slot.account_id, args.account_id) or
            !std.mem.eql(u8, slot.model, args.model) or
            !std.mem.eql(u8, slot.endpoint, args.endpoint) or
            !std.mem.eql(u8, &slot.authorization_fingerprint, &fingerprint));
}

fn appendSlot(args: AcquireArgs) !usize {
    const session_id = try pool_alloc.dupe(u8, sessionKey(args.session_id));
    errdefer pool_alloc.free(session_id);
    const account_id = try pool_alloc.dupe(u8, args.account_id);
    errdefer pool_alloc.free(account_id);
    const model = try pool_alloc.dupe(u8, args.model);
    errdefer pool_alloc.free(model);
    const endpoint = try pool_alloc.dupe(u8, args.endpoint);
    errdefer pool_alloc.free(endpoint);
    try slots.append(pool_alloc, .{
        .session_id = session_id,
        .account_id = account_id,
        .model = model,
        .endpoint = endpoint,
        .authorization_fingerprint = authorizationFingerprint(args.authorization),
        .connection = null,
        .busy = false,
        .health_failures = 0,
        .opened_at_ms = 0,
        .continuation_response_id = null,
        .continuation_baseline = null,
        .continuation_durable_baseline = null,
        .continuation_shape = undefined,
        .continuation_valid = false,
    });
    return slots.items.len - 1;
}

fn continuationMatches(slot: *const Slot, full_input: []const u8, shape: [std.crypto.hash.sha2.Sha256.digest_length]u8) bool {
    if (!slot.continuation_valid or !std.mem.eql(u8, &slot.continuation_shape, &shape)) return false;
    if (slot.continuation_baseline) |baseline| {
        if (continuationDelta(full_input, baseline) != null) return true;
    }
    if (slot.continuation_durable_baseline) |baseline| {
        if (continuationDelta(full_input, baseline) != null) return true;
    }
    return false;
}

const LaneSelection = struct {
    index: ?usize,
    matching_count: usize,
};

fn selectIdleLane(slot_items: []Slot, args: AcquireArgs) LaneSelection {
    var first_idle: ?usize = null;
    var continuation_idle: ?usize = null;
    var matching_count: usize = 0;
    for (slot_items, 0..) |*slot, index| {
        if (!matches(slot, args)) continue;
        matching_count += 1;
        if (slot.busy) continue;
        if (first_idle == null) first_idle = index;
        if (args.continuation_input) |full_input| {
            if (args.continuation_shape) |shape| {
                if (continuationMatches(slot, full_input, shape)) {
                    continuation_idle = index;
                    break;
                }
            }
        }
    }
    return .{ .index = continuation_idle orelse first_idle, .matching_count = matching_count };
}

const LaneChoice = union(enum) {
    existing: usize,
    append,
    wait,
};

fn chooseLane(slot_items: []Slot, args: AcquireArgs, lane_limit: usize) LaneChoice {
    const selection = selectIdleLane(slot_items, args);
    if (selection.index) |index| return .{ .existing = index };
    if (selection.matching_count < lane_limit) return .append;
    return .wait;
}

fn incrementFailure(slot: *Slot) void {
    slot.health_failures = std.math.add(u8, slot.health_failures, 1) catch std.math.maxInt(u8);
}

pub fn acquire(_: Allocator, args: AcquireArgs) !Checkout {
    while (true) {
        if (args.cancel_flag.load(.seq_cst)) return error.Cancelled;
        if (args.deadline) |deadline| {
            const now = std.Io.Clock.Timestamp.now(io_mod.getIo(), .awake);
            if (!std.Io.Clock.Timestamp.compare(now, .lt, deadline)) return error.Timeout;
        }
        pool_mutex.lockUncancelable(io_mod.getIo());
        var locked = true;
        errdefer if (locked) pool_mutex.unlock(io_mod.getIo());
        for (slots.items) |*existing| {
            if (!incompatibleIdentity(existing, args) or existing.busy) continue;
            if (existing.connection) |connection| websocket_transport.close(connection, pool_alloc);
            existing.connection = null;
            existing.clearContinuation();
        }
        // Responses on one WebSocket are exclusive and ordered. Preserve a
        // compatible continuation lane when it is idle; otherwise another
        // retained socket provides bounded parallelism without multiplexing
        // unrelated response events on the same wire.
        const index = switch (chooseLane(slots.items, args, try maxLanes())) {
            .existing => |existing| existing,
            .append => try appendSlot(args),
            .wait => {
                pool_mutex.unlock(io_mod.getIo());
                locked = false;
                io_mod.sleep(10 * std.time.ns_per_ms);
                continue;
            },
        };
        const slot = &slots.items[index];

        const age_limit = try maxConnectionAgeMs();
        if (slot.connection != null and slot.health_failures >= health_budget) {
            websocket_transport.close(slot.connection.?, pool_alloc);
            slot.connection = null;
            slot.clearContinuation();
        }
        if (slot.connection != null and age_limit != 0 and io_mod.milliTimestamp() - slot.opened_at_ms > age_limit) {
            websocket_transport.close(slot.connection.?, pool_alloc);
            slot.connection = null;
            slot.clearContinuation();
        }
        if (slot.connection) |connection| {
            const ping_result = websocket_transport.ping(connection, args.cancel_flag, args.deadline, args.delivery);
            if (ping_result) |_| {
                slot.busy = true;
                const checkout = Checkout{
                    .slot = index,
                    .connection = connection,
                    .reused = true,
                    .handshake_ms = 0,
                    .health_failures = slot.health_failures,
                };
                pool_mutex.unlock(io_mod.getIo());
                return checkout;
            } else |err| {
                websocket_transport.close(connection, pool_alloc);
                slot.connection = null;
                slot.clearContinuation();
                incrementFailure(slot);
                if (err == error.Cancelled) return err;
            }
        }

        const started_at_ms = io_mod.milliTimestamp();
        const connection = websocket_transport.connect(pool_alloc, .{
            .endpoint = args.endpoint,
            .authorization = args.authorization,
            .account_id = args.account_id,
            .session_id = args.session_id,
            .deadline = args.deadline,
            .cancel_flag = args.cancel_flag,
            .delivery = args.delivery,
        }) catch |err| return err;
        slot.connection = connection;
        slot.clearContinuation();
        slot.opened_at_ms = connection.opened_at_ms;
        slot.busy = true;
        const checkout = Checkout{
            .slot = index,
            .connection = connection,
            .reused = false,
            .handshake_ms = @max(io_mod.milliTimestamp() - started_at_ms, 0),
            .health_failures = slot.health_failures,
        };
        pool_mutex.unlock(io_mod.getIo());
        return checkout;
    }
}

pub fn continuation(
    index: usize,
    full_input: []const u8,
    shape: [std.crypto.hash.sha2.Sha256.digest_length]u8,
) ?Continuation {
    pool_mutex.lockUncancelable(io_mod.getIo());
    defer pool_mutex.unlock(io_mod.getIo());
    if (index >= slots.items.len) return null;
    const slot = &slots.items[index];
    if (!slot.busy or !slot.continuation_valid) return null;
    if (!std.mem.eql(u8, &slot.continuation_shape, &shape)) {
        slot.clearContinuation();
        return null;
    }
    const response_id = slot.continuation_response_id orelse return null;
    const baseline = slot.continuation_baseline orelse return null;
    const delta = continuationDelta(full_input, baseline) orelse durable: {
        const durable_baseline = slot.continuation_durable_baseline orelse {
            slot.clearContinuation();
            return null;
        };
        break :durable continuationDelta(full_input, durable_baseline) orelse {
            slot.clearContinuation();
            return null;
        };
    };
    return .{
        .previous_response_id = response_id,
        .delta_input = delta,
    };
}

pub fn recordCompletion(
    index: usize,
    response_id: []const u8,
    baseline: []const u8,
    durable_baseline: []const u8,
    shape: [std.crypto.hash.sha2.Sha256.digest_length]u8,
) void {
    pool_mutex.lockUncancelable(io_mod.getIo());
    defer pool_mutex.unlock(io_mod.getIo());
    if (index >= slots.items.len) return;
    const slot = &slots.items[index];
    slot.clearContinuation();
    const owned_id = pool_alloc.dupe(u8, response_id) catch return;
    const owned_baseline = pool_alloc.dupe(u8, baseline) catch {
        pool_alloc.free(owned_id);
        return;
    };
    const owned_durable_baseline = pool_alloc.dupe(u8, durable_baseline) catch {
        pool_alloc.free(owned_id);
        pool_alloc.free(owned_baseline);
        return;
    };
    slot.continuation_response_id = owned_id;
    slot.continuation_baseline = owned_baseline;
    slot.continuation_durable_baseline = owned_durable_baseline;
    slot.continuation_shape = shape;
    slot.continuation_valid = true;
}

pub fn release(index: usize, outcome: Outcome) void {
    pool_mutex.lockUncancelable(io_mod.getIo());
    defer pool_mutex.unlock(io_mod.getIo());
    if (index >= slots.items.len) return;
    const slot = &slots.items[index];
    slot.busy = false;
    switch (outcome) {
        .completed => slot.health_failures = 0,
        .failed => {
            incrementFailure(slot);
            if (slot.connection) |connection| websocket_transport.close(connection, pool_alloc);
            slot.connection = null;
            slot.clearContinuation();
        },
    }
}

pub fn shutdown() void {
    pool_mutex.lockUncancelable(io_mod.getIo());
    defer pool_mutex.unlock(io_mod.getIo());
    for (slots.items) |*slot| slot.deinit();
    slots.deinit(pool_alloc);
    slots = .empty;
}

test "retained Codex WebSocket identity includes authorization without storing it" {
    const slot = Slot{
        .session_id = @constCast("session-a"),
        .account_id = @constCast("account-a"),
        .model = @constCast("gpt-5.6-sol"),
        .endpoint = @constCast("http://127.0.0.1/responses"),
        .authorization_fingerprint = authorizationFingerprint("Bearer token-a"),
        .connection = null,
        .busy = false,
        .health_failures = 0,
        .opened_at_ms = 0,
        .continuation_response_id = null,
        .continuation_baseline = null,
        .continuation_durable_baseline = null,
        .continuation_shape = undefined,
        .continuation_valid = false,
    };
    const base = AcquireArgs{
        .session_id = "session-a",
        .account_id = "account-a",
        .model = "gpt-5.6-sol",
        .endpoint = "http://127.0.0.1/responses",
        .authorization = "Bearer token-a",
        .deadline = null,
        .cancel_flag = undefined,
        .delivery = undefined,
    };

    try std.testing.expect(matches(&slot, base));
    try std.testing.expect(!incompatibleIdentity(&slot, base));

    var rotated = base;
    rotated.authorization = "Bearer token-b";
    try std.testing.expect(!matches(&slot, rotated));
    try std.testing.expect(incompatibleIdentity(&slot, rotated));

    var changed_model = base;
    changed_model.model = "gpt-5.4";
    try std.testing.expect(!matches(&slot, changed_model));
    try std.testing.expect(incompatibleIdentity(&slot, changed_model));
}

test "Codex WebSocket continuation requires an exact item boundary prefix" {
    try std.testing.expectEqualStrings(
        "{\"role\":\"user\",\"content\":[]}",
        continuationDelta(
            "{\"type\":\"message\"},{\"role\":\"user\",\"content\":[]}",
            "{\"type\":\"message\"}",
        ).?,
    );
    try std.testing.expectEqualStrings(
        "",
        continuationDelta("{\"type\":\"message\"}", "{\"type\":\"message\"}").?,
    );
    try std.testing.expect(continuationDelta("{\"type\":\"message\"}suffix", "{\"type\":\"message\"}") == null);
    try std.testing.expect(continuationDelta("{\"type\":\"other\"}", "{\"type\":\"message\"}") == null);
}

test "Codex WebSocket lane selection preserves continuation affinity" {
    shutdown();
    defer shutdown();

    var cancel_flag = std.atomic.Value(bool).init(false);
    var delivery = gateway_client.DeliveryCertainty.init();
    var shape: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("shape", &shape, .{});
    const args = AcquireArgs{
        .session_id = "session-a",
        .account_id = "account-a",
        .model = "gpt-5.6-sol",
        .endpoint = "http://127.0.0.1/responses",
        .authorization = "Bearer token-a",
        .deadline = null,
        .cancel_flag = &cancel_flag,
        .delivery = &delivery,
        .continuation_input = "{\"type\":\"message\"},{\"role\":\"user\"}",
        .continuation_shape = shape,
    };

    const first = try appendSlot(args);
    slots.items[first].busy = true;
    const second = try appendSlot(args);
    slots.items[second].busy = true;
    recordCompletion(second, "response-2", "{\"type\":\"message\"}", "{\"type\":\"message\"}", shape);
    slots.items[second].busy = false;

    const selection = chooseLane(slots.items, args, 2);
    try std.testing.expectEqual(second, selection.existing);

    slots.items[second].busy = true;
    try std.testing.expect(chooseLane(slots.items, args, 2) == .wait);
    try std.testing.expect(chooseLane(slots.items, args, 3) == .append);
}
