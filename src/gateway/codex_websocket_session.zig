const std = @import("std");
const io_mod = @import("../core/shared/io.zig");
const gateway_client = @import("client.zig");
const websocket_transport = @import("websocket_transport.zig");

const Allocator = std.mem.Allocator;
const pool_alloc = std.heap.c_allocator;

pub const health_budget: u8 = 3;
pub const default_max_connection_age_ms: i64 = 55 * 60 * 1000;
const max_connection_age_env = "FX_CODEX_WEBSOCKET_MAX_CONNECTION_AGE_MS";

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

    fn deinit(self: *Slot) void {
        if (self.connection) |connection| websocket_transport.close(connection, pool_alloc);
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
};

pub const Checkout = struct {
    slot: usize,
    connection: *websocket_transport.Connection,
    reused: bool,
    handshake_ms: i64,
    health_failures: u8,
};

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
    });
    return slots.items.len - 1;
}

fn findSlot(args: AcquireArgs) ?usize {
    for (slots.items, 0..) |*slot, index| if (matches(slot, args)) return index;
    return null;
}

fn incrementFailure(slot: *Slot) void {
    slot.health_failures = std.math.add(u8, slot.health_failures, 1) catch std.math.maxInt(u8);
}

pub fn acquire(_: Allocator, args: AcquireArgs) !Checkout {
    while (true) {
        if (args.cancel_flag.load(.seq_cst)) return error.Cancelled;
        pool_mutex.lockUncancelable(io_mod.getIo());
        var locked = true;
        errdefer if (locked) pool_mutex.unlock(io_mod.getIo());
        for (slots.items) |*existing| {
            if (!incompatibleIdentity(existing, args) or existing.busy) continue;
            if (existing.connection) |connection| websocket_transport.close(connection, pool_alloc);
            existing.connection = null;
        }
        const index = findSlot(args) orelse try appendSlot(args);
        const slot = &slots.items[index];
        if (slot.busy) {
            pool_mutex.unlock(io_mod.getIo());
            locked = false;
            io_mod.sleep(10 * std.time.ns_per_ms);
            continue;
        }

        const age_limit = try maxConnectionAgeMs();
        if (slot.connection != null and slot.health_failures >= health_budget) {
            websocket_transport.close(slot.connection.?, pool_alloc);
            slot.connection = null;
        }
        if (slot.connection != null and age_limit != 0 and io_mod.milliTimestamp() - slot.opened_at_ms > age_limit) {
            websocket_transport.close(slot.connection.?, pool_alloc);
            slot.connection = null;
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
