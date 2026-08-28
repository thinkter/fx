const std = @import("std");
const io_mod = @import("../core/shared/io.zig");
const gateway_client = @import("client.zig");

const Allocator = std.mem.Allocator;

pub const max_frame_bytes: usize = 4 * 1024 * 1024;
pub const max_message_bytes: usize = 64 * 1024 * 1024;

pub const Error = error{
    WebSocketUpgradeRejected,
    WebSocketAcceptInvalid,
    WebSocketProtocolViolation,
    WebSocketUnexpectedBinary,
    WebSocketMessageTooLarge,
    WebSocketInvalidUtf8,
    WebSocketPolicyClosed,
    WebSocketClosedBeforeCompletion,
};

pub const EventHandler = *const fn (context: *anyopaque, json: []const u8) anyerror!bool;

const connect_timeout_ms: i64 = 30_000;
const event_idle_timeout_ms: i64 = 30_000;

pub const Request = struct {
    endpoint: []const u8,
    authorization: []const u8,
    account_id: []const u8,
    session_id: ?[]const u8,
    payload: []const u8,
    deadline: ?std.Io.Clock.Timestamp,
    cancel_flag: *std.atomic.Value(bool),
    delivery: *gateway_client.DeliveryCertainty,
};
const OpenedRequest = struct {
    request: ?std.http.Client.Request,

    pub fn deinit(self: *OpenedRequest, _: Allocator) void {
        if (self.request) |*request| request.deinit();
        self.request = null;
    }

    fn take(self: *OpenedRequest) std.http.Client.Request {
        const request = self.request.?;
        self.request = null;
        return request;
    }
};

const OpenWebSocketOperation = struct {
    client: *std.http.Client,
    uri: std.Uri,
    authorization: []const u8,
    headers: []const std.http.Header,

    pub fn run(self: *@This()) !OpenedRequest {
        return .{ .request = try self.client.request(.GET, self.uri, .{
            .headers = .{
                .authorization = .{ .override = self.authorization },
                .connection = .{ .override = "Upgrade" },
                .accept_encoding = .omit,
            },
            .extra_headers = self.headers,
            .keep_alive = false,
            .redirect_behavior = .unhandled,
        }) };
    }
};

/// Opens one socket, sends one request, and consumes one terminal response.
/// The caller owns delivery certainty: this function returns an error after a
/// frame write without retrying the request.
pub fn stream(
    alloc: Allocator,
    request: Request,
    context: *anyopaque,
    on_event: EventHandler,
) !void {
    if (request.cancel_flag.load(.seq_cst)) return error.Cancelled;
    const uri = try std.Uri.parse(request.endpoint);
    var nonce: [16]u8 = undefined;
    try io_mod.getIo().randomSecure(&nonce);
    var key_buffer: [std.base64.standard.Encoder.calcSize(nonce.len)]u8 = undefined;
    _ = std.base64.standard.Encoder.encode(&key_buffer, &nonce);
    var accept_buffer: [std.base64.standard.Encoder.calcSize(std.crypto.hash.Sha1.digest_length)]u8 = undefined;
    const expected_accept = websocketAccept(&key_buffer, &accept_buffer);

    var extra_headers: [7]std.http.Header = undefined;
    var count: usize = 0;
    extra_headers[count] = .{ .name = "chatgpt-account-id", .value = request.account_id };
    count += 1;
    extra_headers[count] = .{ .name = "originator", .value = "fx" };
    count += 1;
    extra_headers[count] = .{ .name = "OpenAI-Beta", .value = "responses_websockets=v2" };
    count += 1;
    extra_headers[count] = .{ .name = "Upgrade", .value = "websocket" };
    count += 1;
    extra_headers[count] = .{ .name = "Sec-WebSocket-Version", .value = "13" };
    count += 1;
    extra_headers[count] = .{ .name = "Sec-WebSocket-Key", .value = &key_buffer };
    count += 1;
    if (request.session_id) |session_id| if (session_id.len > 0) {
        extra_headers[count] = .{ .name = "session-id", .value = session_id };
        count += 1;
    };

    var client: std.http.Client = .{ .allocator = alloc, .io = io_mod.getIo() };
    defer client.deinit();
    var open_operation = OpenWebSocketOperation{
        .client = &client,
        .uri = uri,
        .authorization = request.authorization,
        .headers = extra_headers[0..count],
    };
    var connect_deadline = std.Io.Clock.Timestamp.fromNow(io_mod.getIo(), .{
        .clock = .awake,
        .raw = .fromMilliseconds(connect_timeout_ms),
    });
    if (request.deadline) |deadline| {
        if (std.Io.Clock.Timestamp.compare(deadline, .lt, connect_deadline)) {
            connect_deadline = deadline;
        }
    }
    var opened = try gateway_client.runBoundedHttpOperation(
        OpenedRequest,
        alloc,
        request.cancel_flag,
        connect_deadline,
        &open_operation,
    );
    var http_request = opened.take();
    defer {
        // An upgraded connection must never return to the HTTP pool.
        if (http_request.connection) |connection| connection.closing = true;
        http_request.deinit();
    }
    var watcher_done = std.atomic.Value(bool).init(false);
    var timeout_fired = std.atomic.Value(bool).init(false);
    var last_progress_ms = std.atomic.Value(i64).init(io_mod.milliTimestamp());
    const watcher = if (http_request.connection) |connection|
        try spawnConnectionWatcher(
            &watcher_done,
            request.cancel_flag,
            request.deadline,
            &timeout_fired,
            &last_progress_ms,
            connection.stream_writer.stream,
        )
    else
        null;
    defer {
        watcher_done.store(true, .seq_cst);
        if (watcher) |thread| thread.join();
    }
    http_request.sendBodiless() catch |err| {
        if (request.cancel_flag.load(.seq_cst)) return error.Cancelled;
        if (timeout_fired.load(.seq_cst)) return error.Timeout;
        return err;
    };
    const response = http_request.receiveHead(&.{}) catch |err| {
        if (request.cancel_flag.load(.seq_cst)) return error.Cancelled;
        if (timeout_fired.load(.seq_cst)) return error.Timeout;
        return err;
    };
    if (response.head.status != .switching_protocols) return error.WebSocketUpgradeRejected;
    if (!hasHeader(response.head, "upgrade", "websocket") or
        !hasTokenHeader(response.head, "connection", "upgrade") or
        !hasHeader(response.head, "sec-websocket-accept", expected_accept))
    {
        return error.WebSocketAcceptInvalid;
    }

    // `receiveHead` leaves any already-buffered WebSocket bytes on this reader.
    const reader = http_request.reader.in;
    const connection = http_request.connection orelse return error.WebSocketConnectionMissing;
    const writer = connection.writer();
    var close_sent = false;
    defer if (!close_sent and !request.cancel_flag.load(.seq_cst)) {
        // Cancellation force-releases the socket from the watcher. Do not race
        // that release with a best-effort close frame from this I/O owner.
        // A fresh Phase 1 socket is never returned to the HTTP pool.
        writeFrame(writer, .close, &.{ 0x03, 0xe8 }) catch {};
        connection.flush() catch {};
    };
    request.delivery.markPossiblySent();
    writeFrame(writer, .text, request.payload) catch |err| {
        if (request.cancel_flag.load(.seq_cst)) return error.Cancelled;
        if (timeout_fired.load(.seq_cst)) return error.Timeout;
        return err;
    };
    connection.flush() catch |err| {
        if (request.cancel_flag.load(.seq_cst)) return error.Cancelled;
        if (timeout_fired.load(.seq_cst)) return error.Timeout;
        return err;
    };
    last_progress_ms.store(io_mod.milliTimestamp(), .seq_cst);

    var message: std.ArrayList(u8) = .empty;
    defer message.deinit(alloc);
    var fragmented_opcode: ?Opcode = null;
    while (true) {
        if (request.cancel_flag.load(.seq_cst)) return error.Cancelled;
        const frame = readFrame(alloc, reader) catch |err| {
            if (request.cancel_flag.load(.seq_cst)) return error.Cancelled;
            if (timeout_fired.load(.seq_cst)) return error.Timeout;
            return err;
        };
        last_progress_ms.store(io_mod.milliTimestamp(), .seq_cst);
        defer alloc.free(frame.payload);
        switch (frame.opcode) {
            .ping => {
                writeFrame(writer, .pong, frame.payload) catch |err| {
                    if (request.cancel_flag.load(.seq_cst)) return error.Cancelled;
                    if (timeout_fired.load(.seq_cst)) return error.Timeout;
                    return err;
                };
                connection.flush() catch |err| {
                    if (request.cancel_flag.load(.seq_cst)) return error.Cancelled;
                    if (timeout_fired.load(.seq_cst)) return error.Timeout;
                    return err;
                };
            },
            .pong => {},
            .close => {
                return closeError(try validateClosePayload(frame.payload));
            },
            .binary => return error.WebSocketUnexpectedBinary,
            .continuation => {
                if (fragmented_opcode == null) return error.WebSocketProtocolViolation;
                try appendMessage(&message, alloc, frame.payload);
                if (!frame.fin) continue;
                const opcode = fragmented_opcode.?;
                fragmented_opcode = null;
                if (opcode != .text) return error.WebSocketUnexpectedBinary;
                if (try dispatchTextMessage(context, on_event, message.items)) {
                    closeAfterCompletion(alloc, reader, writer, connection, request.cancel_flag, &timeout_fired) catch |err| {
                        if (err == error.Cancelled or err == error.Timeout) close_sent = true;
                        return err;
                    };
                    close_sent = true;
                    return;
                }
                message.clearRetainingCapacity();
            },
            .text => {
                if (fragmented_opcode != null) return error.WebSocketProtocolViolation;
                try appendMessage(&message, alloc, frame.payload);
                if (!frame.fin) {
                    fragmented_opcode = .text;
                    continue;
                }
                if (try dispatchTextMessage(context, on_event, message.items)) {
                    closeAfterCompletion(alloc, reader, writer, connection, request.cancel_flag, &timeout_fired) catch |err| {
                        if (err == error.Cancelled or err == error.Timeout) close_sent = true;
                        return err;
                    };
                    close_sent = true;
                    return;
                }
                message.clearRetainingCapacity();
            },
        }
    }
}

const Opcode = enum(u4) { continuation = 0, text = 1, binary = 2, close = 8, ping = 9, pong = 10 };
const Frame = struct { fin: bool, opcode: Opcode, payload: []u8 };

fn websocketAccept(key: []const u8, output: []u8) []const u8 {
    var hash = std.crypto.hash.Sha1.init(.{});
    hash.update(key);
    hash.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    var digest: [std.crypto.hash.Sha1.digest_length]u8 = undefined;
    hash.final(&digest);
    _ = std.base64.standard.Encoder.encode(output, &digest);
    return output;
}

fn hasHeader(head: std.http.Client.Response.Head, name: []const u8, expected: []const u8) bool {
    var it = head.iterateHeaders();
    while (it.next()) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, name) and std.mem.eql(u8, header.value, expected)) return true;
    }
    return false;
}

fn hasTokenHeader(head: std.http.Client.Response.Head, name: []const u8, token: []const u8) bool {
    var it = head.iterateHeaders();
    while (it.next()) |header| {
        if (!std.ascii.eqlIgnoreCase(header.name, name)) continue;
        var tokens = std.mem.splitScalar(u8, header.value, ',');
        while (tokens.next()) |candidate| if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, candidate, " \t"), token)) return true;
    }
    return false;
}

fn appendMessage(message: *std.ArrayList(u8), alloc: Allocator, payload: []const u8) !void {
    if (payload.len > max_message_bytes -| message.items.len) return error.WebSocketMessageTooLarge;
    try message.appendSlice(alloc, payload);
}

fn dispatchTextMessage(context: *anyopaque, on_event: EventHandler, message: []const u8) !bool {
    if (!std.unicode.utf8ValidateSlice(message)) return error.WebSocketInvalidUtf8;
    return on_event(context, message);
}

fn closeAfterCompletion(
    alloc: Allocator,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    connection: anytype,
    cancel_flag: *std.atomic.Value(bool),
    timeout_fired: *std.atomic.Value(bool),
) !void {
    try writeFrame(writer, .close, &.{ 0x03, 0xe8 });
    try connection.flush();
    const frame = readFrame(alloc, reader) catch |err| {
        if (cancel_flag.load(.seq_cst)) return error.Cancelled;
        if (timeout_fired.load(.seq_cst)) return error.Timeout;
        return err;
    };
    defer alloc.free(frame.payload);
    switch (frame.opcode) {
        .close => _ = try validateClosePayload(frame.payload),
        else => return error.WebSocketProtocolViolation,
    }
}

fn closeError(code: ?u16) anyerror {
    if (code == 1008) return error.WebSocketPolicyClosed;
    return error.WebSocketClosedBeforeCompletion;
}

fn validateClosePayload(payload: []const u8) !?u16 {
    if (payload.len == 1) return error.WebSocketProtocolViolation;
    if (payload.len < 2) return null;
    const code = std.mem.readInt(u16, payload[0..2], .big);
    if (code < 1000 or code >= 5000 or code == 1004 or code == 1005 or code == 1006 or code == 1015) {
        return error.WebSocketProtocolViolation;
    }
    if (!std.unicode.utf8ValidateSlice(payload[2..])) return error.WebSocketInvalidUtf8;
    return code;
}

const ConnectionWatcher = struct {
    fn run(
        done: *std.atomic.Value(bool),
        cancel_flag: *std.atomic.Value(bool),
        deadline: ?std.Io.Clock.Timestamp,
        timeout_fired: *std.atomic.Value(bool),
        last_progress_ms: *std.atomic.Value(i64),
        socket: std.Io.net.Stream,
    ) void {
        while (!done.load(.seq_cst)) {
            if (cancel_flag.load(.seq_cst)) {
                socket.shutdown(io_mod.getIo(), .both) catch {};
                return;
            }
            if (deadline) |limit| {
                const now = std.Io.Clock.Timestamp.now(io_mod.getIo(), .awake);
                if (!std.Io.Clock.Timestamp.compare(now, .lt, limit)) {
                    timeout_fired.store(true, .seq_cst);
                    socket.shutdown(io_mod.getIo(), .both) catch {};
                    return;
                }
            }
            const elapsed_ms = io_mod.milliTimestamp() - last_progress_ms.load(.seq_cst);
            if (elapsed_ms >= event_idle_timeout_ms) {
                timeout_fired.store(true, .seq_cst);
                socket.shutdown(io_mod.getIo(), .both) catch {};
                return;
            }
            io_mod.sleep(10 * std.time.ns_per_ms);
        }
    }
};

fn spawnConnectionWatcher(
    done: *std.atomic.Value(bool),
    cancel_flag: *std.atomic.Value(bool),
    deadline: ?std.Io.Clock.Timestamp,
    timeout_fired: *std.atomic.Value(bool),
    last_progress_ms: *std.atomic.Value(i64),
    socket: std.Io.net.Stream,
) !std.Thread {
    return std.Thread.spawn(.{}, ConnectionWatcher.run, .{
        done,
        cancel_flag,
        deadline,
        timeout_fired,
        last_progress_ms,
        socket,
    });
}

fn writeFrame(writer: *std.Io.Writer, opcode: Opcode, payload: []const u8) !void {
    if (payload.len > max_message_bytes) return error.WebSocketMessageTooLarge;
    var mask: [4]u8 = undefined;
    try io_mod.getIo().randomSecure(&mask);
    try writer.writeByte(0x80 | @as(u8, @intFromEnum(opcode)));
    if (payload.len < 126) {
        try writer.writeByte(0x80 | @as(u8, @intCast(payload.len)));
    } else if (payload.len <= std.math.maxInt(u16)) {
        try writer.writeByte(0x80 | 126);
        try writer.writeInt(u16, @intCast(payload.len), .big);
    } else {
        try writer.writeByte(0x80 | 127);
        try writer.writeInt(u64, @intCast(payload.len), .big);
    }
    try writer.writeAll(&mask);
    var chunk: [4096]u8 = undefined;
    var offset: usize = 0;
    while (offset < payload.len) {
        const length = @min(chunk.len, payload.len - offset);
        for (payload[offset..][0..length], 0..) |byte, index| chunk[index] = byte ^ mask[(offset + index) % mask.len];
        try writer.writeAll(chunk[0..length]);
        offset += length;
    }
}

fn readFrame(alloc: Allocator, reader: *std.Io.Reader) !Frame {
    const first = try reader.takeByte();
    const second = try reader.takeByte();
    if (second & 0x80 != 0 or first & 0x70 != 0) return error.WebSocketProtocolViolation;
    const fin = first & 0x80 != 0;
    const opcode = std.enums.fromInt(Opcode, first & 0x0f) orelse return error.WebSocketProtocolViolation;
    var length: u64 = second & 0x7f;
    if (length == 126) length = try reader.takeInt(u16, .big) else if (length == 127) {
        length = try reader.takeInt(u64, .big);
        if (length & (@as(u64, 1) << 63) != 0) return error.WebSocketProtocolViolation;
    }
    if (length > max_frame_bytes) return error.WebSocketMessageTooLarge;
    if (@intFromEnum(opcode) >= @intFromEnum(Opcode.close) and (!fin or length > 125)) return error.WebSocketProtocolViolation;
    const payload = try alloc.alloc(u8, @intCast(length));
    errdefer alloc.free(payload);
    try reader.readSliceAll(payload);
    return .{ .fin = fin, .opcode = opcode, .payload = payload };
}

test "WebSocket accept matches RFC 6455" {
    var output: [28]u8 = undefined;
    try std.testing.expectEqualStrings("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", websocketAccept("dGhlIHNhbXBsZSBub25jZQ==", &output));
}

test "fragment aggregation limits message size" {
    var message: std.ArrayList(u8) = .empty;
    defer message.deinit(std.testing.allocator);
    try appendMessage(&message, std.testing.allocator, "hello");
    try std.testing.expectEqualStrings("hello", message.items);
}

test "extended 127-byte frame keeps the following frame aligned" {
    var encoded: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer encoded.deinit();
    try encoded.writer.writeAll(&.{ 0x81, 126, 0, 127 });
    try encoded.writer.splatByteAll('a', 127);
    try encoded.writer.writeAll(&.{ 0x81, 2, 'o', 'k' });

    var reader = std.Io.Reader.fixed(encoded.written());
    const first = try readFrame(std.testing.allocator, &reader);
    defer std.testing.allocator.free(first.payload);
    try std.testing.expectEqual(Opcode.text, first.opcode);
    try std.testing.expectEqual(@as(usize, 127), first.payload.len);

    const second = try readFrame(std.testing.allocator, &reader);
    defer std.testing.allocator.free(second.payload);
    try std.testing.expectEqual(Opcode.text, second.opcode);
    try std.testing.expectEqualStrings("ok", second.payload);
}

test "text messages reject malformed UTF-8 before event dispatch" {
    const Handler = struct {
        fn handle(_: *anyopaque, _: []const u8) !bool {
            return false;
        }
    };
    var context: u8 = 0;
    try std.testing.expectError(
        error.WebSocketInvalidUtf8,
        dispatchTextMessage(@ptrCast(&context), Handler.handle, &.{ 0xc3, 0x28 }),
    );
}

test "close payload rejects reserved codes and malformed UTF-8 reasons" {
    try std.testing.expectError(error.WebSocketProtocolViolation, validateClosePayload(&.{ 0x03, 0xed }));
    try std.testing.expectError(error.WebSocketInvalidUtf8, validateClosePayload(&.{ 0x03, 0xe8, 0xc3, 0x28 }));
    try std.testing.expectEqual(@as(?u16, 1000), try validateClosePayload(&.{ 0x03, 0xe8, 'o', 'k' }));
    try std.testing.expectEqual(error.WebSocketPolicyClosed, closeError(1008));
    try std.testing.expectEqual(error.WebSocketClosedBeforeCompletion, closeError(1000));
}

const StalledWriteFixture = struct {
    io_backend: std.Io.Threaded = .init_single_threaded,
    server: std.Io.net.Server,
    thread: ?std.Thread = null,
    stopping: std.atomic.Value(bool) = .init(false),
    upgraded: std.atomic.Value(bool) = .init(false),
    failure: ?anyerror = null,

    fn init() !@This() {
        var fixture: @This() = .{ .server = undefined };
        var address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
        fixture.server = try address.listen(fixture.io(), .{ .reuse_address = true });
        return fixture;
    }

    fn io(self: *@This()) std.Io {
        return self.io_backend.io();
    }

    fn endpoint(self: *@This(), buffer: []u8) ![]const u8 {
        return std.fmt.bufPrint(buffer, "http://127.0.0.1:{d}/responses", .{self.server.socket.address.getPort()});
    }

    fn start(self: *@This()) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    fn deinit(self: *@This()) void {
        self.stopping.store(true, .seq_cst);
        if (self.thread) |thread| {
            const listener = std.Io.net.Stream{ .socket = self.server.socket };
            listener.shutdown(self.io(), .both) catch {};
            thread.join();
            self.thread = null;
        }
        self.server.deinit(self.io());
    }

    fn run(self: *@This()) void {
        self.runFallible() catch |err| {
            if (!self.stopping.load(.seq_cst)) self.failure = err;
        };
    }

    fn runFallible(self: *@This()) !void {
        const zio = self.io();
        var client_stream = try self.server.accept(zio);
        defer client_stream.close(zio);
        if (self.stopping.load(.seq_cst)) return;
        const receive_buffer: c_int = 1024;
        std.posix.setsockopt(client_stream.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVBUF, std.mem.asBytes(&receive_buffer)) catch {};

        var socket_buffer: [4096]u8 = undefined;
        var reader = client_stream.reader(zio, &socket_buffer);
        var request: [16 * 1024]u8 = undefined;
        var request_len: usize = 0;
        while (request_len < request.len) {
            request[request_len] = try reader.interface.takeByte();
            request_len += 1;
            if (std.mem.endsWith(u8, request[0..request_len], "\r\n\r\n")) break;
        } else return error.TestRequestTooLarge;
        const key = headerValue(request[0 .. request_len - 4], "sec-websocket-key") orelse return error.TestMissingWebSocketKey;
        var accept_buffer: [28]u8 = undefined;
        const accept = websocketAccept(key, &accept_buffer);
        var write_buffer: [4096]u8 = undefined;
        var writer = client_stream.writer(zio, &write_buffer);
        try writer.interface.print(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {s}\r\n\r\n",
            .{accept},
        );
        try writer.interface.flush();
        self.upgraded.store(true, .seq_cst);
        while (!self.stopping.load(.seq_cst)) {
            var sleep_io: std.Io.Threaded = .init_single_threaded;
            sleep_io.io().sleep(.fromMilliseconds(1), .real) catch {};
        }
    }
};
const LoopbackMode = enum {
    never_accept,
    hang_after_upgrade,
    reset_after_upgrade,
    complete_then_hang_close,
    binary_then_close,
    ping_then_complete,
    oversized_frame,
};

const LoopbackWebSocketFixture = struct {
    io_backend: std.Io.Threaded = .init_single_threaded,
    server: std.Io.net.Server,
    mode: LoopbackMode,
    thread: ?std.Thread = null,
    stopping: std.atomic.Value(bool) = .init(false),
    upgraded: std.atomic.Value(bool) = .init(false),
    failure: ?anyerror = null,

    fn init(mode: LoopbackMode) !@This() {
        var fixture: @This() = .{ .server = undefined, .mode = mode };
        var address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
        fixture.server = try address.listen(fixture.io(), .{ .reuse_address = true });
        return fixture;
    }

    fn io(self: *@This()) std.Io {
        return self.io_backend.io();
    }

    fn endpoint(self: *@This(), buffer: []u8) ![]const u8 {
        return std.fmt.bufPrint(buffer, "http://127.0.0.1:{d}/responses", .{self.server.socket.address.getPort()});
    }

    fn start(self: *@This()) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    fn deinit(self: *@This()) void {
        self.stopping.store(true, .seq_cst);
        if (self.thread) |thread| {
            const listener = std.Io.net.Stream{ .socket = self.server.socket };
            listener.shutdown(self.io(), .both) catch {};
            thread.join();
            self.thread = null;
        }
        self.server.deinit(self.io());
    }

    fn hold(self: *@This()) void {
        while (!self.stopping.load(.seq_cst)) {
            self.io().sleep(.fromMilliseconds(1), .real) catch {};
        }
    }

    fn run(self: *@This()) void {
        self.runFallible() catch |err| {
            if (!self.stopping.load(.seq_cst)) self.failure = err;
        };
    }

    fn runFallible(self: *@This()) !void {
        if (self.mode == .never_accept) return self.hold();
        const zio = self.io();
        var client_stream = try self.server.accept(zio);
        defer client_stream.close(zio);
        if (self.stopping.load(.seq_cst)) return;

        var socket_buffer: [4096]u8 = undefined;
        var reader = client_stream.reader(zio, &socket_buffer);
        var request: [16 * 1024]u8 = undefined;
        var request_len: usize = 0;
        while (request_len < request.len) {
            request[request_len] = try reader.interface.takeByte();
            request_len += 1;
            if (std.mem.endsWith(u8, request[0..request_len], "\r\n\r\n")) break;
        } else return error.TestRequestTooLarge;
        const key = headerValue(request[0 .. request_len - 4], "sec-websocket-key") orelse return error.TestMissingWebSocketKey;
        var accept_buffer: [28]u8 = undefined;
        const accept = websocketAccept(key, &accept_buffer);
        var write_buffer: [4096]u8 = undefined;
        var writer = client_stream.writer(zio, &write_buffer);
        try writer.interface.print(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {s}\r\n\r\n",
            .{accept},
        );
        try writer.interface.flush();
        self.upgraded.store(true, .seq_cst);

        if (self.mode == .reset_after_upgrade) {
            const reset_on_close: std.posix.linger = .{ .onoff = 1, .linger = 0 };
            try std.posix.setsockopt(
                client_stream.socket.handle,
                std.posix.SOL.SOCKET,
                std.posix.SO.LINGER,
                std.mem.asBytes(&reset_on_close),
            );
            return;
        }
        if (self.mode == .hang_after_upgrade) return self.hold();

        try discardClientFrame(&reader.interface);
        switch (self.mode) {
            .complete_then_hang_close => {
                try writeServerFrame(&writer.interface, .text, "{\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}");
                try writeServerFrame(&writer.interface, .text, "{\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}");
                try writer.interface.flush();
                self.hold();
            },
            .binary_then_close => {
                try writeServerFrame(&writer.interface, .binary, &.{0});
                try writer.interface.flush();
            },
            .ping_then_complete => {
                try writeServerFrame(&writer.interface, .ping, "hi");
                try writeServerFrame(&writer.interface, .text, "{\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}");
                try writeServerFrame(&writer.interface, .text, "{\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}");
                try writeServerFrame(&writer.interface, .close, &.{ 0x03, 0xe8 });
                try writer.interface.flush();
                try discardClientFrame(&reader.interface);
            },
            .oversized_frame => {
                try writer.interface.writeAll(&.{ 0x81, 127 });
                try writer.interface.writeInt(u64, max_frame_bytes + 1, .big);
                try writer.interface.flush();
            },
            else => unreachable,
        }
    }
};

fn writeServerFrame(writer: *std.Io.Writer, opcode: Opcode, payload: []const u8) !void {
    try writer.writeByte(0x80 | @as(u8, @intFromEnum(opcode)));
    if (payload.len < 126) {
        try writer.writeByte(@intCast(payload.len));
    } else if (payload.len <= std.math.maxInt(u16)) {
        try writer.writeByte(126);
        try writer.writeInt(u16, @intCast(payload.len), .big);
    } else {
        try writer.writeByte(127);
        try writer.writeInt(u64, @intCast(payload.len), .big);
    }
    try writer.writeAll(payload);
}

fn discardClientFrame(reader: *std.Io.Reader) !void {
    _ = try reader.takeByte();
    const second = try reader.takeByte();
    if (second & 0x80 == 0) return error.WebSocketProtocolViolation;
    var length: u64 = second & 0x7f;
    if (length == 126) length = try reader.takeInt(u16, .big) else if (length == 127) length = try reader.takeInt(u64, .big);
    var mask: [4]u8 = undefined;
    try reader.readSliceAll(&mask);
    var discarded: [4096]u8 = undefined;
    var remaining = length;
    while (remaining > 0) {
        const chunk_len: usize = @intCast(@min(remaining, discarded.len));
        try reader.readSliceAll(discarded[0..chunk_len]);
        remaining -= chunk_len;
    }
}

fn headerValue(headers: []const u8, name: []const u8) ?[]const u8 {
    var lines = std.mem.splitSequence(u8, headers, "\r\n");
    _ = lines.next();
    while (lines.next()) |line| {
        const colon = std.mem.findScalar(u8, line, ':') orelse continue;
        if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, line[0..colon], " \t"), name)) {
            return std.mem.trim(u8, line[colon + 1 ..], " \t");
        }
    }
    return null;
}

test "WebSocket cancellation interrupts a backpressured response.create write" {
    var fixture = try StalledWriteFixture.init();
    defer fixture.deinit();
    try fixture.start();

    var endpoint_buffer: [128]u8 = undefined;
    const payload = try std.testing.allocator.alloc(u8, 4 * 1024 * 1024);
    defer std.testing.allocator.free(payload);
    @memset(payload, 'x');
    var cancelled = std.atomic.Value(bool).init(false);
    var delivery = gateway_client.DeliveryCertainty.init();
    const Canceller = struct {
        fn run(server: *StalledWriteFixture, flag: *std.atomic.Value(bool)) void {
            while (!server.upgraded.load(.seq_cst)) {
                var sleep_io: std.Io.Threaded = .init_single_threaded;
                sleep_io.io().sleep(.fromMilliseconds(1), .real) catch {};
            }
            flag.store(true, .seq_cst);
        }
    };
    const canceller = try std.Thread.spawn(.{}, Canceller.run, .{ &fixture, &cancelled });
    defer canceller.join();
    const result = stream(std.testing.allocator, .{
        .endpoint = try fixture.endpoint(&endpoint_buffer),
        .authorization = "Bearer test",
        .account_id = "test",
        .session_id = null,
        .payload = payload,
        .deadline = null,
        .cancel_flag = &cancelled,
        .delivery = &delivery,
    }, @ptrCast(&cancelled), struct {
        fn ignore(_: *anyopaque, _: []const u8) !bool {
            return false;
        }
    }.ignore);
    try std.testing.expectError(error.Cancelled, result);
    try std.testing.expectEqual(gateway_client.DeliveryCertainty.State.possibly_sent, delivery.load());
    if (fixture.failure) |err| return err;
}

test "WebSocket cancellation interrupts a stalled connect" {
    var cancelled = std.atomic.Value(bool).init(false);
    var delivery = gateway_client.DeliveryCertainty.init();
    const Canceller = struct {
        fn run(flag: *std.atomic.Value(bool)) void {
            io_mod.sleep(20 * std.time.ns_per_ms);
            flag.store(true, .seq_cst);
        }
    };
    const canceller = try std.Thread.spawn(.{}, Canceller.run, .{&cancelled});
    const started = std.Io.Clock.Timestamp.now(io_mod.getIo(), .awake);
    const result = stream(std.testing.allocator, .{
        .endpoint = "http://192.0.2.1:9/responses",
        .authorization = "Bearer test",
        .account_id = "test",
        .session_id = null,
        .payload = "{}",
        .deadline = null,
        .cancel_flag = &cancelled,
        .delivery = &delivery,
    }, @ptrCast(&cancelled), struct {
        fn ignore(_: *anyopaque, _: []const u8) !bool {
            return false;
        }
    }.ignore);
    const elapsed_ms = started.durationTo(std.Io.Clock.Timestamp.now(io_mod.getIo(), .awake)).raw.toMilliseconds();
    canceller.join();
    if (result) |_| {
        return error.TestExpectedError;
    } else |err| {
        if (err == error.Cancelled) {
            try std.testing.expect(elapsed_ms < 2_000);
            try std.testing.expectEqual(gateway_client.DeliveryCertainty.State.definitely_unsent, delivery.load());
            return;
        }
        if (elapsed_ms >= 5) return err;
    }

    var fixture = try LoopbackWebSocketFixture.init(.never_accept);
    defer fixture.deinit();
    try fixture.start();
    var endpoint_buffer: [128]u8 = undefined;
    cancelled.store(false, .seq_cst);
    delivery = gateway_client.DeliveryCertainty.init();
    const fallback_canceller = try std.Thread.spawn(.{}, Canceller.run, .{&cancelled});
    const fallback_started = std.Io.Clock.Timestamp.now(io_mod.getIo(), .awake);
    const fallback_result = stream(std.testing.allocator, .{
        .endpoint = try fixture.endpoint(&endpoint_buffer),
        .authorization = "Bearer test",
        .account_id = "test",
        .session_id = null,
        .payload = "{}",
        .deadline = null,
        .cancel_flag = &cancelled,
        .delivery = &delivery,
    }, @ptrCast(&cancelled), struct {
        fn ignore(_: *anyopaque, _: []const u8) !bool {
            return false;
        }
    }.ignore);
    const fallback_elapsed_ms = fallback_started.durationTo(std.Io.Clock.Timestamp.now(io_mod.getIo(), .awake)).raw.toMilliseconds();
    fallback_canceller.join();
    try std.testing.expectError(error.Cancelled, fallback_result);
    try std.testing.expect(fallback_elapsed_ms < 2_000);
    try std.testing.expectEqual(gateway_client.DeliveryCertainty.State.definitely_unsent, delivery.load());
    if (fixture.failure) |err| return err;
}

test "WebSocket cancellation interrupts a hung close handshake" {
    var fixture = try LoopbackWebSocketFixture.init(.complete_then_hang_close);
    defer fixture.deinit();
    try fixture.start();
    var endpoint_buffer: [128]u8 = undefined;
    var cancelled = std.atomic.Value(bool).init(false);
    var delivery = gateway_client.DeliveryCertainty.init();
    const Canceller = struct {
        fn run(server: *LoopbackWebSocketFixture, flag: *std.atomic.Value(bool)) void {
            while (!server.upgraded.load(.seq_cst)) io_mod.sleep(std.time.ns_per_ms);
            io_mod.sleep(50 * std.time.ns_per_ms);
            flag.store(true, .seq_cst);
        }
    };
    const canceller = try std.Thread.spawn(.{}, Canceller.run, .{ &fixture, &cancelled });
    defer canceller.join();
    const result = stream(std.testing.allocator, .{
        .endpoint = try fixture.endpoint(&endpoint_buffer),
        .authorization = "Bearer test",
        .account_id = "test",
        .session_id = null,
        .payload = "{}",
        .deadline = null,
        .cancel_flag = &cancelled,
        .delivery = &delivery,
    }, @ptrCast(&cancelled), struct {
        fn completed(_: *anyopaque, json: []const u8) !bool {
            return std.mem.find(u8, json, "\"response.completed\"") != null;
        }
    }.completed);
    try std.testing.expectError(error.Cancelled, result);
    try std.testing.expectEqual(gateway_client.DeliveryCertainty.State.possibly_sent, delivery.load());
    if (fixture.failure) |err| return err;
}

test "WebSocket peer reset after upgrade leaves delivery possibly sent" {
    var fixture = try LoopbackWebSocketFixture.init(.reset_after_upgrade);
    defer fixture.deinit();
    try fixture.start();
    var endpoint_buffer: [128]u8 = undefined;
    const payload = try std.testing.allocator.alloc(u8, 4 * 1024);
    defer std.testing.allocator.free(payload);
    @memset(payload, 'x');
    var cancelled = std.atomic.Value(bool).init(false);
    var delivery = gateway_client.DeliveryCertainty.init();
    const result = stream(std.testing.allocator, .{
        .endpoint = try fixture.endpoint(&endpoint_buffer),
        .authorization = "Bearer test",
        .account_id = "test",
        .session_id = null,
        .payload = payload,
        .deadline = null,
        .cancel_flag = &cancelled,
        .delivery = &delivery,
    }, @ptrCast(&cancelled), struct {
        fn ignore(_: *anyopaque, _: []const u8) !bool {
            return false;
        }
    }.ignore);
    if (result) |_| return error.TestExpectedError else |err| try std.testing.expect(err != error.Cancelled);
    try std.testing.expectEqual(gateway_client.DeliveryCertainty.State.possibly_sent, delivery.load());
    if (fixture.failure) |err| return err;
}

test "WebSocket rejects unexpected binary frames" {
    var fixture = try LoopbackWebSocketFixture.init(.binary_then_close);
    defer fixture.deinit();
    try fixture.start();
    var endpoint_buffer: [128]u8 = undefined;
    var cancelled = std.atomic.Value(bool).init(false);
    var delivery = gateway_client.DeliveryCertainty.init();
    const result = stream(std.testing.allocator, .{
        .endpoint = try fixture.endpoint(&endpoint_buffer),
        .authorization = "Bearer test",
        .account_id = "test",
        .session_id = null,
        .payload = "{}",
        .deadline = null,
        .cancel_flag = &cancelled,
        .delivery = &delivery,
    }, @ptrCast(&cancelled), struct {
        fn ignore(_: *anyopaque, _: []const u8) !bool {
            return false;
        }
    }.ignore);
    try std.testing.expectError(error.WebSocketUnexpectedBinary, result);
    try std.testing.expectEqual(gateway_client.DeliveryCertainty.State.possibly_sent, delivery.load());
    if (fixture.failure) |err| return err;
}

test "WebSocket answers ping then completes" {
    var fixture = try LoopbackWebSocketFixture.init(.ping_then_complete);
    defer fixture.deinit();
    try fixture.start();
    var endpoint_buffer: [128]u8 = undefined;
    var cancelled = std.atomic.Value(bool).init(false);
    var delivery = gateway_client.DeliveryCertainty.init();
    var completed = false;
    try stream(std.testing.allocator, .{
        .endpoint = try fixture.endpoint(&endpoint_buffer),
        .authorization = "Bearer test",
        .account_id = "test",
        .session_id = null,
        .payload = "{}",
        .deadline = null,
        .cancel_flag = &cancelled,
        .delivery = &delivery,
    }, @ptrCast(&completed), struct {
        fn handle(context: *anyopaque, json: []const u8) !bool {
            const done: *bool = @ptrCast(@alignCast(context));
            if (std.mem.find(u8, json, "\"response.completed\"") != null) {
                done.* = true;
                return true;
            }
            return false;
        }
    }.handle);
    try std.testing.expect(completed);
    if (fixture.failure) |err| return err;
}

test "WebSocket rejects inbound frames over max_frame_bytes" {
    var fixture = try LoopbackWebSocketFixture.init(.oversized_frame);
    defer fixture.deinit();
    try fixture.start();
    var endpoint_buffer: [128]u8 = undefined;
    var cancelled = std.atomic.Value(bool).init(false);
    var delivery = gateway_client.DeliveryCertainty.init();
    const result = stream(std.testing.allocator, .{
        .endpoint = try fixture.endpoint(&endpoint_buffer),
        .authorization = "Bearer test",
        .account_id = "test",
        .session_id = null,
        .payload = "{}",
        .deadline = null,
        .cancel_flag = &cancelled,
        .delivery = &delivery,
    }, @ptrCast(&cancelled), struct {
        fn ignore(_: *anyopaque, _: []const u8) !bool {
            return false;
        }
    }.ignore);
    try std.testing.expectError(error.WebSocketMessageTooLarge, result);
    if (fixture.failure) |err| return err;
}

test "appendMessage rejects a 64 MiB overflow" {
    var message: std.ArrayList(u8) = .empty;
    defer message.deinit(std.testing.allocator);
    const payload = try std.testing.allocator.alloc(u8, max_message_bytes);
    defer std.testing.allocator.free(payload);
    try appendMessage(&message, std.testing.allocator, payload);
    try std.testing.expectError(error.WebSocketMessageTooLarge, appendMessage(&message, std.testing.allocator, "x"));
}

test "writeFrame rejects payloads over max_message_bytes" {
    var encoded: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer encoded.deinit();
    const payload = try std.testing.allocator.alloc(u8, max_message_bytes + 1);
    defer std.testing.allocator.free(payload);
    try std.testing.expectError(error.WebSocketMessageTooLarge, writeFrame(&encoded.writer, .text, payload));
}
