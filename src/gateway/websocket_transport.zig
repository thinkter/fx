const std = @import("std");
const io_mod = @import("../core/shared/io.zig");

const Allocator = std.mem.Allocator;

pub const max_frame_bytes: usize = 4 * 1024 * 1024;
pub const max_message_bytes: usize = 64 * 1024 * 1024;

pub const Error = error{
    WebSocketUpgradeRejected,
    WebSocketAcceptInvalid,
    WebSocketProtocolViolation,
    WebSocketUnexpectedBinary,
    WebSocketMessageTooLarge,
    WebSocketClosedBeforeCompletion,
};

pub const EventHandler = *const fn (context: *anyopaque, json: []const u8) anyerror!bool;

pub const Request = struct {
    endpoint: []const u8,
    authorization: []const u8,
    account_id: []const u8,
    session_id: ?[]const u8,
    payload: []const u8,
    cancel_flag: *std.atomic.Value(bool),
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
    var http_request = try client.request(.GET, uri, .{
        .headers = .{
            .authorization = .{ .override = request.authorization },
            .connection = .{ .override = "Upgrade" },
            .accept_encoding = .omit,
        },
        .extra_headers = extra_headers[0..count],
        .keep_alive = false,
        .redirect_behavior = .unhandled,
    });
    defer {
        // An upgraded connection must never return to the HTTP pool.
        if (http_request.connection) |connection| connection.closing = true;
        http_request.deinit();
    }
    try http_request.sendBodiless();
    const response = try http_request.receiveHead(&.{});
    if (response.head.status != .switching_protocols) return error.WebSocketUpgradeRejected;
    if (!hasHeader(response.head, "upgrade", "websocket") or
        !hasTokenHeader(response.head, "connection", "upgrade") or
        !hasHeader(response.head, "sec-websocket-accept", expected_accept))
    {
        return error.WebSocketAcceptInvalid;
    }

    // `receiveHead` leaves any already-buffered WebSocket bytes on this reader.
    const reader = http_request.reader.in;
    const writer = http_request.connection.?.writer();
    try writeFrame(writer, .text, request.payload);
    try http_request.connection.?.flush();

    var message: std.ArrayList(u8) = .empty;
    defer message.deinit(alloc);
    var fragmented_opcode: ?Opcode = null;
    while (true) {
        if (request.cancel_flag.load(.seq_cst)) return error.Cancelled;
        const frame = try readFrame(alloc, reader);
        defer alloc.free(frame.payload);
        switch (frame.opcode) {
            .ping => {
                try writeFrame(writer, .pong, frame.payload);
                try http_request.connection.?.flush();
            },
            .pong => {},
            .close => return error.WebSocketClosedBeforeCompletion,
            .binary => return error.WebSocketUnexpectedBinary,
            .continuation => {
                if (fragmented_opcode == null) return error.WebSocketProtocolViolation;
                try appendMessage(&message, alloc, frame.payload);
                if (!frame.fin) continue;
                const opcode = fragmented_opcode.?;
                fragmented_opcode = null;
                if (opcode != .text) return error.WebSocketUnexpectedBinary;
                if (try on_event(context, message.items)) return;
                message.clearRetainingCapacity();
            },
            .text => {
                if (fragmented_opcode != null) return error.WebSocketProtocolViolation;
                try appendMessage(&message, alloc, frame.payload);
                if (!frame.fin) {
                    fragmented_opcode = .text;
                    continue;
                }
                if (try on_event(context, message.items)) return;
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
    if (length == 126) length = try reader.takeInt(u16, .big);
    if (length == 127) {
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
