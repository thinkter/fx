#!/usr/bin/env python3
"""Isolated Phase 0 probe for the ChatGPT Codex Responses WebSocket endpoint.

This is intentionally not part of fx's runtime transport. It uses only Python's
standard library, reads credentials only from explicitly named environment
variables, never writes credentials or response content, and prints a redacted
JSON report.

Required for network use:
  FX_CODEX_PROBE_ACCESS_TOKEN
  FX_CODEX_PROBE_ACCOUNT_ID
  FX_CODEX_PROBE_MODEL

Examples:
  python3 scripts/codex_websocket_probe.py
  python3 scripts/codex_websocket_probe.py --execute --continuation

The default performs only the authenticated WebSocket upgrade. --execute sends
a fixed minimal prompt and consumes subscription usage. --continuation sends a
second request using the first response ID, but does not print that ID.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import secrets
import socket
import ssl
import sys
import time
from dataclasses import dataclass, field
from typing import Any

HOST = "chatgpt.com"
PATH = "/backend-api/codex/responses"
ORIGIN = "https://chatgpt.com"
PROTOCOL_HEADER = "responses_websockets=v2"
CONNECT_TIMEOUT_SECONDS = 15.0
EVENT_IDLE_TIMEOUT_SECONDS = 45.0
MAX_FRAME_BYTES = 1 << 20
MAX_MESSAGE_BYTES = 4 << 20
MAX_EVENTS = 256


class ProbeError(Exception):
    pass


@dataclass
class Report:
    handshake_status: int | None = None
    handshake_elapsed_ms: int | None = None
    selected_header_names: list[str] = field(default_factory=list)
    immediate_close_code: int | None = None
    event_types: list[str] = field(default_factory=list)
    terminal_event: str | None = None
    close_code: int | None = None
    close_reason_length: int | None = None
    continuation_attempted: bool = False
    continuation_accepted: bool | None = None
    error_code: str | None = None
    error: str | None = None

    def emit(self) -> None:
        print(json.dumps(self.__dict__, separators=(",", ":"), sort_keys=True))


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ProbeError(f"missing required environment variable {name}")
    return value


def websocket_accept(key: str) -> str:
    digest = hashlib.sha1(
        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
    ).digest()
    return base64.b64encode(digest).decode("ascii")


def read_exact(sock: ssl.SSLSocket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ProbeError("socket closed while reading frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_http_head(sock: ssl.SSLSocket) -> tuple[int, dict[str, str], bytes]:
    data = bytearray()
    while b"\r\n\r\n" not in data:
        if len(data) >= 64 * 1024:
            raise ProbeError("HTTP upgrade headers exceed local limit")
        chunk = sock.recv(4096)
        if not chunk:
            raise ProbeError("socket closed during HTTP upgrade")
        data.extend(chunk)
    raw_head, remainder = bytes(data).split(b"\r\n\r\n", 1)
    lines = raw_head.decode("iso-8859-1").split("\r\n")
    parts = lines[0].split(" ", 2)
    if len(parts) < 2 or not parts[1].isdigit():
        raise ProbeError("malformed HTTP upgrade status")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line or ":" not in line:
            raise ProbeError("malformed HTTP upgrade header")
        name, value = line.split(":", 1)
        headers[name.strip().lower()] = value.strip()
    return int(parts[1]), headers, remainder


class WebSocket:
    def __init__(self, sock: ssl.SSLSocket, buffered: bytes = b"") -> None:
        self.sock = sock
        self.buffered = bytearray(buffered)

    def _read_exact(self, size: int) -> bytes:
        if len(self.buffered) >= size:
            data = bytes(self.buffered[:size])
            del self.buffered[:size]
            return data
        prefix = bytes(self.buffered)
        self.buffered.clear()
        return prefix + read_exact(self.sock, size - len(prefix))

    def send_text(self, value: str) -> None:
        self._send_frame(0x1, value.encode("utf-8"))

    def send_pong(self, payload: bytes) -> None:
        self._send_frame(0xA, payload)

    def close(self) -> None:
        try:
            self._send_frame(0x8, b"\x03\xe8")
        except OSError:
            pass

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        if len(payload) > MAX_MESSAGE_BYTES:
            raise ProbeError("outbound frame exceeds local limit")
        mask = secrets.token_bytes(4)
        header = bytearray([0x80 | opcode])
        if len(payload) < 126:
            header.append(0x80 | len(payload))
        elif len(payload) <= 0xFFFF:
            header.append(0x80 | 126)
            header.extend(len(payload).to_bytes(2, "big"))
        else:
            header.append(0x80 | 127)
            header.extend(len(payload).to_bytes(8, "big"))
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.sock.sendall(bytes(header) + mask + masked)

    def read_message(self) -> tuple[int, bytes]:
        fragments: list[bytes] = []
        initial_opcode: int | None = None
        while True:
            first, second = self._read_exact(2)
            fin = (first & 0x80) != 0
            opcode = first & 0x0F
            masked = (second & 0x80) != 0
            length = second & 0x7F
            if masked:
                raise ProbeError("server sent a masked WebSocket frame")
            if length == 126:
                length = int.from_bytes(self._read_exact(2), "big")
            elif length == 127:
                length = int.from_bytes(self._read_exact(8), "big")
                if length & (1 << 63):
                    raise ProbeError("invalid WebSocket frame length")
            if length > MAX_FRAME_BYTES:
                raise ProbeError("inbound frame exceeds local limit")
            if opcode >= 0x8 and (not fin or length > 125):
                raise ProbeError("invalid WebSocket control frame")
            payload = self._read_exact(length)
            if opcode == 0x9:
                self.send_pong(payload)
                continue
            if opcode == 0xA:
                continue
            if opcode == 0x8:
                return opcode, payload
            if opcode == 0x0:
                if initial_opcode is None:
                    raise ProbeError("unexpected continuation frame")
            elif opcode in (0x1, 0x2):
                if initial_opcode is not None:
                    raise ProbeError("new data frame before fragmented message completed")
                initial_opcode = opcode
            else:
                raise ProbeError("unsupported WebSocket opcode")
            fragments.append(payload)
            if sum(len(fragment) for fragment in fragments) > MAX_MESSAGE_BYTES:
                raise ProbeError("reassembled WebSocket message exceeds local limit")
            if fin:
                return initial_opcode or opcode, b"".join(fragments)


def connect(token: str, account_id: str, report: Report) -> WebSocket:
    key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    context = ssl.create_default_context()
    started = time.monotonic()
    raw = socket.create_connection((HOST, 443), CONNECT_TIMEOUT_SECONDS)
    sock = context.wrap_socket(raw, server_hostname=HOST)
    sock.settimeout(EVENT_IDLE_TIMEOUT_SECONDS)
    request = "\r\n".join(
        [
            f"GET {PATH} HTTP/1.1",
            f"Host: {HOST}",
            "Connection: Upgrade",
            "Upgrade: websocket",
            "Sec-WebSocket-Version: 13",
            f"Sec-WebSocket-Key: {key}",
            f"Authorization: Bearer {token}",
            f"chatgpt-account-id: {account_id}",
            "originator: fx-phase-0-probe",
            f"OpenAI-Beta: {PROTOCOL_HEADER}",
            f"Origin: {ORIGIN}",
            "\r\n",
        ]
    ).encode("ascii")
    sock.sendall(request)
    status, headers, remainder = read_http_head(sock)
    report.handshake_elapsed_ms = round((time.monotonic() - started) * 1000)
    report.handshake_status = status
    report.selected_header_names = sorted(
        name
        for name in headers
        if name in {"openai-model", "x-codex-turn-state", "x-reasoning-included", "x-models-etag"}
    )
    if status != 101:
        raise ProbeError(f"WebSocket upgrade returned HTTP {status}")
    if headers.get("sec-websocket-accept") != websocket_accept(key):
        raise ProbeError("invalid Sec-WebSocket-Accept response")
    if "upgrade" not in headers.get("connection", "").lower():
        raise ProbeError("upgrade response does not retain Connection: Upgrade")
    if headers.get("upgrade", "").lower() != "websocket":
        raise ProbeError("upgrade response does not select websocket")
    return WebSocket(sock, remainder)


def safe_protocol_label(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > 128:
        return None
    if not value.isascii() or any(not (char.isalnum() or char in "._-") for char in value):
        return None
    return value


def event_type(payload: bytes) -> tuple[str | None, dict[str, Any] | None]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, None
    if not isinstance(value, dict):
        return None, None
    return safe_protocol_label(value.get("type")), value


def response_id(value: dict[str, Any]) -> str | None:
    response = value.get("response")
    if not isinstance(response, dict):
        return None
    identifier = response.get("id")
    return identifier if isinstance(identifier, str) else None


def structured_error_code(value: dict[str, Any]) -> str | None:
    error = value.get("error")
    if not isinstance(error, dict):
        return None
    return safe_protocol_label(error.get("code"))


def wait_for_terminal(ws: WebSocket, report: Report) -> str | None:
    identifier: str | None = None
    for _ in range(MAX_EVENTS):
        opcode, payload = ws.read_message()
        if opcode == 0x8:
            report.close_code = int.from_bytes(payload[:2], "big") if len(payload) >= 2 else None
            report.close_reason_length = max(len(payload) - 2, 0)
            raise ProbeError("server closed before terminal response event")
        if opcode != 0x1:
            raise ProbeError("server sent an unexpected binary message")
        kind, value = event_type(payload)
        if kind is None or value is None:
            report.event_types.append("invalid_json")
            continue
        report.event_types.append(kind)
        if kind == "response.created":
            identifier = response_id(value)
        if kind in {"response.completed", "response.done", "response.incomplete", "response.failed", "error"}:
            report.terminal_event = kind
            report.error_code = structured_error_code(value)
            return identifier
    raise ProbeError("event count exceeds local limit before terminal response")


def request_body(model: str, previous_response_id: str | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "type": "response.create",
        "model": model,
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "Reply with exactly: probe"}]}],
    }
    if previous_response_id is not None:
        body["previous_response_id"] = previous_response_id
    return body


def run(args: argparse.Namespace) -> Report:
    report = Report()
    ws: WebSocket | None = None
    try:
        token = required_env("FX_CODEX_PROBE_ACCESS_TOKEN")
        account_id = required_env("FX_CODEX_PROBE_ACCOUNT_ID")
        model = required_env("FX_CODEX_PROBE_MODEL")
        ws = connect(token, account_id, report)
        if not args.execute:
            ws.sock.settimeout(0.2)
            try:
                opcode, payload = ws.read_message()
                if opcode == 0x8:
                    report.immediate_close_code = int.from_bytes(payload[:2], "big") if len(payload) >= 2 else None
            except (socket.timeout, ssl.SSLWantReadError):
                pass
            return report
        ws.send_text(json.dumps(request_body(model), separators=(",", ":")))
        first_id = wait_for_terminal(ws, report)
        if args.continuation:
            report.continuation_attempted = True
            if not first_id or report.terminal_event != "response.completed":
                report.continuation_accepted = False
                return report
            report.event_types = []
            report.terminal_event = None
            ws.send_text(json.dumps(request_body(model, first_id), separators=(",", ":")))
            wait_for_terminal(ws, report)
            report.continuation_accepted = report.terminal_event == "response.completed"
        return report
    except (OSError, ssl.SSLError, ProbeError) as error:
        report.error = type(error).__name__
        if report.handshake_status is None:
            report.handshake_status = 0
        return report
    finally:
        if ws is not None:
            ws.close()
            ws.sock.close()


def self_test() -> None:
    key = "dGhlIHNhbXBsZSBub25jZQ=="
    assert websocket_accept(key) == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    body = request_body("model", "response")
    assert body["previous_response_id"] == "response"
    assert "stream" not in body and "background" not in body
    code, value = event_type(b'{"type":"response.created","response":{"id":"r"}}')
    assert code == "response.created" and response_id(value or {}) == "r"
    assert safe_protocol_label("response.completed") == "response.completed"
    assert safe_protocol_label("prompt content") is None
    print("codex websocket probe self-test passed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the isolated fx Codex WebSocket Phase 0 probe")
    parser.add_argument("--execute", action="store_true", help="send a fixed minimal model request after the upgrade")
    parser.add_argument("--continuation", action="store_true", help="test previous_response_id after --execute")
    parser.add_argument("--self-test", action="store_true", help="run deterministic local checks without credentials or network")
    args = parser.parse_args()
    if args.continuation and not args.execute:
        parser.error("--continuation requires --execute")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.self_test:
        self_test()
    else:
        result = run(arguments)
        result.emit()
        sys.exit(0 if result.error is None else 1)
