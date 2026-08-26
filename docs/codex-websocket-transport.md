# OpenAI Codex Responses WebSocket transport

## Scope and evidence status

This document concerns the ChatGPT-subscription Codex route at `chatgpt.com/backend-api/codex/responses`, not fx's Vercel AI Gateway route. Both use a Responses WebSocket protocol family, but they are separate backends. Public Responses API, Azure, and AI Gateway documentation must not be treated as a specification for the private ChatGPT backend.

Claims are labelled as follows:

- **Confirmed:** supported by the fx checkout or current upstream `openai/codex` source.
- **Likely, verify:** supported by public protocol documentation or related implementations, but not yet measured against the ChatGPT subscription backend.
- **Undocumented:** a private-backend behavior that requires live, authenticated probing before fx relies on it.

This is an implementation design, not a claim that fx already supports WebSockets.

## Current fx behavior

**Confirmed.** The Codex provider currently uses HTTPS with Server-Sent Events (SSE):

1. fx reads or refreshes the local ChatGPT OAuth session, extracts the ChatGPT account ID, and constructs a bearer token.
2. It serializes a complete Responses request containing the model, system instructions, conversation history, tools, tool outputs, images, and retained encrypted reasoning state.
3. It sends that JSON with `POST https://chatgpt.com/backend-api/codex/responses` and asks for `text/event-stream`.
4. The server streams `data:` records over that HTTP request.
5. `src/gateway/responses_protocol.zig` reduces each JSON event into text, reasoning, tool-call, and final-completion events.

The implementation has a 30-second connection deadline, cancellation handling, and limits for aggregate stream data, events, tool calls, tool identities, tool arguments, and preserved provider state. Each model request opens a new HTTP connection.

## What WebSockets change

**Likely, verify for this backend.** A secure WebSocket begins with an HTTPS Upgrade request. After a valid `101 Switching Protocols` response, both sides retain an encrypted, bidirectional connection.

```text
fx                    Codex service
 HTTPS WebSocket Upgrade  →
 101 Switching Protocols  ←
 response.create frame    →
 response event frames    ←
 response.completed       ←
```

The Responses data remains JSON. The client sends a text message resembling:

```json
{
  "type": "response.create",
  "model": "gpt-5...",
  "instructions": "...",
  "input": [],
  "tools": []
}
```

The server returns text messages such as `response.output_text.delta`, `response.function_call_arguments.delta`, `response.completed`, and `response.failed`. A WebSocket event source should feed decoded JSON text directly into fx's existing Responses reducer. It must not create a second model-event implementation.

## Upstream Codex CLI behavior

**Confirmed for upstream Codex, not automatically for the private backend.** Upstream Codex has a dedicated `responses_websocket` transport, gated by provider capability, and retains a healthy connection for its client session.

### Handshake and metadata

**Confirmed.** Codex constructs a WebSocket URL, attaches provider and authentication headers during the HTTP upgrade, requests a versioned Responses WebSocket beta protocol, and validates the upgrade. It can record selected server model, reasoning inclusion, Codex turn state, rate-limit data, model ETags, moderation metadata, and timing information.

It also supports a handshake probe that upgrades without sending a prompt and briefly waits for an immediate close. This distinguishes a usable connection from one accepted by the edge then rejected by policy.

### One request at a time

**Confirmed.** Codex serializes one response stream per socket. It sends `response.create`, reads until a terminal event, then permits the next request. It does not interleave independent generations on the same connection.

This is the correct initial model for fx. Concurrent requests require request identity, event demultiplexing, flow control, and independent recovery.

### Event failures and reuse

**Confirmed.** Codex applies an idle timeout to each server-event wait. It treats idle timeout, EOF before completion, close before completion, unexpected binary messages, I/O failure, and structured service errors as stream failures. It handles Ping and Pong as transport housekeeping and discards a connection after a terminal stream error.

**Confirmed for upstream source; likely, verify for this backend.** Upstream recognizes `websocket_connection_limit_reached` with a 60-minute connection limit and recognizes `previous_response_not_found` as recoverable by a full-context request.

**Likely, verify.** `response.failed` must invalidate its continuation chain. A later request must not reuse the failed response as `previous_response_id`.

### Incremental continuation

**Confirmed for upstream client behavior; undocumented for the ChatGPT backend.** A retained connection can send a new `response.create` with `previous_response_id` and only newly added input items. This reduces repeated upload of long transcripts and tool history.

Until Phase 0 verifies the ChatGPT backend, fx must regard continuation state as **possibly connection-scoped**, not guaranteed connection-scoped. Saved fx sessions must always retain enough history to reconstruct a full request; remote state is never the sole source of truth.

## The central reliability rule: delivery certainty

A network failure does not simply mean a request failed:

- **Request has not begun writing:** automatic retry is safe.
- **Request may have been written but no acknowledgement arrived:** do not retry blindly.
- **The server acknowledged a response ID:** recover only through supported server state or a known-safe replay.

If fx sends `response.create` then loses the network before `response.created`, the service may still generate a response and issue tool calls. Blindly retrying could duplicate shell commands or other actions.

fx already models this distinction for HTTP with `DeliveryCertainty`. A WebSocket transport must preserve it. Upstream Codex may retry an established stream before falling back, but fx deliberately diverges: its tool-capable turns require a stricter no-blind-replay policy after delivery becomes ambiguous.

## Phase 0: authenticated live-traffic probe

Before transport implementation, run the isolated maintainer probe at `scripts/codex_websocket_probe.py` against `chatgpt.com/backend-api/codex/responses`. It uses only Python's standard library, accepts credentials only through explicit environment variables, never reads fx credential files, and emits one redacted JSON report. It never writes credentials, account IDs, response IDs, prompts, or response content.

```bash
export FX_CODEX_PROBE_ACCESS_TOKEN='...'
export FX_CODEX_PROBE_ACCOUNT_ID='...'
export FX_CODEX_PROBE_MODEL='...'
python3 scripts/codex_websocket_probe.py
python3 scripts/codex_websocket_probe.py --execute --continuation
```

The first invocation performs only an authenticated upgrade. `--execute` sends a fixed minimal prompt and consumes subscription usage; `--continuation` sends a second request using the first response ID without printing that ID. Run `python3 scripts/codex_websocket_probe.py --self-test` for deterministic, credential-free checks.

Record only privacy-safe protocol evidence:

- handshake status, selected response headers, and immediate close behavior;
- event type sequence, terminal event, structured error code, and close code;
- whether WebSocket accepts `previous_response_id` continuation;
- whether HTTP/SSE accepts `previous_response_id` continuation;
- behavior after a failed response in a continuation chain;
- whether model changes on a retained connection produce a policy close such as 1008;
- connection-age behavior and any limit/error code;
- behavior of `store` and related persistence fields, if accepted.

Do not hard-code public API assumptions until this probe confirms them. In particular, the 60-minute limit, `previous_response_not_found`, model pinning, `store` semantics, and SSE continuation support are undocumented for this private backend.

## Proposed implementation plan

### Phase 1: per-turn WebSocket transport

Implement a fresh socket per model request, with no retained connection or cached continuation.

- Keep SSE as the permanent compatibility baseline.
- Reuse existing Codex request generation and Responses reduction.
- Send one full `response.create` request on a fresh socket.
- Fall back to SSE immediately for failed upgrades, connection timeouts, or other failures definitely before delivery.
- After a request may have been transmitted, never silently replay through SSE.
- Treat server close before `response.completed` as an explicit failure class.
- Once Phase 0 confirms behavior, treat policy close 1008 as a wrong-model-on-connection failure: poison the connection and recreate it with the correct model, rather than retrying it as a network failure.
- Maintain a strict retry policy for established streams. Fewer retries, including zero, are safer than duplicating a possible tool call.

### Phase 2: retained socket, full context per turn

After Phase 1 has stable production evidence:

- Retain at most one idle connection per active fx session, provider, account identity, and compatible model.
- Serialize one full request at a time over it.
- Preconnect only as an optimization. Never send prompt data during preconnect.
- Discard and recreate the connection after a close, protocol error, cancellation during a stream, timeout, failed write, authentication transition, or model incompatibility.
- Continue sending full context after reconnect.
- Proactively reconnect before a confirmed connection-age limit, with enough margin that the limit cannot interrupt an in-flight turn.
- Reset a connection-health or fallback budget only after confirmed `response.completed`, not merely after a successful handshake.
- Segment telemetry by authentication mode from the start.

Connect and event-idle timeouts must be configurable and measured per platform. Do not assume Linux and macOS behavior predicts Windows behavior.

### Phase 3: incremental continuation

Only begin after Phase 0 verifies continuation semantics and Phase 2 has stable evidence. Maintain a continuation record containing:

```text
connection identity
credential and account fingerprint
endpoint and protocol version
model and request-shaping options
last fully completed response ID
full request required for recovery
continuation expiry and validity
```

Invalidate it when the connection reconnects, model or relevant options change, authentication changes, the referenced turn fails, cancellation interrupts a stream, a tool call or result is uncertain, the server rejects the previous response ID, or the connection reaches its lifetime.

### Phase 4: optional stream lanes

**Likely, verify.** If the protocol supports a `stream_id` field with tagged events, named lanes could eventually allow concurrent subagent turns to share a retained connection. Requests in one lane must remain ordered; separate lanes require event demultiplexing and independent continuation state.

Do not implement this before Phases 1 through 3 are stable. It substantially changes connection ownership, resource accounting, and failure recovery.

## Transport requirements

Keep WebSocket mechanics separate from Codex request serialization:

```text
src/gateway/websocket_transport.zig  RFC 6455 handshake, frames, cancellation, limits
src/gateway/responses_stream.zig     JSON event source shared by SSE and WebSocket
src/gateway/openai_codex.zig         Codex auth, payload, and transport selection
```

The transport must implement and test:

- TLS certificate and hostname validation;
- HTTP `101 Switching Protocols` and `Sec-WebSocket-Accept` validation;
- mandatory masking of client frames;
- text-message fragmentation and continuation-frame reassembly;
- UTF-8 validation for text messages;
- Ping to Pong handling;
- close codes, bounded close reasons, and a finite close deadline;
- cancellation that unblocks connection, read, and write operations;
- separate connection, write, and event-idle deadlines;
- bounded outbound requests, inbound frames, reassembled messages, and buffered unread bytes;
- strict rejection of unexpected message kinds and protocol violations.

Do not enable per-message WebSocket compression in the first release. It adds decompression limits, compatibility cases, CPU cost, and security surface.

## Connection lifecycle and fallback policy

Represent a connection as a state machine:

```text
disconnected → connecting → open-idle → open-streaming → closing
                                      ↘ poisoned → disconnected
```

One component owns socket I/O. UI, retry, and shutdown paths communicate through controlled cancellation or commands; they must never concurrently read from or write to the socket.

A socket becomes poisoned and its continuation state is discarded after a timeout, protocol violation, unexpected binary event, early EOF, failed write, failed close, failed response, or server error that leaves state uncertain.

Expose a prominent user-facing policy:

```text
auto        Prefer a known-good WebSocket, otherwise use SSE.
sse         Always use HTTP and SSE.
websocket   Require WebSocket and return an actionable error if unavailable.
```

In `auto`, only pre-delivery WebSocket failures may fall back automatically to SSE. After a request may have been delivered, report uncertainty or use verified server recovery. Do not silently replay.

If fx later adds remote Responses compaction for this provider, route it over HTTP unless Phase 0 or subsequent probes confirm WebSocket support for that endpoint.

## Observability and rollout

Ship with a local kill switch and measured rollout. Record only privacy-safe operational data:

- selected transport, protocol version, platform, fx version, and authentication mode;
- handshake status and duration;
- new versus reused connection;
- time to first event and terminal completion;
- error, close, retry, circuit-breaker, and fallback classification;
- bounded byte and event counts;
- continuation hit, invalidation, and recovery reason.

Never log prompts, responses, OAuth tokens, account IDs, raw tool arguments, or unredacted headers.

Track handshake success, terminal completion rate, fallback and ambiguous-delivery rates, latency by transport and auth mode, errors by platform/network, and long-session memory and file-descriptor stability.

## Verification plan

Before broad enablement, require:

1. Zig unit tests for handshake validation, masking, fragmentation, control frames, malformed frames, UTF-8, and resource limits.
2. A scripted loopback fixture for delayed events, oversized frames, invalid events, disconnects at write/read boundaries, and server close before `response.completed`.
3. Reducer-parity tests that feed identical Responses JSON through SSE and WebSocket and assert identical completion data and callback ordering.
4. A delivery-certainty retry matrix, including a request that may have been sent but was never acknowledged and a duplicate-tool-call prevention case.
5. Model-mismatch coverage, after Phase 0 verifies the behavior, proving a policy close is not retried as a generic transient network error.
6. Continuation coverage proving that a failed referenced turn invalidates the chain and forces full context.
7. Circuit-breaker coverage proving that failures deplete the budget and only a confirmed completion resets it.
8. Cancellation and shutdown tests while connecting, writing, waiting for output, receiving tool arguments, and closing, including leak checks.
9. Platform-matrix connect-timeout tests and long-running soak tests with forced reconnects and connection-age expiry.
10. A real-binary smoke test using freshly built `./zig-out/bin/fx` against a loopback fixture and an interactive terminal path.

## Phase 1 implementation review and required fixes

This review applies to the Phase 1 implementation. It is not production-ready and must retain SSE as the default until every release blocker below is resolved and verified.

### Implementation progress

- [x] Frame parsing and UTF-8 validation: regression tests pass in the repository test target. Close-payload validation is implemented; the bounded close handshake remains below.
- [ ] Provider admission and shared Codex preparation.
- [ ] Bounded upgrade, write, and event-idle I/O with cancellation unblocking.
- [ ] Bounded close handshake and close-code reporting.
- [ ] Loopback WebSocket fixture, reducer-parity, delivery-certainty, and real-binary smoke coverage.
- [ ] Authenticated Phase 0 evidence: requires an explicit maintainer-run probe and is not satisfied by automated tests.

### Release blockers

1. **Extended-length frame parsing corrupts a valid 127-byte frame.**

   `src/gateway/websocket_transport.zig` reads the seven-bit length discriminator, then uses two independent `if` statements. If that discriminator is `126` and the following 16-bit length is exactly `127`, the first branch correctly reads `127`, then the second branch incorrectly treats it as the 64-bit discriminator and consumes eight payload bytes as a length. This desynchronizes the stream.

   Fix: make the 64-bit branch `else if (length == 127)`. Add a regression test for a 127-byte text frame followed by another frame, proving the second frame remains aligned.

2. **The WebSocket path does not participate in provider admission.**

   The SSE path calls `request.admission.admit()` before opening transport. The WebSocket path currently does not, which produces the user-visible `provider admission missing` failure and bypasses the normal provider-attempt lifecycle.

   Fix: preserve the admission boundary for every transport before opening its request. Add a focused test that runs the WebSocket path through the same admission fixture as SSE.

3. **The WebSocket path has no bounded connection deadline or cancellation unblock.**

   SSE opens through `runBoundedHttpOperation` with the 30-second connect deadline and installs `spawnHttpCancelWatcher` to interrupt blocked connection I/O. The WebSocket path invokes the HTTP client directly and only checks `cancel_flag` between completed frame reads. A blocked `reader.takeByte()` cannot observe cancellation until the peer sends bytes or closes.

   Fix: use an operation that bounds connection setup, writes, and event-idle reads, and that closes or interrupts the underlying connection when cancellation occurs. Verify cancellation while connecting, while writing, and while waiting for a frame.

4. **Close handling does not meet the documented transport contract.**

   The current code marks the HTTP connection closing and deinitializes it without sending a WebSocket close frame on normal terminal, cancellation, and error paths. It also does not parse or report close code and bounded reason.

   Fix: send one bounded close frame when the socket is open, await a peer close for a finite deadline where appropriate, then forcibly release the connection. Record and classify peer close code and bounded reason. Cover normal completion, cancellation, malformed frames, and early peer close.

5. **Text frames are not explicitly validated as UTF-8.**

   The reducer eventually parses JSON, but WebSocket text-message validity must be enforced at the frame-message boundary so malformed text is classified as a transport protocol error rather than an incidental JSON failure.

   Fix: validate completed text messages before calling the event handler, return a dedicated protocol error, and add malformed UTF-8 coverage.

### Required cleanup before broad enablement

1. **Replace fragile WebSocket request string surgery.**

   `buildWebSocketRequest` finds the literal `,"store":false,"stream":true` in an SSE payload and splices around it. This couples WebSocket behavior to exact field ordering and formatting in `buildRequest`. The current test uses a hardcoded SSE payload, so it cannot catch drift in the real request builder.

   Minimum fix: add an end-to-end unit assertion for `buildWebSocketRequest(buildRequest(...))` using representative system messages, tools, reasoning, images, and structured output. Preferred fix: split request construction into a transport-neutral Responses request model or shared field writer, then add transport-specific envelopes without parsing a serialized request.

2. **Extract shared Codex authentication and endpoint preparation.**

   `streamPrepared` and `streamWebSocketPrepared` duplicate account-ID extraction, authorization-header allocation, and loopback-only E2E endpoint selection.

   Fix: introduce a small helper whose result owns or scopes the prepared account ID, authorization header, and endpoint. Keep credential material zeroed and free it at the same boundary as today.

3. **Create one stream-limits builder.**

   The six stream-limit values are repeated in `CodexLimits`, `WebSocketBridge.streamLimits`, and the SSE `consumeSse` conversion. The WebSocket method ignores `self`, which is evidence it should be a shared pure builder.

   Fix: use one `codexStreamLimits(CodexLimits)` helper returning `responses_protocol.StreamLimits`; use it for both SSE and WebSocket reducers.

### Lower-priority hardening and explicit decisions

- Guard `http_request.connection` after a successful upgrade instead of using `connection.?`. A `101` should have a connection, but treating an impossible state as an error is safer than trapping.
- Document the asymmetric limits: outbound frames permit a full 64 MiB request, while inbound frames are limited to 4 MiB and reassembled messages to 64 MiB. This is reasonable if large outbound contexts are intentional and response events are expected to be fragmented, but it needs rationale and tests at each boundary.
- Keep `auto` mapped to SSE in Phase 1. This is intentional and compatible with the rollout plan, not a bug. Do not change `auto` to WebSocket until pre-delivery fallback, health evidence, and the release blockers are implemented.
- The Phase 0 Python probe is standard-library-only, reads only explicitly named environment credentials, emits redacted output, and is suitable to retain as an untracked diagnostic script.

## Current status

fx has robust Codex HTTPS/SSE request generation and Responses reduction. The uncommitted Phase 1 branch adds a fresh-socket WebSocket experiment behind `FX_CODEX_TRANSPORT=websocket`; SSE remains the default and `auto` maps to SSE. The experiment is blocked from broad use by the required fixes above.
