# Codex WebSocket transport: plan to finish PR #521

This document replaces the earlier `docs/codex-websocket-transport.md` design
notes. It records the verified comparison between PR #521
(`feat/codex-websocket-phase-2`, head `05bf5a4`) and PR #523
(`perf/codex-websocket-transport`, head `f486bb2`), and the work needed to make
#521 the clear landing candidate.

## Implementation status

The plan is implemented on the rebased feature branch:

- Codex WebSocket orchestration now has a separate adapter module.
- The transport uses the dated beta header and strict frame handling.
- Pre-delivery connection failures fall back to SSE and arm a process latch;
  ambiguous delivery never falls back or replays.
- Retained lanes perform network I/O outside the pool mutex, use temporary
  connections under saturation, and have a global LRU storage bound.
- Focused tests cover fallback latching, ambiguous-delivery no-replay,
  continuation recovery, retained connection reuse, expiration, and identity
  churn.

## Verified findings

Each claim below was checked directly against both PR heads and current `main`
(`bb2dc7d`).

- **Stale beta header.** `src/gateway/websocket_transport.zig` sends
  `OpenAI-Beta: responses_websockets=v2`. #523 sends the dated
  `responses_websockets=2026-02-06`, which matches the current upstream Codex
  client.
- **Network I/O under the pool mutex.** `acquire` in
  `src/gateway/codex_websocket_session.zig` holds `pool_mutex` through `ping`,
  `close`, and `connect`. A slow handshake on one lane stalls every other
  lane. When all matching lanes are busy it also busy-polls with a 10ms sleep.
- **Unbounded slot array.** `appendSlot` grows the global slot list per new
  identity. Incompatible slots get their connection closed but the slot entry
  itself is never evicted.
- **Merge state.** Only `src/gateway/openai_codex.zig` conflicts with `main`,
  caused by `cd1a5d3` switching error returns to
  `stream_provider.failResult(...)`.
- **#523's real advantages.** A clean adapter boundary (about 41 lines touched
  in `openai_codex.zig`), a process-wide SSE fallback latch, and the dated
  header.
- **#523's real defects.** Binary frames are accepted as text, close frames
  are not consumed or validated, there is no idle-event timeout, its cache is
  one global entry (alternating sessions displace each other), and its
  fallback and fresh-retry trigger is "no output yet" rather than "provably
  unsent", which permits duplicate provider requests and billing after an
  ambiguous delivery.

## Strategy

Keep #521's functional advantages (safe-prefix continuation, delivery-aware
retries, strict frame validation, retained lanes, deep test coverage). Adopt
every legitimate advantage of #523. Fix the three real defects in this branch.
After that, #523's weaknesses have no counterpart here.

## Phase 1: rebase and restructure

1. **Rebase onto current `main`.** Only `openai_codex.zig` conflicts. While
   resolving, adapt the WebSocket error paths to the new
   `stream_provider.failResult(...)` convention.
2. **Extract the added logic from `openai_codex.zig` into the adapter.** Move
   `buildWebSocketRequest*`, the WebSocket consume loop, and continuation
   orchestration into `codex_websocket_session.zig` (or a new
   `openai_codex_websocket.zig`). Target: the provider file gains only a small
   transport dispatch, comparable to #523's footprint. This removes the
   integration-boundary critique and shrinks the future conflict surface.
3. **Drop `scripts/codex_websocket_probe.py`** from this PR. It is a dev
   probe, not product code. Land it separately if it is worth keeping.

## Phase 2: adopt #523's genuine wins, done better

4. **Update the beta header** to `responses_websockets=2026-02-06`. Re-check
   the current upstream constant when making the change and cite it in the
   commit message.
5. **Add a delivery-aware process-wide SSE fallback latch.** Latch to SSE only
   when the failure is provably pre-send (handshake or upgrade rejection,
   connect timeout, pre-write errors), using the `DeliveryCertainty` already
   threaded through `AcquireArgs`. An ambiguously sent request surfaces an
   error instead of being silently re-issued. This keeps #523's "a broken
   proxy costs one failed handshake, not one per turn" resilience without its
   duplicate-billing hole.
6. **Keep strict transport-env validation.** Erroring on an invalid
   `FX_CODEX_TRANSPORT` value is deliberate and better than silently coercing
   to SSE. Say so in the PR description.

## Phase 3: fix the pool's real defects

7. **Move network I/O outside `pool_mutex`.** Restructure `acquire` to: lock,
   select and reserve a slot (`busy = true`), unlock, then ping, connect, or
   close outside the lock, and relock only to commit or roll back slot state.
   Nothing slow ever runs under the lock.
8. **Delete the busy-wait path.** When all matching lanes are busy at the lane
   limit, hand out a temporary unpooled connection instead of sleeping in 10ms
   slices. Simpler, lower tail latency, and one less polling state.
9. **Bound and evict slots globally.** Add a global slot cap (for example
   `max_lanes` times a small constant). Evict idle slots LRU when appending
   past the cap, and fully remove, not just disconnect, slots whose identity
   is incompatible. Add a unit test proving the bound holds under identity
   churn.

## Phase 4: prove it

10. **Add two E2E tests for the new behavior:**
    - a handshake failure latches the process to SSE and the turn still
      completes;
    - an ambiguous post-send failure does not fall back or re-send, and errors
      with delivery evidence.
    Both PRs added tests to `tests/e2e/tui-auth-source-selection.test.ts`;
    inherit its corpus classification but confirm it still fits per AGENTS.md.
11. **Run the full ready gate:** `zig fmt --check src/`, focused Zig tests,
    build, drive `./zig-out/bin/fx` with `FX_CODEX_TRANSPORT=websocket`
    against the loopback E2E mock, push, and require Full CI green on all four
    runners for the exact head before marking the PR ready.

## Optional: split into a two-PR stack

The strongest structural critique of #521 is that continuation plus pooling is
a lot for a first landing. If reviewers push back, stack the branch:

- **PR A:** strict codec, adapter boundary, single retained connection,
  delivery-aware fallback latch. A superset of #523 with none of its bugs.
- **PR B:** safe-prefix continuation and the multi-lane pool on top.

If a single PR is preferred, Phases 1 through 4 alone make #521 dominate the
comparison on every row except raw diff size.
