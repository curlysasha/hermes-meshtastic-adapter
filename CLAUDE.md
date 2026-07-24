# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A **Hermes Agent platform plugin** (`meshtastic-platform`) that bridges a Meshtastic LoRa mesh to Hermes. It is not a standalone app — it is loaded by the Hermes gateway, which calls `register(ctx)` in `__init__.py`. That entry point registers the platform adapter (`adapter.register`) and the seven `mesh_*` tools.

The naming is intentionally three-way: GitHub repo `hermes-meshtastic-adapter`, Hermes plugin `meshtastic-platform`, Hermes platform `meshtastic`.

## Critical Dependency: Hermes Agent

The code imports `gateway.*` (`gateway.config`, `gateway.platforms.base`, `gateway.platform_registry`) from **Hermes Agent, which is NOT in this repo**. Nothing imports or type-checks without it resolvable on `sys.path`:

- **Locally**: Hermes is expected at `~/.hermes/hermes-agent` (the default in `test_meshtastic.py` via `HERMES_AGENT_PATH`). Commands run through the Hermes venv at `~/.hermes/hermes-agent/venv/bin/python`.
- **CI** (`.github/workflows/ci.yml`): checks out `NousResearch/hermes-agent` into `_deps/hermes-agent`, installs it editable, and points `--search-path` / `HERMES_AGENT_PATH` there.

When working in this repo without Hermes installed, the `gateway.*` imports will fail — this is expected, not a bug to fix.

## Commands

All commands run via the repo's **`.venv`** (uv-managed), which holds the dev
tooling (`ruff`/`pyrefly`/`coverage`) and resolves `gateway.*`. The Hermes venv
(`~/.hermes/hermes-agent/venv`) does **not** have ruff/pyrefly — don't use it for
these gates. Set `HERMES_AGENT_PATH` if Hermes isn't at `~/.hermes/hermes-agent`.

```bash
# Tests (mock serial + temp SQLite):
.venv/bin/python -m unittest test_meshtastic.py
# Run a single test:
.venv/bin/python -m unittest test_meshtastic.TestMeshtasticPlatform.<method_name>

# Format, lint, type-check (the exact gates CI enforces):
.venv/bin/python -m ruff format .            # CI runs: ruff format --check .
.venv/bin/python -m ruff check .
.venv/bin/python -m pyrefly check \
  --python-interpreter-path .venv/bin/python \
  --search-path ~/.hermes/hermes-agent --min-severity warn

# Coverage (also a CI gate, --fail-under=80):
.venv/bin/python -m coverage run -m unittest test_meshtastic.py \
  && .venv/bin/python -m coverage report -m
```

CI runs `ruff format --check`, `ruff check`, `pyrefly check --min-severity warn`,
and `coverage`+`unittest` — all four must pass. Pyrefly hides warnings unless
`--min-severity warn` is passed; CI uses it, so do the same locally.

## Architecture

Five source modules, no package nesting:

- **`adapter.py`** — `MeshtasticAdapter(BasePlatformAdapter)`, the heart of the plugin. Handles serial connection, the inbound→Hermes bridge, and the outbound chunked send path.
- **`tools.py`** — the ten `mesh_*` async tool handlers exposed to the agent. Seven are read-only (they serve already-heard data); three are **solicited requests** that put packets on the air — see below.
- **`schemas.py`** — JSON function schemas for those tools.
- **`telemetry_db.py`** — SQLite persistence (`telemetry`, `positions`, `signal_quality` tables) at `~/.hermes/meshtastic_telemetry.db`.
- **`__init__.py`** — `register(ctx)` plugin entry point.

### Inbound path (mesh → Hermes), and its threading boundary

This is the subtlest part of the code. Meshtastic's `pubsub` delivers packets on a **background thread**, but Hermes runs on an asyncio loop. The bridge:

1. `_on_receive_pubsub` (pubsub thread) → `loop.call_soon_threadsafe` pushes onto `self._incoming_queue` (asyncio.Queue).
2. `_consume_incoming_queue` (loop task) drains it and calls `_on_receive`.
3. `_on_receive` records live freshness for the sender via `_update_observed` (BEFORE the auth gate, so even non-allowlisted nodes get a current `last_heard`/signal), then authorizes the sender, filters self-echo, logs signal/telemetry/position to SQLite, and for TEXT packets builds a `MessageEvent` and calls `self.handle_message(event)`.

### Node freshness overlay

`iface.nodes[x]["lastHeard"]` from the meshtastic library only refreshes from periodic **NodeInfo** packets, so it lags a node's actual transmissions. To fix this, `_on_receive` maintains `self._node_observed` (per node id, bounded at `OBSERVED_NODE_LIMIT`): `last_heard` is bumped from each packet's `rxTime` (clamped to now), and `snr`/`rssi` only from **direct** (0-hop) packets — mirroring the official Meshtastic client. The `mesh_list_nodes` / `mesh_node_info` / `mesh_signal_quality` tools overlay `adapter.get_observed_node(nid)` on top of the library node DB (freshest of the two).

Any new packet-handling work must respect this boundary — do not touch loop state from the pubsub thread except via `call_soon_threadsafe`.

### Chat ID / session scoping

`_on_receive` decides DM vs broadcast and forms the chat_id that becomes the Hermes session key:
- DM → `meshtastic:!da1b1613`
- Broadcast → `meshtastic:channel:0` or `meshtastic:channel:Primary`

`_send_immediate` parses these back apart (`split(":", 2)`) to choose `destinationId` vs `channelIndex`.

**Channels are opt-in.** By default `_on_receive` bridges **DMs only** — a broadcast/channel message is logged and dropped so the agent never replies into a shared channel's airtime. `MESHTASTIC_ALLOW_CHANNELS=true` (or `allow_channels` in plugin extra) enables answering channels.

### Outbound path (Hermes → mesh)

`send()` → `_chunk_message` splits content into UTF-8-byte-bounded chunks with `[i/n]` prefixes (the protocol app-payload ceiling is 233 bytes — `mesh_pb2.Constants.DATA_PAYLOAD_LEN`; `MESHTASTIC_CHUNK_BYTES` overrides, clamped to 233), paces them by `MESHTASTIC_CHUNK_DELAY` → `_send_chunk` → `_send_immediate` calls the blocking `iface.sendText(..., wantAck=True)` via `run_in_executor`.

**ACK/NACK is observability-first.** By default sends are non-blocking; `onAckNak` callbacks just record status into `_pending_acks` / `_ack_responses` (bounded at `ACK_RECORD_LIMIT`). Only when `MESHTASTIC_ACK_TIMEOUT > 0` (or send metadata requests it) does `_wait_for_ack` block and let a NAK/timeout make `SendResult.success` false.

**Real vs implicit ACK.** ACK lifecycle is the `AckStatus` `StrEnum` (`pending` / `ack` / `implicit_ack` / `nak` / `timeout`). `_record_ack_response` distinguishes a **real** end-to-end ACK (routing ACK sender IS the destination → `AckStatus.ACK`) from an **implicit** ACK relayed by another node (sender ≠ destination → `AckStatus.IMPLICIT_ACK` — packet reached the mesh but dest did not confirm). Mirrors the official client's RECEIVED vs DELIVERED (`MeshDataHandlerImpl.handleAckNak`). Only a real ACK (or a NAK) resolves `_wait_for_ack`; an implicit ACK keeps the wait open so a real ACK can still arrive. A definitive ACK/NAK is never downgraded by a later implicit one. Applies to DMs only (dest is a `!node` id). Values remain plain strings on `raw_response` / `get_ack_status`.

**Receipts must be read off the pubsub stream, not just the callback.** The meshtastic library's per-send `onResponse` handler is **one-shot** — `mesh_interface.py` pops it on the first routing response. An early implicit ACK therefore consumed it and the destination's *later* real ACK was never delivered to the callback, making a real ACK on a relayed path impossible to observe (16 real ACKs logged live, all `hops=0`). So `_on_receive` also handles **`ROUTING_APP`** packets — like the official client's `PortNum.ROUTING_APP -> handleRouting` dispatch — feeding them to `_record_ack_response` so late/relayed real ACKs upgrade the record. This runs **before** the self-echo and auth gates (receipts are protocol data, not user content, so the allowlist must not drop them), and `_ack_dest_for` recovers the original `dest` from the pending-ACK bookkeeping via `requestId` (unknown ids are ignored). Because the library invokes the callback *and* publishes the same packet, `_record_ack_response` dedupes on the routing packet's own `id` via `_seen_routing_packets` (bounded at `ACK_RECORD_LIMIT`).

**Optional delivery retry.** `MESHTASTIC_SEND_RETRIES > 0` makes `send()` re-send un-confirmed **DM** chunks up to N times (implies ACK-waiting). `_is_retriable_failure` retries only on **evidence of non-delivery**: `AckStatus.TIMEOUT` (nothing came back at all) or a NAK whose reason isn't in `PERMANENT_NAK_REASONS` — notably `MAX_RETRANSMIT`, the firmware's own "reliable send failed" verdict after its `NUM_RELIABLE_RETX` (3) attempts. `AckStatus.IMPLICIT_ACK` is **not** retried: the mesh carried the packet, so non-delivery isn't established and a real ACK may still arrive. Retrying on implicit is what re-sent one reply up to a dozen times on a relayed path (each app attempt is ~3 radio transmissions, plus the gateway's plain-text fallback repeating the cycle). Broadcasts are never retried. Backoff is `MESHTASTIC_RETRY_BACKOFF`; the per-chunk attempt count lands in `raw_response["chunks"][i]["attempts"]`.

`edit_message` deliberately returns unsupported — LoRa has no edit primitive, and emulating it would flood the mesh.

### Solicited requests (agent asks a node for data)

`mesh_request_telemetry`, `mesh_request_position` and `mesh_traceroute` are the only tools that **transmit**; everything else serves data already heard.

`_solicit()` is the shared path: arm a waiter via `_register_response_waiter(kind, node_id)`, send through `run_in_executor`, then `asyncio.wait_for`. `_on_receive` resolves waiters when the matching `TELEMETRY_APP` / `POSITION_APP` / `TRACEROUTE_APP` packet arrives. Waiter futures follow the same cross-loop discipline as ACK futures — created on the awaiting loop, resolved via `future.get_loop().call_soon_threadsafe` — because a tool call can run on a different loop than `connect()` did. A timeout drops the waiter (`_discard_response_waiter`) so the registry can't leak.

**Never send these through `sendTelemetry` / `sendPosition` / `sendTraceRoute`.** With `wantResponse=True` each of those library helpers calls its own `waitForX()` after posting the packet — a busy-wait on the interface `Timeout` (**300s** on TCP) that blocks the executor thread and raises `MeshInterfaceError` on expiry. That both bypasses our `timeout` entirely (execution never reaches `asyncio.wait_for`) and misreports the result as a send failure. A silent node cost one live tool call five minutes this way. Requests go out through `_post_request()` → `sendData(..., wantResponse=True, onResponse=None)`, which only serializes and posts; we already track the reply ourselves, so `onResponse` would be redundant. `MockSerialInterface` raises `AssertionError` if the blocking helpers are called, so a regression fails in tests instead of on the air.

**A dropped link abandons in-flight requests.** `_on_connection_lost` calls `_abandon_response_waiters()`, failing every pending waiter with `MeshLinkLost`, which `_solicit` reports as a link failure distinct from a silent node. Without it the agent would sit out the full 45–60s timeout waiting for a reply that can no longer arrive over a dead socket — routine on a node that drops TCP under load.

**Airtime discipline is a design constraint, not a detail.** LoRa bandwidth is shared with everyone in range, so each request is addressed to exactly ONE node, is **never retried**, and a silent node returns `answered: false` rather than raising. The schemas say so explicitly to steer the model away from mesh-wide sweeps. Traceroute is the tool of choice for diagnosing delivery: it reports the real relay chain and per-hop SNR in both directions (SNR arrives scaled by 4), which is what distinguishes a weak-direct path from a healthy relayed one.

### Connection lifecycle

`connect()` resolves connection *targets* via `_connection_targets()` and spawns one `_reconnect_loop` per target (exponential backoff, keepalive polling) plus `_drain_queue_loop`. A target is an opaque key: a serial devPath, `mock_port`, or a `tcp://host:port` URL. `_open_interface()` maps the key to a `SerialInterface`, `TCPInterface`, or `MockSerialInterface`. A configured `MESHTASTIC_TCP_HOST` takes precedence and is mutually exclusive with serial (one transport at a time). When no hardware/deps are present, it falls back to **`MockSerialInterface`** (two fake nodes) so the plugin always loads — "Plugin uses mock serial connection" means deps are missing or no port was found.

The outbound queue (`_outbound_queue`) is **in-memory only**, bounded at 100, oldest-first eviction; messages queued during a disconnect are lost if the gateway restarts before draining.

**TCP keepalive is armed by us, not the library.** `_apply_tcp_keepalive()` sets `SO_KEEPALIVE` plus idle/interval/count (30s/10s/3) on the node socket, through whichever knob the platform exposes — `TCP_KEEPIDLE` on Linux, `SIO_KEEPALIVE_VALS` via ioctl on Windows, `TCP_KEEPALIVE` on macOS. Without it a silently dead link stays "connected" until the library's **300s** heartbeat or our next failing send, whichever comes first. `TCPInterface._reconnect()` swaps in a fresh socket on any read/write failure and socket options do not survive that, so the liveness poll re-arms whenever the socket identity changes (`_keepalive_socket_id`); it is a cheap no-op otherwise.

**Drops are classified in the log.** `_note_link_drop` timestamps the outage and `_report_link_recovery` reports it on reconnect, splitting **socket resets** (back within `SOCKET_RESET_MAX_OUTAGE_SECS`, i.e. the node stayed up) from **node absences** (longer — reboot, WiFi drop, power loss), with running session totals. The distinction is the whole diagnosis: a handful of resets is normal for an ESP32 over WiFi, while repeated long absences are the node's own health and not something the adapter can fix. Log forensics of 2026-07-24 turned 16 apparent "drops" into 11 absences (user-initiated reboots) and 5 genuine resets — the counters exist so that analysis doesn't have to be redone by hand.

### Cron / standalone delivery

`_standalone_send` (wired via `cron_deliver_env_var="MESHTASTIC_HOME_CHANNEL"`) spins up a **short-lived** adapter connection with `allow_queueing=False` so cron failures surface. It does not reuse the live gateway adapter.

## Reference implementations — check these before guessing protocol semantics

Meshtastic delivery/ACK behaviour is easy to get subtly wrong from the Python
library alone (see the one-shot `onResponse` trap above). When in doubt, read
the official sources rather than inferring. On this machine they're checked out
at **`C:\GIT\MQTT\SOURCE CODE`** (plus a third-party reference, MeshRadar, at
`C:\GIT\MQTT\MeshRadar` — useful but *not* authoritative).

The parts that have already settled arguments here:

- **Official Android client** — `Meshtastic-Android/core/data/src/commonMain/kotlin/org/meshtastic/core/data/manager/MeshDataHandlerImpl.kt`
  - `handleAckNak()` — the canonical real-vs-implicit rule: `isAck && fromId == p.to` → `RECEIVED` (destination confirmed), `isAck` alone → `DELIVERED` (a relay confirmed), else `ERROR`. Also shows that a `RECEIVED` status is never downgraded, that multiple receipts per message are expected (`relays + 1`), and that the client does **no** app-level resend — a NAK just becomes `ERROR`.
  - `PortNum.ROUTING_APP -> handleRouting` — receipts come from the general portnum dispatch, not a per-send callback.
- **Firmware** — `firmware/src/mesh/NextHopRouter.cpp` / `ReliableRouter.cpp`, `firmware/src/mesh/NextHopRouter.h`
  - `NUM_RELIABLE_RETX = 3` and the `sendAckNak(MAX_RETRANSMIT, ...)` call site: `wantAck=True` makes the firmware retransmit up to 3 times itself, then emit `MAX_RETRANSMIT` locally. So one app-level retry costs ~3 radio transmissions — budget retries accordingly.
  - `mesh.pb.h` — the `Routing_Error` enum values behind `errorReason`.

## Conventions and gotchas

- **`tools.py` is loaded as the module `meshtastic_tools`**, not `tools`, to avoid colliding with Hermes' own `tools` package. `adapter._load_tools_module` and `test_meshtastic.py` both do this dynamic load; preserve it.
- **The adapter↔tools link is a module-level singleton.** `connect()` calls `tools.set_adapter(self)`; handlers reach it via `_get_adapter()`. Tools return `{"error": ...}` JSON when no adapter is active.
- **Dual imports everywhere**: every cross-module import is wrapped `try: from . import x / except ImportError: import x` to work both as a package (in Hermes) and as flat modules (in tests/CI). Keep this pattern when adding modules.
- Node IDs are `!`-prefixed 8-hex (`!da1b1613`); the allowlist matches with and without the `!`.
- Ruff config (`pyproject.toml`): line length 100, double quotes, target py311. `B008` is ignored globally; `E402` is ignored in the test file (it patches `sys.path` before importing).
- Tests use `MockSerialInterface` and a temp SQLite DB — they require Hermes importable but no real hardware.
