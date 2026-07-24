"""
Unit and Integration Test Suite for Meshtastic Platform Adapter.
"""

import asyncio
import json
import os
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

# Add CWD to system path to ensure local imports resolve
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
hermes_agent_path = os.getenv("HERMES_AGENT_PATH", os.path.expanduser("~/.hermes/hermes-agent"))
if os.path.isdir(hermes_agent_path):
    sys.path.append(hermes_agent_path)

# Register the platform inside the registry so that Platform("meshtastic") resolves correctly in venv
from gateway.platform_registry import PlatformEntry, platform_registry

platform_registry.register(
    PlatformEntry(
        name="meshtastic",
        label="Meshtastic",
        adapter_factory=lambda cfg: None,
        check_fn=lambda: True,
    )
)

import importlib.util

# Load local tools.py dynamically to prevent name collision with Hermes core tools package
tools_spec = importlib.util.spec_from_file_location(
    "meshtastic_tools", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools.py")
)
meshtastic_tools = importlib.util.module_from_spec(tools_spec)
sys.modules["meshtastic_tools"] = meshtastic_tools
tools_spec.loader.exec_module(meshtastic_tools)

import telemetry_db
from adapter import (
    HAS_MESHTASTIC,
    AckStatus,
    MeshtasticAdapter,
    MockSerialInterface,
    _env_enablement,
    _standalone_send,
)
from telemetry_db import get_position_history, get_telemetry_history, init_db

handle_mesh_list_nodes = meshtastic_tools.handle_mesh_list_nodes
handle_mesh_node_info = meshtastic_tools.handle_mesh_node_info
handle_mesh_signal_quality = meshtastic_tools.handle_mesh_signal_quality
handle_mesh_send_dm = meshtastic_tools.handle_mesh_send_dm
handle_mesh_send_broadcast = meshtastic_tools.handle_mesh_send_broadcast
handle_mesh_telemetry = meshtastic_tools.handle_mesh_telemetry
handle_mesh_telemetry_history = meshtastic_tools.handle_mesh_telemetry_history
handle_mesh_request_telemetry = meshtastic_tools.handle_mesh_request_telemetry
handle_mesh_request_position = meshtastic_tools.handle_mesh_request_position
handle_mesh_traceroute = meshtastic_tools.handle_mesh_traceroute


def _backdate_signal(node_id: str, when: float) -> None:
    """Age a node's signal rows, to exercise the direct-range expiry window."""
    import sqlite3
    from contextlib import closing

    with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
        conn.execute("UPDATE signal_quality SET timestamp = ? WHERE node_id = ?", (when, node_id))
        conn.commit()


class TestMeshtasticPlatform(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._env_patcher = patch.dict(
            os.environ,
            {
                "MESHTASTIC_SERIAL_PORT": "",
                "MESHTASTIC_BAUD_RATE": "",
                "MESHTASTIC_ALLOWED_NODES": "",
                "MESHTASTIC_ALLOWED_USERS": "",
                "MESHTASTIC_ALLOW_ALL_USERS": "",
                "MESHTASTIC_HOME_CHANNEL": "",
                "MESHTASTIC_CHUNK_BYTES": "",
                "MESHTASTIC_CHUNK_DELAY": "0",
                "MESHTASTIC_ACK_TIMEOUT": "",
                "MESHTASTIC_SEND_RETRIES": "",
                "MESHTASTIC_RETRY_BACKOFF": "0",
                "MESHTASTIC_TELEMETRY_RETENTION_DAYS": "",
            },
        )
        self._env_patcher.start()

        # Isolate SQLite database from the user's live Hermes profile.
        self._tmp_db = tempfile.NamedTemporaryFile(delete=False)
        self._tmp_db.close()
        telemetry_db.DB_PATH = self._tmp_db.name
        init_db()

        # Configure platform mock
        self.config = MagicMock()
        self.config.extra = {
            "serial_port": "mock_port",
            "baud_rate": 115200,
            "allowed_users": "!ab12cd34,!da1b1613",
            "allow_all_users": False,
            "home_channel": "meshtastic:channel:0",
        }

        # Instantiate Adapter
        self.adapter = MeshtasticAdapter(self.config)

        # Mock gateway runner's handle_message
        self.adapter.handle_message = AsyncMock()

        # Connect to mock interface
        await self.adapter.connect()
        # Give reconnect task time to initialize mock interface
        await asyncio.sleep(0.1)

    async def asyncTearDown(self):
        await self.adapter.disconnect()
        self._env_patcher.stop()
        try:
            os.unlink(self._tmp_db.name)
        except Exception:
            pass

    def test_mock_connection(self):
        """Verify mock interface connects successfully."""
        interfaces = self.adapter.get_interfaces()
        self.assertEqual(len(interfaces), 1)
        self.assertIsInstance(interfaces[0], MockSerialInterface)
        self.assertEqual(interfaces[0].getMyNodeId(), "!da1b1613")

    async def test_inbound_dm_scoping(self):
        """Test private Direct Messages create isolated DM sessions."""
        # Simulated Direct Message Packet
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "channel": 0,
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"Hello Hermes, this is a private message.",
            },
            "rxSnr": 7.5,
            "rxRssi": -95,
            "id": 12345,
        }

        # Trigger inbound handler
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])

        # Give asyncio loop a tick to process
        await asyncio.sleep(0.05)

        # Verify event creation & gateway dispatch
        self.adapter.handle_message.assert_called_once()
        event = self.adapter.handle_message.call_args[0][0]

        self.assertIn("Hello Hermes, this is a private message.", event.text)
        self.assertIn("rx_snr: 7.5 dB", event.channel_context)
        self.assertIn("rx_rssi: -95 dBm", event.channel_context)
        self.assertEqual(event.source.chat_id, "meshtastic:!ab12cd34")
        self.assertEqual(event.source.chat_type, "dm")
        self.assertEqual(event.source.user_id, "!ab12cd34")

    async def test_inbound_timestamp_from_rxtime(self):
        """MessageEvent.timestamp mirrors the packet's rxTime, not loop-drain time."""
        fixed = 1_700_000_000
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "rxTime": fixed,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"timed packet"},
            "id": 12345,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)

        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(int(event.timestamp.timestamp()), fixed)

    async def test_inbound_garbage_rxtime_still_delivers(self):
        """A skewed/garbage rxTime must never drop the message (falls back to now)."""
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "rxTime": 99_999_999_999_999,  # would make fromtimestamp raise (year overflow)
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"still here"},
            "id": 12346,
        }
        before = time.time()
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)

        self.adapter.handle_message.assert_called_once()  # message delivered, not dropped
        event = self.adapter.handle_message.call_args[0][0]
        self.assertGreaterEqual(event.timestamp.timestamp(), before - 1)  # fallback: now()

    async def test_inbound_packet_id_zero_not_treated_as_missing(self):
        """A valid (if unusual) packet id of 0 must not fall through to rxTime."""
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "rxTime": 1_700_000_000,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"id zero"},
            "id": 0,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)

        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.message_id, "0")  # not "1700000000"

    async def test_inbound_channel_scoping(self):
        """Test broadcasts create shared channel sessions (when channels enabled)."""
        self.adapter.allow_channels = True  # channels are opt-in
        # Simulated Broadcast Packet
        packet = {
            "fromId": "!ab12cd34",
            "toId": "^all",
            "channel": 0,
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"Hello mesh, this is a broadcast channel update.",
            },
            "rxSnr": 6.2,
            "rxRssi": -101,
            "id": 67890,
        }

        # Trigger inbound handler
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])

        # Give asyncio loop a tick
        await asyncio.sleep(0.05)

        # Verify event scoping
        self.adapter.handle_message.assert_called_once()
        event = self.adapter.handle_message.call_args[0][0]

        self.assertIn("Hello mesh, this is a broadcast channel update.", event.text)
        self.assertIn("rx_snr: 6.2 dB", event.channel_context)
        self.assertIn("rx_rssi: -101 dBm", event.channel_context)
        self.assertEqual(event.source.chat_id, "meshtastic:channel:Primary")
        self.assertEqual(event.source.chat_type, "group")

    def test_channel_field_dict_and_protobuf(self):
        """_channel_field reads both dict channels (mock) and protobuf ones (hw)."""
        d = {"index": 2, "name": "Alpha"}
        self.assertEqual(self.adapter._channel_field(d, "index"), 2)
        self.assertEqual(self.adapter._channel_field(d, "name"), "Alpha")
        # Protobuf Channel: no .get(), name nested under .settings.
        pb = SimpleNamespace(index=3, settings=SimpleNamespace(name="Beta"))
        self.assertEqual(self.adapter._channel_field(pb, "index"), 3)
        self.assertEqual(self.adapter._channel_field(pb, "name"), "Beta")

    async def test_broadcast_scoping_with_protobuf_channels(self):
        """Broadcast scoping must not crash on protobuf channels (real hardware).

        The old ch.get() raised AttributeError on protobuf Channel objects, so
        channel messages crashed and never reached Hermes.
        """
        self.adapter.allow_channels = True  # channels are opt-in
        iface = self.adapter.get_interfaces()[0]
        iface.localNode.channels = [
            SimpleNamespace(index=0, settings=SimpleNamespace(name="Primary")),
        ]
        packet = {
            "fromId": "!ab12cd34",  # authorized
            "toId": "^all",
            "channel": 0,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"channel hello"},
            "id": 4242,
        }
        self.adapter._on_receive(packet, iface)
        await asyncio.sleep(0.05)

        self.adapter.handle_message.assert_called_once()  # no crash, message bridged
        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.source.chat_id, "meshtastic:channel:Primary")

    async def test_channel_message_ignored_by_default(self):
        """By default the agent answers DMs only — channel messages are dropped."""
        self.assertFalse(self.adapter.allow_channels)  # default
        packet = {
            "fromId": "!ab12cd34",  # authorized node, but posting to the channel
            "toId": "^all",
            "channel": 0,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"hi channel"},
            "id": 5150,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)
        self.adapter.handle_message.assert_not_called()  # not bridged -> no public reply

    async def test_dm_still_answered_with_channels_off(self):
        """A DM is still handled when channels are disabled (the default)."""
        self.assertFalse(self.adapter.allow_channels)
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",  # DM to the gateway node
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"direct hi"},
            "id": 5151,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)
        self.adapter.handle_message.assert_called_once()
        self.assertEqual(
            self.adapter.handle_message.call_args[0][0].source.chat_id, "meshtastic:!ab12cd34"
        )

    async def test_self_echo_skipped_before_auth_gate(self):
        """Our own node's packets drop silently, not as "Unauthorized" warnings.

        The local node is normally absent from the allowlist, so running the auth
        gate first logged every self-echo as unauthorized (thousands of bogus
        warnings) and left the echo filter unreachable.
        """
        iface = self.adapter.get_interfaces()[0]
        own_id = iface.getMyNodeId()  # !da1b1613
        # Production shape: the gateway's own node is NOT in the allowlist
        # (the fixture allowlists it, which would hide the bug).
        self.adapter.allowed_nodes = {"!ab12cd34"}
        self.assertFalse(self.adapter._is_authorized_node(own_id))

        packet = {
            "fromId": own_id,
            "toId": "!ab12cd34",
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"echo of our own reply"},
            "id": 6001,
        }
        with self.assertNoLogs("adapter", level="WARNING"):
            self.adapter._on_receive(packet, iface)
            await asyncio.sleep(0.05)
        self.adapter.handle_message.assert_not_called()

    async def test_observability_recorded_for_unauthorized_nodes(self):
        """Telemetry/position/signal are recorded for EVERY heard node.

        The allowlist controls who may talk to the agent, not what the agent can
        see of the mesh. Gating these writes left the DB holding data for the one
        allowlisted node only, so the agent could report nothing current about
        any other node.
        """
        stranger = "!bad55555"  # deliberately not allowlisted
        self.assertFalse(self.adapter._is_authorized_node(stranger))
        iface = self.adapter.get_interfaces()[0]

        self.adapter._on_receive(
            {
                "fromId": stranger,
                "toId": "^all",
                "rxSnr": 5.5,
                "rxRssi": -95,
                "hopStart": 3,
                "hopLimit": 2,
                "decoded": {
                    "portnum": "TELEMETRY_APP",
                    "telemetry": {"deviceMetrics": {"batteryLevel": 77, "voltage": 4.01}},
                },
            },
            iface,
        )
        self.adapter._on_receive(
            {
                "fromId": stranger,
                "toId": "^all",
                "decoded": {
                    "portnum": "POSITION_APP",
                    "position": {"latitude": 55.75, "longitude": 37.61, "altitude": 150},
                },
            },
            iface,
        )
        await asyncio.sleep(0.15)

        self.assertTrue(telemetry_db.get_telemetry_history(stranger, limit=1))
        self.assertTrue(telemetry_db.get_position_history(stranger, limit=1))
        self.assertTrue(telemetry_db.get_signal_history(stranger, limit=1))
        # ...but its text still never reaches the agent.
        self.adapter.handle_message.assert_not_called()

    async def test_unauthorized_filter(self):
        """Verify unauthorized nodes are correctly filtered out."""
        # Packet from non-whitelisted node
        packet = {
            "fromId": "!bad55555",
            "toId": "!da1b1613",
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"Unauthorized prompt injection attempt.",
            },
        }

        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)

        # Verify handler was never called
        self.adapter.handle_message.assert_not_called()

    def test_update_observed_last_heard_and_direct_signal(self):
        """last_heard tracks rx_time; snr/rssi only from direct (0-hop) packets."""
        self.adapter._update_observed("!aaaa1111", 1_700_000_000, 5.0, -80, 0)
        obs = self.adapter.get_observed_node("!aaaa1111")
        self.assertEqual(obs["last_heard"], 1_700_000_000)
        self.assertEqual(obs["snr"], 5.0)
        self.assertEqual(obs["rssi"], -80)
        self.assertEqual(obs["hops_away"], 0)

    def test_update_observed_relayed_packet_skips_signal(self):
        """A relayed (hop>0) packet bumps last_heard but not snr/rssi."""
        self.adapter._update_observed("!bbbb2222", None, 3.0, -90, 2)
        obs = self.adapter.get_observed_node("!bbbb2222")
        self.assertGreater(obs["last_heard"], 0)
        self.assertEqual(obs["hops_away"], 2)
        self.assertNotIn("snr", obs)  # relay metrics belong to the last hop
        self.assertNotIn("rssi", obs)

    def test_update_observed_future_rxtime_clamped(self):
        """A future rx_time (clock skew) is clamped to now."""
        self.adapter._update_observed("!cccc3333", time.time() + 10_000, None, None, None)
        self.assertLessEqual(
            self.adapter.get_observed_node("!cccc3333")["last_heard"], time.time() + 1
        )

    async def test_unauthorized_node_still_observed(self):
        """An unauthorized node is filtered from Hermes but still tracked (watch-only)."""
        packet = {
            "fromId": "!9e754610",  # not in the allowlist
            "toId": "^all",
            "rxTime": int(time.time()),
            "rxSnr": 6.0,
            "rxRssi": -70,
            "hopStart": 3,
            "hopLimit": 3,  # hop_count == 0 → direct
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"watch me"},
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.02)

        self.adapter.handle_message.assert_not_called()  # still not bridged to Hermes
        obs = self.adapter.get_observed_node("!9e754610")
        self.assertGreater(obs.get("last_heard", 0), 0)  # but its freshness IS recorded
        self.assertEqual(obs.get("snr"), 6.0)

    async def test_mesh_list_nodes_prefers_fresh_last_heard(self):
        """mesh_list_nodes overlays observed last_heard over the stale library value."""
        fresher = int(time.time() - 10)  # newer than mock !ab12cd34's lastHeard (now-300)
        self.adapter._update_observed("!ab12cd34", fresher, None, None, None)
        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!ab12cd34")
        self.assertEqual(
            node["last_heard"], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fresher))
        )

    def test_observed_overlay_is_size_bounded(self):
        """The observed overlay evicts the stalest entry past its cap."""
        self.adapter.OBSERVED_NODE_LIMIT = 3
        for i in range(10):
            self.adapter._update_observed(f"!n{i:07d}", 1_700_000_000 + i, None, None, None)
        self.assertLessEqual(len(self.adapter._node_observed), 3)
        self.assertIn("!n0000009", self.adapter._node_observed)  # newest kept
        self.assertNotIn("!n0000000", self.adapter._node_observed)  # stalest evicted

    async def test_payload_splitting_on_send(self):
        """Verify outbound messages >237 chars are split into chunks."""
        long_message = "A" * 300  # Exceeds the 237 char limit

        # Mock low-level sendText
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=True)

        # Send
        res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content=long_message)

        self.assertTrue(res.success)
        # Should split into multiple LoRa-safe numbered chunks.
        self.assertGreater(iface.sendText.call_count, 1)
        calls = iface.sendText.call_args_list
        for call in calls:
            self.assertLessEqual(
                len(call[1]["text"].encode("utf-8")), self.adapter.MAX_MESSAGE_LENGTH
            )
        self.assertTrue(calls[0][1]["text"].startswith("[1/"))

    async def test_telemetry_persistence(self):
        """Test real-time telemetry logging to SQLite."""
        packet = {
            "fromId": "!ab12cd34",
            "decoded": {
                "portnum": "TELEMETRY_APP",
                "telemetry": {
                    "deviceMetrics": {
                        "batteryLevel": 88,
                        "voltage": 4.05,
                        "uptimeSeconds": 3600,
                    },
                    "environmentMetrics": {
                        "temperature": 18.5,
                        "relativeHumidity": 60.1,
                        "barometricPressure": 1012.5,
                    },
                },
            },
        }

        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.1)

        # Query persistent DB
        history = get_telemetry_history("!ab12cd34", limit=1)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["battery_level"], 88)
        self.assertEqual(history[0]["temperature"], 18.5)
        self.assertEqual(history[0]["humidity"], 60.1)
        self.assertEqual(history[0]["uptime"], 3600)

    async def test_telemetry_numeric_portnum_and_zero_metrics(self):
        """Numeric TELEMETRY_APP (67) and falsy metrics (0 / 0.0) must still log.

        Port 4 is NODEINFO_APP — must not be treated as telemetry. batteryLevel 0
        means external power on many devices and must not be dropped by `or`.
        """
        packet = {
            "fromId": "!ab12cd34",
            "decoded": {
                "portnum": 67,  # portnums_pb2.PortNum.TELEMETRY_APP
                "telemetry": {
                    "deviceMetrics": {
                        "batteryLevel": 0,
                        "voltage": 0.0,
                        "uptimeSeconds": 0,
                    },
                },
            },
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.1)

        history = get_telemetry_history("!ab12cd34", limit=1)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["battery_level"], 0)
        self.assertEqual(history[0]["voltage"], 0.0)
        self.assertEqual(history[0]["uptime"], 0)

        # NODEINFO_APP (4) must not be mis-classified as telemetry.
        before = len(get_telemetry_history("!ab12cd34", limit=10))
        self.adapter._on_receive(
            {
                "fromId": "!ab12cd34",
                "decoded": {
                    "portnum": 4,
                    "user": {"id": "!ab12cd34", "longName": "x"},
                },
            },
            self.adapter.get_interfaces()[0],
        )
        await asyncio.sleep(0.1)
        self.assertEqual(len(get_telemetry_history("!ab12cd34", limit=10)), before)

    async def test_zero_snr_is_preserved(self):
        """A direct packet with SNR 0.0 must not be treated as missing signal."""
        # Exercise the packet-path extraction (rxSnr=0 must not fall through to None).
        self.adapter.allow_all = True
        packet = {
            "fromId": "!dddd4444",
            "toId": "!da1b1613",
            "rxSnr": 0.0,
            "rxRssi": -100,
            "hopStart": 3,
            "hopLimit": 3,  # 0 hops away
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"snr zero"},
            "id": 9001,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)
        obs = self.adapter.get_observed_node("!dddd4444")
        self.assertEqual(obs.get("snr"), 0.0)
        self.assertEqual(obs.get("rssi"), -100)

    async def test_inbound_text_field_without_payload(self):
        """decoded.text alone is enough when payload bytes are absent."""
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "hello via text field"},
            "id": 9002,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)
        self.adapter.handle_message.assert_called_once()
        self.assertEqual(self.adapter.handle_message.call_args[0][0].text, "hello via text field")

    async def test_position_persistence(self):
        """Test position logging and coordinates scaling."""
        packet = {
            "fromId": "!ab12cd34",
            "decoded": {
                "portnum": "POSITION_APP",
                "position": {
                    "latitude": 426983000,  # Scaled 1e7
                    "longitude": -711234000,  # Scaled 1e7
                    "altitude": 120,
                },
            },
        }

        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.1)

        # Query persistent DB
        history = get_position_history("!ab12cd34", limit=1)
        self.assertEqual(len(history), 1)
        self.assertAlmostEqual(history[0]["latitude"], 42.6983)
        self.assertAlmostEqual(history[0]["longitude"], -71.1234)
        self.assertEqual(history[0]["altitude"], 120)

    async def test_tool_handlers(self):
        """Test executing tool handlers retrieve data correctly."""
        # 1. Test listing nodes
        res_list = await handle_mesh_list_nodes({})
        self.assertIn("Phoenix HQ", res_list)
        self.assertIn("Park Sensor Node", res_list)

        # 2. Test node info query
        res_info = await handle_mesh_node_info({"node_id": "PARK"})
        self.assertIn("SENSECAP_T1000", res_info)

        # 3. Test sending broadcast tool
        res_send = await handle_mesh_send_broadcast({"message": "Emergency alert!"})
        self.assertIn('"success": true', res_send)

    async def test_tool_handlers_accept_task_id_kwarg(self):
        """Hermes invokes tool handlers with extra kwargs (e.g. task_id)."""
        res_list = await handle_mesh_list_nodes({}, task_id="t-1")
        self.assertIn("Phoenix HQ", res_list)
        res_info = await handle_mesh_node_info({"node_id": "PARK"}, task_id="t-1")
        self.assertIn("SENSECAP_T1000", res_info)
        res_sig = await handle_mesh_signal_quality({"node_id": "!da1b1613"}, task_id="t-1")
        self.assertIn("quality", res_sig)
        res_tel = await handle_mesh_telemetry({"node_id": "PARK"}, task_id="t-1")
        self.assertIn("temperature", res_tel)
        res_hist = await handle_mesh_telemetry_history({"node_id": "PARK"}, task_id="t-1")
        self.assertIn("history", res_hist)
        res_dm = await handle_mesh_send_dm({"node_id": "PARK", "message": "hi"}, task_id="t-1")
        self.assertIn("success", res_dm)
        res_bc = await handle_mesh_send_broadcast({"message": "hi"}, task_id="t-1")
        self.assertIn("success", res_bc)

    async def test_standalone_send(self):
        """Test that cron standalone ephemeral send routes through adapter.send."""
        res = await _standalone_send(
            self.config, "meshtastic:!ab12cd34", "Cron standalone message check"
        )
        self.assertTrue(res.get("success"))

    async def test_utf8_chunking(self):
        """Verify that chunking measures UTF-8 bytes and safely splits multi-byte characters."""
        # The emoji is four UTF-8 bytes; 60 of them (240 bytes) exceed the default
        # 170-byte budget and the 233-byte protocol ceiling.
        long_emoji_msg = "💩" * 60

        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=True)

        res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content=long_emoji_msg)
        self.assertTrue(res.success)

        # Each chunk must be UTF-8 byte safe, including numbering prefixes.
        self.assertGreater(iface.sendText.call_count, 1)
        calls = iface.sendText.call_args_list
        for call in calls:
            self.assertLessEqual(
                len(call[1]["text"].encode("utf-8")), self.adapter.MAX_MESSAGE_LENGTH
            )
        reconstructed = "".join(call[1]["text"].split("] ", 1)[1] for call in calls)
        self.assertEqual(reconstructed, long_emoji_msg)

    def test_mixed_ascii_emoji_chunk_reconstruction(self):
        """Verify mixed ASCII and emoji chunks reconstruct without dropping spaces."""
        message = ("status update " * 30) + ("💩" * 40) + " final words"
        chunks = self.adapter._chunk_message(message)

        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk.encode("utf-8")), self.adapter.MAX_MESSAGE_LENGTH)

        reconstructed = "".join(chunk.split("] ", 1)[1] for chunk in chunks)
        self.assertEqual(reconstructed, message)

    def test_default_chunk_budget_is_conservative(self):
        """With no override, chunks stay within the conservative default budget.

        The raw protocol ceiling is 233 bytes, but that leaves no room for
        encrypted-DM (PKI) overhead — the radio NAKs oversized DM chunks with
        TOO_LARGE — so the default must be lower.
        """
        self.assertEqual(self.adapter.DEFAULT_CHUNK_BYTES, 170)
        self.assertEqual(self.adapter.MAX_MESSAGE_LENGTH, 233)
        # setUp leaves MESHTASTIC_CHUNK_BYTES blank → default budget applies.
        chunks = self.adapter._chunk_message("A" * 400)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk.encode("utf-8")), self.adapter.DEFAULT_CHUNK_BYTES)

    def test_declares_native_chunking(self):
        """The adapter chunks in send(), so the gateway must not truncate payloads."""
        self.assertTrue(self.adapter.splits_long_messages)

    def test_keepalive_tcp_socket(self):
        """TCP liveness follows the socket handle (None == dropped)."""
        self.assertTrue(self.adapter._interface_is_alive(SimpleNamespace(socket=object())))
        self.assertFalse(self.adapter._interface_is_alive(SimpleNamespace(socket=None)))

    def test_keepalive_serial_stream(self):
        """Serial liveness follows the pyserial stream's is_open."""
        alive = SimpleNamespace(stream=SimpleNamespace(is_open=True))
        dead = SimpleNamespace(stream=SimpleNamespace(is_open=False))
        self.assertTrue(self.adapter._interface_is_alive(alive))
        self.assertFalse(self.adapter._interface_is_alive(dead))

    def test_keepalive_isconnected_is_event_not_method(self):
        """meshtastic's isConnected is a threading.Event attribute, not a callable.

        Spec'd stub (only ``isConnected``, no socket/stream) reproduces the real
        interface layout — a plain MagicMock would make ``isConnected()`` return a
        truthy Mock and hide the regression this guards against.
        """
        event = threading.Event()
        iface = SimpleNamespace(isConnected=event)
        self.assertFalse(self.adapter._interface_is_alive(iface))  # cleared == dropped
        event.set()
        self.assertTrue(self.adapter._interface_is_alive(iface))

    def test_keepalive_mock_interface_defaults_alive(self):
        """An interface with no known liveness handle is treated as alive."""
        self.assertTrue(self.adapter._interface_is_alive(self.adapter.get_interfaces()[0]))

    def test_tcp_keepalive_armed_once_per_socket(self):
        """SO_KEEPALIVE is set on connect and re-armed only when the socket changes."""

        class FakeSocket:
            def __init__(self):
                self.opts = []
                self.ioctls = []

            def setsockopt(self, level, opt, value):
                self.opts.append((level, opt, value))

            def ioctl(self, control, args):
                self.ioctls.append((control, args))

        sock = FakeSocket()
        iface = SimpleNamespace(socket=sock)
        self.adapter._apply_tcp_keepalive(iface)
        self.assertIn((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1), sock.opts)
        # Idle/interval tuning goes through whichever knob the platform exposes.
        self.assertTrue(len(sock.opts) > 1 or sock.ioctls)

        # A second poll on the same socket must not re-issue the options.
        before = len(sock.opts) + len(sock.ioctls)
        self.adapter._apply_tcp_keepalive(iface)
        self.assertEqual(len(sock.opts) + len(sock.ioctls), before)

        # ...but the library's self-heal swaps the socket, which must re-arm.
        iface.socket = FakeSocket()
        self.adapter._apply_tcp_keepalive(iface)
        self.assertIn((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1), iface.socket.opts)

    def test_tcp_keepalive_survives_unsupported_socket(self):
        """A socket that rejects the options must not break the connection."""

        class RejectingSocket:
            def setsockopt(self, *args):
                raise OSError("not supported here")

        # Serial interfaces have no socket at all — also a no-op, not a crash.
        self.adapter._apply_tcp_keepalive(SimpleNamespace(socket=None))
        self.adapter._apply_tcp_keepalive(SimpleNamespace(socket=RejectingSocket()))
        self.assertIsNone(self.adapter._keepalive_socket_id)

    def test_link_recovery_separates_socket_resets_from_absent_nodes(self):
        """A drop is classified by how long the node stayed unreachable."""
        target = "tcp://192.168.1.69:4403"

        # Back within seconds: the node stayed up, only the socket died.
        self.adapter._note_link_drop(target)
        self.adapter._report_link_recovery(target)
        self.assertEqual(self.adapter._link_drop_counts["socket_reset"], 1)
        self.assertEqual(self.adapter._link_drop_counts["node_absent"], 0)

        # Gone for minutes: the node itself was away (reboot / WiFi / power).
        self.adapter._note_link_drop(target)
        self.adapter._link_down_since[target] -= 120
        self.adapter._report_link_recovery(target)
        self.assertEqual(self.adapter._link_drop_counts["socket_reset"], 1)
        self.assertEqual(self.adapter._link_drop_counts["node_absent"], 1)

        # A connect that never followed a drop (first ever) reports nothing.
        self.adapter._report_link_recovery(target)
        self.assertEqual(sum(self.adapter._link_drop_counts.values()), 2)
        self.assertFalse(self.adapter._link_down_since)

    async def test_send_without_queueing_fails_when_disconnected(self):
        """Verify cron-style sends do not silently queue on disconnected adapters."""
        adapter = MeshtasticAdapter(self.config)

        res = await adapter.send(
            chat_id="meshtastic:!ab12cd34",
            content="cron should fail loudly when disconnected",
            allow_queueing=False,
        )

        self.assertFalse(res.success)
        self.assertIn("queueing disabled", res.error)
        self.assertEqual(adapter._outbound_queue, [])

    async def test_numeric_node_id_normalization(self):
        """Verify numeric Meshtastic node IDs normalize to !hex IDs for sessions."""
        packet = {
            "from": 0xAB12CD34,
            "toId": "!da1b1613",
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"numeric sender id",
            },
            "id": 24680,
        }

        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)

        self.adapter.handle_message.assert_called_once()
        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.source.chat_id, "meshtastic:!ab12cd34")
        self.assertEqual(event.source.user_id, "!ab12cd34")

    def test_local_node_id_from_dict_myinfo(self):
        """Verify local node ID extraction handles dict-shaped myInfo."""
        iface = SimpleNamespace(myInfo={"my_node_num": 0xAB12CD34})

        self.assertEqual(self.adapter._get_interface_node_id(iface), "!ab12cd34")

    def test_temp_db_isolation(self):
        """Verify tests point telemetry writes at a temporary DB, not the live Hermes DB."""
        self.assertEqual(telemetry_db.DB_PATH, self._tmp_db.name)
        self.assertNotIn(".hermes/meshtastic_telemetry.db", telemetry_db.DB_PATH)

    def test_prune_deletes_only_old_rows(self):
        """prune() removes rows older than the cutoff and keeps recent ones."""
        import sqlite3
        from contextlib import closing

        from telemetry_db import prune

        now = time.time()
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            # One old (10 days ago) and one fresh signal row.
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi) VALUES (?, ?, ?, ?)",
                ("!old", now - 10 * 86400, 1.0, -100),
            )
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi) VALUES (?, ?, ?, ?)",
                ("!new", now, 5.0, -90),
            )
            conn.commit()

        deleted = prune(5.0)  # cutoff: 5 days
        self.assertEqual(deleted, 1)

        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            remaining = {r[0] for r in conn.execute("SELECT node_id FROM signal_quality")}
        self.assertNotIn("!old", remaining)
        self.assertIn("!new", remaining)

    def test_prune_disabled_when_retention_zero(self):
        """prune(0) is a no-op (retention disabled)."""
        from telemetry_db import prune

        self.assertEqual(prune(0.0), 0)

    def test_maybe_prune_throttles_and_respects_env(self):
        """maybe_prune runs at most once per interval and honors the env var."""
        import telemetry_db as tdb

        tdb._last_prune_epoch = time.time()  # just ran -> throttled
        with patch.object(tdb, "prune") as mock_prune:
            tdb.maybe_prune()
        mock_prune.assert_not_called()  # throttled within the interval

        tdb._last_prune_epoch = 0.0  # force a run
        with patch.dict(os.environ, {"MESHTASTIC_TELEMETRY_RETENTION_DAYS": "7"}):
            with patch.object(tdb, "prune") as mock_prune:
                tdb.maybe_prune()
        mock_prune.assert_called_once_with(7.0)

    async def test_send_result_uses_packet_id(self):
        """Verify SendResult exposes the packet id returned by sendText."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=98765))

        res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="packet id check")

        self.assertTrue(res.success)
        self.assertEqual(res.message_id, "98765")

    async def test_wait_for_ack_success(self):
        """Verify ACK callbacks can be awaited and exposed in SendResult."""
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            self.assertTrue(wantAck)
            self.assertIsNotNone(onResponse)
            onResponse({"decoded": {"requestId": 123456, "routing": {"errorReason": "NONE"}}})
            return SimpleNamespace(id=123456)

        iface.sendText = MagicMock(side_effect=send_text)

        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "1"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="ack check")

        self.assertTrue(res.success)
        self.assertEqual(res.message_id, "123456")
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.ACK)
        self.assertEqual(self.adapter.get_ack_status("123456")["status"], AckStatus.ACK)

    async def test_wait_for_nak_fails_send(self):
        """Verify NAK callbacks fail the send when ACK waiting is enabled."""
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            onResponse({"decoded": {"requestId": 222333, "routing": {"errorReason": "NO_ROUTE"}}})
            return SimpleNamespace(id=222333)

        iface.sendText = MagicMock(side_effect=send_text)

        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "1"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="nak check")

        self.assertFalse(res.success)
        self.assertIn("Meshtastic NAK", res.error)
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.NAK)
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["error_reason"], "NO_ROUTE")

    async def test_wait_for_ack_timeout_fails_send(self):
        """Verify missing ACK/NACK fails after the configured timeout."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=333444))

        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "0.01"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="timeout check")

        self.assertFalse(res.success)
        self.assertIn("ACK timeout", res.error)
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.TIMEOUT)

    async def test_ack_arrives_while_waiting_uses_call_soon_threadsafe(self):
        """A real ACK that arrives after the waiter is pending resolves via the loop.

        Existing tests fire onResponse inside sendText (before the future exists),
        so the early-response path in _track_pending_ack resolves the future
        immediately. This test deliberately delays the ACK until _wait_for_ack is
        already waiting, covering call_soon_threadsafe → _set_ack_future_result.
        """
        iface = self.adapter.get_interfaces()[0]
        captured: dict = {}
        pkt_id = 94001

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            # Do NOT call onResponse here — leave the waiter open.
            captured["onResponse"] = onResponse
            return SimpleNamespace(id=pkt_id)

        iface.sendText = MagicMock(side_effect=send_text)

        async def deliver_ack_after_waiter_registered():
            # Poll until _track_pending_ack has registered the future.
            for _ in range(200):
                with self.adapter._ack_lock:
                    fut = self.adapter._ack_futures.get(str(pkt_id))
                if fut is not None and not fut.done():
                    break
                await asyncio.sleep(0.01)
            else:
                self.fail("ACK future was never registered for the waiting send")

            def fire_from_background_thread():
                cb = captured.get("onResponse")
                self.assertIsNotNone(cb)
                cb(
                    {
                        "fromId": "!ab12cd34",  # real ACK from destination
                        "decoded": {
                            "requestId": pkt_id,
                            "routing": {"errorReason": "NONE"},
                        },
                    }
                )

            # Fire from a non-loop thread so call_soon_threadsafe is the real path
            # (same as meshtastic pubsub / radio callbacks).
            await asyncio.to_thread(fire_from_background_thread)

        loop = asyncio.get_running_loop()
        with (
            patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "2"}),
            patch.object(
                loop, "call_soon_threadsafe", wraps=loop.call_soon_threadsafe
            ) as threadsafe,
        ):
            # Keep the adapter's stored loop identity in sync with the patched one.
            self.adapter.loop = loop
            deliver_task = asyncio.create_task(deliver_ack_after_waiter_registered())
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="late real ack")
            await deliver_task

        self.assertTrue(res.success)
        self.assertEqual(res.message_id, str(pkt_id))
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.ACK)
        # asyncio.wait_for may also use call_soon_threadsafe; require our
        # _set_ack_future_result schedule specifically (the L1633 path).
        scheduled = [c.args[0] for c in threadsafe.call_args_list if c.args]
        self.assertIn(self.adapter._set_ack_future_result, scheduled)

    async def test_retry_resends_transient_nak_until_ack(self):
        """A transient NAK is re-sent; delivery succeeds on a later attempt."""
        iface = self.adapter.get_interfaces()[0]
        calls = {"n": 0}

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            calls["n"] += 1
            pid = 5000 + calls["n"]
            reason = "NONE" if calls["n"] >= 2 else "NO_ROUTE"  # NAK once, then ACK
            onResponse({"decoded": {"requestId": pid, "routing": {"errorReason": reason}}})
            return SimpleNamespace(id=pid)

        iface.sendText = MagicMock(side_effect=send_text)

        with patch.dict(os.environ, {"MESHTASTIC_SEND_RETRIES": "2"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="retry me")

        self.assertTrue(res.success)
        self.assertEqual(iface.sendText.call_count, 2)
        self.assertEqual(res.raw_response["chunks"][0]["attempts"], 2)

    async def test_retry_gives_up_after_max_attempts(self):
        """Persistent transient failure fails after retries+1 attempts."""
        iface = self.adapter.get_interfaces()[0]
        calls = {"n": 0}

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            calls["n"] += 1
            pid = 6000 + calls["n"]
            onResponse({"decoded": {"requestId": pid, "routing": {"errorReason": "NO_ROUTE"}}})
            return SimpleNamespace(id=pid)

        iface.sendText = MagicMock(side_effect=send_text)

        with patch.dict(os.environ, {"MESHTASTIC_SEND_RETRIES": "2"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="never lands")

        self.assertFalse(res.success)
        self.assertEqual(iface.sendText.call_count, 3)  # 1 + 2 retries
        self.assertIn("after 3 attempt", res.error)

    async def test_permanent_nak_not_retried(self):
        """A permanent NAK (e.g. TOO_LARGE) is never re-sent, even with retries on."""
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            onResponse({"decoded": {"requestId": 7001, "routing": {"errorReason": "TOO_LARGE"}}})
            return SimpleNamespace(id=7001)

        iface.sendText = MagicMock(side_effect=send_text)

        with patch.dict(os.environ, {"MESHTASTIC_SEND_RETRIES": "3"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="too big")

        self.assertFalse(res.success)
        self.assertEqual(iface.sendText.call_count, 1)  # not retried

    async def test_broadcast_not_retried(self):
        """Broadcasts have no per-recipient ACK, so retry never applies to them."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=8001))  # no ACK -> timeout

        with patch.dict(
            os.environ, {"MESHTASTIC_SEND_RETRIES": "3", "MESHTASTIC_ACK_TIMEOUT": "0.01"}
        ):
            res = await self.adapter.send(chat_id="meshtastic:channel:0", content="broadcast")

        self.assertFalse(res.success)
        self.assertEqual(iface.sendText.call_count, 1)  # single attempt, no retry

    def test_is_retriable_failure_classification(self):
        """Only ACK-observed transient failures are retriable."""
        from gateway.platforms.base import SendResult

        def r(ack):
            return SendResult(success=False, raw_response={"ack": ack} if ack else None)

        self.assertTrue(self.adapter._is_retriable_failure(r({"status": AckStatus.TIMEOUT})))
        self.assertTrue(
            self.adapter._is_retriable_failure(
                r({"status": AckStatus.NAK, "error_reason": "NO_ROUTE"})
            )
        )
        self.assertFalse(
            self.adapter._is_retriable_failure(
                r({"status": AckStatus.NAK, "error_reason": "TOO_LARGE"})
            )
        )
        # PKI / auth failures are permanent — re-sending can't fix a key problem.
        for reason in (
            "PKI_FAILED",
            "PKI_UNKNOWN_PUBKEY",
            "PKI_SEND_FAIL_PUBLIC_KEY",
            "ADMIN_PUBLIC_KEY_UNAUTHORIZED",
            "NOT_AUTHORIZED",
            "DUTY_CYCLE_LIMIT",
            "RATE_LIMIT_EXCEEDED",
        ):
            self.assertFalse(
                self.adapter._is_retriable_failure(
                    r({"status": AckStatus.NAK, "error_reason": reason})
                ),
                f"{reason} should be permanent",
            )
        self.assertFalse(self.adapter._is_retriable_failure(r({"status": AckStatus.ACK})))
        # MAX_RETRANSMIT is the firmware's own "reliable send failed" verdict —
        # evidence of non-delivery, so worth another attempt.
        self.assertTrue(
            self.adapter._is_retriable_failure(
                r({"status": AckStatus.NAK, "error_reason": "MAX_RETRANSMIT"})
            )
        )
        # An implicit ACK is NOT retried: the mesh carried the packet, so
        # non-delivery isn't established, and a real ACK may still arrive.
        self.assertFalse(self.adapter._is_retriable_failure(r({"status": AckStatus.IMPLICIT_ACK})))
        # Plain strings still match (StrEnum + public JSON surface).
        self.assertTrue(self.adapter._is_retriable_failure(r({"status": "timeout"})))
        self.assertFalse(self.adapter._is_retriable_failure(r(None)))  # pre-send error

    async def test_real_ack_from_destination_is_delivery(self):
        """A routing ACK whose sender IS the destination confirms real delivery."""
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            onResponse(
                {
                    "fromId": "!ab12cd34",  # ACK came from the destination itself
                    "decoded": {"requestId": 91001, "routing": {"errorReason": "NONE"}},
                }
            )
            return SimpleNamespace(id=91001)

        iface.sendText = MagicMock(side_effect=send_text)
        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "1"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="real ack")

        self.assertTrue(res.success)
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.ACK)
        # Wire/JSON surface stays the plain string value.
        self.assertEqual(str(res.raw_response["chunks"][0]["ack"]["status"]), "ack")

    async def test_ack_future_binds_to_awaiting_loop_not_self_loop(self):
        """The ACK future must bind to the loop that awaits it, not adapter.loop.

        A send driven from an agent session can run on a different event loop
        than the one captured at connect(). Binding the future to self.loop and
        then awaiting it on the send loop raised "future belongs to a different
        loop". The future must live on the running (send) loop, and pubsub
        resolution must marshal onto that same loop.
        """
        other_loop = asyncio.new_event_loop()
        self.addCleanup(other_loop.close)
        self.adapter.loop = other_loop  # pretend connect() ran on a different loop

        fut = self.adapter._track_pending_ack("77007", "!ab12cd34", "hi", create_future=True)
        self.assertIsNotNone(fut)
        # Bound to the loop awaiting it here, NOT adapter.loop.
        self.assertIs(fut.get_loop(), asyncio.get_running_loop())
        self.assertIsNot(fut.get_loop(), other_loop)
        # And awaiting it must not raise the cross-loop ValueError.
        self.adapter._set_ack_future_result(fut, {"status": AckStatus.ACK})
        record = await fut
        self.assertEqual(record["status"], AckStatus.ACK)

    async def test_implicit_ack_from_relay_is_not_delivery(self):
        """A routing ACK relayed by another node is implicit — not confirmed delivery."""
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            onResponse(
                {
                    "fromId": "!9e77edec",  # a RELAY, not the destination !ab12cd34
                    "decoded": {"requestId": 91002, "routing": {"errorReason": "NONE"}},
                }
            )
            return SimpleNamespace(id=91002)

        iface.sendText = MagicMock(side_effect=send_text)
        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "0.3"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="implicit ack")

        self.assertFalse(res.success)  # relay heard it, destination did not confirm
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.IMPLICIT_ACK)
        self.assertIn("implicit ACK only", res.error or "")

    async def test_implicit_ack_is_not_retried(self):
        """An implicit-only ACK must NOT trigger a re-send.

        The mesh carried the packet, so non-delivery isn't established. Retrying
        here re-sent the same reply many times on a relayed path (every copy
        actually reached the user) — that was the "answered 10 times" spam.
        """
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            onResponse(
                {
                    "fromId": "!9e77edec",  # a relay, not the destination
                    "id": 990001,
                    "decoded": {"requestId": 92001, "routing": {"errorReason": "NONE"}},
                }
            )
            return SimpleNamespace(id=92001)

        iface.sendText = MagicMock(side_effect=send_text)
        with patch.dict(
            os.environ, {"MESHTASTIC_SEND_RETRIES": "3", "MESHTASTIC_ACK_TIMEOUT": "0.3"}
        ):
            res = await self.adapter.send(
                chat_id="meshtastic:!ab12cd34", content="no retry on implicit"
            )

        self.assertFalse(res.success)  # not confirmed by the destination
        self.assertEqual(iface.sendText.call_count, 1)  # sent ONCE despite retries=3
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.IMPLICIT_ACK)

    async def test_late_real_ack_via_routing_packet_upgrades_status(self):
        """A real ACK seen on the pubsub ROUTING_APP path upgrades an implicit one.

        The library's onResponse handler is one-shot: an early implicit ACK pops
        it, so the destination's later real ACK never reached the callback. The
        official client dispatches ROUTING_APP through its general packet
        handler; we do the same, which is the only way a relayed real ACK can be
        observed at all.
        """
        dest = "!ab12cd34"
        fut = self.adapter._track_pending_ack("93001", dest, "hi", create_future=True)
        self.assertIsNotNone(fut)

        # 1) Early implicit ACK (relay) — arrives via the one-shot callback.
        self.adapter._record_ack_response(
            {
                "fromId": "!9e77edec",
                "id": 991001,
                "decoded": {"requestId": 93001, "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(self.adapter.get_ack_status("93001")["status"], AckStatus.IMPLICIT_ACK)
        self.assertFalse(fut.done())  # waiter stays open for a real ACK

        # 2) The destination's real ACK now arrives as a ROUTING_APP packet on
        #    the pubsub path — no closure carries dest, so it's recovered from
        #    the pending-ACK record via requestId.
        self.adapter._on_receive(
            {
                "fromId": dest,
                "toId": "!da1b1613",
                "id": 991002,
                "decoded": {
                    "portnum": "ROUTING_APP",
                    "requestId": 93001,
                    "routing": {"errorReason": "NONE"},
                },
            },
            self.adapter.get_interfaces()[0],
        )
        await asyncio.sleep(0.05)

        self.assertEqual(self.adapter.get_ack_status("93001")["status"], AckStatus.ACK)
        self.assertEqual((await fut)["status"], AckStatus.ACK)

    async def test_routing_receipt_deduped_across_callback_and_pubsub(self):
        """The same physical routing packet must count once, not twice.

        The library invokes onResponse AND publishes the packet to pubsub, so
        without dedupe on the routing packet's own id the first receipt would be
        processed twice.
        """
        dest = "!ab12cd34"
        self.adapter._track_pending_ack("94001", dest, "hi", create_future=False)
        pkt = {
            "fromId": "!9e77edec",
            "id": 992001,  # same routing packet id both times
            "decoded": {"portnum": "ROUTING_APP", "requestId": 94001, "routing": {}},
        }
        self.adapter._record_ack_response(pkt, dest, "hi")
        first = dict(self.adapter.get_ack_status("94001"))
        self.adapter._on_receive(pkt, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)
        # Second delivery of the same packet id changed nothing.
        self.assertEqual(self.adapter.get_ack_status("94001")["response_at"], first["response_at"])

    async def test_unknown_routing_receipt_ignored(self):
        """A receipt whose requestId isn't ours must not create a bogus record."""
        self.adapter._on_receive(
            {
                "fromId": "!ab12cd34",
                "id": 993001,
                "decoded": {
                    "portnum": "ROUTING_APP",
                    "requestId": 99999999,
                    "routing": {"errorReason": "NONE"},
                },
            },
            self.adapter.get_interfaces()[0],
        )
        await asyncio.sleep(0.05)
        self.assertIsNone(self.adapter.get_ack_status("99999999"))

    def test_retry_backoff_defensive_parsing(self):
        """_retry_backoff falls back to the default on non-numeric input."""
        with patch.dict(os.environ, {"MESHTASTIC_RETRY_BACKOFF": "2.5"}):
            self.assertEqual(self.adapter._retry_backoff(), 2.5)
        with patch.dict(os.environ, {"MESHTASTIC_RETRY_BACKOFF": "garbage"}):
            self.assertEqual(self.adapter._retry_backoff(), 5.0)  # default, no crash
        with patch.dict(os.environ, {"MESHTASTIC_RETRY_BACKOFF": ""}):
            self.assertEqual(self.adapter._retry_backoff(), 5.0)

    async def test_get_chat_info_dm_resolves_name(self):
        """get_chat_info returns the long name for a known DM node."""
        info = await self.adapter.get_chat_info("meshtastic:!ab12cd34")
        self.assertEqual(info["type"], "dm")
        self.assertEqual(info["name"], "Park Sensor Node")

    async def test_get_chat_info_dm_unknown_falls_back_to_id(self):
        """An unknown DM node falls back to its raw id as the name."""
        info = await self.adapter.get_chat_info("meshtastic:!deadbeef")
        self.assertEqual(info["type"], "dm")
        self.assertEqual(info["name"], "!deadbeef")

    async def test_get_chat_info_channel(self):
        """get_chat_info reports a channel as a group chat."""
        info = await self.adapter.get_chat_info("meshtastic:channel:Primary")
        self.assertEqual(info["type"], "group")
        self.assertIn("Primary", info["name"])

    def test_dm_policy_reflects_access_mode(self):
        """_dm_policy mirrors the active access mode for the gateway trust path."""
        # Default fixture: allowed_nodes set, allow_all False -> allowlist policy.
        self.assertTrue(self.adapter.enforces_own_access_policy)
        self.assertEqual(self.adapter._dm_policy, "allowlist")
        # Channel broadcasts pass the same intake gate -> same policy.
        self.assertEqual(self.adapter._group_policy, "allowlist")
        # allow_all flips to "open" (adapter forwards everyone).
        self.adapter.allow_all = True
        self.assertEqual(self.adapter._dm_policy, "open")
        self.assertEqual(self.adapter._group_policy, "open")
        # No allowlist + not allow_all -> "open" (adapter default-denies at intake,
        # so the gateway never sees this traffic).
        self.adapter.allow_all = False
        self.adapter.allowed_nodes = set()
        self.assertEqual(self.adapter._dm_policy, "open")

    def test_tool_event_chrome_suppressed(self):
        """format_tool_event returns None so tool progress never hits LoRa."""
        self.assertIsNone(self.adapter.format_tool_event(SimpleNamespace()))

    def test_extract_packet_id_object_and_dict_shapes(self):
        """_extract_packet_id reads id from protobuf objects and dict packets."""
        self.assertEqual(self.adapter._extract_packet_id(SimpleNamespace(id=42)), "42")
        self.assertEqual(self.adapter._extract_packet_id({"id": 99}), "99")
        self.assertIsNone(self.adapter._extract_packet_id(SimpleNamespace()))
        self.assertIsNone(self.adapter._extract_packet_id({}))

    def test_parse_reply_id_coerces_valid_int_only(self):
        """_parse_reply_id returns an int only for genuine packet-id strings."""
        self.assertEqual(self.adapter._parse_reply_id("12345"), 12345)
        self.assertIsNone(self.adapter._parse_reply_id(None))
        self.assertIsNone(self.adapter._parse_reply_id("queued"))  # synthetic marker
        self.assertIsNone(self.adapter._parse_reply_id("not-a-number"))

    def test_tcp_liveness_prefers_isconnected_over_socket(self):
        """A TCP iface mid-self-heal (socket=None, isConnected set) reads alive.

        The library clears socket during its internal reconnect but leaves
        isConnected set; tearing down on the raw socket probe would race the
        self-heal. isConnected is the authoritative signal.
        """
        import threading

        evt = threading.Event()
        evt.set()
        tcp_iface = SimpleNamespace(socket=None, isConnected=evt)
        self.assertTrue(self.adapter._interface_is_alive(tcp_iface))
        # A real drop clears isConnected -> dead.
        evt.clear()
        self.assertFalse(self.adapter._interface_is_alive(tcp_iface))

    def test_connection_lifecycle_handlers_log_without_raising(self):
        """The connection.lost/established pubsub handlers are safe no-ops."""
        with self.assertLogs("adapter", level="WARNING"):
            self.adapter._on_connection_lost(interface="tcp")
        with self.assertLogs("adapter", level="INFO"):
            self.adapter._on_connection_established(interface="tcp")

    async def test_outbound_send_threads_reply_id(self):
        """A valid reply_to is forwarded to sendText as replyId."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=555))
        await self.adapter.send(
            chat_id="meshtastic:!ab12cd34", content="reply body", reply_to="4242"
        )
        self.assertEqual(iface.sendText.call_args.kwargs["replyId"], 4242)

    async def test_outbound_send_no_reply_id_when_absent(self):
        """When reply_to is absent, sendText gets replyId=None (no threading)."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=556))
        await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="plain")
        self.assertIsNone(iface.sendText.call_args.kwargs["replyId"])

    async def test_inbound_reply_id_mapped_to_event(self):
        """decoded.replyId surfaces as MessageEvent.reply_to_message_id."""
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"a threaded reply",
                "replyId": 7788,
            },
            "id": 9001,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)
        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.reply_to_message_id, "7788")

    def test_chunk_bytes_clamped_to_protocol_ceiling(self):
        """MESHTASTIC_CHUNK_BYTES above the 233-byte ceiling is clamped down."""
        # A single-chunk payload (<= default 170) is unaffected by the override.
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "500"}):
            chunks = self.adapter._chunk_message("short message")
        self.assertEqual(chunks, ["short message"])
        # A long payload over 233 bytes must still split — never a single 500-byte chunk.
        long = "y" * 400
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "500"}):
            chunks = self.adapter._chunk_message(long)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c.encode("utf-8")), self.adapter.MAX_MESSAGE_LENGTH)

    def test_chunk_bytes_garbage_falls_back_to_default(self):
        """A non-numeric MESHTASTIC_CHUNK_BYTES falls back to the default, not crash."""
        long = "z" * 400  # exceeds the 170 default, so it must still split
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "not-a-number"}):
            chunks = self.adapter._chunk_message(long)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c.encode("utf-8")), self.adapter.DEFAULT_CHUNK_BYTES)

    def test_split_utf8_handles_no_whitespace_and_multibyte(self):
        """_split_utf8 splits long runs without spaces and respects UTF-8 boundaries."""
        # No whitespace: must still split by byte budget (char_idx<=0 path never trips).
        no_ws = "x" * 500
        parts = self.adapter._split_utf8(no_ws, 50)
        self.assertTrue(len(parts) > 1)
        self.assertEqual("".join(parts), no_ws)
        # Multi-byte: a split point must never land inside a UTF-8 character.
        multibyte = "日本語" * 50  # 3 bytes/char
        parts = self.adapter._split_utf8(multibyte, 20)
        self.assertEqual("".join(parts), multibyte)
        for p in parts:
            p.encode("utf-8")  # each part is valid UTF-8 on its own

    async def test_outbound_queue_evicts_oldest_when_disconnected(self):
        """With no interfaces, sends queue (bounded at 100) and evict oldest-first."""
        self.adapter._interfaces.clear()
        for i in range(102):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content=f"m{i}")
            self.assertTrue(res.success)
            self.assertEqual(res.message_id, "queued")
        with self.adapter._queue_lock:
            self.assertLessEqual(len(self.adapter._outbound_queue), 100)
            # First two (m0, m1) evicted; m2 is now the oldest retained.
            self.assertEqual(self.adapter._outbound_queue[0]["content"], "m2")

    async def test_named_channel_send_resolves_index(self):
        """Sending to a named channel resolves its channel index from localNode."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=4242))

        res = await self.adapter.send(chat_id="meshtastic:channel:Primary", content="hi")

        self.assertTrue(res.success)
        iface.sendText.assert_called_once()
        self.assertEqual(iface.sendText.call_args.kwargs["channelIndex"], 0)

    async def test_send_errors_known_dm_without_public_key(self):
        """Verify direct sends fail hard when node info shows no public key."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ab12cd34"]["user"]["publicKey"] = ""
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=1))

        res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="should not send")

        self.assertFalse(res.success)
        self.assertIn("no public key", res.error)
        iface.sendText.assert_not_called()

    async def test_mesh_send_dm_errors_without_public_key(self):
        """Verify the DM tool returns a hard error for missing node public keys."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ab12cd34"]["user"]["publicKey"] = ""
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=1))

        result = json.loads(await handle_mesh_send_dm({"node_id": "PARK", "message": "hello"}))

        self.assertFalse(result["success"])
        self.assertIn("public key", result["error"])
        iface.sendText.assert_not_called()

    async def test_all_tools_error_when_no_adapter(self):
        """Every mesh_* handler returns a JSON error when no adapter is active."""
        meshtastic_tools.set_adapter(None)
        try:
            calls = [
                handle_mesh_list_nodes({}),
                handle_mesh_node_info({"node_id": "!ab12cd34"}),
                handle_mesh_signal_quality({"node_id": "!ab12cd34"}),
                handle_mesh_send_dm({"node_id": "!ab12cd34", "message": "hi"}),
                handle_mesh_send_broadcast({"message": "hi"}),
                handle_mesh_telemetry({"node_id": "!ab12cd34"}),
                handle_mesh_telemetry_history({"node_id": "!ab12cd34"}),
            ]
            for coro in calls:
                result = json.loads(await coro)
                self.assertIn("error", result)
                self.assertIn("not connected", result["error"])
        finally:
            meshtastic_tools.set_adapter(self.adapter)

    async def test_tools_error_on_missing_required_params(self):
        """Handlers reject calls with missing required parameters."""
        for coro in (
            handle_mesh_node_info({}),
            handle_mesh_signal_quality({}),
            handle_mesh_send_dm({"node_id": "!ab12cd34"}),  # no message
            handle_mesh_send_dm({"message": "hi"}),  # no node_id
            handle_mesh_send_broadcast({}),
            handle_mesh_telemetry({}),
            handle_mesh_telemetry_history({}),
        ):
            result = json.loads(await coro)
            self.assertIn("error", result)
            self.assertIn("required", result["error"])

    async def test_tools_error_on_unresolved_node(self):
        """node_info and send_dm surface a clear error for unknown nodes."""
        result = json.loads(await handle_mesh_node_info({"node_id": "!deadbeef"}))
        self.assertIn("not found", result["error"])
        result = json.loads(await handle_mesh_send_dm({"node_id": "!deadbeef", "message": "x"}))
        self.assertIn("could not be resolved", result["error"])

    def test_resolve_node_lookup_paths(self):
        """resolve_node matches by id, name, numeric num — and misses cleanly."""
        resolve_node = meshtastic_tools.resolve_node
        # Empty query.
        self.assertEqual(resolve_node("", self.adapter), (None, None))
        # Numeric node-num lookup (mock PARK node num).
        _, info = resolve_node("2870135092", self.adapter)
        self.assertEqual(info["user"]["id"], "!ab12cd34")
        # Miss returns (None, None).
        self.assertEqual(resolve_node("no-such-node", self.adapter), (None, None))

    def test_assess_signal_quality_bands(self):
        """assess_signal_quality covers every SNR band."""
        assess = meshtastic_tools.assess_signal_quality
        self.assertEqual(assess(None), "Unknown")
        self.assertEqual(assess(9.0), "Excellent")
        self.assertEqual(assess(5.0), "Good")
        self.assertEqual(assess(0.0), "Fair")
        self.assertEqual(assess(-10.0), "Poor")
        self.assertEqual(assess(-20.0), "No signal")

    async def test_list_nodes_falls_back_to_signal_history(self):
        """A node with no live/observed SNR gets its signal from the DB history."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!cc001122"] = {
            "num": 1,
            "user": {"id": "!cc001122", "longName": "Historic", "shortName": "HIS"},
        }
        telemetry_db.log_signal("!cc001122", snr=2.5, rssi=-110)

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!cc001122")
        self.assertEqual(node["snr"], 2.5)
        self.assertEqual(node["rssi"], -110)

    async def test_list_nodes_marks_relayed_signal_as_not_direct(self):
        """A relayed reading must not read as direct range — the original bug.

        Asked which nodes were in direct line of sight, the agent had no hop
        data in this payload and answered by listing everything with an RSSI,
        which included nodes 1-5 hops out.
        """
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!dd001122"] = {
            "num": 2,
            "user": {"id": "!dd001122", "longName": "Far Relayed", "shortName": "FAR"},
        }
        telemetry_db.log_signal("!dd001122", snr=6.0, rssi=-100, hop_count=3)

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!dd001122")
        self.assertEqual(node["hops_away"], 3)
        self.assertFalse(node["heard_directly"])
        self.assertEqual(node["signal_source"], "relayed")
        self.assertIsNone(node["last_direct_heard"])

    async def test_list_nodes_reports_direct_node_and_prefers_direct_signal(self):
        """A 0-hop node is flagged direct, and its signal comes from a direct packet."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!dd003344"] = {
            "num": 3,
            "user": {"id": "!dd003344", "longName": "Neighbour", "shortName": "NBR"},
        }
        # Heard directly first, then via a relay: the direct reading is the one
        # that describes this node's own link, regardless of which is newer.
        telemetry_db.log_signal("!dd003344", snr=4.0, rssi=-95, hop_count=0)
        telemetry_db.log_signal("!dd003344", snr=-2.0, rssi=-115, hop_count=2)

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!dd003344")
        self.assertEqual(node["signal_source"], "direct")
        self.assertEqual(node["snr"], 4.0)
        self.assertEqual(node["rssi"], -95)
        self.assertIsNotNone(node["last_direct_heard"])

    async def test_hops_survive_a_restart_via_persisted_history(self):
        """Hop data outlives the in-memory observations a restart clears."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!dd005566"] = {
            "num": 4,
            "user": {"id": "!dd005566", "longName": "Persisted", "shortName": "PST"},
        }
        telemetry_db.log_signal("!dd005566", snr=3.0, rssi=-99, hop_count=0)
        self.adapter._node_observed.clear()  # what a gateway restart leaves behind

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!dd005566")
        self.assertEqual(node["hops_away"], 0)
        self.assertTrue(node["heard_directly"])

    async def test_unknown_hops_are_not_claimed_as_direct(self):
        """No hop information anywhere means unknown, never 'direct'."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!dd007788"] = {
            "num": 5,
            "user": {"id": "!dd007788", "longName": "Unknown Hops", "shortName": "UNK"},
            "snr": 5.0,  # library node DB reading, origin unknown
        }

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!dd007788")
        self.assertIsNone(node["hops_away"])
        self.assertFalse(node["heard_directly"])
        self.assertEqual(node["signal_source"], "unknown")

    async def test_stale_direct_reception_expires(self):
        """A node heard directly weeks ago is no longer 'in direct range'.

        Signal history is kept for 30 days, so without a window a node that has
        since moved or gone quiet would report as a neighbour forever.
        """
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ee001122"] = {
            "num": 7,
            "user": {"id": "!ee001122", "longName": "Long Gone", "shortName": "GON"},
        }
        telemetry_db.log_signal("!ee001122", snr=5.0, rssi=-90, hop_count=0)
        _backdate_signal("!ee001122", time.time() - 20 * 86400)

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!ee001122")
        self.assertFalse(node["heard_directly"])
        # The evidence is still reported — it just no longer counts as current.
        self.assertIsNotNone(node["last_direct_heard"])
        self.assertGreater(node["last_direct_heard_age_hours"], 24)

    async def test_recent_direct_reception_still_counts(self):
        """Just inside the window, a direct reception is still direct range."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ee003344"] = {
            "num": 8,
            "user": {"id": "!ee003344", "longName": "Recent", "shortName": "RCT"},
        }
        telemetry_db.log_signal("!ee003344", snr=5.0, rssi=-90, hop_count=0)
        _backdate_signal("!ee003344", time.time() - 6 * 3600)

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!ee003344")
        self.assertTrue(node["heard_directly"])

    async def test_node_info_dates_the_position_fix(self):
        """Coordinates carry their age, so a stale fix can't pass for current."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ee005566"] = {
            "num": 9,
            "user": {"id": "!ee005566", "longName": "Mapped", "shortName": "MAP"},
            "position": {
                "latitude": 55.1,
                "longitude": 61.4,
                "time": time.time() - 48 * 3600,
            },
        }

        res = json.loads(await handle_mesh_node_info({"node_id": "!ee005566"}))
        self.assertAlmostEqual(res["position_age_hours"], 48.0, delta=0.5)
        self.assertTrue(res["position_is_stale"])
        self.assertIsNotNone(res["position_time"])

    async def test_node_info_falls_back_to_recorded_position_time(self):
        """A node DB fix with no timestamp is dated from our own history."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ee007788"] = {
            "num": 10,
            "user": {"id": "!ee007788", "longName": "Undated", "shortName": "UND"},
            "position": {"latitude": 55.2, "longitude": 61.5},  # no time field
        }
        telemetry_db.log_position("!ee007788", latitude=55.2, longitude=61.5, altitude=200)

        res = json.loads(await handle_mesh_node_info({"node_id": "!ee007788"}))
        self.assertIsNotNone(res["position_time"])
        self.assertFalse(res["position_is_stale"])  # just logged

    async def test_node_info_without_position_reports_unknown_age(self):
        """No coordinates at all means no age claim either."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ee009900"] = {
            "num": 11,
            "user": {"id": "!ee009900", "longName": "Nowhere", "shortName": "NOW"},
        }

        res = json.loads(await handle_mesh_node_info({"node_id": "!ee009900"}))
        self.assertIsNone(res["position_time"])
        self.assertIsNone(res["position_age_hours"])
        self.assertIsNone(res["position_is_stale"])

    async def test_signal_quality_reports_hops_per_trend_sample(self):
        """The trend must say which samples were direct — mixing links reads as noise."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!dd009900"] = {
            "num": 6,
            "user": {"id": "!dd009900", "longName": "Trended", "shortName": "TRD"},
        }
        telemetry_db.log_signal("!dd009900", snr=5.0, rssi=-90, hop_count=0)
        telemetry_db.log_signal("!dd009900", snr=-1.0, rssi=-118, hop_count=4)

        res = json.loads(await handle_mesh_signal_quality({"node_id": "!dd009900"}))
        self.assertEqual(res["current"]["signal_source"], "direct")
        # Still in direct range even though the newest packet came via 4 relays.
        self.assertTrue(res["current"]["heard_directly"])
        self.assertEqual(res["current"]["hops_away"], 4)
        self.assertEqual({s["hops_away"] for s in res["trend_history"]}, {0, 4})

    async def test_list_nodes_dedupes_across_interfaces(self):
        """The same node seen on two interfaces appears once."""
        iface = self.adapter.get_interfaces()[0]
        self.adapter._interfaces["second_port"] = iface  # same node DB twice
        try:
            res = json.loads(await handle_mesh_list_nodes({}))
            ids = [n["node_id"] for n in res["nodes"]]
            self.assertEqual(len(ids), len(set(ids)))
        finally:
            self.adapter._interfaces.pop("second_port", None)

    async def test_signal_quality_history_fallback_and_no_data(self):
        """signal_quality falls back to DB history; errors when nothing is known."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!cc001122"] = {
            "num": 2,
            "user": {"id": "!cc001122", "longName": "Historic", "shortName": "HIS"},
        }
        # No live snr, no history -> explicit no-readings error.
        result = json.loads(await handle_mesh_signal_quality({"node_id": "!cc001122"}))
        self.assertIn("No signal quality readings", result["error"])
        # With history -> falls back to the persisted reading and builds a trend.
        telemetry_db.log_signal("!cc001122", snr=1.5, rssi=-115)
        result = json.loads(await handle_mesh_signal_quality({"node_id": "!cc001122"}))
        self.assertEqual(result["current"]["snr"], 1.5)
        self.assertEqual(len(result["trend_history"]), 1)

    async def test_telemetry_history_fallback_and_no_data(self):
        """mesh_telemetry uses DB history when node metrics are absent; errors when neither."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!cc001122"] = {
            "num": 3,
            "user": {"id": "!cc001122", "longName": "Historic", "shortName": "HIS"},
        }
        # No metrics anywhere -> error.
        result = json.loads(await handle_mesh_telemetry({"node_id": "!cc001122"}))
        self.assertIn("No telemetry data", result["error"])
        # Persisted telemetry -> served from the DB fallback.
        telemetry_db.log_telemetry("!cc001122", battery_level=77, temperature=19.5)
        result = json.loads(await handle_mesh_telemetry({"node_id": "!cc001122"}))
        self.assertEqual(result["battery_level"], 77)
        self.assertEqual(result["temperature"], 19.5)

    async def test_history_window_selects_by_time_not_row_count(self):
        """since_hours asks for a period; rows outside it are excluded."""
        now = time.time()
        for age_hours, lat in ((1, 55.1), (10, 55.2), (100, 55.3)):
            telemetry_db.log_position("!ab12cd34", latitude=lat, longitude=61.0, altitude=1)
            with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
                conn.execute(
                    "UPDATE positions SET timestamp = ? WHERE latitude = ?",
                    (now - age_hours * 3600, lat),
                )
                conn.commit()

        res = json.loads(
            await handle_mesh_telemetry_history(
                {"node_id": "!ab12cd34", "metric_type": "positions", "since_hours": 24}
            )
        )
        lats = {h["latitude"] for h in res["history"]}
        self.assertEqual(lats, {55.1, 55.2})  # the 100h-old fix is outside the window
        self.assertEqual(res["returned"], 2)
        self.assertFalse(res["truncated"])
        self.assertIsNotNone(res["oldest_returned"])

    async def test_history_window_reports_truncation(self):
        """A window denser than the cap must say so, not look complete."""
        for i in range(5):
            telemetry_db.log_position(
                "!ab12cd34", latitude=40.0 + i / 100, longitude=61.0, altitude=1
            )

        res = json.loads(
            await handle_mesh_telemetry_history(
                {"node_id": "!ab12cd34", "metric_type": "positions", "since_hours": 24, "limit": 3}
            )
        )
        self.assertEqual(res["returned"], 3)
        self.assertTrue(res["truncated"])

    async def test_history_window_rejects_nonsense_and_caps_range(self):
        """A bad since_hours errors out; an absurd one clamps to the retention period."""
        res = json.loads(
            await handle_mesh_telemetry_history({"node_id": "!ab12cd34", "since_hours": "soon"})
        )
        self.assertIn("error", res)
        res = json.loads(
            await handle_mesh_telemetry_history({"node_id": "!ab12cd34", "since_hours": -5})
        )
        self.assertIn("error", res)

        telemetry_db.log_signal("!ab12cd34", snr=1.0, rssi=-90)
        res = json.loads(
            await handle_mesh_telemetry_history(
                {
                    "node_id": "!ab12cd34",
                    "metric_type": "signal_quality",
                    "since_hours": 99999,  # far beyond retention
                }
            )
        )
        self.assertEqual(res["returned"], 1)  # clamped, not rejected

    async def test_telemetry_history_metric_types_and_limits(self):
        """telemetry_history serves all metric types, rejects bad ones, clamps limits."""
        telemetry_db.log_position("!ab12cd34", latitude=42.0, longitude=-71.0, altitude=10.0)
        telemetry_db.log_signal("!ab12cd34", snr=4.0, rssi=-98)

        res = json.loads(
            await handle_mesh_telemetry_history(
                {"node_id": "!ab12cd34", "metric_type": "positions"}
            )
        )
        self.assertEqual(res["metric_type"], "positions")
        self.assertEqual(len(res["history"]), 1)
        self.assertIn("time", res["history"][0])  # formatted timestamp added

        res = json.loads(
            await handle_mesh_telemetry_history(
                {"node_id": "!ab12cd34", "metric_type": "signal_quality", "limit": "not-a-number"}
            )
        )
        self.assertEqual(res["metric_type"], "signal_quality")  # bad limit falls back to 10
        self.assertEqual(len(res["history"]), 1)

        res = json.loads(
            await handle_mesh_telemetry_history({"node_id": "!ab12cd34", "metric_type": "bogus"})
        )
        self.assertIn("Invalid metric_type", res["error"])

    def test_ack_history_is_bounded(self):
        """Verify ACK bookkeeping does not grow without bound."""
        self.adapter.ACK_RECORD_LIMIT = 5

        for i in range(50):
            self.adapter._track_pending_ack(str(i), "!ab12cd34", "x")

        self.assertLessEqual(len(self.adapter._pending_acks), 5)
        # The most recent packet id is always retained.
        self.assertIn("49", self.adapter._pending_acks)

    async def test_edit_message_unsupported_does_not_send(self):
        """Verify edit updates are rejected instead of spamming LoRa progress messages."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=1))

        res = await self.adapter.edit_message(
            chat_id="meshtastic:!ab12cd34",
            message_id="existing",
            content="partial update",
        )

        self.assertFalse(res.success)
        self.assertIn("does not support editing", res.error)
        iface.sendText.assert_not_called()

    def test_env_parsing_prefers_allowed_nodes_alias(self):
        """Verify preferred MESHTASTIC_ALLOWED_NODES wins over legacy USERS alias."""
        with patch.dict(
            os.environ,
            {
                "MESHTASTIC_SERIAL_PORT": "mock_port",
                "MESHTASTIC_BAUD_RATE": "57600",
                "MESHTASTIC_ALLOWED_NODES": "ab12cd34",
                "MESHTASTIC_ALLOWED_USERS": "bad55555",
                "MESHTASTIC_ALLOW_ALL_USERS": "true",
                "MESHTASTIC_HOME_CHANNEL": "meshtastic:channel:0",
            },
        ):
            # Seed-from-env before the adapter expands the allowlist env for Hermes.
            env_config = _env_enablement()
            self.assertEqual(env_config["allowed_nodes"], "ab12cd34")

            config = MagicMock()
            config.extra = {}
            adapter = MeshtasticAdapter(config)
            # Hermes gateway exact-matches the env allowlist — expansion must
            # include both bang and bare forms so intake and gateway agree.
            expanded = os.environ["MESHTASTIC_ALLOWED_NODES"]
            self.assertIn("ab12cd34", expanded)
            self.assertIn("!ab12cd34", expanded)

        self.assertEqual(adapter.serial_port, "mock_port")
        self.assertEqual(adapter.baud_rate, 57600)
        self.assertTrue(adapter.allow_all)
        self.assertIn("ab12cd34", adapter.allowed_nodes)
        self.assertIn("!ab12cd34", adapter.allowed_nodes)
        self.assertNotIn("bad55555", adapter.allowed_nodes)

    def test_normalize_node_id_forms(self):
        """_normalize_node_id produces stable ! + lowercase 8-hex ids."""
        norm = MeshtasticAdapter._normalize_node_id
        self.assertEqual(norm(0xAB12CD34), "!ab12cd34")
        self.assertEqual(norm("!AB12CD34"), "!ab12cd34")
        self.assertEqual(norm("ab12cd34"), "!ab12cd34")
        self.assertEqual(norm("  !Da1b1613  "), "!da1b1613")
        self.assertIsNone(norm(None))
        self.assertIsNone(norm(""))
        # Non-hex labels are lowercased as-is (not forced into !hex form).
        self.assertEqual(norm("PARK"), "park")
        # bool is a subclass of int — must not become !00000001 / !00000000.
        self.assertEqual(norm(True), "true")
        self.assertEqual(norm(False), "false")

    async def test_wait_for_ack_timeout_does_not_overwrite_concurrent_ack(self):
        """Timeout must not stamp TIMEOUT over a real ACK that landed in the race window."""
        loop = asyncio.get_running_loop()
        self.adapter.loop = loop
        pkt_id = "race-ack-1"
        fut = loop.create_future()
        with self.adapter._ack_lock:
            self.adapter._pending_acks[pkt_id] = {
                "status": AckStatus.PENDING,
                "dest": "!ab12cd34",
            }
            self.adapter._ack_futures[pkt_id] = fut

        async def inject_ack_while_waiting():
            # Land a real ACK under the lock without resolving the future, so
            # wait_for still times out and the except path must preserve ACK.
            await asyncio.sleep(0.05)
            with self.adapter._ack_lock:
                rec = self.adapter._pending_acks[pkt_id]
                rec["status"] = AckStatus.ACK
                rec["error_reason"] = None

        injector = asyncio.create_task(inject_ack_while_waiting())
        record = await self.adapter._wait_for_ack(pkt_id, fut, 0.15)
        await injector

        self.assertEqual(record["status"], AckStatus.ACK)
        self.assertNotEqual(record.get("error_reason"), "ACK_TIMEOUT")

    async def test_wait_for_ack_timeout_stamps_pending_only(self):
        """A still-pending wait correctly becomes TIMEOUT."""
        loop = asyncio.get_running_loop()
        self.adapter.loop = loop
        pkt_id = "race-timeout-1"
        fut = loop.create_future()
        with self.adapter._ack_lock:
            self.adapter._pending_acks[pkt_id] = {
                "status": AckStatus.PENDING,
                "dest": "!ab12cd34",
            }
            self.adapter._ack_futures[pkt_id] = fut

        record = await self.adapter._wait_for_ack(pkt_id, fut, 0.05)
        self.assertEqual(record["status"], AckStatus.TIMEOUT)
        self.assertEqual(record["error_reason"], "ACK_TIMEOUT")

    def test_record_ack_does_not_downgrade_real_ack_to_implicit(self):
        """A later relay implicit ACK must not overwrite a real destination ACK."""
        dest = "!ab12cd34"
        # Real ACK from destination first.
        self.adapter._record_ack_response(
            {
                "fromId": dest,
                "decoded": {"requestId": 81001, "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(self.adapter.get_ack_status("81001")["status"], AckStatus.ACK)

        # Later implicit from a relay — keep the definitive result.
        self.adapter._record_ack_response(
            {
                "fromId": "!9e77edec",
                "decoded": {"requestId": 81001, "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(self.adapter.get_ack_status("81001")["status"], AckStatus.ACK)

    def test_record_ack_upgrades_implicit_to_real(self):
        """A real destination ACK after an implicit relay ACK upgrades status."""
        dest = "!ab12cd34"
        self.adapter._record_ack_response(
            {
                "fromId": "!9e77edec",
                "decoded": {"requestId": 81002, "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(self.adapter.get_ack_status("81002")["status"], AckStatus.IMPLICIT_ACK)

        self.adapter._record_ack_response(
            {
                "fromId": dest,
                "decoded": {"requestId": 81002, "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(self.adapter.get_ack_status("81002")["status"], AckStatus.ACK)

    async def test_record_ack_snapshot_isolates_waiter_from_later_mutation(self):
        """The future is resolved with a snapshot, not the live shared record dict."""
        loop = asyncio.get_running_loop()
        self.adapter.loop = loop
        dest = "!ab12cd34"
        pkt_id = "81003"
        fut = loop.create_future()
        with self.adapter._ack_lock:
            self.adapter._ack_futures[pkt_id] = fut

        self.adapter._record_ack_response(
            {
                "fromId": dest,
                "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        # Mutate the live store after schedule (simulates a concurrent writer).
        with self.adapter._ack_lock:
            live = self.adapter._pending_acks[pkt_id]
            live["status"] = AckStatus.NAK
            live["error_reason"] = "NO_ROUTE"

        result = await asyncio.wait_for(fut, timeout=1.0)
        # Snapshot frozen at real-ACK time must still report ACK.
        self.assertEqual(result["status"], AckStatus.ACK)
        self.assertNotEqual(result.get("error_reason"), "NO_ROUTE")
        # Live store can still show the later mutation.
        self.assertEqual(self.adapter.get_ack_status(pkt_id)["status"], AckStatus.NAK)

    async def test_inbound_uppercase_from_id_normalized(self):
        """Uppercase fromId is lowercased so Hermes allowlist exact-match works."""
        packet = {
            "fromId": "!AB12CD34",  # same node as allowlist !ab12cd34
            "toId": "!da1b1613",
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"case fold"},
            "id": 9100,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)
        self.adapter.handle_message.assert_called_once()
        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.source.user_id, "!ab12cd34")
        self.assertEqual(event.source.chat_id, "meshtastic:!ab12cd34")

    def test_get_interface_node_id_prefers_getMyNodeInfo(self):
        """Real MeshInterface exposes getMyNodeInfo, not getMyNodeId."""
        iface = MagicMock()
        # Simulate library shape: no getMyNodeId, yes getMyNodeInfo.
        del iface.getMyNodeId
        iface.getMyNodeInfo.return_value = {
            "num": 0xDA1B1613,
            "user": {"id": "!DA1B1613"},
        }
        self.assertEqual(self.adapter._get_interface_node_id(iface), "!da1b1613")

    def test_discover_serial_ports_prefers_meshtastic_findPorts(self):
        """auto discovery should use meshtastic.util.findPorts when available."""
        with patch("adapter.HAS_MESHTASTIC", True):
            with patch(
                "meshtastic.util.findPorts", return_value=["/dev/cu.usbserial-mesh"]
            ) as find_ports:
                ports = self.adapter._discover_serial_ports()
        self.assertEqual(ports, ["/dev/cu.usbserial-mesh"])
        find_ports.assert_called_once_with(True)

    def test_register_declares_gateway_authz_env(self):
        """register() wires the allowlist env vars onto the PlatformEntry."""
        from adapter import register

        captured = {}

        class FakeCtx:
            def register_platform(self, **kwargs):
                captured.update(kwargs)

            def register_tool(self, **kwargs):
                pass

        register(FakeCtx())
        self.assertEqual(captured["allowed_users_env"], "MESHTASTIC_ALLOWED_NODES")
        self.assertEqual(captured["allow_all_env"], "MESHTASTIC_ALLOW_ALL_USERS")
        self.assertEqual(captured["max_message_length"], 233)
        self.assertEqual(captured["cron_deliver_env_var"], "MESHTASTIC_HOME_CHANNEL")
        self.assertTrue(callable(captured["standalone_sender_fn"]))


class TestMeshtasticTcpTransport(unittest.IsolatedAsyncioTestCase):
    """Cover the TCP/IP transport selection and connection path."""

    _BLANK_ENV = {
        "MESHTASTIC_SERIAL_PORT": "",
        "MESHTASTIC_BAUD_RATE": "",
        "MESHTASTIC_ALLOWED_NODES": "",
        "MESHTASTIC_ALLOWED_USERS": "",
        "MESHTASTIC_ALLOW_ALL_USERS": "",
        "MESHTASTIC_HOME_CHANNEL": "",
        "MESHTASTIC_CHUNK_BYTES": "",
        "MESHTASTIC_CHUNK_DELAY": "0",
        "MESHTASTIC_ACK_TIMEOUT": "",
        "MESHTASTIC_TCP_HOST": "",
        "MESHTASTIC_TCP_PORT": "",
    }

    async def asyncSetUp(self):
        # Isolate telemetry writes (MeshtasticAdapter.__init__ calls init_db()).
        self._tmp_db = tempfile.NamedTemporaryFile(delete=False)
        self._tmp_db.close()
        telemetry_db.DB_PATH = self._tmp_db.name
        init_db()

    async def asyncTearDown(self):
        try:
            os.unlink(self._tmp_db.name)
        except Exception:
            pass

    def _adapter(self, **env):
        merged = {**self._BLANK_ENV, **env}
        with patch.dict(os.environ, merged):
            config = MagicMock()
            config.extra = {}
            return MeshtasticAdapter(config)

    def test_tcp_host_selected_as_target(self):
        """A configured TCP host produces a single tcp:// target, skipping serial."""
        adapter = self._adapter(
            MESHTASTIC_SERIAL_PORT="/dev/ttyUSB0",
            MESHTASTIC_TCP_HOST="192.168.1.50",
            MESHTASTIC_TCP_PORT="4403",
        )
        self.assertEqual(adapter.tcp_host, "192.168.1.50")
        self.assertEqual(adapter.tcp_port, 4403)
        self.assertEqual(adapter._connection_targets(), ["tcp://192.168.1.50:4403"])

    def test_serial_target_when_no_tcp_host(self):
        """Without a TCP host the adapter keeps the existing serial behaviour."""
        adapter = self._adapter(MESHTASTIC_SERIAL_PORT="/dev/ttyUSB0")
        self.assertEqual(adapter._connection_targets(), ["/dev/ttyUSB0"])

    def test_tcp_port_defaults_to_4403(self):
        adapter = self._adapter(MESHTASTIC_TCP_HOST="meshgw.local")
        self.assertEqual(adapter.tcp_port, 4403)
        self.assertEqual(adapter._connection_targets(), ["tcp://meshgw.local:4403"])

    def test_parse_tcp_target(self):
        self.assertEqual(
            MeshtasticAdapter._parse_tcp_target("tcp://192.168.1.50:4403"),
            ("192.168.1.50", 4403),
        )
        # Missing port falls back to the default.
        self.assertEqual(
            MeshtasticAdapter._parse_tcp_target("tcp://meshgw.local"),
            ("meshgw.local", 4403),
        )

    def test_ipv6_target_round_trip(self):
        """IPv6 literals are bracketed when built and unbracketed when parsed."""
        adapter = self._adapter(MESHTASTIC_TCP_HOST="2001:db8::1", MESHTASTIC_TCP_PORT="8080")
        self.assertEqual(adapter._connection_targets(), ["tcp://[2001:db8::1]:8080"])
        self.assertEqual(
            MeshtasticAdapter._parse_tcp_target("tcp://[2001:db8::1]:8080"),
            ("2001:db8::1", 8080),
        )
        # Bracketed literal without a port falls back to the default.
        self.assertEqual(
            MeshtasticAdapter._parse_tcp_target("tcp://[fe80::1]"),
            ("fe80::1", 4403),
        )

    def test_env_enablement_for_tcp_only(self):
        """The platform enables on a TCP host even without a serial port."""
        with patch.dict(os.environ, {**self._BLANK_ENV, "MESHTASTIC_TCP_HOST": "10.0.0.7"}):
            env_config = _env_enablement()
        self.assertIsNotNone(env_config)
        self.assertEqual(env_config["tcp_host"], "10.0.0.7")
        self.assertEqual(env_config["tcp_port"], 4403)

    @unittest.skipUnless(HAS_MESHTASTIC, "meshtastic library not installed")
    async def test_connect_opens_tcp_interface(self):
        """connect() routes a TCP target through TCPInterface with host/port."""
        adapter = self._adapter(MESHTASTIC_TCP_HOST="192.168.1.50", MESHTASTIC_TCP_PORT="4403")
        adapter.handle_message = AsyncMock()

        fake_iface = MagicMock()
        fake_iface.nodes = {}

        with patch("meshtastic.tcp_interface.TCPInterface", return_value=fake_iface) as tcp_ctor:
            await adapter.connect()
            await asyncio.sleep(0.1)
            try:
                tcp_ctor.assert_called_once_with(hostname="192.168.1.50", portNumber=4403)
                self.assertEqual(adapter.get_interfaces(), [fake_iface])
            finally:
                await adapter.disconnect()


if __name__ == "__main__":
    unittest.main()


class TestMeshtasticSolicitedRequests(unittest.IsolatedAsyncioTestCase):
    """Active over-the-air requests: telemetry, position, traceroute."""

    async def asyncSetUp(self):
        self._env_patcher = patch.dict(
            os.environ,
            {
                "MESHTASTIC_SERIAL_PORT": "",
                "MESHTASTIC_ALLOWED_NODES": "",
                "MESHTASTIC_CHUNK_DELAY": "0",
            },
        )
        self._env_patcher.start()
        self._tmp_db = tempfile.NamedTemporaryFile(delete=False)
        self._tmp_db.close()
        telemetry_db.DB_PATH = self._tmp_db.name
        init_db()

        self.config = MagicMock()
        self.config.extra = {
            "serial_port": "mock_port",
            "allowed_users": "!ab12cd34,!da1b1613",
            "allow_all_users": False,
        }
        self.adapter = MeshtasticAdapter(self.config)
        self.adapter.handle_message = AsyncMock()
        await self.adapter.connect()
        await asyncio.sleep(0.1)
        meshtastic_tools.set_adapter(self.adapter)

    async def asyncTearDown(self):
        meshtastic_tools.set_adapter(None)
        await self.adapter.disconnect()
        self._env_patcher.stop()
        try:
            os.unlink(self._tmp_db.name)
        except Exception:
            pass  # a background DB write may still hold the file on Windows

    def _reply_after_send(self, packet: dict, delay: float = 0.05):
        """Feed *packet* into the receive path shortly after the request goes out."""
        loop = asyncio.get_running_loop()
        loop.call_later(
            delay,
            lambda: self.adapter._on_receive(packet, self.adapter.get_interfaces()[0]),
        )

    async def test_request_telemetry_returns_fresh_metrics(self):
        """A solicited telemetry reply resolves the waiter and is reported."""
        self._reply_after_send(
            {
                "fromId": "!ab12cd34",
                "decoded": {
                    "portnum": "TELEMETRY_APP",
                    "telemetry": {"deviceMetrics": {"batteryLevel": 64, "voltage": 3.91}},
                },
            }
        )
        out = json.loads(
            await handle_mesh_request_telemetry({"node_id": "!ab12cd34", "timeout": 5})
        )
        self.assertTrue(out["answered"])
        self.assertEqual(out["battery_level"], 64)
        self.assertEqual(out["voltage"], 3.91)

    async def test_request_position_scales_protobuf_coordinates(self):
        """Coordinates arrive scaled by 1e7 and must be converted back."""
        self._reply_after_send(
            {
                "fromId": "!ab12cd34",
                "decoded": {
                    "portnum": "POSITION_APP",
                    "position": {
                        "latitude": 551885155,
                        "longitude": 613386332,
                        "altitude": 210,
                    },
                },
            }
        )
        out = json.loads(await handle_mesh_request_position({"node_id": "!ab12cd34", "timeout": 5}))
        self.assertTrue(out["answered"])
        self.assertAlmostEqual(out["latitude"], 55.1885155, places=5)
        self.assertAlmostEqual(out["longitude"], 61.3386332, places=5)

    async def test_traceroute_reports_route_and_per_hop_snr(self):
        """The route is mapped to node ids and SNR is unscaled (sent x4)."""
        self._reply_after_send(
            {
                "fromId": "!ab12cd34",
                "decoded": {
                    "portnum": "TRACEROUTE_APP",
                    "traceroute": {
                        "route": [0x9E77EDEC],
                        "snrTowards": [24, -18],  # 6.0 dB, -4.5 dB
                        "routeBack": [],
                        "snrBack": [],
                    },
                },
            }
        )
        out = json.loads(await handle_mesh_traceroute({"node_id": "!ab12cd34", "timeout": 5}))
        self.assertTrue(out["answered"])
        self.assertEqual(out["route_towards"][0]["node_id"], "!9e77edec")
        self.assertAlmostEqual(out["route_towards"][0]["snr"], 6.0)

    async def test_silent_node_times_out_without_raising(self):
        """No reply is a normal outcome — report it, don't raise or retry."""
        out = json.loads(
            await handle_mesh_request_telemetry({"node_id": "!ab12cd34", "timeout": 5})
        )
        self.assertFalse(out["answered"])
        self.assertIn("did not answer", out["error"])
        # The waiter must not leak after the timeout.
        self.assertFalse(self.adapter._response_waiters)

    async def test_request_without_adapter_reports_error(self):
        """Tools degrade gracefully when no adapter is active."""
        meshtastic_tools.set_adapter(None)
        out = json.loads(await handle_mesh_traceroute({"node_id": "!ab12cd34"}))
        self.assertIn("error", out)

    async def test_requests_never_use_the_blocking_library_helpers(self):
        """Requests go out via sendData — the sendX helpers busy-wait for 300s.

        ``sendTelemetry``/``sendPosition``/``sendTraceRoute`` call ``waitForX()``
        internally when ``wantResponse=True``, stalling the executor thread on
        the library's own Timeout and bypassing ours entirely. The mock raises
        if they are used; here we also pin the packets we do put on the air.
        """
        iface = self.adapter.get_interfaces()[0]

        # Straight at the adapter: the tool layer clamps timeouts to >= 5s, and
        # what is under test here is the packet we put on the air.
        await self.adapter.request_telemetry("!ab12cd34", timeout=0.2)
        await self.adapter.request_position("!ab12cd34", timeout=0.2)
        await self.adapter.request_traceroute("!ab12cd34", hop_limit=3, timeout=0.2)

        sent = iface.sent_data
        self.assertEqual([p["portNum"] for p in sent], [67, 3, 70])  # telemetry, position, trace
        self.assertTrue(all(p["wantResponse"] for p in sent))
        self.assertTrue(all(p["destinationId"] == "!ab12cd34" for p in sent))
        self.assertEqual(sent[2]["hopLimit"], 3)  # traceroute honours hop_limit
        # The telemetry request carries our own metrics, like the stock client.
        self.assertEqual(sent[0]["payload"].device_metrics.battery_level, 85)

    async def test_own_timeout_governs_the_wait_not_the_library(self):
        """A silent node returns after OUR timeout, not the library's 300s."""
        started = time.monotonic()
        out = await self.adapter.request_position("!ab12cd34", timeout=0.3)
        self.assertFalse(out["ok"])
        self.assertIn("did not answer", out["error"])
        self.assertLess(time.monotonic() - started, 3.0)

    async def test_dropped_link_abandons_the_wait_immediately(self):
        """A connection drop fails in-flight requests instead of waiting them out."""

        async def drop_link_soon():
            await asyncio.sleep(0.05)
            self.adapter._on_connection_lost(self.adapter.get_interfaces()[0])

        started = time.monotonic()
        task = asyncio.create_task(drop_link_soon())
        out = json.loads(
            # A timeout long enough that waiting it out would be obvious.
            await handle_mesh_request_position({"node_id": "!ab12cd34", "timeout": 30})
        )
        await task

        self.assertFalse(out["answered"])
        self.assertIn("radio link dropped", out["error"])
        self.assertLess(time.monotonic() - started, 5.0)
        self.assertFalse(self.adapter._response_waiters)  # no leak on the abandon path
