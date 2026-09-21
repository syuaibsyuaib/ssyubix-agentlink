import unittest
import asyncio
import json
import tempfile
import os
import time
from pathlib import Path

from agentlink_mcp import server


class HandleIncomingTests(unittest.TestCase):
    def setUp(self):
        self.original_agent_id = server.agent_id
        self.original_stable_agent_identity_id = server.stable_agent_identity_id
        self.original_inbox = list(server.inbox)
        self.original_current_room = server.current_room
        self.original_local_state_dir = server.local_state_dir
        self.tempdir = tempfile.TemporaryDirectory()
        server.local_state_dir = Path(self.tempdir.name)
        server.stable_agent_identity_id = server._load_or_create_stable_agent_identity_id()
        server.agent_id = None
        server.current_room = {"room_id": "ROOM42", "last_sequence": 0}
        server.inbox.clear()

    def tearDown(self):
        server.agent_id = self.original_agent_id
        server.stable_agent_identity_id = self.original_stable_agent_identity_id
        server.current_room = self.original_current_room
        server.inbox[:] = self.original_inbox
        server.local_state_dir = self.original_local_state_dir
        self.tempdir.cleanup()

    def test_welcome_updates_agent_id_and_tracks_existing_agents(self):
        server._handle_incoming(
            {
                "type": "welcome",
                "agent_id": "LOCAL123",
                "last_sequence": 4,
                "joined_at": "2026-03-08T00:00:00+00:00",
                "last_seen_at": "2026-03-08T00:00:10+00:00",
                "presence": "online",
                "heartbeat_interval_seconds": 30,
                "heartbeat_timeout_seconds": 90,
                "stable_agent_identity_id": server.stable_agent_identity_id,
                "agents": [{
                    "name": "peer-one",
                    "agent_id": "PEER1234",
                    "stable_agent_identity_id": "stable-peer-1234",
                }],
            }
        )

        self.assertEqual(server.agent_id, "LOCAL123")
        self.assertEqual(server.current_room["stable_agent_identity_id"], server.stable_agent_identity_id)
        self.assertEqual(server.current_room["last_sequence"], 4)
        self.assertEqual(server.current_room["heartbeat_interval_seconds"], 30)
        self.assertEqual(server.current_room["heartbeat_timeout_seconds"], 90)
        self.assertIn("PEER1234", server.current_room["peers"])
        self.assertEqual(
            server.current_room["peers"]["PEER1234"]["stable_agent_identity_id"],
            "stable-peer-1234",
        )
        self.assertEqual(len(server.inbox), 1)
        self.assertEqual(server.inbox[0]["event"], "agent_online")
        self.assertEqual(server.inbox[0]["agent_id"], "PEER1234")
        self.assertEqual(server.inbox[0]["stable_agent_identity_id"], "stable-peer-1234")

    def test_message_appends_message_to_inbox(self):
        server._handle_incoming(
            {
                "type": "message",
                "from_name": "peer-two",
                "from": "PEER5678",
                "content": "hello from another device",
                "msg_type": "text",
                "broadcast": True,
                "message_id": "ROOM42:7",
                "sequence": 7,
                "room_id": "ROOM42",
                "timestamp": "2026-03-07T00:00:00+00:00",
            }
        )

        self.assertEqual(len(server.inbox), 1)
        self.assertEqual(server.inbox[0]["from"], "peer-two")
        self.assertTrue(server.inbox[0]["broadcast"])
        self.assertEqual(server.inbox[0]["message_id"], "ROOM42:7")
        self.assertEqual(server.inbox[0]["sequence"], 7)
        self.assertEqual(server.current_room["last_sequence"], 7)

    def test_event_appends_room_event_to_inbox(self):
        server._handle_incoming(
            {
                "type": "event",
                "event": "agent_joined",
                "name": "peer-three",
                "agent_id": "PEER9999",
                "stable_agent_identity_id": "stable-peer-9999",
                "message_id": "ROOM42:8",
                "sequence": 8,
                "room_id": "ROOM42",
                "presence": "online",
                "joined_at": "2026-03-08T00:00:00+00:00",
                "last_seen_at": "2026-03-08T00:00:10+00:00",
                "timestamp": "2026-03-07T00:00:00+00:00",
            }
        )

        self.assertEqual(len(server.inbox), 1)
        self.assertEqual(server.inbox[0]["event"], "agent_joined")
        self.assertEqual(server.inbox[0]["from"], "peer-three")
        self.assertEqual(server.inbox[0]["message_id"], "ROOM42:8")
        self.assertEqual(server.current_room["last_sequence"], 8)
        self.assertEqual(server.current_room["peers"]["PEER9999"]["presence"], "online")
        self.assertEqual(
            server.current_room["peers"]["PEER9999"]["stable_agent_identity_id"],
            "stable-peer-9999",
        )

    def test_task_event_is_preserved_in_inbox(self):
        server._handle_incoming(
            {
                "type": "event",
                "event": "task_offered",
                "name": "planner-one",
                "agent_id": "PLANNER1",
                "stable_agent_identity_id": "stable-planner-1",
                "task_id": "TASK001",
                "task": {
                    "task_id": "TASK001",
                    "status": "waiting_for_acceptance",
                    "offered_to_agent_id": "LOCAL123",
                },
                "message_id": "ROOM42:9",
                "sequence": 9,
                "room_id": "ROOM42",
                "timestamp": "2026-03-07T00:02:00+00:00",
            }
        )

        self.assertEqual(server.inbox[0]["task_id"], "TASK001")
        self.assertEqual(server.inbox[0]["task"]["status"], "waiting_for_acceptance")

    def test_agent_left_event_removes_peer_from_snapshot(self):
        server.current_room["peers"] = {
            "PEER0001": {
                "agent_id": "PEER0001",
                "name": "peer-left",
                "presence": "online",
                "joined_at": "2026-03-08T00:00:00+00:00",
                "last_seen_at": "2026-03-08T00:00:10+00:00",
            }
        }

        server._handle_incoming(
            {
                "type": "event",
                "event": "agent_left",
                "name": "peer-left",
                "agent_id": "PEER0001",
                "message_id": "ROOM42:10",
                "sequence": 10,
                "room_id": "ROOM42",
                "presence": "offline",
                "timestamp": "2026-03-08T00:01:00+00:00",
            }
        )

        self.assertNotIn("PEER0001", server.current_room["peers"])

    def test_pong_updates_room_heartbeat_state(self):
        server._handle_incoming(
            {
                "type": "pong",
                "timestamp": "2026-03-08T00:01:00+00:00",
                "last_seen_at": "2026-03-08T00:01:00+00:00",
                "heartbeat_interval_seconds": 25,
                "heartbeat_timeout_seconds": 75,
            }
        )

        self.assertEqual(server.current_room["last_seen_at"], "2026-03-08T00:01:00+00:00")
        self.assertEqual(server.current_room["heartbeat_interval_seconds"], 25)
        self.assertEqual(server.current_room["heartbeat_timeout_seconds"], 75)
        self.assertIn("last_pong_monotonic", server.current_room)


class AckHandlingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_pending = dict(server.pending_acks)
        server.pending_acks.clear()

    def tearDown(self):
        server.pending_acks.clear()
        server.pending_acks.update(self.original_pending)

    async def test_ack_resolves_pending_future(self):
        future = asyncio.get_running_loop().create_future()
        server.pending_acks["REQ123"] = future

        server._handle_incoming(
            {
                "type": "ack",
                "request_id": "REQ123",
                "message_id": "ROOM42:9",
                "sequence": 9,
                "accepted": True,
                "delivered": True,
            }
        )

        result = await future
        self.assertEqual(result["message_id"], "ROOM42:9")
        self.assertTrue(result["delivered"])

    async def test_error_with_request_id_rejects_pending_future(self):
        future = asyncio.get_running_loop().create_future()
        server.pending_acks["REQERR"] = future

        server._handle_incoming(
            {
                "type": "error",
                "request_id": "REQERR",
                "error": "invalid capability update",
            }
        )

        with self.assertRaisesRegex(RuntimeError, "invalid capability update"):
            await future


class LocalInboxCacheTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_agent_id = server.agent_id
        self.original_stable_agent_identity_id = server.stable_agent_identity_id
        self.original_inbox = list(server.inbox)
        self.original_current_room = server.current_room
        self.original_local_state_dir = server.local_state_dir
        self.original_ws_conn = server.ws_conn
        self.original_retry_replay_task = server.retry_replay_task
        self.original_pending = dict(server.pending_acks)
        self.original_await_ack = server._await_ack
        self.original_room_cache_ttl = server.LOCAL_ROOM_CACHE_TTL_SECONDS
        self.original_room_cache_limit = server.LOCAL_ROOM_CACHE_LIMIT
        self.original_corrupt_cache_limit = server.LOCAL_CORRUPT_CACHE_LIMIT
        self.tempdir = tempfile.TemporaryDirectory()
        server.local_state_dir = Path(self.tempdir.name)
        server.stable_agent_identity_id = server._load_or_create_stable_agent_identity_id()
        server.agent_id = "LOCAL123"
        server.current_room = None
        server.ws_conn = None
        server.retry_replay_task = None
        server.pending_acks.clear()
        server.inbox.clear()

    def tearDown(self):
        server.agent_id = self.original_agent_id
        server.stable_agent_identity_id = self.original_stable_agent_identity_id
        server.current_room = self.original_current_room
        server.inbox[:] = self.original_inbox
        server.local_state_dir = self.original_local_state_dir
        server.ws_conn = self.original_ws_conn
        server.retry_replay_task = self.original_retry_replay_task
        server.pending_acks.clear()
        server.pending_acks.update(self.original_pending)
        server._await_ack = self.original_await_ack
        server.LOCAL_ROOM_CACHE_TTL_SECONDS = self.original_room_cache_ttl
        server.LOCAL_ROOM_CACHE_LIMIT = self.original_room_cache_limit
        server.LOCAL_CORRUPT_CACHE_LIMIT = self.original_corrupt_cache_limit
        self.tempdir.cleanup()

    def test_stable_agent_identity_id_persists_in_local_client_state(self):
        first = server.stable_agent_identity_id
        self.assertTrue(first)
        identity_path = server._client_identity_path()
        self.assertTrue(identity_path.exists())

        server.stable_agent_identity_id = ""
        restored = server._load_or_create_stable_agent_identity_id()

        self.assertEqual(restored, first)
        self.assertEqual(
            json.loads(identity_path.read_text(encoding="utf-8"))["stable_agent_identity_id"],
            first,
        )

    def test_persist_and_restore_local_room_cache(self):
        server.current_room = {
            "room_id": "ROOM42",
            "last_sequence": 7,
            "last_read_sequence": 5,
            "joined_at": "2026-03-09T00:00:00+00:00",
            "last_seen_at": "2026-03-09T00:00:05+00:00",
            "presence": "online",
            "heartbeat_interval_seconds": 30,
            "heartbeat_timeout_seconds": 90,
            "peers": {
                "PEER0002": {
                    "agent_id": "PEER0002",
                    "name": "peer-two",
                    "presence": "online",
                    "joined_at": "2026-03-09T00:00:01+00:00",
                    "last_seen_at": "2026-03-09T00:00:05+00:00",
                }
            },
            "retry_queue": [
                {
                    "retry_id": "retry-1",
                    "room_id": "ROOM42",
                    "action": "send",
                    "payload": {"type": "send", "to": "PEER0001", "content": "retry", "msg_type": "text"},
                    "created_at": "2026-03-09T00:00:00+00:00",
                    "updated_at": "2026-03-09T00:00:00+00:00",
                    "expires_at": "2026-03-09T06:00:00+00:00",
                    "attempts": 1,
                    "last_error": "offline",
                    "next_retry_at": "2026-03-09T00:00:05+00:00",
                }
            ],
        }
        server.inbox[:] = [
            {
                "type": "message",
                "from": "peer-one",
                "agent_id": "PEER0001",
                "content": "cached hello",
                "msg_type": "text",
                "broadcast": False,
                "message_id": "ROOM42:6",
                "sequence": 6,
                "room_id": "ROOM42",
                "timestamp": "2026-03-09T00:00:00+00:00",
            },
            {
                "type": "event",
                "event": "agent_joined",
                "from": "peer-two",
                "agent_id": "PEER0002",
                "message_id": "ROOM42:7",
                "sequence": 7,
                "room_id": "ROOM42",
                "timestamp": "2026-03-09T00:00:01+00:00",
            },
        ]

        server._persist_local_room_state()
        cache_path = server._room_cache_path("ROOM42")
        self.assertTrue(cache_path.exists())

        server.inbox[:] = []
        server.current_room = {"room_id": "ROOM42", "last_sequence": 9}
        server._restore_local_room_state("ROOM42")

        self.assertEqual(len(server.inbox), 2)
        self.assertEqual(server.current_room["last_read_sequence"], 5)
        self.assertEqual(server.current_room["local_cached_message_count"], 2)
        self.assertEqual(server.current_room["local_cached_last_sequence"], 7)
        self.assertTrue(server.current_room["local_cache_restored"])
        self.assertEqual(server.current_room["local_retry_queue_count"], 1)
        self.assertEqual(server.current_room["retry_queue"][0]["retry_id"], "retry-1")
        self.assertEqual(server.current_room["local_summary"]["peer_count"], 1)
        self.assertEqual(server.current_room["local_summary"]["retry_queue_count"], 1)
        self.assertEqual(server.current_room["local_summary"]["recent_activity"]["last_event"], "agent_joined")

    async def test_agent_read_inbox_updates_local_cursor(self):
        server.current_room = {
            "room_id": "ROOM42",
            "last_sequence": 3,
            "last_read_sequence": 2,
            "local_cache_path": str(server._room_cache_path("ROOM42")),
        }
        server.inbox[:] = [
            {
                "type": "message",
                "from": "peer-one",
                "agent_id": "PEER0001",
                "content": "msg-1",
                "msg_type": "text",
                "broadcast": False,
                "message_id": "ROOM42:1",
                "sequence": 1,
                "room_id": "ROOM42",
                "timestamp": "2026-03-09T00:00:00+00:00",
            },
            {
                "type": "message",
                "from": "peer-one",
                "agent_id": "PEER0001",
                "content": "msg-2",
                "msg_type": "text",
                "broadcast": False,
                "message_id": "ROOM42:2",
                "sequence": 2,
                "room_id": "ROOM42",
                "timestamp": "2026-03-09T00:00:01+00:00",
            },
            {
                "type": "message",
                "from": "peer-one",
                "agent_id": "PEER0001",
                "content": "msg-3",
                "msg_type": "text",
                "broadcast": False,
                "message_id": "ROOM42:3",
                "sequence": 3,
                "room_id": "ROOM42",
                "timestamp": "2026-03-09T00:00:02+00:00",
            },
        ]
        server._persist_local_room_state()

        payload = json.loads(
            await server.agent_read_inbox(
                server.ReadInboxInput(limit=10, only_unread=True, mark_read=True, clear=False)
            )
        )

        self.assertEqual(len(payload["messages"]), 1)
        self.assertEqual(payload["messages"][0]["sequence"], 3)
        self.assertEqual(payload["last_read_sequence"], 3)
        self.assertEqual(payload["unread_count"], 0)

        cached = json.loads(server._room_cache_path("ROOM42").read_text(encoding="utf-8"))
        self.assertEqual(cached["last_read_sequence"], 3)

    async def test_agent_read_inbox_clear_updates_local_cache(self):
        server.current_room = {
            "room_id": "ROOM42",
            "last_sequence": 2,
            "last_read_sequence": 0,
            "local_cache_path": str(server._room_cache_path("ROOM42")),
        }
        server.inbox[:] = [
            {
                "type": "message",
                "from": "peer-one",
                "agent_id": "PEER0001",
                "content": "msg-1",
                "msg_type": "text",
                "broadcast": False,
                "message_id": "ROOM42:1",
                "sequence": 1,
                "room_id": "ROOM42",
                "timestamp": "2026-03-09T00:00:00+00:00",
            },
            {
                "type": "message",
                "from": "peer-one",
                "agent_id": "PEER0001",
                "content": "msg-2",
                "msg_type": "text",
                "broadcast": False,
                "message_id": "ROOM42:2",
                "sequence": 2,
                "room_id": "ROOM42",
                "timestamp": "2026-03-09T00:00:01+00:00",
            },
        ]

        payload = json.loads(
            await server.agent_read_inbox(
                server.ReadInboxInput(limit=10, only_unread=False, mark_read=True, clear=True)
            )
        )

        self.assertEqual(payload["count"], 2)
        self.assertEqual(payload["last_read_sequence"], 2)
        self.assertEqual(payload["total_in_inbox"], 0)

        cached = json.loads(server._room_cache_path("ROOM42").read_text(encoding="utf-8"))
        self.assertEqual(cached["messages"], [])
        self.assertEqual(cached["last_read_sequence"], 2)

    async def test_agent_send_queues_when_disconnected(self):
        server.current_room = {
            "room_id": "ROOM42",
            "last_sequence": 0,
            "last_read_sequence": 0,
            "retry_queue": [],
        }
        server.ws_conn = None

        payload = json.loads(
            await server.agent_send(
                server.SendInput(peer_id="PEER0001", message="queued", msg_type="text")
            )
        )

        self.assertFalse(payload["success"])
        self.assertTrue(payload["queued_for_retry"])
        self.assertEqual(payload["retry_queue_count"], 1)
        self.assertEqual(server.current_room["retry_queue"][0]["action"], "send")
        self.assertEqual(server.current_room["retry_queue"][0]["payload"]["to"], "PEER0001")

    async def test_agent_send_queues_when_not_delivered(self):
        async def fake_await_ack(payload, timeout=5.0):
            return "REQ123", {
                "accepted": False,
                "delivered": False,
                "recipient_count": 0,
                "message_id": None,
                "sequence": None,
            }

        server._await_ack = fake_await_ack
        server.current_room = {
            "room_id": "ROOM42",
            "last_sequence": 0,
            "last_read_sequence": 0,
            "retry_queue": [],
        }
        server.ws_conn = object()

        payload = json.loads(
            await server.agent_send(
                server.SendInput(peer_id="PEER0001", message="queued", msg_type="text")
            )
        )

        self.assertFalse(payload["success"])
        self.assertTrue(payload["queued_for_retry"])
        self.assertEqual(server.current_room["local_retry_queue_count"], 1)

    async def test_replay_retry_queue_removes_successful_entry(self):
        async def fake_await_ack(payload, timeout=5.0):
            return "REQ123", {
                "accepted": True,
                "delivered": True,
                "recipient_count": 1,
                "message_id": "ROOM42:4",
                "sequence": 4,
            }

        server._await_ack = fake_await_ack
        server.current_room = {
            "room_id": "ROOM42",
            "last_sequence": 3,
            "last_read_sequence": 0,
            "retry_queue": [
                {
                    "retry_id": "retry-1",
                    "room_id": "ROOM42",
                    "action": "send",
                    "payload": {"type": "send", "to": "PEER0001", "content": "retry", "msg_type": "text"},
                    "created_at": "2000-01-01T00:00:00+00:00",
                    "updated_at": "2000-01-01T00:00:00+00:00",
                    "expires_at": "2099-03-09T06:00:00+00:00",
                    "attempts": 0,
                    "last_error": "offline",
                    "next_retry_at": "2000-01-01T00:00:00+00:00",
                }
            ],
        }
        server.ws_conn = object()

        await server._replay_retry_queue()

        self.assertEqual(server.current_room["retry_queue"], [])
        self.assertEqual(server.current_room["local_retry_queue_count"], 0)

    async def test_room_local_summary_returns_snapshot_for_specific_room(self):
        server.current_room = {
            "room_id": "ROOM42",
            "last_sequence": 2,
            "last_read_sequence": 1,
            "joined_at": "2026-03-09T00:00:00+00:00",
            "last_seen_at": "2026-03-09T00:00:02+00:00",
            "presence": "online",
            "heartbeat_interval_seconds": 30,
            "heartbeat_timeout_seconds": 90,
            "peers": {
                "PEER0001": {
                    "agent_id": "PEER0001",
                    "name": "peer-one",
                    "presence": "online",
                    "joined_at": "2026-03-09T00:00:01+00:00",
                    "last_seen_at": "2026-03-09T00:00:02+00:00",
                }
            },
            "retry_queue": [],
        }
        server.inbox[:] = [
            {
                "type": "message",
                "from": "peer-one",
                "agent_id": "PEER0001",
                "content": "hello from cache",
                "msg_type": "text",
                "broadcast": False,
                "message_id": "ROOM42:2",
                "sequence": 2,
                "room_id": "ROOM42",
                "timestamp": "2026-03-09T00:00:02+00:00",
            }
        ]
        server._persist_local_room_state()
        server.current_room = None

        payload = json.loads(
            await server.room_local_summary(server.LocalRoomSummaryInput(room_id="ROOM42"))
        )

        self.assertTrue(payload["success"])
        self.assertEqual(payload["summary"]["room_id"], "ROOM42")
        self.assertEqual(payload["summary"]["peer_count"], 1)
        self.assertEqual(payload["summary"]["unread_count"], 1)
        self.assertEqual(payload["summary"]["recent_activity"]["last_message_preview"], "hello from cache")

    async def test_room_local_summary_lists_cached_rooms_when_offline(self):
        server.current_room = {
            "room_id": "ROOM42",
            "last_sequence": 1,
            "last_read_sequence": 0,
            "retry_queue": [],
        }
        server.inbox[:] = [
            {
                "type": "message",
                "from": "peer-one",
                "agent_id": "PEER0001",
                "content": "hello",
                "msg_type": "text",
                "broadcast": False,
                "message_id": "ROOM42:1",
                "sequence": 1,
                "room_id": "ROOM42",
                "timestamp": "2026-03-09T00:00:00+00:00",
            }
        ]
        server._persist_local_room_state()
        server.current_room = None
        server.inbox[:] = []

        payload = json.loads(
            await server.room_local_summary(server.LocalRoomSummaryInput())
        )

        self.assertTrue(payload["success"])
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["rooms"][0]["room_id"], "ROOM42")
        self.assertIn("summary", payload["rooms"][0])

    def test_local_summary_marks_stale_snapshot(self):
        summary = server._build_local_room_summary(
            room_id="ROOM99",
            room_state={},
            messages=[],
            retry_queue=[],
            cached_at="2000-01-01T00:00:00+00:00",
        )

        self.assertTrue(summary["is_stale"])
        self.assertGreater(summary["age_seconds"], 0)

    def test_load_local_room_state_quarantines_corrupt_cache(self):
        cache_path = server._room_cache_path("ROOM42")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("{invalid json", encoding="utf-8")

        cached = server._load_local_room_state("ROOM42")

        self.assertFalse(cached["restored"])
        self.assertTrue(cached["recovered_from_corrupt_cache"])
        self.assertIsNotNone(cached["corrupt_cache_path"])
        self.assertFalse(cache_path.exists())
        self.assertTrue(Path(cached["corrupt_cache_path"]).exists())

    def test_restore_local_room_state_compacts_duplicate_messages(self):
        payload = {
            "version": server.LOCAL_STATE_VERSION,
            "server": server.AGENTLINK_URL,
            "room_id": "ROOM42",
            "cached_at": "2026-03-09T00:00:05+00:00",
            "last_sequence": 3,
            "last_read_sequence": 1,
            "messages": [
                {
                    "type": "message",
                    "from": "peer-one",
                    "agent_id": "PEER1",
                    "content": "hello",
                    "msg_type": "text",
                    "broadcast": False,
                    "message_id": "ROOM42:2",
                    "sequence": 2,
                    "room_id": "ROOM42",
                    "timestamp": "2026-03-09T00:00:02+00:00",
                },
                {
                    "type": "message",
                    "from": "peer-one",
                    "agent_id": "PEER1",
                    "content": "hello duplicate",
                    "msg_type": "text",
                    "broadcast": False,
                    "message_id": "ROOM42:2",
                    "sequence": 2,
                    "room_id": "ROOM42",
                    "timestamp": "2026-03-09T00:00:02+00:00",
                },
                {
                    "type": "event",
                    "event": "agent_joined",
                    "from": "peer-two",
                    "agent_id": "PEER2",
                    "message_id": "ROOM42:3",
                    "sequence": 3,
                    "room_id": "ROOM42",
                    "timestamp": "2026-03-09T00:00:03+00:00",
                },
                "bad-entry",
            ],
            "retry_queue": [],
            "summary": {},
        }
        server._write_json_file(server._room_cache_path("ROOM42"), payload)
        server.current_room = {"room_id": "ROOM42", "last_sequence": 3}
        server.inbox[:] = []

        server._restore_local_room_state("ROOM42")

        self.assertEqual(len(server.inbox), 2)
        self.assertEqual(server.inbox[0]["message_id"], "ROOM42:2")
        self.assertEqual(server.inbox[1]["message_id"], "ROOM42:3")

    def test_prune_local_cache_files_removes_stale_and_overflow_entries(self):
        server.LOCAL_ROOM_CACHE_TTL_SECONDS = 3600
        server.LOCAL_ROOM_CACHE_LIMIT = 1

        room_a = server._room_cache_path("ROOMA")
        room_b = server._room_cache_path("ROOMB")
        room_c = server._room_cache_path("ROOMC")
        for index, room_path in enumerate([room_a, room_b, room_c], start=1):
            server._write_json_file(room_path, {
                "version": server.LOCAL_STATE_VERSION,
                "server": server.AGENTLINK_URL,
                "room_id": room_path.stem,
                "cached_at": "2026-03-09T00:00:00+00:00",
                "last_sequence": index,
                "last_read_sequence": 0,
                "messages": [],
                "retry_queue": [],
                "summary": {},
            })

        now = time.time()
        os.utime(room_a, (now - 7200, now - 7200))
        os.utime(room_b, (now - 200, now - 200))
        os.utime(room_c, (now - 100, now - 100))

        server._prune_local_cache_files()

        self.assertFalse(room_a.exists())
        self.assertFalse(room_b.exists())
        self.assertTrue(room_c.exists())


class _FakeCapabilityResponse:
    def __init__(self, *, status: int, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    async def text(self):
        return str(self._payload)


class _FakeCapabilitySession:
    def __init__(self, response: _FakeCapabilityResponse):
        self.response = response
        self.calls = []

    def get(self, url, params=None):
        self.calls.append({"url": url, "params": params or {}})
        return self.response


class CapabilityResourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_http_session = server.http_session
        self.original_room_credentials = server.room_credentials
        server.room_credentials = {"room_id": "ROOM42", "token": "SECRET123"}

    def tearDown(self):
        server.http_session = self.original_http_session
        server.room_credentials = self.original_room_credentials

    def test_room_resource_auth_params_only_exposes_current_room_token(self):
        self.assertEqual(
            server._room_resource_auth_params("room42"),
            {"token": "SECRET123"},
        )
        self.assertEqual(
            server._room_resource_auth_params("ROOM99"),
            {},
        )

    async def test_fetch_capability_resource_uses_room_token_when_available(self):
        fake_session = _FakeCapabilitySession(
            _FakeCapabilityResponse(
                status=200,
                payload={"success": True, "room_id": "ROOM42", "count": 1, "agents": []},
            )
        )
        server.http_session = fake_session

        payload = await server._fetch_capability_resource("room42", "agents")

        self.assertTrue(payload["success"])
        self.assertEqual(fake_session.calls[0]["url"], f"{server.AGENTLINK_URL}/capabilities/ROOM42/agents")
        self.assertEqual(fake_session.calls[0]["params"], {"token": "SECRET123"})

    async def test_fetch_task_resource_uses_room_token_when_available(self):
        fake_session = _FakeCapabilitySession(
            _FakeCapabilityResponse(
                status=200,
                payload={"success": True, "room_id": "ROOM42", "count": 1, "tasks": []},
            )
        )
        server.http_session = fake_session

        payload = await server._fetch_task_resource("room42")

        self.assertTrue(payload["success"])
        self.assertEqual(fake_session.calls[0]["url"], f"{server.AGENTLINK_URL}/tasks/ROOM42")
        self.assertEqual(fake_session.calls[0]["params"], {"token": "SECRET123"})

    async def test_fetch_capability_resource_raises_for_private_room_without_token(self):
        server.room_credentials = None
        fake_session = _FakeCapabilitySession(
            _FakeCapabilityResponse(
                status=403,
                payload={"success": False, "error": "Room ID or token is wrong."},
            )
        )
        server.http_session = fake_session

        with self.assertRaisesRegex(RuntimeError, "Room ID or token is wrong"):
            await server._fetch_capability_resource("ROOM42", "agents")


class CapabilityToolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_current_room = server.current_room
        self.original_ws_conn = server.ws_conn
        self.original_agent_id = server.agent_id
        self.original_stable_agent_identity_id = server.stable_agent_identity_id
        self.original_await_ack = server._await_ack
        self.original_fetch_self = server._fetch_self_capability_profile
        server.current_room = {"room_id": "ROOM42"}
        server.ws_conn = object()
        server.agent_id = "SELF1234"
        server.stable_agent_identity_id = "stable-self-1234"

    def tearDown(self):
        server.current_room = self.original_current_room
        server.ws_conn = self.original_ws_conn
        server.agent_id = self.original_agent_id
        server.stable_agent_identity_id = self.original_stable_agent_identity_id
        server._await_ack = self.original_await_ack
        server._fetch_self_capability_profile = self.original_fetch_self

    async def test_capability_upsert_self_returns_updated_profile(self):
        async def fake_await_ack(payload, timeout=5.0):
            self.assertEqual(payload["type"], "capability_upsert")
            self.assertEqual(payload["summary"], "Codes and reviews")
            return "REQ123", {
                "accepted": True,
                "message_id": "ROOM42:9",
                "sequence": 9,
            }

        async def fake_fetch_self():
            return {
                "success": True,
                "room_id": "ROOM42",
                "agent": {
                    "agent_id": "SELF1234",
                    "summary": "Codes and reviews",
                },
            }

        server._await_ack = fake_await_ack
        server._fetch_self_capability_profile = fake_fetch_self

        payload = json.loads(
            await server.capability_upsert_self(
                server.CapabilityUpsertInput(summary="Codes and reviews")
            )
        )

        self.assertTrue(payload["success"])
        self.assertEqual(payload["my_agent_id"], "SELF1234")
        self.assertEqual(payload["agent"]["summary"], "Codes and reviews")
        self.assertEqual(payload["resource_uri"], "ssyubix://rooms/ROOM42/agents/SELF1234")

    async def test_capability_set_availability_updates_via_room_ack(self):
        async def fake_await_ack(payload, timeout=5.0):
            self.assertEqual(payload["type"], "capability_set_availability")
            self.assertEqual(payload["availability"], "busy")
            self.assertEqual(payload["current_load"], 2)
            return "REQ124", {
                "accepted": True,
                "message_id": "ROOM42:10",
                "sequence": 10,
            }

        async def fake_fetch_self():
            return {
                "success": True,
                "room_id": "ROOM42",
                "agent": {
                    "agent_id": "SELF1234",
                    "availability": "busy",
                    "current_load": 2,
                },
            }

        server._await_ack = fake_await_ack
        server._fetch_self_capability_profile = fake_fetch_self

        payload = json.loads(
            await server.capability_set_availability(
                server.CapabilityAvailabilityInput(availability="busy", current_load=2)
            )
        )

        self.assertTrue(payload["success"])
        self.assertEqual(payload["agent"]["availability"], "busy")
        self.assertEqual(payload["agent"]["current_load"], 2)

    async def test_capability_remove_self_returns_fallback_profile(self):
        async def fake_await_ack(payload, timeout=5.0):
            self.assertEqual(payload["type"], "capability_remove")
            return "REQ125", {
                "accepted": True,
                "message_id": "ROOM42:11",
                "sequence": 11,
            }

        async def fake_fetch_self():
            return {
                "success": True,
                "room_id": "ROOM42",
                "agent": {
                    "agent_id": "SELF1234",
                    "summary": "",
                    "skills": [],
                },
            }

        server._await_ack = fake_await_ack
        server._fetch_self_capability_profile = fake_fetch_self

        payload = json.loads(await server.capability_remove_self())

        self.assertTrue(payload["success"])
        self.assertIn("Custom capability profile removed", payload["message"])
        self.assertEqual(payload["agent"]["skills"], [])


class TaskToolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_current_room = server.current_room
        self.original_ws_conn = server.ws_conn
        self.original_agent_id = server.agent_id
        self.original_stable_agent_identity_id = server.stable_agent_identity_id
        self.original_await_ack = server._await_ack
        self.original_fetch_task_by_id = server._fetch_task_by_id
        self.original_fetch_task_resource = server._fetch_task_resource
        server.current_room = {"room_id": "ROOM42"}
        server.ws_conn = object()
        server.agent_id = "SELF1234"
        server.stable_agent_identity_id = "stable-self-1234"

    def tearDown(self):
        server.current_room = self.original_current_room
        server.ws_conn = self.original_ws_conn
        server.agent_id = self.original_agent_id
        server.stable_agent_identity_id = self.original_stable_agent_identity_id
        server._await_ack = self.original_await_ack
        server._fetch_task_by_id = self.original_fetch_task_by_id
        server._fetch_task_resource = self.original_fetch_task_resource

    async def test_task_offer_returns_task_manifest_after_ack(self):
        async def fake_await_ack(payload, timeout=5.0):
            self.assertEqual(payload["type"], "task_offer")
            self.assertEqual(payload["title"], "Review checklist")
            self.assertEqual(payload["to_agent_id"], "PEER1234")
            return "REQ200", {
                "accepted": True,
                "message_id": "ROOM42:20",
                "sequence": 20,
                "task_id": "TASK001",
            }

        async def fake_fetch_task(room_id, task_id):
            self.assertEqual(room_id, "ROOM42")
            self.assertEqual(task_id, "TASK001")
            return {
                "success": True,
                "room_id": "ROOM42",
                "task": {
                    "task_id": "TASK001",
                    "title": "Review checklist",
                    "status": "waiting_for_acceptance",
                },
            }

        server._await_ack = fake_await_ack
        server._fetch_task_by_id = fake_fetch_task

        payload = json.loads(
            await server.task_offer(
                server.TaskOfferInput(
                    title="Review checklist",
                    to_agent_id="PEER1234",
                    priority="high",
                )
            )
        )

        self.assertTrue(payload["success"])
        self.assertEqual(payload["task_id"], "TASK001")
        self.assertEqual(payload["delegated_by"], "SELF1234")
        self.assertEqual(payload["delegated_by_stable_identity_id"], "stable-self-1234")
        self.assertEqual(payload["task"]["status"], "waiting_for_acceptance")

    async def test_task_accept_reads_updated_task_after_ack(self):
        async def fake_await_ack(payload, timeout=5.0):
            self.assertEqual(payload["type"], "task_accept")
            self.assertEqual(payload["task_id"], "TASK001")
            return "REQ201", {
                "accepted": True,
                "message_id": "ROOM42:21",
                "sequence": 21,
                "task_id": "TASK001",
            }

        async def fake_fetch_task(room_id, task_id):
            return {
                "success": True,
                "room_id": room_id,
                "task": {
                    "task_id": task_id,
                    "status": "accepted",
                    "responsible_agent_id": "SELF1234",
                    "responsible_identity_id": "stable-self-1234",
                },
            }

        server._await_ack = fake_await_ack
        server._fetch_task_by_id = fake_fetch_task

        payload = json.loads(await server.task_accept(server.TaskTransitionInput(task_id="TASK001")))

        self.assertTrue(payload["success"])
        self.assertEqual(payload["task"]["status"], "accepted")
        self.assertEqual(payload["task"]["responsible_identity_id"], "stable-self-1234")

    async def test_task_defer_forwards_reason_and_schedule_hint(self):
        async def fake_await_ack(payload, timeout=5.0):
            self.assertEqual(payload["type"], "task_defer")
            self.assertEqual(payload["reason"], "Busy right now")
            self.assertEqual(payload["deferred_until"], "2026-03-10T09:00:00+00:00")
            return "REQ202", {
                "accepted": True,
                "message_id": "ROOM42:22",
                "sequence": 22,
                "task_id": "TASK001",
            }

        async def fake_fetch_task(room_id, task_id):
            return {
                "success": True,
                "room_id": room_id,
                "task": {
                    "task_id": task_id,
                    "status": "deferred",
                    "response_reason": "Busy right now",
                    "deferred_until": "2026-03-10T09:00:00+00:00",
                },
            }

        server._await_ack = fake_await_ack
        server._fetch_task_by_id = fake_fetch_task

        payload = json.loads(
            await server.task_defer(
                server.TaskDeferInput(
                    task_id="TASK001",
                    reason="Busy right now",
                    deferred_until="2026-03-10T09:00:00+00:00",
                )
            )
        )

        self.assertTrue(payload["success"])
        self.assertEqual(payload["task"]["status"], "deferred")
        self.assertEqual(payload["task"]["response_reason"], "Busy right now")

    async def test_task_list_returns_room_task_manifest(self):
        async def fake_fetch_task_resource(room_id, resource_path=""):
            self.assertEqual(room_id, "ROOM42")
            self.assertEqual(resource_path, "")
            return {
                "success": True,
                "room_id": room_id,
                "count": 1,
                "tasks": [{"task_id": "TASK001", "title": "Review checklist"}],
            }

        server._fetch_task_resource = fake_fetch_task_resource

        payload = json.loads(await server.task_list())

        self.assertTrue(payload["success"])
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["tasks"][0]["task_id"], "TASK001")


class OnboardingGuideTests(unittest.TestCase):
    def test_readme_first_resource_mentions_core_best_practices(self):
        markdown = server.readme_first_resource()
        self.assertIn("agent_register", markdown)
        self.assertIn("room_join", markdown)
        self.assertIn("capability_upsert_self", markdown)
        self.assertIn("ssyubix://guides/readme-first", markdown)

    def test_readme_first_prompt_points_agents_to_the_onboarding_resource(self):
        prompt = server.readme_first_prompt()
        self.assertIn("ssyubix://guides/readme-first", prompt)
        self.assertIn("agent_read_inbox", prompt)


class ToolDefinitionMetadataTests(unittest.IsolatedAsyncioTestCase):
    async def test_tool_metadata_discloses_key_side_effects_and_annotations(self):
        tools = {tool.name: tool for tool in await server.mcp.list_tools()}

        self.assertTrue(tools["capability_get_self"].annotations.readOnlyHint)
        self.assertTrue(tools["task_list"].annotations.readOnlyHint)
        self.assertTrue(tools["capability_remove_self"].annotations.destructiveHint)
        self.assertTrue(tools["room_leave"].annotations.destructiveHint)
        self.assertTrue(tools["room_admin_remove"].annotations.destructiveHint)

        self.assertIn("closes the previous WebSocket", tools["room_join"].description)
        self.assertIn("RPA inbox notifier", tools["room_join"].description)
        self.assertIn("local retry queue", tools["room_leave"].description)
        self.assertIn("queued_for_retry", tools["agent_send"].description)
        self.assertIn("permanently removes", tools["agent_read_inbox"].description)
        self.assertIn("RPA inbox notifier", tools["inbox_enable_notifications"].description)
        self.assertNotIn("reason", tools["task_accept"].inputSchema["properties"])


class PrivateOnlyRoomTests(unittest.IsolatedAsyncioTestCase):
    async def test_room_discovery_tool_is_not_exposed(self):
        tools = {tool.name for tool in await server.mcp.list_tools()}

        # Semua room private: tidak ada permukaan untuk menemukan room tanpa token.
        self.assertNotIn("room_list", tools)
        self.assertIn("room_join", tools)

    async def test_room_create_no_longer_accepts_a_privacy_toggle(self):
        tools = {tool.name: tool for tool in await server.mcp.list_tools()}
        schema = tools["room_create"].inputSchema

        self.assertNotIn("is_private", json.dumps(schema))

    async def test_room_join_requires_a_token(self):
        tools = {tool.name: tool for tool in await server.mcp.list_tools()}
        self.assertIn("token", json.dumps(tools["room_join"].inputSchema))

        with self.assertRaises(Exception):
            server.JoinRoomInput(room_id="ABC123")
        with self.assertRaises(Exception):
            server.JoinRoomInput(room_id="ABC123", token="")

        joined = server.JoinRoomInput(room_id="ABC123", token="SECRET123")
        self.assertEqual(joined.token, "SECRET123")

    def test_create_room_input_rejects_is_private(self):
        with self.assertRaises(Exception):
            server.CreateRoomInput(name="demo", is_private=False)

        room = server.CreateRoomInput(name="demo")
        self.assertEqual(room.name, "demo")


class DefaultRelayEndpointTests(unittest.TestCase):
    """Kunci nama host default.

    Worker pernah berganti nama dari `agentlink` ke `ssyubix` tanpa default di sini
    ikut berubah, sehingga setiap install tanpa konfigurasi memanggil host yang sudah
    tidak ada. Test ini menahan bentuk kesalahan yang sama supaya tidak terulang diam-diam.
    """

    def test_default_points_at_the_deployed_worker(self):
        self.assertEqual(
            server.DEFAULT_AGENTLINK_URL,
            "https://agentlink.ssyubix.com",
        )

    def test_default_does_not_use_retired_workers_dev_hosts(self):
        self.assertNotIn("workers.dev", server.DEFAULT_AGENTLINK_URL)
        self.assertNotIn("syuaibsyuaib.workers.dev", server.DEFAULT_AGENTLINK_URL)

    def test_default_is_a_bare_https_origin(self):
        # Path atau slash di ujung akan merusak URL yang dibentuk lewat f-string.
        self.assertTrue(server.DEFAULT_AGENTLINK_URL.startswith("https://"))
        self.assertFalse(server.DEFAULT_AGENTLINK_URL.endswith("/"))
        self.assertEqual(server.DEFAULT_AGENTLINK_URL.count("/"), 2)

    def test_readme_documents_the_same_default(self):
        repo_root = Path(__file__).resolve().parents[2]
        for name in ("README.md", "python/README.md"):
            readme = repo_root / name
            if not readme.exists():
                continue
            text = readme.read_text(encoding="utf-8")
            self.assertIn(
                server.DEFAULT_AGENTLINK_URL, text,
                f"{name} tidak menyebut endpoint default yang benar",
            )
            self.assertNotIn(
                "ssyubix.syuaibsyuaib.workers.dev", text,
                f"{name} masih menyebut hostname workers.dev yang sudah diganti",
            )
            self.assertNotIn(
                "agentlink.syuaibsyuaib.workers.dev", text,
                f"{name} masih menyebut hostname yang sudah pensiun",
            )


class InboxRpaNotifierTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_inbox = list(server.inbox)
        self.original_current_room = server.current_room
        self.original_session = server._mcp_notify_session
        self.original_enabled = server.inbox_notifier_enabled
        self.original_last = server._last_notified_unread_count
        self.original_local_state_dir = server.local_state_dir
        self.tempdir = tempfile.TemporaryDirectory()
        server.local_state_dir = Path(self.tempdir.name)
        server.inbox.clear()
        server.current_room = {
            "room_id": "ROOM42",
            "last_read_sequence": 0,
            "local_cache_path": str(Path(self.tempdir.name) / "ROOM42.json"),
            "retry_queue": [],
        }
        server.inbox_notifier_enabled = True
        server._mcp_notify_session = None
        server._last_notified_unread_count = None

    def tearDown(self):
        server.inbox[:] = self.original_inbox
        server.current_room = self.original_current_room
        server._mcp_notify_session = self.original_session
        server.inbox_notifier_enabled = self.original_enabled
        server._last_notified_unread_count = self.original_last
        server.local_state_dir = self.original_local_state_dir
        self.tempdir.cleanup()

    def test_status_payload_is_minimal(self):
        server.inbox[:] = [
            {"type": "message", "sequence": 1, "content": "secret body must not leak"},
            {"type": "message", "sequence": 2, "content": "another secret"},
        ]
        payload = server._inbox_status_payload()
        self.assertEqual(payload["unread_count"], 2)
        self.assertEqual(payload["room_id"], "ROOM42")
        self.assertNotIn("messages", payload)
        self.assertNotIn("content", payload)
        dumped = json.dumps(payload)
        self.assertNotIn("secret", dumped)

    async def test_emit_sends_resource_updated_and_log_without_bodies(self):
        calls = {"resource": 0, "logs": []}

        class FakeSession:
            async def send_resource_updated(self, uri):
                calls["resource"] += 1
                self.last_uri = str(uri)

            async def send_log_message(self, level, data, logger=None, related_request_id=None):
                calls["logs"].append({"level": level, "logger": logger, "data": data})

        session = FakeSession()
        server._bind_mcp_notify_session(session)
        server.inbox[:] = [
            {"type": "message", "sequence": 3, "content": "do-not-send"},
        ]
        await server._emit_inbox_rpa_notification(force=True)

        self.assertEqual(calls["resource"], 1)
        self.assertEqual(session.last_uri, server.INBOX_STATUS_URI)
        self.assertEqual(len(calls["logs"]), 1)
        self.assertEqual(calls["logs"][0]["logger"], "ssyubix.inbox.rpa")
        self.assertEqual(calls["logs"][0]["data"]["unread_count"], 1)
        self.assertNotIn("do-not-send", json.dumps(calls["logs"][0]["data"]))

    async def test_emit_skips_duplicate_unread_count(self):
        calls = {"resource": 0}

        class FakeSession:
            async def send_resource_updated(self, uri):
                calls["resource"] += 1

            async def send_log_message(self, **kwargs):
                return None

        server._bind_mcp_notify_session(FakeSession())
        server.inbox[:] = [{"type": "message", "sequence": 1, "content": "x"}]
        await server._emit_inbox_rpa_notification(force=True)
        await server._emit_inbox_rpa_notification(force=False)
        self.assertEqual(calls["resource"], 1)

    async def test_inbox_status_resource_and_tool_are_registered(self):
        tools = {tool.name: tool for tool in await server.mcp.list_tools()}
        self.assertIn("inbox_enable_notifications", tools)
        self.assertTrue(tools["inbox_enable_notifications"].annotations.readOnlyHint)

        resources = await server.mcp.list_resources()
        uris = {str(resource.uri) for resource in resources}
        self.assertIn(server.INBOX_STATUS_URI, uris)

        status = json.loads(server.inbox_status_resource())
        self.assertIn("unread_count", status)


class PackageMetadataTests(unittest.TestCase):
    def test_pyproject_version_is_3_1_1(self):
        repo_root = Path(__file__).resolve().parents[2]
        text = (repo_root / "python" / "pyproject.toml").read_text(encoding="utf-8")
        self.assertRegex(text, r'(?m)^version\s*=\s*"3\.1\.1"\s*$')

    def test_mcp_sdk_is_pinned_below_v2(self):
        repo_root = Path(__file__).resolve().parents[2]
        text = (repo_root / "python" / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn('"mcp>=1.0.0,<2"', text)

    def test_changelog_documents_3_1_1(self):
        repo_root = Path(__file__).resolve().parents[2]
        text = (repo_root / "CHANGELOG.md").read_text(encoding="utf-8")
        self.assertIn("## [3.1.1]", text)
        self.assertIn("mcp>=1.0.0,<2", text)
        self.assertIn("https://agentlink.ssyubix.com", text)


if __name__ == "__main__":
    unittest.main()
