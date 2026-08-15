"""Contract tests for the Mupot operator inbox STREAM (event-driven SSE receive).

Covers the swap from the poll-based InboxWatcher to the SSE-stream
InboxStream: settings parsing, SSE event parsing, per-source event
handling (SOS /watch + Mupot /api/inbox/stream), cursor advancement,
baseline suppression, and dedupe.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from plugin.inbox_stream import (
    DEFAULT_STATE_FILE,
    DEFAULT_WATCH_URL,
    InboxStream,
    InboxStreamSettings,
    _sse_events,
)


def _settings(**overrides) -> InboxStreamSettings:
    base = {
        "inbox_watch_enabled": True,
        "inbox_watch_sources": ("sos",),
        "inbox_watch_sos_agent": "hadi-hermes",
    }
    base.update(overrides)
    return InboxStreamSettings.from_mapping(base)


class TestSettings(unittest.TestCase):
    def test_disabled_by_default(self) -> None:
        s = InboxStreamSettings.from_mapping({})
        self.assertFalse(s.enabled)

    def test_enabled_parses_sources(self) -> None:
        s = _settings(inbox_watch_sources=("mupot", "sos"))
        self.assertTrue(s.enabled)
        self.assertEqual(s.sources, ("mupot", "sos"))

    def test_unknown_source_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _settings(inbox_watch_sources=("telegram",))

    def test_defaults(self) -> None:
        s = _settings()
        self.assertEqual(s.watch_url, DEFAULT_WATCH_URL)
        self.assertEqual(s.sos_agent, "hadi-hermes")
        self.assertEqual(s.state_file, DEFAULT_STATE_FILE)


class TestSseParsing(unittest.TestCase):
    def test_parses_data_lines(self) -> None:
        block = [
            "data: {\"stream_id\": \"1786816749123-0\", \"text\": \"hi\", \"type\": \"send\"}",
        ]
        events = _sse_events(block)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["stream_id"], "1786816749123-0")

    def test_skips_keepalive_comments(self) -> None:
        # A bare keepalive comment has no `data:` line at all.
        block = [": keepalive", ""]
        self.assertEqual(_sse_events(block), [])

    def test_empty_data_dict_is_filtered_downstream(self) -> None:
        # `data: {}` parses to an empty dict; the handler's
        # "not stream_id and not text" guard drops it before delivery.
        block = ["data: {}"]
        events = _sse_events(block)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0], {})

    def test_skips_malformed_json(self) -> None:
        block = ["data: {not json"]
        self.assertEqual(_sse_events(block), [])


class TestStreamBehavior(unittest.TestCase):
    def _stream(self, sources=("sos",)) -> InboxStream:
        delivered: list[str] = []
        state = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        state.close()
        Path(state.name).write_text(json.dumps({}))

        stream = InboxStream(
            _settings(inbox_watch_sources=sources, state_file=state.name),
            deliver=lambda text: delivered.append(text) or True,
            state_path=Path(state.name),
        )
        stream._deliver = lambda items: stream._deliver_batch(items)  # type: ignore[attr-defined]
        return stream

    def test_sos_handles_push_event(self) -> None:
        stream = self._stream()
        events = [
            {
                "stream_id": "1786816749123-0",
                "sender": "agent:kasra",
                "target": "agent:hadi-hermes",
                "type": "send",
                "text": "gate verdict: PASS",
                "timestamp": "2026-08-15T18:00:00Z",
            }
        ]
        with patch.object(stream, "deliver", return_value=True) as deliver:
            stream._handle_sos_events(events)
            deliver.assert_called_once()
            text = deliver.call_args.args[0]
            self.assertIn("agent:kasra", text)
            self.assertIn("PASS", text)
        # Cursor advanced to the pushed stream id.
        self.assertEqual(stream._state.sos_cursor, "1786816749123-0")

    def test_sos_skips_own_checkins_and_heartbeats(self) -> None:
        stream = self._stream()
        events = [
            {"stream_id": "1786816705634-0", "sender": "agent:hadi-hermes",
             "target": "agent:hadi-hermes", "type": "check_in", "text": "hi"},
            {"stream_id": "1786816705635-0", "sender": "agent:system",
             "target": "agent:hadi-hermes", "type": "heartbeat", "text": "ping"},
        ]
        with patch.object(stream, "deliver") as deliver:
            stream._handle_sos_events(events)
            deliver.assert_not_called()

    def test_sos_dedupe_ring_prevents_redelivery(self) -> None:
        stream = self._stream()
        events = [
            {"stream_id": "1786816749123-0", "sender": "agent:kasra",
             "target": "agent:hadi-hermes", "type": "send", "text": "first"},
        ]
        with patch.object(stream, "deliver") as deliver:
            stream._handle_sos_events(events)
            stream._handle_sos_events(events)  # same stream id again
            deliver.assert_called_once()

    def test_mupot_initial_frame_advances_cursor_without_delivery(self) -> None:
        stream = self._stream(sources=("mupot",))
        events = [
            {"type": "initial", "since": 0, "messages": [
                {"seq": 100, "from_agent": "kasra", "body": "backlog"},
            ]},
        ]
        with patch.object(stream, "deliver") as deliver:
            stream._handle_mupot_events(events)
            deliver.assert_not_called()  # baseline suppressed
        self.assertEqual(stream._state.mupot_cursor, 100)

    def test_mupot_message_above_cursor_delivered(self) -> None:
        stream = self._stream(sources=("mupot",))
        stream._state.mupot_cursor = 100
        events = [
            {"type": "message", "message": {
                "seq": 101, "from_agent": "river", "body": "fresh", "created_at": "now",
            }},
        ]
        with patch.object(stream, "deliver", return_value=True) as deliver:
            stream._handle_mupot_events(events)
            deliver.assert_called_once()
            self.assertIn("river", deliver.call_args.args[0])
            self.assertIn("fresh", deliver.call_args.args[0])
        self.assertEqual(stream._state.mupot_cursor, 101)

    def test_mupot_heartbeat_ignored(self) -> None:
        stream = self._stream(sources=("mupot",))
        with patch.object(stream, "deliver") as deliver:
            stream._handle_mupot_events([{"type": "heartbeat"}])
            deliver.assert_not_called()

    def test_state_persisted_after_delivery(self) -> None:
        stream = self._stream()
        events = [
            {"stream_id": "1786816749123-0", "sender": "agent:kasra",
             "target": "agent:hadi-hermes", "type": "send", "text": "persist me"},
        ]
        with patch.object(stream, "deliver", return_value=True):
            stream._handle_sos_events(events)
        raw = json.loads(Path(stream.state_path).read_text())
        self.assertEqual(raw["sos_cursor"], "1786816749123-0")

    def test_missing_token_raises_per_source(self) -> None:
        stream = self._stream()
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                stream._stream_sos()

    def test_start_skips_when_disabled(self) -> None:
        stream = InboxStream(
            InboxStreamSettings(enabled=False),
            deliver=lambda text: True,
        )
        stream.start()
        self.assertEqual(stream._threads, {})


if __name__ == "__main__":
    unittest.main()
