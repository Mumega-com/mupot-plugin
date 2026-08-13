"""Contract tests for the Mupot operator inbox watcher."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from plugin.inbox_watch import (
    DEFAULT_STATE_FILE,
    InboxWatchSettings,
    InboxWatcher,
    WatchItem,
    _SOS_LINE,
)


def _settings(tmp: str, **overrides) -> InboxWatchSettings:
    base = dict(
        inbox_watch_enabled=True,
        inbox_watch_poll_seconds=30,
        inbox_watch_sources=["mupot", "sos"],
        inbox_watch_state_file=str(Path(tmp) / "state.json"),
        timeout=5.0,
    )
    base.update(overrides)
    return InboxWatchSettings.from_mapping(base)


def _mupot_response(messages):
    return {"ok": True, "tool": "inbox", "result": {"messages": messages, "remaining": len(messages)}}


class SettingsTests(unittest.TestCase):
    def test_disabled_by_default(self):
        settings = InboxWatchSettings.from_mapping({})
        self.assertFalse(settings.enabled)

    def test_enabled_requires_valid_sources(self):
        settings = InboxWatchSettings.from_mapping(
            {"inbox_watch_enabled": True, "inbox_watch_sources": ["mupot", "sos"]}
        )
        self.assertTrue(settings.enabled)
        self.assertEqual(settings.sources, ("mupot", "sos"))

    def test_unknown_source_rejected(self):
        with self.assertRaises(ValueError):
            InboxWatchSettings.from_mapping(
                {"inbox_watch_enabled": True, "inbox_watch_sources": ["carrier-pigeon"]}
            )

    def test_poll_floor_enforced(self):
        with self.assertRaises(ValueError):
            InboxWatchSettings.from_mapping(
                {"inbox_watch_enabled": True, "inbox_watch_poll_seconds": 2}
            )

    def test_default_state_file_is_under_hermes_home(self):
        # The cursor file lives under the Hermes home (never a shared global
        # path) so desktop and CLI homes keep separate cursors.
        self.assertTrue(DEFAULT_STATE_FILE.startswith("~/.hermes/"))


class SosLineParsingTests(unittest.TestCase):
    def test_parses_real_bus_line(self):
        line = (
            "[2026-08-13T15:30:47Z] agent:kasra: GATE: PASS on PR #719 "
            "[stream_id:1786635047180-0]"
        )
        match = _SOS_LINE.match(line)
        self.assertIsNotNone(match)
        self.assertEqual(match.group("ts"), "2026-08-13T15:30:47Z")
        self.assertEqual(match.group("from"), "agent:kasra")
        self.assertIn("GATE: PASS", match.group("body"))
        self.assertEqual(match.group("sid"), "1786635047180-0")

    def test_body_with_colons_survives(self):
        line = "[2026-08-13T15:30:47Z] agent:kasra: ratio 3:2 holds [stream_id:1-0]"
        match = _SOS_LINE.match(line)
        self.assertEqual(match.group("body"), "ratio 3:2 holds")


class PollCycleTests(unittest.TestCase):
    """Semantics under test:
    - the FIRST successful poll per source suppresses the observed backlog
      (enabling the watcher never replays history) via watermark + dedupe ring;
    - items arriving AFTER the baseline are delivered exactly once;
    - the delivered-watermark only advances on actual delivery (throttled items
      are retried next cycle, never lost);
    - transport failures degrade, never crash.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = self._tmp.name
        self.delivered: list[str] = []
        self.inject_ok = True
        self._inbox = {"messages": []}

    def _inbox_callable(self):
        return lambda: _mupot_response(list(self._inbox["messages"]))

    def _watcher(self, sos_text="", **settings_overrides):
        settings = _settings(self.tmp, **settings_overrides)
        outer = self

        def deliver(text: str) -> bool:
            outer.delivered.append(text)
            return outer.inject_ok

        watcher = InboxWatcher(
            settings,
            deliver=deliver,
            mupot_inbox=self._inbox_callable() if "mupot" in settings.sources else None,
            clock=lambda: 1000.0 + len(outer.delivered) * 1000,  # always past min gap
        )
        watcher._sos_call = lambda tool, args: sos_text  # deterministic transport
        return watcher

    def test_baseline_cycle_suppresses_existing_backlog(self):
        self._inbox["messages"] = [{"seq": 5, "from_agent": "kasra", "body": "old"}]
        watcher = self._watcher(sos_text="")
        self.assertEqual(watcher.poll_once(), 0)
        self.assertEqual(self.delivered, [])
        self.assertTrue(watcher._state.mupot_baseline_done)
        self.assertEqual(watcher._state.mupot_delivered_max_seq, 5)

    def test_new_message_after_baseline_is_delivered(self):
        self._inbox["messages"] = [{"seq": 5, "from_agent": "x", "body": "old"}]
        watcher = self._watcher(sos_text="")
        watcher.poll_once()  # baseline suppresses seq 5
        self._inbox["messages"].append(
            {"seq": 6, "from_agent": "kasra", "body": "verdict: PASS"}
        )
        self.assertEqual(watcher.poll_once(), 1)
        self.assertEqual(len(self.delivered), 1)
        self.assertIn("verdict: PASS", self.delivered[0])
        self.assertIn("kasra", self.delivered[0])
        self.assertIn("peek-only", self.delivered[0])
        self.assertEqual(watcher._state.mupot_delivered_max_seq, 6)

    def test_no_duplicate_delivery(self):
        self._inbox["messages"] = []
        watcher = self._watcher(sos_text="")
        watcher.poll_once()  # empty baseline
        self._inbox["messages"].append({"seq": 6, "from_agent": "k", "body": "once"})
        watcher.poll_once()
        count_after_first_delivery = len(self.delivered)
        self.assertEqual(count_after_first_delivery, 1)
        watcher.poll_once()  # still unread (peek-only) — must NOT re-deliver
        self.assertEqual(len(self.delivered), count_after_first_delivery)

    def test_sos_baseline_then_delivery(self):
        old = "[2026-08-13T15:00:00Z] agent:river: old news [stream_id:100-0]"
        watcher = self._watcher(sos_text=old, inbox_watch_sources=["sos"])
        watcher.poll_once()  # baseline suppresses the old line
        self.assertEqual(self.delivered, [])
        new_text = old + "\n[2026-08-13T15:30:47Z] agent:kasra: fresh verdict [stream_id:101-0]"
        watcher._sos_call = lambda tool, args: new_text
        self.assertEqual(watcher.poll_once(), 1)
        self.assertIn("fresh verdict", self.delivered[0])

    def test_sos_cursor_passed_as_since(self):
        seen_args = []

        def capture(tool, args):
            seen_args.append(dict(args))
            return ""

        watcher = self._watcher(sos_text="", inbox_watch_sources=["sos"])
        watcher._sos_call = capture
        watcher.poll_once()
        watcher._state.sos_cursor = "1786635047180-0"
        watcher.poll_once()
        self.assertEqual(seen_args[-1].get("since"), "1786635047180-0")

    def test_failed_source_does_not_kill_cycle(self):
        def broken():
            raise RuntimeError("pot unreachable")

        settings = _settings(self.tmp)
        watcher = InboxWatcher(settings, deliver=lambda t: True, mupot_inbox=broken)
        watcher._sos_call = lambda tool, args: ""
        self.assertEqual(watcher.poll_once(), 0)  # no exception escapes
        self.assertGreaterEqual(watcher._source_failures.get("mupot", 0), 1)
        # The healthy source still baselined — a dead source must not stall the rest.
        self.assertTrue(watcher._state.sos_baseline_done)

    def test_delivery_failure_falls_back_without_crash(self):
        self.inject_ok = False
        watcher = self._watcher(sos_text="")
        watcher.poll_once()  # empty baseline
        self._inbox["messages"].append({"seq": 6, "from_agent": "k", "body": "hi"})
        watcher._notify_macos = lambda title, body: None  # no real osascript in tests
        watcher.poll_once()  # must not raise even though inject failed
        self.assertEqual(len(self.delivered), 1)

    def test_throttled_items_are_retried_not_lost(self):
        # More items than the per-cycle cap: the overflow must come back next
        # cycle instead of being silently dropped.
        from plugin.inbox_watch import MAX_ITEMS_PER_CYCLE

        watcher = self._watcher(sos_text="")
        watcher.poll_once()  # empty baseline
        for i in range(1, MAX_ITEMS_PER_CYCLE + 4):
            self._inbox["messages"].append({"seq": i, "from_agent": "k", "body": f"m{i}"})
        first = watcher.poll_once()
        self.assertEqual(first, MAX_ITEMS_PER_CYCLE)
        second = watcher.poll_once()
        self.assertEqual(second, 3)  # the overflow arrives on the retry

    def test_state_persists_across_instances(self):
        self._inbox["messages"] = [{"seq": 9, "from_agent": "k", "body": "persisted"}]
        settings = _settings(self.tmp)
        watcher = InboxWatcher(settings, deliver=lambda t: True,
                               mupot_inbox=self._inbox_callable(), clock=lambda: 9999.0)
        watcher.poll_once()
        watcher2 = InboxWatcher(settings, deliver=lambda t: True,
                                mupot_inbox=self._inbox_callable(), clock=lambda: 99999.0)
        self.assertTrue(watcher2._state.mupot_baseline_done)
        self.assertEqual(watcher2._state.mupot_delivered_max_seq, 9)
        self.assertEqual(watcher2.poll_once(), 0)  # no replay of seq 9

    def test_body_truncated_in_delivery(self):
        watcher = self._watcher(sos_text="")
        watcher.poll_once()
        self._inbox["messages"].append({"seq": 6, "from_agent": "k", "body": "x" * 2000})
        watcher.poll_once()
        self.assertIn("[truncated]", self.delivered[0])


class WatchItemTests(unittest.TestCase):
    def test_item_shape(self):
        item = WatchItem(source="mupot", key="mupot:1", sender="kasra", body="hi")
        self.assertEqual(item.source, "mupot")
        self.assertEqual(item.ts, "")
        self.assertEqual(item.seq, 0)


if __name__ == "__main__":
    unittest.main()
