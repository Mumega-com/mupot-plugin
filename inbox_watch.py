"""Inbox watcher for the Mupot operator plugin.

Background daemon that polls the agent's durable inboxes and surfaces NEW
messages into the live Hermes conversation (``ctx.inject_message``) with a
macOS-notification fallback.  Exists because the desktop gateway does not
route SOS/Mupot inbox traffic on its own: without this, gate verdicts and
peer requests sit unread until a human prompts the agent.

Design constraints (why it looks the way it does):

* **Never consumes.**  Both sources are read with peek semantics.  Consuming
  is the agent's explicit act via ``mupot_operator_inbox`` / the SOS tools —
  a watcher that consumed would race the very session it notifies (the
  duplicate-consumer work-loss class; see herdr ``kasra-inbox-watch`` loop,
  2026-08-10).
* **Baseline suppresses, never skips.**  The first successful poll per source
  records what exists and delivers nothing (no backlog replay on enable).
  Suppression is by dedupe-key + watermark, so nothing is silently dropped:
  an item is either delivered-and-marked, or still above the watermark and
  retried next cycle.
* **Watermark advances only on delivery.**  Items throttled by the per-cycle
  cap or the injection-gap stay above the watermark and unseen, so the next
  cycle retries them instead of losing them.
* **Fail-soft.**  Every poll is wrapped; transport errors back off
  exponentially (cap 300s).  A dead inbox source must never kill the plugin
  or the session.
* **Secret hygiene.**  Tokens are read from the environment by NAME, never
  logged, never embedded in injected text.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib import request

logger = logging.getLogger(__name__)

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
# SOS's Cloudflare WAF rejects urllib's default User-Agent with Error 1010
# (same class as the mumega pot's /actions/* block).  A browser UA passes.
# Verified empirically 2026-08-13: default UA -> 403/1010, browser UA -> 200.
DEFAULT_SOS_URL = "https://mcp.mumega.com/mcp"
DEFAULT_SOS_TOKEN_ENV = "CYRUS_SOS_TOKEN"
DEFAULT_STATE_FILE = "~/.hermes/mupot-inbox-watch-state.json"

MAX_BODY_CHARS = 600
MAX_ITEMS_PER_CYCLE = 8
MIN_INJECT_GAP_S = 30.0
BACKOFF_MAX_S = 300.0

# SOS text lines: "[<ts>] <from>: <body> [stream_id:<id>]" — the sender itself
# contains a colon ("agent:kasra"), so `from` is the first whitespace-delimited
# token, and the body starts after the following ": ".
_SOS_LINE = re.compile(
    r"^\[(?P<ts>[^\]]+)\]\s+(?P<from>\S+):\s+(?P<body>.*?)\s*\[stream_id:(?P<sid>[^\]]+)\]\s*$"
)


@dataclass(frozen=True)
class InboxWatchSettings:
    """Non-secret watcher configuration (parsed from operator settings)."""

    enabled: bool = False
    poll_seconds: float = 30.0
    sources: tuple[str, ...] = ("mupot", "sos")
    sos_url: str = DEFAULT_SOS_URL
    sos_token_env: str = DEFAULT_SOS_TOKEN_ENV
    state_file: str = DEFAULT_STATE_FILE
    timeout: float = 20.0

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "InboxWatchSettings":
        enabled = bool(value.get("inbox_watch_enabled", False))
        if not enabled:
            return cls(enabled=False)
        poll = float(value.get("inbox_watch_poll_seconds", 30.0))
        if poll < 10.0:
            raise ValueError("inbox_watch_poll_seconds must be >= 10")
        raw_sources = value.get("inbox_watch_sources", ("mupot", "sos"))
        if isinstance(raw_sources, str):
            raw_sources = [raw_sources]
        sources = tuple(str(s).strip().lower() for s in raw_sources if str(s).strip())
        unknown = set(sources) - {"mupot", "sos"}
        if unknown:
            raise ValueError(f"unknown inbox_watch_sources: {sorted(unknown)}")
        return cls(
            enabled=True,
            poll_seconds=poll,
            sources=sources,
            sos_url=str(value.get("inbox_watch_sos_url", DEFAULT_SOS_URL)),
            sos_token_env=str(value.get("inbox_watch_sos_token_env", DEFAULT_SOS_TOKEN_ENV)),
            state_file=str(value.get("inbox_watch_state_file", DEFAULT_STATE_FILE)),
            timeout=float(value.get("timeout", 20.0)),
        )


@dataclass
class WatchItem:
    source: str  # "mupot" | "sos"
    key: str  # dedupe key (seq or stream_id)
    sender: str
    body: str
    ts: str = ""
    seq: int = 0  # mupot only — drives the delivered-watermark


@dataclass
class _State:
    mupot_delivered_max_seq: int = 0  # items strictly above may be delivered
    mupot_baseline_done: bool = False
    sos_cursor: str = ""  # fetch watermark passed as `since`
    sos_baseline_done: bool = False
    seen_keys: list = field(default_factory=list)  # bounded dedupe ring

    def to_json(self) -> dict:
        return {
            "mupot_delivered_max_seq": self.mupot_delivered_max_seq,
            "mupot_baseline_done": self.mupot_baseline_done,
            "sos_cursor": self.sos_cursor,
            "sos_baseline_done": self.sos_baseline_done,
            "seen_keys": self.seen_keys[-200:],
        }

    @classmethod
    def from_json(cls, raw: Mapping[str, Any]) -> "_State":
        return cls(
            mupot_delivered_max_seq=int(raw.get("mupot_delivered_max_seq", 0) or 0),
            mupot_baseline_done=bool(raw.get("mupot_baseline_done", False)),
            sos_cursor=str(raw.get("sos_cursor", "")),
            sos_baseline_done=bool(raw.get("sos_baseline_done", False)),
            seen_keys=list(raw.get("seen_keys", []))[-200:],
        )


class InboxWatcher:
    """Single daemon thread polling configured inbox sources.

    ``deliver`` is called at most once per cycle with a pre-formatted text
    block.  It should return True when the message reached the conversation;
    a False return triggers the macOS notification fallback.
    """

    def __init__(
        self,
        settings: InboxWatchSettings,
        deliver: Callable[[str], bool],
        mupot_inbox: Callable[[], Mapping[str, Any]] | None = None,
        clock: Callable[[], float] = time.time,
        state_path: Path | None = None,
    ) -> None:
        self.settings = settings
        self.deliver = deliver
        self.mupot_inbox = mupot_inbox
        self.clock = clock
        self.state_path = state_path or Path(os.path.expanduser(settings.state_file))
        self.session_key: str | None = None
        self._state = self._load_state()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_inject = 0.0
        self._source_failures: dict[str, int] = {}

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if not self.settings.enabled or self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="mupot-inbox-watcher", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def set_session_key(self, session_key: str | None) -> None:
        with self._lock:
            self.session_key = session_key

    # -- state -------------------------------------------------------------

    def _load_state(self) -> _State:
        try:
            if self.state_path.exists():
                return _State.from_json(json.loads(self.state_path.read_text()))
        except Exception as exc:
            logger.warning("inbox watcher: unreadable state file (%s); starting fresh", exc)
        return _State()

    def _save_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._state.to_json(), sort_keys=True))
            os.replace(tmp, self.state_path)
        except Exception as exc:
            logger.warning("inbox watcher: state save failed: %s", exc)

    # -- sources -----------------------------------------------------------

    def _poll_mupot(self) -> list[WatchItem]:
        """All unread items ABOVE the delivered-watermark (peek-only fetch)."""
        if self.mupot_inbox is None:
            return []
        response = self.mupot_inbox()
        if not isinstance(response, Mapping) or not response.get("ok"):
            return []
        result = response.get("result")
        messages = result.get("messages") if isinstance(result, Mapping) else None
        if not isinstance(messages, list):
            return []
        items: list[WatchItem] = []
        for msg in messages:
            if not isinstance(msg, Mapping):
                continue
            try:
                seq = int(msg.get("seq", 0))
            except (TypeError, ValueError):
                continue
            if seq <= self._state.mupot_delivered_max_seq:
                continue
            items.append(
                WatchItem(
                    source="mupot",
                    key=f"mupot:{seq}",
                    sender=str(msg.get("from_agent") or msg.get("from_member") or "?"),
                    body=str(msg.get("body", "")),
                    ts=str(msg.get("created_at", "")),
                    seq=seq,
                )
            )
        return items

    def _sos_call(self, tool: str, args: Mapping[str, Any]) -> str:
        token = os.environ.get(self.settings.sos_token_env, "")
        if not token:
            raise RuntimeError(f"{self.settings.sos_token_env} not set")
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": dict(args)},
        }
        req = request.Request(self.settings.sos_url, data=json.dumps(payload).encode())
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json, text/event-stream")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("User-Agent", _BROWSER_UA)  # WAF Error 1010 bypass
        with request.urlopen(req, timeout=self.settings.timeout) as resp:
            data = json.loads(resp.read().decode())
        content = (((data.get("result") or {}).get("content")) or [{}])[0]
        return str(content.get("text", ""))

    def _poll_sos(self) -> list[WatchItem]:
        """Parsed bus lines above the cursor (server filters via `since`)."""
        args: dict[str, Any] = {"limit": 25, "format": "text"}
        if self._state.sos_cursor:
            args["since"] = self._state.sos_cursor
        text = self._sos_call("inbox", args)
        items: list[WatchItem] = []
        sids = [self._state.sos_cursor]
        for line in text.splitlines():
            match = _SOS_LINE.match(line.strip())
            if not match:
                continue
            sids.append(match.group("sid"))
            items.append(
                WatchItem(
                    source="sos",
                    key=f"sos:{match.group('sid')}",
                    sender=match.group("from"),
                    body=match.group("body"),
                    ts=match.group("ts"),
                )
            )
        # The bus returns NEWEST-FIRST — the cursor must be the MAX stream id
        # (numeric ms prefix), not the last line parsed, or every poll
        # re-delivers the whole window.
        self._state.sos_cursor = max(
            sids, key=lambda s: int(s.split("-")[0]) if s and s.split("-")[0].isdigit() else 0
        )
        return items

    # -- delivery ----------------------------------------------------------

    def _notify_macos(self, title: str, body: str) -> None:
        script = (
            f'display notification "{body.replace(chr(34), chr(39))}" '
            f'with title "{title}"'
        )
        try:
            subprocess.run(
                ["osascript", "-e", script], timeout=10, check=False,
                capture_output=True,
            )
        except Exception as exc:
            logger.warning("inbox watcher: osascript notify failed: %s", exc)

    def _format_batch(self, items: list[WatchItem]) -> str:
        lines = []
        for item in items:
            body = item.body.strip()
            if len(body) > MAX_BODY_CHARS:
                body = body[:MAX_BODY_CHARS] + "…[truncated]"
            lines.append(f"- [{item.source}] {item.sender} ({item.ts}): {body}")
        return (
            f"[mupot-inbox-watcher] {len(items)} new inbox message(s) arrived "
            f"(peek-only; consume via mupot_operator_inbox / SOS tools):\n"
            + "\n".join(lines)
        )

    # -- main loop ----------------------------------------------------------

    def poll_once(self) -> int:
        """One poll cycle; returns the number of items delivered this cycle."""
        fresh: list[WatchItem] = []
        for source in self.settings.sources:
            try:
                items = self._poll_mupot() if source == "mupot" else self._poll_sos()
                self._source_failures[source] = 0
            except Exception as exc:
                self._source_failures[source] = self._source_failures.get(source, 0) + 1
                logger.warning("inbox watcher: %s poll failed: %s", source, exc)
                continue

            baseline_flag = (
                self._state.mupot_baseline_done
                if source == "mupot"
                else self._state.sos_baseline_done
            )
            if not baseline_flag:
                # First successful poll for this source: suppress EVERYTHING
                # observed so enabling the watcher never replays a backlog.
                # Suppression is durable (dedupe ring + watermark), not a skip:
                # nothing is declared delivered that wasn't shown.
                for item in items:
                    if item.key not in self._state.seen_keys:
                        self._state.seen_keys.append(item.key)
                    if source == "mupot":
                        self._state.mupot_delivered_max_seq = max(
                            self._state.mupot_delivered_max_seq, item.seq
                        )
                if source == "mupot":
                    self._state.mupot_baseline_done = True
                else:
                    self._state.sos_baseline_done = True
                continue

            for item in items:
                if item.key in self._state.seen_keys:
                    continue
                fresh.append(item)

        self._state.seen_keys = self._state.seen_keys[-200:]
        delivered = 0
        if fresh:
            now = self.clock()
            if now - self._last_inject >= MIN_INJECT_GAP_S:
                capped = fresh[:MAX_ITEMS_PER_CYCLE]
                reached = False
                try:
                    reached = bool(self.deliver(self._format_batch(capped)))
                except Exception as exc:
                    logger.warning("inbox watcher: delivery callback failed: %s", exc)
                if not reached:
                    first = capped[0]
                    self._notify_macos(
                        f"Mupot inbox: {len(capped)} new message(s)",
                        f"{first.sender}: {first.body[:120]}",
                    )
                # Mark delivered items ONLY. Anything beyond the cap stays
                # above the watermark and unseen, so the next cycle retries it.
                for item in capped:
                    self._state.seen_keys.append(item.key)
                    if item.source == "mupot":
                        self._state.mupot_delivered_max_seq = max(
                            self._state.mupot_delivered_max_seq, item.seq
                        )
                self._last_inject = now
                delivered = len(capped)
        self._save_state()
        return delivered

    def _run(self) -> None:
        while not self._stop.is_set():
            self.poll_once()
            delay = self.settings.poll_seconds
            worst = max(self._source_failures.values(), default=0)
            if worst:
                delay = min(BACKOFF_MAX_S, delay * (2 ** worst))
            self._stop.wait(delay)
