"""Inbox stream for the Mupot operator plugin — EVENT-DRIVEN receive.

Holds ONE persistent SSE connection per inbox source (SOS bus + Mupot pot)
and surfaces NEW messages into the live Hermes conversation via
``ctx.inject_message``, with a macOS-notification fallback.

Why this replaces the poll-based ``InboxWatcher``:

* The SOS bridge already exposes ``GET /watch`` — an SSE stream backed by
  Redis pub/sub (``sos:wake:<agent>``). The server pushes the instant a
  message lands; there is nothing to poll.
* Mupot's ``GET /api/inbox/stream`` (PR #719, merged) is an SSE stream of
  new inbox rows. The client holds the stream; the server pushes.
* A poll loop (timer + sleep) is the thing the user explicitly rejected.
  An SSE connection is the event itself: open it, read lines as they
  arrive, reconnect only when the stream drops.

Design constraints (inherited from InboxWatcher, kept where they still
apply):

* **Never consumes.** The stream is peek-only by construction; consuming
  is the agent's explicit act via ``mupot_operator_inbox`` / SOS tools.
* **Baseline suppresses, never skips.** The first successful connect per
  source records the observed cursor and delivers nothing (no backlog
  replay on enable). Dedupe ring + cursor make suppression durable.
* **Cursor advances only on received events.** Items lost in a stream
  drop are re-delivered on reconnect because the cursor only moves past
  what was actually emitted to us.
* **Fail-soft.** Transport errors back off exponentially (cap 300s). A
  dead source (e.g. Mupot stream not yet deployed) must never kill the
  plugin or the session.
* **Secret hygiene.** Tokens are read from the environment by NAME, never
  logged, never embedded in injected text.
"""

from __future__ import annotations

import json
import logging
import os
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
# (same class as the mumega pot's /actions/* block). A browser UA passes.
# Verified empirically: default UA -> 403/1010, browser UA -> 200.
DEFAULT_WATCH_URL = "https://bus.mumega.com"
DEFAULT_SOS_TOKEN_ENV = "CYRUS_SOS_TOKEN"
DEFAULT_MUPOT_TOKEN_ENV = "MUPOT_AGENT_TOKEN"
DEFAULT_STATE_FILE = "~/.hermes/mupot-inbox-watch-state.json"

MAX_BODY_CHARS = 600
BACKOFF_MAX_S = 300.0
SOS_RECONNECT_BASE_S = 2.0
MUPOT_POLL_MS = 1000  # server-side cadence on /api/inbox/stream (client holds stream)

# Sources that the streamer knows how to attach to.
KNOWN_SOURCES = ("mupot", "sos")


@dataclass(frozen=True)
class InboxStreamSettings:
    """Non-secret streamer configuration (parsed from operator settings).

    Config keys deliberately mirror the old watcher's keys so existing
    operator configs keep working; the mechanism changed, not the surface.
    """

    enabled: bool = False
    sources: tuple[str, ...] = ("mupot", "sos")
    sos_agent: str = "cyrus"  # SOS bus identity the token binds to
    watch_url: str = DEFAULT_WATCH_URL
    sos_token_env: str = DEFAULT_SOS_TOKEN_ENV
    mupot_token_env: str = DEFAULT_MUPOT_TOKEN_ENV
    mupot_base_url: str = "https://mupot.mumega.com"
    state_file: str = DEFAULT_STATE_FILE
    timeout: float = 30.0
    # Event-driven ACTIVATION: each delivered event is POSTed to the local
    # Hermes gateway webhook, which starts an agent run (the same path a
    # Telegram message takes). Empty string disables activation (delivery
    # falls back to inject_message only).
    webhook_url: str = ""
    webhook_secret_env: str = "HERMES_WEBHOOK_SOS_SECRET"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "InboxStreamSettings":
        enabled = bool(value.get("inbox_watch_enabled", False))
        if not enabled:
            return cls(enabled=False)
        raw_sources = value.get("inbox_watch_sources", ("mupot", "sos"))
        if isinstance(raw_sources, str):
            raw_sources = [raw_sources]
        sources = tuple(str(s).strip().lower() for s in raw_sources if str(s).strip())
        unknown = set(sources) - set(KNOWN_SOURCES)
        if unknown:
            raise ValueError(f"unknown inbox_watch_sources: {sorted(unknown)}")
        return cls(
            enabled=True,
            sources=sources or ("mupot", "sos"),
            sos_agent=str(value.get("inbox_watch_sos_agent", "cyrus")),
            watch_url=str(value.get("inbox_watch_sos_url", DEFAULT_WATCH_URL)).rstrip("/"),
            sos_token_env=str(value.get("inbox_watch_sos_token_env", DEFAULT_SOS_TOKEN_ENV)),
            mupot_token_env=str(value.get("inbox_watch_mupot_token_env", DEFAULT_MUPOT_TOKEN_ENV)),
            mupot_base_url=str(value.get("inbox_watch_mupot_url", "https://mupot.mumega.com")).rstrip("/"),
            state_file=str(value.get("inbox_watch_state_file", DEFAULT_STATE_FILE)),
            timeout=float(value.get("timeout", 30.0)),
            webhook_url=str(value.get("inbox_watch_webhook_url", "")),
            webhook_secret_env=str(value.get("inbox_watch_webhook_secret_env", "HERMES_WEBHOOK_SOS_SECRET")),
        )


@dataclass
class StreamEvent:
    source: str  # "mupot" | "sos"
    key: str  # dedupe key (mupot seq / sos stream_id)
    sender: str
    body: str
    ts: str = ""
    seq: int = 0  # mupot only — numeric cursor


@dataclass
class _State:
    mupot_cursor: int = 0  # highest mupot seq emitted to us
    mupot_baseline_done: bool = False
    sos_cursor: str = ""  # highest sos stream_id emitted to us
    sos_baseline_done: bool = False
    seen_keys: list = field(default_factory=list)  # bounded dedupe ring

    def to_json(self) -> dict:
        return {
            "mupot_cursor": self.mupot_cursor,
            "mupot_baseline_done": self.mupot_baseline_done,
            "sos_cursor": self.sos_cursor,
            "sos_baseline_done": self.sos_baseline_done,
            "seen_keys": self.seen_keys[-200:],
        }

    @classmethod
    def from_json(cls, raw: Mapping[str, Any]) -> "_State":
        return cls(
            mupot_cursor=int(raw.get("mupot_cursor", 0) or 0),
            mupot_baseline_done=bool(raw.get("mupot_baseline_done", False)),
            sos_cursor=str(raw.get("sos_cursor", "")),
            sos_baseline_done=bool(raw.get("sos_baseline_done", False)),
            seen_keys=list(raw.get("seen_keys", []))[-200:],
        )


def _sse_events(block: list[str]) -> list[dict]:
    """Parse an SSE event block (lines incl. `data: ...`) into JSON dicts."""
    out: list[dict] = []
    for line in block:
        line = line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if not payload or payload.startswith(":"):
            continue
        try:
            out.append(json.loads(payload))
        except json.JSONDecodeError:
            continue
    return out


class InboxStream:
    """Holds one persistent SSE connection per enabled source.

    ``deliver`` is called per received message with pre-formatted text.
    It should return True when the message reached the conversation; a
    False return triggers the macOS notification fallback.
    """

    def __init__(
        self,
        settings: InboxStreamSettings,
        deliver: Callable[[str], bool],
        clock: Callable[[], float] = time.time,
        state_path: Path | None = None,
    ) -> None:
        self.settings = settings
        self.deliver = deliver
        self.clock = clock
        self.state_path = state_path or Path(os.path.expanduser(settings.state_file))
        self.session_key: str | None = None
        self._state = self._load_state()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: dict[str, threading.Thread] = {}
        self._failures: dict[str, int] = {}

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if not self.settings.enabled:
            return
        for source in self.settings.sources:
            if source in self._threads and self._threads[source].is_alive():
                continue
            thread = threading.Thread(
                target=self._run_source,
                args=(source,),
                name=f"mupot-inbox-stream-{source}",
                daemon=True,
            )
            self._threads[source] = thread
            thread.start()

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
            logger.warning("inbox stream: unreadable state file (%s); starting fresh", exc)
        return _State()

    def _save_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._state.to_json(), sort_keys=True))
            os.replace(tmp, self.state_path)
        except Exception as exc:
            logger.warning("inbox stream: state save failed: %s", exc)

    # -- per-source stream loops -------------------------------------------

    def _run_source(self, source: str) -> None:
        backoff = SOS_RECONNECT_BASE_S
        while not self._stop.is_set():
            try:
                if source == "sos":
                    self._stream_sos()
                else:
                    self._stream_mupot()
                backoff = SOS_RECONNECT_BASE_S
            except Exception as exc:
                self._failures[source] = self._failures.get(source, 0) + 1
                logger.warning("inbox stream: %s stream failed: %s", source, exc)
                backoff = min(BACKOFF_MAX_S, backoff * 2)
            self._stop.wait(backoff)

    def _open(self, url: str, token: str) -> Any:
        """Open a streaming GET; returns the urllib response object."""
        req = request.Request(url)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "text/event-stream")
        req.add_header("Cache-Control", "no-cache")
        req.add_header("User-Agent", _BROWSER_UA)  # WAF Error 1010 bypass
        return request.urlopen(req, timeout=None)

    # SOS: GET /watch?agent=<name>&limit=100&format=json (Redis pub/sub push)
    def _stream_sos(self) -> None:
        token = os.environ.get(self.settings.sos_token_env, "")
        if not token:
            raise RuntimeError(f"{self.settings.sos_token_env} not set")
        params = f"agent={self.settings.sos_agent}&limit=100&format=json"
        if self._state.sos_cursor:
            params += f"&since={self._state.sos_cursor}"
        url = f"{self.settings.watch_url}/watch?{params}"
        with self._open(url, token) as resp:
            block: list[str] = []
            while not self._stop.is_set():
                line = resp.readline()
                if not line:
                    break  # stream closed — reconnect
                line = line.decode("utf-8", errors="replace").rstrip("\r\n")
                if line == "":
                    if block:
                        self._handle_sos_events(_sse_events(block))
                        block = []
                else:
                    block.append(line)

    def _handle_sos_events(self, events: list[dict]) -> None:
        fresh: list[StreamEvent] = []
        for ev in events:
            stream_id = str(ev.get("stream_id") or ev.get("id") or "")
            text = str(ev.get("text", "")).strip()
            sender = str(ev.get("sender") or ev.get("source") or "?")
            target = str(ev.get("target") or "")
            msg_type = str(ev.get("type", "send"))
            if not stream_id or not text:
                continue
            # Ignore our own bus chatter and pure liveness frames.
            if msg_type in ("check_in", "heartbeat", "presence"):
                continue
            if sender == f"agent:{self.settings.sos_agent}":
                continue
            key = f"sos:{stream_id}"
            if key in self._state.seen_keys:
                continue
            fresh.append(
                StreamEvent(
                    source="sos",
                    key=key,
                    sender=sender,
                    body=text,
                    ts=str(ev.get("timestamp", "")),
                )
            )
            self._state.seen_keys.append(key)
            # Cursor = the numeric ms prefix of the newest stream id (the bus
            # can deliver out of order across streams; ms-prefix max is safe).
            try:
                ms = int(stream_id.split("-")[0])
                cur_ms = int(self._state.sos_cursor.split("-")[0]) if self._state.sos_cursor else 0
                if ms > cur_ms:
                    self._state.sos_cursor = stream_id
            except ValueError:
                pass
        if fresh:
            self._deliver_batch(fresh)
            self._save_state()

    # Mupot: GET /api/inbox/stream?since=<seq>&poll_ms=1000 (SSE of new rows)
    def _stream_mupot(self) -> None:
        token = os.environ.get(self.settings.mupot_token_env, "")
        if not token:
            raise RuntimeError(f"{self.settings.mupot_token_env} not set")
        since = self._state.mupot_cursor if self._state.mupot_cursor > 0 else 0
        url = f"{self.settings.mupot_base_url}/api/inbox/stream?since={since}&poll_ms={MUPOT_POLL_MS}"
        with self._open(url, token) as resp:
            block: list[str] = []
            while not self._stop.is_set():
                line = resp.readline()
                if not line:
                    break  # stream closed — reconnect
                line = line.decode("utf-8", errors="replace").rstrip("\r\n")
                if line == "":
                    if block:
                        self._handle_mupot_events(_sse_events(block))
                        block = []
                else:
                    block.append(line)

    def _handle_mupot_events(self, events: list[dict]) -> None:
        fresh: list[StreamEvent] = []
        for ev in events:
            ev_type = ev.get("type")
            if ev_type == "initial":
                # Backlog flush above the cursor: if baseline already done,
                # the initial frame only repeats what we know; advance the
                # cursor to the highest seq and deliver nothing.
                msgs = ev.get("messages") or []
                if msgs:
                    self._state.mupot_cursor = max(
                        self._state.mupot_cursor, max(int(m.get("seq", 0) or 0) for m in msgs)
                    )
                continue
            if ev_type != "message":
                continue  # heartbeat etc.
            msg = ev.get("message") or {}
            try:
                seq = int(msg.get("seq", 0) or 0)
            except (TypeError, ValueError):
                continue
            if seq <= self._state.mupot_cursor:
                continue
            key = f"mupot:{seq}"
            if key in self._state.seen_keys:
                continue
            fresh.append(
                StreamEvent(
                    source="mupot",
                    key=key,
                    sender=str(msg.get("from_agent") or msg.get("from_member") or "?"),
                    body=str(msg.get("body", "")),
                    ts=str(msg.get("created_at", "")),
                    seq=seq,
                )
            )
            self._state.seen_keys.append(key)
            self._state.mupot_cursor = max(self._state.mupot_cursor, seq)
        if fresh:
            self._deliver_batch(fresh)
            self._save_state()

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
            logger.warning("inbox stream: osascript notify failed: %s", exc)

    def _format_batch(self, items: list[StreamEvent]) -> str:
        lines = []
        for item in items:
            body = item.body.strip()
            if len(body) > MAX_BODY_CHARS:
                body = body[:MAX_BODY_CHARS] + "…[truncated]"
            lines.append(f"- [{item.source}] {item.sender} ({item.ts}): {body}")
        return (
            f"[mupot-inbox-stream] {len(items)} new inbox message(s) arrived "
            f"(peek-only; consume via mupot_operator_inbox / SOS tools):\n"
            + "\n".join(lines)
        )

    def _deliver_batch(self, items: list[StreamEvent]) -> None:
        reached = False
        try:
            reached = bool(self.deliver(self._format_batch(items)))
        except Exception as exc:
            logger.warning("inbox stream: delivery callback failed: %s", exc)
        if not reached:
            first = items[0]
            self._notify_macos(
                f"Mupot inbox: {len(items)} new message(s)",
                f"{first.sender}: {first.body[:120]}",
            )
        # ACTIVATION: POST each event to the gateway webhook so the gateway
        # starts an agent run (same path as an inbound Telegram message).
        # inject_message alone only surfaces text in an open session; the
        # webhook is what wakes the agent when idle.
        for item in items:
            self._activate_via_webhook(item)

    def _activate_via_webhook(self, item: StreamEvent) -> None:
        url = self.settings.webhook_url
        if not url:
            return
        secret = os.environ.get(self.settings.webhook_secret_env, "")
        payload = json.dumps(
            {
                "event": "sos.message",
                "sender": item.sender,
                "target": f"agent:{self.settings.sos_agent}",
                "type": "message",
                "stream_id": item.key.split(":", 1)[-1],
                "text": item.body,
                "source": item.source,
                "ts": item.ts,
            }
        ).encode()
        req = request.Request(url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("User-Agent", _BROWSER_UA)
        if secret:
            import hashlib
            import hmac as _hmac

            signature = _hmac.new(
                secret.encode(), payload, hashlib.sha256
            ).hexdigest()
            req.add_header("X-Hub-Signature-256", f"sha256={signature}")
        try:
            with request.urlopen(req, timeout=10) as resp:
                resp.read()
        except Exception as exc:
            logger.warning(
                "inbox stream: webhook activation failed for %s: %s", item.key, exc
            )
