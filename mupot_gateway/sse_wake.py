"""Event-based wake for the native Mupot receive path.

Mupot's ``GET /api/inbox/stream`` is a PEEK-ONLY SSE feed of the bound agent's
unread inbox (``data: {"type":"initial"|"message",...}`` frames plus ``: ping``
heartbeat comments). This module holds that stream open and turns every new
message into a *wake signal* for the adapter's existing poll loop, which then
runs its unchanged ``inbox_lease -> process -> reply -> inbox_lease_ack`` cycle
immediately instead of at the next timed tick.

Hard boundary: nothing here consumes, leases, acks, or reads message bodies
into Hermes. A frame only ever calls ``on_wake()``. Every custody guarantee
(durable lease fence, reply outbox, e-stop gates, sender fence, mupot-dispatch
handling) stays on the lease path, so a spurious, duplicated, or forged wake
costs at most one extra ``inbox_lease`` that comes back empty.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable, Optional
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger(__name__)

# A single SSE frame larger than this is dropped (the stream stays up). The
# server caps one inbox row well below this; the cap only bounds memory.
MAX_FRAME_BYTES = 1024 * 1024

STREAM_PATH = "/api/inbox/stream"


class SSEHTTPError(RuntimeError):
    """The stream endpoint answered with a non-200 status."""

    def __init__(self, status: int) -> None:
        super().__init__(f"inbox stream HTTP {status}")
        self.status = status


@dataclass(frozen=True)
class SSEFrame:
    """One dispatched SSE frame, reduced to what a wake decision needs."""

    kind: str  # "heartbeat" | "message" | "initial" | "other"
    max_seq: Optional[int] = None


class SSEFrameParser:
    """Incremental text/event-stream parser (WHATWG field rules, data only).

    Feed decoded lines without their terminator; a blank line dispatches the
    accumulated frame. Comment lines (``:``) count as heartbeats. Only the
    ``data`` field is interpreted -- the Mupot stream sends no ``event``/``id``.
    """

    def __init__(self, max_frame_bytes: int = MAX_FRAME_BYTES) -> None:
        self._max = max_frame_bytes
        self._data: list[str] = []
        self._size = 0
        self._overflow = False
        self._comment = False

    def feed_line(self, line: str) -> Optional[SSEFrame]:
        if line == "":
            return self._dispatch()
        if line.startswith(":"):
            self._comment = True
            return None
        field, sep, value = line.partition(":")
        if not sep:
            value = ""
        elif value.startswith(" "):
            value = value[1:]
        if field != "data":
            return None
        self._size += len(value) + 1
        if self._size > self._max:
            self._overflow = True
            self._data = []
            return None
        if not self._overflow:
            self._data.append(value)
        return None

    def _dispatch(self) -> Optional[SSEFrame]:
        data, overflow, comment = self._data, self._overflow, self._comment
        self._data, self._size, self._overflow, self._comment = [], 0, False, False
        if overflow:
            logger.warning("[mupot] inbox stream frame exceeded %d bytes; dropped", self._max)
            return SSEFrame("other")
        if not data:
            return SSEFrame("heartbeat") if comment else None
        return parse_event_payload("\n".join(data))


def _seq(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def parse_event_payload(payload: str) -> SSEFrame:
    """Classify one ``data:`` payload. Never raises."""
    try:
        event = json.loads(payload)
    except (ValueError, TypeError):
        return SSEFrame("other")
    if not isinstance(event, dict):
        return SSEFrame("other")
    kind = event.get("type")
    if kind == "message":
        message = event.get("message")
        seq = _seq(message.get("seq")) if isinstance(message, dict) else None
        return SSEFrame("message", seq)
    if kind == "initial":
        messages = event.get("messages")
        if not isinstance(messages, list) or not messages:
            return SSEFrame("other")
        seqs = [
            s for s in (_seq(m.get("seq")) for m in messages if isinstance(m, dict))
            if s is not None
        ]
        return SSEFrame("initial", max(seqs) if seqs else None)
    return SSEFrame("other")


def stream_url_from_mcp_url(mcp_url: str) -> str:
    """Derive ``<origin>[/prefix]/api/inbox/stream`` from the MCP endpoint URL.

    ``https://mupot.example/mcp`` -> ``https://mupot.example/api/inbox/stream``.
    The bearer token rides on this request, so plain http is refused except
    for loopback development hosts.
    """
    parts = urlsplit(str(mcp_url or "").strip())
    host = (parts.hostname or "").lower()
    if not host:
        raise ValueError("mupot MCP url has no host")
    if parts.scheme != "https" and not (
        parts.scheme == "http" and host in {"localhost", "127.0.0.1", "::1"}
    ):
        raise ValueError("inbox stream requires an https mupot url")
    path = parts.path.rstrip("/")
    if path.endswith(STREAM_PATH):
        # Already the stream endpoint (an explicit sse_url override).
        return urlunsplit((parts.scheme, parts.netloc, path, "", ""))
    if path.endswith("/mcp"):
        path = path[: -len("/mcp")]
    return urlunsplit((parts.scheme, parts.netloc, path + STREAM_PATH, "", ""))


def backoff_delay(
    failures: int,
    *,
    base: float,
    cap: float,
    rand: Callable[[], float] = random.random,
) -> float:
    """Capped exponential backoff with equal jitter: in [d/2, d], d=min(cap, base*2^n)."""
    ceiling = min(cap, base * (2 ** max(0, min(failures, 30))))
    return ceiling / 2 + rand() * (ceiling / 2)


async def capped_lines(
    chunks: AsyncIterator[bytes], *, max_line_bytes: int = MAX_FRAME_BYTES
) -> AsyncIterator[str]:
    """Split a byte stream into SSE lines (LF, CRLF or CR), bounding memory.

    ``httpx.Response.aiter_lines`` buffers one line without limit; a line that
    grows past ``max_line_bytes`` here raises instead, which the waker treats
    as a dropped connection (reconnect with backoff).
    """
    buffer = b""
    pending_cr = False
    async for chunk in chunks:
        if not chunk:
            continue
        if pending_cr and chunk.startswith(b"\n"):
            chunk = chunk[1:]
        pending_cr = False
        buffer += chunk
        while True:
            positions = [i for i in (buffer.find(b"\n"), buffer.find(b"\r")) if i >= 0]
            if not positions:
                break
            cut = min(positions)
            line, terminator = buffer[:cut], buffer[cut:cut + 1]
            rest = buffer[cut + 1:]
            if terminator == b"\r":
                if rest.startswith(b"\n"):
                    rest = rest[1:]
                elif not rest:
                    pending_cr = True
            buffer = rest
            yield line.decode("utf-8", errors="replace")
        if len(buffer) > max_line_bytes:
            raise ValueError("inbox stream line exceeds the frame cap")
    if buffer:
        yield buffer.decode("utf-8", errors="replace")


# opener(since) -> async context manager yielding an async iterator of lines.
StreamOpener = Callable[[Optional[int]], AbstractAsyncContextManager[AsyncIterator[str]]]


class InboxStreamWaker:
    """Hold the SSE stream open; call ``on_wake`` for every new message frame."""

    def __init__(
        self,
        *,
        opener: StreamOpener,
        on_wake: Callable[[], None],
        idle_timeout: float = 45.0,
        backoff_base: float = 1.0,
        backoff_cap: float = 60.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        rand: Callable[[], float] = random.random,
    ) -> None:
        self._opener = opener
        self._on_wake = on_wake
        self.idle_timeout = max(1.0, float(idle_timeout))
        self._backoff_base = max(0.01, float(backoff_base))
        self._backoff_cap = max(self._backoff_base, float(backoff_cap))
        self._sleep = sleep
        self._clock = clock
        self._rand = rand
        self.last_seq: Optional[int] = None
        self.connected = False
        self.last_activity: Optional[float] = None
        self.failures = 0
        self.wakes = 0
        self._stopped = False
        self._productive = False

    def healthy(self) -> bool:
        """Stream is open and has shown life (frame or ping) within idle_timeout."""
        return (
            self.connected
            and self.last_activity is not None
            and self._clock() - self.last_activity < self.idle_timeout
        )

    def stop(self) -> None:
        self._stopped = True

    def _wake(self) -> None:
        self.wakes += 1
        try:
            self._on_wake()
        except Exception:
            logger.warning("[mupot] inbox stream wake callback failed", exc_info=True)

    def handle_frame(self, frame: SSEFrame) -> None:
        self.last_activity = self._clock()
        if frame.kind not in {"message", "initial"}:
            return
        if frame.max_seq is not None:
            if self.last_seq is not None and frame.max_seq <= self.last_seq:
                return
            self.last_seq = frame.max_seq
        self._wake()

    async def run_once(self) -> None:
        """One connection: open with since=<last seq>, read until it drops."""
        parser = SSEFrameParser()
        self._productive = False
        async with self._opener(self.last_seq) as lines:
            self.connected = True
            self.last_activity = self._clock()
            iterator = lines.__aiter__()
            while not self._stopped:
                try:
                    line = await asyncio.wait_for(
                        iterator.__anext__(), timeout=self.idle_timeout
                    )
                except StopAsyncIteration:
                    return
                except asyncio.TimeoutError:
                    logger.warning(
                        "[mupot] inbox stream idle for %.0fs (no frame or ping); reconnecting",
                        self.idle_timeout,
                    )
                    return
                # Any line (including a bare ping comment) proves liveness and
                # means this connection was healthy: reset the backoff ladder.
                self.last_activity = self._clock()
                self.failures = 0
                self._productive = True
                frame = parser.feed_line(line)
                if frame is not None:
                    self.handle_frame(frame)

    async def run(self) -> None:
        """Reconnect forever (until stopped/cancelled) with capped jittered backoff."""
        while not self._stopped:
            error: Optional[BaseException] = None
            try:
                await self.run_once()
            except asyncio.CancelledError:
                self.connected = False
                raise
            except SSEHTTPError as exc:
                error = exc
                logger.warning("[mupot] inbox stream refused: HTTP %s", exc.status)
            except Exception as exc:
                error = exc
                logger.warning(
                    "[mupot] inbox stream failed: %s", type(exc).__name__
                )
            self.connected = False
            if self._stopped:
                return
            # The stream dropped after showing life: anything that arrived
            # while it was down (or between its last frame and the drop) is
            # only visible to a lease. One wake hands that to the lease path
            # now; the poll loop then falls back to its normal poll_interval
            # because healthy() is False. A connection that never produced a
            # single line is a failure for backoff purposes (a 200 that closes
            # immediately must not become a tight reconnect loop).
            if self._productive:
                self._wake()
            else:
                self.failures += 1
            if error is not None:
                logger.debug("[mupot] inbox stream error class=%s", type(error).__name__)
            delay = backoff_delay(
                self.failures,
                base=self._backoff_base,
                cap=self._backoff_cap,
                rand=self._rand,
            )
            await self._sleep(delay)
