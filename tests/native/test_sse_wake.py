"""Event-based wake (SSE) for the native receive path, and lease self-heal.

The stream is a hint, never custody: these tests pin that a frame only wakes
the existing lease loop, that the stream path never leases/acks/reads the
inbox itself, that a dead stream degrades to the configured poll interval,
and that a lease quarantine heals in-process through connect()'s own bounded
reconcile (never re-executing a still-leased turn).
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Optional

import httpx
import pytest

from gateway.config import PlatformConfig

from plugin.mupot_gateway import adapter as adapter_module
from plugin.mupot_gateway.adapter import MupotAdapter, MupotTransportError, StateStore
from plugin.mupot_gateway.sse_wake import (
    InboxStreamWaker,
    SSEFrame,
    SSEFrameParser,
    SSEHTTPError,
    backoff_delay,
    capped_lines,
    parse_event_payload,
    stream_url_from_mcp_url,
)
from plugin.tests.native.test_adapter import (
    FAKE_SCOPE,
    FakeMupotClient,
    fake_attempt_result,
)


# --------------------------------------------------------------------------
# frame parser


def feed(parser: SSEFrameParser, text: str) -> list[SSEFrame]:
    frames = []
    for line in text.split("\n"):
        frame = parser.feed_line(line)
        if frame is not None:
            frames.append(frame)
    return frames


def test_parser_classifies_ping_message_and_initial_frames() -> None:
    parser = SSEFrameParser()
    stream = (
        ": ping\n\n"
        'data: {"type":"message","message":{"id":"m","seq":41}}\n\n'
        'data: {"type":"initial","since":44,"messages":[{"seq":42},{"seq":44},{"seq":"43"}]}\n\n'
    )
    assert feed(parser, stream) == [
        SSEFrame("heartbeat"),
        SSEFrame("message", 41),
        SSEFrame("initial", 44),
    ]


def test_parser_joins_multiline_data_and_accepts_no_space_field() -> None:
    parser = SSEFrameParser()
    frames = feed(parser, 'data:{"type":"message",\ndata: "message":{"seq":7}}\n\n')
    assert frames == [SSEFrame("message", 7)]


@pytest.mark.parametrize(
    "payload",
    ["not json", "[]", '{"type":"heartbeat"}', '{"type":"initial","messages":[]}',
     '{"type":"message","message":"x"}'],
)
def test_parser_never_raises_on_odd_payloads(payload: str) -> None:
    frame = parse_event_payload(payload)
    assert frame.kind in {"other", "message"}
    if frame.kind == "message":
        assert frame.max_seq is None


@pytest.mark.parametrize("bad", [True, -1, 1.5, "7a", None])
def test_parser_rejects_non_integer_seq(bad: Any) -> None:
    frame = parse_event_payload(json.dumps({"type": "message", "message": {"seq": bad}}))
    assert frame == SSEFrame("message", None)


def test_parser_drops_oversized_frame_then_recovers() -> None:
    parser = SSEFrameParser(max_frame_bytes=64)
    frames = feed(
        parser,
        "data: " + "x" * 100 + "\n\n" + 'data: {"type":"message","message":{"seq":3}}\n\n',
    )
    assert frames == [SSEFrame("other"), SSEFrame("message", 3)]


def test_parser_ignores_non_data_fields_and_empty_dispatch() -> None:
    parser = SSEFrameParser()
    assert feed(parser, "event: message\nid: 9\nretry: 10\n\n") == []


# --------------------------------------------------------------------------
# line splitting, url, backoff


async def _chunks(parts: list[bytes]) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


@pytest.mark.asyncio
async def test_capped_lines_handles_crlf_split_across_chunks_and_bare_cr() -> None:
    parts = [b": ping\r", b"\n\r\ndata: a\rdata: b\n", b"\n", b"tail"]
    lines = [line async for line in capped_lines(_chunks(parts))]
    assert lines == [": ping", "", "data: a", "data: b", "", "tail"]


@pytest.mark.asyncio
async def test_capped_lines_refuses_an_unbounded_line() -> None:
    with pytest.raises(ValueError):
        async for _ in capped_lines(_chunks([b"x" * 50, b"y" * 50]), max_line_bytes=64):
            pass


@pytest.mark.parametrize(
    ("mcp_url", "expected"),
    [
        ("https://mupot.mumega.com/mcp", "https://mupot.mumega.com/api/inbox/stream"),
        ("https://mupot.mumega.com/mcp/", "https://mupot.mumega.com/api/inbox/stream"),
        ("https://pot.example/tenant/mcp?x=1#f", "https://pot.example/tenant/api/inbox/stream"),
        ("https://pot.example/api/inbox/stream", "https://pot.example/api/inbox/stream"),
        ("http://127.0.0.1:8787/mcp", "http://127.0.0.1:8787/api/inbox/stream"),
    ],
)
def test_stream_url_is_derived_from_the_mcp_url(mcp_url: str, expected: str) -> None:
    assert stream_url_from_mcp_url(mcp_url) == expected


@pytest.mark.parametrize("bad", ["http://mupot.mumega.com/mcp", "ftp://x/mcp", "", "/mcp"])
def test_stream_url_refuses_plaintext_or_hostless_urls(bad: str) -> None:
    with pytest.raises(ValueError):
        stream_url_from_mcp_url(bad)


def test_backoff_is_exponential_capped_and_jittered() -> None:
    top = [backoff_delay(n, base=1.0, cap=60.0, rand=lambda: 1.0) for n in range(9)]
    assert top == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0]
    bottom = [backoff_delay(n, base=1.0, cap=60.0, rand=lambda: 0.0) for n in range(9)]
    assert bottom == [t / 2 for t in top]
    assert backoff_delay(10_000, base=1.0, cap=60.0, rand=lambda: 1.0) == 60.0


# --------------------------------------------------------------------------
# waker (no adapter)


class FakeStream:
    """Scripted SSE server: one queue of lines per connection."""

    def __init__(self) -> None:
        self.opens: list[Optional[int]] = []
        self.connections: list[asyncio.Queue] = []
        self.refuse: list[Exception] = []
        self.opened = asyncio.Event()

    def next_connection(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        self.connections.append(queue)
        return queue

    def opener(self, since: Optional[int]):
        self.opens.append(since)
        stream = self

        @asynccontextmanager
        async def connection():
            if stream.refuse:
                raise stream.refuse.pop(0)
            queue = (
                stream.connections[len(stream.opens) - 1]
                if len(stream.connections) >= len(stream.opens)
                else stream.next_connection()
            )
            stream.opened.set()

            async def lines() -> AsyncIterator[str]:
                while True:
                    item = await queue.get()
                    if item is None:
                        return
                    if isinstance(item, BaseException):
                        raise item
                    yield item

            yield lines()

        return connection()


def message_frame(seq: int) -> list[str]:
    return [f'data: {{"type":"message","message":{{"id":"m-{seq}","seq":{seq}}}}}', ""]


async def wait_for(predicate, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_waker_wakes_once_per_new_seq_and_resumes_since_last_seq() -> None:
    stream = FakeStream()
    first, second = stream.next_connection(), stream.next_connection()
    wakes: list[int] = []
    sleeps: list[float] = []

    async def fast_sleep(delay: float) -> None:
        sleeps.append(delay)

    waker = InboxStreamWaker(
        opener=stream.opener, on_wake=lambda: wakes.append(1), sleep=fast_sleep,
    )
    task = asyncio.create_task(waker.run())
    try:
        for line in [": ping", "", *message_frame(5), *message_frame(5), *message_frame(4),
                     *message_frame(6)]:
            first.put_nowait(line)
        await wait_for(lambda: waker.last_seq == 6)
        # 5 wakes once, the replayed 5 and the stale 4 do not, 6 wakes.
        assert len(wakes) == 2
        first.put_nowait(None)  # server closes the stream
        await wait_for(lambda: len(stream.opens) == 2)
        # The drop itself wakes the lease loop once (gap cover) ...
        assert len(wakes) == 3
        # ... and the reconnect resumes strictly after the last seen seq.
        assert stream.opens == [None, 6]
        second.put_nowait(": ping")
        await wait_for(lambda: waker.healthy())
    finally:
        waker.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_waker_heartbeat_is_liveness_not_a_wake_and_idle_forces_reconnect() -> None:
    stream = FakeStream()
    first = stream.next_connection()
    stream.next_connection()
    wakes: list[int] = []
    clock = [0.0]

    async def fast_sleep(_delay: float) -> None:
        return None

    waker = InboxStreamWaker(
        opener=stream.opener, on_wake=lambda: wakes.append(1), idle_timeout=1.0,
        sleep=fast_sleep, clock=lambda: clock[0],
    )
    task = asyncio.create_task(waker.run())
    try:
        first.put_nowait(": ping")
        first.put_nowait("")
        await wait_for(lambda: waker.last_activity is not None and waker.connected)
        assert waker.healthy() and wakes == []
        clock[0] = 5.0  # no activity for longer than idle_timeout
        assert not waker.healthy()
        # No line arrives for idle_timeout seconds of real time: reconnect.
        await wait_for(lambda: len(stream.opens) == 2, timeout=3.0)
    finally:
        waker.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_waker_backs_off_exponentially_on_refusal_and_resets_after_life() -> None:
    stream = FakeStream()
    stream.refuse = [SSEHTTPError(503), SSEHTTPError(503), SSEHTTPError(401),
                     RuntimeError("tls")]
    healthy_conn = None
    sleeps: list[float] = []
    reached = asyncio.Event()

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)
        if len(sleeps) == 5:
            reached.set()
        await asyncio.sleep(0)

    waker = InboxStreamWaker(
        opener=stream.opener, on_wake=lambda: None, backoff_base=1.0, backoff_cap=4.0,
        sleep=record_sleep, rand=lambda: 1.0,
    )
    for _ in range(4):
        stream.next_connection()  # placeholders consumed by the refused opens
    healthy_conn = stream.next_connection()
    task = asyncio.create_task(waker.run())
    try:
        await wait_for(lambda: len(sleeps) == 4)
        assert sleeps == [2.0, 4.0, 4.0, 4.0]  # 1*2^1, 2^2, then capped at 4
        assert waker.failures == 4
        healthy_conn.put_nowait(": ping")
        healthy_conn.put_nowait(None)
        await asyncio.wait_for(reached.wait(), 2)
        assert waker.failures == 0
        assert sleeps[4] == 1.0  # ladder reset after a productive connection
    finally:
        waker.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_waker_counts_a_silent_200_as_a_failure() -> None:
    stream = FakeStream()
    for _ in range(3):
        stream.next_connection().put_nowait(None)  # 200, then closes, no bytes
    sleeps: list[float] = []
    wakes: list[int] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)
        await asyncio.sleep(0)

    waker = InboxStreamWaker(
        opener=stream.opener, on_wake=lambda: wakes.append(1), backoff_base=1.0,
        backoff_cap=60.0, sleep=record_sleep, rand=lambda: 1.0,
    )
    task = asyncio.create_task(waker.run())
    try:
        await wait_for(lambda: len(sleeps) >= 3)
        assert sleeps[:3] == [2.0, 4.0, 8.0]
        assert wakes == []  # nothing was ever observed: no gap wake either
    finally:
        waker.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_waker_holds_no_client_and_cannot_consume() -> None:
    """Structural: the waker is built from an opener and a wake callback only."""
    import inspect

    params = set(inspect.signature(InboxStreamWaker.__init__).parameters)
    assert params == {
        "self", "opener", "on_wake", "idle_timeout", "backoff_base", "backoff_cap",
        "sleep", "clock", "rand",
    }


# --------------------------------------------------------------------------
# adapter integration


class RecordingClient(FakeMupotClient):
    """Fake mupot that records every tool call and can queue messages."""

    def __init__(self) -> None:
        super().__init__()
        self.tools: list[str] = []
        self.queue: list[dict] = []
        self.acked_ids: list[str] = []
        self.leased: dict[str, dict] = {}

    def push(self, message_id: str, seq: int, sender: str = "hadi-codex") -> None:
        self.queue.append({
            **self.message, "id": message_id, "seq": seq, "from_agent": sender,
            "request_id": f"req-{message_id}",
        })

    async def call(self, tool: str, arguments: dict) -> dict:
        self.tools.append(tool)
        if tool == "inbox_lease":
            self.lease_calls += 1
            attempt_id = arguments["attempt_id"]
            if not self.queue:
                return fake_attempt_result(attempt_id, "empty")
            message = self.queue[0]
            self.leased[attempt_id] = message
            return fake_attempt_result(attempt_id, "leased", [message])
        if tool == "inbox_lease_ack":
            message = self.leased.pop(arguments["attempt_id"])
            self.queue.remove(message)
            self.acked_ids.append(message["id"])
            return {**FAKE_SCOPE, "attempt_id": arguments["attempt_id"], "state": "acked",
                    "consumed": True}
        if tool in {"inbox", "inbox_ack"}:
            raise AssertionError(f"{tool} must never be called by the native gateway")
        return await super().call(tool, arguments)


def sse_adapter(tmp_path: Path, client: FakeMupotClient, stream: FakeStream,
                **extra: Any) -> MupotAdapter:
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={
            "allowed_agents": "hadi-codex",
            "poll_interval": 30,  # a timed poll would never fire inside a test
            "sse_safety_poll_interval": 300,
            "sse_wake_enabled": True,
            "state_path": str(tmp_path / "state.json"),
            **extra,
        },
    )
    adapter = MupotAdapter(config, client_factory=lambda *_: client)
    adapter._open_inbox_stream = stream.opener  # type: ignore[method-assign]

    async def handler(event):
        return f"{{ack_for:{event.message_id}}} done"

    adapter.set_message_handler(handler)
    return adapter


@pytest.mark.asyncio
async def test_sse_frame_triggers_an_immediate_lease_and_full_cycle(tmp_path: Path) -> None:
    client = RecordingClient()
    stream = FakeStream()
    conn = stream.next_connection()
    adapter = sse_adapter(tmp_path, client, stream)
    assert await adapter.connect()
    try:
        await wait_for(lambda: client.lease_calls == 1)  # the connect-time lease
        await stream.opened.wait()
        conn.put_nowait(": ping")
        conn.put_nowait("")
        await asyncio.sleep(0.1)
        assert client.lease_calls == 1  # healthy + idle: no timed poll for 300s

        client.push("m-9", 9)
        for line in message_frame(9):
            conn.put_nowait(line)
        await wait_for(lambda: client.acked_ids == ["m-9"], timeout=3.0)
        assert "m-9" in StateStore(tmp_path / "state.json").load()["processed"]
        assert client.sent and client.sent[-1]["in_reply_to"] == "m-9"
        # Every consume went through the lease path; SSE never touched the inbox.
        assert "inbox" not in client.tools and "inbox_ack" not in client.tools
        assert client.tools.count("inbox_lease_ack") == 1
    finally:
        await adapter.disconnect()
    assert adapter._sse_task is None


@pytest.mark.asyncio
async def test_wake_storm_never_double_processes(tmp_path: Path) -> None:
    client = RecordingClient()
    stream = FakeStream()
    conn = stream.next_connection()
    # After a lease that found work the loop returns at poll_interval (the
    # historical drain pace), so keep it short; the property under test is
    # exactly-once custody under a storm of duplicate/phantom wakes.
    adapter = sse_adapter(tmp_path, client, stream, poll_interval=0.02)
    assert await adapter.connect()
    try:
        await stream.opened.wait()
        client.push("a", 11)
        client.push("b", 12)
        for seq in (11, 12, 12, 11, 13, 14):  # duplicates, reorder, phantom seqs
            for line in message_frame(seq):
                conn.put_nowait(line)
        await wait_for(lambda: client.acked_ids == ["a", "b"], timeout=5.0)
        await asyncio.sleep(0.1)
        assert client.acked_ids == ["a", "b"]
        assert len(client.sent) == 2
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_backlog_drains_without_waiting_for_the_safety_poll(tmp_path: Path) -> None:
    """One wake, three queued rows: work found -> come back at poll_interval."""
    client = RecordingClient()
    stream = FakeStream()
    conn = stream.next_connection()
    adapter = sse_adapter(tmp_path, client, stream, poll_interval=0.02)
    assert await adapter.connect()
    try:
        await stream.opened.wait()
        conn.put_nowait(": ping")
        conn.put_nowait("")
        for index, message_id in enumerate(("x", "y", "z")):
            client.push(message_id, 20 + index)
        for line in message_frame(22):
            conn.put_nowait(line)
        await wait_for(lambda: client.acked_ids == ["x", "y", "z"], timeout=3.0)
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_dead_stream_falls_back_to_poll_interval(tmp_path: Path) -> None:
    client = RecordingClient()
    stream = FakeStream()
    stream.refuse = [SSEHTTPError(503)] * 50
    adapter = sse_adapter(tmp_path, client, stream, poll_interval=0.02)
    assert await adapter.connect()
    try:
        await wait_for(lambda: client.lease_calls >= 5, timeout=2.0)
        assert not adapter._sse_waker.healthy()  # type: ignore[union-attr]
        client.push("late", 30)
        await wait_for(lambda: client.acked_ids == ["late"], timeout=2.0)
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_healthy_stream_slows_the_timed_poll(tmp_path: Path) -> None:
    client = RecordingClient()
    stream = FakeStream()
    conn = stream.next_connection()
    adapter = sse_adapter(tmp_path, client, stream, poll_interval=0.02)
    assert await adapter.connect()
    try:
        await stream.opened.wait()
        conn.put_nowait(": ping")
        conn.put_nowait("")
        await wait_for(lambda: adapter._sse_waker.healthy())  # type: ignore[union-attr]
        # Let any wait that started before the stream was healthy expire.
        await asyncio.sleep(0.1)
        baseline = client.lease_calls
        await asyncio.sleep(0.3)  # 15 poll_intervals
        assert client.lease_calls == baseline
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_stream_drop_wakes_a_lease_immediately(tmp_path: Path) -> None:
    client = RecordingClient()
    stream = FakeStream()
    conn = stream.next_connection()
    stream.next_connection()
    adapter = sse_adapter(tmp_path, client, stream)
    assert await adapter.connect()
    try:
        await stream.opened.wait()
        conn.put_nowait(": ping")
        conn.put_nowait("")
        await wait_for(lambda: client.lease_calls == 1)
        await asyncio.sleep(0.05)
        client.push("gap", 40)  # arrived while the stream was going away
        conn.put_nowait(None)
        await wait_for(lambda: client.acked_ids == ["gap"], timeout=2.0)
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_estop_blocks_wake_driven_leases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecordingClient()
    stream = FakeStream()
    conn = stream.next_connection()
    adapter = sse_adapter(tmp_path, client, stream, poll_interval=0.02)
    monkeypatch.setattr(adapter_module, "_estop_engaged", lambda: True)
    assert await adapter.connect()
    try:
        await stream.opened.wait()
        client.push("paused", 50)
        for seq in range(50, 60):
            for line in message_frame(seq):
                conn.put_nowait(line)
        await asyncio.sleep(0.2)
        assert client.lease_calls == 0
        assert client.acked_ids == []
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_mupot_dispatch_envelope_handling_is_unchanged_by_wake(tmp_path: Path) -> None:
    """A disallowed sender (incl. mupot-dispatch) takes the same lease-path
    branch with or without SSE: DLQ + lease-ack. The wake adds nothing."""
    results = {}
    for enabled in (False, True):
        client = RecordingClient()
        stream = FakeStream()
        conn = stream.next_connection()
        path = tmp_path / str(enabled)
        path.mkdir()
        adapter = sse_adapter(path, client, stream, sse_wake_enabled=enabled,
                              poll_interval=0.02)
        client.push("dispatch-1", 60, sender="mupot-dispatch")
        assert await adapter.connect()
        try:
            if enabled:
                await stream.opened.wait()
                for line in message_frame(60):
                    conn.put_nowait(line)
            await wait_for(lambda: client.acked_ids == ["dispatch-1"], timeout=2.0)
            await asyncio.sleep(0.05)
        finally:
            await adapter.disconnect()
        state = StateStore(path / "state.json").load()
        results[enabled] = (
            [row["reason"] for row in state["dlq"]], client.acked_ids, client.sent,
        )
    assert results[False] == results[True] == (["sender_policy"], ["dispatch-1"], [])


@pytest.mark.asyncio
async def test_disabled_wake_never_opens_a_stream(tmp_path: Path) -> None:
    client = RecordingClient()
    stream = FakeStream()
    adapter = sse_adapter(tmp_path, client, stream, sse_wake_enabled=False,
                          poll_interval=0.02)
    assert await adapter.connect()
    try:
        await wait_for(lambda: client.lease_calls >= 3)
        assert stream.opens == []
        assert adapter.sse_wake_status() == {"enabled": False}
    finally:
        await adapter.disconnect()


def test_sse_wake_enabled_must_be_a_boolean(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="sse_wake_enabled"):
        MupotAdapter(PlatformConfig(enabled=True, extra={
            "sse_wake_enabled": "true", "state_path": str(tmp_path / "s.json"),
        }), client_factory=lambda *_: FakeMupotClient())


def test_plaintext_sse_url_override_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="https"):
        MupotAdapter(PlatformConfig(enabled=True, extra={
            "sse_url": "http://mupot.example/api/inbox/stream",
            "state_path": str(tmp_path / "s.json"),
        }), client_factory=lambda *_: FakeMupotClient())


@pytest.mark.asyncio
async def test_real_opener_is_a_bearer_get_on_the_stream_endpoint_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []
    body = (b": ping\n\n"
            b'data: {"type":"message","message":{"id":"m","seq":77}}\n\n')

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=body,
                              headers={"content-type": "text/event-stream"})

    real_client = httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        kwargs["transport"] = httpx.MockTransport(handle)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(adapter_module.httpx, "AsyncClient", client_factory)
    monkeypatch.setattr(adapter_module, "read_profile_secret", lambda name: "tok-scoped")
    monkeypatch.setattr(adapter_module, "require_supported_profile_runtime", lambda _c: None)
    adapter = MupotAdapter(PlatformConfig(enabled=True, extra={
        "sse_wake_enabled": True, "sse_poll_ms": 500,
        "sse_url": "https://pot.example/mcp",
        "state_path": str(tmp_path / "s.json"),
    }), client_factory=lambda *_: FakeMupotClient())

    async with adapter._open_inbox_stream(12) as lines:
        received = [line async for line in lines]
    assert received[:2] == [": ping", ""]
    (request,) = requests
    assert request.method == "GET"
    assert request.url.path == "/api/inbox/stream"
    assert dict(request.url.params) == {"since": "12", "poll_ms": "500"}
    assert request.headers["authorization"] == "Bearer tok-scoped"
    assert request.headers["accept"] == "text/event-stream"

    def refuse(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "unauthorized"})

    monkeypatch.setattr(
        adapter_module.httpx, "AsyncClient",
        lambda *a, **k: real_client(*a, **{**k, "transport": httpx.MockTransport(refuse)}),
    )
    with pytest.raises(SSEHTTPError) as refused:
        async with adapter._open_inbox_stream(None):
            pass
    assert refused.value.status == 401
    assert "tok-scoped" not in str(refused.value)


# --------------------------------------------------------------------------
# in-process lease self-heal (deliverable A)


class QuarantineThenHealClient(RecordingClient):
    def __init__(self, reconcile_state: str) -> None:
        super().__init__()
        self.reconcile_state = reconcile_state
        self.fail_next_lease = True

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease" and self.fail_next_lease:
            self.tools.append(tool)
            self.lease_calls += 1
            self.fail_next_lease = False
            raise MupotTransportError("Mupot request failed")
        if tool == "inbox_lease_reconcile":
            self.tools.append(tool)
            messages = [self.message] if self.reconcile_state == "leased" else []
            return fake_attempt_result(arguments["attempt_id"], self.reconcile_state, messages)
        return await super().call(tool, arguments)


@pytest.mark.asyncio
async def test_lease_quarantine_self_heals_in_process_on_a_clean_tombstone(
    tmp_path: Path,
) -> None:
    client = QuarantineThenHealClient("expired")
    stream = FakeStream()
    adapter = sse_adapter(tmp_path, client, stream, sse_wake_enabled=False,
                          poll_interval=0.02, lease_self_heal_interval=0.05)
    assert await adapter.connect()
    try:
        await wait_for(lambda: adapter._lease_quarantined)
        await wait_for(lambda: "inbox_lease_reconcile" in client.tools, timeout=2.0)
        await wait_for(lambda: not adapter._lease_quarantined and adapter._running)
        assert StateStore(tmp_path / "state.json").load().get("lease_reconciliation") is None
        client.push("after-heal", 70)
        await wait_for(lambda: client.acked_ids == ["after-heal"], timeout=2.0)
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_lease_self_heal_never_executes_a_still_leased_attempt(
    tmp_path: Path,
) -> None:
    client = QuarantineThenHealClient("leased")
    stream = FakeStream()
    turns: list[str] = []
    adapter = sse_adapter(tmp_path, client, stream, sse_wake_enabled=False,
                          poll_interval=0.02, lease_self_heal_interval=0.02,
                          lease_self_heal_cap=0.04)

    async def handler(event):
        turns.append(event.message_id)
        return "should never run"

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    try:
        await wait_for(lambda: client.tools.count("inbox_lease_reconcile") >= 3, timeout=3.0)
        assert adapter._lease_quarantined is True
        assert adapter._running is False
        assert turns == [] and client.acked_ids == [] and client.sent == []
        marker = StateStore(tmp_path / "state.json").load().get("lease_reconciliation")
        assert isinstance(marker, dict) and marker["required"] is True
    finally:
        await adapter.disconnect()
    assert adapter._lease_self_heal_task is None
