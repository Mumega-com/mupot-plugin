from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
import time
import types
from contextvars import Context
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import ProcessingOutcome

from plugin.mupot_gateway.adapter import (  # noqa: E402
    MupotProtocolError,
    MupotTransportError,
    MupotAdapter,
    StateStore,
    _EstopDeferred,
    _LeaseExpiredDeferred,
    _TurnFailureDeferred,
    _configured_mcp_tool_timeout,
    build_mupot_event,
    is_ack_envelope,
    is_terminal_ack,
)
import plugin.mupot_gateway.adapter as mupot_adapter_module


FAKE_LEASE_EXPIRY = "2099-01-01T00:00:00.000Z"
FAKE_SCOPE = {
    "tenant": "tenant-a",
    "agent_id": "agent-consumer",
    "effective_inbox_seat": None,
}


def fake_attempt_result(
    attempt_id: str,
    state: str,
    messages: list[dict] | None = None,
) -> dict:
    return {
        **FAKE_SCOPE,
        "attempt_id": attempt_id,
        "state": state,
        "lease_expires_at": FAKE_LEASE_EXPIRY if state == "leased" else None,
        "messages": messages or [],
        "consumed": False,
    }


class FakeMupotClient:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.acked = False
        self.connect_calls = 0
        self.lease_calls = 0
        self.message = {
            "id": "m-1",
            "seq": 7,
            "from_agent": "hadi-codex",
            "from_member": "member-code",
            "body": "full Mupot answer",
            "project_id": "project-1",
            "request_id": "req-7",
            "in_reply_to": None,
            "kind": "message",
            "created_at": "2026-09-13T00:00:00.000Z",
            "delivery_attempts": 1,
            "lease_expires_at": FAKE_LEASE_EXPIRY,
        }

    async def connect(self) -> None:
        self.connect_calls += 1
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease":
            self.lease_calls += 1
            attempt_id = arguments.get("attempt_id")
            if isinstance(attempt_id, str):
                messages = [] if self.acked else [self.message]
                return fake_attempt_result(
                    attempt_id,
                    "empty" if self.acked else "leased",
                    messages,
                )
            return {
                "messages": [] if self.acked else [self.message],
                "remaining": 0,
                "complete": True,
                "dead_lettered": 0,
                "lease_seconds": arguments["lease_seconds"],
            }
        if tool == "inbox_ack":
            assert arguments == {"ids": ["m-1"]}
            self.acked = True
            return {"acked": ["m-1"], "already_read": [], "refused": []}
        if tool == "inbox_lease_ack":
            self.acked = True
            return {
                **FAKE_SCOPE,
                "attempt_id": arguments["attempt_id"],
                "state": "acked",
                "consumed": True,
            }
        if tool == "send":
            self.sent.append(arguments)
            return {
                "id": "m-2",
                "seq": 8,
                "duplicate": False,
                "to": arguments["to"],
                "project_id": arguments.get("project_id"),
                "target_seat": None,
            }
        if tool == "inbox_consumer_status":
            return {
                "strict_scope": True,
                **FAKE_SCOPE,
                "mode": "bearer_only",
                "generation": 0,
                "key_matches": True,
            }
        raise AssertionError(f"unexpected tool: {tool} {arguments}")


class AckMupotClient(FakeMupotClient):
    def __init__(
        self,
        *,
        fail_ack_once: bool = False,
        safe_ack_once: bool = False,
    ) -> None:
        super().__init__()
        self.message = {
            "id": "ack-1",
            "seq": 9,
            "from_agent": "hadi-codex",
            "from_member": "member-code",
            "body": "{ack_for:request-1} received",
            "request_id": "ack:request-1",
            "in_reply_to": "source-1",
            "kind": "ack",
            "expects_reply": False,
            "created_at": "2026-09-13T00:00:00.000Z",
            "delivery_attempts": 1,
            "lease_expires_at": FAKE_LEASE_EXPIRY,
        }
        self.fail_ack_once = fail_ack_once
        self.safe_ack_once = safe_ack_once
        self.ack_calls = 0

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool in {"inbox_ack", "inbox_lease_ack"}:
            self.ack_calls += 1
            if self.safe_ack_once and self.ack_calls == 1:
                from plugin.mupot_gateway import adapter as adapter_module

                safe_error = getattr(adapter_module, "MupotSafeRetryError", RuntimeError)
                raise safe_error("Mupot request failed")
            if self.fail_ack_once and self.ack_calls == 1:
                if tool == "inbox_lease_ack":
                    return {
                        **FAKE_SCOPE,
                        "attempt_id": arguments["attempt_id"],
                        "state": "expired",
                        "consumed": False,
                    }
                return {"acked": [], "already_read": [], "refused": ["ack-1"]}
            self.acked = True
            if tool == "inbox_lease_ack":
                return {
                    **FAKE_SCOPE,
                    "attempt_id": arguments["attempt_id"],
                    "state": "acked",
                    "consumed": True,
                }
            return {"acked": ["ack-1"], "already_read": [], "refused": []}
        return await super().call(tool, arguments)


class LeaseFailureClient(FakeMupotClient):
    def __init__(self, failure: Exception) -> None:
        super().__init__()
        self.failure = failure
        self.lease_calls = 0
        self.first_lease = asyncio.Event()

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease":
            self.lease_calls += 1
            self.first_lease.set()
            raise self.failure
        return await super().call(tool, arguments)


class LeasePayloadClient(FakeMupotClient):
    def __init__(self, payload: object) -> None:
        super().__init__()
        self.payload = payload
        self.first_lease = asyncio.Event()

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease":
            self.lease_calls += 1
            self.first_lease.set()
            if callable(self.payload):
                return self.payload(arguments)  # type: ignore[no-any-return]
            return self.payload  # type: ignore[return-value]
        return await super().call(tool, arguments)


class TimedLeaseFailureClient(FakeMupotClient):
    def __init__(
        self,
        clock: list[float],
        outcomes: list[tuple[float, Exception]],
    ) -> None:
        super().__init__()
        self.clock = clock
        self.outcomes = outcomes
        self.first_lease = asyncio.Event()

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease":
            self.lease_calls += 1
            self.first_lease.set()
            issued_at, failure = self.outcomes.pop(0)
            self.clock[0] = issued_at
            raise failure
        return await super().call(tool, arguments)


class ReconciliationClient(FakeMupotClient):
    def __init__(self, status: object, reconcile_outcome: object = None) -> None:
        super().__init__()
        self.status = (
            {"strict_scope": True, **FAKE_SCOPE, **status}
            if isinstance(status, dict)
            else status
        )
        self.reconcile_outcome = reconcile_outcome
        self.tools: list[str] = []

    async def call(self, tool: str, arguments: dict) -> dict:
        self.tools.append(tool)
        if tool == "inbox_consumer_status":
            return self.status  # type: ignore[return-value]
        if tool == "inbox_lease_reconcile":
            if self.reconcile_outcome is not None:
                return self.reconcile_outcome  # type: ignore[return-value]
            return fake_attempt_result(arguments["attempt_id"], "cancelled")
        raise AssertionError(f"unexpected reconciliation tool: {tool} {arguments}")


class GenerationClient(FakeMupotClient):
    def __init__(self) -> None:
        super().__init__()
        self.acked_ids: list[str] = []

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_ack":
            message_id = arguments["ids"][0]
            self.acked_ids.append(message_id)
            return {"acked": [message_id], "already_read": [], "refused": []}
        return await super().call(tool, arguments)


def delivery_message(
    message_id: str,
    *,
    sender: str = "hadi-codex",
    project: str = "project-1",
    body: str = "run this turn",
    lease_expires_at: str | None = None,
) -> dict:
    message = {
        "id": message_id,
        "seq": 7,
        "from_agent": sender,
        "body": body,
        "project_id": project,
        "request_id": f"request-{message_id}",
        "in_reply_to": None,
        "kind": "message",
    }
    if lease_expires_at is not None:
        message["lease_expires_at"] = lease_expires_at
    return message


async def persist_ambiguous_lease_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    monkeypatch.setattr(time, "time", lambda: 100.0)
    state_path = tmp_path / "state.json"
    client = LeaseFailureClient(MupotTransportError("Mupot request failed"))
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "lease_seconds": 1,
                "rpc_timeout": 5,
                "poll_interval": 0.01,
                "state_path": str(state_path),
            },
        ),
        client_factory=lambda *_: client,
    )
    assert await adapter.connect()
    try:
        await asyncio.wait_for(client.first_lease.wait(), 1)
        for _ in range(100):
            if not adapter._running:
                break
            await asyncio.sleep(0.01)
        assert adapter._running is False
    finally:
        await adapter.disconnect()
    return state_path


def test_build_mupot_event_preserves_project_and_correlation() -> None:
    event = build_mupot_event(
        {
            "id": "m-1",
            "seq": 7,
            "from_agent": "hadi-codex",
            "body": "answer",
            "project_id": "project-1",
            "request_id": "req-7",
            "in_reply_to": "m-0",
        }
    )

    assert event.text == "answer"
    assert event.message_id == "m-1"
    assert event.internal is True
    assert event.source.chat_id == "hadi-codex"
    assert event.source.thread_id == "project-1"
    assert event.metadata["request_id"] == "req-7"
    assert event.metadata["in_reply_to"] == "m-0"


def test_unscoped_message_gets_isolated_session() -> None:
    event = build_mupot_event(
        {
            "id": "m-unscoped",
            "seq": 8,
            "from_agent": "hadi-codex",
            "body": "control command",
            "request_id": "req-8",
        }
    )

    assert event.internal is True
    assert event.source.thread_id == "m-unscoped"


def test_lease_seconds_defaults_to_turn_timeout_plus_mcp_tool_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Class fix (2026-09-15): before this, `lease_seconds` defaulted to
    `turn_timeout + 60s` alone, smaller than `turn_timeout + mcp_tool_timeout`
    whenever a real deployment's MCP tool budget exceeds 60s (the kayhermes
    incident ran with it at 180s) -- a single slow-but-legitimate tool call
    could then always outlive the lease. Pin the new formula and its knobs.

    Item 7 (kasra-review re-gate round 2, 2026-09-15): `mcp_tool_timeout`'s
    default now reads the mupot MCP server's own configured `timeout` (see
    `_configured_mcp_tool_timeout`) instead of a hardcoded 300.0 -- pin that
    with a monkeypatched reader so this test is deterministic regardless of
    what a given test environment's own Hermes config happens to have for
    the `mupot` MCP server (this repo's own dev config resolves it to 180.0,
    confirmed by running this test unpatched).

    OPEN-D (kasra re-gate round 3, 2026-09-15): also pins that an explicit
    `lease_seconds` above the safe minimum is honored verbatim, while one
    below it is clamped UP to the minimum (with a WARNING) rather than
    honored -- see `explicit_lease_clamped` below."""
    monkeypatch.setattr(
        mupot_adapter_module, "_configured_mcp_tool_timeout", lambda _name: 300.0
    )
    default_adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "a.json")}),
        client_factory=lambda *_: FakeMupotClient(),
    )
    assert default_adapter.turn_timeout == 300.0
    assert default_adapter.mcp_tool_timeout == 300.0
    assert default_adapter.lease_seconds == 300 + 300 + 60

    monkeypatch.setattr(
        mupot_adapter_module, "_configured_mcp_tool_timeout", lambda _name: 180.0
    )
    reads_configured_default = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "configured.json")}),
        client_factory=lambda *_: FakeMupotClient(),
    )
    assert reads_configured_default.mcp_tool_timeout == 180.0
    assert reads_configured_default.lease_seconds == 300 + 180 + 60

    overridden = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "state_path": str(tmp_path / "b.json"),
                "turn_timeout": 120,
                "mcp_tool_timeout": 180,
            },
        ),
        client_factory=lambda *_: FakeMupotClient(),
    )
    assert overridden.lease_seconds == 120 + 180 + 60

    explicit_lease_honored = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "state_path": str(tmp_path / "c.json"),
                "turn_timeout": 120,
                "mcp_tool_timeout": 180,
                # Above the safe minimum (120 + 180 + 60 = 360) -- must be
                # honored exactly, not silently clamped to the minimum.
                "lease_seconds": 500,
            },
        ),
        client_factory=lambda *_: FakeMupotClient(),
    )
    assert explicit_lease_honored.lease_seconds == 500

    # OPEN-D (kasra re-gate round 3, 2026-09-15): an explicit `lease_seconds`
    # SMALLER than turn_timeout + mcp_tool_timeout + margin used to be
    # honored verbatim -- a configuration that guarantees the message's own
    # lease expires before a legitimately-slow turn can finish, on every
    # single turn. Must clamp UP to the safe minimum instead, and log loudly
    # about it (an operator who set it deliberately needs to know it didn't
    # take effect).
    with caplog.at_level(logging.WARNING, logger="plugin.mupot_gateway.adapter"):
        explicit_lease_clamped = MupotAdapter(
            PlatformConfig(
                enabled=True,
                extra={
                    "state_path": str(tmp_path / "d.json"),
                    "turn_timeout": 120,
                    "mcp_tool_timeout": 180,
                    "lease_seconds": 42,
                },
            ),
            client_factory=lambda *_: FakeMupotClient(),
        )
    assert explicit_lease_clamped.lease_seconds == 120 + 180 + 60
    assert any(
        "lease_seconds=42" in record.getMessage() and "clamping up" in record.getMessage()
        for record in caplog.records
    )


def test_max_delivery_attempts_clamped_to_server_ceiling(tmp_path: Path) -> None:
    """OPEN-D (kasra re-gate round 3, 2026-09-15): the server's own hard
    ceiling on `delivery_attempts` before it stops redelivering entirely is
    `MAX_DELIVERY_ATTEMPTS = 5` (mupot/src/agents/messages.ts:879). An
    operator-configured `max_delivery_attempts` above that used to be
    honored verbatim -- dead configuration past attempt 5, since the server
    never delivers a 6th attempt for `_resolve_turn_failure` to see, leaving
    it waiting forever for a `delivery_attempts` value that will never
    arrive (no local DLQ row, no `mupot_gateway_status` visibility)."""
    below_ceiling = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"state_path": str(tmp_path / "below.json"), "max_delivery_attempts": 4},
        ),
        client_factory=lambda *_: FakeMupotClient(),
    )
    assert below_ceiling.max_delivery_attempts == 4

    above_ceiling = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"state_path": str(tmp_path / "above.json"), "max_delivery_attempts": 99},
        ),
        client_factory=lambda *_: FakeMupotClient(),
    )
    assert above_ceiling.max_delivery_attempts == 5
    assert above_ceiling.max_delivery_attempts == mupot_adapter_module._SERVER_MAX_DELIVERY_ATTEMPTS


def test_default_max_delivery_attempts_is_five_not_a_mutated_default(tmp_path: Path) -> None:
    """M-A (kasra re-gate round 3, 2026-09-15): the unconfigured default for
    `max_delivery_attempts` is a named-constant fallback
    (`... or _SERVER_MAX_DELIVERY_ATTEMPTS`) inside the constructor -- a
    mutation of that constant's value is invisible to any test that reads
    the count back off `adapter.max_delivery_attempts` itself (both sides of
    the assertion see the SAME mutated value) OR compares it against the
    constant by name. Pin the literal directly instead.

    Round 4 (Athena second eye, head c68e0839, MED "cap surface"): the
    default changed from a bare `3` to `_SERVER_MAX_DELIVERY_ATTEMPTS`
    (currently `5`) -- an operator-configured local cap smaller than the
    server's own dead-letter ceiling let this adapter DLQ a message the
    server would have redelivered for two more attempts, and (before the
    at-cap no-ack fix, same round) acking at that smaller local cap
    permanently prevented the server's own reaper from ever dead-lettering
    the row itself."""
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: FakeMupotClient(),
    )
    assert adapter.max_delivery_attempts == 5


def test_lease_is_binding_boundary_classifies_as_turn_failure_not_lease_expiry() -> None:
    """M-C / OPEN-D (kasra re-gate round 3, 2026-09-15): at an EXACT tie
    between the turn's own deadline and its message's lease deadline,
    `_lease_is_binding` must return `False` -- classified as a TURN failure
    (`_TurnFailureDeferred`, bounded by `delivery_attempts` and DLQ'd at the
    cap), not a lease expiry (`_LeaseExpiredDeferred`, retried unboundedly
    by design). Neither reading is more "correct" at an exact tie; this
    picks the side that fails BOUNDED rather than the side that fails OPEN
    to unbounded redelivery. Pre-fix `<=` would have returned `True` here --
    a pure boundary-value unit test on the static method itself, immune to
    any floating-point/clock rounding an end-to-end `_deliver()` repro of
    the same tie would be exposed to."""
    assert MupotAdapter._lease_is_binding(100.0, 100.0) is False
    assert MupotAdapter._lease_is_binding(100.0, 99.999999) is True
    assert MupotAdapter._lease_is_binding(100.0, 100.000001) is False


def test_configured_mcp_tool_timeout_reads_real_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OPEN-E (kasra re-gate round 3, 2026-09-15): this test used to assert
    `_configured_mcp_tool_timeout("mupot") == 180.0` by reading THIS REPO'S
    OWN ambient dev Hermes config (`~/.hermes/config.yaml` or equivalent) --
    a real, silent CI hazard: any environment whose `mupot` MCP server
    happens to be configured differently (or not configured at all --
    `_DEFAULT_MCP_TOOL_TIMEOUT_SECONDS` is 300.0) fails this assertion for a
    reason that has nothing to do with the code under test. Renamed (was
    `..._reads_real_hermes_config`) and rewritten to inject its own config
    via a monkeypatched `load_config`, matching the pattern already used by
    `tests/native/test_mcp_transport.py`'s `install_transport` -- CI parity
    with the ambient host is exactly what this fix rejects."""
    from hermes_cli import config as config_module
    from hermes_cli import mcp_config

    monkeypatch.setattr(
        config_module,
        "load_config",
        lambda: {"mcp_servers": {"mupot": {"timeout": 180}}},
    )
    monkeypatch.setattr(mcp_config, "_resolve_mcp_server_config", lambda value: value)
    assert _configured_mcp_tool_timeout("mupot") == 180.0


def test_configured_mcp_tool_timeout_falls_back_when_server_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Countercase: an MCP server name with no entry in `mcp_servers` must
    not raise -- constructing an adapter must never fail just because
    lease-sizing's default couldn't be read. Injects its own (empty) config,
    same OPEN-E reasoning as the positive case above -- this must not depend
    on whatever the host's ambient config happens to configure (or not)."""
    from hermes_cli import config as config_module

    monkeypatch.setattr(config_module, "load_config", lambda: {"mcp_servers": {}})
    assert (
        _configured_mcp_tool_timeout("a-server-name-nothing-configures")
        == mupot_adapter_module._DEFAULT_MCP_TOOL_TIMEOUT_SECONDS
        == 300.0
    )


def test_configured_mcp_tool_timeout_falls_back_when_configured_without_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OPEN-E: a server present in `mcp_servers` but missing an explicit
    `timeout` key used to fall back to a DIFFERENT, smaller hardcoded number
    (30.0) than the "server absent entirely" case (300.0) -- two unrelated
    fallback values for two variants of the same "we don't actually know
    this server's budget" situation, with the smaller one risking an
    under-sized `lease_seconds` (see OPEN-D). Both fallbacks must now be the
    SAME documented default."""
    from hermes_cli import config as config_module
    from hermes_cli import mcp_config

    monkeypatch.setattr(
        config_module,
        "load_config",
        lambda: {"mcp_servers": {"mupot": {"url": "https://pot.example.invalid/mcp"}}},
    )
    monkeypatch.setattr(mcp_config, "_resolve_mcp_server_config", lambda value: value)
    assert (
        _configured_mcp_tool_timeout("mupot")
        == mupot_adapter_module._DEFAULT_MCP_TOOL_TIMEOUT_SECONDS
        == 300.0
    )


def test_terminal_ack_requires_no_explicit_reply_expectation() -> None:
    assert is_terminal_ack({"id": "ack-1", "kind": "ack", "expects_reply": False})
    assert not is_terminal_ack({"id": "ack-1", "kind": "ack"})
    assert not is_terminal_ack({"id": "ack-1", "kind": "ack", "expects_reply": True})
    assert not is_terminal_ack({"kind": "ack", "expects_reply": False})
    assert not is_terminal_ack({"kind": "message", "expects_reply": False})
    assert is_ack_envelope({"kind": "ack", "body": "spoof"})
    assert not is_ack_envelope({"kind": "message", "expects_reply": False, "body": "{ack_for:x}"})


@pytest.mark.asyncio
async def test_send_without_delivery_context_fails_closed(tmp_path: Path) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    adapter._begin_delivery(delivery_message("live-but-unbound"))

    result = await Context().run(
        lambda: asyncio.create_task(adapter.send("hadi-codex", "must not escape"))
    )

    assert result.success is False
    assert result.retryable is False
    assert client.sent == []


@pytest.mark.asyncio
async def test_wrong_recipient_and_inconsistent_callback_fail_closed(tmp_path: Path) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    event, runtime = adapter._begin_delivery(delivery_message("source-context"))
    await adapter.on_processing_start(event)

    result = await adapter.send("other-agent", "misdirected")
    event.raw_message = {**event.raw_message, "request_id": "other-request"}
    await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)

    assert result.success is False
    assert client.sent == []
    assert runtime.completion_event.is_set() is False


@pytest.mark.asyncio
async def test_callback_without_context_cannot_complete_live_generation(
    tmp_path: Path,
) -> None:
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: GenerationClient(),
    )
    event, runtime = adapter._begin_delivery(delivery_message("source-no-context"))

    task = Context().run(
        lambda: asyncio.create_task(
            adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
        )
    )
    await task

    assert runtime.completion_event.is_set() is False
    assert runtime.outcome is None


@pytest.mark.asyncio
async def test_expired_leased_event_never_starts_model_work(tmp_path: Path) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    handled: list[str] = []

    async def handler(event):
        handled.append(event.message_id)
        return "unexpected"

    adapter.set_message_handler(handler)
    expired = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()

    # Class fix (2026-09-15, see _LeaseExpiredDeferred): an already-expired
    # lease is a deferral, not a silent no-op -- _deliver raises so callers
    # (the poll loop, reconcile_inbox_polling) release the lease instead of
    # quarantining.
    with pytest.raises(_LeaseExpiredDeferred):
        await adapter._deliver(delivery_message("expired", lease_expires_at=expired))

    assert handled == []
    assert client.sent == []
    assert client.acked_ids == []


@pytest.mark.asyncio
async def test_queued_event_expiry_cancels_session_before_model_start(tmp_path: Path) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    adapter.turn_timeout = 0.05
    handled: list[str] = []

    async def handler(event):
        handled.append(event.message_id)
        return "unexpected"

    adapter.set_message_handler(handler)
    message = delivery_message("queued-expiry")
    preview = build_mupot_event(message, adapter.platform)
    session_key = adapter._event_session_key(preview)
    blocker = asyncio.create_task(asyncio.Event().wait())
    adapter._active_sessions[session_key] = asyncio.Event()
    adapter._session_tasks[session_key] = blocker
    adapter._background_tasks.add(blocker)
    try:
        # Class fix (2026-09-15, see _LeaseExpiredDeferred): the queued
        # message's own runtime expires (via the turn-timeout wait) before
        # the handler ever runs -- a deferral, not a silent no-op.
        #
        # Round 2 (kasra-review re-gate BLOCK-2, 2026-09-15): `message`
        # carries no `lease_expires_at` at all (see `delivery_message`'s
        # default), so `runtime.lease_deadline is None` -- the lease can
        # never be the binding constraint here, only `turn_timeout` can be,
        # so this is now `_TurnFailureDeferred(kind="turn_timeout")`, not a
        # lease expiry (see `_lease_is_binding`).
        with pytest.raises(_TurnFailureDeferred) as excinfo:
            await adapter._deliver(message)
        assert excinfo.value.kind == "turn_timeout"
        assert handled == []
        assert session_key not in adapter._pending_messages
        assert blocker.cancelled()
        assert client.sent == []
    finally:
        blocker.cancel()
        await asyncio.gather(blocker, return_exceptions=True)


@pytest.mark.asyncio
async def test_timeout_invalidates_generation_before_bounded_cancellation(
    tmp_path: Path,
) -> None:
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: GenerationClient(),
    )
    adapter.turn_timeout = 0.02
    adapter.cancel_timeout = 0.02
    cancellation_started = asyncio.Event()
    registry_snapshots = []

    async def handler(_event):
        await asyncio.Event().wait()

    async def stuck_cancel(session_key):
        registry_snapshots.append((session_key, dict(adapter._live_generations)))
        cancellation_started.set()
        await asyncio.Event().wait()

    adapter.set_message_handler(handler)
    adapter.cancel_session_processing = stuck_cancel
    started = time.monotonic()
    # Class fix (2026-09-15, see _LeaseExpiredDeferred): a genuine turn
    # timeout is a deferral, not a silent no-op -- _deliver raises instead
    # of returning.
    #
    # Round 2 (kasra-review re-gate BLOCK-2, 2026-09-15): `delivery_message`
    # carries no `lease_expires_at`, so the lease can never be the binding
    # constraint -- this is a turn that hung past its OWN `turn_timeout`
    # with nothing to say about the lease at all, i.e. exactly the class
    # `_TurnFailureDeferred` exists to name (kind="turn_timeout"), bounded
    # by `delivery_attempts` rather than an unbounded `_LeaseExpiredDeferred`
    # retry loop.
    #
    # Round 3 (kasra re-gate BLOCK-A, 2026-09-15): `pending` used to be
    # pinned SET here on the theory that a hung handler might have taken
    # real side effects worth fencing for a human as crash-ambiguity. That
    # was itself the BLOCK-A defect: a below-cap `_TurnFailureDeferred` is
    # not a crash -- it's this function returning cleanly enough to reach
    # its own raise -- and leaving `pending` set reproduced the exact
    # durable-brick shape this whole fix exists to close (next restart's
    # `_legacy_pending_ambiguous` refusing `connect()` forever over a
    # perfectly bounded, in-flight redelivery). `_resolve_turn_failure` now
    # clears `pending` before raising, same as every other
    # `_DeliveryDeferred` site; this test pins THAT invariant instead.
    with pytest.raises(_TurnFailureDeferred) as excinfo:
        await adapter._deliver(delivery_message("bounded-timeout"))
    assert excinfo.value.kind == "turn_timeout"
    elapsed = time.monotonic() - started

    assert cancellation_started.is_set()
    assert registry_snapshots[0][1] == {}
    assert elapsed < 0.2
    pending = StateStore(tmp_path / "state.json").load()["pending"]
    assert pending is None
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_disconnect_invalidates_generation_before_surviving_callback(
    tmp_path: Path,
) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    adapter.turn_timeout = 1.0
    started = asyncio.Event()
    late_release = asyncio.Event()
    late_done = asyncio.Event()
    late_results = []
    cancellation_snapshots = []
    original_cancel = adapter.cancel_session_processing

    async def checking_cancel(session_key):
        cancellation_snapshots.append((session_key, dict(adapter._live_generations)))
        await original_cancel(session_key)

    adapter.cancel_session_processing = checking_cancel

    async def handler(event):
        async def finish_after_disconnect() -> None:
            await late_release.wait()
            late_results.append(await adapter.send(event.source.chat_id, "late output"))
            late_done.set()

        asyncio.create_task(finish_after_disconnect())
        started.set()
        await asyncio.Event().wait()

    adapter.set_message_handler(handler)
    delivery = asyncio.create_task(adapter._deliver(delivery_message("disconnect-source")))
    try:
        await asyncio.wait_for(started.wait(), 1)
        await adapter.disconnect()
        await asyncio.wait_for(delivery, 1)
        assert adapter._live_generations == {}
        assert cancellation_snapshots[0][1] == {}

        late_release.set()
        await asyncio.wait_for(late_done.wait(), 1)
        assert len(late_results) == 1
        assert late_results[0].success is False
        assert client.sent == []
        pending = StateStore(tmp_path / "state.json").load()["pending"]
        assert pending["message"]["id"] == "disconnect-source"
    finally:
        late_release.set()
        await asyncio.gather(delivery, return_exceptions=True)


@pytest.mark.asyncio
async def test_timed_out_thread_callback_cannot_send_with_next_delivery_context(
    tmp_path: Path,
) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "state_path": str(tmp_path / "state.json"),
                "notification_recipients": {"telegram": "owner"},
            },
        ),
        client_factory=lambda *_: client,
    )
    adapter.turn_timeout = 0.05
    thread_ready = threading.Event()
    release_thread = threading.Event()
    thread_done = threading.Event()
    late_results = []
    b_started = asyncio.Event()
    release_b = asyncio.Event()
    late_event = None

    async def handler(event):
        nonlocal late_event
        if event.message_id == "source-a":
            late_event = event
            loop = asyncio.get_running_loop()

            def late_thread_callback() -> None:
                thread_ready.set()
                release_thread.wait(2)
                future = asyncio.run_coroutine_threadsafe(
                    adapter.send(event.source.chat_id, "late A final"), loop
                )
                late_results.append(future.result(2))
                completion = asyncio.run_coroutine_threadsafe(
                    adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS), loop
                )
                completion.result(2)
                thread_done.set()

            await asyncio.to_thread(late_thread_callback)
            return "late A handler return"
        b_started.set()
        await release_b.wait()
        return "B final"

    adapter.set_message_handler(handler)
    a = delivery_message("source-a", sender="hadi-codex", project="project-a")
    b = delivery_message("source-b", sender="kasra", project="project-b")
    try:
        # Class fix (2026-09-15, see _LeaseExpiredDeferred): source-a's
        # turn_timeout (0.05) is exhausted while its late thread callback is
        # still blocked on `release_thread` -- a genuine turn timeout, now a
        # deferral rather than a silent no-op.
        #
        # Round 2 (kasra-review re-gate BLOCK-2, 2026-09-15): `a` carries no
        # `lease_expires_at`, so the lease can never be the binding
        # constraint -- `turn_timeout` is, which is now
        # `_TurnFailureDeferred(kind="turn_timeout")`, not a lease expiry.
        with pytest.raises(_TurnFailureDeferred) as excinfo:
            await adapter._deliver(a)
        assert excinfo.value.kind == "turn_timeout"
        assert thread_ready.wait(1)
        adapter.turn_timeout = 1.0
        b_delivery = asyncio.create_task(adapter._deliver(b))
        await asyncio.wait_for(b_started.wait(), 1)
        release_thread.set()
        await asyncio.wait_for(asyncio.to_thread(thread_done.wait, 2), 3)

        assert len(late_results) == 1
        assert late_results[0].success is False
        assert b_delivery.done() is False
        assert client.sent == []
        assert StateStore(tmp_path / "state.json").load().get("notification_outbox") == {}

        release_b.set()
        await asyncio.wait_for(b_delivery, 1)
        assert client.sent == [
            {
                "to": "kasra",
                "body": "B final",
                "kind": "ack",
                "project_id": "project-b",
                "request_id": "resp-source-b",
                "in_reply_to": "source-b",
            }
        ]
        notices = StateStore(tmp_path / "state.json").load()["notification_outbox"]
        assert list(notices) == ["source-b"]
        assert client.acked_ids == ["source-b"]
    finally:
        release_thread.set()
        release_b.set()
        await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_late_same_source_redelivery_cannot_complete_new_generation(
    tmp_path: Path,
) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    adapter.turn_timeout = 0.05
    late_release = asyncio.Event()
    late_done = asyncio.Event()
    b_started = asyncio.Event()
    release_b = asyncio.Event()
    handler_calls = 0

    async def handler(event):
        nonlocal handler_calls
        handler_calls += 1
        if handler_calls == 1:
            async def finish_late() -> None:
                await late_release.wait()
                await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
                late_done.set()

            asyncio.create_task(finish_late())
            await asyncio.Event().wait()
        b_started.set()
        await release_b.wait()
        return "redelivery final"

    adapter.set_message_handler(handler)
    message = delivery_message(
        "same-source",
        sender="hadi-codex",
        project="same-project",
        body="same body",
    )
    try:
        # Class fix (2026-09-15, see _LeaseExpiredDeferred): the first
        # delivery's turn_timeout (0.05) expires while its handler is still
        # blocked -- a genuine turn timeout, now a deferral rather than a
        # silent no-op.
        #
        # Round 2 (kasra-review re-gate BLOCK-2, 2026-09-15): `message`
        # carries no `lease_expires_at`, so this is
        # `_TurnFailureDeferred(kind="turn_timeout")`, not a lease expiry.
        with pytest.raises(_TurnFailureDeferred) as excinfo:
            await adapter._deliver(message)
        assert excinfo.value.kind == "turn_timeout"
        adapter.turn_timeout = 1.0
        b_delivery = asyncio.create_task(adapter._deliver(dict(message)))
        await asyncio.wait_for(b_started.wait(), 1)
        late_release.set()
        await asyncio.wait_for(late_done.wait(), 1)
        await asyncio.sleep(0)
        assert b_delivery.done() is False
        assert client.acked_ids == []

        release_b.set()
        await asyncio.wait_for(b_delivery, 1)
        assert client.acked_ids == ["same-source"]
    finally:
        late_release.set()
        release_b.set()
        await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_adapter_leases_then_acks_only_after_success(tmp_path: Path) -> None:
    client = FakeMupotClient()
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={
            "allowed_agents": "hadi-codex,hadi-codex-cli",
            "poll_interval": 0.01,
            "state_path": str(tmp_path / "state.json"),
        },
    )
    adapter = MupotAdapter(config, client_factory=lambda *_: client)

    async def handler(event):
        assert not client.acked
        return "{ack_for:req-7} accepted"

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    try:
        for _ in range(100):
            if client.acked:
                break
            await asyncio.sleep(0.01)
        assert client.acked
        assert client.sent == [
            {
                "to": "hadi-codex",
                "body": "{ack_for:req-7} accepted",
                "kind": "ack",
                "project_id": "project-1",
                "request_id": "resp-m-1",
                "in_reply_to": "m-1",
            }
        ]
        state = StateStore(tmp_path / "state.json").load()
        assert state["pending"] is None
        assert "m-1" in state["processed"]
        assert os.stat(tmp_path / "state.json").st_mode & 0o777 == 0o600
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_terminal_ack_is_consumed_without_outbound_response(tmp_path: Path) -> None:
    client = AckMupotClient()
    handled: list[str] = []
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={"poll_interval": 0.01, "state_path": str(tmp_path / "state.json")},
    )
    adapter = MupotAdapter(config, client_factory=lambda *_: client)
    async def handler(event):
        handled.append(event.text)

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    try:
        for _ in range(100):
            if client.acked:
                break
            await asyncio.sleep(0.01)
        assert client.acked
        assert handled == []
        assert client.sent == []
        state = StateStore(tmp_path / "state.json").load()
        assert state["pending"] is None
        assert "ack-1" in state["processed"]
        assert state["terminal_receipts"] == [client.message]
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_terminal_ack_domain_failure_quarantines_without_retry(
    tmp_path: Path,
) -> None:
    client = AckMupotClient(fail_ack_once=True)
    handled: list[str] = []
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={"poll_interval": 0.01, "state_path": str(tmp_path / "state.json")},
    )
    adapter = MupotAdapter(config, client_factory=lambda *_: client)
    async def handler(event):
        handled.append(event.text)

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    try:
        for _ in range(100):
            if client.ack_calls >= 1:
                break
            await asyncio.sleep(0.01)
        assert client.ack_calls == 1
        assert "ack-1" not in StateStore(tmp_path / "state.json").load().get("processed", [])
        await asyncio.sleep(0.05)
        assert client.ack_calls == 1
        assert client.acked is False
        assert getattr(adapter, "_lease_quarantined", False) is True
        assert adapter._running is False
        assert handled == []
        state = StateStore(tmp_path / "state.json").load()
        assert "ack-1" not in state["processed"]
        assert len(state["terminal_receipts"]) == 1
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_terminal_ack_retries_one_safe_before_send_failure_without_releasing(
    tmp_path: Path,
) -> None:
    client = AckMupotClient(safe_ack_once=True)
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"poll_interval": 0.01, "state_path": str(tmp_path / "state.json")},
        ),
        client_factory=lambda *_: client,
    )

    assert await adapter.connect()
    try:
        for _ in range(100):
            if client.acked:
                break
            await asyncio.sleep(0.01)
        assert client.acked is True
        assert client.ack_calls == 2
        assert client.lease_calls == 1
        assert getattr(adapter, "_lease_quarantined", False) is False
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        MupotTransportError("Mupot request failed"),
        MupotProtocolError("Mupot MCP request failed"),
    ],
)
async def test_poll_loop_quarantines_ambiguous_or_protocol_lease_without_releasing(
    tmp_path: Path,
    failure: Exception,
) -> None:
    client = LeaseFailureClient(failure)
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "poll_interval": 0.01,
                "state_path": str(tmp_path / "state.json"),
            },
        ),
        client_factory=lambda *_: client,
    )

    assert await adapter.connect()
    try:
        await asyncio.wait_for(client.first_lease.wait(), 1)
        await asyncio.sleep(0.05)
        assert client.lease_calls == 1
        assert getattr(adapter, "_lease_quarantined", False) is True
        assert adapter._running is False
        assert await adapter.connect(is_reconnect=True) is False
        assert client.lease_calls == 1
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{}, {"messages": "not-a-list"}, {"messages": [{}, {}]}])
async def test_poll_loop_quarantines_malformed_lease_result_without_releasing(
    tmp_path: Path,
    payload: object,
) -> None:
    client = LeasePayloadClient(payload)
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"poll_interval": 0.01, "state_path": str(tmp_path / "state.json")},
        ),
        client_factory=lambda *_: client,
    )

    assert await adapter.connect()
    try:
        await asyncio.wait_for(client.first_lease.wait(), 1)
        await asyncio.sleep(0.05)
        assert client.lease_calls == 1
        assert getattr(adapter, "_lease_quarantined", False) is True
        assert adapter._running is False
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_poll_loop_retries_only_classified_safe_before_send_failure(
    tmp_path: Path,
) -> None:
    from plugin.mupot_gateway import adapter as adapter_module

    safe_error = getattr(adapter_module, "MupotSafeRetryError", None)
    assert safe_error is not None
    client = LeaseFailureClient(safe_error("Mupot request failed"))
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "poll_interval": 0.01,
                "state_path": str(tmp_path / "state.json"),
            },
        ),
        client_factory=lambda *_: client,
    )

    assert await adapter.connect()
    try:
        for _ in range(100):
            if client.lease_calls >= 2:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        assert client.lease_calls == 2
        assert getattr(adapter, "_lease_quarantined", False) is True
        assert adapter._running is False
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_reconstructed_adapter_self_heals_via_connect_on_clean_tombstone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Class fix (2026-09-15, see connect()'s `self._lease_quarantined` handling).

    Before this fix, `connect()` never attempted `reconcile_inbox_polling()` on its
    own -- a `required: true` marker blocked every future `connect()` call forever,
    across every operator restart, unless a human noticed and reconciled manually
    (this is what made the live kayhermes incident, 2026-09-15, survive three
    restarts). This test (renamed from
    `test_reconstructed_adapter_stays_fenced_without_network`, which asserted the
    OLD "zero network calls" behavior this fix intentionally changes) proves the
    self-heal: a matching-scope marker whose attempt reconciles as a clean,
    unconsumed tombstone (`"cancelled"`, `ReconciliationClient`'s default
    `inbox_lease_reconcile` response, with no `pending`/`reply_outbox` residue)
    now clears automatically and `connect()` proceeds -- real network calls
    happen, and no operator action is required.
    """
    state_path = await persist_ambiguous_lease_quarantine(tmp_path, monkeypatch)
    marker = StateStore(state_path).load().get("lease_reconciliation")
    assert isinstance(marker, dict)
    assert marker["required"] is True
    assert marker["version"] == 3
    monkeypatch.setattr(time, "time", lambda: 200.0)

    client = ReconciliationClient(
        {
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 0,
            "key_matches": True,
        }
    )
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )

    try:
        assert await reconstructed.connect() is True
    finally:
        await reconstructed.disconnect()
    assert client.connect_calls >= 1
    assert client.tools[:2] == ["inbox_consumer_status", "inbox_lease_reconcile"]
    assert StateStore(state_path).load().get("lease_reconciliation") is None
    assert getattr(reconstructed, "_lease_quarantined", True) is False


@pytest.mark.asyncio
async def test_connect_reconciles_lease_marker_despite_ambiguous_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BLOCK-B (kasra re-gate round 3, 2026-09-15, head 7295e057): a restart
    carrying BOTH a stale `pending` (ambiguous -- unrelated to any staged
    reply) AND a v3 `lease_reconciliation` marker used to refuse at the
    `_legacy_pending_ambiguous` check, which ran BEFORE the
    `_lease_quarantined` auto-reconcile block in `_connect_with_active_scope`
    -- so the marker's self-heal NEVER got a chance to run, even though
    `lease_reconciliation_status()["connect_will_attempt_auto_reconcile"]`
    reported `True`. Fixed two ways: (1) the reconcile attempt now runs
    BEFORE the pending check, and (2) the pending check is re-evaluated live
    (`_compute_legacy_pending_ambiguous()`) rather than off the stale
    `__init__`-time snapshot, so it picks up
    `_reconcile_inbox_polling_with_active_scope`'s own terminal-tombstone
    branch dropping this exact unstaged `pending` as a side effect of the
    SAME reconcile call. The result: this restart now self-heals BOTH
    markers and `connect()` succeeds, in one call, with no operator action.
    """
    state_path = await persist_ambiguous_lease_quarantine(tmp_path, monkeypatch)
    store = StateStore(state_path)
    state = store.load()
    assert state.get("lease_reconciliation") is not None
    # An ambiguous `pending` unrelated to the marker's own attempt/source --
    # no reply_outbox record exists for it, so nothing here is a candidate
    # for custody the terminal-tombstone branch would need to preserve.
    state["pending"] = {"message": {"id": "stray-crash-survivor"}}
    store.save(state)

    reconstructed_status = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: ReconciliationClient(
            {"agent_id": "agent-consumer", "mode": "bearer_only", "generation": 0,
             "key_matches": True}
        ),
    )
    assert reconstructed_status._legacy_pending_ambiguous is True, (
        "constructor must still see the injected pending as ambiguous"
    )
    assert reconstructed_status._is_lease_reconcile_reachable() is True, (
        "no other gate should block the reconcile attempt from being reachable"
    )
    status = reconstructed_status.lease_reconciliation_status()
    assert status is not None
    assert status["connect_will_attempt_auto_reconcile"] is True

    client = ReconciliationClient(
        {"agent_id": "agent-consumer", "mode": "bearer_only", "generation": 0,
         "key_matches": True}
    )
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )
    try:
        assert await reconstructed.connect() is True, (
            "BLOCK-B: the lease-marker self-heal must run (and the unstaged "
            "pending must be dropped with it) despite the ambiguous pending "
            "seen at construction"
        )
    finally:
        await reconstructed.disconnect()
    final = StateStore(state_path).load()
    assert final.get("lease_reconciliation") is None
    assert final.get("pending") is None
    assert reconstructed._lease_quarantined is False
    assert reconstructed._legacy_pending_ambiguous is False


@pytest.mark.asyncio
async def test_corrupt_existing_state_fails_closed_without_network(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text("{not-valid-json", encoding="utf-8")
    client = ReconciliationClient(
        {
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 0,
            "key_matches": True,
        }
    )
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )

    assert await adapter.connect() is False
    assert client.connect_calls == 0
    assert client.tools == []


@pytest.mark.asyncio
async def test_prelease_fence_write_failure_makes_zero_lease_calls(tmp_path: Path) -> None:
    client = LeasePayloadClient(
        lambda args: fake_attempt_result(args["attempt_id"], "empty")
    )
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"poll_interval": 0.01, "state_path": str(tmp_path / "state.json")},
        ),
        client_factory=lambda *_: client,
    )

    def fail_save(_state: dict) -> None:
        raise OSError("state unavailable")

    adapter.store.save = fail_save
    assert await adapter.connect()
    try:
        await asyncio.sleep(0.05)
        assert client.lease_calls == 0
        assert adapter._running is False
        assert adapter.fatal_error_retryable is False
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_postlease_save_failure_leaves_prelease_fence_for_restart(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    client = LeasePayloadClient(
        lambda args: fake_attempt_result(args["attempt_id"], "empty")
    )
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"poll_interval": 0.01, "state_path": str(state_path)},
        ),
        client_factory=lambda *_: client,
    )
    real_save = adapter.store.save
    save_calls = 0

    def fail_after_prelease(state: dict) -> None:
        nonlocal save_calls
        save_calls += 1
        if save_calls > 1:
            raise OSError("post-lease state unavailable")
        real_save(state)

    adapter.store.save = fail_after_prelease
    assert await adapter.connect()
    try:
        await asyncio.sleep(0.05)
    finally:
        await adapter.disconnect()

    assert save_calls >= 2
    assert isinstance(StateStore(state_path).load().get("lease_reconciliation"), dict)
    reconstructed_client = ReconciliationClient({})
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: reconstructed_client,
    )
    # Class fix (2026-09-15): connect() now attempts reconcile_inbox_polling()
    # once before refusing (see the self-heal test above) -- an empty status
    # response has no matching mode/generation, so the readback mismatches
    # and this marker stays fenced exactly as before, but no longer "without
    # network": one real inbox_consumer_status readback attempt happens.
    assert await reconstructed.connect() is False
    assert reconstructed_client.connect_calls == 1
    assert reconstructed_client.tools == ["inbox_consumer_status"]
    assert reconstructed_client.lease_calls == 0


@pytest.mark.asyncio
async def test_explicit_reconciliation_before_lease_deadline_does_no_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "state.json"
    StateStore(state_path).save({
        "lease_reconciliation": {
            "version": 1,
            "required": True,
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 0,
            "reconcile_after": 106.0,
        }
    })
    monkeypatch.setattr(time, "time", lambda: 105.999)
    client = ReconciliationClient(
        {
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 0,
            "key_matches": True,
        }
    )
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )

    reconcile = getattr(reconstructed, "reconcile_inbox_polling", None)
    assert callable(reconcile)
    assert await reconcile() is False
    assert client.connect_calls == 0
    assert client.tools == []
    assert isinstance(StateStore(state_path).load().get("lease_reconciliation"), dict)


@pytest.mark.asyncio
async def test_explicit_reconciliation_clears_only_after_exact_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = await persist_ambiguous_lease_quarantine(tmp_path, monkeypatch)
    monkeypatch.setattr(time, "time", lambda: 106.0)
    client = ReconciliationClient(
        {
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 0,
            "key_matches": True,
        }
    )
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )

    reconcile = getattr(reconstructed, "reconcile_inbox_polling", None)
    assert callable(reconcile)
    assert await reconcile() is True
    assert client.connect_calls == 1
    assert client.tools == ["inbox_consumer_status", "inbox_lease_reconcile"]
    assert StateStore(state_path).load().get("lease_reconciliation") is None
    assert reconstructed._running is False
    assert reconstructed.has_fatal_error is False

    fresh = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: ReconciliationClient({}),
    )
    assert getattr(fresh, "_lease_quarantined", True) is False


@pytest.mark.asyncio
async def test_failed_reconciliation_readback_remains_durably_fenced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = await persist_ambiguous_lease_quarantine(tmp_path, monkeypatch)
    monkeypatch.setattr(time, "time", lambda: 106.0)
    client = ReconciliationClient(
        {
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 1,
            "key_matches": True,
        }
    )
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )

    reconcile = getattr(reconstructed, "reconcile_inbox_polling", None)
    assert callable(reconcile)
    assert await reconcile() is False
    assert isinstance(StateStore(state_path).load().get("lease_reconciliation"), dict)
    assert getattr(reconstructed, "_lease_quarantined", False) is True
    # Class fix (2026-09-15): connect() now attempts reconcile_inbox_polling()
    # once before refusing, so this generation mismatch is rediscovered a
    # second time (one more `inbox_consumer_status` readback) rather than
    # zero times -- the marker still stays durably fenced either way.
    assert await reconstructed.connect() is False
    assert client.tools == ["inbox_consumer_status", "inbox_consumer_status"]


@pytest.mark.asyncio
async def test_terminal_ack_receipt_persistence_failure_prevents_ack(tmp_path: Path) -> None:
    client = AckMupotClient()
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={"state_path": str(tmp_path / "state.json")},
    )
    adapter = MupotAdapter(config, client_factory=lambda *_: client)

    def fail_save(_state):
        raise OSError("receipt disk unavailable")

    adapter.store.save = fail_save
    with pytest.raises(OSError, match="receipt disk unavailable"):
        await adapter._handle_ack_envelope(client.message)
    assert client.ack_calls == 0
    assert "ack-1" not in adapter._state["processed"]


@pytest.mark.asyncio
async def test_estop_engaged_defers_ack_envelope_before_any_state_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Third path from the same class as P0-2: a peer terminal-ACK body is
    enqueue()'d verbatim into the notification outbox (attacker-reachable, per
    notifications.py's own docstring) and this function ACKs the source --
    neither goes through _deliver, so the round-1 fix never covered it either.
    Gate it the same way as _handle_routine_event: refuse before touching any
    durable state.

    re-gate #2 (2026-09-14): this must raise _EstopDeferred, not just return
    silently -- a bare return left the caller (_poll_loop / _process_leased_
    message) with no way to tell "deferred by a pause" apart from "the message
    was simply never processed", and the poll loop answered the latter with
    `_protocol_error()` -> `_quarantine_inbox_polling()`, turning a temporary
    pause into a durable, connect()-refusing quarantine. See
    test_routine_events.py's real-poll-loop tests for that end-to-end proof;
    this test only proves the narrower "no durable state was touched" property
    at the direct-call level.
    """
    fake_estop = types.SimpleNamespace(is_engaged=lambda: True)
    monkeypatch.setitem(sys.modules, "agent.estop", fake_estop)

    client = AckMupotClient()
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={"state_path": str(tmp_path / "state.json")},
    )
    adapter = MupotAdapter(config, client_factory=lambda *_: client)

    with pytest.raises(_EstopDeferred):
        await adapter._handle_ack_envelope(client.message)

    assert client.ack_calls == 0
    assert client.sent == []
    assert "ack-1" not in adapter._state.get("processed", [])
    assert adapter._state.get("terminal_receipts") in (None, [])
    assert adapter._state.get("notification_outbox", {}) == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("tenant,agent,accepted", [("right-tenant", "right-agent", True),
    ("other-tenant", "right-agent", False), ("right-tenant", "other-agent", False)])
async def test_gateway_verifies_operator_identity_before_reading_mail(tmp_path, tenant, agent, accepted):
    calls = []
    class BoundClient(FakeMupotClient):
        async def call(self, tool, arguments):
            calls.append(tool)
            if tool == "boot_context":
                return {"tenant": tenant, "bound_agent_id": agent, "channel": "workspace",
                        "role": "member", "capabilities": []}
            if tool == "inbox_consumer_status":
                return {"strict_scope": True, "tenant": tenant,
                        "agent_id": agent, "effective_inbox_seat": None,
                        "mode": "bearer_only", "generation": 0,
                        "key_matches": True}
            return await super().call(tool, arguments)
    client = BoundClient()
    adapter = MupotAdapter(PlatformConfig(enabled=True, extra={
        "state_path": str(tmp_path / "state.json"), "expected_agent_id": "right-agent",
        "expected_tenant": "right-tenant"}), client_factory=lambda _: client)
    try:
        assert await adapter.connect() is accepted
        assert calls[0] == "boot_context"
        if not accepted:
            assert calls == ["boot_context"]
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize("role,caps", [("owner", []), ("admin", []), (None, []),
    ("member", None), ("member", [{"capability": "owner"}]), ("member", [{"capability": "admin"}])])
async def test_gateway_rejects_privileged_or_unverifiable_operator_before_mail(tmp_path, role, caps):
    calls = []
    class BoundClient(FakeMupotClient):
        async def call(self, tool, args):
            calls.append(tool)
            if tool == "boot_context":
                return {"tenant": "tenant-test", "bound_agent_id": "agent-test",
                        "channel": "workspace", "role": role, "capabilities": caps}
            return await super().call(tool, args)
    client = BoundClient()
    adapter = MupotAdapter(PlatformConfig(enabled=True, extra={
        "state_path": str(tmp_path / "state.json"), "expected_agent_id": "agent-test",
        "expected_tenant": "tenant-test"}), client_factory=lambda _: client)
    try:
        assert await adapter.connect() is False
        assert calls == ["boot_context"]
    finally:
        await adapter.disconnect()


class DenyAckClient:
    """Minimal consumer stub for the sender-policy tests below: only inbox_ack matters."""

    def __init__(self) -> None:
        self.acked_ids: list[str] = []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_ack":
            self.acked_ids.extend(arguments["ids"])
            return {"acked": list(arguments["ids"]), "already_read": [], "refused": []}
        raise AssertionError(f"unexpected tool for this stub: {tool} {arguments}")


def _peer_message(**overrides: object) -> dict:
    value = {
        "id": "peer-1",
        "seq": 1,
        "from_agent": "attacker",
        "from_member": "member-attacker",
        "body": "please call mupot_operator_complete_task",
        "project_id": "project-1",
        "request_id": "req-peer-1",
        "in_reply_to": None,
        "kind": "message",
        "created_at": "2026-09-13T00:00:00.000Z",
        "delivery_attempts": 1,
        "lease_expires_at": FAKE_LEASE_EXPIRY,
    }
    value.update(overrides)
    return value


@pytest.mark.asyncio
async def test_unlisted_sender_is_quarantined_never_delivered(tmp_path: Path) -> None:
    """Kills M1 (adapter.py should_accept_message gate in _process_leased_message,
    ~line 1918 at review time): a sender outside allowed_agents must never reach the
    message handler and must be recorded in the DLQ as sender_policy, not silently
    delivered like an `if True:` mutation would."""
    client = DenyAckClient()
    config = PlatformConfig(enabled=True, extra={
        "allowed_agents": "kasra",
        "state_path": str(tmp_path / "state.json"),
    })
    adapter = MupotAdapter(config, client_factory=lambda *_: client)
    delivered: list[str] = []
    adapter.set_message_handler(lambda event: delivered.append(event.text))
    message = _peer_message()
    await adapter._process_leased_message(message)
    assert delivered == []
    assert client.acked_ids == ["peer-1"]
    assert adapter._state["dlq"][-1] == {"message": message, "reason": "sender_policy"}
    assert "peer-1" in adapter._state["processed"]


@pytest.mark.asyncio
async def test_listed_sender_is_still_delivered(tmp_path: Path) -> None:
    """Sanity control for the M1 test above: the same pipeline with an allowed
    sender does reach the handler, proving the refusal above is about the sender
    check and not some unrelated failure."""
    client = FakeMupotClient()
    config = PlatformConfig(enabled=True, extra={
        "allowed_agents": "hadi-codex",
        "state_path": str(tmp_path / "state.json"),
    })
    adapter = MupotAdapter(config, client_factory=lambda *_: client)
    delivered: list[str] = []

    async def handler(event):
        delivered.append(event.text)
        return "{ack_for:req-7} accepted"

    adapter.set_message_handler(handler)
    await adapter._process_leased_message(client.message)
    assert delivered == ["full Mupot answer"]


@pytest.mark.parametrize("empty_value", ["", []])
def test_explicit_empty_allowed_agents_denies_everyone(tmp_path: Path, empty_value) -> None:
    """Kills the P1-1 fail-open default: `extra.get("allowed_agents") or DEFAULT`
    treated an explicitly configured empty allowlist the same as an absent key,
    silently falling back to the trust-everyone default. An explicit "" or [] must
    mean deny-all."""
    config = PlatformConfig(enabled=True, extra={
        "allowed_agents": empty_value,
        "state_path": str(tmp_path / "state.json"),
    })
    adapter = MupotAdapter(config, client_factory=lambda *_: FakeMupotClient())
    assert adapter.allowed_agents == set()


def test_absent_allowed_agents_key_falls_back_to_the_documented_default(tmp_path: Path) -> None:
    """The default roster is still honored when the key is genuinely not configured
    (as opposed to configured empty) -- distinguishing these two is the whole fix."""
    config = PlatformConfig(enabled=True, extra={
        "state_path": str(tmp_path / "state.json"),
    })
    adapter = MupotAdapter(config, client_factory=lambda *_: FakeMupotClient())
    assert adapter.allowed_agents == {"hadi-codex", "hadi-codex-cli", "kasra", "hermes"}


def test_allow_from_is_not_derived_on_the_internal_true_path(tmp_path: Path) -> None:
    """CORRECTED CLAIM (kasra-review re-gate, 2026-09-14): the removed
    `extra["allow_from"] = sorted(self.allowed_agents)` line implied Hermes
    consults a second copy of this allowlist. The prior version of this test
    (and its docstring/comment) asserted a FALSE global negative: "allow_from"
    IS read at the pinned rev, at gateway/authz_mixin.py's
    _adapter_extra_allowlist_authorizes, and handled by gateway/config_loader.py
    and gateway/pairing.py. The reason the removal is still correct is narrower
    and SCOPED to this adapter's own call path: that reader lives on Hermes's
    non-internal auth branch, and build_mupot_event always sets internal=True,
    which skips that branch entirely today (gateway/run_inbound.py:174). If
    internal=True is ever dropped from build_mupot_event, allow_from must be
    revisited -- this test only guards against the dead line silently coming
    back, it does not (and must not be read to) claim Hermes never consults
    "allow_from" anywhere in the tree."""
    config = PlatformConfig(enabled=True, extra={
        "allowed_agents": "kasra",
        "state_path": str(tmp_path / "state.json"),
    })
    MupotAdapter(config, client_factory=lambda *_: FakeMupotClient())
    assert "allow_from" not in (config.extra or {})


@pytest.mark.parametrize("bad_value", [0, 1, True, False, 3.5, {"a": "b"}])
def test_allowed_agents_non_iterable_value_raises_clear_config_error(
    tmp_path: Path, bad_value: object,
) -> None:
    """A non-string, non-list allowed_agents (int/bool/float/dict) previously fell
    straight into `for item in allowed` and raised a raw, unhelpful TypeError.
    Match the style already used for native_gateway_enabled/routine_events_enabled:
    a clear config-error message naming the offending field."""
    config = PlatformConfig(enabled=True, extra={
        "allowed_agents": bad_value,
        "state_path": str(tmp_path / "state.json"),
    })
    with pytest.raises(ValueError, match="allowed_agents must be"):
        MupotAdapter(config, client_factory=lambda *_: FakeMupotClient())


@pytest.mark.parametrize("bad_entry", [123, True, False, 3.5, None, {"a": "b"}, ["x"]])
def test_allowed_agents_non_string_list_entry_raises_clear_config_error(
    tmp_path: Path, bad_entry: object,
) -> None:
    """P3 (kasra-review re-gate #2, 2026-09-14): a non-string entry inside an
    otherwise-valid list (e.g. [123, "kasra"]) previously reached
    normalize_agent()'s `str(value or "")`, which silently stringifies it
    into a plausible-looking agent name instead of failing the
    misconfiguration loudly -- same class as the non-iterable top-level
    check above, one level deeper."""
    config = PlatformConfig(enabled=True, extra={
        "allowed_agents": ["kasra", bad_entry],
        "state_path": str(tmp_path / "state.json"),
    })
    with pytest.raises(ValueError, match="allowed_agents entries must be strings"):
        MupotAdapter(config, client_factory=lambda *_: FakeMupotClient())


@pytest.mark.asyncio
async def test_estop_engaged_blocks_dispatch_before_any_state_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kills the P0-2 e-stop bypass: build_mupot_event always sets internal=True, which
    at the pinned Hermes rev skips gateway/run_inbound.py's own e-stop gate entirely
    (:174 returns before :233). This adapter must enforce the same property itself in
    _deliver, since Hermes's own gate never runs for this call path.

    re-gate #2 (2026-09-14): _deliver must raise _EstopDeferred, not just return
    silently, so the poll loop can tell "deferred by a pause" apart from a genuine
    protocol violation (see test_routine_events.py's real-poll-loop tests for the
    end-to-end proof that a bare return previously turned every pause into a
    durable inbox quarantine)."""
    from plugin.mupot_gateway import adapter as adapter_module

    fake_estop = types.SimpleNamespace(is_engaged=lambda: True)
    monkeypatch.setitem(sys.modules, "agent.estop", fake_estop)

    client = FakeMupotClient()
    config = PlatformConfig(enabled=True, extra={
        "allowed_agents": "hadi-codex",
        "state_path": str(tmp_path / "state.json"),
    })
    adapter = MupotAdapter(config, client_factory=lambda *_: client)
    handled: list[str] = []
    adapter.set_message_handler(lambda event: handled.append(event.text))

    with pytest.raises(adapter_module._EstopDeferred):
        await adapter._deliver(dict(client.message))

    assert handled == []
    assert client.sent == []
    assert client.acked is False
    assert adapter._state["pending"] is None


@pytest.mark.asyncio
async def test_estop_not_engaged_dispatches_normally(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control for the test above: with the (faked) e-stop module reporting not-engaged,
    delivery proceeds as normal -- proves the block above is really about estop state."""
    fake_estop = types.SimpleNamespace(is_engaged=lambda: False)
    monkeypatch.setitem(sys.modules, "agent.estop", fake_estop)

    client = FakeMupotClient()
    config = PlatformConfig(enabled=True, extra={
        "allowed_agents": "hadi-codex",
        "state_path": str(tmp_path / "state.json"),
    })
    adapter = MupotAdapter(config, client_factory=lambda *_: client)
    handled: list[str] = []

    async def handler(event):
        handled.append(event.text)
        return "{ack_for:req-7} accepted"

    adapter.set_message_handler(handler)
    await adapter._deliver(dict(client.message))
    assert handled == ["full Mupot answer"]


class PausableFakeMupotClient(FakeMupotClient):
    """Like FakeMupotClient, but fires ``on_first_lease`` the moment
    inbox_lease hands back the message for the first time -- simulates an
    e-stop engaging during the inbox_lease round trip, i.e. AFTER the poll
    loop's own pre-lease pause check already passed but BEFORE the leased
    message is actually handled."""

    def __init__(self, *, on_first_lease=None) -> None:
        super().__init__()
        self.on_first_lease = on_first_lease
        self._fired_first_lease = False

    async def call(self, tool: str, arguments: dict) -> dict:
        result = await super().call(tool, arguments)
        if (
            tool == "inbox_lease"
            and not self._fired_first_lease
            and result.get("messages")
        ):
            self._fired_first_lease = True
            if self.on_first_lease is not None:
                self.on_first_lease()
        return result


@pytest.mark.asyncio
async def test_real_poll_loop_pauses_before_lease_then_resumes_deliver(
    tmp_path: Path,
) -> None:
    """P0 (kasra-review re-gate #2, 2026-09-14), proven through the REAL
    `_poll_loop` background task for the peer `_deliver` path specifically
    (the routine-event equivalent lives in test_routine_events.py). Engages
    the REAL agent/estop.py sentinel BEFORE `connect()`, lets the real
    background poll task run for several ticks, and asserts zero
    `inbox_lease` calls, the poll task still alive, `_lease_quarantined`
    still False, and `state.json` carrying no `lease_reconciliation` key --
    then disengages and confirms the SAME still-running poll task delivers
    the message exactly once."""
    import hermes_constants
    from agent import estop as real_estop

    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    token = hermes_constants.set_hermes_home_override(str(hermes_home))
    try:
        real_estop.engage(reason="kasra-review-regate2-poll-loop-deliver-test")
        assert real_estop.is_engaged() is True

        client = PausableFakeMupotClient()
        config = PlatformConfig(
            enabled=True,
            typing_indicator=False,
            extra={
                "allowed_agents": "hadi-codex",
                "poll_interval": 0.01,
                "state_path": str(tmp_path / "state.json"),
            },
        )
        adapter = MupotAdapter(config, client_factory=lambda *_: client)
        handled: list[str] = []

        async def handler(event):
            handled.append(event.text)
            return "{ack_for:req-7} accepted"

        adapter.set_message_handler(handler)

        assert await adapter.connect()
        try:
            for _ in range(20):
                await asyncio.sleep(0.01)

            assert client.lease_calls == 0, (
                f"inbox_lease must never be attempted while paused, got "
                f"{client.lease_calls} calls"
            )
            assert handled == []
            assert client.sent == []
            assert client.acked is False
            assert adapter._poll_task is not None
            assert not adapter._poll_task.done()
            assert adapter._lease_quarantined is False
            state = StateStore(tmp_path / "state.json").load()
            assert "lease_reconciliation" not in state
            assert state.get("processed", []) == []

            real_estop.disengage()
            assert real_estop.is_engaged() is False

            for _ in range(200):
                if client.acked:
                    break
                await asyncio.sleep(0.01)
            assert client.acked
            assert handled == ["full Mupot answer"]
            state = StateStore(tmp_path / "state.json").load()
            assert "m-1" in state["processed"]
            assert not adapter._poll_task.done()
        finally:
            await adapter.disconnect()
    finally:
        real_estop.disengage()
        hermes_constants.reset_hermes_home_override(token)


@pytest.mark.asyncio
async def test_real_poll_loop_defers_mid_message_deliver_then_resumes_without_quarantine(
    tmp_path: Path,
) -> None:
    """Mid-message half of the same P0 class, for `_deliver`: the e-stop
    engages in the narrow window AFTER the poll loop's pre-lease check passes
    but BEFORE the leased message is handled. This must release the lease (no
    `inbox_lease_ack`, no `_protocol_error()`, no
    `_quarantine_inbox_polling()`) and let the SAME still-unacked message be
    leased and delivered again once resumed, exactly once."""
    import hermes_constants
    from agent import estop as real_estop

    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    token = hermes_constants.set_hermes_home_override(str(hermes_home))
    try:
        assert real_estop.is_engaged() is False

        client = PausableFakeMupotClient(
            on_first_lease=lambda: real_estop.engage(
                reason="kasra-review-regate2-mid-message-deliver-test"
            ),
        )
        config = PlatformConfig(
            enabled=True,
            typing_indicator=False,
            extra={
                "allowed_agents": "hadi-codex",
                "poll_interval": 0.01,
                "state_path": str(tmp_path / "state.json"),
            },
        )
        adapter = MupotAdapter(config, client_factory=lambda *_: client)
        handled: list[str] = []

        async def handler(event):
            handled.append(event.text)
            return "{ack_for:req-7} accepted"

        adapter.set_message_handler(handler)

        assert await adapter.connect()
        try:
            for _ in range(200):
                if client.lease_calls:
                    break
                await asyncio.sleep(0.01)
            for _ in range(20):
                await asyncio.sleep(0.01)

            assert real_estop.is_engaged() is True
            assert handled == []
            assert client.sent == []
            assert client.acked is False
            assert adapter._poll_task is not None
            assert not adapter._poll_task.done()
            assert adapter._lease_quarantined is False
            state = StateStore(tmp_path / "state.json").load()
            assert "lease_reconciliation" not in state
            assert state.get("processed", []) == []

            real_estop.disengage()
            assert real_estop.is_engaged() is False

            for _ in range(200):
                if client.acked:
                    break
                await asyncio.sleep(0.01)
            assert client.acked
            assert handled == ["full Mupot answer"]
            state = StateStore(tmp_path / "state.json").load()
            assert "m-1" in state["processed"]
        finally:
            await adapter.disconnect()
    finally:
        real_estop.disengage()
        hermes_constants.reset_hermes_home_override(token)


def test_estop_engaged_fails_open_to_false_when_hermes_estop_is_unimportable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """P1 residual (kasra-review re-gate, 2026-09-14): this test used to
    `monkeypatch.delitem(sys.modules, "agent.estop")` and assert the result was
    False. But this test file runs in the NATIVE suite, where `agent.estop`
    genuinely exists on sys.path -- deleting the cached module only forces a
    successful RE-import, so the test passed for the wrong reason. Proven:
    replacing `except ImportError: return False` with `raise AssertionError`
    left the whole 276-test native suite green, i.e. the fail-open branch had
    zero real coverage.

    Fix: force a REAL ImportError via the import machinery itself
    (builtins.__import__), which is the only honest way to exercise this
    except-branch from inside an environment where the module actually is
    importable. Also assert the WARNING log line fires exactly once (the
    fail-open must never be silent), reset via the module's own log-once flag
    so this test is independent of whatever ran before it.
    """
    import builtins

    from plugin.mupot_gateway import adapter as adapter_module

    monkeypatch.setattr(adapter_module, "_ESTOP_IMPORT_WARNED", False)
    real_import = builtins.__import__

    def blocking_import(name, *args, **kwargs):
        if name == "agent.estop":
            raise ImportError("agent.estop forced unavailable for this test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocking_import)

    with caplog.at_level(logging.WARNING, logger="plugin.mupot_gateway.adapter"):
        assert adapter_module._estop_engaged() is False
        # A second call must not spam a second warning (log once per process).
        assert adapter_module._estop_engaged() is False

    warnings = [
        record for record in caplog.records
        if "agent.estop is not importable" in record.getMessage()
    ]
    assert len(warnings) == 1


@pytest.mark.asyncio
async def test_stranded_notifications_are_logged_at_startup_and_surfaced_by_status_tool(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    """P2 inspector: activation_unknown/transport_unknown notices are a 'preserved for
    inspection' terminal state that nothing used to look at. Prove both halves: a
    startup warning log with the count, and a queryable surface (the adapter method the
    mupot_gateway_status tool reads)."""
    state_path = tmp_path / "state.json"
    StateStore(state_path).save({
        "notification_outbox": {
            "stranded-1": {"status": "activation_unknown", "last_error": "ActivationOutcomeUnknown"},
            "stranded-2": {"status": "transport_unknown", "last_error": "DeliveryUnknown"},
            "healthy-1": {"status": "delivered"},
        },
    })
    config = PlatformConfig(enabled=True, extra={"state_path": str(state_path)})

    with caplog.at_level(logging.WARNING, logger="plugin.mupot_gateway.adapter"):
        adapter = MupotAdapter(config, client_factory=lambda *_: FakeMupotClient())

    stranded = adapter.stranded_notifications()
    assert {item["source_id"] for item in stranded} == {"stranded-1", "stranded-2"}
    assert any(
        "2 notification" in record.getMessage() and "stranded-1" in record.getMessage()
        for record in caplog.records
    )


def test_gateway_status_tool_reports_stranded_notifications(tmp_path: Path) -> None:
    from plugin.mupot_gateway.adapter import register as register_native_gateway

    state_path = tmp_path / "state.json"
    StateStore(state_path).save({
        "notification_outbox": {
            "stranded-1": {"status": "activation_unknown"},
        },
    })

    tools: dict[str, object] = {}

    class Ctx:
        def inject_message(self, *_a, **_kw):
            return True

        def register_platform(self, **kwargs):
            self.adapter_factory = kwargs["adapter_factory"]

        def register_tool(self, **kwargs):
            tools[kwargs["name"]] = kwargs["handler"]

    ctx = Ctx()
    register_native_gateway(ctx)
    assert "mupot_gateway_status" in tools
    # Before the platform ever connects there is no live adapter yet.
    before = json.loads(tools["mupot_gateway_status"]({}))
    assert before == {"ok": False, "error": "native_gateway_not_connected"}

    ctx.adapter_factory(PlatformConfig(enabled=True, extra={"state_path": str(state_path)}))
    after = json.loads(tools["mupot_gateway_status"]({}))
    assert after["ok"] is True
    assert after["stranded_notifications"] == [{
        "source_id": "stranded-1",
        "status": "activation_unknown",
        "activation_status": None,
        "delivery_status": None,
        "last_error": None,
    }]


@pytest.mark.asyncio
async def test_gateway_status_clears_to_disconnected_after_adapter_disconnect(
    tmp_path: Path,
) -> None:
    """P2 hygiene item from the kasra-review re-gate (2026-09-14): _live_adapter
    was never cleared on disconnect/close, so mupot_gateway_status could go on
    reporting a DEAD adapter's stale local state (stranded_notifications etc)
    as if the native gateway were still connected. register()'s adapter_factory
    must wrap disconnect() so the status tool's live-instance lookup reverts to
    native_gateway_not_connected once Hermes tears the platform down."""
    from plugin.mupot_gateway.adapter import register as register_native_gateway

    tools: dict[str, object] = {}

    class Ctx:
        def inject_message(self, *_a, **_kw):
            return True

        def register_platform(self, **kwargs):
            self.adapter_factory = kwargs["adapter_factory"]

        def register_tool(self, **kwargs):
            tools[kwargs["name"]] = kwargs["handler"]

    ctx = Ctx()
    register_native_gateway(ctx)
    instance = ctx.adapter_factory(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")})
    )
    # Swap in a client whose close() is a real no-op coroutine so disconnect()
    # can run to completion without a live MCP connection.
    instance._client = FakeMupotClient()
    instance._send_client = instance._client

    connected = json.loads(tools["mupot_gateway_status"]({}))
    assert connected["ok"] is True

    await instance.disconnect()

    disconnected = json.loads(tools["mupot_gateway_status"]({}))
    assert disconnected == {"ok": False, "error": "native_gateway_not_connected"}


def test_gateway_status_survives_real_hermes_registry_dispatch(tmp_path: Path) -> None:
    """Live-defect regression (kayhermes gateway journal, 2026-09-14, plugin 0.4.0):
    ``TypeError: register.<locals>.gateway_status() got an unexpected keyword argument
    'task_id'``. The two tests above call the captured handler directly with a bare
    ``{}`` — that can never reproduce this, because a *real* Hermes turn never calls a
    tool handler that way. ``tools/registry.py``'s ``dispatch()`` always calls
    ``entry.handler(args, **kwargs)`` with ``task_id``/``session_id``/``user_task``
    (``model_tools.py``'s ``_execute_tool`` builds those into ``dispatch_kwargs`` for
    every tool call), so this routes through the actual registry the way a live gateway
    turn does."""
    from plugin.mupot_gateway.adapter import register as register_native_gateway
    from tools.registry import registry

    tool_name = "mupot_gateway_status"

    class Ctx:
        def inject_message(self, *_a, **_kw):
            return True

        def register_platform(self, **kwargs):
            self.adapter_factory = kwargs["adapter_factory"]

        def register_tool(self, **kwargs):
            registry.register(
                name=kwargs["name"], toolset=kwargs["toolset"],
                schema=kwargs["schema"], handler=kwargs["handler"],
            )

    registry.deregister(tool_name)  # no-op if nothing is registered yet
    try:
        ctx = Ctx()
        register_native_gateway(ctx)
        ctx.adapter_factory(
            PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")})
        )

        raw = registry.dispatch(
            tool_name, {}, task_id="task-native-1", session_id="session-native-1",
            user_task="probe mupot_gateway_status",
        )
        result = json.loads(raw)
        # `connected`/`lease_reconciliation` added 2026-09-15 (see
        # lease_reconciliation_status()) so an operator can see a durable
        # quarantine here instead of only in state.json. `reconciling`/
        # `turn_failure_dlq` added round 2, 2026-09-15 (items 6/2).
        assert result == {
            "ok": True,
            "connected": False,
            "reconciling": False,
            "lease_reconciliation": None,
            "stranded_notifications": [],
            "turn_failure_dlq": [],
        }
    finally:
        registry.deregister(tool_name)
