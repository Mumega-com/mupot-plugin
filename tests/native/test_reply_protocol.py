"""Hostile terminal-reply durability and replay contract tests."""

from __future__ import annotations

import asyncio
import copy
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from gateway.config import PlatformConfig

from plugin.mupot_gateway.adapter import (
    MupotAdapter,
    MupotProtocolError,
    MupotTransportError,
    StateStore,
    _DeliveryDeferred,
)


class SimulatedCrash(BaseException):
    """Stop execution at a crash boundary that ordinary recovery must not swallow."""


class ScopeOwner:
    def __init__(self, fingerprint: str) -> None:
        self.fingerprint = fingerprint

    def validated_fingerprint(self) -> str:
        return self.fingerprint

    @contextmanager
    def activate(self):
        yield


ATTEMPT_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
ATTEMPT_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
STRICT_SCOPE = {
    "tenant": "tenant-test",
    "agent_id": "receiver",
    "effective_inbox_seat": "native",
    "mode": "bearer_only",
    "generation": 0,
}


class ProtocolClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.messages_by_key: dict[str, dict[str, Any]] = {}
        self.acked_ids: list[str] = []
        self.fail_after_store_once = False
        self.crash_before_store_once = False
        self.malformed_receipt = False
        self.permanent_send_failure = False

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((tool, copy.deepcopy(arguments)))
        if tool == "inbox_consumer_status":
            return {
                "strict_scope": True,
                "tenant": "tenant-test",
                "agent_id": "receiver",
                "effective_inbox_seat": "native",
                "mode": "bearer_only",
                "generation": 0,
                "key_matches": True,
            }
        if tool == "inbox_lease":
            return {
                "tenant": "tenant-test",
                "agent_id": "receiver",
                "effective_inbox_seat": "native",
                "attempt_id": arguments["attempt_id"],
                "state": "empty",
                "lease_expires_at": None,
                "messages": [],
                "consumed": False,
            }
        if tool == "inbox_ack":
            message_id = arguments["ids"][0]
            self.acked_ids.append(message_id)
            return {"acked": [message_id], "already_read": [], "refused": []}
        if tool != "send":
            raise AssertionError(f"unexpected tool: {tool}")

        if self.crash_before_store_once:
            self.crash_before_store_once = False
            raise SimulatedCrash("process stopped before transport completion")
        if self.permanent_send_failure:
            raise MupotProtocolError("Mupot MCP request failed")

        key = arguments["request_id"]
        existing = self.messages_by_key.get(key)
        duplicate = existing is not None
        if existing is not None and existing != arguments:
            raise MupotProtocolError("Mupot MCP request failed")
        self.messages_by_key.setdefault(key, copy.deepcopy(arguments))
        if self.fail_after_store_once:
            self.fail_after_store_once = False
            raise MupotTransportError("Mupot request failed")
        if self.malformed_receipt:
            return {"id": ""}
        return {
            "id": f"delivery-{len(self.messages_by_key)}",
            "seq": len(self.messages_by_key),
            "duplicate": duplicate,
            "to": arguments["to"],
            "project_id": arguments.get("project_id"),
            "target_seat": None,
        }


class AttemptReplayClient(ProtocolClient):
    def __init__(
        self,
        *,
        attempt_state: str,
        consumed: bool,
        status_scope: dict[str, Any] | None = None,
        status_error: Exception | None = None,
        before_attempt_ack: Any = None,
    ) -> None:
        super().__init__()
        self.attempt_state = attempt_state
        self.consumed = consumed
        self.status_scope = status_scope or STRICT_SCOPE
        self.status_error = status_error
        self.before_attempt_ack = before_attempt_ack
        self.attempts = {ATTEMPT_A: attempt_state, ATTEMPT_B: "leased"}
        self.message_read = {ATTEMPT_B: False}

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if tool == "inbox_consumer_status":
            self.calls.append((tool, copy.deepcopy(arguments)))
            if self.status_error is not None:
                raise self.status_error
            return {"strict_scope": True, "key_matches": True, **self.status_scope}
        if tool == "inbox_lease_ack":
            self.calls.append((tool, copy.deepcopy(arguments)))
            assert arguments == {"attempt_id": ATTEMPT_A}
            if self.before_attempt_ack is not None:
                self.before_attempt_ack()
            return {
                "tenant": "tenant-test",
                "agent_id": "receiver",
                "effective_inbox_seat": "native",
                "attempt_id": ATTEMPT_A,
                "state": self.attempt_state,
                "consumed": self.consumed,
            }
        if tool == "inbox_ack":
            self.attempts[ATTEMPT_B] = "acked"
            self.message_read[ATTEMPT_B] = True
        return await super().call(tool, arguments)


def source_message(source_id: str = "source-1") -> dict[str, Any]:
    return {
        "id": source_id,
        "seq": 7,
        "tenant": "tenant-test",
        "to_agent": "receiver",
        "target_seat": "native",
        "from_agent": "sender",
        "from_member": "member-sender",
        "kind": "request",
        "body": "Please complete the task.",
        "request_id": "request-1",
        "in_reply_to": None,
        "created_at": "2026-09-13T10:00:00.000Z",
        "project_id": "project-1",
        "fenced_delivery_id": "lease-1",
        "body_length": 25,
        "checksum_sha256": "a" * 64,
        "is_intact": True,
        "expects_reply": True,
        "reply_basis": "request_id_field",
        "delivery_attempts": 1,
        "lease_expires_at": "2099-09-13T10:05:00.000Z",
    }


def adapter_at(
    tmp_path: Path,
    client: ProtocolClient,
    owner: ScopeOwner | None = None,
) -> MupotAdapter:
    return MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "sender,receiver",
                "poll_interval": 0.01,
                "state_path": str(tmp_path / "state.json"),
                "notification_recipients": {"telegram": "owner"},
            },
        ),
        client_factory=lambda _server: client,
        secret_owner=owner,  # type: ignore[arg-type]
    )


async def bind_delivery(
    adapter: MupotAdapter,
    message: dict[str, Any] | None = None,
) -> None:
    event, _runtime = adapter._begin_delivery(message or source_message())
    await adapter.on_processing_start(event)


async def bind_attempt_delivery(
    adapter: MupotAdapter,
    message: dict[str, Any] | None = None,
) -> None:
    source = message or source_message()
    adapter._consumer_fence = copy.deepcopy(STRICT_SCOPE)
    adapter._state["lease_reconciliation"] = {
        "version": 3,
        "required": True,
        **STRICT_SCOPE,
        "profile_owner_fingerprint": adapter._profile_owner_fingerprint,
        "attempt_id": ATTEMPT_A,
    }
    adapter._state["pending"] = {"message": copy.deepcopy(source)}
    adapter.store.save(adapter._state)
    event, _runtime = adapter._begin_delivery(source, attempt_id=ATTEMPT_A)
    await adapter.on_processing_start(event)


async def wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"condition not met within {timeout}s")


def clear_lease_marker_for_replay(path: Path) -> None:
    state_path = path / "state.json"
    state = StateStore(state_path).load()
    state.pop("lease_reconciliation", None)
    StateStore(state_path).save(state)


async def persist_prepared_attempt(
    path: Path,
    owner: ScopeOwner | None = None,
) -> dict[str, Any]:
    client = ProtocolClient()
    client.crash_before_store_once = True
    adapter = adapter_at(path, client, owner)
    await bind_attempt_delivery(adapter)
    with pytest.raises(SimulatedCrash):
        await adapter.send("sender", "Prepared exact final.")
    clear_lease_marker_for_replay(path)
    return copy.deepcopy(StateStore(path / "state.json").load())


@pytest.mark.asyncio
async def test_progress_and_final_use_distinct_terminal_ack_keys_and_one_notice(
    tmp_path: Path,
) -> None:
    """Reusing the final key for progress makes the real final conflict and disappear."""
    client = ProtocolClient()
    adapter = adapter_at(tmp_path, client)
    await bind_delivery(adapter)

    first = await adapter.send(
        "sender", "still working", metadata={"_interim_send": True}
    )
    replay = await adapter.send(
        "sender", "still working", metadata={"_interim_send": True}
    )
    final = await adapter.send("sender", "Final answer.")

    assert first.success and replay.success and final.success
    progress_key = (
        "prog-f76248e305c7c3ff97ee68020794226ba"
        "3417eb42e0c0e35c7cd8c255e8c272d"
    )
    assert list(client.messages_by_key) == [progress_key, "resp-source-1"]
    assert client.messages_by_key[progress_key]["kind"] == "ack"
    assert client.messages_by_key["resp-source-1"] == {
        "to": "sender",
        "body": "Final answer.",
        "kind": "ack",
        "project_id": "project-1",
        "request_id": "resp-source-1",
        "in_reply_to": "source-1",
    }
    state = StateStore(tmp_path / "state.json").load()
    assert list(state["notification_outbox"]) == ["source-1"]
    assert state["reply_outbox"]["source-1"]["arguments"]["body"] == "Final answer."


@pytest.mark.asyncio
async def test_lost_final_receipt_retry_reuses_immutable_envelope_and_stores_once(
    tmp_path: Path,
) -> None:
    """An ambiguous response plus formatting fallback must not mint a second reply."""
    client = ProtocolClient()
    client.fail_after_store_once = True
    adapter = adapter_at(tmp_path, client)
    await bind_delivery(adapter)

    first = await adapter.send("sender", "Original exact final.")
    retry = await adapter.send("sender", "Fallback reformatted final.")

    assert first.success is False
    assert retry.success is True
    assert list(client.messages_by_key) == ["resp-source-1"]
    send_calls = [args for tool, args in client.calls if tool == "send"]
    assert len(send_calls) == 2
    assert send_calls[0] == send_calls[1]
    assert send_calls[1]["body"] == "Original exact final."
    persisted = StateStore(tmp_path / "state.json").load()
    assert persisted["reply_outbox"]["source-1"]["arguments"] == send_calls[0]


@pytest.mark.asyncio
async def test_restart_replays_prepared_final_without_another_model_turn(
    tmp_path: Path,
) -> None:
    """A crash after preparation must replay bytes from disk before leasing new work."""
    first_client = ProtocolClient()
    first_client.crash_before_store_once = True
    first = adapter_at(tmp_path, first_client)
    message = source_message()
    await bind_attempt_delivery(first, message)

    with pytest.raises(SimulatedCrash):
        await first.send("sender", "Exact prepared final.")
    prepared = copy.deepcopy(StateStore(tmp_path / "state.json").load())
    assert prepared["reply_outbox"]["source-1"]["ack_ownership"]["attempt_id"] == (
        ATTEMPT_A
    )
    clear_lease_marker_for_replay(tmp_path)

    replay_client = AttemptReplayClient(attempt_state="acked", consumed=True)
    restarted = adapter_at(tmp_path, replay_client)
    model_turns: list[str] = []

    async def model(event: Any) -> str:
        model_turns.append(event.message_id)
        return "regenerated output"

    restarted.set_message_handler(model)
    await restarted._replay_reply_outbox()

    assert model_turns == []
    sent = [args for tool, args in replay_client.calls if tool == "send"]
    assert sent == [prepared["reply_outbox"]["source-1"]["arguments"]]
    assert sent[0]["body"] == "Exact prepared final."


@pytest.mark.asyncio
async def test_preownership_pending_reply_outbox_stays_fenced_without_network(
    tmp_path: Path,
) -> None:
    client = ProtocolClient()
    adapter = adapter_at(tmp_path, client)
    message = source_message()
    adapter._state["pending"] = {"message": copy.deepcopy(message)}
    adapter.store.save(adapter._state)
    await bind_delivery(adapter, message)
    assert (await adapter.send("sender", "Legacy ambiguous final.")).success is True
    state = StateStore(tmp_path / "state.json").load()
    record = state["reply_outbox"]["source-1"]
    record["version"] = 1
    record.pop("ack_ownership")
    StateStore(tmp_path / "state.json").save(state)

    restarted_client = ProtocolClient()
    restarted = adapter_at(tmp_path, restarted_client)
    assert await restarted.connect() is False
    with pytest.raises(RuntimeError, match="Mupot MCP request failed"):
        await restarted._replay_reply_outbox()

    assert restarted_client.calls == []
    durable = StateStore(tmp_path / "state.json").load()
    assert durable["reply_outbox"]["source-1"] == record
    assert "source-1" not in durable.get("processed", [])


@pytest.mark.asyncio
async def test_prepared_explicit_legacy_reply_stays_fenced_without_send(
    tmp_path: Path,
) -> None:
    first_client = ProtocolClient()
    first_client.crash_before_store_once = True
    first = adapter_at(tmp_path, first_client)
    message = source_message()
    first._state["pending"] = {"message": copy.deepcopy(message)}
    first.store.save(first._state)
    await bind_delivery(first, message)
    with pytest.raises(SimulatedCrash):
        await first.send("sender", "Prepared legacy final.")
    before = copy.deepcopy(StateStore(tmp_path / "state.json").load())
    assert before["reply_outbox"]["source-1"]["ack_ownership"] == {
        "version": 1,
        "kind": "legacy_non_attempt",
    }

    client = ProtocolClient()
    restarted = adapter_at(tmp_path, client)
    with pytest.raises(RuntimeError, match="Mupot MCP request failed"):
        await restarted._replay_reply_outbox()

    assert client.calls == []
    assert restarted._lease_quarantined is True
    assert StateStore(tmp_path / "state.json").load() == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope_update", "status_error", "owner_fingerprint"),
    [
        ({"tenant": "other-tenant"}, None, None),
        ({"agent_id": "other-agent"}, None, None),
        ({"effective_inbox_seat": "other-seat"}, None, None),
        ({"mode": "gateway"}, None, None),
        ({"generation": 1}, None, None),
        ({"key_matches": False}, None, None),
        ({}, MupotTransportError("Mupot request failed"), None),
        ({}, None, "b" * 64),
    ],
)
async def test_prepared_attempt_replay_preflight_failure_sends_nothing_and_is_unchanged(
    tmp_path: Path,
    scope_update: dict[str, Any],
    status_error: Exception | None,
    owner_fingerprint: str | None,
) -> None:
    original_owner = ScopeOwner("a" * 64)
    before = await persist_prepared_attempt(tmp_path, original_owner)
    client = AttemptReplayClient(
        attempt_state="acked",
        consumed=True,
        status_scope={**STRICT_SCOPE, **scope_update},
        status_error=status_error,
    )
    current_owner = ScopeOwner(owner_fingerprint or original_owner.fingerprint)
    restarted = adapter_at(tmp_path, client, current_owner)

    with pytest.raises(RuntimeError):
        await restarted._replay_reply_outbox()

    assert not any(
        tool in {"send", "inbox_lease_reconcile", "inbox_lease_ack", "inbox_ack"}
        for tool, _arguments in client.calls
    )
    if owner_fingerprint is not None:
        assert client.calls == []
    else:
        assert client.calls == [("inbox_consumer_status", {"strict_scope": True})]
    assert restarted._lease_quarantined is True
    assert restarted._fatal_error_code == "mupot_inbox_replay_preflight_required"
    assert StateStore(tmp_path / "state.json").load() == before


@pytest.mark.asyncio
async def test_prepared_attempt_valid_preflight_sends_then_custodies_then_acks_once(
    tmp_path: Path,
) -> None:
    before = await persist_prepared_attempt(tmp_path)
    assert before["reply_outbox"]["source-1"]["status"] == "prepared"
    ack_order: list[str] = []

    def assert_custody_before_ack() -> None:
        state = StateStore(tmp_path / "state.json").load()
        assert state["reply_outbox"]["source-1"]["status"] == "custodied"
        assert state["notification_outbox"]["source-1"]["custody_status"] == "durable"
        ack_order.append("ack")

    client = AttemptReplayClient(
        attempt_state="acked",
        consumed=True,
        before_attempt_ack=assert_custody_before_ack,
    )
    restarted = adapter_at(tmp_path, client)

    await restarted._replay_reply_outbox()
    await restarted._replay_reply_outbox()

    tools = [tool for tool, _arguments in client.calls]
    assert tools == [
        "inbox_consumer_status",
        "send",
        "inbox_consumer_status",
        "inbox_lease_ack",
    ]
    state = StateStore(tmp_path / "state.json").load()
    assert state["notification_outbox"]["source-1"]["custody_status"] == "durable"
    assert ack_order == ["ack"]
    assert state["processed"] == ["source-1"]
    assert state["reply_outbox"]["source-1"]["status"] == "complete"


@pytest.mark.asyncio
async def test_peer_restart_expired_attempt_a_never_generic_acks_live_attempt_b(
    tmp_path: Path,
) -> None:
    """BLOCK-2 P1 (adversarial gate, PR #11 round 5): a restart whose only
    live attempt for this custodied reply has since expired server-side no
    longer durably quarantines (the "restart flap" -- `connect()` used to
    report `True` and then die identically, every restart, on this exact
    replay). `_ack_persisted_ownership` now treats a well-formed `expired`
    ack response as a deferral: the reply already reached human custody, so
    it commits locally instead. The safety property this test's name
    describes -- ATTEMPT_B (a live, unrelated attempt) is never
    generic-acked as a side effect -- is unchanged and still asserted below.
    """
    first = adapter_at(tmp_path, ProtocolClient())
    await bind_attempt_delivery(first)
    result = await first.send("sender", "Durable exact final.")
    assert result.success is True
    ownership = StateStore(tmp_path / "state.json").load()["reply_outbox"][
        "source-1"
    ]["ack_ownership"]
    assert ownership == {
        "version": 1,
        "kind": "attempt",
        "attempt_id": ATTEMPT_A,
        **STRICT_SCOPE,
        "profile_owner_fingerprint": first._profile_owner_fingerprint,
    }
    clear_lease_marker_for_replay(tmp_path)

    client = AttemptReplayClient(attempt_state="expired", consumed=False)
    restarted = adapter_at(tmp_path, client)
    await restarted._replay_reply_outbox()  # no longer raises -- see docstring

    assert ("inbox_lease_ack", {"attempt_id": ATTEMPT_A}) in client.calls
    assert not any(tool == "inbox_ack" for tool, _arguments in client.calls)
    assert client.attempts[ATTEMPT_B] == "leased"  # never touched
    assert client.message_read[ATTEMPT_B] is False
    state = StateStore(tmp_path / "state.json").load()
    assert "source-1" in state.get("processed", [])
    assert state["reply_outbox"]["source-1"]["status"] == "complete"
    assert restarted._lease_quarantined is False
    assert restarted._reply_reconciliation_required is False


@pytest.mark.asyncio
async def test_two_restarts_second_connects_and_completes_through_real_poll_loop(
    tmp_path: Path,
) -> None:
    """BLOCK-2 P1 (adversarial gate, PR #11 round 5) -- the "restart flap"
    -- driven through the REAL poll loop (`connect()`/`_poll_loop`, not a
    direct `_replay_reply_outbox()` call): a custodied reply whose only
    attempt died mid-turn (e.g. `disconnect()` before its ack ran) used to
    durably quarantine on the first restart's very first replay tick, and
    because the quarantine flag was never persisted, `connect()` kept
    reporting `True` and dying immediately on EVERY subsequent restart --
    an infinite flap invisible to anything that only checks `connect()`'s
    return value.

    First restart: its poll loop's first tick replays the stale record,
    gets a well-formed `expired` ack response for the dead attempt, commits
    locally (custody already existed), and KEEPS RUNNING -- no crash, no
    fatal error, `inbox_lease` proceeds normally afterward. Second restart:
    a completely ordinary process with nothing left to reconcile at all.
    """
    first = adapter_at(tmp_path, ProtocolClient())
    await bind_attempt_delivery(first)
    assert (await first.send("sender", "Durable exact final.")).success is True
    clear_lease_marker_for_replay(tmp_path)

    client = AttemptReplayClient(attempt_state="expired", consumed=False)
    restarted = adapter_at(tmp_path, client)
    assert await restarted.connect() is True
    try:
        await wait_until(
            lambda: "source-1"
            in StateStore(tmp_path / "state.json").load().get("processed", [])
        )
        # The poll loop is still alive and healthy after committing locally
        # -- not the flap's "connect() succeeded, then died on this exact
        # tick" shape.
        await asyncio.sleep(0.05)
        assert restarted._running is True
        assert restarted._fatal_error_code is None
    finally:
        await restarted.disconnect()

    state = StateStore(tmp_path / "state.json").load()
    assert "source-1" in state["processed"]
    assert state["reply_outbox"]["source-1"]["status"] == "complete"
    assert state.get("lease_reconciliation") is None
    assert not any(tool == "inbox_ack" for tool, _arguments in client.calls)
    assert client.attempts[ATTEMPT_B] == "leased"  # never generic-acked

    # Second restart: an ordinary process, nothing left to reconcile.
    third_client = ProtocolClient()
    third = adapter_at(tmp_path, third_client)
    assert await third.connect() is True
    await asyncio.sleep(0.05)
    assert third._running is True
    assert third._fatal_error_code is None
    assert third._lease_quarantined is False
    assert third._reply_reconciliation_required is False
    await third.disconnect()
    assert not any(tool == "inbox_lease_ack" for tool, _args in third_client.calls)


@pytest.mark.asyncio
async def test_peer_restart_acked_attempt_replay_commits_once_without_generic_ack(
    tmp_path: Path,
) -> None:
    first = adapter_at(tmp_path, ProtocolClient())
    await bind_attempt_delivery(first)
    assert (await first.send("sender", "Durable exact final.")).success is True
    clear_lease_marker_for_replay(tmp_path)

    client = AttemptReplayClient(attempt_state="acked", consumed=True)
    restarted = adapter_at(tmp_path, client)
    await restarted._replay_reply_outbox()
    await restarted._replay_reply_outbox()

    assert [tool for tool, _arguments in client.calls].count("inbox_lease_ack") == 1
    assert not any(tool == "inbox_ack" for tool, _arguments in client.calls)
    state = StateStore(tmp_path / "state.json").load()
    assert state["processed"] == ["source-1"]
    assert state["reply_outbox"]["source-1"]["status"] == "complete"


@pytest.mark.asyncio
async def test_peer_restart_scope_swap_stops_before_attempt_or_generic_ack(
    tmp_path: Path,
) -> None:
    first = adapter_at(tmp_path, ProtocolClient())
    await bind_attempt_delivery(first)
    assert (await first.send("sender", "Durable exact final.")).success is True
    clear_lease_marker_for_replay(tmp_path)

    client = AttemptReplayClient(
        attempt_state="acked",
        consumed=True,
        status_scope={**STRICT_SCOPE, "effective_inbox_seat": "other-seat"},
    )
    restarted = adapter_at(tmp_path, client)
    with pytest.raises(RuntimeError, match="Mupot MCP request failed"):
        await restarted._replay_reply_outbox()

    assert client.calls == [("inbox_consumer_status", {"strict_scope": True})]
    state = StateStore(tmp_path / "state.json").load()
    assert "source-1" not in state.get("processed", [])
    assert state["reply_outbox"]["source-1"]["status"] == "custodied"


@pytest.mark.asyncio
async def test_peer_restart_profile_owner_swap_stops_before_status_or_ack(
    tmp_path: Path,
) -> None:
    first = adapter_at(tmp_path, ProtocolClient(), ScopeOwner("a" * 64))
    await bind_attempt_delivery(first)
    assert (await first.send("sender", "Durable exact final.")).success is True
    clear_lease_marker_for_replay(tmp_path)

    client = AttemptReplayClient(attempt_state="acked", consumed=True)
    restarted = adapter_at(tmp_path, client, ScopeOwner("b" * 64))
    with pytest.raises(RuntimeError, match="Mupot MCP request failed"):
        await restarted._replay_reply_outbox()

    assert client.calls == []
    state = StateStore(tmp_path / "state.json").load()
    assert "source-1" not in state.get("processed", [])
    assert state["reply_outbox"]["source-1"]["status"] == "custodied"


@pytest.mark.asyncio
async def test_two_native_receivers_exchange_one_final_ack_then_stop(
    tmp_path: Path,
) -> None:
    """A structured terminal ACK is a receipt, not another model request."""
    sender_client = ProtocolClient()
    receiver_a = adapter_at(tmp_path / "a", sender_client)
    receiver_a.set_message_handler(lambda _event: _async_value("Completed once."))
    await receiver_a._deliver(source_message("request-a"))

    assert sender_client.acked_ids == ["request-a"]
    assert len(sender_client.messages_by_key) == 1
    outgoing = next(iter(sender_client.messages_by_key.values()))
    incoming_ack = {
        "id": "ack-at-b",
        "seq": 8,
        "from_agent": "receiver",
        "from_member": "member-receiver",
        "kind": outgoing["kind"],
        "body": outgoing["body"],
        "request_id": outgoing["request_id"],
        "in_reply_to": outgoing["in_reply_to"],
        "project_id": outgoing["project_id"],
        "expects_reply": False,
    }

    receiver_client = ProtocolClient()
    receiver_b = adapter_at(tmp_path / "b", receiver_client)
    model_turns: list[str] = []
    receiver_b.set_message_handler(lambda event: _record_model(model_turns, event.message_id))
    await receiver_b._handle_ack_envelope(incoming_ack)

    assert model_turns == []
    assert receiver_client.acked_ids == ["ack-at-b"]
    assert receiver_client.messages_by_key == {}
    state = StateStore(tmp_path / "b" / "state.json").load()
    assert state["terminal_receipts"] == [incoming_ack]


async def _async_value(value: str) -> str:
    return value


async def _record_model(seen: list[str], message_id: str) -> str:
    seen.append(message_id)
    return "must not run"


@pytest.mark.asyncio
async def test_missing_final_receipt_and_empty_output_never_authorize_source_consume(
    tmp_path: Path,
) -> None:
    """Hermes success alone is insufficient without a concrete final send receipt."""
    # Round 4 (adversarial BLOCK-2, PR #11 round 3, 2026-09-15): outcome ==
    # SUCCESS with no reply ever reaching human custody is now a deferral
    # (`empty_output`), not a silent return -- see `_DeliveryDeferred`. Both
    # scenarios below still never ack/consume the source.
    malformed = ProtocolClient()
    malformed.malformed_receipt = True
    missing_receipt = adapter_at(tmp_path / "missing", malformed)
    missing_receipt.set_message_handler(lambda _event: _async_value("Final answer."))
    with pytest.raises(_DeliveryDeferred) as exc_info:
        await missing_receipt._deliver(source_message("missing-receipt"))
    # A malformed receipt fails the send itself (both the primary attempt and
    # the plain-text fallback), so the turn ends via the fall-through FAILURE
    # exit (`handler_error`), not the empty-output exit -- the handler DID
    # produce text, it just never reached a validated receipt.
    assert exc_info.value.reason == "handler_error"
    assert malformed.acked_ids == []

    empty_client = ProtocolClient()
    empty = adapter_at(tmp_path / "empty", empty_client)
    empty.set_message_handler(lambda _event: _async_value(""))
    with pytest.raises(_DeliveryDeferred) as exc_info:
        await empty._deliver(source_message("empty-output"))
    assert exc_info.value.reason == "empty_output"
    assert empty_client.acked_ids == []
    assert empty_client.messages_by_key == {}


@pytest.mark.asyncio
async def test_legacy_pending_ambiguity_is_preserved_and_fences_connect(
    tmp_path: Path,
) -> None:
    """Old pending work has no immutable reply bytes and must never be regenerated."""
    state_path = tmp_path / "state.json"
    legacy = {
        "processed": ["done-1"],
        "terminal_receipts": [{"id": "ack-1"}],
        "notification_outbox": {"notice-1": {"status": "transport_unknown"}},
        "pending": {"message": source_message("legacy-pending")},
        "legacy_extension": {"version": 7},
    }
    StateStore(state_path).save(copy.deepcopy(legacy))
    client = ProtocolClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda _server: client,
    )

    assert await adapter.connect() is False
    assert client.calls == []
    persisted = StateStore(state_path).load()
    assert persisted["pending"] == legacy["pending"]
    assert persisted["processed"] == legacy["processed"]
    assert persisted["terminal_receipts"] == legacy["terminal_receipts"]
    assert persisted["notification_outbox"] == legacy["notification_outbox"]
    assert persisted["legacy_extension"] == legacy["legacy_extension"]


@pytest.mark.asyncio
async def test_long_source_id_uses_bounded_stable_response_key(tmp_path: Path) -> None:
    """Raw source IDs that cannot fit the server RID grammar need a stable hash fallback."""
    client = ProtocolClient()
    adapter = adapter_at(tmp_path, client)
    await bind_delivery(adapter, source_message("x" * 200))

    result = await adapter.send("sender", "Final answer.")

    assert result.success is True
    assert list(client.messages_by_key) == [
        "resp-aa20c23e3201834050679e1d88941b9a6fed0557c9a705cb2c315e2e63fd486d"
    ]


@pytest.mark.asyncio
async def test_permanent_final_conflict_requires_restart_reconciliation(
    tmp_path: Path,
) -> None:
    """A possible request-id conflict must fence automatic replay and leasing."""
    client = ProtocolClient()
    client.permanent_send_failure = True
    adapter = adapter_at(tmp_path, client)
    message = source_message()
    adapter._state["pending"] = {"message": copy.deepcopy(message)}
    adapter.store.save(adapter._state)
    await bind_delivery(adapter, message)

    result = await adapter.send("sender", "Immutable final.")
    assert result.success is False
    prepared = StateStore(tmp_path / "state.json").load()["reply_outbox"]["source-1"]
    assert prepared["arguments"]["body"] == "Immutable final."
    assert prepared["status"] == "reconciliation_required"

    restarted_client = ProtocolClient()
    restarted = adapter_at(tmp_path, restarted_client)
    assert await restarted.connect() is False
    assert restarted_client.calls == []


@pytest.mark.asyncio
async def test_corrupt_persisted_envelope_returns_generic_failure_without_transport(
    tmp_path: Path,
) -> None:
    """Invalid durable state must fail closed without an unbound error-path variable."""
    client = ProtocolClient()
    adapter = adapter_at(tmp_path, client)
    message = source_message()
    adapter._state["reply_outbox"] = {
        "source-1": {"version": 1, "status": "prepared"}
    }
    adapter.store.save(adapter._state)
    await bind_delivery(adapter, message)

    result = await adapter.send("sender", "Final answer.")

    assert result.success is False
    assert result.error == "Mupot MCP request failed"
    assert [call for call in client.calls if call[0] == "send"] == []
