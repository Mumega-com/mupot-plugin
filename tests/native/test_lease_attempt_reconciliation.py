"""Server-authoritative inbox lease attempt recovery contract."""

from __future__ import annotations

import asyncio
import inspect
import math
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from gateway.config import PlatformConfig

from plugin.mupot_gateway.adapter import (
    MupotAdapter,
    MupotSafeRetryError,
    MupotTransportError,
    StateStore,
)


ATTEMPT_RE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
LEASE_EXPIRY = "2099-01-01T00:00:00.000Z"
SCOPE = {
    "tenant": "tenant-a",
    "agent_id": "agent-consumer",
    "effective_inbox_seat": "seat-a",
}


def attempt_result(
    attempt_id: str,
    state: str,
    messages: list[dict[str, Any]] | None = None,
    *,
    lease_expires_at: str | None = None,
) -> dict[str, Any]:
    return {
        **SCOPE,
        "attempt_id": attempt_id,
        "state": state,
        "lease_expires_at": lease_expires_at,
        "messages": messages or [],
        "consumed": False,
    }


def ack_result(
    attempt_id: str,
    *,
    state: str = "acked",
    consumed: bool = True,
    scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        **(scope or SCOPE),
        "attempt_id": attempt_id,
        "state": state,
        "consumed": consumed,
    }


def leased_ack(message_id: str = "ack-recovered") -> dict[str, Any]:
    return {
        "id": message_id,
        "seq": 31,
        "from_agent": "hadi-codex",
        "from_member": "member-code",
        "body": "{ack_for:request-recovered} complete",
        "project_id": None,
        "request_id": "ack:request-recovered",
        "in_reply_to": "source-recovered",
        "kind": "ack",
        "expects_reply": False,
        "created_at": "2026-09-13T00:00:00.000Z",
        "delivery_attempts": 1,
        "lease_expires_at": LEASE_EXPIRY,
    }


class AttemptClient:
    def __init__(
        self,
        *,
        lease_outcomes: list[Any] | None = None,
        reconcile_outcome: Any = None,
        ack_outcome: Any = None,
        status: Any = None,
    ) -> None:
        self.lease_outcomes = list(lease_outcomes or [])
        self.reconcile_outcome = reconcile_outcome
        self.ack_outcome = ack_outcome
        self.status = status if status is not None else {
            "strict_scope": True,
            **SCOPE,
            "mode": "bearer_only",
            "generation": 7,
            "key_matches": True,
        }
        self.connect_calls = 0
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.acked_ids: list[str] = []
        self.first_lease = asyncio.Event()

    async def connect(self) -> None:
        self.connect_calls += 1

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((tool, dict(arguments)))
        if tool == "inbox_consumer_status":
            if isinstance(self.status, Exception):
                raise self.status
            return dict(self.status)
        if tool == "inbox_lease":
            self.first_lease.set()
            outcome = self.lease_outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            if callable(outcome):
                return outcome(arguments)
            return outcome
        if tool == "inbox_lease_reconcile":
            outcome = self.reconcile_outcome
            if isinstance(outcome, Exception):
                raise outcome
            if callable(outcome):
                return outcome(arguments)
            return outcome
        if tool == "inbox_lease_ack":
            outcome = self.ack_outcome
            if isinstance(outcome, Exception):
                raise outcome
            if callable(outcome):
                return outcome(arguments)
            return outcome or ack_result(arguments["attempt_id"])
        if tool == "inbox_ack":
            message_id = arguments["ids"][0]
            self.acked_ids.append(message_id)
            return {"acked": [message_id], "already_read": [], "refused": []}
        raise AssertionError(f"unexpected tool: {tool} {arguments}")


def make_adapter(
    state_path: Path,
    client: AttemptClient,
    owner: ScopeOwner | None = None,
) -> MupotAdapter:
    return MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "hadi-codex",
                "lease_seconds": 30,
                "poll_interval": 0.01,
                "state_path": str(state_path),
            },
        ),
        client_factory=lambda *_: client,
        secret_owner=owner,  # type: ignore[arg-type]
    )


class ScopeOwner:
    def __init__(self, fingerprint: str = "a" * 64) -> None:
        self.active = False
        self.activations = 0
        self.fingerprint = fingerprint

    @contextmanager
    def activate(self):
        self.activations += 1
        self.active = True
        try:
            yield
        finally:
            self.active = False


class RejectingScopeOwner:
    @contextmanager
    def activate(self):
        raise RuntimeError("profile scope unavailable")
        yield


class ScopedAttemptClient(AttemptClient):
    def __init__(self, owner: ScopeOwner, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.owner = owner

    async def connect(self) -> None:
        assert self.owner.active is True
        await super().connect()

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        assert self.owner.active is True
        return await super().call(tool, arguments)


async def wait_stopped(adapter: MupotAdapter) -> None:
    for _ in range(100):
        if not adapter._running:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("adapter did not stop")


async def persist_v2_ambiguous(
    state_path: Path,
    *,
    outcomes: list[Any] | None = None,
    owner: ScopeOwner | None = None,
) -> tuple[str, AttemptClient]:
    client = AttemptClient(
        lease_outcomes=outcomes or [MupotTransportError("Mupot request failed")]
    )
    adapter = make_adapter(state_path, client, owner)
    assert await adapter.connect()
    try:
        await asyncio.wait_for(client.first_lease.wait(), 1)
        await wait_stopped(adapter)
    finally:
        await adapter.disconnect()
    marker = StateStore(state_path).load()["lease_reconciliation"]
    return marker["attempt_id"], client


@pytest.mark.asyncio
async def test_ambiguous_attempt_is_random_bounded_durable_and_reconciled_immediately(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, first = await persist_v2_ambiguous(state_path)
    marker = StateStore(state_path).load()["lease_reconciliation"]
    owner_fingerprint = marker.pop("profile_owner_fingerprint")
    assert re.fullmatch(r"[0-9a-f]{64}", owner_fingerprint)
    assert marker == {
        "version": 3,
        "required": True,
        **SCOPE,
        "mode": "bearer_only",
        "generation": 7,
        "attempt_id": attempt_id,
    }
    assert ATTEMPT_RE.fullmatch(attempt_id)
    assert first.calls[0] == ("inbox_consumer_status", {"strict_scope": True})
    assert first.calls[-1] == (
        "inbox_lease",
        {"limit": 1, "lease_seconds": 30, "attempt_id": attempt_id},
    )

    monkeypatch.setattr(time, "time", lambda: math.nan)
    recovered = AttemptClient(
        reconcile_outcome=lambda args: attempt_result(args["attempt_id"], "cancelled")
    )
    adapter = make_adapter(state_path, recovered)
    assert await adapter.reconcile_inbox_polling() is True
    assert recovered.calls == [
        ("inbox_consumer_status", {"strict_scope": True}),
        ("inbox_lease_reconcile", {"attempt_id": attempt_id}),
    ]
    assert StateStore(state_path).load().get("lease_reconciliation") is None


@pytest.mark.asyncio
async def test_one_safe_retry_reuses_same_attempt_id(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, client = await persist_v2_ambiguous(
        state_path,
        outcomes=[
            MupotSafeRetryError("Mupot request failed"),
            MupotTransportError("Mupot request failed"),
        ],
    )
    leases = [args for tool, args in client.calls if tool == "inbox_lease"]
    assert len(leases) == 2
    assert [entry["attempt_id"] for entry in leases] == [attempt_id, attempt_id]


@pytest.mark.asyncio
@pytest.mark.parametrize("clock", [-1e12, 0.0, 1e12, math.nan])
async def test_v1_marker_never_uses_local_clock_to_clear(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    clock: float,
) -> None:
    state_path = tmp_path / "state.json"
    StateStore(state_path).save(
        {
            "lease_reconciliation": {
                "version": 1,
                "required": True,
                "agent_id": "agent-consumer",
                "mode": "bearer_only",
                "generation": 7,
                "reconcile_after": 1.0,
            }
        }
    )
    monkeypatch.setattr(time, "time", lambda: clock)
    client = AttemptClient(reconcile_outcome={})
    adapter = make_adapter(state_path, client)
    assert await adapter.reconcile_inbox_polling() is False
    assert client.connect_calls == 0
    assert client.calls == []
    assert StateStore(state_path).load()["lease_reconciliation"]["version"] == 1


@pytest.mark.asyncio
async def test_pre_owner_v3_marker_remains_fenced_without_network(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    StateStore(state_path).save(
        {
            "lease_reconciliation": {
                "version": 3,
                "required": True,
                **SCOPE,
                "mode": "bearer_only",
                "generation": 7,
                "attempt_id": "legacy-attempt-id-1234",
            }
        }
    )
    client = AttemptClient()
    adapter = make_adapter(state_path, client, ScopeOwner())

    assert await adapter.connect() is False
    assert await adapter.reconcile_inbox_polling() is False
    assert client.connect_calls == 0
    assert client.calls == []
    assert StateStore(state_path).load()["lease_reconciliation"]["attempt_id"] == (
        "legacy-attempt-id-1234"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["empty", "cancelled", "expired", "acked"])
async def test_terminal_attempt_tombstone_clears_without_processing_or_ack(
    tmp_path: Path,
    state: str,
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(state_path)
    client = AttemptClient(
        reconcile_outcome=attempt_result(attempt_id, state),
    )
    adapter = make_adapter(state_path, client)
    handled: list[str] = []

    async def handler(event: Any) -> None:
        handled.append(event.message_id)

    adapter.set_message_handler(handler)
    assert await adapter.reconcile_inbox_polling() is True
    assert handled == []
    assert client.acked_ids == []
    assert not any(tool in {"inbox_ack", "inbox_lease_ack"} for tool, _ in client.calls)
    assert StateStore(state_path).load().get("lease_reconciliation") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["empty", "cancelled", "expired"])
async def test_terminal_attempt_tombstone_drops_stale_unstaged_pending(
    tmp_path: Path,
    state: str,
) -> None:
    """Class fix (2026-09-15, see _LeaseExpiredDeferred): a terminal, unconsumed
    tombstone whose local `pending` points at a source with NO reply_outbox
    record (the clean case -- nothing was ever staged for it) clears the fence
    AND drops the stale `pending`, exactly as the genuinely-empty case above.
    `pending` can never resume once the attempt tombstones server-side, so
    leaving it would fence an unrelated future crash as ambiguous for no reason.
    """
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(state_path)
    saved = StateStore(state_path).load()
    saved["pending"] = {"message": {"id": "orphaned-source"}}
    StateStore(state_path).save(saved)
    client = AttemptClient(
        reconcile_outcome=attempt_result(attempt_id, state),
    )
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "hadi-codex",
                "lease_seconds": 30,
                "poll_interval": 0.01,
                "state_path": str(state_path),
            },
        ),
        client_factory=lambda *_: client,
    )

    assert await adapter.reconcile_inbox_polling() is True
    final = StateStore(state_path).load()
    assert final.get("lease_reconciliation") is None
    assert final.get("pending") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["empty", "cancelled", "expired"])
async def test_terminal_attempt_tombstone_preserves_fence_when_reply_staged(
    tmp_path: Path,
    state: str,
) -> None:
    """Countercase for the class fix above: when `reply_outbox` still holds a
    record for the source the local `pending` points at, the attempt itself
    may be gone, but the reply's fate is not provably safe to abandon -- a
    later `_replay_reply_outbox` could transmit or ack using ownership tied
    to an attempt this call just tombstoned. This MUST stay fail-closed:
    `reconcile_inbox_polling()` returns False and the marker survives.
    """
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(state_path)
    saved = StateStore(state_path).load()
    saved["pending"] = {"message": {"id": "staged-source"}}
    saved["reply_outbox"] = {"staged-source": {"status": "prepared"}}
    StateStore(state_path).save(saved)
    client = AttemptClient(
        reconcile_outcome=attempt_result(attempt_id, state),
    )
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "hadi-codex",
                "lease_seconds": 30,
                "poll_interval": 0.01,
                "state_path": str(state_path),
            },
        ),
        client_factory=lambda *_: client,
    )

    assert await adapter.reconcile_inbox_polling() is False
    final = StateStore(state_path).load()
    assert isinstance(final.get("lease_reconciliation"), dict)
    assert final["lease_reconciliation"]["attempt_id"] == attempt_id
    assert final.get("pending") == {"message": {"id": "staged-source"}}
    assert final.get("reply_outbox") == {"staged-source": {"status": "prepared"}}


@pytest.mark.asyncio
async def test_exact_leased_attempt_is_processed_and_acked_before_clear(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(state_path)
    message = leased_ack()
    client = AttemptClient(
        reconcile_outcome=attempt_result(
            attempt_id,
            "leased",
            [message],
            lease_expires_at=LEASE_EXPIRY,
        )
    )
    adapter = make_adapter(state_path, client)

    assert await adapter.reconcile_inbox_polling() is True
    assert client.acked_ids == []
    assert ("inbox_lease_ack", {"attempt_id": attempt_id}) in client.calls
    assert not any(tool == "inbox_ack" for tool, _ in client.calls)
    state = StateStore(state_path).load()
    assert message["id"] in state["processed"]
    assert state["terminal_receipts"] == [message]
    assert state.get("lease_reconciliation") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response_factory",
    [
        lambda attempt, _message: attempt_result("other-attempt-id-1234", "empty"),
        lambda attempt, message: attempt_result(
            attempt,
            "leased",
            [{**message, "lease_expires_at": "2098-01-01T00:00:00.000Z"}],
            lease_expires_at=LEASE_EXPIRY,
        ),
        lambda attempt, message: attempt_result(attempt, "cancelled", [message]),
        lambda attempt, _message: {
            **SCOPE,
            "attempt_id": attempt,
            "state": "unknown",
            "lease_expires_at": None,
            "messages": [],
            "consumed": False,
        },
        lambda attempt, _message: attempt_result(
            attempt,
            "empty",
        ) | {"tenant": "tenant-b"},
        lambda attempt, _message: attempt_result(
            attempt,
            "empty",
        ) | {"effective_inbox_seat": "seat-b"},
    ],
)
async def test_reconcile_mismatch_or_malformed_response_remains_fenced(
    tmp_path: Path,
    response_factory: Any,
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(state_path)
    message = leased_ack()
    client = AttemptClient(
        reconcile_outcome=response_factory(attempt_id, message),
    )
    adapter = make_adapter(state_path, client)

    assert await adapter.reconcile_inbox_polling() is False
    assert client.acked_ids == []
    assert StateStore(state_path).load()["lease_reconciliation"]["attempt_id"] == attempt_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ack_outcome",
    [
        ack_result("other-attempt-id-1234"),
        ack_result("placeholder", state="expired", consumed=False),
        ack_result("placeholder") | {"tenant": "tenant-b"},
        {"state": "acked", "consumed": True},
    ],
)
async def test_attempt_ack_malformed_nonconsumed_or_scope_mismatch_stays_fenced(
    tmp_path: Path,
    ack_outcome: dict[str, Any],
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(state_path)
    message = leased_ack()
    if ack_outcome.get("attempt_id") == "placeholder":
        ack_outcome = {**ack_outcome, "attempt_id": attempt_id}
    client = AttemptClient(
        reconcile_outcome=attempt_result(
            attempt_id,
            "leased",
            [message],
            lease_expires_at=LEASE_EXPIRY,
        ),
        ack_outcome=ack_outcome,
    )
    adapter = make_adapter(state_path, client)

    assert await adapter.reconcile_inbox_polling() is False
    assert message["id"] not in StateStore(state_path).load().get("processed", [])
    assert StateStore(state_path).load()["lease_reconciliation"]["attempt_id"] == attempt_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        {**SCOPE, "strict_scope": True, "mode": "bearer_only", "generation": 8, "key_matches": True},
        {**SCOPE, "strict_scope": True, "mode": "bearer_only", "generation": 7, "key_matches": True, "tenant": "tenant-b"},
        {**SCOPE, "strict_scope": True, "mode": "bearer_only", "generation": 7, "key_matches": True, "agent_id": "agent-b"},
        {**SCOPE, "strict_scope": True, "mode": "bearer_only", "generation": 7, "key_matches": True, "effective_inbox_seat": "seat-b"},
    ],
)
async def test_profile_or_scope_swap_makes_zero_reconcile_or_ack_calls(
    tmp_path: Path,
    status: dict[str, Any],
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(state_path)
    client = AttemptClient(status=status, reconcile_outcome=attempt_result(attempt_id, "empty"))
    adapter = make_adapter(state_path, client)

    assert await adapter.reconcile_inbox_polling() is False
    assert client.calls == [("inbox_consumer_status", {"strict_scope": True})]
    assert StateStore(state_path).load()["lease_reconciliation"]["attempt_id"] == attempt_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        MupotTransportError("Mupot request failed"),
        {},
        {**SCOPE, "strict_scope": False, "mode": "bearer_only", "generation": 7, "key_matches": True},
        {**SCOPE, "strict_scope": True, "mode": "bearer_only", "generation": 7, "key_matches": False},
    ],
)
async def test_strict_scope_lookup_failure_stays_fenced_without_reconcile_or_ack(
    tmp_path: Path,
    status: Any,
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(state_path)
    client = AttemptClient(status=status)
    adapter = make_adapter(state_path, client)

    assert await adapter.reconcile_inbox_polling() is False
    assert not any(
        tool in {"inbox_lease_reconcile", "inbox_lease_ack", "inbox_ack"}
        for tool, _ in client.calls
    )
    assert StateStore(state_path).load()["lease_reconciliation"]["attempt_id"] == attempt_id


@pytest.mark.asyncio
async def test_profile_scope_failure_stays_fenced_without_network(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(state_path)
    client = AttemptClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
        secret_owner=RejectingScopeOwner(),  # type: ignore[arg-type]
    )

    assert await adapter.reconcile_inbox_polling() is False
    assert client.connect_calls == 0
    assert client.calls == []
    assert StateStore(state_path).load()["lease_reconciliation"]["attempt_id"] == attempt_id


@pytest.mark.asyncio
async def test_same_server_scope_distinct_profile_owner_stays_fenced_without_network(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    first_client = AttemptClient(
        lease_outcomes=[MupotTransportError("Mupot request failed")]
    )
    first_owner = ScopeOwner("a" * 64)
    first = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "hadi-codex",
                "lease_seconds": 30,
                "poll_interval": 0.01,
                "state_path": str(state_path),
            },
        ),
        client_factory=lambda *_: first_client,
        secret_owner=first_owner,  # type: ignore[arg-type]
    )
    assert await first.connect()
    try:
        await asyncio.wait_for(first_client.first_lease.wait(), 1)
        await wait_stopped(first)
    finally:
        await first.disconnect()

    marker = StateStore(state_path).load()["lease_reconciliation"]
    assert marker["profile_owner_fingerprint"] == first_owner.fingerprint

    second_client = AttemptClient(
        reconcile_outcome=attempt_result(marker["attempt_id"], "cancelled")
    )
    second = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: second_client,
        secret_owner=ScopeOwner("b" * 64),  # type: ignore[arg-type]
    )

    assert await second.reconcile_inbox_polling() is False
    assert second_client.connect_calls == 0
    assert second_client.calls == []
    assert StateStore(state_path).load()["lease_reconciliation"] == marker


@pytest.mark.asyncio
async def test_distinct_profile_owner_cannot_attempt_ack(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(
        state_path,
        owner=ScopeOwner("a" * 64),
    )
    client = AttemptClient()
    adapter = make_adapter(state_path, client, ScopeOwner("b" * 64))

    with pytest.raises(RuntimeError, match="Mupot MCP request failed"):
        await adapter._ack_expected("untrusted-message", attempt_id=attempt_id)

    assert client.calls == []


@pytest.mark.asyncio
async def test_current_adapter_owner_change_cannot_attempt_ack(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    owner = ScopeOwner("a" * 64)
    attempt_id, _first = await persist_v2_ambiguous(state_path, owner=owner)
    client = AttemptClient()
    adapter = make_adapter(state_path, client, owner)
    owner.fingerprint = "b" * 64

    with pytest.raises(RuntimeError, match="Mupot MCP request failed"):
        await adapter._ack_expected("untrusted-message", attempt_id=attempt_id)

    assert client.calls == []


@pytest.mark.asyncio
async def test_nonattempt_legacy_ack_uses_generic_exact_id(
    tmp_path: Path,
) -> None:
    client = AttemptClient()
    adapter = make_adapter(tmp_path / "state.json", client)

    await adapter._ack_expected("legacy-message-id")

    assert client.calls == [("inbox_ack", {"ids": ["legacy-message-id"]})]


def test_delivery_context_carries_attempt_id_for_attempt_originated_work(
    tmp_path: Path,
) -> None:
    client = AttemptClient()
    adapter = make_adapter(tmp_path / "state.json", client)
    assert "attempt_id" in inspect.signature(adapter._begin_delivery).parameters
    message = {
        **leased_ack("request-recovered"),
        "kind": "request",
        "expects_reply": True,
        "body": "perform exact recovered work",
    }
    _event, runtime = adapter._begin_delivery(
        message,
        attempt_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    )
    assert runtime.context.attempt_id == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


@pytest.mark.asyncio
async def test_delayed_reconcile_stays_fenced_until_server_tombstone(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    attempt_id, _first = await persist_v2_ambiguous(state_path)
    unavailable = AttemptClient(
        reconcile_outcome=MupotTransportError("Mupot request failed")
    )
    first_recovery = make_adapter(state_path, unavailable)
    assert await first_recovery.reconcile_inbox_polling() is False
    assert StateStore(state_path).load()["lease_reconciliation"]["attempt_id"] == attempt_id

    tombstoned = AttemptClient(
        reconcile_outcome=attempt_result(attempt_id, "expired")
    )
    second_recovery = make_adapter(state_path, tombstoned)
    assert await second_recovery.reconcile_inbox_polling() is True
    assert tombstoned.acked_ids == []
    assert StateStore(state_path).load().get("lease_reconciliation") is None


@pytest.mark.asyncio
async def test_reconciliation_uses_owning_profile_scope(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    owner = ScopeOwner()
    attempt_id, _first = await persist_v2_ambiguous(state_path, owner=owner)
    owner.activations = 0
    client = ScopedAttemptClient(
        owner,
        reconcile_outcome=attempt_result(attempt_id, "cancelled"),
    )
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"state_path": str(state_path)},
        ),
        client_factory=lambda *_: client,
        secret_owner=owner,  # type: ignore[arg-type]
    )

    assert await adapter.reconcile_inbox_polling() is True
    assert owner.activations == 1
    assert owner.active is False


@pytest.mark.asyncio
async def test_same_profile_token_rotation_may_reconcile_exact_scope(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    first_owner = ScopeOwner("c" * 64)
    attempt_id, _first = await persist_v2_ambiguous(
        state_path,
        owner=first_owner,
    )
    rotated_owner = ScopeOwner("c" * 64)
    client = ScopedAttemptClient(
        rotated_owner,
        reconcile_outcome=attempt_result(attempt_id, "cancelled"),
    )
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
        secret_owner=rotated_owner,  # type: ignore[arg-type]
    )

    assert await adapter.reconcile_inbox_polling() is True
    assert StateStore(state_path).load().get("lease_reconciliation") is None
