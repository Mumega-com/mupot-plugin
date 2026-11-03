"""Definite pre-delivery activation refusals are retried after readiness;
ambiguous outcomes stay the fail-closed ``activation_unknown`` fence."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from gateway.config import PlatformConfig
from plugin.mupot_gateway import notifications
from plugin.mupot_gateway.adapter import MupotAdapter, StateStore


class _Client:
    async def call(self, tool, arguments):  # pragma: no cover - never reached
        raise AssertionError("no mupot call expected")


SESSION = [{
    "id": "human", "session_key": "agent:main:telegram:dm:123", "source": "telegram",
    "user_id": "owner", "chat_id": "123", "chat_type": "dm", "last_active": 1,
}]


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    import hermes_constants

    home = tmp_path / "hermes-home"
    home.mkdir()
    token = hermes_constants.set_hermes_home_override(str(home))
    monkeypatch.setattr(notifications, "active_sessions", lambda: SESSION)
    yield
    hermes_constants.reset_hermes_home_override(token)


def make_adapter(tmp_path: Path, injector, ready=None) -> MupotAdapter:
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={
            "allowed_agents": "kasra",
            "state_path": str(tmp_path / "inbox.json"),
            "notification_recipients": {"telegram": "owner"},
            "notification_activate": True,
        }),
        client_factory=lambda _: _Client(),
        message_injector=injector,
        activation_ready=ready,
    )
    return adapter


def enqueue(adapter, sid="n-1"):
    notifications.enqueue(
        adapter._state, adapter.store,
        {"id": sid, "from_agent": "kasra", "request_id": f"r-{sid}", "kind": "ack",
         "expects_reply": False},
        "Please review.",
    )


def durable(tmp_path, sid="n-1"):
    return StateStore(tmp_path / "inbox.json").load()["notification_outbox"][sid]


def release_backoff(adapter, sid="n-1"):
    adapter._state["notification_outbox"][sid]["retry_at"] = 0
    adapter.store.save(adapter._state)


@pytest.mark.asyncio
async def test_not_ready_at_startup_is_deferred_then_delivered_exactly_once(tmp_path):
    calls: list[str] = []
    gateway_live = {"v": False}
    adapter = make_adapter(
        tmp_path,
        lambda content, **kw: calls.append(content) or True,
        ready=lambda: None if gateway_live["v"] else "no_live_gateway",
    )
    enqueue(adapter)

    await adapter._flush_notifications()
    assert calls == []  # injector never called while not ready
    notice = durable(tmp_path)
    assert notice["status"] == "activation_deferred"
    assert notice["activation_status"] == "deferred"
    assert notice["activation_deferred_reason"] == "no_live_gateway"
    assert notice["activation_deferrals"] == 1
    assert notice["retry_at"] > time.time()

    # Backoff not elapsed: still not retried even once the gateway is live.
    gateway_live["v"] = True
    await adapter._flush_notifications()
    assert calls == []

    release_backoff(adapter)
    await adapter._flush_notifications()
    assert len(calls) == 1
    assert durable(tmp_path)["status"] == "activation_queued"

    # Exactly once: further flushes (and a restart) never re-inject.
    await adapter._flush_notifications()
    restarted = make_adapter(tmp_path, lambda content, **kw: calls.append(content) or True)
    await restarted._flush_notifications()
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_ambiguous_exception_stays_activation_unknown_without_retry(tmp_path):
    calls: list[str] = []

    def boom(content, **kw):
        calls.append(content)
        raise RuntimeError("response lost")

    adapter = make_adapter(tmp_path, boom, ready=lambda: None)
    enqueue(adapter)
    await adapter._flush_notifications()
    assert durable(tmp_path)["status"] == "activation_unknown"
    release_backoff(adapter)
    await adapter._flush_notifications()
    assert len(calls) == 1
    assert durable(tmp_path)["status"] == "activation_unknown"


@pytest.mark.asyncio
async def test_bare_false_after_ready_probe_stays_activation_unknown(tmp_path):
    """A bare ``False`` from inject_message has several causes (denied,
    scheduling failed); readiness said ready, so it is NOT a definite refusal."""
    calls: list[str] = []
    adapter = make_adapter(
        tmp_path, lambda content, **kw: calls.append(content) and False, ready=lambda: None
    )
    enqueue(adapter)
    await adapter._flush_notifications()
    assert durable(tmp_path)["status"] == "activation_unknown"
    release_backoff(adapter)
    await adapter._flush_notifications()
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_probe_exception_is_a_definite_defer_not_an_injection(tmp_path):
    calls: list[str] = []

    def bad_probe():
        raise RuntimeError("probe broke")

    adapter = make_adapter(tmp_path, lambda c, **kw: calls.append(c) or True, ready=bad_probe)
    enqueue(adapter)
    await adapter._flush_notifications()
    assert calls == []
    notice = durable(tmp_path)
    assert notice["status"] == "activation_deferred"
    assert notice["activation_deferred_reason"] == "readiness_probe_RuntimeError"


@pytest.mark.asyncio
async def test_backoff_is_capped_and_reaches_terminal_failure(tmp_path):
    calls: list[str] = []
    adapter = make_adapter(
        tmp_path, lambda c, **kw: calls.append(c) or True, ready=lambda: "no_live_gateway"
    )
    enqueue(adapter)
    delays = []
    for _ in range(notifications.MAX_ACTIVATION_DEFERRALS - 1):
        before = time.time()
        await adapter._flush_notifications()
        notice = durable(tmp_path)
        assert notice["status"] == "activation_deferred"
        delays.append(notice["retry_at"] - before)
        release_backoff(adapter)
    assert max(delays) <= 301
    assert delays[0] < 11 and delays[1] > 15  # exponential growth
    await adapter._flush_notifications()
    notice = durable(tmp_path)
    assert notice["status"] == "activation_failed"
    assert notice["last_error"] == "ActivationNeverReady"
    assert calls == []
    # Terminal: never retried, even when ready later; and it is surfaced.
    adapter.activation_ready = lambda: None
    release_backoff(adapter)
    await adapter._flush_notifications()
    assert calls == []
    assert durable(tmp_path)["status"] == "activation_failed"
    assert [i["status"] for i in adapter.stranded_notifications()] == ["activation_failed"]


@pytest.mark.asyncio
async def test_existing_activation_unknown_rows_are_untouched(tmp_path):
    calls: list[str] = []
    adapter = make_adapter(
        tmp_path, lambda c, **kw: calls.append(c) or True, ready=lambda: None
    )
    enqueue(adapter)
    adapter._state["notification_outbox"]["n-1"].update(
        status="activation_unknown", activation_status="unknown",
        last_error="ActivationOutcomeUnknown",
    )
    adapter.store.save(adapter._state)
    before = dict(durable(tmp_path))

    restarted = make_adapter(tmp_path, lambda c, **kw: calls.append(c) or True, ready=lambda: None)
    await restarted._flush_notifications()
    await restarted._flush_notifications()
    assert calls == []
    assert durable(tmp_path) == before


@pytest.mark.asyncio
async def test_deferral_persist_failure_does_not_call_injector(tmp_path, monkeypatch):
    calls: list[str] = []
    adapter = make_adapter(
        tmp_path, lambda c, **kw: calls.append(c) or True, ready=lambda: "no_live_gateway"
    )
    enqueue(adapter)
    real_save = adapter.store.save

    def fail(value):
        n = value.get("notification_outbox", {}).get("n-1", {})
        if n.get("status") == "activation_deferred":
            raise OSError("disk")
        real_save(value)

    monkeypatch.setattr(adapter.store, "save", fail)
    await adapter._flush_notifications()
    assert calls == []
    assert durable(tmp_path)["status"] == "pending"


def test_gateway_injector_readiness_probe_reads_registration():
    from plugin.mupot_gateway.adapter import _gateway_injector_readiness

    class Mgr:
        has_gateway_message_injector = False

    class Ctx:
        _manager = Mgr()

    probe = _gateway_injector_readiness(Ctx())
    assert probe() == "no_live_gateway"
    Ctx._manager.has_gateway_message_injector = True
    assert probe() is None

    class Bare:
        pass

    assert _gateway_injector_readiness(Bare())() is None  # unknown => prior behaviour
