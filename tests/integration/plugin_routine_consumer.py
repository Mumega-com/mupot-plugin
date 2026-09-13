"""Consume one leased Mupot envelope through a discovered native plugin profile."""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import site
import sys
from pathlib import Path


def _load_runtime() -> None:
    hermes = Path(os.environ["HERMES_SOURCE"]).resolve()
    sys.path.insert(0, str(hermes))
    for root in (hermes / ".venv", hermes / "venv"):
        for candidate in root.glob("lib/python*/site-packages"):
            site.addsitedir(str(candidate))


class CaptureClient:
    def __init__(
        self,
        consumer_status: dict[str, object],
        attempt_result: dict[str, object],
        attempt_ack: dict[str, object],
    ) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.consumer_status = consumer_status
        self.attempt_result = attempt_result
        self.attempt_ack = attempt_ack

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict[str, object]) -> dict[str, object]:
        self.calls.append((tool, arguments.copy()))
        if tool == "inbox_consumer_status":
            if arguments != {"strict_scope": True}:
                raise AssertionError("Routine consumer did not request strict scope")
            return self.consumer_status
        if tool == "inbox_lease_reconcile":
            return self.attempt_result
        if tool == "inbox_lease_ack":
            return self.attempt_ack
        raise AssertionError(f"Routine consumer called unexpected tool {tool}")


async def _main() -> None:
    _load_runtime()
    from gateway.config import PlatformConfig
    from gateway.platform_registry import platform_registry
    from hermes_cli import plugins

    payload = json.load(sys.stdin)
    if not isinstance(payload, dict):
        raise RuntimeError("integration input is invalid")
    envelope = payload.get("envelope")
    assigned_agent_id = payload.get("assigned_agent_id")
    attempt_id = payload.get("attempt_id")
    consumer_status = payload.get("consumer_status")
    attempt_result = payload.get("attempt_result")
    attempt_ack = payload.get("attempt_ack")
    if (
        not isinstance(envelope, dict)
        or not isinstance(assigned_agent_id, str)
        or not isinstance(attempt_id, str)
        or not isinstance(consumer_status, dict)
        or not isinstance(attempt_result, dict)
        or not isinstance(attempt_ack, dict)
    ):
        raise RuntimeError("integration input is invalid")
    source_id = envelope.get("id")
    if not isinstance(source_id, str):
        raise RuntimeError("server envelope has no source ID")

    home = Path(os.environ["HERMES_HOME"]).resolve()
    state_path = Path(os.environ["MUPOT_PLUGIN_STATE_PATH"])
    client = CaptureClient(consumer_status, attempt_result, attempt_ack)
    activations: list[tuple[str, dict[str, object]]] = []
    peer_turns: list[str] = []

    def activate_gateway(**kwargs: object) -> bool:
        content = kwargs.get("content")
        if not isinstance(content, str):
            raise AssertionError("native activation has no content")
        activations.append((content, kwargs))
        return True

    manager = plugins.PluginManager(scope_key=str(home))
    manager._scan_entry_points = lambda: []
    activation_owner = object()
    manager.set_gateway_message_injector(activation_owner, activate_gateway)
    manager.discover_and_load()
    loaded = manager._plugins.get("mupot")
    if loaded is None or not loaded.enabled or loaded.error is not None:
        raise RuntimeError("native Mupot profile could not be loaded")

    adapter = platform_registry.create_adapter(
        "mupot",
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "kasra",
                "routine_events_enabled": True,
                "state_path": str(state_path),
                "notification_recipients": {"telegram": "owner"},
            },
        ),
    )
    if adapter is None:
        raise RuntimeError("native Mupot adapter could not be created")
    profile_agent_id = getattr(adapter, "expected_agent_id", None)
    if not isinstance(profile_agent_id, str) or not profile_agent_id:
        raise RuntimeError("native Mupot profile has no configured agent ID")

    if profile_agent_id != assigned_agent_id:
        manager.unload("mupot")
        print(json.dumps({
            "ack_attempt_ids": [],
            "activation_count": 0,
            "custody_recorded": False,
            "outcome": "assignment_mismatch",
            "peer_turn_count": 0,
            "profile_agent_id": profile_agent_id,
            "source_id": source_id,
        }, separators=(",", ":"), sort_keys=True))
        return

    adapter_module = importlib.import_module(
        f"{loaded.module.__name__}.mupot_gateway.adapter"
    )
    adapter_module.StateStore(state_path).save({
        "lease_reconciliation": {
            "version": 3,
            "required": True,
            "tenant": consumer_status["tenant"],
            "agent_id": consumer_status["agent_id"],
            "effective_inbox_seat": consumer_status["effective_inbox_seat"],
            "mode": consumer_status["mode"],
            "generation": consumer_status["generation"],
            "attempt_id": attempt_id,
        }
    })
    adapter = platform_registry.create_adapter(
        "mupot",
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "kasra",
                "routine_events_enabled": True,
                "state_path": str(state_path),
                "notification_recipients": {"telegram": "owner"},
            },
        ),
    )
    if adapter is None:
        raise RuntimeError("native Mupot adapter could not be reconstructed")
    adapter._client = client
    adapter._send_client = client
    notifications = importlib.import_module(
        f"{loaded.module.__name__}.mupot_gateway.notifications"
    )
    setattr(notifications, "active_sessions", lambda: [
        {
            "id": "human-session",
            "session_key": "agent:main:telegram:dm:123",
            "source": "telegram",
            "user_id": "owner",
            "chat_id": "123",
            "chat_type": "dm",
            "last_active": 1,
        }
    ])

    async def peer_handler(event: object) -> None:
        peer_turns.append(str(event))

    adapter.set_message_handler(peer_handler)
    if not await adapter.reconcile_inbox_polling():
        raise RuntimeError("native Mupot attempt reconciliation failed")
    await adapter._flush_notifications()
    await adapter._flush_notifications()

    state = adapter.store.load()
    notice = state["notification_outbox"][source_id]
    ack_attempt_ids: list[str] = []
    for tool, arguments in client.calls:
        acknowledged_attempt = arguments.get("attempt_id")
        if tool == "inbox_lease_ack" and isinstance(acknowledged_attempt, str):
            ack_attempt_ids.append(acknowledged_attempt)
    manager.unload("mupot")
    print(json.dumps({
        "ack_attempt_ids": ack_attempt_ids,
        "activation_count": len(activations),
        "activation_status": notice["activation_status"],
        "delivery_status": notice["delivery_status"],
        "peer_turn_count": len(peer_turns),
        "send_count": sum(tool == "send" for tool, _arguments in client.calls),
        "processed": state["processed"],
        "profile_agent_id": profile_agent_id,
        "source_id": source_id,
    }, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    asyncio.run(_main())
