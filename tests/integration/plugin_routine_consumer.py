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


async def _main() -> None:
    _load_runtime()
    from gateway.config import PlatformConfig
    from gateway.platform_registry import platform_registry
    from hermes_cli import plugins

    payload = json.load(sys.stdin)
    if not isinstance(payload, dict):
        raise RuntimeError("integration input is invalid")
    assigned_agent_id = payload.get("assigned_agent_id")
    source_id = payload.get("source_id")
    if not isinstance(assigned_agent_id, str) or not isinstance(source_id, str):
        raise RuntimeError("integration input is invalid")

    home = Path(os.environ["HERMES_HOME"]).resolve()
    state_path = Path(os.environ["MUPOT_PLUGIN_STATE_PATH"])
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
            "activation_count": 0,
            "custody_recorded": False,
            "outcome": "assignment_mismatch",
            "peer_turn_count": 0,
            "profile_agent_id": profile_agent_id,
            "source_id": source_id,
        }, separators=(",", ":"), sort_keys=True))
        return

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
    if not await adapter.connect():
        raise RuntimeError("native Mupot adapter could not connect")
    try:
        for _ in range(500):
            if source_id in adapter.store.load().get("processed", []):
                break
            if adapter._fatal_error_code is not None:
                raise RuntimeError("native Mupot poller entered fatal state")
            await asyncio.sleep(0.01)
        else:
            raise RuntimeError("native Mupot adapter did not consume source")
        await adapter._flush_notifications()
        await adapter._flush_notifications()
    finally:
        await adapter.disconnect()

    state = adapter.store.load()
    notice = state["notification_outbox"][source_id]
    routine_receipt = state["routine_event_receipts"][source_id]
    manager.unload("mupot")
    print(json.dumps({
        "ack_ownership": routine_receipt["ack_ownership"],
        "activation_count": len(activations),
        "activation_status": notice["activation_status"],
        "delivery_status": notice["delivery_status"],
        "peer_turn_count": len(peer_turns),
        "processed": state["processed"],
        "profile_agent_id": profile_agent_id,
        "source_id": source_id,
    }, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    asyncio.run(_main())
