"""Consume one real leased Mupot envelope through the native plugin adapter."""
from __future__ import annotations

import asyncio
import importlib.util
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

    plugin_root = Path(os.environ["MUPOT_PLUGIN_SOURCE"]).resolve()
    spec = importlib.util.spec_from_file_location(
        "plugin",
        plugin_root / "__init__.py",
        submodule_search_locations=[str(plugin_root)],
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("plugin package could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules["plugin"] = module
    spec.loader.exec_module(module)


class CaptureClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def call(self, tool: str, arguments: dict[str, object]) -> dict[str, object]:
        self.calls.append((tool, arguments.copy()))
        if tool != "inbox_ack":
            raise AssertionError(f"Routine consumer called unexpected tool {tool}")
        ids = arguments.get("ids")
        if not isinstance(ids, list) or len(ids) != 1 or not isinstance(ids[0], str):
            raise AssertionError("Routine consumer emitted an invalid source ACK")
        return {"acked": ids, "already_read": [], "refused": []}


async def _main() -> None:
    _load_runtime()
    from gateway.config import PlatformConfig
    from plugin.mupot_gateway import notifications
    from plugin.mupot_gateway.adapter import MupotAdapter, StateStore

    envelope = json.load(sys.stdin)
    source_id = envelope.get("id")
    if not isinstance(source_id, str):
        raise RuntimeError("server envelope has no source ID")

    state_path = Path(os.environ["MUPOT_PLUGIN_STATE_PATH"])
    profile_agent_id = os.environ["MUPOT_PROFILE_AGENT_ID"]
    client = CaptureClient()
    activations: list[tuple[str, dict[str, object]]] = []
    peer_turns: list[str] = []

    def activate(content: str, **kwargs: object) -> bool:
        activations.append((content, kwargs))
        return True

    notifications.active_sessions = lambda: [
        {
            "id": "human-session",
            "session_key": "agent:main:telegram:dm:123",
            "source": "telegram",
            "user_id": "owner",
            "chat_id": "123",
            "chat_type": "dm",
            "last_active": 1,
        }
    ]
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "kasra",
                "routine_events_enabled": True,
                "state_path": str(state_path),
                "notification_recipients": {"telegram": "owner"},
            },
        ),
        client_factory=lambda *_args: client,
        message_injector=activate,
    )

    async def peer_handler(event: object) -> None:
        peer_turns.append(str(event))

    adapter.set_message_handler(peer_handler)
    await adapter._process_leased_message(envelope)
    await adapter._flush_notifications()
    await adapter._flush_notifications()

    state = StateStore(state_path).load()
    notice = state["notification_outbox"][source_id]
    ack_ids: list[str] = []
    for tool, arguments in client.calls:
        ids = arguments.get("ids")
        if tool == "inbox_ack" and isinstance(ids, list) and ids:
            first_id = ids[0]
            if isinstance(first_id, str):
                ack_ids.append(first_id)
    print(json.dumps({
        "ack_ids": ack_ids,
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
