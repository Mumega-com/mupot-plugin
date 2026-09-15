"""Real primary-profile startup and Telegram secret-scope integration."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from agent.secret_scope import set_multiplex_active
from gateway.config import PlatformConfig


PLUGIN_ROOT = Path(__file__).resolve().parents[2]
GLOBAL_AGENT_SECRET = "wrong-global-agent-secret"
GLOBAL_WEBHOOK_SECRET = "wrong-global-webhook-secret"
SCOPED_AGENT_SECRET = "owning-profile-agent-secret"
SCOPED_WEBHOOK_SECRET = "owning-profile-webhook-secret"
OTHER_PROFILE_SECRET = "other-profile-secret"


def write_secrets(
    home: Path,
    *,
    agent_secret: str | None = SCOPED_AGENT_SECRET,
    webhook_secret: str | None = SCOPED_WEBHOOK_SECRET,
) -> None:
    lines = []
    if agent_secret is not None:
        lines.append(f"MUPOT_AGENT_TOKEN={agent_secret}")
    if webhook_secret is not None:
        lines.append(f"TEST_IM_WEBHOOK_SECRET={webhook_secret}")
    (home / ".env").write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_profile_owner_fingerprint_is_stable_and_home_specific(tmp_path: Path) -> None:
    from plugin.profile_scope import ProfileSecretOwner

    first_home = tmp_path / "profile-a"
    second_home = tmp_path / "profile-b"
    first_home.mkdir()
    second_home.mkdir()
    first_stat = first_home.stat()
    second_stat = second_home.stat()
    first = ProfileSecretOwner(first_home.resolve(), first_stat.st_dev, first_stat.st_ino)
    same = ProfileSecretOwner(first_home.resolve(), first_stat.st_dev, first_stat.st_ino)
    second = ProfileSecretOwner(second_home.resolve(), second_stat.st_dev, second_stat.st_ino)

    assert first.fingerprint == same.fingerprint
    assert first.fingerprint != second.fingerprint
    assert len(first.fingerprint) == 64


def discover_plugin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from hermes_cli import plugins

    home = tmp_path / "hermes-home"
    plugin_dir = home / "plugins" / "mupot"
    plugin_dir.parent.mkdir(parents=True)
    plugin_dir.symlink_to(PLUGIN_ROOT, target_is_directory=True)
    (home / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "mcp_servers": {
                    "mupot": {
                        "url": "https://pot.example.invalid/mcp",
                        "headers": {
                            "Authorization": "Bearer ${MUPOT_AGENT_TOKEN}"
                        },
                        "timeout": 5,
                    }
                },
                "plugins": {
                    "enabled": ["mupot"],
                    "entries": {
                        "mupot": {
                            "settings": {
                                "mode": "operator",
                                "operator": {
                                    "base_url": "https://pot.example.invalid",
                                    "expected_tenant": "tenant-test",
                                    "squad_id": "squad-test",
                                    "agent_id": "agent-test",
                                    "approval_owner": "human-test",
                                    "native_gateway_enabled": True,
                                    "telegram_control_enabled": True,
                                    "telegram_control_webhook_secret_env": (
                                        "TEST_IM_WEBHOOK_SECRET"
                                    ),
                                },
                            }
                        }
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    write_secrets(home)
    empty_bundled = tmp_path / "empty-bundled"
    empty_bundled.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_BUNDLED_PLUGINS", str(empty_bundled))
    monkeypatch.setenv("MUPOT_AGENT_TOKEN", GLOBAL_AGENT_SECRET)
    monkeypatch.setenv("TEST_IM_WEBHOOK_SECRET", GLOBAL_WEBHOOK_SECRET)

    manager = plugins.PluginManager(scope_key=str(home.resolve()))
    monkeypatch.setattr(manager, "_scan_entry_points", lambda: [])
    manager.discover_and_load()
    loaded = manager._plugins["mupot"]
    assert loaded.enabled is True
    assert loaded.error is None
    return home, manager, loaded


class StreamingResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.raw = json.dumps(payload).encode("utf-8")
        self.headers = {"content-type": "application/json"}
        self.status_code = 200

    async def aiter_raw(self):
        yield self.raw


class StreamContext:
    def __init__(self, response: StreamingResponse) -> None:
        self.response = response

    async def __aenter__(self) -> StreamingResponse:
        return self.response

    async def __aexit__(self, *_args: object) -> None:
        return None


class NativeHttpClient:
    def __init__(
        self,
        calls: list[dict[str, Any]],
        *,
        headers: dict[str, str],
        timeout: float,
    ) -> None:
        self.calls = calls
        self.headers = headers
        self.timeout = timeout
        self.is_closed = False

    def stream(
        self,
        method: str,
        url: str,
        *,
        json: dict[str, Any],
        headers: dict[str, str],
    ) -> StreamContext:
        self.calls.append(
            {"method": method, "url": url, "json": json, "headers": headers}
        )
        tool = json["params"]["name"]
        if tool == "boot_context":
            value = {
                "tenant": "tenant-test",
                "bound_agent_id": "agent-test",
                "role": "member",
                "capabilities": [],
                "channel": "workspace",
            }
        elif tool == "inbox_consumer_status":
            value = {
                "strict_scope": True,
                "tenant": "tenant-test",
                "agent_id": "agent-test",
                "effective_inbox_seat": None,
                "mode": "bearer_only",
                "generation": 0,
                "key_matches": True,
            }
        else:
            raise AssertionError(f"unexpected MCP tool: {tool}")
        return StreamContext(
            StreamingResponse(
                {
                    "jsonrpc": "2.0",
                    "id": json["id"],
                    "result": {"structuredContent": value},
                }
            )
        )

    async def aclose(self) -> None:
        self.is_closed = True


def native_adapter(manager: Any, loaded: Any, tmp_path: Path, monkeypatch: Any):
    from gateway.platform_registry import platform_registry

    adapter_module = __import__(
        f"{loaded.module.__name__}.mupot_gateway.adapter",
        fromlist=["adapter"],
    )
    calls: list[dict[str, Any]] = []

    def client_factory(**kwargs: Any) -> NativeHttpClient:
        return NativeHttpClient(calls, **kwargs)

    monkeypatch.setattr(adapter_module.httpx, "AsyncClient", client_factory)
    entry = platform_registry.get("mupot")
    assert entry is not None
    adapter = entry.adapter_factory(
        PlatformConfig(
            enabled=True,
            extra={
                "mcp_server": "mupot",
                "state_path": str(tmp_path / "mupot-state.json"),
            },
        )
    )

    async def idle_poll() -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(adapter, "_poll_loop", idle_poll)
    return adapter, calls


async def initial_connect(adapter: Any) -> bool:
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._platform_lock_takeover_on_start = False
    runner._platform_connect_timeout_secs = lambda *_args, **_kwargs: 0.0
    return await runner._connect_initial_adapter_with_timeout(adapter, adapter.platform)


@pytest.mark.asyncio
async def test_primary_initial_connect_uses_owning_profile_scope_without_preinstall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _home, manager, loaded = discover_plugin(tmp_path, monkeypatch)
    adapter, calls = native_adapter(manager, loaded, tmp_path, monkeypatch)
    try:
        assert await initial_connect(adapter) is True
        assert [call["json"]["params"]["name"] for call in calls] == [
            "boot_context",
            "inbox_consumer_status",
        ]
        assert all(
            call["headers"]["Authorization"] == f"Bearer {SCOPED_AGENT_SECRET}"
            for call in calls
        )
        assert GLOBAL_AGENT_SECRET not in str(calls)
    finally:
        await adapter.disconnect()
        manager.unload("mupot")


@pytest.mark.asyncio
async def test_adapter_construction_does_not_leak_scoped_secret_into_ambient_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BLOCK-C (kasra re-gate round 3, 2026-09-15, head 7295e057): the
    constructor's own `mcp_tool_timeout` resolution
    (`_configured_mcp_tool_timeout` -> `hermes_cli.config.load_config` ->
    `hermes_cli.mcp_config._resolve_mcp_server_config`) used to run OUTSIDE
    any profile secret scope. `_resolve_mcp_server_config` itself falls back
    to `load_hermes_dotenv()` -- loading `.env` INTO `os.environ` -- exactly
    when `current_secret_scope() is None`, which is true again by
    construction time even for a real, correctly-threaded `secret_owner`:
    `__init__.py`'s own `with secret_owner.activate():` block (that wraps
    `register_native_gateway`) has already exited by the time Hermes's
    gateway runner lazily calls `adapter_factory(config)` and constructs
    `MupotAdapter` -- registration-time scoping does not cover construction-
    time reads.

    This scenario reuses `discover_plugin`'s exact setup: the ambient
    process env carries `MUPOT_AGENT_TOKEN=GLOBAL_AGENT_SECRET` (as any
    OTHER tenant's already-running process might, or a stale shell export),
    while THIS discovered profile's own `.env` carries a DIFFERENT,
    genuinely scoped `MUPOT_AGENT_TOKEN=SCOPED_AGENT_SECRET`. Before the
    fix, merely CONSTRUCTING the adapter (`native_adapter`, no `connect()`
    needed) overwrote the ambient `os.environ["MUPOT_AGENT_TOKEN"]` with the
    scoped secret -- a multiplex cross-tenant leak into shared process
    state that any other, unscoped code reading `os.environ` directly
    (rather than through `read_profile_secret`) would then see. Fixed by
    resolving `mcp_tool_timeout` inside `self._profile_scope()`, which
    activates `secret_owner` again for the duration of just this read --
    `current_secret_scope()` is then non-`None`, so
    `_resolve_mcp_server_config` never calls `load_hermes_dotenv()` at all.
    """
    _home, manager, loaded = discover_plugin(tmp_path, monkeypatch)
    assert os.environ.get("MUPOT_AGENT_TOKEN") == GLOBAL_AGENT_SECRET
    adapter, _calls = native_adapter(manager, loaded, tmp_path, monkeypatch)
    try:
        assert os.environ.get("MUPOT_AGENT_TOKEN") == GLOBAL_AGENT_SECRET, (
            "BLOCK-C: constructing the adapter must never overwrite the "
            "ambient os.environ with this profile's own scoped secret"
        )
    finally:
        await adapter.disconnect()
        manager.unload("mupot")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["missing_secret", "missing_home", "moved", "multiplex"]
)
async def test_primary_initial_connect_fails_closed_before_network_when_owner_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure: str,
) -> None:
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    home, manager, loaded = discover_plugin(tmp_path, monkeypatch)
    adapter, calls = native_adapter(manager, loaded, tmp_path, monkeypatch)
    stale_home_token = None
    if failure == "missing_secret":
        write_secrets(home, agent_secret=None)
    elif failure == "missing_home":
        home.rename(tmp_path / "moved-hermes-home")
    elif failure == "moved":
        other_home = tmp_path / "other-hermes-home"
        other_home.mkdir()
        write_secrets(other_home, agent_secret=OTHER_PROFILE_SECRET)
        monkeypatch.setenv("HERMES_HOME", str(other_home))
        stale_home_token = set_hermes_home_override(home)
    else:
        set_multiplex_active(True)
    try:
        assert await initial_connect(adapter) is False
        assert calls == []
        assert GLOBAL_AGENT_SECRET not in caplog.text
        assert SCOPED_AGENT_SECRET not in caplog.text
        assert OTHER_PROFILE_SECRET not in caplog.text
    finally:
        if stale_home_token is not None:
            reset_hermes_home_override(stale_home_token)
        set_multiplex_active(False)
        await adapter.disconnect()
        manager.unload("mupot")


class TelegramResponse:
    def __enter__(self) -> "TelegramResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _size: int = -1) -> bytes:
        return b'{"ok":true,"reply":"Ready."}'


class TelegramOpener:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, float]] = []

    def open(self, request: Any, timeout: float) -> TelegramResponse:
        self.calls.append((request, timeout))
        return TelegramResponse()


def wire_telegram_handler(manager: Any, loaded: Any, monkeypatch: Any):
    handlers: list[Any] = []
    factories = manager.get_telegram_handler_factories()
    assert len(factories) == 1
    factory, owner = factories[0]
    assert owner == "mupot"
    application = SimpleNamespace(
        add_handler=handlers.append,
        remove_handler=lambda handler, group=0: handlers.remove(handler),
    )
    factory(application, object())
    assert handlers
    control_module = __import__(
        f"{loaded.module.__name__}.telegram_control",
        fromlist=["telegram_control"],
    )
    opener = TelegramOpener()
    monkeypatch.setattr(control_module, "build_opener", lambda *_: opener)
    return handlers[0], opener


def telegram_update(replies: list[str], update_id: int = 1) -> Any:
    async def reply_text(value: str) -> None:
        replies.append(value)

    message = SimpleNamespace(text="/needs", reply_text=reply_text)
    return SimpleNamespace(
        update_id=update_id,
        effective_user=SimpleNamespace(id=123, full_name="Ada"),
        effective_chat=SimpleNamespace(id=123, type="private"),
        effective_message=message,
    )


@pytest.mark.asyncio
async def test_primary_telegram_handler_uses_fresh_owning_profile_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home, manager, loaded = discover_plugin(tmp_path, monkeypatch)
    handler, opener = wire_telegram_handler(manager, loaded, monkeypatch)
    replies: list[str] = []
    try:
        await handler.callback(telegram_update(replies), object())
        write_secrets(home, webhook_secret="rotated-owning-profile-webhook-secret")
        await handler.callback(telegram_update(replies, 2), object())
    finally:
        manager.unload("mupot")

    headers = [
        {name.lower(): value for name, value in request.header_items()}
        for request, _timeout in opener.calls
    ]
    assert replies == ["Ready.", "Ready."]
    assert [entry["x-telegram-bot-api-secret-token"] for entry in headers] == [
        SCOPED_WEBHOOK_SECRET,
        "rotated-owning-profile-webhook-secret",
    ]
    assert GLOBAL_WEBHOOK_SECRET not in str(headers)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["missing_secret", "missing_home", "moved", "multiplex"]
)
async def test_primary_telegram_handler_fails_closed_before_network_when_owner_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure: str,
) -> None:
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    home, manager, loaded = discover_plugin(tmp_path, monkeypatch)
    handler, opener = wire_telegram_handler(manager, loaded, monkeypatch)
    stale_home_token = None
    if failure == "missing_secret":
        write_secrets(home, webhook_secret=None)
    elif failure == "missing_home":
        home.rename(tmp_path / "moved-hermes-home")
    elif failure == "moved":
        other_home = tmp_path / "other-hermes-home"
        other_home.mkdir()
        write_secrets(other_home, webhook_secret=OTHER_PROFILE_SECRET)
        monkeypatch.setenv("HERMES_HOME", str(other_home))
        stale_home_token = set_hermes_home_override(home)
    else:
        set_multiplex_active(True)
    replies: list[str] = []
    try:
        await handler.callback(telegram_update(replies), object())
        assert replies == ["Mupot project control is temporarily unavailable."]
        assert opener.calls == []
        assert GLOBAL_WEBHOOK_SECRET not in caplog.text
        assert SCOPED_WEBHOOK_SECRET not in caplog.text
        assert OTHER_PROFILE_SECRET not in caplog.text
    finally:
        if stale_home_token is not None:
            reset_hermes_home_override(stale_home_token)
        set_multiplex_active(False)
        manager.unload("mupot")
