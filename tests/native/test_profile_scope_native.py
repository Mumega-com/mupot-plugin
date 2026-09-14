"""Pinned-Hermes integration tests for profile-scoped Mupot credentials."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, AsyncIterator

import json

import pytest

from agent.secret_scope import (
    reset_secret_scope,
    set_multiplex_active,
    set_secret_scope,
)
from gateway.config import PlatformConfig

from plugin.mupot_gateway.adapter import HermesMCPClient, MupotAdapter
from plugin.telegram_control import TelegramControlSettings, relay_telegram_update


class Response:
    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, _size: int = -1) -> bytes:
        return b'{"ok":true,"reply":"Ready."}'


class Opener:
    def __init__(self) -> None:
        self.requests: list[Any] = []

    def open(self, request: Any, timeout: float) -> Response:
        self.requests.append((request, timeout))
        return Response()


def update(update_id: int = 1) -> Any:
    message = SimpleNamespace(text="/needs")
    return SimpleNamespace(
        update_id=update_id,
        effective_user=SimpleNamespace(id=123, full_name="Ada"),
        effective_chat=SimpleNamespace(id=123, type="private"),
        effective_message=message,
    )


def test_telegram_relay_uses_real_hermes_scope_and_observes_rotation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugin import telegram_control

    global_secret = "wrong-global-webhook-secret"
    secrets = {"TEST_IM_WEBHOOK_SECRET": "first-scoped-webhook-secret"}
    monkeypatch.setenv("TEST_IM_WEBHOOK_SECRET", global_secret)
    opener = Opener()
    monkeypatch.setattr(telegram_control, "build_opener", lambda *_: opener)
    settings = TelegramControlSettings(
        enabled=True,
        base_url="https://pot.example.invalid",
        webhook_secret_env="TEST_IM_WEBHOOK_SECRET",
    )
    token = set_secret_scope(secrets)
    try:
        assert relay_telegram_update(settings, update()) == "Ready."
        secrets["TEST_IM_WEBHOOK_SECRET"] = "rotated-scoped-webhook-secret"
        assert relay_telegram_update(settings, update(2)) == "Ready."
    finally:
        reset_secret_scope(token)

    headers = [
        {key.lower(): value for key, value in request.header_items()}
        for request, _timeout in opener.requests
    ]
    assert [entry["x-telegram-bot-api-secret-token"] for entry in headers] == [
        "first-scoped-webhook-secret",
        "rotated-scoped-webhook-secret",
    ]
    assert global_secret not in str(headers)


def test_telegram_scoped_miss_refuses_global_secret_before_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugin import telegram_control

    global_secret = "must-not-cross-profile-boundary"
    monkeypatch.setenv("TEST_IM_WEBHOOK_SECRET", global_secret)
    opener = Opener()
    monkeypatch.setattr(telegram_control, "build_opener", lambda *_: opener)
    settings = TelegramControlSettings(
        enabled=True,
        base_url="https://pot.example.invalid",
        webhook_secret_env="TEST_IM_WEBHOOK_SECRET",
    )
    token = set_secret_scope({})
    try:
        with pytest.raises(RuntimeError) as failure:
            relay_telegram_update(settings, update())
    finally:
        reset_secret_scope(token)

    assert str(failure.value) == "profile secret is unavailable"
    assert global_secret not in str(failure.value)
    assert opener.requests == []


class JsonRpcResponse:
    def __init__(self, payload: Any | None = None) -> None:
        self.payload = payload if payload is not None else {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"structuredContent": {"ok": True}},
        }
        self.headers = {"content-type": "application/json"}
        self.status_code = 200

    def json(self) -> Any:
        return self.payload

    async def aiter_raw(self) -> AsyncIterator[bytes]:
        yield json.dumps(self.payload).encode("utf-8")


class StreamContext:
    def __init__(self, response: JsonRpcResponse) -> None:
        self.response = response

    async def __aenter__(self) -> JsonRpcResponse:
        return self.response

    async def __aexit__(self, *_args: object) -> None:
        return None


class HttpClient:
    def __init__(self, *, headers: dict[str, str], timeout: float) -> None:
        self.headers = headers
        self.timeout = timeout
        self.is_closed = False
        self.calls: list[dict[str, Any]] = []

    async def post(
        self,
        url: str,
        *,
        json: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> JsonRpcResponse:
        self.calls.append(
            {"url": url, "json": json, "headers": headers or self.headers}
        )
        return JsonRpcResponse()

    def stream(
        self,
        method: str,
        url: str,
        *,
        json: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> StreamContext:
        self.calls.append(
            {"method": method, "url": url, "json": json, "headers": headers or self.headers}
        )
        response = JsonRpcResponse()
        response.payload["id"] = json["id"]
        return StreamContext(response)

    async def aclose(self) -> None:
        self.is_closed = True


def install_mcp_config(monkeypatch: pytest.MonkeyPatch) -> None:
    from hermes_cli import config as config_module
    from hermes_cli import mcp_config

    raw_config = {
        "mcp_servers": {
            "mupot": {
                "url": "https://pot.example.invalid/mcp",
                "headers": {},
                "timeout": 5,
            }
        }
    }
    monkeypatch.setattr(config_module, "load_config", lambda: raw_config)
    monkeypatch.setattr(mcp_config, "_resolve_mcp_server_config", lambda value: value)


@pytest.mark.asyncio
async def test_mupot_client_without_scope_refuses_global_token_before_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugin.mupot_gateway import adapter as adapter_module

    install_mcp_config(monkeypatch)
    network_calls: list[object] = []

    def client_factory(**kwargs: Any) -> HttpClient:
        network_calls.append(kwargs)
        return HttpClient(**kwargs)

    monkeypatch.setattr(adapter_module.httpx, "AsyncClient", client_factory)
    monkeypatch.setenv("MUPOT_AGENT_TOKEN", "must-not-reach-mcp-request")
    token = set_secret_scope(None)
    try:
        with pytest.raises(RuntimeError) as failure:
            await HermesMCPClient("mupot").call("status", {})
    finally:
        reset_secret_scope(token)

    assert str(failure.value) == "profile secret is unavailable"
    assert network_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"jsonrpc": "2.0", "id": 1, "error": {"message": "reflected-secret"}},
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "isError": True,
                "content": [{"type": "text", "text": "reflected-secret"}],
            },
        },
        "reflected-secret",
    ],
)
async def test_mupot_client_never_propagates_server_controlled_error_detail(
    monkeypatch: pytest.MonkeyPatch,
    payload: Any,
) -> None:
    from plugin.mupot_gateway import adapter as adapter_module

    install_mcp_config(monkeypatch)

    class ErrorHttpClient(HttpClient):
        def stream(
            self,
            method: str,
            url: str,
            *,
            json: dict[str, Any],
            headers: dict[str, str] | None = None,
        ) -> StreamContext:
            self.calls.append(
                {"method": method, "url": url, "json": json, "headers": headers or self.headers}
            )
            return StreamContext(JsonRpcResponse(payload))

    monkeypatch.setattr(adapter_module.httpx, "AsyncClient", ErrorHttpClient)
    token = set_secret_scope({"MUPOT_AGENT_TOKEN": "profile-agent-token"})
    try:
        with pytest.raises(RuntimeError) as failure:
            await HermesMCPClient("mupot").call("status", {})
    finally:
        reset_secret_scope(token)

    assert str(failure.value) == "Mupot MCP request failed"
    assert "reflected-secret" not in str(failure.value)


@pytest.mark.asyncio
async def test_mupot_client_reads_scoped_token_for_every_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from hermes_cli import config as config_module
    from hermes_cli import mcp_config
    from plugin.mupot_gateway import adapter as adapter_module

    raw_config = {
        "mcp_servers": {
            "mupot": {
                "url": "https://pot.example.invalid/mcp",
                "headers": {
                    "authorization": "Bearer stale-or-global-agent-token",
                    "X-Transport": "native-test",
                },
                "timeout": 5,
            }
        }
    }
    monkeypatch.setattr(config_module, "load_config", lambda: raw_config)
    monkeypatch.setattr(mcp_config, "_resolve_mcp_server_config", lambda value: value)
    clients: list[HttpClient] = []

    def client_factory(**kwargs: Any) -> HttpClient:
        client = HttpClient(**kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr(adapter_module.httpx, "AsyncClient", client_factory)
    monkeypatch.setattr(adapter_module, "get_hermes_home", lambda: tmp_path)
    monkeypatch.setenv("MUPOT_AGENT_TOKEN", "wrong-global-agent-token")
    secrets = {"MUPOT_AGENT_TOKEN": "first-scoped-agent-token"}
    token = set_secret_scope(secrets)
    try:
        client = HermesMCPClient("mupot")
        assert await client.call("status", {}) == {"ok": True}
        secrets["MUPOT_AGENT_TOKEN"] = "rotated-scoped-agent-token"
        assert await client.call("status", {}) == {"ok": True}
    finally:
        reset_secret_scope(token)

    assert [call["headers"]["Authorization"] for call in clients[0].calls] == [
        "Bearer first-scoped-agent-token",
        "Bearer rotated-scoped-agent-token",
    ]
    assert all(
        [name.lower() for name in call["headers"]].count("authorization") == 1
        for call in clients[0].calls
    )
    assert "stale-or-global-agent-token" not in str(clients[0].calls)


class ConnectProbe:
    def __init__(self) -> None:
        self.connect_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_multiplex_activated_after_adapter_registration_refuses_connect(
    tmp_path: Path,
) -> None:
    probes: list[ConnectProbe] = []

    def client_factory(_server_name: str) -> ConnectProbe:
        probe = ConnectProbe()
        probes.append(probe)
        return probe

    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"state_path": str(tmp_path / "state.json")},
        ),
        client_factory=client_factory,
    )
    set_multiplex_active(True)
    try:
        assert await adapter.connect() is False
    finally:
        set_multiplex_active(False)

    assert [probe.connect_calls for probe in probes] == [0, 0]
