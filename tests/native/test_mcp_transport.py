"""Hostile native MCP transport and concrete send-receipt contract tests."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from agent.secret_scope import reset_secret_scope, set_secret_scope
from gateway.config import PlatformConfig

from plugin.mupot_gateway.adapter import HermesMCPClient, MupotAdapter


GENERIC_PROTOCOL_ERROR = "Mupot MCP request failed"
SENSITIVE_RESPONSE_LIMIT = 64 * 1024
ABSOLUTE_RESPONSE_LIMIT = 1024 * 1024
ATTEMPT_SCOPE = {
    "tenant": "tenant-test",
    "agent_id": "agent-test",
    "effective_inbox_seat": "seat-test",
}


def tool_result(
    value: Any,
    *,
    request_id: Any = 1,
    tool: str = "status",
    wrapper: str = "structured",
) -> dict[str, Any]:
    text = {
        "type": "text",
        "text": json.dumps(
            {"ok": True, "tool": tool, "result": value},
            separators=(",", ":"),
        ),
    }
    if wrapper == "structured":
        result = {"structuredContent": value}
    elif wrapper == "text":
        result = {"content": [text]}
    elif wrapper == "both":
        result = {"content": [text], "structuredContent": value}
    else:
        raise AssertionError(f"unknown wrapper fixture: {wrapper}")
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


class StreamingResponse:
    def __init__(
        self,
        payload: Any = None,
        *,
        raw: bytes | None = None,
        headers: dict[str, str] | None = None,
        chunks: list[bytes] | None = None,
        status_code: int = 200,
    ) -> None:
        self._raw = raw if raw is not None else json.dumps(payload).encode("utf-8")
        self._chunks = chunks if chunks is not None else [self._raw]
        self.headers = {"content-type": "application/json", **(headers or {})}
        self.status_code = status_code
        self.bytes_read = 0

    def json(self) -> Any:
        return json.loads(self._raw)

    async def aiter_raw(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            self.bytes_read += len(chunk)
            yield chunk


class StreamContext:
    def __init__(
        self,
        response: StreamingResponse,
        error: Exception | None = None,
    ) -> None:
        self.response = response
        self.error = error

    async def __aenter__(self) -> StreamingResponse:
        if self.error is not None:
            raise self.error
        return self.response

    async def __aexit__(self, *_args: object) -> None:
        return None


class HttpClient:
    def __init__(
        self,
        response: StreamingResponse,
        calls: list[dict[str, Any]],
        *,
        error: Exception | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> None:
        self.response = response
        self.calls = calls
        self.error = error
        self.headers = headers or {}
        self.timeout = timeout
        self.is_closed = False

    async def post(
        self,
        url: str,
        *,
        json: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> StreamingResponse:
        self.calls.append({"url": url, "json": json, "headers": headers or {}})
        if self.error is not None:
            raise self.error
        return self.response

    def stream(
        self,
        method: str,
        url: str,
        *,
        json: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> StreamContext:
        self.calls.append(
            {"method": method, "url": url, "json": json, "headers": headers or {}}
        )
        return StreamContext(self.response, self.error)

    async def aclose(self) -> None:
        self.is_closed = True


def install_transport(
    monkeypatch: pytest.MonkeyPatch,
    response: StreamingResponse,
    *,
    error: Exception | None = None,
) -> list[dict[str, Any]]:
    from hermes_cli import config as config_module
    from hermes_cli import mcp_config
    from plugin.mupot_gateway import adapter as adapter_module

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
    calls: list[dict[str, Any]] = []

    def client_factory(**kwargs: Any) -> HttpClient:
        return HttpClient(response, calls, error=error, **kwargs)

    monkeypatch.setattr(adapter_module.httpx, "AsyncClient", client_factory)
    return calls


async def call_native(
    monkeypatch: pytest.MonkeyPatch,
    response: StreamingResponse,
    *,
    tool: str = "status",
    arguments: dict[str, Any] | None = None,
    error: Exception | None = None,
) -> tuple[Any, list[dict[str, Any]]]:
    calls = install_transport(monkeypatch, response, error=error)
    scope = set_secret_scope({"MUPOT_AGENT_TOKEN": "native-test-token"})
    try:
        value = await HermesMCPClient("mupot").call(tool, arguments or {})
    finally:
        reset_secret_scope(scope)
    return value, calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        tool_result({"ok": True}, request_id=2),
        {"jsonrpc": "2.0", "result": {"structuredContent": {"ok": True}}},
        tool_result({"ok": True}, request_id=True),
        {"jsonrpc": "1.0", "id": 1, "result": {"structuredContent": {"ok": True}}},
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"structuredContent": {"ok": True}},
            "error": {"message": "reflected-secret"},
        },
        {"jsonrpc": "2.0", "id": 1},
    ],
)
async def test_rpc_envelope_requires_exact_integer_correlation_and_one_outcome(
    monkeypatch: pytest.MonkeyPatch,
    payload: Any,
) -> None:
    response = StreamingResponse(payload)
    with pytest.raises(RuntimeError) as failure:
        await call_native(monkeypatch, response)

    assert str(failure.value) == GENERIC_PROTOCOL_ERROR
    assert "reflected-secret" not in str(failure.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        {},
        {"isError": True, "content": [{"type": "text", "text": "reflected-secret"}]},
        {"isError": "false", "structuredContent": {"ok": True}},
        {"content": "not-a-content-array"},
        {"content": [{"type": "image", "data": "reflected-secret"}]},
        {"content": [{"type": "text", "text": "not-json"}]},
        {
            "content": [
                {"type": "text", "text": '{"ok":true,"tool":"status","result":{}}'},
                {"type": "text", "text": '{"ok":true,"tool":"status","result":{}}'},
            ]
        },
        {
            "content": [
                {
                    "type": "text",
                    "text": '{"ok":true,"tool":"other","result":{"ok":true}}',
                }
            ]
        },
    ],
)
async def test_tool_result_rejects_empty_error_and_malformed_wrapper_shapes(
    monkeypatch: pytest.MonkeyPatch,
    result: Any,
) -> None:
    response = StreamingResponse({"jsonrpc": "2.0", "id": 1, "result": result})
    with pytest.raises(RuntimeError) as failure:
        await call_native(monkeypatch, response)

    assert str(failure.value) == GENERIC_PROTOCOL_ERROR
    assert "reflected-secret" not in str(failure.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("wrapper", ["text", "structured", "both"])
async def test_tool_result_accepts_real_server_wrapper_forms(
    monkeypatch: pytest.MonkeyPatch,
    wrapper: str,
) -> None:
    expected = {"tenant": "tenant-test", "bound_agent_id": "agent-test"}
    value, calls = await call_native(
        monkeypatch,
        StreamingResponse(tool_result(expected, wrapper=wrapper)),
    )

    assert value == expected
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_tool_result_rejects_conflicting_text_and_structured_wrappers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = tool_result({"source": "structured"}, wrapper="both")
    payload["result"]["content"][0]["text"] = (
        '{"ok":true,"tool":"status","result":{"source":"text"}}'
    )

    with pytest.raises(RuntimeError) as failure:
        await call_native(monkeypatch, StreamingResponse(payload))

    assert str(failure.value) == GENERIC_PROTOCOL_ERROR


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "raw_size", "declared_size"),
    [
        ("send", 100, SENSITIVE_RESPONSE_LIMIT + 1),
        ("inbox_ack", SENSITIVE_RESPONSE_LIMIT + 1, None),
        ("status", ABSOLUTE_RESPONSE_LIMIT + 1, None),
    ],
)
async def test_response_byte_ceilings_reject_declared_and_chunked_oversize_before_decode(
    monkeypatch: pytest.MonkeyPatch,
    tool: str,
    raw_size: int,
    declared_size: int | None,
) -> None:
    valid = json.dumps(tool_result({"ok": True}, tool=tool)).encode("utf-8")
    raw = valid + b" " * max(0, raw_size - len(valid))
    headers = {} if declared_size is None else {"content-length": str(declared_size)}
    response = StreamingResponse(
        raw=raw,
        headers=headers,
        chunks=[raw[:1024], raw[1024:]],
    )

    with pytest.raises(RuntimeError) as failure:
        await call_native(monkeypatch, response, tool=tool)

    assert str(failure.value) == GENERIC_PROTOCOL_ERROR
    if declared_size is not None:
        assert response.bytes_read == 0


@pytest.mark.asyncio
async def test_sensitive_response_accepts_valid_escaped_json_at_exact_byte_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = {"escaped": '\"', "padding": ""}
    payload = tool_result(value, tool="inbox_ack", wrapper="structured")
    base = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    value["padding"] = "x" * (SENSITIVE_RESPONSE_LIMIT - len(base))
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    assert len(raw) == SENSITIVE_RESPONSE_LIMIT

    result, _calls = await call_native(
        monkeypatch,
        StreamingResponse(
            raw=raw,
            headers={"content-length": str(SENSITIVE_RESPONSE_LIMIT)},
            chunks=[raw[:32767], raw[32767:]],
        ),
        tool="inbox_ack",
    )

    assert result == value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        {"content-type": "text/html"},
        {"content-type": ""},
        {"content-encoding": "gzip"},
        {"content-encoding": "br"},
        {"content-length": "not-an-integer"},
    ],
)
async def test_response_headers_reject_non_json_compression_and_invalid_lengths(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
) -> None:
    response = StreamingResponse(tool_result({"ok": True}), headers=headers)
    with pytest.raises(RuntimeError) as failure:
        await call_native(monkeypatch, response)

    assert str(failure.value) == GENERIC_PROTOCOL_ERROR
    assert response.bytes_read == 0


@pytest.mark.asyncio
async def test_ambiguous_inbox_lease_transport_is_not_repeated_inside_one_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = StreamingResponse(tool_result({"messages": []}, tool="inbox_lease"))
    calls = install_transport(
        monkeypatch,
        response,
        error=httpx.ReadTimeout("response boundary lost"),
    )
    scope = set_secret_scope({"MUPOT_AGENT_TOKEN": "native-test-token"})
    try:
        with pytest.raises(RuntimeError) as failure:
            await HermesMCPClient("mupot").call("inbox_lease", {"limit": 1})
    finally:
        reset_secret_scope(scope)

    assert str(failure.value) == "Mupot request failed"
    assert type(failure.value).__name__ == "MupotTransportError"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_connect_failure_is_the_only_explicit_safe_before_send_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = StreamingResponse(tool_result({"messages": []}, tool="inbox_lease"))
    calls = install_transport(
        monkeypatch,
        response,
        error=httpx.ConnectError("connection was never established"),
    )
    scope = set_secret_scope({"MUPOT_AGENT_TOKEN": "native-test-token"})
    try:
        with pytest.raises(RuntimeError) as failure:
            await HermesMCPClient("mupot").call("inbox_lease", {"limit": 1})
    finally:
        reset_secret_scope(scope)

    assert str(failure.value) == "Mupot request failed"
    assert type(failure.value).__name__ == "MupotSafeRetryError"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_real_httpx_wire_forces_identity_over_configured_compression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from hermes_cli import config as config_module
    from hermes_cli import mcp_config
    from plugin.mupot_gateway import adapter as adapter_module

    real_async_client = httpx.AsyncClient
    seen_headers: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(request.headers.get_list("accept-encoding"))
        return httpx.Response(
            200,
            stream=httpx.ByteStream(
                json.dumps(tool_result({"ok": True})).encode("utf-8")
            ),
            headers={"content-type": "application/json"},
        )

    transport = httpx.MockTransport(handler)

    def client_factory(**kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(transport=transport, **kwargs)

    raw_config = {
        "mcp_servers": {
            "mupot": {
                "url": "https://pot.example.invalid/mcp",
                "headers": {
                    "Accept-Encoding": "gzip",
                    "accept-encoding": "br",
                },
                "timeout": 5,
            }
        }
    }
    monkeypatch.setattr(config_module, "load_config", lambda: raw_config)
    monkeypatch.setattr(mcp_config, "_resolve_mcp_server_config", lambda value: value)
    monkeypatch.setattr(adapter_module.httpx, "AsyncClient", client_factory)
    scope = set_secret_scope({"MUPOT_AGENT_TOKEN": "native-test-token"})
    try:
        result = await HermesMCPClient("mupot").call("status", {})
    finally:
        reset_secret_scope(scope)

    assert result == {"ok": True}
    assert seen_headers == [["identity"]]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        {
            **ATTEMPT_SCOPE,
            "attempt_id": "other-attempt-id-1234",
            "state": "empty",
            "lease_expires_at": None,
            "messages": [],
            "consumed": False,
        },
        {
            **ATTEMPT_SCOPE,
            "attempt_id": "attempt-id-12345678",
            "state": "unknown",
            "lease_expires_at": None,
            "messages": [],
            "consumed": False,
        },
        {
            **ATTEMPT_SCOPE,
            "attempt_id": "attempt-id-12345678",
            "state": "cancelled",
            "lease_expires_at": None,
            "messages": [{}],
            "consumed": False,
        },
    ],
)
async def test_native_client_rejects_invalid_attempt_tool_result(
    monkeypatch: pytest.MonkeyPatch,
    result: dict[str, Any],
) -> None:
    payload = tool_result(
        result,
        tool="inbox_lease_reconcile",
        wrapper="structured",
    )
    with pytest.raises(RuntimeError) as failure:
        await call_native(
            monkeypatch,
            StreamingResponse(payload),
            tool="inbox_lease_reconcile",
            arguments={"attempt_id": "attempt-id-12345678"},
        )

    assert str(failure.value) == GENERIC_PROTOCOL_ERROR


@pytest.mark.asyncio
async def test_native_client_accepts_exact_scope_bound_attempt_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt_id = "attempt-id-12345678"
    receipt = {
        **ATTEMPT_SCOPE,
        "attempt_id": attempt_id,
        "state": "acked",
        "consumed": True,
    }
    result, _calls = await call_native(
        monkeypatch,
        StreamingResponse(tool_result(receipt, tool="inbox_lease_ack")),
        tool="inbox_lease_ack",
        arguments={"attempt_id": attempt_id},
    )

    assert result == receipt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt",
    [
        {
            **ATTEMPT_SCOPE,
            "attempt_id": "other-attempt-id-1234",
            "state": "acked",
            "consumed": True,
        },
        {
            **ATTEMPT_SCOPE,
            "attempt_id": "attempt-id-12345678",
            "state": "unknown",
            "consumed": False,
        },
        {
            **ATTEMPT_SCOPE,
            "attempt_id": "attempt-id-12345678",
            "state": "acked",
            "consumed": 1,
        },
        {"attempt_id": "attempt-id-12345678", "state": "acked", "consumed": True},
    ],
)
async def test_native_client_rejects_malformed_attempt_ack(
    monkeypatch: pytest.MonkeyPatch,
    receipt: dict[str, Any],
) -> None:
    with pytest.raises(RuntimeError) as failure:
        await call_native(
            monkeypatch,
            StreamingResponse(tool_result(receipt, tool="inbox_lease_ack")),
            tool="inbox_lease_ack",
            arguments={"attempt_id": "attempt-id-12345678"},
        )

    assert str(failure.value) == GENERIC_PROTOCOL_ERROR


class ReceiptClient:
    def __init__(self, receipt: Any) -> None:
        self.receipt = receipt

    async def call(self, tool: str, _arguments: dict[str, Any]) -> Any:
        assert tool == "send"
        return self.receipt


class DomainClient:
    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, _arguments: dict[str, Any]) -> Any:
        return self.responses[tool]


def concrete_receipt() -> dict[str, Any]:
    return {
        "id": "delivery-1",
        "seq": 17,
        "duplicate": False,
        "to": "agent-target",
        "project_id": "project-1",
        "target_seat": None,
    }


async def adapter_for_receipt(tmp_path: Path, receipt: Any) -> MupotAdapter:
    client = ReceiptClient(receipt)
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"state_path": str(tmp_path / "state.json")},
        ),
        client_factory=lambda _server: client,
    )
    event, _runtime = adapter._begin_delivery({
        "id": "source-1",
        "from_agent": "agent-target",
        "body": "request",
        "request_id": "request-1",
        "project_id": "project-1",
    })
    await adapter.on_processing_start(event)
    return adapter


@pytest.mark.asyncio
async def test_send_accepts_concrete_matching_server_receipt(tmp_path: Path) -> None:
    receipt = concrete_receipt()
    adapter = await adapter_for_receipt(tmp_path, receipt)
    result = await adapter.send("agent-target", "answer")

    assert result.success is True
    assert result.message_id == "delivery-1"
    assert result.raw_response == receipt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt",
    [
        {},
        {**concrete_receipt(), "id": ""},
        {**concrete_receipt(), "id": "   "},
        {**concrete_receipt(), "seq": 0},
        {**concrete_receipt(), "seq": True},
        {key: value for key, value in concrete_receipt().items() if key != "duplicate"},
        {**concrete_receipt(), "duplicate": 0},
        {**concrete_receipt(), "to": "other-agent"},
        {**concrete_receipt(), "project_id": "other-project"},
        {key: value for key, value in concrete_receipt().items() if key != "project_id"},
    ],
)
async def test_send_rejects_missing_invalid_or_misattributed_receipt_permanently(
    tmp_path: Path,
    receipt: dict[str, Any],
) -> None:
    adapter = await adapter_for_receipt(tmp_path, receipt)
    result = await adapter.send("agent-target", "answer")

    assert result.success is False
    assert result.message_id is None
    assert result.retryable is False
    assert result.error_kind == "unknown"
    assert result.error == GENERIC_PROTOCOL_ERROR
    assert "other-agent" not in result.error
    assert "other-project" not in result.error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fence",
    [
        {"mode": "bearer_only", "generation": 1},
        {"mode": "reflected-secret", "generation": 1, "key_matches": True},
    ],
)
async def test_consumer_status_domain_failure_is_permanent_and_secret_free(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    fence: dict[str, Any],
) -> None:
    client = DomainClient({"inbox_consumer_status": fence})
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda _server: client,
    )
    try:
        assert await adapter.connect() is False
    finally:
        await adapter.disconnect()

    assert "reflected-secret" not in caplog.text


@pytest.mark.asyncio
async def test_inbox_ack_domain_failure_is_permanent_and_secret_free(tmp_path: Path) -> None:
    client = DomainClient(
        {
            "inbox_ack": {
                "acked": [],
                "already_read": [],
                "refused": ["reflected-secret"],
            }
        }
    )
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda _server: client,
    )

    with pytest.raises(RuntimeError) as failure:
        await adapter._ack_expected("expected-delivery")

    assert str(failure.value) == GENERIC_PROTOCOL_ERROR
    assert "reflected-secret" not in str(failure.value)
