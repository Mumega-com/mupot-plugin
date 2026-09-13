from __future__ import annotations

import asyncio
import httpx
import json
import logging
import os
from collections import deque
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
)
from gateway.session import SessionSource
from hermes_constants import get_hermes_home
from ..mupot_operator import validate_operator_identity
from ..profile_scope import read_profile_secret, require_supported_profile_runtime

logger = logging.getLogger(__name__)


_ABSOLUTE_MCP_RESPONSE_LIMIT = 1024 * 1024
_SENSITIVE_MCP_RESPONSE_LIMIT = 64 * 1024
_SENSITIVE_MCP_TOOLS = frozenset({"send", "inbox_ack", "inbox_consumer_status"})
_GENERIC_MCP_PROTOCOL_ERROR = "Mupot MCP request failed"
_GENERIC_MCP_TRANSPORT_ERROR = "Mupot request failed"


class MupotProtocolError(RuntimeError):
    """A permanent, detail-free refusal of an invalid MCP response."""


class MupotTransportError(RuntimeError):
    """A detail-free transport failure whose server outcome may be unknown."""


def _protocol_error() -> MupotProtocolError:
    return MupotProtocolError(_GENERIC_MCP_PROTOCOL_ERROR)


def _decode_text_wrapper(value: Any, tool: str) -> Any:
    if not isinstance(value, str):
        raise _protocol_error()
    try:
        wrapper = json.loads(value)
    except (json.JSONDecodeError, UnicodeError, TypeError):
        raise _protocol_error() from None
    if (
        not isinstance(wrapper, dict)
        or set(wrapper) != {"ok", "tool", "result"}
        or wrapper.get("ok") is not True
        or wrapper.get("tool") != tool
    ):
        raise _protocol_error()
    return wrapper["result"]


def decode_mcp_result(payload: Any, request_id: int, tool: str) -> dict[str, Any]:
    """Validate one correlated JSON-RPC tools/call result and unwrap its data."""
    if (
        not isinstance(payload, dict)
        or payload.get("jsonrpc") != "2.0"
        or type(payload.get("id")) is not int
        or payload.get("id") != request_id
    ):
        raise _protocol_error()

    has_result = "result" in payload
    has_error = "error" in payload
    if has_result == has_error or has_error:
        raise _protocol_error()

    result = payload.get("result")
    if not isinstance(result, dict):
        raise _protocol_error()
    if "isError" in result and result.get("isError") is not False:
        raise _protocol_error()

    has_content = "content" in result
    has_structured = "structuredContent" in result
    if not has_content and not has_structured:
        raise _protocol_error()

    text_value: Any = None
    if has_content:
        content = result.get("content")
        if not isinstance(content, list):
            raise _protocol_error()
        if len(content) == 0:
            if not has_structured:
                raise _protocol_error()
        elif len(content) == 1:
            item = content[0]
            if not isinstance(item, dict) or item.get("type") != "text":
                raise _protocol_error()
            text_value = _decode_text_wrapper(item.get("text"), tool)
        else:
            raise _protocol_error()

    structured_value = result.get("structuredContent") if has_structured else None
    if has_structured and not isinstance(structured_value, dict):
        raise _protocol_error()
    if text_value is not None and not isinstance(text_value, dict):
        raise _protocol_error()
    if text_value is not None and has_structured and text_value != structured_value:
        raise _protocol_error()

    value = structured_value if has_structured else text_value
    if not isinstance(value, dict):
        raise _protocol_error()
    return value


def validate_send_receipt(
    result: Any,
    to: str,
    project_id: Optional[str],
) -> dict[str, Any]:
    """Require proof that Mupot durably accepted the exact outbound message."""
    if not isinstance(result, dict):
        raise _protocol_error()
    delivery_id = result.get("id")
    sequence = result.get("seq")
    if not isinstance(delivery_id, str) or not delivery_id.strip():
        raise _protocol_error()
    if type(sequence) is not int or sequence <= 0:
        raise _protocol_error()
    if type(result.get("duplicate")) is not bool:
        raise _protocol_error()
    if result.get("to") != to:
        raise _protocol_error()
    if "project_id" not in result or result.get("project_id") != project_id:
        raise _protocol_error()
    return result


def _response_limit(tool: str) -> int:
    if tool in _SENSITIVE_MCP_TOOLS:
        return _SENSITIVE_MCP_RESPONSE_LIMIT
    return _ABSOLUTE_MCP_RESPONSE_LIMIT


async def _read_mcp_response(response: httpx.Response, tool: str) -> Any:
    limit = _response_limit(tool)
    content_type = str(response.headers.get("content-type") or "")
    media_type = content_type.split(";", 1)[0].strip().lower()
    if not (
        media_type == "application/json"
        or (media_type.startswith("application/") and media_type.endswith("+json"))
    ):
        raise _protocol_error()

    encoding = str(response.headers.get("content-encoding") or "").strip().lower()
    if encoding and any(value.strip() != "identity" for value in encoding.split(",")):
        raise _protocol_error()

    declared: Optional[int] = None
    raw_length = response.headers.get("content-length")
    if raw_length is not None:
        text_length = str(raw_length).strip()
        if not text_length.isascii() or not text_length.isdigit():
            raise _protocol_error()
        declared = int(text_length)
        if declared > limit:
            raise _protocol_error()

    if response.status_code < 200 or response.status_code >= 300:
        raise _protocol_error()

    chunks: list[bytes] = []
    total = 0
    async for chunk in response.aiter_raw():
        total += len(chunk)
        if total > limit:
            raise _protocol_error()
        chunks.append(chunk)
    if declared is not None and total != declared:
        raise _protocol_error()

    try:
        return json.loads(b"".join(chunks).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeError):
        raise _protocol_error() from None


def normalize_agent(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text.split(":", 1)[1].strip() if text.startswith("agent:") else text


def should_accept_message(
    message: dict[str, Any], allowed_agents: Iterable[str]
) -> bool:
    sender = normalize_agent(message.get("from_agent"))
    allowed = {normalize_agent(value) for value in allowed_agents if normalize_agent(value)}
    return bool(sender and sender in allowed and str(message.get("body") or "").strip())


def is_ack_envelope(message: dict[str, Any]) -> bool:
    return str(message.get("kind") or "").strip().lower() == "ack"


def is_terminal_ack(message: dict[str, Any]) -> bool:
    """Recognize a complete authenticated terminal ACK envelope."""
    return (
        is_ack_envelope(message)
        and message.get("expects_reply") is False
        and bool(str(message.get("id") or "").strip())
    )


def _platform_for_mupot() -> Platform:
    try:
        return Platform("mupot")
    except ValueError:
        return Platform.LOCAL


def build_mupot_event(
    message: dict[str, Any], platform: Optional[Platform] = None
) -> MessageEvent:
    sender = normalize_agent(message.get("from_agent"))
    project_id = str(message.get("project_id") or "").strip() or None
    message_id = str(message.get("id") or "")
    source = SessionSource(
        platform=platform or _platform_for_mupot(),
        chat_id=sender,
        chat_name=f"Mupot agent {sender}",
        chat_type="dm",
        user_id=sender,
        user_name=sender,
        # Project events share a governed project session. Unscoped control
        # messages receive an isolated session so an old/stalled DM turn
        # cannot redirect or absorb a new command.
        thread_id=project_id or message_id,
        scope_id="mupot",
        message_id=message_id,
    )
    return MessageEvent(
        text=str(message.get("body") or ""),
        message_type=MessageType.TEXT,
        source=source,
        raw_message=message,
        message_id=message_id,
        # Mupot input is externally supplied but already passed both sender
        # authorization fences. Mark it internal only for Hermes busy-routing:
        # queue it as a distinct turn instead of interrupting/steering another
        # command and emitting a misleading busy response as the correlated ACK.
        internal=True,
        metadata={
            "seq": message.get("seq"),
            "request_id": message.get("request_id"),
            "in_reply_to": message.get("in_reply_to"),
            "project_id": project_id,
            "mupot_message_id": message_id,
        },
    )


class StateStore:
    def __init__(self, path: Path):
        self.path = Path(path).expanduser()

    def load(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}

    def save(self, value: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(value, handle, ensure_ascii=False, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


class HermesMCPClient:
    def __init__(self, server_name: str):
        self.server_name = server_name
        self._client: Optional[httpx.AsyncClient] = None
        self._url: Optional[str] = None
        self._headers: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._request_id = 0

    async def _ensure_client_locked(self) -> None:
        require_supported_profile_runtime({})
        # Validate availability at native connect as well as at request time.
        # The value is deliberately not cached here; call() observes rotation.
        read_profile_secret("MUPOT_AGENT_TOKEN")
        if self._client is None or self._client.is_closed:
            from hermes_cli.config import load_config
            from hermes_cli.mcp_config import _resolve_mcp_server_config

            raw_cfg = (load_config().get("mcp_servers") or {}).get(self.server_name)
            if not isinstance(raw_cfg, dict):
                raise RuntimeError(f"MCP server {self.server_name!r} is not configured")
            cfg = _resolve_mcp_server_config(raw_cfg)
            self._url = cfg.get("url")
            if not self._url:
                raise RuntimeError(f"MCP server {self.server_name!r} missing url")
            self._headers = dict(cfg.get("headers") or {})
            # Authorization is always supplied per request from Hermes's active
            # profile scope. Never retain a resolved/global fallback header.
            self._headers = {
                name: value
                for name, value in self._headers.items()
                if name.lower() != "authorization"
            }
            timeout_sec = float(cfg.get("timeout") or 30.0)
            self._client = httpx.AsyncClient(
                headers=self._headers,
                timeout=timeout_sec,
            )

    async def connect(self) -> None:
        async with self._lock:
            await self._ensure_client_locked()

    async def call(self, tool: str, arguments: dict[str, Any]) -> Any:
        async with self._lock:
            await self._ensure_client_locked()
            self._request_id += 1
            req_id = self._request_id
            body = {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {
                    "name": tool,
                    "arguments": arguments,
                },
            }
            token = read_profile_secret("MUPOT_AGENT_TOKEN")
            headers = {**self._headers, "Authorization": f"Bearer {token}"}
            client = self._client
            url = self._url
            if client is None or url is None:
                raise MupotTransportError(_GENERIC_MCP_TRANSPORT_ERROR)
            try:
                async with client.stream(
                    "POST", url, json=body, headers=headers
                ) as response:
                    payload = await _read_mcp_response(response, tool)
            except MupotProtocolError:
                raise
            except Exception:
                logger.warning("[mupot] MCP request failed; resetting client")
                try:
                    await client.aclose()
                except Exception:
                    pass
                self._client = None
                raise MupotTransportError(_GENERIC_MCP_TRANSPORT_ERROR) from None

        return decode_mcp_result(payload, req_id, tool)

    async def close(self) -> None:
        async with self._lock:
            client, self._client = self._client, None
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass


class MupotAdapter(BasePlatformAdapter):
    supports_async_delivery = True
    interactive_resume = False

    def __init__(
        self,
        config: PlatformConfig,
        client_factory: Callable[[str], Any] = HermesMCPClient,
        message_injector: Optional[Callable[..., bool]] = None,
    ) -> None:
        super().__init__(config, _platform_for_mupot())
        extra = config.extra or {}
        allowed = extra.get("allowed_agents") or (
            "hadi-codex,hadi-codex-cli,kasra,hermes"
        )
        if isinstance(allowed, str):
            allowed = [item.strip() for item in allowed.split(",")]
        self.allowed_agents = {
            normalize_agent(item) for item in allowed if normalize_agent(item)
        }
        # Hermes performs a central gateway authorization check before the
        # adapter lifecycle runs. Keep that check and the Mupot sender policy
        # on one canonical allowlist so the two fences cannot drift.
        extra["allow_from"] = sorted(self.allowed_agents)
        self.server_name = str(extra.get("mcp_server") or "mupot")
        self.expected_agent_id = extra.get("expected_agent_id")
        self.expected_tenant = extra.get("expected_tenant")
        self.poll_interval = max(0.01, float(extra.get("poll_interval") or 2.0))
        self.rpc_timeout = max(5.0, float(extra.get("rpc_timeout") or 20.0))
        self.turn_timeout = max(10.0, float(extra.get("turn_timeout") or 300.0))
        requested_lease = float(extra.get("lease_seconds") or (self.turn_timeout + 60.0))
        self.lease_seconds = max(1, min(3600, int(requested_lease)))
        state_path = extra.get("state_path") or str(get_hermes_home() / "platforms" / "mupot" / "state.json")
        self.store = StateStore(Path(str(state_path)))
        loaded = self.store.load()
        self.notification_recipients = dict(extra.get("notification_recipients") or {})
        self.notification_activate = extra.get("notification_activate") is True
        self.message_injector = message_injector
        self._state: dict[str, Any] = {
            # Mupot owns retry timing through visibility leases. Never replay
            # a stale local in-flight record immediately after a crash.
            "pending": None,
            "processed": list(loaded.get("processed") or [])[-1000:],
            "dlq": list(loaded.get("dlq") or [])[-100:],
            "terminal_receipts": list(loaded.get("terminal_receipts") or [])[-100:],
            "notification_outbox": dict(loaded.get("notification_outbox") or {}),
        }
        # Keep durable inbox lease/ACK traffic isolated from outbound sends.
        # Cancelling a timed-out MCP send can close that SDK session; it must
        # never poison the authoritative consumer transport.
        self._client = client_factory(self.server_name)
        self._send_client = client_factory(self.server_name)
        self._poll_task: Optional[asyncio.Task] = None
        self._completion_event = asyncio.Event()
        self._completion_outcome: Optional[ProcessingOutcome] = None
        self._current_message: Optional[dict[str, Any]] = None

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        try:
            require_supported_profile_runtime({})
            if self._running:
                return True
            await self._client.connect()
            await self._send_client.connect()
            if self.expected_agent_id or self.expected_tenant:
                boot = await asyncio.wait_for(self._client.call("boot_context", {}), timeout=self.rpc_timeout)
                if (not self.expected_agent_id or not self.expected_tenant or not isinstance(boot, dict)
                        or validate_operator_identity(boot, expected_tenant=self.expected_tenant,
                                                      agent_id=self.expected_agent_id) is not None
                        or boot.get("channel") != "workspace"):
                    raise RuntimeError("Mupot native gateway identity or tenant mismatch")
            fence = await asyncio.wait_for(
                self._client.call("inbox_consumer_status", {}),
                timeout=self.rpc_timeout,
            )
            generation = fence.get("generation") if isinstance(fence, dict) else None
            if (
                not isinstance(fence, dict)
                or fence.get("key_matches") is not True
                or fence.get("mode") not in {"bearer_only", "gateway"}
                or type(generation) is not int
                or generation < 0
            ):
                raise _protocol_error()
            self._mark_connected()
            self._poll_task = asyncio.create_task(
                self._poll_loop(), name="hermes-mupot-inbox-poller"
            )
            logger.info(
                "[mupot] connected consumer_mode=%s allowed=%s",
                fence.get("mode"),
                sorted(self.allowed_agents),
            )
            return True
        except Exception as exc:
            logger.error("[mupot] connect failed: %s", exc, exc_info=True)
            self._mark_disconnected()
            try:
                await self._client.close()
            except Exception:
                pass
            try:
                await self._send_client.close()
            except Exception:
                pass
            return False

    async def disconnect(self) -> None:
        self._running = False
        task, self._poll_task = self._poll_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self.cancel_background_tasks()
        await self._client.close()
        if self._send_client is not self._client:
            await self._send_client.close()
        self._mark_disconnected()

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                await self._flush_notifications()
                payload = await asyncio.wait_for(
                    self._client.call(
                        "inbox_lease",
                        {"limit": 1, "lease_seconds": self.lease_seconds},
                    ),
                    timeout=self.rpc_timeout,
                )
                if not isinstance(payload, dict):
                    raise RuntimeError("Mupot inbox_lease returned an invalid payload")
                messages = payload.get("messages", [])
                if messages and isinstance(messages[0], dict):
                    message = messages[0]
                    message_id = str(message.get("id") or "")
                    logger.info(
                        "[mupot] leased message=%s seq=%s attempts=%s request_id=%s",
                        message_id,
                        message.get("seq"),
                        message.get("delivery_attempts"),
                        message.get("request_id"),
                    )
                    if message_id in self._state["processed"]:
                        await self._ack_expected(message_id)
                    elif is_ack_envelope(message) and should_accept_message(
                        message, self.allowed_agents
                    ):
                        await self._handle_ack_envelope(message)
                    elif should_accept_message(message, self.allowed_agents):
                        await self._deliver(message)
                    else:
                        self._state["dlq"].append(
                            {"message": message, "reason": "sender_policy"}
                        )
                        self._state["dlq"] = self._state["dlq"][-100:]
                        self.store.save(self._state)
                        await self._ack_expected(message_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[mupot] poll error: %s", exc, exc_info=True)
            await asyncio.sleep(self.poll_interval)

    async def _handle_ack_envelope(self, message: dict[str, Any]) -> None:
        """Quarantine incomplete ACKs; persist complete receipts before ACKing."""
        message_id = str(message.get("id") or "")
        if not is_terminal_ack(message):
            quarantined = self._state.get("dlq") or []
            quarantined.append({"message": dict(message), "reason": "invalid_ack_envelope"})
            self._state["dlq"] = quarantined[-100:]
            self.store.save(self._state)
            if message_id:
                await self._ack_expected(message_id)
                self._commit(message_id)
            return
        receipts = deque(self._state.get("terminal_receipts") or [], maxlen=100)
        if not any(str(receipt.get("id") or "") == message_id for receipt in receipts):
            receipts.append(dict(message))
        self._state["terminal_receipts"] = list(receipts)
        # A persistence error occurs before inbox_ack, leaving the lease
        # available for retry and preserving the source receipt.
        self.store.save(self._state)
        if self.notification_recipients:
            from .notifications import enqueue
            enqueue(self._state, self.store, message, str(message.get("body") or ""))
        await self._ack_expected(message_id)
        self._commit(message_id)

    async def _deliver(self, message: dict[str, Any]) -> None:
        message_id = str(message.get("id") or "")
        self._state["pending"] = {"message": message}
        self.store.save(self._state)
        self._current_message = message
        self._completion_event.clear()
        self._completion_outcome = None
        await self.handle_message(build_mupot_event(message, self.platform))
        try:
            await asyncio.wait_for(self._completion_event.wait(), self.turn_timeout)
        except asyncio.TimeoutError:
            self._completion_outcome = ProcessingOutcome.FAILURE
            logger.error("[mupot] turn timeout message=%s", message_id)
        if self._completion_outcome == ProcessingOutcome.SUCCESS:
            await self._ack_expected(message_id)
            self._commit(message_id)
            self._current_message = None
            return
        # Do not acknowledge failure and do not immediately replay locally.
        # The server-side visibility lease expires, retries safely, and moves
        # poison messages to Mupot's durable dead-letter state.
        self._state["pending"] = None
        self.store.save(self._state)
        self._current_message = None

    async def _ack_expected(self, expected_id: str) -> None:
        payload = await asyncio.wait_for(
            self._client.call("inbox_ack", {"ids": [expected_id]}),
            timeout=self.rpc_timeout,
        )
        if not isinstance(payload, dict):
            raise _protocol_error()
        categories: list[set[str]] = []
        for name in ("acked", "already_read", "refused"):
            values = payload.get(name)
            if (
                not isinstance(values, list)
                or any(not isinstance(value, str) or not value.strip() for value in values)
            ):
                raise _protocol_error()
            category = set(values)
            if category - {expected_id}:
                raise _protocol_error()
            categories.append(category)
        acked, already_read, refused = categories
        matched = sum(expected_id in category for category in categories)
        if expected_id in refused or matched != 1:
            raise _protocol_error()
        state = "acked" if expected_id in acked else "already_read"
        logger.info("[mupot] inbox_ack message=%s state=%s", expected_id, state)

    def _commit(self, message_id: str) -> None:
        processed = deque(self._state.get("processed") or [], maxlen=1000)
        if message_id not in processed:
            processed.append(message_id)
        self._state["processed"] = list(processed)
        self._state["pending"] = None
        self.store.save(self._state)

    async def on_processing_complete(
        self, event: MessageEvent, outcome: ProcessingOutcome
    ) -> None:
        event_id = str(event.message_id or "")
        current_id = str((self._current_message or {}).get("id") or "")
        if not current_id or event_id != current_id:
            logger.debug(
                "[mupot] ignoring lifecycle completion event=%s current=%s outcome=%s",
                event_id,
                current_id,
                outcome,
            )
            return
        logger.info(
            "[mupot] lifecycle complete message=%s outcome=%s",
            current_id,
            outcome,
        )
        self._completion_outcome = outcome
        self._completion_event.set()

    async def _flush_notifications(self) -> None:
        from .notifications import flush
        if self.notification_activate and self.message_injector is None:
            raise RuntimeError("Mupot notification activation has no native plugin injector")
        await flush(self._state, self.store, self.notification_recipients,
                    activate=self.message_injector if self.notification_activate else None)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        message = self._current_message or {}
        arguments: dict[str, Any] = {
            "to": normalize_agent(chat_id),
            "body": str(content),
        }
        project_id = message.get("project_id")
        request_id = message.get("request_id")
        inbound_id = message.get("id")
        if project_id:
            arguments["project_id"] = project_id
        if inbound_id:
            arguments["in_reply_to"] = inbound_id
            # Distinct sender idempotency key per inbound turn redelivery:
            arguments["request_id"] = f"resp-{inbound_id}"
        elif request_id:
            arguments["request_id"] = f"resp-{request_id}"
        try:
            result = await asyncio.wait_for(
                self._send_client.call("send", arguments),
                timeout=self.rpc_timeout,
            )
            receipt = validate_send_receipt(
                result,
                arguments["to"],
                arguments.get("project_id"),
            )
            if self.notification_recipients and inbound_id and not (metadata or {}).get("_interim_send"):
                from .notifications import enqueue
                enqueue(self._state, self.store, message, str(content))
            return SendResult(
                success=True,
                message_id=receipt["id"],
                raw_response=receipt,
            )
        except Exception as exc:
            permanent = isinstance(exc, MupotProtocolError)
            logger.warning(
                "[mupot] response send failed message=%s request_id=%s error=%s",
                inbound_id,
                arguments.get("request_id"),
                exc,
            )
            return SendResult(
                success=False,
                error=str(exc),
                retryable=not permanent,
                error_kind="unknown" if permanent else "transient",
            )

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        agent = normalize_agent(chat_id)
        return {"name": agent, "type": "dm", "chat_id": agent}


def _requirements_available() -> bool:
    try:
        import mcp  # noqa: F401
        return True
    except ImportError:
        return False


def _is_connected(config: PlatformConfig) -> bool:
    extra = config.extra or {}
    return bool(config.enabled and extra.get("mcp_server"))


def _apply_yaml_config(_yaml: dict, platform: dict) -> dict:
    return {
        key: value
        for key, value in platform.items()
        if key not in {
            "enabled",
            "home_channel",
            "reply_to_mode",
            "typing_indicator",
            "gateway_restart_notification",
        }
    }


def register(ctx, *, expected_agent_id=None, expected_tenant=None) -> None:
    def adapter_factory(config):
        extra = dict(config.extra or {})
        if expected_agent_id is not None or expected_tenant is not None:
            extra.update(expected_agent_id=expected_agent_id, expected_tenant=expected_tenant)
        return MupotAdapter(replace(config, extra=extra), message_injector=ctx.inject_message)

    ctx.register_platform(
        name="mupot",
        label="Mupot",
        adapter_factory=adapter_factory,
        check_fn=_requirements_available,
        is_connected=_is_connected,
        apply_yaml_config_fn=_apply_yaml_config,
        emoji="🪴",
        platform_hint=(
            "You are responding over Mupot as your authenticated agent identity. "
            "Respect project_id and task/flight governance. When request_id is "
            "present, begin the response with {ack_for:<request_id>}. Reply directly "
            "and include concrete evidence rather than acknowledgement loops."
        ),
    )
