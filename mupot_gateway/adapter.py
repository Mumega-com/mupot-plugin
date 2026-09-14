from __future__ import annotations

import asyncio
import copy
import hashlib
import httpx
import json
import logging
import math
import os
import re
import secrets
import time
from collections import deque
from contextlib import nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional

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
from ..profile_scope import (
    ProfileSecretOwner,
    read_profile_secret,
    require_supported_profile_runtime,
)
from .lease_ownership import (
    ATTEMPT_ID_RE as _LEASE_ATTEMPT_ID_RE,
    AckOwnershipError,
    attempt_ack_ownership,
    legacy_ack_ownership,
    validate_ack_ownership,
)

logger = logging.getLogger(__name__)


_ABSOLUTE_MCP_RESPONSE_LIMIT = 1024 * 1024
_SENSITIVE_MCP_RESPONSE_LIMIT = 64 * 1024
_SENSITIVE_MCP_TOOLS = frozenset({"send", "inbox_ack", "inbox_consumer_status"})
_GENERIC_MCP_PROTOCOL_ERROR = "Mupot MCP request failed"
_GENERIC_MCP_TRANSPORT_ERROR = "Mupot request failed"
_GENERIC_DELIVERY_CONTEXT_ERROR = "Mupot delivery context unavailable"
_DELIVERY_CONTEXT_METADATA_KEY = "_mupot_delivery_context"
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_REPLY_OUTBOX_VERSION = 2
_LEGACY_REPLY_OUTBOX_VERSION = 1
_LEASE_ATTEMPT_MARKER_VERSION = 3
# _LEASE_ATTEMPT_ID_RE itself is imported above from .lease_ownership (single shared
# pattern -- see the comment there) rather than redefined here.
_PROFILE_OWNER_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")
_STRANDED_NOTIFICATION_STATUSES = frozenset({"activation_unknown", "transport_unknown"})
_LEASE_ATTEMPT_STATES = frozenset(
    {"leased", "empty", "cancelled", "expired", "acked"}
)
_IMMUTABLE_REPLY_SOURCE_FIELDS = (
    "id",
    "seq",
    "tenant",
    "to_agent",
    "target_seat",
    "from_agent",
    "from_member",
    "kind",
    "body",
    "request_id",
    "in_reply_to",
    "created_at",
    "project_id",
    "fenced_delivery_id",
    "body_length",
    "checksum_sha256",
    "expects_reply",
    "reply_basis",
)


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    """Immutable authority for one leased message processing generation."""

    source_id: str
    generation: int
    sender: str
    project: Optional[str]
    request_id: Optional[str]
    session_key: str
    attempt_id: Optional[str]


@dataclass(slots=True)
class _LiveDelivery:
    context: DeliveryContext
    expires_at: float
    source: Mapping[str, Any]
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)
    outcome: Optional[ProcessingOutcome] = None
    invalidated: bool = False
    cancellation_started: bool = False


_delivery_context: ContextVar[Optional[DeliveryContext]] = ContextVar(
    "mupot_delivery_context",
    default=None,
)


class MupotProtocolError(RuntimeError):
    """A permanent, detail-free refusal of an invalid MCP response."""


class MupotTransportError(RuntimeError):
    """A detail-free transport failure whose server outcome may be unknown."""


class MupotSafeRetryError(RuntimeError):
    """A connection failure proven to occur before any request bytes were sent."""


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


def decode_mcp_result(
    payload: Any,
    request_id: int,
    tool: str,
    arguments: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
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
    if tool in {"inbox_lease", "inbox_lease_reconcile"}:
        attempt_id = (arguments or {}).get("attempt_id")
        if not isinstance(attempt_id, str):
            raise _protocol_error()
        return validate_lease_attempt_result(value, attempt_id)
    if tool == "inbox_lease_ack":
        attempt_id = (arguments or {}).get("attempt_id")
        if not isinstance(attempt_id, str):
            raise _protocol_error()
        return validate_lease_attempt_ack(value, attempt_id)
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


def validate_lease_attempt_result(
    result: Any,
    attempt_id: str,
    expected_scope: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Validate the server-authoritative outcome for one exact lease attempt."""
    if (
        not isinstance(result, dict)
        or set(result) != {
            "tenant",
            "agent_id",
            "effective_inbox_seat",
            "attempt_id",
            "state",
            "lease_expires_at",
            "messages",
            "consumed",
        }
        or not _LEASE_ATTEMPT_ID_RE.fullmatch(attempt_id)
        or result.get("attempt_id") != attempt_id
        or result.get("state") not in _LEASE_ATTEMPT_STATES
        or result.get("consumed") is not False
        or not isinstance(result.get("messages"), list)
    ):
        raise _protocol_error()
    scope = _attempt_scope_echo(result)
    if scope is None or expected_scope is not None and any(
        scope[field] != expected_scope.get(field)
        for field in ("tenant", "agent_id", "effective_inbox_seat")
    ):
        raise _protocol_error()
    state = result["state"]
    messages = result["messages"]
    lease_expires_at = result.get("lease_expires_at")
    if state != "leased":
        if lease_expires_at is not None or messages:
            raise _protocol_error()
        return result
    if (
        not isinstance(lease_expires_at, str)
        or not lease_expires_at.strip()
        or len(messages) != 1
        or not isinstance(messages[0], dict)
    ):
        raise _protocol_error()
    message = messages[0]
    if (
        not isinstance(message.get("id"), str)
        or not message["id"].strip()
        or type(message.get("seq")) is not int
        or message["seq"] <= 0
        or type(message.get("delivery_attempts")) is not int
        or message["delivery_attempts"] <= 0
        or message.get("lease_expires_at") != lease_expires_at
        or not isinstance(message.get("from_agent"), str)
        or not message["from_agent"].strip()
        or not isinstance(message.get("from_member"), str)
        or not message["from_member"].strip()
        or not isinstance(message.get("kind"), str)
        or not isinstance(message.get("body"), str)
        or not isinstance(message.get("created_at"), str)
        or not message["created_at"].strip()
    ):
        raise _protocol_error()
    return result


def validate_lease_attempt_ack(
    result: Any,
    attempt_id: str,
    expected_scope: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    if (
        not isinstance(result, dict)
        or set(result) != {
            "tenant",
            "agent_id",
            "effective_inbox_seat",
            "attempt_id",
            "state",
            "consumed",
        }
        or result.get("attempt_id") != attempt_id
        or not _LEASE_ATTEMPT_ID_RE.fullmatch(attempt_id)
        or result.get("state") not in _LEASE_ATTEMPT_STATES
        or type(result.get("consumed")) is not bool
    ):
        raise _protocol_error()
    scope = _attempt_scope_echo(result)
    if scope is None or expected_scope is not None and any(
        scope[field] != expected_scope.get(field)
        for field in ("tenant", "agent_id", "effective_inbox_seat")
    ):
        raise _protocol_error()
    return result


def _attempt_scope_echo(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    tenant = value.get("tenant")
    agent_id = value.get("agent_id")
    seat = value.get("effective_inbox_seat")
    if (
        not isinstance(tenant, str)
        or not tenant.strip()
        or tenant != tenant.strip()
        or not isinstance(agent_id, str)
        or not agent_id.strip()
        or agent_id != agent_id.strip()
        or not (
            seat is None
            or isinstance(seat, str) and bool(seat.strip()) and seat == seat.strip()
        )
    ):
        return None
    return {
        "tenant": tenant,
        "agent_id": agent_id,
        "effective_inbox_seat": seat,
    }


def _profile_owner_fingerprint(
    owner: Any,
    *,
    validate: bool = False,
) -> Optional[str]:
    if owner is None:
        try:
            owner = ProfileSecretOwner.from_active_home()
        except Exception:
            return None
    try:
        validated = getattr(owner, "validated_fingerprint", None)
        value = validated() if validate and callable(validated) else owner.fingerprint
    except Exception:
        return None
    if not isinstance(value, str) or not _PROFILE_OWNER_FINGERPRINT_RE.fullmatch(
        value
    ):
        return None
    return value


def _consumer_fence_proof(
    value: Any,
    expected_agent_id: Optional[str] = None,
    expected_tenant: Optional[str] = None,
    *,
    strict_scope: bool = False,
) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    agent_id = value.get("agent_id")
    mode = value.get("mode")
    generation = value.get("generation")
    if (
        not isinstance(agent_id, str)
        or not agent_id.strip()
        or agent_id != agent_id.strip()
        or (expected_agent_id is not None and agent_id != expected_agent_id)
        or mode not in {"bearer_only", "gateway"}
        or type(generation) is not int
        or generation < 0
        or value.get("key_matches") is not True
    ):
        return None
    proof = {"agent_id": agent_id, "mode": mode, "generation": generation}
    if not strict_scope:
        return proof
    scope = _attempt_scope_echo(value)
    if (
        value.get("strict_scope") is not True
        or scope is None
        or expected_tenant is not None
        and scope["tenant"] != expected_tenant
    ):
        return None
    return {**scope, "mode": mode, "generation": generation}


def _lease_reconciliation_proof(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    version = value.get("version")
    common = {
        "version",
        "required",
        "agent_id",
        "mode",
        "generation",
    }
    if version == 1:
        if set(value) != common | {"reconcile_after"}:
            return None
    elif version == 2:
        if set(value) != common | {"attempt_id"}:
            return None
    elif version == _LEASE_ATTEMPT_MARKER_VERSION:
        if set(value) != common | {
            "tenant",
            "effective_inbox_seat",
            "profile_owner_fingerprint",
            "attempt_id",
        }:
            return None
    else:
        return None
    fence = _consumer_fence_proof(
        {
            "agent_id": value.get("agent_id"),
            "mode": value.get("mode"),
            "generation": value.get("generation"),
            "key_matches": True,
        }
    )
    if value.get("required") is not True or fence is None:
        return None
    if version in {2, _LEASE_ATTEMPT_MARKER_VERSION}:
        attempt_id = value.get("attempt_id")
        if not isinstance(attempt_id, str) or not _LEASE_ATTEMPT_ID_RE.fullmatch(
            attempt_id
        ):
            return None
        if version == 2:
            return {"version": version, **fence, "attempt_id": attempt_id}
        scope = _attempt_scope_echo(value)
        owner_fingerprint = value.get("profile_owner_fingerprint")
        if (
            scope is None
            or not isinstance(owner_fingerprint, str)
            or not _PROFILE_OWNER_FINGERPRINT_RE.fullmatch(owner_fingerprint)
        ):
            return None
        return {
            "version": version,
            **scope,
            "mode": fence["mode"],
            "generation": fence["generation"],
            "profile_owner_fingerprint": owner_fingerprint,
            "attempt_id": attempt_id,
        }

    deadline = value.get("reconcile_after")
    if (
        not isinstance(deadline, (int, float))
        or isinstance(deadline, bool)
        or not math.isfinite(deadline)
        or deadline < 0
    ):
        return None
    return {"version": version, **fence, "reconcile_after": float(deadline)}


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
    # Verified against Mumega-com/mupot src/agents/messages.ts (sendAgentMessage: fromAgent
    # is auth.boundAgentId, a raw member_tokens.agent_id UUID -- see src/mcp/index.ts) and
    # src/agents/inbox-routes.ts (the delivered from_agent column is that same value):
    # mupot never emits an "agent:"-prefixed from_agent. The strip below is defensive only
    # (for a deployer hand-typing allowed_agents entries as "agent:kasra"), a no-op against
    # real traffic, and must not be read as evidence mupot performs this normalization for
    # us. Lower-casing matters for real traffic: agents.slug is lowercase-only, and mupot's
    # own system-constant senders (e.g. "mupot-flights") are lowercase too.
    text = str(value or "").strip().lower()
    return text.split(":", 1)[1].strip() if text.startswith("agent:") else text


def should_accept_message(
    message: dict[str, Any], allowed_agents: Iterable[str]
) -> bool:
    sender = normalize_agent(message.get("from_agent"))
    allowed = {normalize_agent(value) for value in allowed_agents if normalize_agent(value)}
    return bool(sender and sender in allowed and str(message.get("body") or "").strip())


def _estop_engaged() -> bool:
    """Enforce Hermes's own global emergency stop for a mupot-originated turn.

    build_mupot_event always sets event.internal=True so a mupot turn queues as its own
    turn instead of interrupting/steering whatever the human is doing (Hermes's own
    busy-routing distinction). But at the pinned Hermes rev, gateway/run_inbound.py:174
    returns for any internal event before it ever reaches the global e-stop gate at :233
    -- `hermes pause` silently does not stop mupot traffic through that path. Rather than
    drop internal=True (which would also skip _is_user_authorized_for_source and route
    mupot's synthetic, unpaired sources through end-user auth they were never designed to
    satisfy), enforce the same property directly here.
    """
    try:
        from agent.estop import is_engaged
    except ImportError:
        return False
    try:
        return bool(is_engaged())
    except Exception:
        # Fail SAFE like agent.estop.is_engaged itself does on a stat error: block
        # dispatch rather than silently let a mupot turn through while a global pause
        # cannot be confirmed lifted.
        return True


def is_ack_envelope(message: dict[str, Any]) -> bool:
    return str(message.get("kind") or "").strip().lower() == "ack"


def is_terminal_ack(message: dict[str, Any]) -> bool:
    """Recognize a complete authenticated terminal ACK envelope."""
    return (
        is_ack_envelope(message)
        and message.get("expects_reply") is False
        and bool(str(message.get("id") or "").strip())
    )


def _reply_source_fingerprint(source: Mapping[str, Any]) -> str:
    stable = {name: source.get(name) for name in _IMMUTABLE_REPLY_SOURCE_FIELDS}
    payload = json.dumps(
        stable,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _final_request_id(source_id: str) -> str:
    candidate = f"resp-{source_id}"
    if _REQUEST_ID_RE.fullmatch(candidate):
        return candidate
    digest = hashlib.sha256(source_id.encode("utf-8")).hexdigest()
    return f"resp-{digest}"


def _progress_request_id(source_id: str, body: str) -> str:
    digest = hashlib.sha256((source_id + body).encode("utf-8")).hexdigest()
    return f"prog-{digest}"


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
        # Mark internal for Hermes busy-routing only: queue this as a distinct turn
        # instead of interrupting/steering another command and emitting a misleading
        # busy response as the correlated ACK. This does NOT mean the message already
        # passed Hermes's own authorization/e-stop gates -- at the pinned Hermes rev,
        # internal=True makes gateway/run_inbound.py:174 skip both of those entirely
        # (see _estop_engaged's docstring). should_accept_message's allowlist check
        # (run in _process_leased_message before _deliver is ever reached) and the
        # explicit _estop_engaged() check in _deliver are what actually gate this.
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
        value, _valid = self.load_checked()
        return value

    def load_checked(self) -> tuple[dict[str, Any], bool]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
            return (value, True) if isinstance(value, dict) else ({}, False)
        except FileNotFoundError:
            return {}, True
        except (json.JSONDecodeError, OSError):
            return {}, False

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
            directory_flag = getattr(os, "O_DIRECTORY", None)
            if directory_flag is not None:
                directory_fd = os.open(
                    self.path.parent,
                    os.O_RDONLY | directory_flag,
                )
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


class HermesMCPClient:
    def __init__(
        self,
        server_name: str,
        *,
        secret_owner: ProfileSecretOwner | None = None,
    ):
        self.server_name = server_name
        self._secret_owner = secret_owner
        self._client: Optional[httpx.AsyncClient] = None
        self._url: Optional[str] = None
        self._headers: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._request_id = 0

    def _profile_scope(self):
        if self._secret_owner is None:
            return nullcontext()
        return self._secret_owner.activate()

    async def _ensure_client_locked(self) -> None:
        with self._profile_scope():
            self._ensure_client()

    def _ensure_client(self) -> None:
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
                if name.lower() not in {"authorization", "accept-encoding"}
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
            with self._profile_scope():
                self._ensure_client()
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
                headers = {
                    **self._headers,
                    "Authorization": f"Bearer {token}",
                    "Accept-Encoding": "identity",
                }
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
                except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
                    logger.warning("[mupot] MCP connection failed before request send")
                    try:
                        await client.aclose()
                    except Exception:
                        pass
                    self._client = None
                    raise MupotSafeRetryError(_GENERIC_MCP_TRANSPORT_ERROR) from None
                except Exception:
                    logger.warning("[mupot] MCP request failed; resetting client")
                    try:
                        await client.aclose()
                    except Exception:
                        pass
                    self._client = None
                    raise MupotTransportError(_GENERIC_MCP_TRANSPORT_ERROR) from None

                return decode_mcp_result(payload, req_id, tool, arguments)

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
        secret_owner: ProfileSecretOwner | None = None,
    ) -> None:
        super().__init__(config, _platform_for_mupot())
        extra = config.extra or {}
        allowed = extra.get("allowed_agents")
        if allowed is None:
            # Key genuinely absent (not configured) -- fall back to the historical
            # default operator roster. An explicit empty value ("" or []) means
            # deny-all and must NOT fall through to this default: `x or DEFAULT`
            # previously treated "configured empty" the same as "not configured",
            # which fails OPEN to the default four agents instead of closed.
            allowed = "hadi-codex,hadi-codex-cli,kasra,hermes"
        if isinstance(allowed, str):
            allowed = [item.strip() for item in allowed.split(",")]
        self.allowed_agents = {
            normalize_agent(item) for item in allowed if normalize_agent(item)
        }
        # NOTE on internal=True (see build_mupot_event): at the pinned Hermes rev,
        # gateway/run_inbound.py:174 returns for any event.internal before it ever
        # reaches _is_user_authorized_for_source (:185) or the global e-stop gate
        # (:233) -- Hermes's own authorization and `hermes pause` never run for a
        # mupot turn. `self.allowed_agents` (should_accept_message, checked before
        # _deliver is ever called) is therefore the ONLY sender fence, and the
        # e-stop is enforced explicitly in _deliver (see _estop_engaged) instead of
        # relying on Hermes's bypassed gate. There used to be a stale
        # `extra["allow_from"] = sorted(self.allowed_agents)` here implying Hermes
        # consults a second, canonical copy of this allowlist -- it does not
        # (verified: "allow_from" is read nowhere in gateway/*.py at the pinned
        # rev); that line was dead and has been removed rather than fixed to avoid
        # two copies of one predicate.
        self.server_name = str(extra.get("mcp_server") or "mupot")
        self.expected_agent_id = extra.get("expected_agent_id")
        self.expected_tenant = extra.get("expected_tenant")
        self.poll_interval = max(0.01, float(extra.get("poll_interval") or 2.0))
        self.rpc_timeout = max(5.0, float(extra.get("rpc_timeout") or 20.0))
        self.turn_timeout = max(10.0, float(extra.get("turn_timeout") or 300.0))
        self.cancel_timeout = max(0.01, float(extra.get("cancel_timeout") or 6.0))
        requested_lease = float(extra.get("lease_seconds") or (self.turn_timeout + 60.0))
        self.lease_seconds = max(1, min(3600, int(requested_lease)))
        state_path = extra.get("state_path") or str(get_hermes_home() / "platforms" / "mupot" / "state.json")
        self.store = StateStore(Path(str(state_path)))
        loaded, state_valid = self.store.load_checked()
        self.notification_recipients = dict(extra.get("notification_recipients") or {})
        self.notification_activate = extra.get("notification_activate") is True
        routine_events_enabled = extra.get("routine_events_enabled", False)
        if not isinstance(routine_events_enabled, bool):
            raise ValueError("routine_events_enabled must be a boolean")
        self.routine_events_enabled = routine_events_enabled
        self.message_injector = message_injector
        self._secret_owner = secret_owner
        self._profile_owner_fingerprint = _profile_owner_fingerprint(secret_owner)
        self._state: dict[str, Any] = copy.deepcopy(loaded) if state_valid else {}
        loaded_reply_outbox = loaded.get("reply_outbox")
        reply_outbox_valid = loaded_reply_outbox is None or isinstance(
            loaded_reply_outbox, dict
        )
        loaded_routine_receipts = loaded.get("routine_event_receipts")
        loaded_routine_quarantine = loaded.get("routine_event_quarantine")
        routine_state_valid = (
            (loaded_routine_receipts is None or isinstance(loaded_routine_receipts, dict))
            and (
                loaded_routine_quarantine is None
                or isinstance(loaded_routine_quarantine, dict)
            )
        )
        pending = copy.deepcopy(loaded.get("pending"))
        self._state.update({
            # Mupot owns retry timing through visibility leases. Never replay
            # a stale local in-flight record by rerunning the model after a
            # crash. Preserve it so a prepared terminal envelope can finish,
            # or so ambiguous legacy work can remain visibly fenced.
            "pending": pending,
            "processed": list(loaded.get("processed") or [])[-1000:],
            "dlq": list(loaded.get("dlq") or [])[-100:],
            "terminal_receipts": list(loaded.get("terminal_receipts") or [])[-100:],
            "notification_outbox": dict(loaded.get("notification_outbox") or {}),
            "routine_event_receipts": copy.deepcopy(loaded_routine_receipts)
            if isinstance(loaded_routine_receipts, dict)
            else {},
            "routine_event_quarantine": copy.deepcopy(loaded_routine_quarantine)
            if isinstance(loaded_routine_quarantine, dict)
            else {},
            "reply_outbox": (
                copy.deepcopy(loaded_reply_outbox)
                if isinstance(loaded_reply_outbox, dict)
                else {}
            ),
            "lease_reconciliation": (
                loaded.get("lease_reconciliation")
                if state_valid
                else {"state_invalid": True}
            ),
        })
        # Keep durable inbox lease/ACK traffic isolated from outbound sends.
        # Cancelling a timed-out MCP send can close that SDK session; it must
        # never poison the authoritative consumer transport.
        self._client = client_factory(self.server_name)
        self._send_client = client_factory(self.server_name)
        self._poll_task: Optional[asyncio.Task] = None
        self._delivery_generation = 0
        self._live_generations: dict[int, _LiveDelivery] = {}
        self._consumer_fence: Optional[dict[str, Any]] = None
        self._lease_quarantined = self._state["lease_reconciliation"] is not None
        self._reply_state_invalid = not state_valid or not reply_outbox_valid
        self._routine_state_invalid = not state_valid or not routine_state_valid
        pending_routine_records: list[dict[str, Any]] = []
        if not self._routine_state_invalid:
            try:
                from .routine_events import pending_routine_receipts

                pending_routine_records = pending_routine_receipts(self._state)
            except Exception:
                self._routine_state_invalid = True
        self._routine_reconciliation_required = bool(
            pending_routine_records
        ) and not self.routine_events_enabled
        pending_message = pending.get("message") if isinstance(pending, dict) else None
        pending_id = (
            str(pending_message.get("id") or "").strip()
            if isinstance(pending_message, dict)
            else ""
        )
        self._legacy_pending_ambiguous = pending is not None and (
            not pending_id or pending_id not in self._state["reply_outbox"]
        )
        self._reply_reconciliation_required = any(
            not isinstance(record, dict)
            or record.get("status") == "reconciliation_required"
            or (
                record.get("version") == _LEGACY_REPLY_OUTBOX_VERSION
                and record.get("status") != "complete"
            )
            for record in self._state["reply_outbox"].values()
        )
        for source_id, record in self._state["reply_outbox"].items():
            try:
                validated = self._validated_reply_record(source_id, record)
                if (
                    pending_id == source_id
                    and isinstance(pending_message, dict)
                    and validated["source_fingerprint"]
                    != _reply_source_fingerprint(pending_message)
                ):
                    self._reply_state_invalid = True
            except MupotProtocolError:
                self._reply_state_invalid = True
        self._log_stranded_notifications_at_startup()

    def stranded_notifications(self) -> list[dict[str, Any]]:
        """Notices parked in a terminal state that nothing else ever reconciles.

        ``activation_unknown``/``transport_unknown`` are written by notifications.flush
        when an activation or a send crashes mid-flight, and its source has already been
        inbox_lease_ack'd -- refused activation from here on strands the human-wait
        silently unless something actually looks at these. "Preserved for inspection"
        with no inspector is a known anti-pattern; this method (and the startup log
        below) are that inspector.
        """
        stranded = []
        for source_id, notice in self._state.get("notification_outbox", {}).items():
            if (
                isinstance(notice, dict)
                and notice.get("status") in _STRANDED_NOTIFICATION_STATUSES
            ):
                stranded.append({
                    "source_id": source_id,
                    "status": notice.get("status"),
                    "activation_status": notice.get("activation_status"),
                    "delivery_status": notice.get("delivery_status"),
                    "last_error": notice.get("last_error"),
                })
        return stranded

    def _log_stranded_notifications_at_startup(self) -> None:
        stranded = self.stranded_notifications()
        if stranded:
            logger.warning(
                "[mupot] %d notification(s) loaded in a stranded terminal state "
                "(activation_unknown/transport_unknown) and require operator "
                "reconciliation: %s",
                len(stranded),
                sorted(item["source_id"] for item in stranded),
            )

    def set_message_handler(
        self,
        handler: Callable[[MessageEvent], Awaitable[Any]],
    ) -> None:
        async def context_bound_handler(event: MessageEvent) -> Any:
            runtime = self._runtime_for_event(event)
            if runtime is None or self._expire_if_needed(runtime):
                return None
            _delivery_context.set(runtime.context)
            return await handler(event)

        super().set_message_handler(context_bound_handler)

    @staticmethod
    def _optional_text(value: Any) -> Optional[str]:
        text = str(value or "").strip()
        return text or None

    def _delivery_deadline(self, message: dict[str, Any]) -> float:
        deadline = time.time() + self.turn_timeout
        raw_expiry = message.get("lease_expires_at")
        if raw_expiry is None:
            return deadline
        if not isinstance(raw_expiry, str) or not raw_expiry.strip():
            raise _protocol_error()
        try:
            parsed = datetime.fromisoformat(raw_expiry.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("timezone required")
            lease_deadline = parsed.timestamp()
        except (OverflowError, TypeError, ValueError):
            raise _protocol_error() from None
        if not math.isfinite(lease_deadline):
            raise _protocol_error()
        return min(deadline, lease_deadline)

    def _begin_delivery(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str] = None,
    ) -> tuple[MessageEvent, _LiveDelivery]:
        event = build_mupot_event(message, self.platform)
        source_id = str(message.get("id") or "").strip()
        sender = normalize_agent(message.get("from_agent"))
        session_key = str(self._event_session_key(event) or "").strip()
        if not source_id or not sender or not session_key:
            raise _protocol_error()
        self._delivery_generation += 1
        context = DeliveryContext(
            source_id=source_id,
            generation=self._delivery_generation,
            sender=sender,
            project=self._optional_text(message.get("project_id")),
            request_id=self._optional_text(message.get("request_id")),
            session_key=session_key,
            attempt_id=attempt_id,
        )
        runtime = _LiveDelivery(
            context=context,
            expires_at=self._delivery_deadline(message),
            source=MappingProxyType(copy.deepcopy(message)),
        )
        metadata = dict(event.metadata or {})
        metadata[_DELIVERY_CONTEXT_METADATA_KEY] = context
        event.metadata = metadata
        self._live_generations[context.generation] = runtime
        return event, runtime

    def _runtime_for_event(self, event: MessageEvent) -> Optional[_LiveDelivery]:
        metadata = event.metadata or {}
        marker = metadata.get(_DELIVERY_CONTEXT_METADATA_KEY)
        if not isinstance(marker, DeliveryContext):
            return None
        runtime = self._live_generations.get(marker.generation)
        if runtime is None or runtime.context is not marker or runtime.invalidated:
            return None
        raw_message = event.raw_message
        if not isinstance(raw_message, dict):
            return None
        if (
            str(event.message_id or "").strip() != marker.source_id
            or str(raw_message.get("id") or "").strip() != marker.source_id
            or normalize_agent(event.source.chat_id) != marker.sender
            or normalize_agent(raw_message.get("from_agent")) != marker.sender
            or self._optional_text(metadata.get("project_id")) != marker.project
            or self._optional_text(raw_message.get("project_id")) != marker.project
            or self._optional_text(metadata.get("request_id")) != marker.request_id
            or self._optional_text(raw_message.get("request_id")) != marker.request_id
            or str(self._event_session_key(event) or "").strip() != marker.session_key
        ):
            return None
        return runtime

    def _runtime_for_current_context(self) -> Optional[_LiveDelivery]:
        context = _delivery_context.get()
        if context is None:
            return None
        runtime = self._live_generations.get(context.generation)
        if runtime is None or runtime.context is not context or runtime.invalidated:
            return None
        if self._expire_if_needed(runtime):
            return None
        return runtime

    def _invalidate_delivery(
        self,
        runtime: _LiveDelivery,
        outcome: ProcessingOutcome = ProcessingOutcome.FAILURE,
    ) -> None:
        current = self._live_generations.get(runtime.context.generation)
        if current is runtime:
            self._live_generations.pop(runtime.context.generation, None)
        runtime.invalidated = True
        if runtime.outcome is None:
            runtime.outcome = outcome
        runtime.completion_event.set()

    def _expire_if_needed(self, runtime: _LiveDelivery) -> bool:
        if runtime.invalidated or time.time() >= runtime.expires_at:
            self._invalidate_delivery(runtime)
            return True
        return False

    async def _cancel_delivery_processing(self, runtime: _LiveDelivery) -> None:
        if runtime.cancellation_started:
            return
        runtime.cancellation_started = True
        try:
            await asyncio.wait_for(
                self.cancel_session_processing(runtime.context.session_key),
                timeout=self.cancel_timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "[mupot] bounded session cancellation did not complete session=%s",
                runtime.context.session_key,
            )

    def _delivery_ack_ownership(
        self,
        attempt_id: Optional[str],
    ) -> dict[str, Any]:
        if attempt_id is None:
            return legacy_ack_ownership()
        marker = _lease_reconciliation_proof(
            self._state.get("lease_reconciliation")
        )
        current_owner = _profile_owner_fingerprint(
            self._secret_owner,
            validate=True,
        )
        if (
            marker is None
            or marker.get("version") != _LEASE_ATTEMPT_MARKER_VERSION
            or marker.get("attempt_id") != attempt_id
            or current_owner != self._profile_owner_fingerprint
            or marker.get("profile_owner_fingerprint") != current_owner
        ):
            raise _protocol_error()
        try:
            return attempt_ack_ownership(marker)
        except AckOwnershipError:
            raise _protocol_error() from None

    async def _ack_persisted_ownership(
        self,
        expected_id: str,
        ownership_value: Any,
    ) -> None:
        try:
            ownership = validate_ack_ownership(ownership_value)
        except AckOwnershipError:
            self._lease_quarantined = True
            raise _protocol_error() from None
        if ownership["kind"] == "legacy_non_attempt":
            await self._ack_expected(expected_id)
            return
        ownership = await self._preflight_persisted_ownership(
            ownership,
            require_attempt=True,
        )
        try:
            payload = await self._call_consumer(
                "inbox_lease_ack",
                {"attempt_id": ownership["attempt_id"]},
            )
            receipt = validate_lease_attempt_ack(
                payload,
                ownership["attempt_id"],
                ownership,
            )
            if receipt["state"] != "acked" or receipt["consumed"] is not True:
                raise _protocol_error()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._lease_quarantined = True
            self._set_fatal_error(
                "mupot_inbox_attempt_ack_reconciliation_required",
                "Mupot inbox attempt acknowledgement requires reconciliation",
                retryable=False,
            )
            raise

    async def _preflight_persisted_ownership(
        self,
        ownership_value: Any,
        *,
        require_attempt: bool = False,
    ) -> dict[str, Any]:
        """Validate replay ownership without sending, reconciling, or ACKing."""
        try:
            ownership = validate_ack_ownership(ownership_value)
            if ownership["kind"] != "attempt":
                if require_attempt:
                    raise _protocol_error()
                return ownership
            current_owner = _profile_owner_fingerprint(
                self._secret_owner,
                validate=True,
            )
            if (
                current_owner != self._profile_owner_fingerprint
                or ownership["profile_owner_fingerprint"] != current_owner
            ):
                raise _protocol_error()
            status = await self._call_consumer(
                "inbox_consumer_status",
                {"strict_scope": True},
            )
            proof = _consumer_fence_proof(
                status,
                ownership["agent_id"],
                ownership["tenant"],
                strict_scope=True,
            )
            if proof is None or any(
                proof[field] != ownership[field]
                for field in (
                    "tenant",
                    "agent_id",
                    "effective_inbox_seat",
                    "mode",
                    "generation",
                )
            ):
                raise _protocol_error()
            return ownership
        except asyncio.CancelledError:
            raise
        except Exception:
            self._lease_quarantined = True
            self._set_fatal_error(
                "mupot_inbox_replay_preflight_required",
                "Mupot inbox replay ownership requires reconciliation",
                retryable=False,
            )
            raise

    def _persist_reply_record(
        self,
        source_id: str,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        durable, valid = self.store.load_checked()
        if not valid:
            raise _protocol_error()
        candidate = copy.deepcopy(durable if self.store.path.exists() else self._state)
        outbox = candidate.setdefault("reply_outbox", {})
        if not isinstance(outbox, dict):
            raise _protocol_error()
        outbox[source_id] = copy.deepcopy(record)
        self.store.save(candidate)
        readback, readback_valid = self.store.load_checked()
        durable_outbox = readback.get("reply_outbox")
        if (
            not readback_valid
            or not isinstance(durable_outbox, dict)
            or durable_outbox.get(source_id) != record
        ):
            raise _protocol_error()
        self._state = readback
        return copy.deepcopy(record)

    def _validated_reply_record(
        self,
        source_id: str,
        record: Any,
        source: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise _protocol_error()
        arguments = record.get("arguments")
        receipt = record.get("receipt")
        version = record.get("version")
        if (
            version not in {_LEGACY_REPLY_OUTBOX_VERSION, _REPLY_OUTBOX_VERSION}
            or record.get("source_id") != source_id
            or not isinstance(record.get("source"), dict)
            or not isinstance(record.get("source_fingerprint"), str)
            or not isinstance(arguments, dict)
            or not {
                "to",
                "body",
                "kind",
                "request_id",
                "in_reply_to",
            }.issubset(arguments)
            or set(arguments) - {
                "to",
                "body",
                "kind",
                "project_id",
                "request_id",
                "in_reply_to",
            }
            or arguments.get("kind") != "ack"
            or not isinstance(arguments.get("to"), str)
            or not arguments["to"].strip()
            or not str(arguments.get("body") or "").strip()
            or not isinstance(arguments.get("request_id"), str)
            or _REQUEST_ID_RE.fullmatch(arguments["request_id"]) is None
            or arguments["request_id"] != _final_request_id(source_id)
            or arguments.get("in_reply_to") != source_id
            or (
                "project_id" in arguments
                and (
                    not isinstance(arguments["project_id"], str)
                    or not arguments["project_id"].strip()
                )
            )
            or record.get("status")
            not in {
                "prepared",
                "sent",
                "custodied",
                "complete",
                "reconciliation_required",
            }
            or (receipt is not None and not isinstance(receipt, dict))
        ):
            raise _protocol_error()
        if version == _REPLY_OUTBOX_VERSION:
            try:
                validate_ack_ownership(record.get("ack_ownership"))
            except AckOwnershipError:
                raise _protocol_error() from None
        elif "ack_ownership" in record:
            raise _protocol_error()
        persisted_source = record["source"]
        if record["source_fingerprint"] != _reply_source_fingerprint(persisted_source):
            raise _protocol_error()
        if source is not None and record["source_fingerprint"] != _reply_source_fingerprint(
            source
        ):
            raise _protocol_error()
        status = record["status"]
        if status == "prepared" and receipt is not None:
            raise _protocol_error()
        if status in {"sent", "custodied", "complete"}:
            validate_send_receipt(
                receipt,
                arguments["to"],
                arguments.get("project_id"),
            )
        return copy.deepcopy(record)

    def _prepare_final_reply(
        self,
        runtime: _LiveDelivery,
        recipient: str,
        content: str,
    ) -> dict[str, Any]:
        source_id = runtime.context.source_id
        durable, valid = self.store.load_checked()
        if not valid:
            raise _protocol_error()
        outbox = durable.get("reply_outbox")
        if outbox is not None and not isinstance(outbox, dict):
            raise _protocol_error()
        existing = (outbox or {}).get(source_id)
        if existing is not None:
            return self._validated_reply_record(source_id, existing, runtime.source)
        if not content.strip():
            raise _protocol_error()
        arguments: dict[str, Any] = {
            "to": recipient,
            "body": content,
            "kind": "ack",
            "request_id": _final_request_id(source_id),
            "in_reply_to": source_id,
        }
        if runtime.context.project:
            arguments["project_id"] = runtime.context.project
        source = copy.deepcopy(dict(runtime.source))
        ack_ownership = self._delivery_ack_ownership(runtime.context.attempt_id)
        record = {
            "version": _REPLY_OUTBOX_VERSION,
            "source_id": source_id,
            "source": source,
            "source_fingerprint": _reply_source_fingerprint(source),
            "ack_ownership": ack_ownership,
            "arguments": arguments,
            "status": "prepared",
            "receipt": None,
        }
        return self._persist_reply_record(source_id, record)

    def _update_reply_record(
        self,
        source_id: str,
        record: dict[str, Any],
        *,
        status: str,
        receipt: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        updated = copy.deepcopy(record)
        updated["status"] = status
        if receipt is not None:
            updated["receipt"] = copy.deepcopy(receipt)
        persisted = self._persist_reply_record(source_id, updated)
        self._reply_reconciliation_required = status == "reconciliation_required" or any(
            isinstance(value, dict)
            and value.get("status") == "reconciliation_required"
            for key, value in self._state.get("reply_outbox", {}).items()
            if key != source_id
        )
        return persisted

    async def _transmit_final_reply(
        self,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        source_id = record["source_id"]
        record = self._validated_reply_record(source_id, record)
        if record["status"] == "reconciliation_required":
            raise _protocol_error()
        if record["status"] == "prepared":
            try:
                result = await asyncio.wait_for(
                    self._send_client.call("send", copy.deepcopy(record["arguments"])),
                    timeout=self.rpc_timeout,
                )
                receipt = validate_send_receipt(
                    result,
                    record["arguments"]["to"],
                    record["arguments"].get("project_id"),
                )
            except MupotProtocolError:
                self._update_reply_record(
                    source_id,
                    record,
                    status="reconciliation_required",
                )
                raise
            record = self._update_reply_record(
                source_id,
                record,
                status="sent",
                receipt=receipt,
            )
        if record["status"] == "sent":
            from .notifications import enqueue

            enqueue(
                self._state,
                self.store,
                record["source"],
                record["arguments"]["body"],
            )
            # enqueue performs its own durable readback. Persisting this status
            # after it means source consumption can require both custody proofs.
            durable_record = self._state.get("reply_outbox", {}).get(source_id)
            record = self._validated_reply_record(source_id, durable_record)
            record = self._update_reply_record(
                source_id,
                record,
                status="custodied",
            )
        final_receipt = record.get("receipt")
        if not isinstance(final_receipt, dict):
            raise _protocol_error()
        return final_receipt

    def _reply_has_human_custody(self, source_id: str) -> bool:
        record = self._state.get("reply_outbox", {}).get(source_id)
        notice = self._state.get("notification_outbox", {}).get(source_id)
        return bool(
            isinstance(record, dict)
            and record.get("status") in {"custodied", "complete"}
            and isinstance(record.get("receipt"), dict)
            and isinstance(notice, dict)
            and notice.get("custody_status") == "durable"
        )

    def _mark_reply_complete(self, source_id: str) -> None:
        record = self._state.get("reply_outbox", {}).get(source_id)
        validated = self._validated_reply_record(source_id, record)
        if validated["status"] != "complete":
            self._update_reply_record(source_id, validated, status="complete")

    async def _replay_reply_outbox(self) -> None:
        outbox = self._state.get("reply_outbox")
        if not isinstance(outbox, dict):
            raise _protocol_error()
        for source_id in list(outbox):
            record = self._validated_reply_record(source_id, outbox[source_id])
            if record["status"] == "complete":
                continue
            if record["version"] == _LEGACY_REPLY_OUTBOX_VERSION:
                self._reply_reconciliation_required = True
                raise _protocol_error()
            if record["status"] == "reconciliation_required":
                self._reply_reconciliation_required = True
                raise _protocol_error()
            if source_id in self._state.get("processed", []):
                self._mark_reply_complete(source_id)
                continue
            pending = self._state.get("pending")
            pending_message = pending.get("message") if isinstance(pending, dict) else None
            if (
                not isinstance(pending_message, dict)
                or str(pending_message.get("id") or "").strip() != source_id
                or _reply_source_fingerprint(pending_message)
                != record["source_fingerprint"]
            ):
                self._update_reply_record(
                    source_id,
                    record,
                    status="reconciliation_required",
                )
                raise _protocol_error()
            if record["status"] == "prepared":
                await self._preflight_persisted_ownership(
                    record["ack_ownership"],
                    require_attempt=True,
                )
            await self._transmit_final_reply(record)
            if not self._reply_has_human_custody(source_id):
                raise _protocol_error()
            await self._ack_persisted_ownership(
                source_id,
                record["ack_ownership"],
            )
            self._commit(source_id)
            self._mark_reply_complete(source_id)

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if self._secret_owner is not None:
            try:
                with self._secret_owner.activate():
                    return await self._connect_with_active_scope(
                        is_reconnect=is_reconnect
                    )
            except Exception:
                logger.error("[mupot] connect failed: profile scope unavailable")
                self._mark_disconnected()
                return False
        return await self._connect_with_active_scope(is_reconnect=is_reconnect)

    async def _connect_with_active_scope(self, *, is_reconnect: bool = False) -> bool:
        if (
            self._profile_owner_fingerprint is None
            or _profile_owner_fingerprint(self._secret_owner, validate=True)
            != self._profile_owner_fingerprint
        ):
            logger.error("[mupot] connect blocked; profile owner unavailable")
            return False
        if (
            self._reply_state_invalid
            or self._legacy_pending_ambiguous
            or self._reply_reconciliation_required
        ):
            logger.error("[mupot] connect blocked; reply reconciliation required")
            return False
        if self._routine_state_invalid:
            logger.error("[mupot] connect blocked; Routine event reconciliation required")
            return False
        if self._routine_reconciliation_required:
            logger.error(
                "[mupot] connect blocked; Routine events are disabled with pending custody"
            )
            return False
        if self._lease_quarantined:
            logger.error("[mupot] connect blocked; inbox reconciliation required")
            return False
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
                self._client.call("inbox_consumer_status", {"strict_scope": True}),
                timeout=self.rpc_timeout,
            )
            proof = _consumer_fence_proof(
                fence,
                self.expected_agent_id,
                self.expected_tenant,
                strict_scope=True,
            )
            if proof is None:
                raise _protocol_error()
            self._consumer_fence = proof
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
        live = list(self._live_generations.values())
        for runtime in live:
            self._invalidate_delivery(runtime)
        await asyncio.gather(
            *(self._cancel_delivery_processing(runtime) for runtime in live),
            return_exceptions=True,
        )
        task, self._poll_task = self._poll_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self.cancel_background_tasks()
        await self._client.close()
        if self._send_client is not self._client:
            await self._send_client.close()
        self._mark_disconnected()

    async def _call_consumer(
        self,
        tool: str,
        arguments: dict[str, Any],
        before_attempt: Optional[Callable[[], None]] = None,
    ) -> Any:
        """Retry once only when transport proves no request bytes were sent."""
        for attempt in range(2):
            if before_attempt is not None:
                before_attempt()
            try:
                return await asyncio.wait_for(
                    self._client.call(tool, arguments),
                    timeout=self.rpc_timeout,
                )
            except MupotSafeRetryError:
                if attempt == 0:
                    logger.warning(
                        "[mupot] retrying consumer request after safe pre-send failure"
                    )
                    continue
                raise
        raise AssertionError("unreachable")

    @staticmethod
    def _new_lease_attempt_id() -> str:
        attempt_id = secrets.token_urlsafe(24)
        if not _LEASE_ATTEMPT_ID_RE.fullmatch(attempt_id):
            raise _protocol_error()
        return attempt_id

    def _persist_prelease_fence(self, attempt_id: str) -> None:
        proof = self._consumer_fence
        owner_fingerprint = self._profile_owner_fingerprint
        if (
            proof is None
            or owner_fingerprint is None
            or _profile_owner_fingerprint(self._secret_owner, validate=True)
            != owner_fingerprint
            or not _LEASE_ATTEMPT_ID_RE.fullmatch(attempt_id)
        ):
            raise _protocol_error()
        fenced = dict(self._state)
        fenced["lease_reconciliation"] = {
            "version": _LEASE_ATTEMPT_MARKER_VERSION,
            "required": True,
            **proof,
            "profile_owner_fingerprint": owner_fingerprint,
            "attempt_id": attempt_id,
        }
        self.store.save(fenced)
        self._state = fenced

    def _clear_lease_fence(self) -> None:
        cleared = dict(self._state)
        cleared.pop("lease_reconciliation", None)
        self.store.save(cleared)
        self._state = cleared

    def _quarantine_inbox_polling(self) -> None:
        if _lease_reconciliation_proof(
            self._state.get("lease_reconciliation")
        ) is not None:
            self._lease_quarantined = True
            self._set_fatal_error(
                "mupot_inbox_reconciliation_required",
                "Mupot inbox polling requires reconciliation",
                retryable=False,
            )
            logger.error("[mupot] inbox polling quarantined; reconciliation required")
            return
        proof = self._consumer_fence
        if proof is None:
            self._lease_quarantined = True
            self._set_fatal_error(
                "mupot_inbox_reconciliation_state_missing",
                "Mupot inbox reconciliation state is unavailable",
                retryable=False,
            )
            logger.error("[mupot] inbox polling quarantined without fence state")
            return
        self._state["lease_reconciliation"] = {
            "version": 1,
            "required": True,
            "agent_id": proof["agent_id"],
            "mode": proof["mode"],
            "generation": proof["generation"],
            "reconcile_after": 0,
        }
        self._lease_quarantined = True
        try:
            self.store.save(self._state)
        except Exception:
            self._set_fatal_error(
                "mupot_inbox_reconciliation_persistence_failed",
                "Mupot inbox reconciliation state could not be persisted",
                retryable=False,
            )
            logger.error("[mupot] inbox polling quarantine persistence failed")
            return
        self._set_fatal_error(
            "mupot_inbox_reconciliation_required",
            "Mupot inbox polling requires reconciliation",
            retryable=False,
        )
        logger.error("[mupot] inbox polling quarantined; reconciliation required")

    async def reconcile_inbox_polling(self) -> bool:
        """Reconcile a durable attempt through its authoritative server receipt."""
        if self._secret_owner is not None:
            try:
                with self._secret_owner.activate():
                    return await self._reconcile_inbox_polling_with_active_scope()
            except Exception:
                logger.error("[mupot] inbox reconciliation profile scope failed")
                return False
        return await self._reconcile_inbox_polling_with_active_scope()

    async def _reconcile_inbox_polling_with_active_scope(self) -> bool:
        marker = _lease_reconciliation_proof(
            self._state.get("lease_reconciliation")
        )
        current_owner_fingerprint = _profile_owner_fingerprint(
            self._secret_owner,
            validate=True,
        )
        if (
            not self._lease_quarantined
            or marker is None
            or marker.get("version") != _LEASE_ATTEMPT_MARKER_VERSION
            or current_owner_fingerprint != self._profile_owner_fingerprint
            or marker.get("profile_owner_fingerprint")
            != current_owner_fingerprint
        ):
            return False
        try:
            require_supported_profile_runtime({})
            await self._client.connect()
            value = await asyncio.wait_for(
                self._client.call("inbox_consumer_status", {"strict_scope": True}),
                timeout=self.rpc_timeout,
            )
        except Exception:
            logger.error("[mupot] inbox reconciliation readback failed")
            return False
        proof = _consumer_fence_proof(
            value,
            marker["agent_id"],
            marker["tenant"],
            strict_scope=True,
        )
        if proof is None or any(
            proof[field] != marker[field]
            for field in (
                "tenant",
                "agent_id",
                "effective_inbox_seat",
                "mode",
                "generation",
            )
        ):
            logger.error("[mupot] inbox reconciliation readback mismatch")
            return False
        try:
            result = await asyncio.wait_for(
                self._client.call(
                    "inbox_lease_reconcile",
                    {"attempt_id": marker["attempt_id"]},
                ),
                timeout=self.rpc_timeout,
            )
            outcome = validate_lease_attempt_result(
                result,
                marker["attempt_id"],
                marker,
            )
            if outcome["state"] == "leased":
                message = outcome["messages"][0]
                await self._process_leased_message(
                    message,
                    attempt_id=marker["attempt_id"],
                )
                message_id = message["id"]
                if message_id not in self._state.get("processed", []):
                    raise _protocol_error()
            self._clear_lease_fence()
        except Exception:
            logger.error("[mupot] inbox attempt reconciliation failed")
            return False
        self._consumer_fence = proof
        self._lease_quarantined = False
        self._fatal_error_code = self._fatal_error_message = None
        self._fatal_error_retryable = True
        self._mark_disconnected()
        return True

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                await self._replay_routine_events()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._set_fatal_error(
                    "mupot_routine_event_reconciliation_required",
                    "Mupot Routine event reconciliation is required",
                    retryable=False,
                )
                logger.error("[mupot] Routine event replay requires reconciliation: %s", exc)
                return
            try:
                await self._replay_reply_outbox()
            except asyncio.CancelledError:
                raise
            except MupotProtocolError:
                self._reply_reconciliation_required = True
                self._set_fatal_error(
                    "mupot_reply_reconciliation_required",
                    "Mupot reply reconciliation is required",
                    retryable=False,
                )
                logger.error("[mupot] reply replay requires reconciliation")
                return
            except Exception as exc:
                # The exact immutable envelope remains durable. Do not lease
                # new work while its terminal response is unresolved.
                logger.warning("[mupot] reply replay deferred: %s", exc)
                await asyncio.sleep(self.poll_interval)
                continue
            try:
                await self._flush_notifications()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "[mupot] notification flush error: %s", exc, exc_info=True
                )
                await asyncio.sleep(self.poll_interval)
                continue

            try:
                attempt_id = self._new_lease_attempt_id()
                arguments = {
                    "limit": 1,
                    "lease_seconds": self.lease_seconds,
                    "attempt_id": attempt_id,
                }
                payload = await self._call_consumer(
                    "inbox_lease",
                    arguments,
                    before_attempt=lambda: self._persist_prelease_fence(attempt_id),
                )
                outcome = validate_lease_attempt_result(
                    payload,
                    attempt_id,
                    self._consumer_fence,
                )
                if outcome["state"] == "leased":
                    message = outcome["messages"][0]
                    message_id = str(message.get("id") or "")
                    logger.info(
                        "[mupot] leased message=%s seq=%s attempts=%s request_id=%s",
                        message_id,
                        message.get("seq"),
                        message.get("delivery_attempts"),
                        message.get("request_id"),
                    )
                    await self._process_leased_message(message, attempt_id=attempt_id)
                    if message_id not in self._state.get("processed", []):
                        raise _protocol_error()
                self._clear_lease_fence()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._quarantine_inbox_polling()
                return
            await asyncio.sleep(self.poll_interval)

    async def _process_leased_message(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str] = None,
    ) -> None:
        """Route one authenticated leased row without widening peer authority."""
        from .routine_events import is_routine_event_candidate, quarantine_routine_event

        message_id = str(message.get("id") or "")
        if is_routine_event_candidate(message):
            if self.routine_events_enabled:
                await self._handle_routine_event(message, attempt_id=attempt_id)
            else:
                quarantine_routine_event(
                    self._state, self.store, message, "routine_events_disabled"
                )
                await self._ack_expected(message_id, attempt_id=attempt_id)
                self._commit(message_id)
            return
        if message_id in self._state["processed"]:
            await self._ack_expected(message_id, attempt_id=attempt_id)
            return
        if is_ack_envelope(message) and should_accept_message(
            message, self.allowed_agents
        ):
            await self._handle_ack_envelope(message, attempt_id=attempt_id)
            return
        if should_accept_message(message, self.allowed_agents):
            await self._deliver(message, attempt_id=attempt_id)
            return
        self._state["dlq"].append({"message": message, "reason": "sender_policy"})
        self._state["dlq"] = self._state["dlq"][-100:]
        self.store.save(self._state)
        await self._ack_expected(message_id, attempt_id=attempt_id)
        self._commit(message_id)

    async def _handle_routine_event(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str] = None,
    ) -> None:
        """Take custody, ACK exactly one source, then durably mark it processed."""
        from .notifications import enqueue
        from .routine_events import (
            RoutineEventValidationError,
            mark_routine_processed,
            persist_routine_receipt,
            quarantine_routine_event,
            validate_routine_event,
        )

        if not self.routine_events_enabled:
            raise RuntimeError("Mupot Routine events are disabled")
        message_id = str(message.get("id") or "")
        try:
            event = validate_routine_event(message)
        except RoutineEventValidationError:
            quarantine_routine_event(
                self._state, self.store, message, "invalid_routine_event"
            )
            await self._ack_expected(message_id, attempt_id=attempt_id)
            self._commit(message_id)
            return

        ack_ownership = self._delivery_ack_ownership(attempt_id)
        persist_routine_receipt(
            self._state,
            self.store,
            message,
            event,
            ack_ownership,
        )
        enqueue(
            self._state,
            self.store,
            message,
            event.notice,
            activation_required=True,
            activation_after_processed=True,
        )
        await self._ack_persisted_ownership(event.source_id, ack_ownership)
        mark_routine_processed(self._state, self.store, event.source_id)

    async def _replay_routine_events(self) -> None:
        """Close crash windows by retrying the exact source ACK before activation."""
        if not self.routine_events_enabled:
            return
        from .notifications import enqueue
        from .routine_events import (
            mark_routine_processed,
            pending_routine_receipts,
            validate_routine_event,
        )

        for record in pending_routine_receipts(self._state):
            source = record["source"]
            event = validate_routine_event(source)
            enqueue(
                self._state,
                self.store,
                source,
                event.notice,
                activation_required=True,
                activation_after_processed=True,
            )
            await self._ack_persisted_ownership(
                event.source_id,
                record["ack_ownership"],
            )
            mark_routine_processed(self._state, self.store, event.source_id)

    async def _handle_ack_envelope(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str] = None,
    ) -> None:
        """Quarantine incomplete ACKs; persist complete receipts before ACKing."""
        message_id = str(message.get("id") or "")
        if not is_terminal_ack(message):
            quarantined = self._state.get("dlq") or []
            quarantined.append({"message": dict(message), "reason": "invalid_ack_envelope"})
            self._state["dlq"] = quarantined[-100:]
            self.store.save(self._state)
            if message_id:
                await self._ack_expected(message_id, attempt_id=attempt_id)
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
        await self._ack_expected(message_id, attempt_id=attempt_id)
        self._commit(message_id)

    async def _deliver(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str] = None,
    ) -> None:
        message_id = str(message.get("id") or "")
        if _estop_engaged():
            # Refuse before touching any durable state: leave the message unacked so
            # Mupot's own visibility lease expires and redelivers it once `hermes
            # resume` lifts the pause, instead of recording a local "pending" that
            # would need its own reconciliation path.
            logger.warning(
                "[mupot] deferring leased message=%s: Hermes global emergency stop is engaged",
                message_id,
            )
            return
        self._state["pending"] = {"message": message}
        self.store.save(self._state)
        event, runtime = self._begin_delivery(message, attempt_id=attempt_id)
        if self._expire_if_needed(runtime):
            logger.warning("[mupot] refusing expired leased message=%s", message_id)
            self.store.save(self._state)
            return
        token = _delivery_context.set(runtime.context)
        try:
            await self.handle_message(event)
        finally:
            _delivery_context.reset(token)
        timed_out = False
        try:
            remaining = max(0.0, runtime.expires_at - time.time())
            await asyncio.wait_for(runtime.completion_event.wait(), remaining)
        except asyncio.TimeoutError:
            timed_out = True
            self._invalidate_delivery(runtime)
            logger.error("[mupot] turn timeout message=%s", message_id)
        except asyncio.CancelledError:
            self._invalidate_delivery(runtime, ProcessingOutcome.CANCELLED)
            await self._cancel_delivery_processing(runtime)
            self.store.save(self._state)
            raise
        if timed_out or runtime.invalidated and runtime.outcome != ProcessingOutcome.SUCCESS:
            await self._cancel_delivery_processing(runtime)
            self.store.save(self._state)
            return
        self._invalidate_delivery(
            runtime,
            runtime.outcome or ProcessingOutcome.FAILURE,
        )
        if runtime.outcome == ProcessingOutcome.SUCCESS:
            if not self._reply_has_human_custody(message_id):
                # Hermes treats an empty response as a successful no-op. A
                # peer request is not consumable until a terminal ACK has a
                # concrete Mupot receipt and its human notice has custody.
                self.store.save(self._state)
                return
            durable_record = self._validated_reply_record(
                message_id,
                self._state.get("reply_outbox", {}).get(message_id),
                runtime.source,
            )
            await self._ack_persisted_ownership(
                message_id,
                durable_record["ack_ownership"],
            )
            self._commit(message_id)
            self._mark_reply_complete(message_id)
            return
        # Do not acknowledge failure and do not immediately replay locally.
        # The server-side visibility lease expires, retries safely, and moves
        # poison messages to Mupot's durable dead-letter state.
        self.store.save(self._state)

    async def on_processing_start(self, event: MessageEvent) -> None:
        runtime = self._runtime_for_event(event)
        if runtime is None or self._expire_if_needed(runtime):
            _delivery_context.set(None)
            return
        _delivery_context.set(runtime.context)

    async def _ack_expected(
        self,
        expected_id: str,
        attempt_id: Optional[str] = None,
    ) -> None:
        if attempt_id is not None:
            marker = _lease_reconciliation_proof(
                self._state.get("lease_reconciliation")
            )
            current_owner_fingerprint = _profile_owner_fingerprint(
                self._secret_owner,
                validate=True,
            )
            if (
                marker is None
                or marker.get("version") != _LEASE_ATTEMPT_MARKER_VERSION
                or marker.get("attempt_id") != attempt_id
                or current_owner_fingerprint != self._profile_owner_fingerprint
                or marker.get("profile_owner_fingerprint")
                != current_owner_fingerprint
            ):
                raise _protocol_error()
            payload = await self._call_consumer(
                "inbox_lease_ack",
                {"attempt_id": attempt_id},
            )
            receipt = validate_lease_attempt_ack(payload, attempt_id, marker)
            if receipt["state"] != "acked" or receipt["consumed"] is not True:
                raise _protocol_error()
            logger.info(
                "[mupot] inbox_lease_ack attempt=%s state=acked",
                attempt_id,
            )
            return
        payload = await self._call_consumer(
            "inbox_ack",
            {"ids": [expected_id]},
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
        runtime = self._runtime_for_event(event)
        current = _delivery_context.get()
        if (
            runtime is None
            or current is None
            or runtime.context is not current
            or self._expire_if_needed(runtime)
        ):
            logger.debug(
                "[mupot] ignoring unbound lifecycle completion event=%s outcome=%s",
                event.message_id,
                outcome,
            )
            return
        logger.info(
            "[mupot] lifecycle complete message=%s outcome=%s",
            runtime.context.source_id,
            outcome,
        )
        runtime.outcome = outcome
        runtime.completion_event.set()

    async def _flush_notifications(self) -> None:
        from .notifications import flush
        if (
            self.notification_activate or self.routine_events_enabled
        ) and self.message_injector is None:
            raise RuntimeError("Mupot notification activation has no native plugin injector")
        await flush(
            self._state,
            self.store,
            self.notification_recipients,
            activate=(
                self.message_injector
                if self.notification_activate or self.routine_events_enabled
                else None
            ),
            activation_default=self.notification_activate,
        )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        runtime = self._runtime_for_current_context()
        context = runtime.context if runtime is not None else None
        recipient = normalize_agent(chat_id)
        metadata = metadata or {}
        context_consistent = (
            context is not None
            and recipient == context.sender
            and (reply_to is None or str(reply_to) == context.source_id)
            and (
                "project_id" not in metadata
                or self._optional_text(metadata.get("project_id")) == context.project
            )
            and (
                "request_id" not in metadata
                or self._optional_text(metadata.get("request_id")) == context.request_id
            )
            and (
                "mupot_message_id" not in metadata
                or str(metadata.get("mupot_message_id") or "") == context.source_id
            )
        )
        if not context_consistent:
            logger.warning("[mupot] response send refused without live delivery context")
            return SendResult(
                success=False,
                error=_GENERIC_DELIVERY_CONTEXT_ERROR,
                retryable=False,
                error_kind="unknown",
            )
        assert runtime is not None
        assert context is not None
        inbound_id = context.source_id
        interim = metadata.get("_interim_send") is True
        arguments: dict[str, Any] = {}
        try:
            if interim:
                arguments = {
                    "to": recipient,
                    "body": str(content),
                    "kind": "ack",
                    "request_id": _progress_request_id(inbound_id, str(content)),
                    "in_reply_to": inbound_id,
                }
                if context.project:
                    arguments["project_id"] = context.project
                result = await asyncio.wait_for(
                    self._send_client.call("send", arguments),
                    timeout=self.rpc_timeout,
                )
                receipt = validate_send_receipt(
                    result,
                    arguments["to"],
                    arguments.get("project_id"),
                )
            else:
                record = self._prepare_final_reply(runtime, recipient, str(content))
                arguments = record["arguments"]
                receipt = await self._transmit_final_reply(record)
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


def register(
    ctx,
    *,
    expected_agent_id=None,
    expected_tenant=None,
    secret_owner: ProfileSecretOwner | None = None,
) -> None:
    # Populated by adapter_factory once Hermes actually connects the platform, so the
    # status tool below can report on the live instance's local state (stranded
    # notifications) without a second, independent path into the adapter's storage.
    _live_adapter: list["MupotAdapter"] = []

    def adapter_factory(config):
        extra = dict(config.extra or {})
        if expected_agent_id is not None or expected_tenant is not None:
            extra.update(expected_agent_id=expected_agent_id, expected_tenant=expected_tenant)

        def client_factory(server_name: str) -> HermesMCPClient:
            return HermesMCPClient(server_name, secret_owner=secret_owner)

        instance = MupotAdapter(
            replace(config, extra=extra),
            client_factory=client_factory,
            message_injector=ctx.inject_message,
            secret_owner=secret_owner,
        )
        _live_adapter[:] = [instance]
        return instance

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

    def gateway_status(args: dict[str, Any]) -> str:
        adapter = _live_adapter[0] if _live_adapter else None
        if adapter is None:
            value = {"ok": False, "error": "native_gateway_not_connected"}
        else:
            value = {"ok": True, "stranded_notifications": adapter.stranded_notifications()}
        return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)

    register_tool = getattr(ctx, "register_tool", None)
    if callable(register_tool):
        register_tool(
            name="mupot_gateway_status",
            handler=gateway_status,
            schema={
                "name": "mupot_gateway_status",
                "description": (
                    "Report native Mupot gateway health that nothing else surfaces, "
                    "including notifications stranded in activation_unknown or "
                    "transport_unknown (interrupted mid-flight; require operator "
                    "reconciliation)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            },
            toolset="mupot-operator",
        )
