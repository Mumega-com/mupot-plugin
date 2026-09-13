"""Deterministic Telegram project-control relay."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import math
import os
import re
from typing import Any, Mapping
from urllib.parse import urljoin, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener


_COMMANDS = ("start", "needs", "answer", "approve", "reject")
_FORWARDING_MARKERS = (
    "forward_origin",
    "forward_from",
    "forward_from_chat",
    "forward_date",
)
_ENV_NAME = re.compile(r"[A-Z_][A-Z0-9_]{0,127}\Z")
_MAX_REQUEST_BYTES = 32 * 1024
_MAX_RESPONSE_BYTES = 64 * 1024
_MAX_TEXT_CHARS = 4096
_MAX_REPLY_CHARS = 4096
_UNAVAILABLE_REPLY = "Mupot project control is temporarily unavailable."
_REFUSAL_REPLY = (
    "This command is available only in your private, unforwarded Telegram chat."
)


class _NoRedirect(HTTPRedirectHandler):
    """Never forward the webhook secret header to another URL."""

    def redirect_request(
        self,
        req: Any,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


@dataclass(frozen=True)
class TelegramControlSettings:
    enabled: bool
    base_url: str
    webhook_secret_env: str = "IM_WEBHOOK_SECRET"
    timeout: float = 20.0

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "TelegramControlSettings":
        enabled = value.get("telegram_control_enabled", False)
        if not isinstance(enabled, bool):
            raise ValueError("telegram_control_enabled must be a boolean")

        base_url = value.get("base_url")
        if not isinstance(base_url, str) or not base_url.strip():
            raise ValueError("telegram control base_url is required")

        env_name = value.get("telegram_control_webhook_secret_env", "IM_WEBHOOK_SECRET")
        if not isinstance(env_name, str):
            raise ValueError(
                "telegram control webhook secret environment-variable name is invalid"
            )

        timeout_value = value.get(
            "telegram_control_timeout", value.get("timeout", 20.0)
        )
        try:
            timeout = float(timeout_value)
        except (TypeError, ValueError) as exc:
            raise ValueError("telegram control timeout must be numeric") from exc

        settings = cls(
            enabled=enabled,
            base_url=base_url.strip(),
            webhook_secret_env=env_name.strip(),
            timeout=timeout,
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError("telegram control enabled must be a boolean")

        if not isinstance(self.base_url, str) or self.base_url != self.base_url.strip():
            raise ValueError("telegram control base_url must be a valid HTTPS URL")
        parsed = urlparse(self.base_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("telegram control base_url must be an absolute HTTPS URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError(
                "telegram control base_url must not contain credentials, query, or fragment"
            )

        if not isinstance(self.webhook_secret_env, str) or not _ENV_NAME.fullmatch(
            self.webhook_secret_env
        ):
            raise ValueError(
                "telegram control webhook secret environment-variable name is invalid"
            )

        if (
            isinstance(self.timeout, bool)
            or not isinstance(self.timeout, (int, float))
            or not math.isfinite(float(self.timeout))
            or not 1 <= float(self.timeout) <= 120
        ):
            raise ValueError(
                "telegram control timeout must be between 1 and 120 seconds"
            )


def _required_telegram_id(value: Any, label: str) -> int | str:
    if isinstance(value, bool):
        raise ValueError(f"telegram {label} is invalid")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip() and len(value.strip()) <= 64:
        return value.strip()
    raise ValueError(f"telegram {label} is invalid")


def _sanitized_envelope(update: Any) -> dict[str, Any]:
    user = getattr(update, "effective_user", None)
    chat = getattr(update, "effective_chat", None)
    message = getattr(update, "effective_message", None)
    if user is None or chat is None or message is None:
        raise ValueError("telegram private message is required")

    if getattr(chat, "type", None) != "private":
        raise ValueError("telegram project control requires a private chat")
    user_id = _required_telegram_id(getattr(user, "id", None), "user id")
    chat_id = _required_telegram_id(getattr(chat, "id", None), "chat id")
    if str(user_id) != str(chat_id):
        raise ValueError("telegram project control requires a private user chat")

    if any(
        getattr(message, marker, None) is not None for marker in _FORWARDING_MARKERS
    ):
        raise ValueError("forwarded telegram project control messages are refused")

    update_id = getattr(update, "update_id", None)
    if isinstance(update_id, bool) or not isinstance(update_id, int) or update_id < 0:
        raise ValueError("telegram update id is invalid")
    text = getattr(message, "text", None)
    if not isinstance(text, str) or not text:
        raise ValueError("telegram command text is required")
    if len(text) > _MAX_TEXT_CHARS:
        raise RuntimeError("telegram control request exceeds the size limit")
    command = text.split(maxsplit=1)[0].split("@", 1)[0]
    if command not in {f"/{name}" for name in _COMMANDS}:
        raise ValueError("telegram command is not supported")

    display_name = getattr(user, "full_name", "")
    if not isinstance(display_name, str):
        display_name = ""

    return {
        "update_id": update_id,
        "message": {
            "from": {"id": user_id, "first_name": display_name[:256]},
            "chat": {"id": chat_id, "type": "private"},
            "text": text,
        },
    }


def relay_telegram_update(settings: TelegramControlSettings, update: Any) -> str:
    settings.validate()
    if not settings.enabled:
        raise RuntimeError("telegram control is disabled")

    envelope = _sanitized_envelope(update)
    body = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    if len(body) > _MAX_REQUEST_BYTES:
        raise RuntimeError("telegram control request exceeds the size limit")

    secret = os.environ.get(settings.webhook_secret_env, "")
    if not secret or len(secret) > 256:
        raise RuntimeError("telegram control webhook secret is unavailable")

    request = Request(
        urljoin(settings.base_url.rstrip("/") + "/", "/im/webhook"),
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Telegram-Bot-Api-Secret-Token": secret,
        },
        method="POST",
    )
    try:
        with build_opener(_NoRedirect()).open(
            request, timeout=float(settings.timeout)
        ) as response:
            raw = response.read(_MAX_RESPONSE_BYTES + 1)
    except Exception:
        raise RuntimeError("telegram control request failed") from None

    if len(raw) > _MAX_RESPONSE_BYTES:
        raise RuntimeError("telegram control response exceeds the size limit")
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise RuntimeError("telegram control response is invalid") from None
    if not isinstance(parsed, dict) or parsed.get("ok") is not True:
        raise RuntimeError("telegram control response is invalid")
    reply = parsed.get("reply")
    if not isinstance(reply, str) or not reply or len(reply) > _MAX_REPLY_CHARS:
        raise RuntimeError("telegram control response is invalid")
    return reply


def register_telegram_control(ctx: Any, settings: TelegramControlSettings) -> None:
    settings.validate()
    if not settings.enabled:
        return

    wired_applications: list[tuple[Any, list[Any]]] = []

    def factory(application: Any, adapter: Any) -> None:
        # Hermes loads backend plugins without platform SDK dependencies. Keep
        # the optional PTB import inside the native factory invoked at connect.
        from telegram.ext import CommandHandler

        if any(existing is application for existing, _ in wired_applications):
            return

        async def handle(update: Any, context: Any) -> None:
            try:
                reply = await asyncio.to_thread(relay_telegram_update, settings, update)
            except ValueError:
                reply = _REFUSAL_REPLY
            except Exception:
                reply = _UNAVAILABLE_REPLY
            message = getattr(update, "effective_message", None)
            if message is not None:
                await message.reply_text(reply)

        handlers: list[Any] = []
        for command in _COMMANDS:
            handler = CommandHandler(command, handle)
            application.add_handler(handler)
            handlers.append(handler)
        wired_applications.append((application, handlers))

    def unload() -> None:
        for application, handlers in wired_applications:
            for handler in handlers:
                application.remove_handler(handler, group=0)
        wired_applications.clear()

        manager = getattr(ctx, "_manager", None)
        factories = getattr(manager, "_platform_handler_factories", None)
        if not isinstance(factories, dict):
            return
        telegram_factories = factories.get("telegram", [])
        telegram_factories[:] = [
            entry for entry in telegram_factories if entry[0] is not factory
        ]
        if not telegram_factories:
            factories.pop("telegram", None)

    ctx.register_telegram_handler(factory)
    on_unload = getattr(ctx, "on_unload", None)
    if callable(on_unload):
        on_unload(unload)
