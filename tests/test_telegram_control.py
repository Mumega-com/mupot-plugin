"""Contract tests for deterministic Telegram project-control relay."""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import replace
import http.server
import json
import sys
import threading
import types
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, build_opener

import pytest

from plugin import register
from plugin.telegram_control import (
    TelegramControlSettings,
    _NoRedirect,
    register_telegram_control,
    relay_telegram_update,
)


@pytest.fixture(autouse=True)
def simplex_hermes_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    from plugin.tests.test_profile_scope import install_secret_scope

    install_secret_scope(
        monkeypatch,
        scope={
            "TEST_IM_WEBHOOK_SECRET": "test-profile-webhook-secret",
            "MUPOT_AGENT_TOKEN": "test-profile-agent-token",
        },
    )


class User:
    def __init__(self, user_id: int = 123, full_name: str = "Ada Example") -> None:
        self.id = user_id
        self.full_name = full_name
        self.username = "must-not-be-forwarded"


class Chat:
    def __init__(self, chat_id: int = 123, chat_type: str = "private") -> None:
        self.id = chat_id
        self.type = chat_type
        self.title = "must-not-be-forwarded"


class Message:
    def __init__(self, text: str = "/needs", **forwarding: object) -> None:
        self.text = text
        self.message_id = 999
        self.caption = "must-not-be-forwarded"
        for key, value in forwarding.items():
            setattr(self, key, value)


class Update:
    def __init__(
        self,
        *,
        update_id: int = 456,
        user: User | None = None,
        chat: Chat | None = None,
        message: Message | None = None,
    ) -> None:
        self.update_id = update_id
        self.effective_user = user if user is not None else User()
        self.effective_chat = chat if chat is not None else Chat()
        self.effective_message = message if message is not None else Message()
        self.callback_query = "must-not-be-forwarded"


class Response:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.read_sizes: list[int] = []

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        return self.body if size < 0 else self.body[:size]


class Opener:
    def __init__(self, response: Response | BaseException) -> None:
        self.response = response
        self.calls: list[tuple[object, float]] = []

    def open(self, request: object, timeout: float) -> Response:
        self.calls.append((request, timeout))
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


def valid_settings(**changes: object) -> TelegramControlSettings:
    settings = TelegramControlSettings(
        enabled=True,
        base_url="https://pot.example.invalid/base",
        webhook_secret_env="TEST_IM_WEBHOOK_SECRET",
        timeout=7.0,
    )
    return replace(settings, **changes)


def relay_with(
    monkeypatch: pytest.MonkeyPatch,
    update: Update | None = None,
    *,
    response: bytes = b'{"ok":true,"reply":"Needs: approve decision d-1."}',
) -> tuple[str, Opener]:
    monkeypatch.setattr(
        "plugin.telegram_control.read_profile_secret",
        lambda _name: "runtime-webhook-secret",
    )
    opener = Opener(Response(response))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)
    return relay_telegram_update(valid_settings(), update or Update()), opener


def test_settings_are_explicitly_opt_in_and_require_boolean() -> None:
    settings = TelegramControlSettings.from_mapping(
        {"base_url": "https://pot.example.invalid"}
    )
    assert settings.enabled is False
    with pytest.raises(ValueError, match="boolean"):
        TelegramControlSettings.from_mapping(
            {
                "base_url": "https://pot.example.invalid",
                "telegram_control_enabled": "true",
            }
        )


@pytest.mark.parametrize(
    "bad_url",
    [
        "http://pot.example.invalid",
        "https://user:pass@pot.example.invalid",
        "https://pot.example.invalid?secret=value",
        "https://pot.example.invalid#fragment",
        "not-a-url",
    ],
)
def test_settings_require_a_credential_free_https_base_url(bad_url: str) -> None:
    with pytest.raises(ValueError, match="base_url"):
        valid_settings(base_url=bad_url).validate()


@pytest.mark.parametrize(
    "bad_env_name",
    ["", "IM-WEBHOOK-SECRET", "1M_WEBHOOK_SECRET", "lowercase", "A" * 129],
)
def test_settings_validate_secret_environment_variable_name(bad_env_name: str) -> None:
    with pytest.raises(ValueError, match="environment-variable name"):
        valid_settings(webhook_secret_env=bad_env_name).validate()


@pytest.mark.parametrize("bad_timeout", [0, 0.9, 121, float("inf")])
def test_settings_bound_timeout(bad_timeout: float) -> None:
    with pytest.raises(ValueError, match="timeout"):
        valid_settings(timeout=bad_timeout).validate()


def test_disabled_control_registers_no_native_handler() -> None:
    calls: list[object] = []
    ctx = types.SimpleNamespace(register_telegram_handler=calls.append)
    register_telegram_control(ctx, replace(valid_settings(), enabled=False))
    assert calls == []


def test_factory_registers_only_the_five_exact_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factories: list[object] = []
    ctx = types.SimpleNamespace(register_telegram_handler=factories.append)
    register_telegram_control(ctx, valid_settings())
    assert len(factories) == 1

    handlers: list[object] = []

    class CommandHandler:
        def __init__(self, command: str, callback: object) -> None:
            self.command = command
            self.callback = callback

    telegram = types.ModuleType("telegram")
    telegram_ext = types.ModuleType("telegram.ext")
    telegram_ext.CommandHandler = CommandHandler
    monkeypatch.setitem(sys.modules, "telegram", telegram)
    monkeypatch.setitem(sys.modules, "telegram.ext", telegram_ext)
    application = types.SimpleNamespace(add_handler=handlers.append)
    factories[0](application, object())

    assert [handler.command for handler in handlers] == [
        "start",
        "needs",
        "answer",
        "approve",
        "reject",
    ]
    assert all(handler.callback is handlers[0].callback for handler in handlers)


@pytest.mark.parametrize(
    "update",
    [
        Update(chat=Chat(chat_type="group")),
        Update(user=User(user_id=124)),
    ],
)
def test_relay_refuses_non_private_or_mismatched_identity_before_network(
    monkeypatch: pytest.MonkeyPatch, update: Update
) -> None:
    opener = Opener(AssertionError("network must not run"))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)
    with pytest.raises(ValueError, match="private"):
        relay_telegram_update(valid_settings(), update)
    assert opener.calls == []


@pytest.mark.parametrize(
    "marker",
    ["forward_origin", "forward_from", "forward_from_chat", "forward_date"],
)
def test_relay_refuses_every_forwarding_marker_before_network(
    monkeypatch: pytest.MonkeyPatch, marker: str
) -> None:
    opener = Opener(AssertionError("network must not run"))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)
    update = Update(message=Message("/approve d-1", **{marker: object()}))
    with pytest.raises(ValueError, match="forwarded"):
        relay_telegram_update(valid_settings(), update)
    assert opener.calls == []


def test_relay_posts_only_the_sanitized_envelope_and_runtime_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    update = Update(message=Message("/answer d-1 accept"))
    reply, opener = relay_with(monkeypatch, update)
    assert reply == "Needs: approve decision d-1."
    assert len(opener.calls) == 1
    request, timeout = opener.calls[0]
    assert request.full_url == "https://pot.example.invalid/im/webhook"
    assert timeout == 7.0
    headers = {key.lower(): value for key, value in request.header_items()}
    assert headers == {
        "content-type": "application/json",
        "x-telegram-bot-api-secret-token": "runtime-webhook-secret",
    }
    assert json.loads(request.data) == {
        "update_id": 456,
        "message": {
            "from": {"id": 123, "first_name": "Ada Example"},
            "chat": {"id": 123, "type": "private"},
            "text": "/answer d-1 accept",
        },
    }
    assert "runtime-webhook-secret" not in request.data.decode()
    assert "must-not-be-forwarded" not in request.data.decode()


def test_relay_sends_only_the_active_profile_scoped_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugin.tests.test_profile_scope import install_secret_scope

    process_secret = "wrong-process-global-webhook-secret"
    scoped_secret = "right-profile-scoped-webhook-secret"
    monkeypatch.setenv("TEST_IM_WEBHOOK_SECRET", process_secret)
    install_secret_scope(
        monkeypatch,
        scope={"TEST_IM_WEBHOOK_SECRET": scoped_secret},
    )
    opener = Opener(Response(b'{"ok":true,"reply":"Ready."}'))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)

    assert relay_telegram_update(valid_settings(), Update()) == "Ready."

    headers = {key.lower(): value for key, value in opener.calls[0][0].header_items()}
    assert headers["x-telegram-bot-api-secret-token"] == scoped_secret
    assert process_secret not in str(headers)


def test_relay_without_profile_scope_refuses_global_secret_before_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugin.tests.test_profile_scope import install_secret_scope

    process_secret = "must-not-reach-telegram-request"
    monkeypatch.setenv("TEST_IM_WEBHOOK_SECRET", process_secret)
    install_secret_scope(monkeypatch, scope=None)
    opener = Opener(AssertionError("network must not run"))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)

    with pytest.raises(RuntimeError) as failure:
        relay_telegram_update(valid_settings(), Update())

    assert str(failure.value) == "profile secret is unavailable"
    assert process_secret not in str(failure.value)
    assert opener.calls == []


def test_relay_reads_secret_at_request_time(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = valid_settings()
    secrets = {"TEST_IM_WEBHOOK_SECRET": "first-scoped-secret"}
    monkeypatch.setattr(
        "plugin.telegram_control.read_profile_secret",
        lambda name: secrets[name],
    )
    opener = Opener(Response(b'{"ok":true,"reply":"Ready."}'))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)
    assert relay_telegram_update(settings, Update()) == "Ready."
    secrets["TEST_IM_WEBHOOK_SECRET"] = "rotated-scoped-secret"
    assert relay_telegram_update(settings, Update(update_id=457)) == "Ready."
    first_headers = {
        key.lower(): value for key, value in opener.calls[0][0].header_items()
    }
    rotated_headers = {
        key.lower(): value for key, value in opener.calls[1][0].header_items()
    }
    assert first_headers["x-telegram-bot-api-secret-token"] == "first-scoped-secret"
    assert rotated_headers["x-telegram-bot-api-secret-token"] == "rotated-scoped-secret"


def test_relay_bounds_request_before_network(monkeypatch: pytest.MonkeyPatch) -> None:
    opener = Opener(AssertionError("network must not run"))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)
    update = Update(message=Message("/answer " + "x" * 4097))
    with pytest.raises(RuntimeError, match="request"):
        relay_telegram_update(valid_settings(), update)
    assert opener.calls == []


def test_relay_bounds_response_and_never_returns_partial_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = b'{"ok":true,"reply":"' + b"x" * 300_000 + b'"}'
    monkeypatch.setattr(
        "plugin.telegram_control.read_profile_secret",
        lambda _name: "runtime-webhook-secret",
    )
    body = Response(response)
    opener = Opener(body)
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)
    with pytest.raises(RuntimeError, match="response"):
        relay_telegram_update(valid_settings(), Update())
    assert body.read_sizes and body.read_sizes[0] < len(response)


@pytest.mark.parametrize(
    "response",
    [
        b"not-json",
        b'{"ok":false,"reply":"do not trust this"}',
        b'{"ok":true,"reply":17}',
        json.dumps({"ok": True, "reply": "x" * 5000}).encode(),
    ],
)
def test_relay_refuses_malformed_or_unbounded_reply(
    monkeypatch: pytest.MonkeyPatch, response: bytes
) -> None:
    with pytest.raises(RuntimeError, match="response"):
        relay_with(monkeypatch, response=response)


def test_relay_failure_is_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "sensitive-secret-value"
    approval = "/approve invite-code-123"
    monkeypatch.setattr(
        "plugin.telegram_control.read_profile_secret", lambda _name: secret
    )
    opener = Opener(RuntimeError(f"failed with {secret} for {approval}"))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)
    with pytest.raises(RuntimeError) as failure:
        relay_telegram_update(valid_settings(), Update(message=Message(approval)))
    rendered = str(failure.value)
    assert secret not in rendered
    assert "invite-code-123" not in rendered
    assert approval not in rendered


@pytest.mark.asyncio
async def test_native_callback_returns_safe_failure_without_starting_an_llm_turn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factories: list[object] = []
    handlers: list[object] = []

    class CommandHandler:
        def __init__(self, command: str, callback: object) -> None:
            self.command = command
            self.callback = callback

    telegram = types.ModuleType("telegram")
    telegram_ext = types.ModuleType("telegram.ext")
    telegram_ext.CommandHandler = CommandHandler
    monkeypatch.setitem(sys.modules, "telegram", telegram)
    monkeypatch.setitem(sys.modules, "telegram.ext", telegram_ext)
    monkeypatch.setattr(
        "plugin.telegram_control.relay_telegram_update",
        lambda *_: (_ for _ in ()).throw(RuntimeError("sensitive approval body")),
    )
    register_telegram_control(
        types.SimpleNamespace(register_telegram_handler=factories.append),
        valid_settings(),
    )
    factories[0](types.SimpleNamespace(add_handler=handlers.append), object())
    replies: list[str] = []

    async def reply_text(text: str) -> None:
        replies.append(text)

    update = Update()
    update.effective_message.reply_text = reply_text
    await handlers[0].callback(update, object())
    assert replies == ["Mupot project control is temporarily unavailable."]
    assert "sensitive approval body" not in replies[0]


@pytest.mark.asyncio
async def test_runtime_multiplex_activation_after_registration_refuses_relay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugin.tests.test_profile_scope import install_secret_scope

    runtime = {"multiplex": False}
    secret_scope = install_secret_scope(
        monkeypatch,
        scope={"TEST_IM_WEBHOOK_SECRET": "profile-secret"},
    )
    secret_scope.is_multiplex_active = lambda: runtime["multiplex"]
    factories: list[object] = []
    handlers: list[object] = []

    class CommandHandler:
        def __init__(self, command: str, callback: object) -> None:
            self.command = command
            self.callback = callback

    telegram = types.ModuleType("telegram")
    telegram_ext = types.ModuleType("telegram.ext")
    telegram_ext.CommandHandler = CommandHandler
    monkeypatch.setitem(sys.modules, "telegram", telegram)
    monkeypatch.setitem(sys.modules, "telegram.ext", telegram_ext)
    register_telegram_control(
        types.SimpleNamespace(register_telegram_handler=factories.append),
        valid_settings(),
    )
    factories[0](types.SimpleNamespace(add_handler=handlers.append), object())
    opener = Opener(AssertionError("network must not run"))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)
    replies: list[str] = []

    async def reply_text(text: str) -> None:
        replies.append(text)

    update = Update()
    update.effective_message.reply_text = reply_text
    runtime["multiplex"] = True

    await handlers[0].callback(update, object())

    assert replies == ["Mupot project control is temporarily unavailable."]
    assert opener.calls == []


def operator_settings(**overrides: object) -> dict[str, object]:
    return {
        "mode": "operator",
        "operator": {
            "base_url": "https://pot.example.invalid",
            "expected_tenant": "tenant-test",
            "squad_id": "squad-test",
            "agent_id": "agent-test",
            "approval_owner": "human-test",
            "telegram_control_enabled": True,
            "telegram_control_webhook_secret_env": "TEST_IM_WEBHOOK_SECRET",
            **overrides,
        },
    }


class RegistrationContext:
    def __init__(self) -> None:
        self.events: list[str] = []

    def register_telegram_handler(self, factory: object) -> None:
        self.events.append("telegram")

    def register_tool(self, **_: object) -> None:
        self.events.append("tool")


def test_plugin_registers_telegram_before_native_and_operator_side_effects() -> None:
    ctx = RegistrationContext()
    secret_owner = types.SimpleNamespace(
        activate=nullcontext,
        read_secret=lambda _name: "mupot_test_agent_token",
    )
    native = types.ModuleType("plugin.mupot_gateway.adapter")
    native.register = lambda *_args, **_kwargs: ctx.events.append("native")
    with (
        patch(
            "plugin._load_plugin_settings",
            return_value=operator_settings(native_gateway_enabled=True),
        ),
        patch("plugin.ProfileSecretOwner.from_context", return_value=secret_owner),
        patch.dict("os.environ", {"MUPOT_AGENT_TOKEN": "mupot_test_agent_token"}),
        patch.dict(sys.modules, {native.__name__: native}),
    ):
        register(ctx)
    assert ctx.events[0] == "telegram"
    assert ctx.events[1] == "native"
    assert "tool" in ctx.events[2:]


def test_plugin_registration_injects_secret_reader_without_resolving_token() -> None:
    ctx = RegistrationContext()
    captured: list[object] = []
    secret_reader = Mock(
        side_effect=AssertionError("registration must not read the token")
    )
    secret_owner = types.SimpleNamespace(
        activate=nullcontext,
        read_secret=secret_reader,
    )

    class Client:
        def __init__(self, _settings: object, **kwargs: object) -> None:
            captured.append(kwargs.get("secret_reader"))

    with (
        patch(
            "plugin._load_plugin_settings",
            return_value=operator_settings(native_gateway_enabled=False),
        ),
        patch("plugin.MupotOperatorClient", Client),
        patch("plugin.ProfileSecretOwner.from_context", return_value=secret_owner),
        patch("plugin.register_operator_tools"),
        patch("plugin._maybe_start_inbox_stream"),
    ):
        register(ctx)

    assert captured == [secret_reader]
    secret_reader.assert_not_called()


def test_invalid_telegram_config_leaves_no_partial_plugin_surface() -> None:
    ctx = RegistrationContext()
    with (
        patch(
            "plugin._load_plugin_settings",
            return_value=operator_settings(
                telegram_control_webhook_secret_env="invalid-secret-env"
            ),
        ),
        pytest.raises(ValueError, match="environment-variable name"),
    ):
        register(ctx)
    assert ctx.events == []


def test_relay_refuses_an_unsupported_command_before_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kills M12 (telegram_control.py command-allowlist refusal): asserts the actual
    relay refusal behavior for a command outside _COMMANDS, not merely that the five
    supported commands got registered (test_factory_registers_only_the_five_exact_commands
    proves registration shape only)."""
    opener = Opener(AssertionError("network must not run"))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)
    update = Update(message=Message("/shutdown"))
    with pytest.raises(ValueError, match="not supported"):
        relay_telegram_update(valid_settings(), update)
    assert opener.calls == []


@pytest.mark.asyncio
async def test_native_callback_replies_with_refusal_for_an_unsupported_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same refusal (M12) proven through the actual registered PTB callback, not
    just through relay_telegram_update directly."""
    factories: list[object] = []
    handlers: list[object] = []

    class CommandHandler:
        def __init__(self, command: str, callback: object) -> None:
            self.command = command
            self.callback = callback

    telegram = types.ModuleType("telegram")
    telegram_ext = types.ModuleType("telegram.ext")
    telegram_ext.CommandHandler = CommandHandler
    monkeypatch.setitem(sys.modules, "telegram", telegram)
    monkeypatch.setitem(sys.modules, "telegram.ext", telegram_ext)
    opener = Opener(AssertionError("network must not run"))
    monkeypatch.setattr("plugin.telegram_control.build_opener", lambda *_: opener)
    register_telegram_control(
        types.SimpleNamespace(register_telegram_handler=factories.append),
        valid_settings(),
    )
    factories[0](types.SimpleNamespace(add_handler=handlers.append), object())
    replies: list[str] = []

    async def reply_text(text: str) -> None:
        replies.append(text)

    update = Update(message=Message("/shutdown"))
    update.effective_message.reply_text = reply_text
    await handlers[0].callback(update, object())
    assert replies == [
        "This command is available only in your private, unforwarded Telegram chat."
    ]
    assert opener.calls == []


def test_no_redirect_refuses_a_real_302_and_never_forwards_the_secret_header() -> None:
    """Kills M18 (telegram_control.py:39 _NoRedirect): a real local HTTP server issues
    a genuine 302, and a second real local HTTP server stands in as the attacker-owned
    redirect target. Proves both that the request is refused (raised, not silently
    followed) and that the secret header never reaches the second server -- against
    real sockets, not a mock."""
    captured: dict[str, object] = {}

    class TargetHandler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler naming)
            captured["hit"] = True
            captured["headers"] = dict(self.headers)
            body = b'{"ok":true,"reply":"leaked"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: object) -> None:
            return None

    target = http.server.ThreadingHTTPServer(("127.0.0.1", 0), TargetHandler)
    target_thread = threading.Thread(target=target.serve_forever, daemon=True)
    target_thread.start()
    target_port = target.server_address[1]

    class RedirectHandler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{target_port}/attacker")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_args: object) -> None:
            return None

    redirector = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
    redirector_thread = threading.Thread(target=redirector.serve_forever, daemon=True)
    redirector_thread.start()
    redirector_port = redirector.server_address[1]

    try:
        request = Request(
            f"http://127.0.0.1:{redirector_port}/im/webhook",
            data=b"{}",
            headers={
                "Content-Type": "application/json",
                "X-Telegram-Bot-Api-Secret-Token": "must-not-leak-to-redirect-target",
            },
            method="POST",
        )
        with pytest.raises(HTTPError) as failure:
            build_opener(_NoRedirect()).open(request, timeout=5)
        assert failure.value.code == 302
    finally:
        target.shutdown()
        redirector.shutdown()
        target_thread.join(timeout=5)
        redirector_thread.join(timeout=5)

    assert captured == {}
