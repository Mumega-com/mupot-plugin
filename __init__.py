"""Mupot Hermes backend plugin entry point.

The plugin has two intentionally separate modes:

* ``provisioner`` keeps the human-controlled Cloudflare setup tools.
* ``operator`` registers only the restricted, agent-bound Mupot action wrappers.

A single Hermes profile never receives both surfaces. Production agents use
``operator`` mode; provisioning belongs in a separate human-controlled profile.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

from .mupot_operator import MupotOperatorClient, OperatorSettings, register_operator_tools
from .schemas import (
    MUPOT_BRAIN_ENABLE_SCHEMA,
    MUPOT_PROVISION_SCHEMA,
    MUPOT_STATUS_SCHEMA,
)
from .tools import mupot_brain_enable, mupot_provision, mupot_status

# Process-global registry of running inbox streamers keyed by state-file path.
# Prevents duplicate daemon threads when the plugin is force-reloaded within
# one process; each Hermes home runs its own process.
_ACTIVE_WATCHERS: dict[str, Any] = {}


def _load_plugin_settings() -> dict[str, Any]:
    """Load non-secret Mupot settings from the active Hermes profile."""

    try:
        from hermes_cli.config import cfg_get, load_config

        config = load_config()
        value = cfg_get(config, "plugins", "entries", "mupot", "settings", default={})
        if not isinstance(value, Mapping):
            raise ValueError("plugins.entries.mupot.settings must be a mapping")
        return dict(value)
    except (ImportError, OSError, TypeError, ValueError) as exc:
        raise RuntimeError("unable to load Mupot plugin settings; no tools were registered") from exc


def _tool_schema(name: str, description: str, parameters: dict[str, Any]) -> dict[str, Any]:
    return {"name": name, "description": description, "parameters": parameters}


def _result(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def _register_provisioner_tools(ctx: Any) -> None:
    """Register the legacy human-controlled setup surface using the current API."""

    def provision(args: dict[str, Any]) -> str:
        values = dict(args)
        values.setdefault("cf_account_id", os.environ.get("MUPOT_CF_ACCOUNT_ID", ""))
        values.setdefault("cf_api_token", os.environ.get("MUPOT_CF_API_TOKEN", ""))
        return _result(mupot_provision(**values))

    def status(args: dict[str, Any]) -> str:
        return _result(mupot_status(**args))

    def brain_enable(args: dict[str, Any]) -> str:
        return _result(mupot_brain_enable(**args))

    registrations = (
        (
            "mupot_provision",
            provision,
            MUPOT_PROVISION_SCHEMA,
            "Idempotently plan or provision a human-owned Mupot Cloudflare deployment.",
        ),
        (
            "mupot_status",
            status,
            MUPOT_STATUS_SCHEMA,
            "Probe a Mupot deployment health endpoint.",
        ),
        (
            "mupot_brain_enable",
            brain_enable,
            MUPOT_BRAIN_ENABLE_SCHEMA,
            "Plan a Mupot DMN brain profile and schedule.",
        ),
    )
    for name, handler, parameters, description in registrations:
        ctx.register_tool(
            name=name,
            handler=handler,
            schema=_tool_schema(name, description, parameters),
            toolset="mupot-provisioner",
        )


def _register_provisioner_reminder(ctx: Any) -> None:
    reminded: set[str] = set()

    def on_session_start(event: Any) -> None:
        session_id = str(getattr(event, "session_id", getattr(event, "id", "default")))
        if session_id in reminded:
            return
        reminded.add(session_id)
        if os.environ.get("MUPOT_CF_ACCOUNT_ID", "").strip():
            return
        inject = getattr(ctx, "inject_message", None)
        if callable(inject):
            inject(
                "[mupot] Provisioner mode is active, but no Cloudflare account is configured. "
                "Use mupot_provision after supplying a scoped Cloudflare credential."
            )

    register_hook = getattr(ctx, "register_hook", None)
    if callable(register_hook):
        register_hook("on_session_start", on_session_start)
    legacy_on = getattr(ctx, "on", None)
    if callable(legacy_on):
        legacy_on("on_session_start", on_session_start)


def _maybe_start_inbox_stream(
    ctx: Any, operator_value: Mapping[str, Any]
) -> None:
    """Start the event-driven inbox stream when operator settings enable it.

    Holds one persistent SSE connection per source (SOS bus ``/watch`` +
    Mupot ``/api/inbox/stream``) and pushes NEW messages into the live
    conversation as they arrive — no polling, no cron, no watcher.

    Fail-closed on bad configuration (explicit opt-in), but a runtime
    delivery limitation (no inject_message surface) degrades to macOS
    notifications rather than blocking registration.
    """
    from .inbox_stream import InboxStream, InboxStreamSettings

    stream_settings = InboxStreamSettings.from_mapping(operator_value)
    if not stream_settings.enabled:
        return

    # Resolve the default state file against the ACTIVE Hermes home so two
    # homes (desktop vs CLI) keep separate cursors and never race on one file.
    state_file = stream_settings.state_file
    if state_file == "~/.hermes/mupot-inbox-watch-state.json":
        home = os.environ.get("HERMES_HOME", "").strip() or "~/.hermes"
        state_file = os.path.join(home, "mupot-inbox-watch-state.json")

    key = os.path.expanduser(state_file)
    existing = _ACTIVE_WATCHERS.get(key)
    if existing is not None:
        return  # already running in this process (e.g. forced plugin reload)

    def deliver(text: str) -> bool:
        inject = getattr(ctx, "inject_message", None)
        if not callable(inject):
            return False
        try:
            return bool(inject(text))
        except Exception:
            return False

    stream = InboxStream(
        stream_settings,
        deliver=deliver,
        state_path=Path(state_file),
    )

    def on_session_start(**kwargs: Any) -> None:
        # Record the live session for routing/log context. Delivery itself uses
        # ctx.inject_message (CLI/desktop loop reference), not the session key.
        session_id = kwargs.get("session_id")
        if session_id:
            stream.set_session_key(str(session_id))

    register_hook = getattr(ctx, "register_hook", None)
    if callable(register_hook):
        register_hook("on_session_start", on_session_start)

    stream.start()
    _ACTIVE_WATCHERS[key] = stream


def register(ctx: Any) -> None:
    settings = _load_plugin_settings()
    configured_mode = settings.get("mode") or os.environ.get("MUPOT_PLUGIN_MODE")
    if not isinstance(configured_mode, str) or not configured_mode.strip():
        raise ValueError("Mupot plugin mode must be explicitly set to 'operator' or 'provisioner'")
    mode = configured_mode.strip().lower()

    if mode == "operator":
        operator_value = settings.get("operator", settings)
        if not isinstance(operator_value, Mapping):
            raise ValueError("plugins.entries.mupot.settings.operator must be a mapping")
        native_gateway = operator_value.get("native_gateway_enabled", False)
        if not isinstance(native_gateway, bool):
            raise ValueError("native_gateway_enabled must be a boolean")
        if native_gateway and operator_value.get("inbox_watch_enabled"):
            raise ValueError("native_gateway_enabled and inbox_watch_enabled are mutually exclusive")
        if native_gateway and _ACTIVE_WATCHERS:
            raise ValueError("restart the gateway before switching an active legacy inbox stream to native receive")
        operator_settings = OperatorSettings.from_mapping(operator_value)
        token = os.environ.get("MUPOT_AGENT_TOKEN", "")
        client = MupotOperatorClient(operator_settings, token=token)
        if native_gateway:
            from .mupot_gateway.adapter import register as register_native_gateway
            register_native_gateway(ctx, expected_agent_id=operator_settings.agent_id,
                                    expected_tenant=operator_settings.expected_tenant)
        register_operator_tools(ctx, client)
        if not native_gateway:
            _maybe_start_inbox_stream(ctx, operator_value)
        return

    if mode == "provisioner":
        _register_provisioner_tools(ctx)
        _register_provisioner_reminder(ctx)
        return

    raise ValueError("Mupot plugin mode must be 'operator' or 'provisioner'")
