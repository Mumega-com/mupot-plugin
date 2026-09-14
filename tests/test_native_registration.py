"""The single plugin must select one receiver without partially registering an invalid mode."""
from contextlib import nullcontext
import sys
import types
from unittest.mock import patch

import pytest

import plugin
from plugin import register


def settings(**overrides):
    return {"mode": "operator", "operator": {
        "base_url": "https://pot.example.invalid", "expected_tenant": "tenant-test",
        "squad_id": "squad-test", "agent_id": "agent-test", "approval_owner": "human-test",
        **overrides}}


class Context:
    def __init__(self):
        self.tools = []
        self.platforms = []
    def register_tool(self, **kwargs):
        self.tools.append(kwargs)
    def register_platform(self, **kwargs):
        self.platforms.append(kwargs)
    def inject_message(self, content, **kwargs):
        return True


def test_native_receiver_registers_from_operator_plugin_and_never_starts_legacy_stream():
    ctx = Context()
    calls = []
    secret_owner = types.SimpleNamespace(
        activate=nullcontext,
        read_secret=lambda _name: "mupot_test_agent_token",
    )
    module = types.ModuleType("plugin.mupot_gateway.adapter")
    module.register = lambda context, **kwargs: calls.append((context, kwargs))
    with patch("plugin._load_plugin_settings", return_value=settings(native_gateway_enabled=True)), \
         patch("plugin._maybe_start_inbox_stream") as legacy, \
         patch("plugin.ProfileSecretOwner.from_context", return_value=secret_owner), \
         patch.dict("os.environ", {"MUPOT_AGENT_TOKEN": "mupot_test_agent_token"}), \
         patch.dict(sys.modules, {module.__name__: module}):
        register(ctx)
    assert calls == [(ctx, {
        "expected_agent_id": "agent-test",
        "expected_tenant": "tenant-test",
        "secret_owner": secret_owner,
    })]
    assert ctx.tools
    legacy.assert_not_called()


@pytest.mark.parametrize("extra", [
    {"native_gateway_enabled": True, "inbox_watch_enabled": True},
    {"native_gateway_enabled": "true"},
])
def test_invalid_receiver_choice_has_no_registration_side_effects(extra):
    ctx = Context()
    with patch("plugin._load_plugin_settings", return_value=settings(**extra)), \
         patch("plugin._maybe_start_inbox_stream"), \
         pytest.raises(ValueError):
        register(ctx)
    assert ctx.tools == []
    assert ctx.platforms == []


def test_switching_to_native_receive_with_an_active_legacy_stream_is_refused():
    """Kills M7 (plugin/__init__.py:205, the _ACTIVE_WATCHERS competing-receiver guard):
    a legacy inbox-stream watcher already running in this process (e.g. a forced plugin
    reload) must block a switch to native_gateway_enabled rather than run both receivers
    against the same state concurrently."""
    ctx = Context()
    with patch("plugin._load_plugin_settings",
               return_value=settings(native_gateway_enabled=True)), \
         patch.dict(plugin._ACTIVE_WATCHERS,
                    {"/tmp/existing-legacy-stream-state.json": object()}, clear=False), \
         pytest.raises(ValueError, match="restart the gateway"):
        register(ctx)
    assert ctx.tools == []
    assert ctx.platforms == []


def test_native_receive_registers_normally_once_no_legacy_stream_is_active():
    """The guard is scoped to an ACTIVE watcher, not a permanent lock: with
    _ACTIVE_WATCHERS empty (the common case), native registration still succeeds."""
    assert plugin._ACTIVE_WATCHERS == {}
    ctx = Context()
    secret_owner = types.SimpleNamespace(
        activate=nullcontext,
        read_secret=lambda _name: "mupot_test_agent_token",
    )
    module = types.ModuleType("plugin.mupot_gateway.adapter")
    module.register = lambda context, **kwargs: None
    with patch("plugin._load_plugin_settings",
               return_value=settings(native_gateway_enabled=True)), \
         patch("plugin.ProfileSecretOwner.from_context", return_value=secret_owner), \
         patch.dict(sys.modules, {module.__name__: module}):
        register(ctx)
    assert ctx.tools
