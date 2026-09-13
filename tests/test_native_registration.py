"""The single plugin must select one receiver without partially registering an invalid mode."""
import sys
import types
from unittest.mock import patch

import pytest

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
    module = types.ModuleType("plugin.mupot_gateway.adapter")
    module.register = lambda context, **kwargs: calls.append((context, kwargs))
    with patch("plugin._load_plugin_settings", return_value=settings(native_gateway_enabled=True)), \
         patch("plugin._maybe_start_inbox_stream") as legacy, \
         patch("plugin.read_profile_secret", return_value="mupot_test_agent_token"), \
         patch.dict("os.environ", {"MUPOT_AGENT_TOKEN": "mupot_test_agent_token"}), \
         patch.dict(sys.modules, {module.__name__: module}):
        register(ctx)
    assert calls == [(ctx, {"expected_agent_id": "agent-test", "expected_tenant": "tenant-test"})]
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
