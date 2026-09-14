"""Fail-closed profile runtime and credential boundary tests."""

from __future__ import annotations

import os
import sys
import types

import pytest

from plugin import register
from plugin.profile_scope import (
    read_profile_secret,
    require_supported_profile_runtime,
)


def install_secret_scope(
    monkeypatch: pytest.MonkeyPatch,
    *,
    scope: dict[str, str] | None,
    multiplex: bool = False,
    failure: BaseException | None = None,
) -> types.ModuleType:
    """Install the pinned Hermes secret-scope surface without Hermes itself."""

    secret_scope = types.ModuleType("agent.secret_scope")
    secret_scope.current_secret_scope = lambda: scope
    secret_scope.is_multiplex_active = lambda: multiplex

    def get_secret(name: str, default: str | None = None) -> str | None:
        if failure is not None:
            raise failure
        if scope is not None:
            value = scope.get(name)
            if value is not None:
                return value
            return default if multiplex else os.environ.get(name, default)
        # This mirrors the pinned simplex behavior. The plugin must make an
        # installed scope authoritative and refuse this fallback after a miss.
        return os.environ.get(name, default)

    secret_scope.get_secret = get_secret
    agent = types.ModuleType("agent")
    agent.__path__ = []
    agent.secret_scope = secret_scope
    monkeypatch.setitem(sys.modules, "agent", agent)
    monkeypatch.setitem(sys.modules, "agent.secret_scope", secret_scope)
    return secret_scope


def test_scoped_secret_wins_over_distinct_process_global_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MUPOT_AGENT_TOKEN", "wrong-process-global-secret")
    install_secret_scope(
        monkeypatch,
        scope={"MUPOT_AGENT_TOKEN": "right-profile-scoped-secret"},
    )

    assert read_profile_secret("MUPOT_AGENT_TOKEN") == "right-profile-scoped-secret"


def test_absent_scope_never_falls_back_to_process_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process_secret = "must-not-satisfy-absent-profile-scope"
    monkeypatch.setenv("MUPOT_AGENT_TOKEN", process_secret)
    install_secret_scope(monkeypatch, scope=None)

    with pytest.raises(RuntimeError) as failure:
        read_profile_secret("MUPOT_AGENT_TOKEN")

    assert str(failure.value) == "profile secret is unavailable"
    assert process_secret not in str(failure.value)


def test_installed_scope_miss_never_falls_back_to_process_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process_secret = "must-not-cross-profile-boundary"
    monkeypatch.setenv("MUPOT_AGENT_TOKEN", process_secret)
    install_secret_scope(monkeypatch, scope={})

    with pytest.raises(RuntimeError) as failure:
        read_profile_secret("MUPOT_AGENT_TOKEN")

    assert process_secret not in str(failure.value)


def test_null_scoped_secret_never_falls_back_to_process_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process_secret = "must-not-satisfy-null-profile-secret"
    monkeypatch.setenv("MUPOT_AGENT_TOKEN", process_secret)
    install_secret_scope(
        monkeypatch,
        scope={"MUPOT_AGENT_TOKEN": None},  # type: ignore[dict-item]
    )

    with pytest.raises(RuntimeError) as failure:
        read_profile_secret("MUPOT_AGENT_TOKEN")

    assert str(failure.value) == "profile secret is unavailable"
    assert process_secret not in str(failure.value)


@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_or_empty_profile_secret_fails_with_generic_error(
    monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    scope = {} if value is None else {"MUPOT_AGENT_TOKEN": value}
    install_secret_scope(monkeypatch, scope=scope)

    with pytest.raises(RuntimeError) as failure:
        read_profile_secret("MUPOT_AGENT_TOKEN")

    assert str(failure.value) == "profile secret is unavailable"


def test_secret_provider_failure_is_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "provider-error-secret-value"
    install_secret_scope(
        monkeypatch,
        scope=None,
        failure=RuntimeError(f"provider failed while reading {secret}"),
    )

    with pytest.raises(RuntimeError) as failure:
        read_profile_secret("MUPOT_AGENT_TOKEN")

    assert str(failure.value) == "profile secret is unavailable"
    assert secret not in str(failure.value)


@pytest.mark.parametrize(
    "config",
    [
        {"gateway": {"multiplex_profiles": True}},
        {"multiplex_profiles": True},
        types.SimpleNamespace(multiplex_profiles=True),
    ],
)
def test_configured_multiplex_runtime_is_rejected(config: object) -> None:
    with pytest.raises(RuntimeError, match="unsupported profile runtime"):
        require_supported_profile_runtime(config)


def test_runtime_multiplex_activation_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_secret_scope(monkeypatch, scope={}, multiplex=True)

    with pytest.raises(RuntimeError, match="unsupported profile runtime"):
        require_supported_profile_runtime({})


def test_configured_multiplex_registers_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    ctx = types.SimpleNamespace(
        register_tool=lambda **_: events.append("tool"),
        register_platform=lambda **_: events.append("platform"),
        register_telegram_handler=lambda *_: events.append("telegram"),
    )
    raw_config = {
        "gateway": {"multiplex_profiles": True},
        "plugins": {
            "entries": {
                "mupot": {
                    "settings": {
                        "mode": "operator",
                        "operator": {"base_url": "https://pot.example.invalid"},
                    }
                }
            }
        },
    }
    config_module = types.ModuleType("hermes_cli.config")
    config_module.load_config = lambda: raw_config
    config_module.cfg_get = lambda *_args, **_kwargs: raw_config["plugins"][
        "entries"
    ]["mupot"]["settings"]
    hermes_cli = types.ModuleType("hermes_cli")
    hermes_cli.__path__ = []
    hermes_cli.config = config_module
    monkeypatch.setitem(sys.modules, "hermes_cli", hermes_cli)
    monkeypatch.setitem(sys.modules, "hermes_cli.config", config_module)
    install_secret_scope(monkeypatch, scope={})

    with pytest.raises(RuntimeError, match="unsupported profile runtime"):
        register(ctx)

    assert events == []
