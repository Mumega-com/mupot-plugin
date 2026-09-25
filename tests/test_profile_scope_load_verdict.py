"""Load-time multiplex verdict after Hermes flipped the multiplex default on.

Regression for the kayhermes outage (2026-09-17 23:11Z -> 2026-09-25): Hermes
a10bbf95bb made ``gateway.multiplex_profiles: True`` a DEFAULT_CONFIG value, so
``load_config()`` reported ``True`` for every profile that never set it. The
plugin read that merged value as "this process multiplexes" and refused to load
("unsupported profile runtime") on a named profile whose gateway Hermes itself
keeps standalone. The load-time check must follow the operator's explicit
choice, and Hermes's own settlement rules when it is unset, and still fail
closed whenever it cannot decide.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any, Callable

import pytest

from plugin.profile_scope import (
    _explicit_multiplex_verdict,
    require_supported_profile_runtime_at_load,
)
from plugin.tests.test_profile_scope import install_secret_scope

# What load_config() returns on the live Hermes rev for a profile that never
# mentions multiplexing (DEFAULT_CONFIG merged in).
MERGED_DEFAULT_ON = {"gateway": {"multiplex_profiles": True}}


def install_hermes_multiplex_surface(
    monkeypatch: pytest.MonkeyPatch,
    *,
    explicit: bool | None | Callable[[Path], Any],
    active_profile: str,
    served_profiles: int = 1,
    home: Path = Path("/hermes/profiles/kayhermes"),
) -> list[Path]:
    """Install the three Hermes surfaces the verdict reads, without Hermes."""

    seen_homes: list[Path] = []

    def explicit_multiplex_flag(default_home: Path) -> bool | None:
        seen_homes.append(default_home)
        if callable(explicit):
            return explicit(default_home)
        return explicit

    mode = types.ModuleType("hermes_cli.gateway_multiplex_mode")
    mode.explicit_multiplex_flag = explicit_multiplex_flag
    profiles = types.ModuleType("hermes_cli.profiles")
    profiles.get_active_profile_name = lambda: active_profile
    profiles.profiles_to_serve = lambda multiplex: [
        (f"p{index}", Path(f"/p{index}")) for index in range(served_profiles)
    ] if multiplex else [(active_profile, home)]
    hermes_cli = types.ModuleType("hermes_cli")
    hermes_cli.__path__ = []
    hermes_cli.gateway_multiplex_mode = mode
    hermes_cli.profiles = profiles
    constants = types.ModuleType("hermes_constants")
    constants.get_hermes_home = lambda: home
    monkeypatch.setitem(sys.modules, "hermes_cli", hermes_cli)
    monkeypatch.setitem(sys.modules, "hermes_cli.gateway_multiplex_mode", mode)
    monkeypatch.setitem(sys.modules, "hermes_cli.profiles", profiles)
    monkeypatch.setitem(sys.modules, "hermes_constants", constants)
    return seen_homes


def test_named_profile_with_merged_default_on_loads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact kayhermes shape: merged default says True, nothing explicit."""
    install_secret_scope(monkeypatch, scope={})
    seen = install_hermes_multiplex_surface(
        monkeypatch, explicit=None, active_profile="kayhermes", served_profiles=30
    )

    assert _explicit_multiplex_verdict(MERGED_DEFAULT_ON) is False
    require_supported_profile_runtime_at_load(MERGED_DEFAULT_ON)
    # The explicit choice is read for THIS profile's home, not a default root.
    assert seen and seen[-1] == Path("/hermes/profiles/kayhermes")


def test_explicit_true_is_refused_even_on_a_named_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_secret_scope(monkeypatch, scope={})
    install_hermes_multiplex_surface(
        monkeypatch, explicit=True, active_profile="kayhermes"
    )

    with pytest.raises(RuntimeError, match="unsupported profile runtime"):
        require_supported_profile_runtime_at_load(MERGED_DEFAULT_ON)


def test_explicit_false_loads_on_a_multi_profile_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_secret_scope(monkeypatch, scope={})
    install_hermes_multiplex_surface(
        monkeypatch, explicit=False, active_profile="default", served_profiles=5
    )

    require_supported_profile_runtime_at_load(MERGED_DEFAULT_ON)


def test_unset_default_profile_single_install_loads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_secret_scope(monkeypatch, scope={})
    install_hermes_multiplex_surface(
        monkeypatch, explicit=None, active_profile="default", served_profiles=1
    )

    require_supported_profile_runtime_at_load(MERGED_DEFAULT_ON)


def test_unset_default_profile_with_other_profiles_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hermes may fold this gateway onto a multiplexer at boot: fail closed."""
    install_secret_scope(monkeypatch, scope={})
    install_hermes_multiplex_surface(
        monkeypatch, explicit=None, active_profile="default", served_profiles=2
    )

    with pytest.raises(RuntimeError, match="unsupported profile runtime"):
        require_supported_profile_runtime_at_load(MERGED_DEFAULT_ON)


def test_undecidable_verdict_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(_home: Path) -> bool | None:
        raise OSError("config.yaml unreadable")

    install_secret_scope(monkeypatch, scope={})
    install_hermes_multiplex_surface(
        monkeypatch, explicit=broken, active_profile="kayhermes"
    )

    with pytest.raises(RuntimeError, match="unsupported profile runtime"):
        require_supported_profile_runtime_at_load({})


def test_runtime_latch_still_refuses_a_standalone_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_secret_scope(monkeypatch, scope={}, multiplex=True)
    install_hermes_multiplex_surface(
        monkeypatch, explicit=False, active_profile="kayhermes"
    )

    with pytest.raises(RuntimeError, match="unsupported profile runtime"):
        require_supported_profile_runtime_at_load({})


def test_older_hermes_without_the_module_uses_the_merged_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-a10bbf95bb Hermes has no implicit default: merged value is explicit."""
    install_secret_scope(monkeypatch, scope={})
    hermes_cli = types.ModuleType("hermes_cli")
    hermes_cli.__path__ = []
    monkeypatch.setitem(sys.modules, "hermes_cli", hermes_cli)
    monkeypatch.delitem(sys.modules, "hermes_cli.gateway_multiplex_mode", raising=False)

    with pytest.raises(RuntimeError, match="unsupported profile runtime"):
        require_supported_profile_runtime_at_load(MERGED_DEFAULT_ON)
    require_supported_profile_runtime_at_load({"gateway": {}})
