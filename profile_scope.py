"""Hermes profile runtime and secret-scope boundary."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


_UNAVAILABLE = "profile secret is unavailable"
_UNSUPPORTED_RUNTIME = "unsupported profile runtime"
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})


def _configured_multiplex(config: Any) -> bool:
    if isinstance(config, Mapping):
        value = config.get("multiplex_profiles")
        gateway = config.get("gateway")
        if value is None and isinstance(gateway, Mapping):
            value = gateway.get("multiplex_profiles")
    else:
        value = getattr(config, "multiplex_profiles", None)
    if isinstance(value, str):
        return value.strip().lower() in _TRUE_VALUES
    return bool(value)


def require_supported_profile_runtime(config: Any) -> None:
    """Require the plugin's supported one-process-per-profile runtime."""
    try:
        from agent.secret_scope import is_multiplex_active

        runtime_multiplex = bool(is_multiplex_active())
    except Exception:
        raise RuntimeError(_UNSUPPORTED_RUNTIME) from None

    if _configured_multiplex(config) or runtime_multiplex:
        raise RuntimeError(_UNSUPPORTED_RUNTIME)

def read_profile_secret(name: str) -> str:
    """Read one credential through Hermes's active profile scope."""
    try:
        require_supported_profile_runtime({})
    except RuntimeError:
        raise RuntimeError(_UNSUPPORTED_RUNTIME) from None

    try:
        from agent.secret_scope import current_secret_scope, get_secret

        scope = current_secret_scope()
        if scope is None:
            raise RuntimeError(_UNAVAILABLE)
        # In pinned Hermes simplex mode get_secret intentionally falls through
        # to the process environment after an installed-scope miss. For this
        # plugin an installed scope is authoritative: another profile's global
        # value must never satisfy the miss.
        scoped_value = scope.get(name)
        if not isinstance(scoped_value, str) or not scoped_value.strip():
            raise RuntimeError(_UNAVAILABLE)
        value = get_secret(name, None)
    except Exception:
        raise RuntimeError(_UNAVAILABLE) from None

    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(_UNAVAILABLE)
    return value
