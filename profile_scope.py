"""Hermes profile runtime and secret-scope boundary."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


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


def _active_home() -> Path:
    try:
        from hermes_constants import get_process_hermes_home

        # A stale task-local override must never mask that the process moved to
        # another profile after this plugin captured its immutable owner.
        return get_process_hermes_home().resolve(strict=True)
    except Exception:
        raise RuntimeError(_UNAVAILABLE) from None


@dataclass(frozen=True)
class ProfileSecretOwner:
    """Immutable owner of one supported Hermes profile secret scope."""

    home: Path
    device: int
    inode: int

    @classmethod
    def from_context(cls, ctx: Any) -> "ProfileSecretOwner":
        try:
            manager = ctx._manager
            home = Path(manager.home_path).resolve(strict=True)
            active_home = _active_home()
            metadata = home.stat()
        except Exception:
            raise RuntimeError(_UNAVAILABLE) from None
        if active_home != home or not home.is_dir():
            raise RuntimeError(_UNAVAILABLE)
        return cls(home=home, device=metadata.st_dev, inode=metadata.st_ino)

    def _validate(self) -> None:
        require_supported_profile_runtime({})
        try:
            metadata = self.home.stat()
            active_home = _active_home()
        except Exception:
            raise RuntimeError(_UNAVAILABLE) from None
        if (
            not self.home.is_dir()
            or active_home != self.home
            or metadata.st_dev != self.device
            or metadata.st_ino != self.inode
        ):
            raise RuntimeError(_UNAVAILABLE)

    @contextmanager
    def activate(self) -> Iterator[None]:
        """Install a fresh scope for the unchanged owning home, then restore it."""
        self._validate()
        try:
            from agent.secret_scope import (
                build_profile_secret_scope,
                reset_secret_scope,
                set_secret_scope,
            )
            from hermes_constants import (
                reset_hermes_home_override,
                set_hermes_home_override,
            )
        except Exception:
            raise RuntimeError(_UNAVAILABLE) from None

        home_token = set_hermes_home_override(self.home)
        try:
            try:
                secrets = build_profile_secret_scope(self.home)
                secret_token = set_secret_scope(secrets)
            except Exception:
                raise RuntimeError(_UNAVAILABLE) from None
            try:
                yield
            finally:
                reset_secret_scope(secret_token)
        finally:
            reset_hermes_home_override(home_token)

    def read_secret(self, name: str) -> str:
        with self.activate():
            return read_profile_secret(name)

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
