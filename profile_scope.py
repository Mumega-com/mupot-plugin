"""Hermes profile runtime and secret-scope boundary."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
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


def _explicit_multiplex_verdict(merged_config: Any) -> bool:
    """True when this process may end up multiplexing profiles.

    Load-time half of the runtime fence. ``hermes_cli.config.load_config()``
    returns DEFAULT_CONFIG merged with config.yaml, and Hermes a10bbf95bb
    (2026-09-16) flipped ``gateway.multiplex_profiles`` to ``True`` in
    DEFAULT_CONFIG. That default is a *request* the gateway settles at boot
    (``hermes_cli.gateway_multiplex_mode.resolve_multiplex_mode``): a named
    profile's own gateway always stays standalone. Reading the merged value
    therefore refused every named-profile install with "unsupported profile
    runtime" even though the process never multiplexes -- the kayhermes
    native gateway failed to load on every start from 2026-09-17 23:11Z.

    Decision, mirroring Hermes's own settlement and failing closed:
    - explicit ``true`` (env ``GATEWAY_MULTIPLEX_PROFILES`` or config.yaml) ->
      multiplex;
    - explicit ``false`` -> standalone;
    - unset -> standalone when Hermes's first two ``implicit_multiplex_blocker``
      rules already guarantee it (a non-default profile's own gateway, or a
      single-profile install -- both pure reads). A default profile with other
      profiles on the host may still be folded onto a multiplexer at boot, so
      it is refused until the operator pins ``gateway.multiplex_profiles``.
    Older Hermes without ``gateway_multiplex_mode`` has no implicit default,
    so the merged config value is still authoritative there. Any other
    failure to decide is treated as multiplex.
    """
    try:
        from hermes_cli.gateway_multiplex_mode import explicit_multiplex_flag
    except ImportError:
        return _configured_multiplex(merged_config)
    try:
        from hermes_cli.profiles import get_active_profile_name, profiles_to_serve
        from hermes_constants import get_hermes_home

        explicit = explicit_multiplex_flag(get_hermes_home())
        if explicit is not None:
            return bool(explicit)
        if (get_active_profile_name() or "default") != "default":
            return False
        return len(profiles_to_serve(multiplex=True)) >= 2
    except Exception:
        return True


def require_supported_profile_runtime_at_load(merged_config: Any) -> None:
    """Plugin-load variant of :func:`require_supported_profile_runtime`.

    Checks the live runtime latch exactly as before, then the operator's
    explicit multiplex choice instead of the merged-default value (see
    :func:`_explicit_multiplex_verdict`). Every later secret read still
    re-checks the runtime latch through ``require_supported_profile_runtime``.
    """
    require_supported_profile_runtime({})
    if _explicit_multiplex_verdict(merged_config):
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

    @property
    def fingerprint(self) -> str:
        """Return a non-secret identity digest for this immutable profile home."""
        payload = (
            f"mupot-profile-owner-v1\0{self.home}\0{self.device}\0{self.inode}"
        ).encode("utf-8")
        return sha256(payload).hexdigest()

    def validated_fingerprint(self) -> str:
        """Revalidate the active profile before returning its identity digest."""
        self._validate()
        return self.fingerprint

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

    @classmethod
    def from_active_home(cls) -> "ProfileSecretOwner":
        """Capture the current simplex profile for direct adapter callers."""
        try:
            home = _active_home()
            metadata = home.stat()
        except Exception:
            raise RuntimeError(_UNAVAILABLE) from None
        if not home.is_dir():
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
