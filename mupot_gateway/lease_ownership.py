"""Exact durable ownership proof for replaying one source acknowledgement."""

from __future__ import annotations

import copy
import re
from typing import Any, Mapping


_VERSION = 1
# Single shared pattern: adapter.py imports this instead of keeping its own copy
# (two copies of one predicate drift silently; see _LEASE_ATTEMPT_ID_RE there).
ATTEMPT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
_ATTEMPT_ID_RE = ATTEMPT_ID_RE
_OWNER_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")
_SCOPE_FIELDS = (
    "tenant",
    "agent_id",
    "effective_inbox_seat",
    "mode",
    "generation",
)


class AckOwnershipError(ValueError):
    """The persisted acknowledgement owner is absent, malformed, or ambiguous."""


def legacy_ack_ownership() -> dict[str, Any]:
    """Identify work that positively originated outside an attempt lease."""
    return {"version": _VERSION, "kind": "legacy_non_attempt"}


def attempt_ack_ownership(marker: Mapping[str, Any]) -> dict[str, Any]:
    """Copy an exact attempt owner from a validated durable v3 lease marker."""
    value = {
        "version": _VERSION,
        "kind": "attempt",
        "attempt_id": marker.get("attempt_id"),
        "tenant": marker.get("tenant"),
        "agent_id": marker.get("agent_id"),
        "effective_inbox_seat": marker.get("effective_inbox_seat"),
        "mode": marker.get("mode"),
        "generation": marker.get("generation"),
        "profile_owner_fingerprint": marker.get("profile_owner_fingerprint"),
    }
    return validate_ack_ownership(value)


def validate_ack_ownership(value: Any) -> dict[str, Any]:
    """Return one exact ownership record or reject it without normalization."""
    if not isinstance(value, dict) or value.get("version") != _VERSION:
        raise AckOwnershipError("acknowledgement ownership is unavailable")
    if value.get("kind") == "legacy_non_attempt":
        if set(value) != {"version", "kind"}:
            raise AckOwnershipError("legacy acknowledgement ownership is invalid")
        return copy.deepcopy(value)
    expected = {
        "version",
        "kind",
        "attempt_id",
        *_SCOPE_FIELDS,
        "profile_owner_fingerprint",
    }
    if value.get("kind") != "attempt" or set(value) != expected:
        raise AckOwnershipError("attempt acknowledgement ownership is invalid")
    attempt_id = value.get("attempt_id")
    tenant = value.get("tenant")
    agent_id = value.get("agent_id")
    seat = value.get("effective_inbox_seat")
    owner = value.get("profile_owner_fingerprint")
    if (
        not isinstance(attempt_id, str)
        or _ATTEMPT_ID_RE.fullmatch(attempt_id) is None
        or not isinstance(tenant, str)
        or not tenant.strip()
        or tenant != tenant.strip()
        or not isinstance(agent_id, str)
        or not agent_id.strip()
        or agent_id != agent_id.strip()
        or not (
            seat is None
            or isinstance(seat, str) and bool(seat.strip()) and seat == seat.strip()
        )
        or value.get("mode") not in {"bearer_only", "gateway"}
        or type(value.get("generation")) is not int
        or value["generation"] < 0
        or not isinstance(owner, str)
        or _OWNER_FINGERPRINT_RE.fullmatch(owner) is None
    ):
        raise AckOwnershipError("attempt acknowledgement ownership is invalid")
    return copy.deepcopy(value)
