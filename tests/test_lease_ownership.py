"""Unit tests for the shared lease/ACK-ownership predicate.

Kills M17 (mupot_gateway/lease_ownership.py:71, attempt_id format check): asserts
attempt_ack_ownership/validate_ack_ownership actually refuse a malformed attempt_id,
not merely that a well-formed one is accepted.
"""
from __future__ import annotations

import pytest

from plugin.mupot_gateway.lease_ownership import (
    ATTEMPT_ID_RE,
    AckOwnershipError,
    attempt_ack_ownership,
    legacy_ack_ownership,
    validate_ack_ownership,
)


VALID_MARKER = {
    "attempt_id": "a" * 16,
    "tenant": "tenant-a",
    "agent_id": "agent-a",
    "effective_inbox_seat": None,
    "mode": "bearer_only",
    "generation": 0,
    "profile_owner_fingerprint": "0" * 64,
}


def test_legacy_ack_ownership_round_trips() -> None:
    value = legacy_ack_ownership()
    assert validate_ack_ownership(value) == value


def test_attempt_ack_ownership_accepts_a_valid_marker() -> None:
    value = attempt_ack_ownership(VALID_MARKER)
    assert value["kind"] == "attempt"
    assert value["attempt_id"] == "a" * 16


@pytest.mark.parametrize(
    "bad_attempt_id",
    [
        "",  # empty
        "a" * 15,  # one under the 16-char floor
        "a" * 129,  # one over the 128-char ceiling
        "not a valid attempt id!!",  # disallowed characters (spaces, punctuation)
        "a" * 15 + "\n",  # disallowed trailing control character
        None,  # not a string at all
        123,  # not a string at all
    ],
)
def test_attempt_ack_ownership_refuses_a_malformed_attempt_id(bad_attempt_id: object) -> None:
    marker = {**VALID_MARKER, "attempt_id": bad_attempt_id}
    with pytest.raises(AckOwnershipError):
        attempt_ack_ownership(marker)


@pytest.mark.parametrize(
    "bad_attempt_id",
    ["", "a" * 15, "a" * 129, "not a valid attempt id!!"],
)
def test_validate_ack_ownership_refuses_a_malformed_attempt_id_directly(
    bad_attempt_id: str,
) -> None:
    value = {
        "version": 1,
        "kind": "attempt",
        "attempt_id": bad_attempt_id,
        "tenant": "tenant-a",
        "agent_id": "agent-a",
        "effective_inbox_seat": None,
        "mode": "bearer_only",
        "generation": 0,
        "profile_owner_fingerprint": "0" * 64,
    }
    with pytest.raises(AckOwnershipError):
        validate_ack_ownership(value)


def test_attempt_id_pattern_matches_the_accepted_boundary_lengths_only() -> None:
    assert ATTEMPT_ID_RE.fullmatch("a" * 16)
    assert ATTEMPT_ID_RE.fullmatch("a" * 128)
    assert ATTEMPT_ID_RE.fullmatch("a" * 15) is None
    assert ATTEMPT_ID_RE.fullmatch("a" * 129) is None
    assert ATTEMPT_ID_RE.fullmatch("a" * 16 + " ") is None
