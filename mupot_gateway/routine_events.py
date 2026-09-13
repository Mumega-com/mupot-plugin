"""Dedicated receive contract for server-owned Routine human waits.

Routine waits are synthetic server events, not peer messages. This module
keeps their validation and durable custody separate from the peer allowlist
and from Hermes model-turn routing.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


_SOURCE_AGENT = "mupot-routines"
_SOURCE_MEMBER = "system:routines"
_BODY_VERSION = "routine.human-wait/v1"
_BODY_TYPE = "routine_human_wait"
_BODY_LIMIT = 8000
_REF_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")
_SOURCE_ID_RE = re.compile(r"^[^\s]{1,128}$")
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_CHECKSUM_RE = re.compile(r"^[a-f0-9]{64}$")
_RECEIPT_VERSION = 1
_IMMUTABLE_SOURCE_FIELDS = (
    "seq",
    "id",
    "from_agent",
    "from_member",
    "kind",
    "body",
    "request_id",
    "in_reply_to",
    "created_at",
    "project_id",
    "target_seat",
    "body_length",
    "checksum_sha256",
    "is_intact",
    "expects_reply",
    "reply_basis",
)


class RoutineEventValidationError(ValueError):
    """The leased envelope is not an authentic Routine human-wait event."""


class RoutineEventCustodyError(RuntimeError):
    """The local ledger cannot prove custody of a Routine event."""


class RoutineEventConflict(RuntimeError):
    """One source ID was replayed with different immutable facts."""


@dataclass(frozen=True, slots=True)
class RoutineHumanWaitEvent:
    source_id: str
    request_id: str
    project_id: str
    run_id: str
    action_key: str
    reason: str
    decision: Mapping[str, Any]
    notice: str


def _utf16_length(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def _valid_instant(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (OverflowError, TypeError, ValueError):
        return False
    return parsed.tzinfo is not None


def _exact_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise RoutineEventValidationError("Routine event body is invalid")
        value[key] = item
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str]) -> bool:
    return set(value) == expected


def _valid_ref(value: Any) -> bool:
    return isinstance(value, str) and _REF_RE.fullmatch(value) is not None


def _bounded_utf8(value: Any, *, minimum: int, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and len(value.strip()) >= minimum
        and len(value.encode("utf-8")) <= maximum
    )


def _parse_decision(body: Mapping[str, Any]) -> Mapping[str, Any]:
    reason = body.get("reason")
    decision = body.get("decision")
    if not isinstance(decision, dict):
        raise RoutineEventValidationError("Routine event body is invalid")
    truncated = decision.get("truncated")
    if truncated is not None and truncated is not True:
        raise RoutineEventValidationError("Routine event body is invalid")

    if reason == "review":
        expected = {"type", "task_id"} | ({"truncated"} if truncated is True else set())
        if (
            not _exact_keys(decision, expected)
            or decision.get("type") != "review"
            or not _valid_ref(decision.get("task_id"))
        ):
            raise RoutineEventValidationError("Routine event body is invalid")
        return decision

    if reason != "answer":
        raise RoutineEventValidationError("Routine event body is invalid")
    expected = {"type", "question", "choices"} | (
        {"truncated"} if truncated is True else set()
    )
    choices = decision.get("choices")
    if (
        not _exact_keys(decision, expected)
        or decision.get("type") != "answer"
        or not isinstance(choices, list)
        or len(choices) > 5
        or any(not isinstance(choice, str) for choice in choices)
    ):
        raise RoutineEventValidationError("Routine event body is invalid")
    if truncated is True:
        if (
            not isinstance(decision.get("question"), str)
            or len(str(decision["question"]).encode("utf-8")) > 2000
            or any(len(choice.encode("utf-8")) > 500 for choice in choices)
        ):
            raise RoutineEventValidationError("Routine event body is invalid")
    elif (
        not _bounded_utf8(decision.get("question"), minimum=1, maximum=2000)
        or len(choices) == 1
        or any(not _bounded_utf8(choice, minimum=1, maximum=500) for choice in choices)
        or len(set(choices)) != len(choices)
    ):
        raise RoutineEventValidationError("Routine event body is invalid")
    return decision


def _request_id(run_id: str, action_key: str) -> str:
    candidate = f"routine-human:{run_id}:{action_key}"
    if len(candidate) <= 128:
        return candidate
    digest = hashlib.sha256(f"{run_id}:{action_key}".encode("utf-8")).hexdigest()
    return f"routine-human:{digest}"


def _human_notice(
    *,
    project_id: str,
    run_id: str,
    action_key: str,
    reason: str,
    decision: Mapping[str, Any],
) -> str:
    lines = [
        "A Mupot Routine is waiting for human input.",
        f"Project: {project_id}",
        f"Run: {run_id}",
        f"Action: {action_key}",
        f"Reason: {reason}",
    ]
    if decision.get("type") == "review":
        lines.append(f"Task: {decision['task_id']}")
    elif decision.get("truncated") is not True:
        lines.append(f"Question: {decision['question']}")
        choices = decision.get("choices") or []
        if choices:
            lines.append("Choices: " + " | ".join(str(choice) for choice in choices))
    else:
        lines.append("The decision summary was truncated and cannot be used to answer.")
    lines.extend(
        [
            "Fetch the current allowed actions from Mupot before deciding.",
            "This notification is context only and is not executable consent.",
        ]
    )
    return "\n".join(lines)


def is_routine_event_candidate(message: Mapping[str, Any]) -> bool:
    """Catch authentic and spoofed Routine-shaped rows before peer routing."""
    if not isinstance(message, Mapping):
        return False
    if message.get("from_agent") == _SOURCE_AGENT:
        return True
    if message.get("from_member") == _SOURCE_MEMBER:
        return True
    request_id = message.get("request_id")
    if isinstance(request_id, str) and request_id.startswith("routine-human:"):
        return True
    body = message.get("body")
    if not isinstance(body, str) or len(body) > _BODY_LIMIT:
        return False
    if '"routine.human-wait/v1"' in body or '"routine_human_wait"' in body:
        return True
    try:
        parsed = json.loads(body, object_pairs_hook=_exact_object)
    except (json.JSONDecodeError, RoutineEventValidationError, TypeError, UnicodeError):
        return False
    return isinstance(parsed, dict) and (
        parsed.get("version") == _BODY_VERSION or parsed.get("type") == _BODY_TYPE
    )


def validate_routine_event(message: Mapping[str, Any]) -> RoutineHumanWaitEvent:
    if not isinstance(message, Mapping):
        raise RoutineEventValidationError("Routine event envelope is invalid")
    body = message.get("body")
    source_id = message.get("id")
    request_id = message.get("request_id")
    project_id = message.get("project_id")
    target_seat = message.get("target_seat")
    if (
        message.get("from_agent") != _SOURCE_AGENT
        or message.get("from_member") != _SOURCE_MEMBER
        or message.get("kind") != "ack"
        or message.get("expects_reply") is not False
        or message.get("reply_basis") != "ack_is_terminal"
        or message.get("in_reply_to") is not None
        or message.get("is_intact") is not True
        or type(message.get("seq")) is not int
        or message["seq"] <= 0
        or type(message.get("delivery_attempts")) is not int
        or message["delivery_attempts"] <= 0
        or not isinstance(source_id, str)
        or _SOURCE_ID_RE.fullmatch(source_id) is None
        or not isinstance(request_id, str)
        or _REQUEST_ID_RE.fullmatch(request_id) is None
        or not _valid_ref(project_id)
        or not _valid_instant(message.get("created_at"))
        or not _valid_instant(message.get("lease_expires_at"))
        or (target_seat is not None and not _valid_ref(target_seat))
        or not isinstance(body, str)
        or not body.strip()
        or _utf16_length(body) > _BODY_LIMIT
        or type(message.get("body_length")) is not int
        or message.get("body_length") != _utf16_length(body)
        or not isinstance(message.get("checksum_sha256"), str)
        or _CHECKSUM_RE.fullmatch(str(message["checksum_sha256"])) is None
        or message.get("checksum_sha256")
        != hashlib.sha256(body.encode("utf-8")).hexdigest()
    ):
        raise RoutineEventValidationError("Routine event envelope is invalid")

    try:
        parsed = json.loads(body, object_pairs_hook=_exact_object)
    except (json.JSONDecodeError, RoutineEventValidationError, TypeError, UnicodeError):
        raise RoutineEventValidationError("Routine event body is invalid") from None
    if (
        not isinstance(parsed, dict)
        or not _exact_keys(
            parsed,
            {
                "version",
                "type",
                "project_id",
                "run_id",
                "action_key",
                "reason",
                "decision",
            },
        )
        or parsed.get("version") != _BODY_VERSION
        or parsed.get("type") != _BODY_TYPE
        or parsed.get("project_id") != project_id
        or not _valid_ref(parsed.get("run_id"))
        or not _valid_ref(parsed.get("action_key"))
    ):
        raise RoutineEventValidationError("Routine event body is invalid")
    decision = _parse_decision(parsed)
    run_id = str(parsed["run_id"])
    action_key = str(parsed["action_key"])
    if request_id != _request_id(run_id, action_key):
        raise RoutineEventValidationError("Routine event request ID is invalid")
    reason = str(parsed["reason"])
    return RoutineHumanWaitEvent(
        source_id=source_id,
        request_id=request_id,
        project_id=str(project_id),
        run_id=run_id,
        action_key=action_key,
        reason=reason,
        decision=copy.deepcopy(decision),
        notice=_human_notice(
            project_id=str(project_id),
            run_id=run_id,
            action_key=action_key,
            reason=reason,
            decision=decision,
        ),
    )


def _source_fingerprint(source: Mapping[str, Any]) -> str:
    stable = {name: source.get(name) for name in _IMMUTABLE_SOURCE_FIELDS}
    payload = json.dumps(
        stable, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _persist_exact(
    state, store, candidate, root: str, source_id: str, expected
) -> None:
    store.save(candidate)
    durable, valid = store.load_checked()
    records = durable.get(root)
    if not valid or not isinstance(records, dict) or records.get(source_id) != expected:
        raise RoutineEventCustodyError("Routine event custody readback failed")
    state.clear()
    state.update(durable)


def persist_routine_receipt(state, store, source, event: RoutineHumanWaitEvent) -> None:
    fingerprint = _source_fingerprint(source)
    durable, valid = store.load_checked()
    if not valid:
        raise RoutineEventCustodyError("Routine event custody storage is unavailable")
    records = durable.get("routine_event_receipts")
    quarantine = durable.get("routine_event_quarantine")
    if records is not None and not isinstance(records, dict):
        raise RoutineEventCustodyError("Routine event custody storage is invalid")
    if isinstance(quarantine, dict) and event.source_id in quarantine:
        raise RoutineEventConflict("Routine event source conflict")
    existing = (records or {}).get(event.source_id)
    if existing is not None:
        if (
            not isinstance(existing, dict)
            or existing.get("version") != _RECEIPT_VERSION
            or existing.get("source_fingerprint") != fingerprint
            or existing.get("notice") != event.notice
        ):
            raise RoutineEventConflict("Routine event source conflict")
        candidate = copy.deepcopy(durable)
        expected = candidate["routine_event_receipts"][event.source_id]
        _persist_exact(
            state, store, candidate, "routine_event_receipts", event.source_id, expected
        )
        return
    candidate = copy.deepcopy(durable if store.path.exists() else state)
    candidate.setdefault("routine_event_receipts", {})
    expected = {
        "version": _RECEIPT_VERSION,
        "source_id": event.source_id,
        "source_fingerprint": fingerprint,
        "source": copy.deepcopy(dict(source)),
        "notice": event.notice,
        "status": "custody",
    }
    candidate["routine_event_receipts"][event.source_id] = expected
    _persist_exact(
        state, store, candidate, "routine_event_receipts", event.source_id, expected
    )


def quarantine_routine_event(state, store, source, reason: str) -> None:
    source_id = str(source.get("id") or "")
    if not source_id:
        raise RoutineEventCustodyError("Routine quarantine source is invalid")
    fingerprint = _source_fingerprint(source)
    durable, valid = store.load_checked()
    if not valid:
        raise RoutineEventCustodyError("Routine quarantine storage is unavailable")
    records = durable.get("routine_event_quarantine")
    receipts = durable.get("routine_event_receipts")
    if records is not None and not isinstance(records, dict):
        raise RoutineEventCustodyError("Routine quarantine storage is invalid")
    if isinstance(receipts, dict) and source_id in receipts:
        raise RoutineEventConflict("Routine event source conflict")
    existing = (records or {}).get(source_id)
    if existing is not None:
        if (
            not isinstance(existing, dict)
            or existing.get("source_fingerprint") != fingerprint
        ):
            raise RoutineEventConflict("Routine quarantine source conflict")
        candidate = copy.deepcopy(durable)
        expected = candidate["routine_event_quarantine"][source_id]
    else:
        candidate = copy.deepcopy(durable if store.path.exists() else state)
        candidate.setdefault("routine_event_quarantine", {})
        expected = {
            "version": _RECEIPT_VERSION,
            "source_id": source_id,
            "source_fingerprint": fingerprint,
            "reason": reason,
        }
        candidate["routine_event_quarantine"][source_id] = expected
    _persist_exact(
        state, store, candidate, "routine_event_quarantine", source_id, expected
    )


def mark_routine_processed(state, store, source_id: str) -> None:
    durable, valid = store.load_checked()
    records = durable.get("routine_event_receipts")
    if (
        not valid
        or not isinstance(records, dict)
        or not isinstance(records.get(source_id), dict)
    ):
        raise RoutineEventCustodyError("Routine event receipt is unavailable")
    candidate = copy.deepcopy(durable)
    processed = list(candidate.get("processed") or [])
    if source_id not in processed:
        processed.append(source_id)
    candidate["processed"] = processed[-1000:]
    candidate["routine_event_receipts"][source_id]["status"] = "processed"
    expected = candidate["routine_event_receipts"][source_id]
    _persist_exact(
        state, store, candidate, "routine_event_receipts", source_id, expected
    )


def pending_routine_receipts(state) -> list[dict[str, Any]]:
    records = state.get("routine_event_receipts")
    if records is None:
        return []
    if not isinstance(records, dict):
        raise RoutineEventCustodyError("Routine event custody storage is invalid")
    pending: list[dict[str, Any]] = []
    for source_id, record in records.items():
        if not isinstance(record, dict):
            raise RoutineEventCustodyError("Routine event custody record is invalid")
        if (
            record.get("version") != _RECEIPT_VERSION
            or record.get("source_id") != source_id
            or not isinstance(record.get("source"), dict)
            or record.get("status") not in {"custody", "processed"}
        ):
            raise RoutineEventCustodyError("Routine event custody record is invalid")
        event = validate_routine_event(record["source"])
        if (
            event.source_id != source_id
            or record.get("source_fingerprint") != _source_fingerprint(record["source"])
            or record.get("notice") != event.notice
        ):
            raise RoutineEventConflict("Routine event source conflict")
        if record.get("status") == "processed":
            continue
        pending.append(copy.deepcopy(record))
    return pending
