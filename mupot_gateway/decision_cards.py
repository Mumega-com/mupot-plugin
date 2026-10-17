"""Receive contract for mupot "decision cards" (a human-held gate awaits a verdict).

mupot's review gate sends ONE inbox message per agent owned by the gate-holding
member: ``from_agent == "mupot-decision-card"`` (a human-notice-only system sender), a JSON body of
``type == "decision_card"``. The card is a notice for the HUMAN. This module only
validates it and renders the human text; it never starts a model turn, never
widens ``allowed_agents``, and nothing in a card is ever read back as consent --
the human answers with ``/muapprove <short_id>`` / ``/mureject <short_id>``,
which telegram_control relays to mupot's own ``/im/webhook`` for authentication.

All free text in a card is untrusted (task titles and results are authored by
agents): it is stripped of markup/format/control characters, collapsed to one
line, and any embedded slash-command is neutralised so a card cannot forge a
"Reply:" line or plant a command for the human to tap.
"""

from __future__ import annotations

import json
import re
import unicodedata
from typing import Any, Mapping

from .routine_events import RoutineEventValidationError, _exact_object, _utf16_length

SOURCE_AGENT = "mupot-decision-card"
CARD_TYPE = "decision_card"
REQUEST_ID_PREFIX = "decision-card:"

_BODY_LIMIT = 8000
_SHORT_ID_RE = re.compile(r"^[0-9a-f]{8}$")
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9-]{8,64}$")
# https only, no userinfo/query/fragment; path limited to characters that are
# inert in Telegram markdown (no underscore).
_URL_RE = re.compile(r"^https://[A-Za-z0-9.-]{1,253}(?::\d{1,5})?(?:/[A-Za-z0-9.~%/-]{0,200})?$")
# EVERY slash-word is inert (/yolo, /approvals, /stop, /sethome, ...): only the
# plugin-rendered Reply/hint lines and dashboard_url may carry a live command/link.
_SLASH_RE = re.compile(r"/(?=[A-Za-z0-9])")  # digit-first (/123) autolinks too
_SCHEME_RE = re.compile(r"://")
# Any dot-like char between word characters (any script, any TLD): covers IDN
# TLDs, ideographic/fullwidth/halfwidth dots and bare IPv4, which Telegram autolinks.
_DOT_RE = re.compile(r"(?<=\w)[.\u3002\uff0e\uff61](?=\w)")
# +digits with spaces/dashes/parens, >= 7 digits: Telegram autolinks it as a phone.
_PHONE_RE = re.compile(r"\+\(?\d[\d\s().-]{5,}\d")
# & kills HTML entities (&sol; &commat;); $ kills cashtags and $$ rich-markdown routing.
_MARKUP_CHARS = "`*_[]~<>\\|&$"

# (field, hard cap in UTF-16 units before the card is rejected, required)
_FIELD_CAPS = {
    # None cap = any length (the 8000-char body cap bounds it); display-truncated.
    "title": (None, True),
    "priority": (32, False),
    "project": (None, False),
    "asked_by": (None, False),
    "result_excerpt": (500, False),
}
_TITLE_DISPLAY = 200
_EXCERPT_DISPLAY = 400


class DecisionCardValidationError(ValueError):
    """The leased envelope is not an authentic, well-formed decision card."""


def is_decision_card_candidate(message: Mapping[str, Any]) -> bool:
    """True for a mupot-decision-card row whose body claims to be a decision card.

    Deliberately narrow: any other body, and every mupot-review-gate row (task_review JSON,
    ...) is NOT a candidate and keeps its existing handling.
    """
    if not isinstance(message, Mapping) or message.get("from_agent") != SOURCE_AGENT:
        return False
    body = message.get("body")
    if not isinstance(body, str):
        return False
    try:
        parsed = json.loads(body, object_pairs_hook=_exact_object)
    except (json.JSONDecodeError, RoutineEventValidationError, TypeError, UnicodeError):
        # A mupot-decision-card row naming the card type in a body we cannot
        # parse is a malformed card (-> DLQ), not a task_review.
        return f'"{CARD_TYPE}"' in body
    return isinstance(parsed, dict) and parsed.get("type") == CARD_TYPE


def _clean(value: str, limit: int | None = None) -> str:
    kept = []
    for ch in value:
        if ch in _MARKUP_CHARS:
            continue
        if unicodedata.category(ch)[0] == "C":
            kept.append(" ")  # control, format (bidi/zero-width), surrogate, private
        elif ch.isspace():
            kept.append(" ")
        else:
            kept.append(ch)
    text = " ".join("".join(kept).split())
    text = _SCHEME_RE.sub("\uff1a\uff0f\uff0f", text)
    text = _DOT_RE.sub("\u2024", text)  # ONE DOT LEADER: visible, never autolinked
    text = _PHONE_RE.sub(
        lambda m: m.group(0).replace("+", "\uff0b") if sum(c.isdigit() for c in m.group(0)) >= 7 else m.group(0),
        text,
    )
    text = _SLASH_RE.sub("\uff0f", text)  # fullwidth solidus: visible, not a command
    text = text.replace("@", "\uff20").replace("#", "\uff03")  # no mentions/hashtags
    if limit is not None and len(text) > limit:
        text = text[: limit - 1].rstrip() + "…"
    return text


def validate_decision_card(message: Mapping[str, Any]) -> tuple[dict[str, str], str]:
    """Return (normalised card fields, human notice text) or raise
    DecisionCardValidationError -- and nothing else: a lone UTF-16 surrogate
    (a server-side truncation can split an emoji) or any other unexpected
    failure must dead-letter the card, not escape into the poll loop."""
    try:
        return _validate_decision_card(message)
    except DecisionCardValidationError:
        raise
    except Exception:
        raise DecisionCardValidationError("card could not be processed") from None


def _validate_decision_card(message: Mapping[str, Any]) -> tuple[dict[str, str], str]:
    if not isinstance(message, Mapping):
        raise DecisionCardValidationError("envelope is invalid")
    body = message.get("body")
    source_id = message.get("id")
    request_id = message.get("request_id")
    if (
        message.get("from_agent") != SOURCE_AGENT
        or message.get("kind") != "request"
        or not isinstance(source_id, str)
        or not source_id
        or len(source_id) > 128
        or not isinstance(request_id, str)
        or not isinstance(body, str)
        or not body.strip()
        or _utf16_length(body) > _BODY_LIMIT
    ):
        raise DecisionCardValidationError("envelope is invalid")
    try:
        parsed = json.loads(body, object_pairs_hook=_exact_object)
    except (json.JSONDecodeError, RoutineEventValidationError, TypeError, UnicodeError):
        raise DecisionCardValidationError("body is invalid") from None
    if not isinstance(parsed, dict) or parsed.get("type") != CARD_TYPE:
        raise DecisionCardValidationError("body type is invalid")

    for value in parsed.values():
        if isinstance(value, str):
            try:
                value.encode("utf-8")  # a lone surrogate cannot be stored or sent
            except UnicodeError:
                raise DecisionCardValidationError("card text is not valid Unicode") from None

    short_id = parsed.get("short_id")
    task_id = parsed.get("task_id")
    if (
        not isinstance(short_id, str)
        or _SHORT_ID_RE.fullmatch(short_id) is None
        or not isinstance(task_id, str)
        or _TASK_ID_RE.fullmatch(task_id) is None
        or not task_id.startswith(short_id)
        or not request_id.startswith(f"{REQUEST_ID_PREFIX}{task_id}:")
    ):
        raise DecisionCardValidationError("identifiers are invalid")
    if (
        parsed.get("approve") != f"/muapprove {short_id}"
        or parsed.get("reject") != f"/mureject {short_id}"
    ):
        raise DecisionCardValidationError("reply commands do not match the card")

    fields: dict[str, str] = {}
    for name, (cap, required) in _FIELD_CAPS.items():
        value = parsed.get(name)
        if value is None and not required:
            fields[name] = ""
            continue
        if not isinstance(value, str) or (cap is not None and _utf16_length(value) > cap):
            raise DecisionCardValidationError(f"{name} is invalid")
        fields[name] = value
    if not _clean(fields["title"]):
        raise DecisionCardValidationError("title is invalid")

    hint = parsed.get("reject_hint")
    if hint is not None and hint != f"/mureject {short_id} <reason>":
        raise DecisionCardValidationError("reject_hint does not match the card")

    url = parsed.get("dashboard_url")
    if url is None:
        url = ""
    elif not isinstance(url, str) or (url and _URL_RE.fullmatch(url) is None):
        raise DecisionCardValidationError("dashboard_url is invalid")

    title = _clean(fields["title"], _TITLE_DISPLAY)
    asked_by = _clean(fields["asked_by"], 100)
    priority = _clean(fields["priority"], 16)
    excerpt = _clean(fields["result_excerpt"], _EXCERPT_DISPLAY)
    project = _clean(fields["project"], 150)

    meta = " · ".join(part for part in (f"Asked by {asked_by}" if asked_by else "", priority, project) if part)
    lines = [f"Decision needed: {title}"]
    if meta:
        lines.append(meta)
    if excerpt:
        lines.append(excerpt)
    lines.append("")
    lines.append(f"Reply: /muapprove {short_id}   or   /mureject {short_id}")
    if hint is not None:
        lines.append(f"To reject with a reason: /mureject {short_id} then your reason")
    if url:
        lines.append(url)
    return {"short_id": short_id, "task_id": task_id}, "\n".join(lines)
