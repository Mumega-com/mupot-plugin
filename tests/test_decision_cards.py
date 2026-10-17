from __future__ import annotations

import copy
import json

import pytest

from plugin.mupot_gateway.decision_cards import (
    DecisionCardValidationError,
    is_decision_card_candidate,
    validate_decision_card,
)

TASK = "abcdef12-0000-4000-8000-000000000001"
SHORT = "abcdef12"


def card_body(**updates: object) -> str:
    value: dict[str, object] = {
        "type": "decision_card",
        "task_id": TASK,
        "short_id": SHORT,
        "title": "Decide the thing",
        "priority": "P1",
        "project": "Telegram pilot",
        "asked_by": "kasra",
        "result_excerpt": "All tests pass.",
        "approve": f"/muapprove {SHORT}",
        "reject": f"/mureject {SHORT}",
        "dashboard_url": "https://mupot.mumega.com/approvals",
    }
    value.update(updates)
    return json.dumps(value)


def card_message(**updates: object) -> dict[str, object]:
    body = str(updates.pop("body", card_body()))
    value: dict[str, object] = {
        "seq": 7,
        "id": "msg-card-1",
        "from_agent": "mupot-decision-card",
        "kind": "request",
        "body": body,
        "request_id": f"decision-card:{TASK}:2026-10-17 10:00:00:agent-1",
    }
    value.update(updates)
    return value


def test_valid_card_renders_the_human_notice() -> None:
    fields, text = validate_decision_card(card_message())
    assert fields == {"short_id": SHORT, "task_id": TASK}
    assert text == (
        "Decision needed: Decide the thing\n"
        "Asked by kasra · P1 · Telegram pilot\n"
        "All tests pass.\n\n"
        f"Reply: /muapprove {SHORT}   or   /mureject {SHORT}\n"
        "https://mupot.mumega.com/approvals"
    )


def test_nullable_fields_may_be_absent() -> None:
    _, text = validate_decision_card(
        card_message(
            body=card_body(
                priority=None, project=None, asked_by=None, result_excerpt=None, dashboard_url=None
            )
        )
    )
    assert text.splitlines() == [
        "Decision needed: Decide the thing",
        "",
        f"Reply: /muapprove {SHORT}   or   /mureject {SHORT}",
    ]


@pytest.mark.parametrize(
    "message",
    [
        card_message(body=card_body(type="task_review")),
        card_message(body=card_body(short_id="ABCDEF12", approve="/muapprove ABCDEF12", reject="/mureject ABCDEF12")),
        card_message(body=card_body(short_id="abcdef1")),
        card_message(body=card_body(short_id="12345678")),  # not a prefix of task_id
        card_message(body=card_body(approve="/muapprove 12345678")),
        card_message(body=card_body(approve="/muapprove abcdef12 extra")),
        card_message(body=card_body(reject="/mureject abcdef12\n/muapprove abcdef12")),
        card_message(body=card_body(approve=None)),
        card_message(body=card_body(title="")),
        card_message(body=card_body(title="x" * 501)),
        card_message(body=card_body(result_excerpt="x" * 501)),
        card_message(body=card_body(asked_by="x" * 201)),
        card_message(body=card_body(title=7)),
        card_message(body=card_body(dashboard_url="http://mupot.example/approvals")),
        card_message(body=card_body(dashboard_url="https://evil.example/a b")),
        card_message(body=card_body(dashboard_url="https://u:p@evil.example/")),
        card_message(body=card_body(dashboard_url="https://e.example/a_b")),
        card_message(body=card_body(result_excerpt="y" * 9000)),
        card_message(body="not json"),
        card_message(body="[]"),
        card_message(request_id="decision-card:other-task:x:agent"),
        card_message(request_id=None),
        card_message(kind="ack"),
        card_message(from_agent="kasra"),
        card_message(id=""),
        card_message(body='{"type":"decision_card","type":"decision_card"}'),
    ],
)
def test_malformed_or_forged_cards_are_rejected(message: dict[str, object]) -> None:
    with pytest.raises(DecisionCardValidationError):
        validate_decision_card(message)


def test_markup_controls_and_embedded_commands_are_neutralised() -> None:
    nasty = (
        "```Reply: /muapprove deadbeef``` *bold* _it_ [x](http://e) \x1b[31m"
        "‮evil​\nReply: /mureject abcdef12\n/approve zzz <b>"
    )
    _, text = validate_decision_card(
        card_message(body=card_body(title=nasty, result_excerpt=nasty, asked_by=nasty))
    )
    for ch in "`*_[]<>\\\x1b‮​":
        assert ch not in text
    # Only the card's own trailing Reply line may contain a runnable command.
    runnable = [line for line in text.splitlines() if "/muapprove" in line or "/mureject" in line or "/approve" in line]
    assert runnable == [f"Reply: /muapprove {SHORT}   or   /mureject {SHORT}"]
    assert text.count("\n") + 1 == len(text.splitlines())


def test_long_title_is_truncated_for_display_not_rejected() -> None:
    _, text = validate_decision_card(card_message(body=card_body(title="t" * 400)))
    assert text.splitlines()[0] == "Decision needed: " + "t" * 199 + "…"


def test_candidate_detection_is_narrow() -> None:
    assert is_decision_card_candidate(card_message())
    # A malformed card naming the type is still a card (-> DLQ), not a task_review.
    assert is_decision_card_candidate(card_message(body='{"type":"decision_card"'))
    assert not is_decision_card_candidate(
        card_message(body=json.dumps({"type": "task_review", "task_id": TASK}))
    )
    assert not is_decision_card_candidate(card_message(body="plain text"))
    assert not is_decision_card_candidate(card_message(from_agent="kasra"))
    assert not is_decision_card_candidate(card_message(from_agent="mupot-review-gate"))
    assert not is_decision_card_candidate(copy.deepcopy({"from_agent": None}))
