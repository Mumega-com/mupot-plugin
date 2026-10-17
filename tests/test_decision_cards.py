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
        card_message(body=card_body(result_excerpt="x" * 501)),
        card_message(body=card_body(reject_hint="/mureject abcdef12")),
        card_message(body=card_body(reject_hint="/mureject 12345678 <reason>")),
        card_message(body=card_body(reject_hint=5)),
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


def _free_text(text: str) -> str:
    _, out = validate_decision_card(
        card_message(body=card_body(title="T", asked_by="a", project="p", result_excerpt=text))
    )
    return out.splitlines()[2]


@pytest.mark.parametrize(
    "word", ["yolo", "approvals", "deny", "config", "model", "stop", "start", "sethome", "approve", "muapprove"]
)
def test_every_slash_word_is_inert_in_every_free_text_field(word: str) -> None:
    for field in ("title", "asked_by", "project", "result_excerpt"):
        _, out = validate_decision_card(
            card_message(body=card_body(**{field: f"x /{word} y"}))
        )
        runnable = [ln for ln in out.splitlines() if "/" in ln]
        assert runnable == [
            f"Reply: /muapprove {SHORT}   or   /mureject {SHORT}",
            "https://mupot.mumega.com/approvals",
        ], (field, word, out)


def test_urls_domains_mentions_hashtags_and_spoilers_are_defanged() -> None:
    out = _free_text("see https://evil.example/x and t.me/joinchat and @admin #urgent ||spoiler||")
    assert "://" not in out
    assert "t.me" not in out and "evil.example" not in out
    assert "@" not in out and "#" not in out and "|" not in out
    assert "spoiler" in out


def test_oversized_title_asked_by_project_are_truncated_not_rejected() -> None:
    _, out = validate_decision_card(
        card_message(body=card_body(title="t" * 2000, asked_by="a" * 2000, project="p" * 2000))
    )
    lines = out.splitlines()
    assert lines[0].endswith("\u2026") and len(lines[0]) < 230
    assert "\u2026" in lines[1] and len(lines[1]) < 300


def test_reject_hint_is_accepted_and_rendered_only_when_exact() -> None:
    _, out = validate_decision_card(
        card_message(body=card_body(reject_hint=f"/mureject {SHORT} <reason>"))
    )
    assert f"To reject with a reason: /mureject {SHORT} then your reason" in out
    _, plain = validate_decision_card(card_message())
    assert "To reject with a reason" not in plain


def test_short_id_must_be_a_prefix_of_task_id() -> None:
    other = "12345678"
    with pytest.raises(DecisionCardValidationError):
        validate_decision_card(
            card_message(body=card_body(short_id=other, approve=f"/muapprove {other}", reject=f"/mureject {other}"))
        )


@pytest.mark.parametrize("field", ["title", "result_excerpt", "asked_by", "project"])
def test_lone_surrogate_is_a_validation_error_not_a_unicode_error(field: str) -> None:
    body = card_body(**{field: "ok \ud83d"})
    assert "\\ud83d" in body  # json.dumps emits the escape, as the server does
    with pytest.raises(DecisionCardValidationError):
        validate_decision_card(card_message(body=body))


@pytest.mark.parametrize(
    "text",
    [
        "mupot.рф", "пример.рф", "evil。com", "evil．com", "evil｡com", "1.2.3.4",
        "a.b.c", "mupot.com", "https://x.рф/p",
    ],
)
def test_every_dot_like_char_between_word_chars_is_broken(text: str) -> None:
    out = _free_text(f"visit {text} now")
    for dot in ".\u3002\uff0e\uff61":
        assert dot not in out, (text, out)
    assert "\u2024" in out


@pytest.mark.parametrize(
    "text", ["+1 555 123 4567", "+1-555-123-4567", "+44 (20) 7946 0958", "+15551234567"]
)
def test_phone_numbers_are_defanged(text: str) -> None:
    out = _free_text(f"call {text} now")
    assert "+" not in out


def test_short_plus_number_is_left_alone() -> None:
    assert "+12" in _free_text("score +12 today")


def test_cashtags_html_entities_and_rich_markdown_dollars_are_neutralised() -> None:
    for text in ("buy $TSLA now", "$$ x $$", "&sol;yolo", "&commat;admin", "&#47;stop"):
        out = _free_text(text)
        assert "$" not in out and "&" not in out, (text, out)
    for field in ("title", "asked_by", "project", "result_excerpt"):
        _, out = validate_decision_card(card_message(body=card_body(**{field: "&sol;yolo $$"})))
        assert [ln for ln in out.splitlines() if "/" in ln] == [
            f"Reply: /muapprove {SHORT}   or   /mureject {SHORT}",
            "https://mupot.mumega.com/approvals",
        ]


@pytest.mark.parametrize("word", ["123", "1start", "9"])
def test_digit_first_slash_commands_are_inert(word: str) -> None:
    out = _free_text(f"x /{word} y")
    assert "/" not in out
