"""Contract tests for the Telegram inline-button approval mechanism."""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from plugin.telegram_inline_approval import (
    CALLBACK_PREFIX,
    TOKEN_VERSION,
    ApprovalReceiptStore,
    TelegramInlineApprovalSettings,
    _ApprovalTokenStore,
    _handle_callback,
    _passes_callback_fence,
    _single_pending_task_id,
    build_approval_keyboard,
    maybe_build_needs_keyboard,
    register_telegram_inline_approval,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class User:
    def __init__(self, user_id: int = 123) -> None:
        self.id = user_id


class Chat:
    def __init__(self, chat_id: int = 123, chat_type: str = "private") -> None:
        self.id = chat_id
        self.type = chat_type


class Message:
    def __init__(self, chat: Chat | None = None) -> None:
        self.chat = chat if chat is not None else Chat()
        self.edited: list[Any] = []

    async def edit_reply_markup(self, reply_markup: Any = None) -> None:
        self.edited.append(reply_markup)


class CallbackQuery:
    def __init__(
        self,
        *,
        data: str = "",
        message: Message | None = None,
        from_user: User | None = None,
    ) -> None:
        self.data = data
        self.message = message if message is not None else Message()
        self.from_user = from_user if from_user is not None else User()
        self.answers: list[tuple[Any, bool]] = []

    async def answer(self, text: Any = None, show_alert: bool = False) -> None:
        self.answers.append((text, show_alert))


class Update:
    def __init__(self, callback_query: CallbackQuery | None = None) -> None:
        self.callback_query = callback_query


class FakeClient:
    def __init__(self, result: Any = None, *, raises: BaseException | None = None) -> None:
        self.result = result if result is not None else {"ok": True, "result": {}}
        self.raises = raises
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def call(self, action: str, args: dict[str, Any]) -> Any:
        self.calls.append((action, dict(args)))
        if self.raises is not None:
            raise self.raises
        return self.result


def applied_result(*, applied: bool = True, reason: str = "ok", verdict_id: str = "v-1") -> dict:
    return {
        "ok": True,
        "result": {"human_origin": {"applied": applied, "reason": reason}, "id": verdict_id},
    }


def _install_fake_telegram(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    handlers: list[Any] = []

    class CallbackQueryHandler:
        def __init__(self, callback: Any, pattern: str) -> None:
            self.callback = callback
            self.pattern = pattern

    class InlineKeyboardButton:
        def __init__(self, text: str, callback_data: str) -> None:
            self.text = text
            self.callback_data = callback_data

    class InlineKeyboardMarkup:
        def __init__(self, rows: Any) -> None:
            self.rows = rows

    telegram = types.ModuleType("telegram")
    telegram.InlineKeyboardButton = InlineKeyboardButton
    telegram.InlineKeyboardMarkup = InlineKeyboardMarkup
    telegram_ext = types.ModuleType("telegram.ext")
    telegram_ext.CallbackQueryHandler = CallbackQueryHandler
    monkeypatch.setitem(sys.modules, "telegram", telegram)
    monkeypatch.setitem(sys.modules, "telegram.ext", telegram_ext)
    return {
        "CallbackQueryHandler": CallbackQueryHandler,
        "InlineKeyboardButton": InlineKeyboardButton,
        "InlineKeyboardMarkup": InlineKeyboardMarkup,
    }


def valid_settings(**changes: object) -> TelegramInlineApprovalSettings:
    from dataclasses import replace

    return replace(TelegramInlineApprovalSettings(enabled=True), **changes)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def test_settings_default_disabled() -> None:
    settings = TelegramInlineApprovalSettings.from_mapping({})
    assert settings.enabled is False


def test_settings_rejects_non_boolean_enabled() -> None:
    with pytest.raises(ValueError, match="boolean"):
        TelegramInlineApprovalSettings.from_mapping(
            {"telegram_inline_approval_enabled": "true"}
        )


@pytest.mark.parametrize("bad_ttl", [0, 0.9, 601, float("inf"), True])
def test_settings_bound_ttl(bad_ttl: float) -> None:
    with pytest.raises(ValueError, match="ttl_seconds"):
        TelegramInlineApprovalSettings.from_mapping(
            {
                "telegram_inline_approval_enabled": True,
                "telegram_inline_approval_token_ttl_seconds": bad_ttl,
            }
        )


# ---------------------------------------------------------------------------
# Token store: mint / bind / claim (bind-or-burn, single-use, TTL, version)
# ---------------------------------------------------------------------------


def test_mint_triplet_produces_three_distinct_unbound_nonces() -> None:
    store = _ApprovalTokenStore()
    nonces = store.mint_triplet(task_id="task-1", chat_id=123, user_id=123, ttl_seconds=60)
    assert len(set(nonces.values())) == 3
    assert set(nonces) == {"approve", "reject", "details"}
    # Unbound (no prompt_message_id yet) tokens refuse to claim.
    status, record = store.claim(nonces["approve"])
    assert status == "not_bound"
    assert record is None


def test_claim_after_bind_succeeds_exactly_once_then_replay_is_refused() -> None:
    """Negative test: replay of a used token."""
    store = _ApprovalTokenStore()
    nonces = store.mint_triplet(task_id="task-1", chat_id=123, user_id=123, ttl_seconds=60)
    store.bind_message(nonces.values(), "999")

    status, record = store.claim(nonces["approve"])
    assert status == "ok"
    assert record is not None
    assert record.task_id == "task-1"
    assert record.prompt_message_id == "999"

    replay_status, replay_record = store.claim(nonces["approve"])
    assert replay_status == "used"
    assert replay_record is None


def test_claim_refuses_an_expired_token() -> None:
    """Negative test: expired token."""
    store = _ApprovalTokenStore()
    nonces = store.mint_triplet(task_id="task-1", chat_id=123, user_id=123, ttl_seconds=60)
    store.bind_message(nonces.values(), "999")
    nonce = nonces["approve"]
    # Force expiry without sleeping.
    store._entries[nonce].expires_at = 0.0
    status, record = store.claim(nonce)
    assert status == "expired"
    assert record is None
    # An expired token is burned on the failed claim -- it can never later
    # resurrect as "ok" even if something re-extended time.
    assert nonce not in store._entries


def test_claim_refuses_a_version_mismatch() -> None:
    """Negative test: version mismatch."""
    store = _ApprovalTokenStore()
    nonces = store.mint_triplet(task_id="task-1", chat_id=123, user_id=123, ttl_seconds=60)
    store.bind_message(nonces.values(), "999")
    nonce = nonces["reject"]
    store._entries[nonce].version = TOKEN_VERSION + 1
    status, record = store.claim(nonce)
    assert status == "version_mismatch"
    assert record is None


def test_claim_refuses_an_unknown_nonce() -> None:
    """Negative test: forged callback data without a stored nonce."""
    store = _ApprovalTokenStore()
    status, record = store.claim("this-was-never-minted")
    assert status == "unknown"
    assert record is None


def test_burn_removes_and_logs(caplog: pytest.LogCaptureFixture) -> None:
    store = _ApprovalTokenStore()
    nonces = store.mint_triplet(task_id="task-1", chat_id=123, user_id=123, ttl_seconds=60)
    with caplog.at_level("WARNING"):
        store.burn(nonces.values(), reason="send_failed")
    for nonce in nonces.values():
        assert nonce not in store._entries
    assert "send_failed" in caplog.text


def test_overflow_eviction_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    store = _ApprovalTokenStore(max_entries=3)
    with caplog.at_level("WARNING"):
        store.mint_triplet(task_id="task-old", chat_id=1, user_id=1, ttl_seconds=60)
        store.mint_triplet(task_id="task-new", chat_id=1, user_id=1, ttl_seconds=60)
    assert len(store) == 3
    assert "overflow" in caplog.text


# ---------------------------------------------------------------------------
# Fence
# ---------------------------------------------------------------------------


def test_fence_accepts_a_genuine_private_chat() -> None:
    query = CallbackQuery(message=Message(Chat(chat_id=123, chat_type="private")), from_user=User(123))
    assert _passes_callback_fence(query) is True


def test_fence_refuses_a_group_chat() -> None:
    """Negative test: press in a group chat."""
    query = CallbackQuery(message=Message(Chat(chat_id=-555, chat_type="group")), from_user=User(123))
    assert _passes_callback_fence(query) is False


def test_fence_refuses_a_different_user_in_the_same_chat() -> None:
    """Negative test: press from a different user in the same chat (group)."""
    # Even if a forged/malformed update reports chat.type == "private" (the
    # only literal a genuine Telegram DM ever carries), a presser whose id
    # does not match the chat id must still be refused -- this is the
    # invariant that makes a real Telegram GROUP (chat.id never equal to any
    # single member's user.id) fail closed regardless of who presses.
    query = CallbackQuery(
        message=Message(Chat(chat_id=123, chat_type="private")), from_user=User(999)
    )
    assert _passes_callback_fence(query) is False


# ---------------------------------------------------------------------------
# _handle_callback: end-to-end refusal + verdict-submission behavior
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_callback_refuses_forged_data_with_no_stored_nonce() -> None:
    query = CallbackQuery(data=CALLBACK_PREFIX + "not-a-real-nonce")
    update = Update(query)
    client = FakeClient()
    store = ApprovalReceiptStore("/nonexistent/should/not/be/written.json")
    await _handle_callback(update, client=client, secret_owner=None, receipt_store=store)
    assert client.calls == []
    assert query.answers and query.answers[0][1] is True  # show_alert


@pytest.mark.asyncio
async def test_handle_callback_refuses_a_group_press_before_touching_the_client(
    tmp_path: Any,
) -> None:
    token_store = _ApprovalTokenStore()
    nonces = token_store.mint_triplet(task_id="task-1", chat_id=123, user_id=123, ttl_seconds=60)
    token_store.bind_message(nonces.values(), "999")
    query = CallbackQuery(
        data=CALLBACK_PREFIX + nonces["approve"],
        message=Message(Chat(chat_id=-555, chat_type="group")),
        from_user=User(123),
    )
    update = Update(query)
    client = FakeClient()
    store = ApprovalReceiptStore(tmp_path / "receipts.json")
    await _handle_callback(
        update, client=client, secret_owner=None, receipt_store=store, token_store=token_store
    )
    assert client.calls == []
    # The fence refuses before ever touching the token store: the token is
    # still claimable afterward (a group-chat forgery attempt must not burn a
    # legitimate pending decision).
    status, _record = token_store.claim(nonces["approve"])
    assert status == "not_bound" or status == "ok"  # not "used" / not consumed by the refusal


@pytest.mark.asyncio
async def test_handle_callback_details_never_mutates(tmp_path: Any) -> None:
    """Details = answerCallbackQuery with a summary, no state change."""
    token_store = _ApprovalTokenStore()
    nonces = token_store.mint_triplet(task_id="task-1", chat_id=123, user_id=123, ttl_seconds=60)
    token_store.bind_message(nonces.values(), "999")
    message = Message(Chat(chat_id=123, chat_type="private"))
    query = CallbackQuery(
        data=CALLBACK_PREFIX + nonces["details"], message=message, from_user=User(123)
    )
    update = Update(query)
    client = FakeClient()
    store = ApprovalReceiptStore(tmp_path / "receipts.json")
    await _handle_callback(
        update, client=client, secret_owner=None, receipt_store=store, token_store=token_store
    )
    assert client.calls == []  # no verdict submitted
    assert message.edited == []  # message never touched
    assert query.answers and query.answers[0][1] is True
    data, valid = store.load_checked()
    assert valid and data.get("receipts", {}) == {}  # no receipt written
    # Approve/Reject remain independently claimable -- Details is its own nonce.
    status, _record = token_store.claim(nonces["approve"])
    assert status == "ok"


@pytest.mark.asyncio
async def test_handle_callback_approve_success_records_and_clears_keyboard(tmp_path: Any) -> None:
    token_store = _ApprovalTokenStore()
    nonces = token_store.mint_triplet(task_id="task-abc123", chat_id=123, user_id=123, ttl_seconds=60)
    token_store.bind_message(nonces.values(), "999")
    message = Message(Chat(chat_id=123, chat_type="private"))
    query = CallbackQuery(
        data=CALLBACK_PREFIX + nonces["approve"], message=message, from_user=User(123)
    )
    update = Update(query)
    client = FakeClient(applied_result(applied=True))
    store = ApprovalReceiptStore(tmp_path / "receipts.json")
    await _handle_callback(
        update, client=client, secret_owner=None, receipt_store=store, token_store=token_store
    )
    assert client.calls == [
        (
            "task_verdict",
            {
                "task_id": "task-abc123",
                "verdict": "approve",
                "human_origin": {
                    "channel": "telegram",
                    "user_id": "123",
                    "chat_id": "123",
                    "message_id": "999",
                    "message_at": client.calls[0][1]["human_origin"]["message_at"],
                    "text": "approve task-abc123",
                },
            },
        )
    ]
    assert message.edited == [None]
    assert query.answers[-1] == ("Recorded.", False)
    data, valid = store.load_checked()
    assert valid
    receipt = next(iter(data["receipts"].values()))
    assert receipt["applied"] is True
    assert receipt["task_id"] == "task-abc123"
    assert receipt["verdict"] == "approve"


@pytest.mark.asyncio
async def test_handle_callback_surfaces_applied_false_without_corrupting_local_state(
    tmp_path: Any,
) -> None:
    """Token for task A used after task A left review -> server applied:false
    surfaced, no local state corruption (the token stays consumed either way)."""
    token_store = _ApprovalTokenStore()
    nonces = token_store.mint_triplet(task_id="task-1", chat_id=123, user_id=123, ttl_seconds=60)
    token_store.bind_message(nonces.values(), "999")
    message = Message(Chat(chat_id=123, chat_type="private"))
    query = CallbackQuery(
        data=CALLBACK_PREFIX + nonces["approve"], message=message, from_user=User(123)
    )
    update = Update(query)
    client = FakeClient(applied_result(applied=False, reason="not_in_review"))
    store = ApprovalReceiptStore(tmp_path / "receipts.json")
    await _handle_callback(
        update, client=client, secret_owner=None, receipt_store=store, token_store=token_store
    )
    assert client.calls  # the call was made
    assert query.answers[-1][1] is True  # show_alert on the failure path
    data, valid = store.load_checked()
    assert valid
    receipt = next(iter(data["receipts"].values()))
    assert receipt["applied"] is False
    assert receipt["reason"] == "not_in_review"
    # No corruption: the token is (still) consumed, a second press is refused
    # as "used" -- never resurrected into a fresh, re-payable attempt.
    status, _record = token_store.claim(nonces["approve"])
    assert status == "used"


@pytest.mark.asyncio
async def test_handle_callback_transport_exception_is_caught_and_surfaced(tmp_path: Any) -> None:
    token_store = _ApprovalTokenStore()
    nonces = token_store.mint_triplet(task_id="task-1", chat_id=123, user_id=123, ttl_seconds=60)
    token_store.bind_message(nonces.values(), "999")
    message = Message(Chat(chat_id=123, chat_type="private"))
    query = CallbackQuery(
        data=CALLBACK_PREFIX + nonces["reject"], message=message, from_user=User(123)
    )
    update = Update(query)
    client = FakeClient(raises=RuntimeError("network exploded"))
    store = ApprovalReceiptStore(tmp_path / "receipts.json")
    await _handle_callback(
        update, client=client, secret_owner=None, receipt_store=store, token_store=token_store
    )
    assert query.answers[-1][1] is True
    data, valid = store.load_checked()
    assert valid
    receipt = next(iter(data["receipts"].values()))
    assert receipt["applied"] is False


# ---------------------------------------------------------------------------
# build_approval_keyboard / send_approval_prompt
# ---------------------------------------------------------------------------


def test_build_approval_keyboard_binds_via_on_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_telegram(monkeypatch)
    store = _ApprovalTokenStore()
    keyboard, on_sent = build_approval_keyboard(
        task_id="task-1", chat_id=123, user_id=123, store=store
    )
    row = keyboard.rows[0]
    assert [button.callback_data[: len(CALLBACK_PREFIX)] for button in row] == [
        CALLBACK_PREFIX
    ] * 3
    nonce = row[0].callback_data[len(CALLBACK_PREFIX):]
    status, _record = store.claim(nonce)
    assert status == "not_bound"
    on_sent(4242)
    status, record = store.claim(nonce)
    assert status == "ok"
    assert record.prompt_message_id == "4242"


def test_build_approval_keyboard_on_sent_burns_when_no_message_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_telegram(monkeypatch)
    store = _ApprovalTokenStore()
    keyboard, on_sent = build_approval_keyboard(
        task_id="task-1", chat_id=123, user_id=123, store=store
    )
    nonce = keyboard.rows[0][0].callback_data[len(CALLBACK_PREFIX):]
    on_sent(None)
    status, _record = store.claim(nonce)
    assert status == "unknown"


# ---------------------------------------------------------------------------
# /needs keyboard (needs_you_list-driven, conservative single-task shape)
# ---------------------------------------------------------------------------


def test_single_pending_task_id_requires_exactly_one_task() -> None:
    assert _single_pending_task_id({"ok": True, "result": []}) is None
    assert (
        _single_pending_task_id({"ok": True, "result": [{"id": "a"}, {"id": "b"}]})
        is None
    )
    assert _single_pending_task_id({"ok": True, "result": [{"id": "only-one"}]}) == "only-one"
    assert (
        _single_pending_task_id({"ok": True, "result": {"tasks": [{"task_id": "t-1"}]}})
        == "t-1"
    )
    assert _single_pending_task_id({"ok": False}) is None
    assert _single_pending_task_id({"ok": True, "result": {"tasks": "not-a-list"}}) is None
    assert _single_pending_task_id("not-a-dict") is None


@pytest.mark.asyncio
async def test_maybe_build_needs_keyboard_builds_for_exactly_one_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_telegram(monkeypatch)
    client = FakeClient({"ok": True, "result": [{"id": "solo-task"}]})
    update = Update()
    update.effective_chat = Chat(chat_id=123)
    update.effective_user = User(123)
    built = await maybe_build_needs_keyboard(client, secret_owner=None, update=update)
    assert built is not None
    keyboard, _on_sent = built
    assert len(keyboard.rows[0]) == 3
    assert client.calls == [("needs_you_list", {})]


@pytest.mark.asyncio
async def test_maybe_build_needs_keyboard_none_for_zero_or_many_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_telegram(monkeypatch)
    update = Update()
    update.effective_chat = Chat(chat_id=123)
    update.effective_user = User(123)
    for payload in ([], [{"id": "a"}, {"id": "b"}]):
        client = FakeClient({"ok": True, "result": payload})
        assert await maybe_build_needs_keyboard(client, secret_owner=None, update=update) is None


@pytest.mark.asyncio
async def test_maybe_build_needs_keyboard_never_raises_on_transport_failure() -> None:
    client = FakeClient(raises=RuntimeError("boom"))
    update = Update()
    update.effective_chat = Chat(chat_id=123)
    update.effective_user = User(123)
    assert await maybe_build_needs_keyboard(client, secret_owner=None, update=update) is None


@pytest.mark.asyncio
async def test_maybe_build_needs_keyboard_refuses_non_self_chat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_telegram(monkeypatch)
    client = FakeClient({"ok": True, "result": [{"id": "solo-task"}]})
    update = Update()
    update.effective_chat = Chat(chat_id=-555)  # a group chat id
    update.effective_user = User(123)
    assert await maybe_build_needs_keyboard(client, secret_owner=None, update=update) is None


# ---------------------------------------------------------------------------
# Registration (factory / unload) -- mirrors telegram_control's own tests
# ---------------------------------------------------------------------------


def test_disabled_inline_approval_registers_no_native_handler() -> None:
    calls: list[object] = []
    ctx = types.SimpleNamespace(register_telegram_handler=calls.append)
    register_telegram_inline_approval(
        ctx, TelegramInlineApprovalSettings(enabled=False), client=FakeClient()
    )
    assert calls == []


def test_factory_registers_exactly_one_scoped_callback_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fakes = _install_fake_telegram(monkeypatch)
    factories: list[object] = []
    ctx = types.SimpleNamespace(register_telegram_handler=factories.append)
    register_telegram_inline_approval(
        ctx, valid_settings(), client=FakeClient(), receipt_store=ApprovalReceiptStore("/tmp/x.json")
    )
    assert len(factories) == 1

    handlers: list[object] = []
    application = types.SimpleNamespace(add_handler=handlers.append)
    factories[0](application, object())
    assert len(handlers) == 1
    assert isinstance(handlers[0], fakes["CallbackQueryHandler"])
    assert handlers[0].pattern == f"^{CALLBACK_PREFIX}"

    # Calling the factory again for the SAME application must not double-register.
    factories[0](application, object())
    assert len(handlers) == 1


def test_unload_removes_the_registered_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_telegram(monkeypatch)
    factories: list[object] = []
    unloaders: list[object] = []
    ctx = types.SimpleNamespace(
        register_telegram_handler=factories.append,
        on_unload=unloaders.append,
        _manager=types.SimpleNamespace(_platform_handler_factories={}),
        manifest=types.SimpleNamespace(name="mupot"),
    )
    register_telegram_inline_approval(
        ctx, valid_settings(), client=FakeClient(), receipt_store=ApprovalReceiptStore("/tmp/x.json")
    )
    removed: list[object] = []
    application = types.SimpleNamespace(
        add_handler=lambda h: None, remove_handler=lambda h, group=0: removed.append(h)
    )
    factories[0](application, object())
    unloaders[0]()
    assert len(removed) == 1


# ---------------------------------------------------------------------------
# Durable receipt ledger
# ---------------------------------------------------------------------------


def test_receipt_store_round_trip(tmp_path: Any) -> None:
    store = ApprovalReceiptStore(tmp_path / "sub" / "receipts.json")
    data, valid = store.load_checked()
    assert valid and data == {}
    store.save({"receipts": {"n1": {"task_id": "t"}}})
    data, valid = store.load_checked()
    assert valid
    assert data["receipts"]["n1"]["task_id"] == "t"


def test_receipt_store_invalid_json_reports_invalid(tmp_path: Any) -> None:
    path = tmp_path / "receipts.json"
    path.write_text("not json", encoding="utf-8")
    store = ApprovalReceiptStore(path)
    _data, valid = store.load_checked()
    assert valid is False
