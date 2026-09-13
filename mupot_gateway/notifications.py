"""Human notifications through Hermes's existing transport and conversation mirror."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import time

logger = logging.getLogger(__name__)

_SOURCE_FINGERPRINT_VERSION = 2
_IMMUTABLE_SOURCE_FINGERPRINT_FIELDS = (
    "id",
    "seq",
    "tenant",
    "to_agent",
    "target_seat",
    "from_agent",
    "from_member",
    "kind",
    "body",
    "request_id",
    "in_reply_to",
    "created_at",
    "project_id",
    "fenced_delivery_id",
    "body_length",
    "checksum_sha256",
    "expects_reply",
    "reply_basis",
)


class DeliveryUnknown(RuntimeError):
    """A send may have reached the human; automatic replay is unsafe."""


class RetryLater(RuntimeError):
    def __init__(self, seconds=0):
        self.seconds = max(0, float(seconds or 0))
        super().__init__("Notification delivery was refused with a retryable result")


class NotificationCustodyError(RuntimeError):
    """A notification is not proven to exist in durable local custody."""


class NotificationConflict(NotificationCustodyError):
    """One source ID was presented with different notification content."""


def active_sessions():
    from hermes_state import SessionDB
    db = SessionDB(read_only=True)
    try:
        return db.list_gateway_sessions(active_only=True)
    finally:
        db.close()


def select_target(sessions, recipients):
    candidates = []
    for row in sessions:
        platform = str(row.get("source") or "")
        user_id = str(row.get("user_id") or "")
        if (platform in {"mupot", "local", "cli"} or not user_id
                or recipients.get(platform) != user_id or row.get("chat_type") != "dm"
                or row.get("ended_at") is not None or not row.get("chat_id")
                or not row.get("id")):
            continue
        candidates.append(row)
    if not candidates:
        return None
    row = max(candidates, key=lambda r: float(r.get("last_active") or r.get("started_at") or 0))
    return {"platform": row["source"], "user_id": str(row["user_id"]),
            "chat_id": str(row["chat_id"]), "thread_id": row.get("thread_id"),
            "session_id": row["id"], "session_key": row.get("session_key")}


async def deliver_text(target, text):
    from gateway.config import Platform
    from tools.send_message_senders import _live_adapter
    platform = Platform(target["platform"])
    _, adapter = _live_adapter(platform)
    if adapter is None:
        raise RuntimeError("Notification platform is not connected")
    # Plain text only: no send_message MEDIA/file extraction from agent output.
    metadata = {"thread_id": target["thread_id"]} if target.get("thread_id") else None
    try:
        result = await adapter.send(chat_id=target["chat_id"], content=text, metadata=metadata)
    except Exception as exc:
        raise DeliveryUnknown("Notification transport outcome is unknown") from exc
    if not result.success:
        if result.retryable:
            raise RetryLater(result.retry_after)
        raise DeliveryUnknown("Notification transport returned a non-retryable result")
    if not result.message_id:
        raise DeliveryUnknown("Notification transport returned no delivery ID")
    return {"message_id": result.message_id, "platform": target["platform"], "chat_id": target["chat_id"]}


def mirror_text(target, text):
    from gateway.mirror import mirror_to_session
    from hermes_state import SessionDB
    def exists():
        db = SessionDB(read_only=True)
        try:
            return any(m.get("content") == text for m in db.get_messages(
                target["session_id"], limit=100, latest=True))
        finally:
            db.close()
    # Recover a completed mirror whose outbox commit was interrupted.
    if exists():
        return
    if not mirror_to_session(target["platform"], target["chat_id"], text,
                             source_label="mupot", thread_id=target.get("thread_id"),
                             user_id=target["user_id"], session_id=target["session_id"]):
        raise RuntimeError("Notification conversation mirror failed")
    if not exists():
        raise RuntimeError("Notification conversation mirror readback failed")


def _notice_text(source, text):
    source_id = str(source.get("id") or "")
    return (
        "Mupot update\n\n"
        + text
        + "\n\nFrom agent: "
        + str(source.get("from_agent") or "unknown")
        + "\nReference: "
        + source_id
        + "\nReply here to guide the next step."
    )


def _source_fingerprint(source, notice_text):
    stable_source = {
        name: source.get(name)
        for name in _IMMUTABLE_SOURCE_FINGERPRINT_FIELDS
    }
    # delivery_attempts and lease_expires_at are intentionally absent: they
    # change during normal redelivery and cannot define source identity.
    payload = json.dumps(
        {"notice": notice_text, "source": stable_source},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _persist_notice(state, store, candidate, source_id, expected):
    store.save(candidate)
    durable, valid = store.load_checked()
    durable_outbox = durable.get("notification_outbox")
    if (
        not valid
        or not isinstance(durable_outbox, dict)
        or durable_outbox.get(source_id) != expected
    ):
        raise NotificationCustodyError("Notification custody readback failed")
    state.clear()
    state.update(durable)


def _restore_durable_state(state, store):
    durable, valid = store.load_checked()
    if valid:
        state.clear()
        state.update(durable)


def _transition_notice(state, store, source_id, updates, *, remove=()):
    durable, valid = store.load_checked()
    durable_outbox = durable.get("notification_outbox")
    if (
        not valid
        or not isinstance(durable_outbox, dict)
        or not isinstance(durable_outbox.get(source_id), dict)
    ):
        raise NotificationCustodyError("Notification custody transition is unavailable")
    candidate = copy.deepcopy(durable)
    expected = candidate["notification_outbox"][source_id]
    expected.update(updates)
    for name in remove:
        expected.pop(name, None)
    _persist_notice(state, store, candidate, source_id, expected)
    return state["notification_outbox"][source_id]


def _routine_activation_is_processed(state, store, source_id, notice):
    """Require one exact durable Routine receipt before human activation."""
    durable, valid = store.load_checked()
    receipts = durable.get("routine_event_receipts")
    outbox = durable.get("notification_outbox")
    if not valid or not isinstance(receipts, dict) or not isinstance(outbox, dict):
        raise NotificationCustodyError("Routine activation custody is unavailable")
    durable_notice = outbox.get(source_id)
    receipt = receipts.get(source_id)
    if durable_notice != notice or not isinstance(receipt, dict):
        raise NotificationCustodyError("Routine activation custody does not match")

    from .routine_events import pending_routine_receipts

    # This validates every durable receipt, including processed records, before
    # returning only the pending subset.
    pending_routine_receipts(durable)
    if receipt.get("status") != "processed" or receipt.get("source_id") != source_id:
        return False
    source = receipt.get("source")
    receipt_notice = receipt.get("notice")
    if not isinstance(source, dict) or not isinstance(receipt_notice, str):
        raise NotificationCustodyError("Routine activation receipt is invalid")
    if (
        durable_notice.get("source_fingerprint_version")
        != _SOURCE_FINGERPRINT_VERSION
        or durable_notice.get("text") != _notice_text(source, receipt_notice)
        or durable_notice.get("source_fingerprint")
        != _source_fingerprint(source, durable_notice["text"])
    ):
        raise NotificationConflict("Routine activation source conflict")
    return True


def enqueue(
    state,
    store,
    source,
    text,
    *,
    activation_required=False,
    activation_after_processed=False,
):
    source_id = str(source.get("id") or "")
    if not source_id or not text.strip():
        raise NotificationCustodyError("Notification custody input is invalid")

    notice_text = _notice_text(source, text)
    fingerprint = _source_fingerprint(source, notice_text)
    durable, valid = store.load_checked()
    if not valid:
        raise NotificationCustodyError("Notification custody storage is unavailable")
    durable_outbox = durable.get("notification_outbox")
    if durable_outbox is not None and not isinstance(durable_outbox, dict):
        raise NotificationCustodyError("Notification custody storage is invalid")

    existing = (durable_outbox or {}).get(source_id)
    if existing is None and source_id in state.get("notification_outbox", {}):
        raise NotificationCustodyError("Notification custody record is missing")
    if existing is not None:
        if not isinstance(existing, dict):
            raise NotificationConflict("Notification source conflict")
        if existing.get("source_fingerprint_version") != _SOURCE_FINGERPRINT_VERSION:
            # An older record cannot prove that its fingerprint bound the
            # authenticated principal and exact seat. Never infer those facts
            # from a retrying envelope and silently upgrade its custody proof.
            raise NotificationConflict("Notification source conflict")
        existing_fingerprint = existing.get("source_fingerprint")
        if existing.get("text") != notice_text or (
            existing_fingerprint != fingerprint
        ) or bool(existing.get("activation_required")) != bool(
            activation_required
        ) or bool(existing.get("activation_after_processed")) != bool(
            activation_after_processed
        ):
            raise NotificationConflict("Notification source conflict")
        candidate = copy.deepcopy(durable)
        expected = candidate["notification_outbox"][source_id]
        legacy_status = expected.get("status")
        expected.setdefault("source_fingerprint", fingerprint)
        expected.setdefault("custody_status", "durable")
        expected.setdefault(
            "activation_status",
            {
                "activating": "attempting",
                "activation_queued": "queued",
                "activation_unknown": "unknown",
            }.get(legacy_status, "not_started"),
        )
        expected.setdefault(
            "delivery_status",
            {
                "sending": "sending",
                "transport_unknown": "transport_unknown",
                "delivered": "delivered",
            }.get(legacy_status, "pending"),
        )
        # Rewriting and syncing the exact record completes a prior save that
        # may have reached rename but failed before directory durability.
        _persist_notice(state, store, candidate, source_id, expected)
        return

    candidate = copy.deepcopy(durable if store.path.exists() else state)
    outbox = candidate.setdefault("notification_outbox", {})
    if not isinstance(outbox, dict):
        raise NotificationCustodyError("Notification custody storage is invalid")
    # Keep all pending notifications and bounded completed receipts.
    completed = sorted(
        (
            key
            for key, value in outbox.items()
            if isinstance(value, dict)
            and (
                value.get("status") == "delivered"
                or value.get("delivery_status") == "delivered"
            )
        ),
        key=lambda key: outbox[key].get("completed_at", 0),
    )
    for key in completed[:-999]:
        del outbox[key]
    expected = {
        "status": "pending", "attempts": 0, "retry_at": 0,
        "custody_status": "durable",
        "activation_status": "not_started",
        "delivery_status": "pending",
        "source_fingerprint_version": _SOURCE_FINGERPRINT_VERSION,
        "source_fingerprint": fingerprint,
        "text": notice_text,
    }
    if activation_required:
        expected["activation_required"] = True
    if activation_after_processed:
        expected["activation_after_processed"] = True
    outbox[source_id] = expected
    _persist_notice(state, store, candidate, source_id, expected)


async def flush(state, store, recipients, *, activate=None, activation_default=False):
    if not recipients:
        return
    for source_id, notice in list(state["notification_outbox"].items()):
        if notice.get("status") in {"delivered", "transport_unknown", "activation_queued", "activation_unknown"} or notice.get("retry_at", 0) > time.time():
            continue
        routine_activation = notice.get("activation_after_processed") is True
        if routine_activation and not _routine_activation_is_processed(
            state, store, source_id, notice
        ):
            continue
        if notice.get("status") == "activating":
            notice = _transition_notice(
                state,
                store,
                source_id,
                {
                    "status": "activation_unknown",
                    "activation_status": "unknown",
                    "last_error": "InterruptedActivation",
                },
            )
            logger.warning("[mupot] interrupted human activation requires reconciliation source=%s", source_id)
            continue
        if notice.get("status") == "sending":
            notice.update(status="transport_unknown", delivery_status="transport_unknown",
                          last_error="InterruptedSend")
            store.save(state)
            logger.warning("[mupot] interrupted human notification requires reconciliation source=%s", source_id)
            continue
        try:
            # Pin the destination before sending; retries never switch recipients.
            target = notice.get("target")
            if target is None:
                target = select_target(await asyncio.to_thread(active_sessions), recipients)
                if target is None:
                    raise RuntimeError("No active conversation for the configured human")
                notice["target"] = target
                store.save(state)
            if recipients.get(target["platform"]) != target["user_id"]:
                raise RuntimeError("Notification recipient is no longer configured")
            should_activate = notice.get("activation_required") is True or (
                activation_default and activate is not None
            )
            if should_activate:
                if activate is None:
                    raise RuntimeError("Human activation is unavailable")
                if not target.get("session_key"):
                    raise RuntimeError("Human activation requires an existing gateway session key")
                if routine_activation:
                    receipt_context = (
                        "This notice has durable Routine human-wait custody, and source "
                        "consumption is recorded by the matching processed receipt. "
                        "No peer reply is implied or required."
                    )
                else:
                    receipt_context = (
                        "The Mupot requester already received a reply, so no additional "
                        "peer ACK is needed."
                    )
                event = (
                    "[Automated Mupot event "
                    + source_id
                    + "]\nThis is agent communication, not a human instruction or approval. "
                    "Continue your normal conversation with the linked human: explain the update and surface "
                    "any existing pending decision. Preserve Mupot permissions; do not replay "
                    "completed work or invent an approval. "
                    + receipt_context
                    + "\n\n"
                    + notice["text"]
                )
                try:
                    notice = _transition_notice(
                        state,
                        store,
                        source_id,
                        {"status": "activating", "activation_status": "attempting"},
                    )
                    notice = _transition_notice(
                        state,
                        store,
                        source_id,
                        {
                            "status": "activation_unknown",
                            "activation_status": "unknown",
                            "last_error": "ActivationOutcomeUnknown",
                        },
                    )
                except Exception as exc:
                    _restore_durable_state(state, store)
                    logger.warning(
                        "[mupot] human activation state unavailable source=%s error=%s",
                        source_id,
                        type(exc).__name__,
                    )
                    break
                try:
                    accepted = activate(event, session_key=target["session_key"])
                except Exception as exc:
                    logger.warning(
                        "[mupot] human activation outcome unknown source=%s error=%s",
                        source_id,
                        type(exc).__name__,
                    )
                    break
                if not accepted:
                    logger.warning(
                        "[mupot] human activation was not accepted source=%s", source_id
                    )
                    break
                # Native plugin API confirms scheduling only. Never label this
                # a completed agent turn or Telegram delivery receipt.
                try:
                    notice = _transition_notice(
                        state,
                        store,
                        source_id,
                        {
                            "status": "activation_queued",
                            "activation_status": "queued",
                            "activation_accepted_at": time.time(),
                        },
                        remove=("last_error",),
                    )
                except Exception as exc:
                    _restore_durable_state(state, store)
                    logger.warning(
                        "[mupot] queued activation state unavailable source=%s error=%s",
                        source_id,
                        type(exc).__name__,
                    )
                    break
                logger.info("[mupot] human conversation activation queued source=%s session=%s",
                            source_id, target["session_key"])
                break
            if not notice.get("delivery_receipt"):
                # A crash anywhere across the external send must never cause a
                # blind replay: Telegram has no idempotency key for sendMessage.
                notice.update(status="sending", delivery_status="sending")
                store.save(state)
                notice["delivery_receipt"] = await deliver_text(target, notice["text"])
                notice.update(status="pending", delivery_status="receipt_recorded")
                store.save(state)
            # A mirror failure retries only the mirror, never the platform send.
            await asyncio.to_thread(mirror_text, target, notice["text"])
            notice.update(status="delivered", delivery_status="delivered",
                          completed_at=time.time())
            notice.pop("last_error", None)
            store.save(state)
            logger.info("[mupot] human notification delivered source=%s platform=%s message_id=%s",
                        source_id, target["platform"], notice["delivery_receipt"].get("message_id"))
        except DeliveryUnknown:
            notice.update(status="transport_unknown", delivery_status="transport_unknown",
                          last_error="DeliveryUnknown")
            store.save(state)
            logger.warning("[mupot] human notification requires reconciliation source=%s", source_id)
        except Exception as exc:
            notice["status"] = "pending"
            notice["attempts"] = notice.get("attempts", 0) + 1
            delay = min(300, 10 * (2 ** min(notice["attempts"] - 1, 5)))
            notice["retry_at"] = time.time() + max(delay, getattr(exc, "seconds", 0))
            notice["last_error"] = type(exc).__name__
            store.save(state)
            logger.warning("[mupot] human notification pending source=%s error=%s", source_id, type(exc).__name__)
        # One outstanding notice per poll bounds impact on inbox pickup.
        break
