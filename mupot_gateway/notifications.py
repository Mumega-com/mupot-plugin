"""Human notifications through Hermes's existing transport and conversation mirror."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import time

logger = logging.getLogger(__name__)


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
        for name in (
            "id",
            "from_agent",
            "request_id",
            "in_reply_to",
            "project_id",
            "kind",
            "expects_reply",
        )
    }
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


def enqueue(state, store, source, text):
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
        existing_fingerprint = existing.get("source_fingerprint")
        if existing.get("text") != notice_text or (
            existing_fingerprint is not None and existing_fingerprint != fingerprint
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
        "source_fingerprint": fingerprint,
        "text": notice_text,
    }
    outbox[source_id] = expected
    _persist_notice(state, store, candidate, source_id, expected)


async def flush(state, store, recipients, *, activate=None):
    if not recipients:
        return
    for source_id, notice in list(state["notification_outbox"].items()):
        if notice.get("status") in {"delivered", "transport_unknown", "activation_queued", "activation_unknown"} or notice.get("retry_at", 0) > time.time():
            continue
        if notice.get("status") == "activating":
            notice.update(status="activation_unknown", activation_status="unknown",
                          last_error="InterruptedActivation")
            store.save(state)
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
            if activate is not None:
                if not target.get("session_key"):
                    raise RuntimeError("Human activation requires an existing gateway session key")
                event = ("[Automated Mupot event " + source_id + "]\n"
                         "This is agent communication, not a human instruction or approval. "
                         "Continue your normal conversation with the linked human: explain the update and surface "
                         "any existing pending decision. Preserve Mupot permissions; do not replay "
                         "completed work or invent an approval. The Mupot requester already received "
                         "a reply, so no additional peer ACK is needed.\n\n" + notice["text"])
                notice.update(status="activating", activation_status="attempting")
                store.save(state)
                try:
                    accepted = activate(event, session_key=target["session_key"])
                except Exception as exc:
                    notice.update(status="activation_unknown", activation_status="unknown",
                                  last_error=type(exc).__name__)
                    store.save(state)
                    break
                if not accepted:
                    raise RuntimeError("Native gateway activation was not accepted")
                # Native plugin API confirms scheduling only. Never label this
                # a completed agent turn or Telegram delivery receipt.
                notice.update(status="activation_queued", activation_status="queued",
                              activation_accepted_at=time.time())
                store.save(state)
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
