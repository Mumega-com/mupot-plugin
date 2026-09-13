"""Human notifications through Hermes's existing transport and conversation mirror."""
from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class DeliveryUnknown(RuntimeError):
    """A send may have reached the human; automatic replay is unsafe."""


class RetryLater(RuntimeError):
    def __init__(self, seconds=0):
        self.seconds = max(0, float(seconds or 0))
        super().__init__("Notification delivery was refused with a retryable result")


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


def enqueue(state, store, source, text):
    source_id = str(source.get("id") or "")
    if not source_id or not text.strip():
        return
    outbox = state["notification_outbox"]
    if source_id in outbox:
        return
    # Keep all pending notifications and bounded completed receipts.
    completed = sorted((key for key, value in outbox.items() if value.get("status") == "delivered"),
                       key=lambda key: outbox[key].get("completed_at", 0))
    for key in completed[:-999]:
        del outbox[key]
    outbox[source_id] = {
        "status": "pending", "attempts": 0, "retry_at": 0,
        "text": ("Mupot update\n\n" + text + "\n\nFrom agent: "
                 + str(source.get("from_agent") or "unknown")
                 + "\nReference: " + source_id + "\nReply here to guide the next step."),
    }
    store.save(state)


async def flush(state, store, recipients, *, activate=None):
    if not recipients:
        return
    for source_id, notice in list(state["notification_outbox"].items()):
        if notice.get("status") in {"delivered", "transport_unknown", "activation_queued", "activation_unknown"} or notice.get("retry_at", 0) > time.time():
            continue
        if notice.get("status") == "activating":
            notice.update(status="activation_unknown", last_error="InterruptedActivation")
            store.save(state)
            logger.warning("[mupot] interrupted human activation requires reconciliation source=%s", source_id)
            continue
        if notice.get("status") == "sending":
            notice.update(status="transport_unknown", last_error="InterruptedSend")
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
                notice["status"] = "activating"
                store.save(state)
                try:
                    accepted = activate(event, session_key=target["session_key"])
                except Exception as exc:
                    notice.update(status="activation_unknown", last_error=type(exc).__name__)
                    store.save(state)
                    break
                if not accepted:
                    raise RuntimeError("Native gateway activation was not accepted")
                # Native plugin API confirms scheduling only. Never label this
                # a completed agent turn or Telegram delivery receipt.
                notice.update(status="activation_queued", activation_accepted_at=time.time())
                store.save(state)
                logger.info("[mupot] human conversation activation queued source=%s session=%s",
                            source_id, target["session_key"])
                break
            if not notice.get("delivery_receipt"):
                # A crash anywhere across the external send must never cause a
                # blind replay: Telegram has no idempotency key for sendMessage.
                notice["status"] = "sending"
                store.save(state)
                notice["delivery_receipt"] = await deliver_text(target, notice["text"])
                notice["status"] = "pending"
                store.save(state)
            # A mirror failure retries only the mirror, never the platform send.
            await asyncio.to_thread(mirror_text, target, notice["text"])
            notice.update(status="delivered", completed_at=time.time())
            notice.pop("last_error", None)
            store.save(state)
            logger.info("[mupot] human notification delivered source=%s platform=%s message_id=%s",
                        source_id, target["platform"], notice["delivery_receipt"].get("message_id"))
        except DeliveryUnknown:
            notice.update(status="transport_unknown", last_error="DeliveryUnknown")
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
