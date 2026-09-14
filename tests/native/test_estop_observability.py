"""Round-5 e-stop observability tests (kasra-review re-gate #4, 2026-09-14).

Re-gate #4 was AMBER, not a security hole -- every choke point still refused
correctly -- but flagged: (a) the fail-safe `except Exception: return True`
inside `_estop_engaged()` returned True with zero log records; (b) 4 of the
plugin's 12 gate sites (`_refuse_ack_if_estop_engaged`,
`_transmit_final_reply`, `_process_leased_message`'s own top gate, and
`send()`'s interim path) refused silently; (c) a pause-refused final reply
surfaced through `SendResult.error` with no mention of a pause. This file
pins the fix for all three, plus the "once per pause window, not once per
message/tick" discipline the fix uses everywhere (see adapter.py's
`_note_estop_pause_once`) so a long pause or a burst of refused traffic
cannot flood the log.
"""
from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path as _Path

import pytest

from gateway.config import PlatformConfig

from plugin.mupot_gateway import adapter as adapter_module
from plugin.mupot_gateway.adapter import MupotAdapter, StateStore, _EstopDeferred

sys.path.insert(0, str(_Path(__file__).parent))
rp = importlib.import_module("test_reply_protocol")

PEER_MSG = {
    "id": "m-peer", "seq": 7, "from_agent": "hadi-codex", "from_member": "member-code",
    "body": "full Mupot answer", "project_id": "project-1", "request_id": "req-7",
    "in_reply_to": None, "kind": "message", "created_at": "2026-09-13T00:00:00.000Z",
    "delivery_attempts": 1, "lease_expires_at": "2099-01-01T00:00:00.000Z",
}


def _adapter_at(tmp_path, client=None):
    return MupotAdapter(
        PlatformConfig(enabled=True, extra={
            "allowed_agents": "hadi-codex",
            "state_path": str(tmp_path / "state.json"),
        }),
        client_factory=lambda *_: client,
    )


class AckOnlyClient:
    """Answers `inbox_ack` (legacy, no attempt_id) for one message id."""

    async def connect(self):
        return None

    async def close(self):
        return None

    async def call(self, tool, arguments):
        assert tool == "inbox_ack", tool
        return {"acked": list(arguments["ids"]), "already_read": [], "refused": []}


@pytest.mark.asyncio
async def test_refuse_ack_if_estop_engaged_logs_once_per_pause_window(tmp_path, caplog):
    """Choke point at adapter.py's `_refuse_ack_if_estop_engaged` (used by
    both `_ack_expected` and `_ack_persisted_ownership`, so effectively every
    ack in the module) refused silently before round 5."""
    import hermes_constants
    from agent import estop as real_estop

    home = tmp_path / "hh"
    home.mkdir()
    token = hermes_constants.set_hermes_home_override(str(home))
    adapter_module._clear_estop_pause_log_sites()
    try:
        assert real_estop.is_engaged() is False
        real_estop.engage(reason="obs-refuse-ack")
        with caplog.at_level(logging.INFO, logger="plugin.mupot_gateway.adapter"):
            with pytest.raises(_EstopDeferred):
                adapter_module._refuse_ack_if_estop_engaged("m1")
            with pytest.raises(_EstopDeferred):
                adapter_module._refuse_ack_if_estop_engaged("m2")
        records = [r for r in caplog.records if "refusing ack/commit" in r.message]
        assert len(records) == 1, "logged once per refusal instead of once per pause window"

        real_estop.disengage()
        assert real_estop.is_engaged() is False
        # The pause-log-site set is cleared lazily, the next time
        # _estop_engaged() itself observes NOT-engaged (in production this
        # is every poll tick / every choke-point check in between pauses) --
        # simulate that one natural check here rather than assuming
        # disengage() alone resets it.
        assert adapter_module._estop_engaged() is False

        real_estop.engage(reason="obs-refuse-ack-2")
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="plugin.mupot_gateway.adapter"):
            with pytest.raises(_EstopDeferred):
                adapter_module._refuse_ack_if_estop_engaged("m3")
        records2 = [r for r in caplog.records if "refusing ack/commit" in r.message]
        assert len(records2) == 1, "a NEW pause window did not log again"
    finally:
        real_estop.disengage()
        adapter_module._clear_estop_pause_log_sites()
        hermes_constants.reset_hermes_home_override(token)


@pytest.mark.asyncio
async def test_process_leased_message_top_gate_logs_once_per_pause_window(tmp_path, caplog):
    """`_process_leased_message`'s own top-of-dispatch gate (round 4's
    structural fix) refused silently before round 5."""
    import hermes_constants
    from agent import estop as real_estop

    home = tmp_path / "hh"
    home.mkdir()
    token = hermes_constants.set_hermes_home_override(str(home))
    adapter_module._clear_estop_pause_log_sites()
    adapter = _adapter_at(tmp_path)
    try:
        assert real_estop.is_engaged() is False
        real_estop.engage(reason="obs-leased-msg")
        with caplog.at_level(logging.INFO, logger="plugin.mupot_gateway.adapter"):
            with pytest.raises(_EstopDeferred):
                await adapter._process_leased_message(dict(PEER_MSG, id="m-a"))
            with pytest.raises(_EstopDeferred):
                await adapter._process_leased_message(dict(PEER_MSG, id="m-b"))
        records = [
            r for r in caplog.records if "refusing leased message dispatch" in r.message
        ]
        assert len(records) == 1, "logged once per leased message instead of once per window"
    finally:
        real_estop.disengage()
        adapter_module._clear_estop_pause_log_sites()
        hermes_constants.reset_hermes_home_override(token)


@pytest.mark.asyncio
async def test_transmit_final_reply_choke_point_logs_once_and_send_result_names_the_pause(
    tmp_path, caplog
):
    """`_transmit_final_reply`'s "prepared" branch (the peer `send` choke
    point for a non-interim final reply) refused silently before round 5,
    AND the resulting `SendResult.error` gave no hint a pause was the cause
    (it fell into the generic `except Exception` and returned `str(exc)`, a
    bare source id). Drives this through the real `adapter.send()` path
    (not by calling `_transmit_final_reply` directly) so both fixes are
    proven end to end."""
    import hermes_constants
    from agent import estop as real_estop

    token = hermes_constants.set_hermes_home_override(str(tmp_path / "hh"))
    (tmp_path / "hh").mkdir()
    adapter_module._clear_estop_pause_log_sites()
    try:
        assert real_estop.is_engaged() is False
        client = rp.ProtocolClient()
        adapter = rp.adapter_at(tmp_path, client)
        await rp.bind_delivery(adapter, rp.source_message())

        real_estop.engage(reason="obs-transmit-final-reply")
        with caplog.at_level(logging.INFO, logger="plugin.mupot_gateway.adapter"):
            result = await adapter.send("sender", "Exact prepared final.")
        assert result.success is False
        assert "send" not in [t for t, _ in client.calls]
        assert "estop_paused" in result.error, (
            "SendResult.error did not name the pause as the refusal reason"
        )
        records = [
            r for r in caplog.records if "refusing peer send" in r.message
        ]
        assert len(records) == 1
    finally:
        real_estop.disengage()
        adapter_module._clear_estop_pause_log_sites()
        hermes_constants.reset_hermes_home_override(token)


@pytest.mark.asyncio
async def test_send_interim_choke_point_logs_once_and_send_result_names_the_pause(
    tmp_path, caplog
):
    """`send()`'s own interim-ACK gate (the OTHER call site that ever
    invokes the peer `send` MCP tool, alongside `_transmit_final_reply`)
    refused silently before round 5, and its `SendResult.error` also gave no
    hint a pause was the cause."""
    import hermes_constants
    from agent import estop as real_estop

    token = hermes_constants.set_hermes_home_override(str(tmp_path / "hh"))
    (tmp_path / "hh").mkdir()
    adapter_module._clear_estop_pause_log_sites()
    try:
        assert real_estop.is_engaged() is False
        client = rp.ProtocolClient()
        adapter = rp.adapter_at(tmp_path, client)
        await rp.bind_delivery(adapter, rp.source_message())

        real_estop.engage(reason="obs-send-interim")
        with caplog.at_level(logging.INFO, logger="plugin.mupot_gateway.adapter"):
            r1 = await adapter.send("sender", "still working", metadata={"_interim_send": True})
            r2 = await adapter.send("sender", "still working", metadata={"_interim_send": True})
        assert r1.success is False and r2.success is False
        assert "send" not in [t for t, _ in client.calls]
        assert "estop_paused" in r1.error
        assert "estop_paused" in r2.error
        records = [
            r for r in caplog.records if "refusing interim progress-ACK send" in r.message
        ]
        assert len(records) == 1, "logged once per interim send instead of once per window"
    finally:
        real_estop.disengage()
        adapter_module._clear_estop_pause_log_sites()
        hermes_constants.reset_hermes_home_override(token)


def test_estop_engaged_failsafe_warns_once_per_failure_window(monkeypatch, caplog):
    """`_estop_engaged()`'s own `except Exception: return True` fail-safe
    (round 4) returned True with zero log records before round 5. A real
    `is_engaged()` failure is an ERROR condition, not a pause/tick -- must
    warn once per distinct failure window, never once per call (which this
    function is, in the hot path of every choke point)."""
    import agent.estop as real_estop_module

    adapter_module._ESTOP_CHECK_FAILSAFE_WARNED = False

    def boom():
        raise OSError("simulated stat failure")

    monkeypatch.setattr(real_estop_module, "is_engaged", boom)
    try:
        with caplog.at_level(logging.WARNING, logger="plugin.mupot_gateway.adapter"):
            assert adapter_module._estop_engaged() is True
            assert adapter_module._estop_engaged() is True
            assert adapter_module._estop_engaged() is True
        failsafe_records = [
            r for r in caplog.records if "failing SAFE" in r.message
        ]
        assert len(failsafe_records) == 1, (
            "warned once per call instead of once per distinct failure window"
        )

        # Recovery: a successful check clears the warned flag, so a NEW
        # failure window (e.g. after the underlying issue reappears) warns
        # again rather than staying silent forever.
        monkeypatch.setattr(real_estop_module, "is_engaged", lambda: False)
        assert adapter_module._estop_engaged() is False

        monkeypatch.setattr(real_estop_module, "is_engaged", boom)
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="plugin.mupot_gateway.adapter"):
            assert adapter_module._estop_engaged() is True
        failsafe_records2 = [
            r for r in caplog.records if "failing SAFE" in r.message
        ]
        assert len(failsafe_records2) == 1, "a NEW failure window did not warn again"
    finally:
        adapter_module._ESTOP_CHECK_FAILSAFE_WARNED = False


@pytest.mark.asyncio
async def test_sender_policy_dlq_append_is_idempotent_across_a_pause_before_the_ack(
    tmp_path,
):
    """P3 (kasra-review re-gate #4, 2026-09-14): `_process_leased_message`'s
    sender_policy DLQ branch writes a DLQ row, THEN acks. A pause landing
    between the write and the ack (`_ack_expected`'s own
    `_refuse_ack_if_estop_engaged` choke point) leaves the message unacked --
    Mupot redelivers it, and this exact branch runs again for the SAME
    message id. Before this round's fix the append was unconditional,
    producing a second row for one message. Proves: after the pause lands
    exactly there, then resumes and the message is (simulated as)
    redelivered, there is exactly one DLQ row for this message id."""
    import hermes_constants
    from agent import estop as real_estop

    token = hermes_constants.set_hermes_home_override(str(tmp_path / "hh"))
    (tmp_path / "hh").mkdir()
    adapter_module._clear_estop_pause_log_sites()
    try:
        assert real_estop.is_engaged() is False
        adapter = _adapter_at(tmp_path, AckOnlyClient())
        evil = dict(PEER_MSG, id="m-evil", from_agent="attacker-not-allowed")

        real_save = adapter.store.save
        engaged_once = {"done": False}

        def save_then_engage_once(value):
            real_save(value)
            if not engaged_once["done"]:
                engaged_once["done"] = True
                real_estop.engage(reason="obs-dlq-race")

        adapter.store.save = save_then_engage_once  # type: ignore[method-assign]

        with pytest.raises(_EstopDeferred):
            await adapter._process_leased_message(evil)

        st = adapter.store.load()
        assert len(st.get("dlq") or []) == 1, "DLQ write did not happen before the pause"
        assert real_estop.is_engaged() is True

        # Resume, restore the real save, and simulate Mupot's own
        # redelivery of the same unacked message.
        real_estop.disengage()
        assert adapter_module._estop_engaged() is False
        adapter.store.save = real_save  # type: ignore[method-assign]
        await adapter._process_leased_message(evil)

        st2 = adapter.store.load()
        dlq_rows = [
            entry
            for entry in (st2.get("dlq") or [])
            if str((entry.get("message") or {}).get("id") or "") == "m-evil"
        ]
        assert len(dlq_rows) == 1, "duplicate DLQ row after redelivery following a pause"
        assert "m-evil" in st2.get("processed", [])
    finally:
        real_estop.disengage()
        adapter_module._clear_estop_pause_log_sites()
        hermes_constants.reset_hermes_home_override(token)
