"""Shared Telegram forwarding-marker check.

``telegram_control.py``'s deterministic command relay and
``mupot_gateway/human_origin.py``'s origin capture both need to refuse a
forwarded Telegram message before trusting anything about its sender. This is
the ONE place that predicate lives, so a future round can't drift the two
copies apart the way ``lease_ownership.py``'s docstring warns about for
``ATTEMPT_ID_RE``.
"""

from __future__ import annotations

from typing import Any

# Telegram's own markers for "this message is a forward, its apparent sender is
# not who actually wrote the text". Any one of these being present is enough.
FORWARDING_MARKERS = (
    "forward_origin",
    "forward_from",
    "forward_from_chat",
    "forward_date",
)


def is_forwarded_telegram_message(message: Any) -> bool:
    """True when a raw PTB ``telegram.Message`` (or any duck-typed equivalent,
    e.g. a test double) carries any forwarding marker.

    ``message is None`` is treated as forwarded (fail closed): a caller that
    cannot positively verify a message is an original, first-party send must
    never treat the absence of proof as proof of safety. In production this
    only matters for a caller working from Hermes's normalized ``MessageEvent``
    rather than the raw PTB ``Update`` -- a genuine Telegram-sourced event
    always carries its raw ``telegram.Message`` on ``MessageEvent.raw_message``
    (see ``plugins/platforms/telegram/adapter.py``'s ``build_event``,
    ``raw_message=message``); only a synthetic/internal event lacks one, and
    those must never be treated as an original first-party human message
    either.
    """
    if message is None:
        return True
    return any(getattr(message, marker, None) is not None for marker in FORWARDING_MARKERS)
