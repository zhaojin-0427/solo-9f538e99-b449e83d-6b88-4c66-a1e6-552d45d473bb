"""Opaque pagination cursors for the lineage sync API.

A cursor encodes the sequence number of the last event already delivered to
the consumer. Clients treat it as an opaque token; the server is the only side
that encodes/decodes it. Because event sequence numbers are assigned by an
``INTEGER PRIMARY KEY AUTOINCREMENT`` column inside serialised write
transactions, pagination by ``seq > cursor`` cannot skip or repeat batches.
"""

from __future__ import annotations

import base64

_MAGIC = "lin1"  # format marker, lets us reject foreign / malformed tokens


def encode(after_seq: int) -> str:
    """Encode the last delivered event sequence number into an opaque token."""
    raw = f"{_MAGIC}:{int(after_seq)}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode(token: str) -> int:
    """Decode a cursor previously produced by :func:`encode`.

    Raises ``ValueError`` for any token that is malformed, uses an unknown
    format, or does not encode a non-negative sequence number.
    """
    if not isinstance(token, str) or not token:
        raise ValueError("empty cursor")
    padded = token + "=" * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("malformed cursor") from exc
    marker, sep, value = raw.partition(":")
    if sep != ":" or marker != _MAGIC:
        raise ValueError("unsupported cursor format")
    try:
        after_seq = int(value)
    except ValueError as exc:
        raise ValueError("malformed cursor") from exc
    if after_seq < 0:
        raise ValueError("cursor sequence must be non-negative")
    return after_seq
