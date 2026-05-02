"""
Vendored Relay global ID helpers.

These are the sole functions we need from ``graphql-relay``. Inlining them
removes the direct dependency while keeping full spec compliance.
The encoding is defined by the Relay spec: base64("{type}:{id}").
"""

from __future__ import annotations

import base64
from typing import NamedTuple


class ResolvedGlobalId(NamedTuple):
    type: str
    id: str


def to_global_id(type_: str, id_: object) -> str:
    """Encode a type name and database id into a Relay global ID."""
    return base64.b64encode(f"{type_}:{id_}".encode()).decode()


def from_global_id(global_id: str) -> ResolvedGlobalId:
    """Decode a Relay global ID back into a ``(type, id)`` named tuple."""
    try:
        decoded = base64.b64decode(global_id).decode()
        type_, id_ = decoded.split(":", 1)
        return ResolvedGlobalId(type=type_, id=id_)
    except Exception as exc:
        raise ValueError(f"Invalid Relay global ID: {global_id!r}") from exc
