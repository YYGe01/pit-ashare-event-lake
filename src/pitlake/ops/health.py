"""Source health helpers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceHealthStatus:
    source_id: str
    status: str
    notes: str = ""

