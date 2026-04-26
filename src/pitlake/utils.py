"""Small shared utilities for timestamps, hashes, JSON, and paths."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

CN_TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")


def now_cn() -> datetime:
    """Return current time in Asia/Shanghai."""

    return datetime.now(tz=CN_TZ)


def isoformat(dt: datetime | None = None) -> str:
    """Return an ISO 8601 timestamp with second precision."""

    value = dt or now_cn()
    if value.tzinfo is None:
        value = value.replace(tzinfo=CN_TZ)
    return value.isoformat(timespec="seconds")


def compact_timestamp(dt: datetime | None = None) -> str:
    """Return a filename-safe timestamp."""

    value = dt or now_cn()
    if value.tzinfo is None:
        value = value.replace(tzinfo=CN_TZ)
    return value.strftime("%Y%m%dT%H%M%S%z")


def sha256_bytes(data: bytes) -> str:
    """Return a project-standard SHA-256 content hash."""

    return "sha256:" + hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    """Hash UTF-8 encoded text."""

    return sha256_bytes(text.encode("utf-8"))


def stable_json_dumps(value: Any) -> str:
    """Dump JSON in a deterministic form for hashing and manifests."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_json(value: Any) -> str:
    """Hash a JSON-serializable value after stable serialization."""

    return sha256_text(stable_json_dumps(value))


def write_json(path: Path, value: Any) -> None:
    """Write pretty JSON with UTF-8 encoding."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    """Read JSON from a UTF-8 file."""

    return json.loads(path.read_text(encoding="utf-8"))


def sanitize_for_path(value: str, max_length: int = 80) -> str:
    """Make an identifier safe for filenames while keeping it readable."""

    cleaned = re.sub(r"[^A-Za-z0-9_.=-]+", "_", value.strip())
    cleaned = cleaned.strip("._")
    if not cleaned:
        cleaned = "item"
    return cleaned[:max_length]


def resolve_project_path(project_root: Path, value: str | Path) -> Path:
    """Resolve a config path relative to the project root."""

    path = Path(value)
    if path.is_absolute():
        return path
    return (project_root / path).resolve()

