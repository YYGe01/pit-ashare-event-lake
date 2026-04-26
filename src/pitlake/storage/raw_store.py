"""Append-only filesystem raw store."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.utils import (
    compact_timestamp,
    isoformat,
    sanitize_for_path,
    sha256_bytes,
    stable_json_dumps,
    write_json,
)


@dataclass(frozen=True)
class RawWriteResult:
    raw_object_id: str
    source_id: str
    provider_id: str
    logical_dataset: str
    raw_uri: str
    storage_path: Path
    metadata_path: Path
    mime_type: str
    size_bytes: int
    content_hash: str
    stored_at: str
    first_seen_at: str
    run_id: str | None


class RawStore:
    """Write immutable raw bytes and sidecar metadata to local disk."""

    def __init__(self, settings: ProjectSettings) -> None:
        self.settings = settings
        self.layout = LakeLayout(settings)

    def put_bytes(
        self,
        *,
        source_id: str,
        provider_id: str,
        logical_dataset: str,
        content: bytes,
        extension: str,
        mime_type: str,
        run_id: str | None = None,
        filename_prefix: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RawWriteResult:
        stored_at = isoformat()
        first_seen_at = stored_at
        content_hash = sha256_bytes(content)
        hash_prefix = content_hash.split(":", 1)[1][:16]
        timestamp = compact_timestamp()
        safe_prefix = sanitize_for_path(filename_prefix or source_id)
        safe_source = sanitize_for_path(source_id)
        ext = extension.lstrip(".").lower() or "bin"

        dt = stored_at[:10]
        directory = self.layout.raw_root / f"source={safe_source}" / f"dt={dt}"
        directory.mkdir(parents=True, exist_ok=True)

        filename = f"{safe_prefix}_{timestamp}_{hash_prefix}.{ext}"
        storage_path = directory / filename
        if not storage_path.exists():
            storage_path.write_bytes(content)

        metadata_path = storage_path.with_suffix(storage_path.suffix + ".meta.json")
        sidecar = {
            "raw_object_id": str(uuid4()),
            "source_id": source_id,
            "provider_id": provider_id,
            "logical_dataset": logical_dataset,
            "run_id": run_id,
            "stored_at": stored_at,
            "first_seen_at": first_seen_at,
            "mime_type": mime_type,
            "size_bytes": len(content),
            "content_hash": content_hash,
            "metadata": metadata or {},
        }
        if not metadata_path.exists():
            write_json(metadata_path, sidecar)

        raw_uri = storage_path.relative_to(self.settings.data_lake_root).as_posix()
        return RawWriteResult(
            raw_object_id=sidecar["raw_object_id"],
            source_id=source_id,
            provider_id=provider_id,
            logical_dataset=logical_dataset,
            raw_uri=raw_uri,
            storage_path=storage_path,
            metadata_path=metadata_path,
            mime_type=mime_type,
            size_bytes=len(content),
            content_hash=content_hash,
            stored_at=stored_at,
            first_seen_at=first_seen_at,
            run_id=run_id,
        )

    def put_json(
        self,
        *,
        source_id: str,
        provider_id: str,
        logical_dataset: str,
        payload: Any,
        run_id: str | None = None,
        filename_prefix: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RawWriteResult:
        content = (stable_json_dumps(payload) + "\n").encode("utf-8")
        return self.put_bytes(
            source_id=source_id,
            provider_id=provider_id,
            logical_dataset=logical_dataset,
            content=content,
            extension="json",
            mime_type="application/json",
            run_id=run_id,
            filename_prefix=filename_prefix,
            metadata=metadata,
        )

