"""Raw and bronze object writers for quant_data_center."""

from __future__ import annotations

import hashlib
import json
import re
from io import BytesIO
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase


class QdcObjectStore:
    """Append raw JSON and structured Parquet objects, then index them in DuckDB."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.database = QdcDatabase(settings)

    def put_json(
        self,
        *,
        dataset: str,
        source_id: str,
        partition_value: str,
        stem: str,
        payload: dict[str, Any],
        job_id: str | None = None,
    ) -> str:
        content = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=_json_default)
        path = self._object_path(
            root=self.settings.raw_root,
            dataset=dataset,
            source_id=source_id,
            partition_value=partition_value,
            stem=stem,
            suffix=".json",
        )
        path.write_text(content, encoding="utf-8")
        return self._index_object(
            dataset=dataset,
            source_id=source_id,
            layer="raw",
            path=path,
            content=content.encode("utf-8"),
            job_id=job_id,
        )

    def put_bronze_parquet(
        self,
        *,
        dataset: str,
        source_id: str,
        partition_value: str,
        stem: str,
        records: list[dict[str, Any]],
        job_id: str | None = None,
    ) -> str | None:
        if not records:
            return None
        path = self._object_path(
            root=self.settings.parquet_root / "bronze",
            dataset=dataset,
            source_id=source_id,
            partition_value=partition_value,
            stem=stem,
            suffix=".parquet",
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        buffer = BytesIO()
        pd.DataFrame(records).to_parquet(buffer, index=False)
        content = buffer.getvalue()
        path.write_bytes(content)
        return self._index_object(
            dataset=dataset,
            source_id=source_id,
            layer="bronze",
            path=path,
            content=content,
            job_id=job_id,
        )

    def _object_path(
        self,
        *,
        root: Path,
        dataset: str,
        source_id: str,
        partition_value: str,
        stem: str,
        suffix: str,
    ) -> Path:
        directory = (
            root
            / _safe_segment(dataset)
            / _safe_segment(source_id)
            / f"dt={_safe_segment(partition_value)}"
        )
        directory.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
        filename = f"{timestamp}_{_short_segment(stem)}_{uuid4().hex[:12]}{suffix}"
        return directory / filename

    def _index_object(
        self,
        *,
        dataset: str,
        source_id: str,
        layer: str,
        path: Path,
        content: bytes,
        job_id: str | None,
    ) -> str:
        return self.database.insert_source_object(
            dataset=dataset,
            source_id=source_id,
            layer=layer,
            uri=str(path),
            content_hash=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
            job_id=job_id,
        )


def _safe_segment(value: str) -> str:
    text = str(value).strip() or "unknown"
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", text)


def _short_segment(value: str, *, max_length: int = 32) -> str:
    text = _safe_segment(value)
    if len(text) <= max_length:
        return text
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]
    return f"{text[: max_length - 9]}_{digest}"


def _json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return _json_default(value.item())
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
