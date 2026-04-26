"""V0 local alert sink."""

from __future__ import annotations

import json
from pathlib import Path

from pitlake.utils import isoformat


def write_local_alert(logs_dir: Path, message: str) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / "alerts.jsonl"
    line = json.dumps({"time": isoformat(), "message": message}, ensure_ascii=False) + "\n"
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line)
    return path
