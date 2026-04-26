"""Schedule policy loader."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pitlake.control.registry import load_yaml_file


@dataclass(frozen=True)
class SchedulePolicy:
    timezone: str
    policies: list[dict[str, Any]]
    path: Path

    @classmethod
    def load(cls, config_dir: str | Path) -> "SchedulePolicy":
        path = Path(config_dir) / "schedule_policy.yaml"
        payload = load_yaml_file(path)
        return cls(
            timezone=str(payload.get("timezone", "Asia/Shanghai")),
            policies=list(payload.get("policies", [])),
            path=path,
        )

