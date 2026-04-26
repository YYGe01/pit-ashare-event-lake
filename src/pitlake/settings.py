"""Project settings loaded from config/project.yaml."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pitlake.exceptions import ConfigError, require_dependency
from pitlake.utils import resolve_project_path


@dataclass(frozen=True)
class ProjectSettings:
    project_root: Path
    config_dir: Path
    data_lake_root: Path
    metadata_db: Path
    logs_dir: Path
    local_backup_dir: Path
    timezone: str
    metadata_backend: str
    raw_store: str
    alert_backend: str
    prefer_free_sources: bool
    paid_providers_enabled: bool

    @classmethod
    def from_yaml(cls, config_path: str | Path) -> "ProjectSettings":
        path = Path(config_path).resolve()
        yaml = require_dependency("yaml", "pyyaml")
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        project_root = path.parent.parent.resolve()

        paths = payload.get("paths", {})
        project = payload.get("project", {})
        runtime = payload.get("runtime", {})
        policy = payload.get("policy", {})

        required_paths = ["data_lake_root", "metadata_db", "logs_dir", "local_backup_dir"]
        missing = [key for key in required_paths if key not in paths]
        if missing:
            raise ConfigError(f"Missing required project path fields: {', '.join(missing)}")

        return cls(
            project_root=project_root,
            config_dir=path.parent.resolve(),
            data_lake_root=resolve_project_path(project_root, paths["data_lake_root"]),
            metadata_db=resolve_project_path(project_root, paths["metadata_db"]),
            logs_dir=resolve_project_path(project_root, paths["logs_dir"]),
            local_backup_dir=resolve_project_path(project_root, paths["local_backup_dir"]),
            timezone=str(project.get("timezone", "Asia/Shanghai")),
            metadata_backend=str(runtime.get("metadata_backend", "sqlite")),
            raw_store=str(runtime.get("raw_store", "filesystem")),
            alert_backend=str(runtime.get("alert_backend", "local_report")),
            prefer_free_sources=bool(policy.get("prefer_free_sources", True)),
            paid_providers_enabled=bool(policy.get("paid_providers_enabled", False)),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "project_root": str(self.project_root),
            "config_dir": str(self.config_dir),
            "data_lake_root": str(self.data_lake_root),
            "metadata_db": str(self.metadata_db),
            "logs_dir": str(self.logs_dir),
            "local_backup_dir": str(self.local_backup_dir),
            "timezone": self.timezone,
            "metadata_backend": self.metadata_backend,
            "raw_store": self.raw_store,
            "alert_backend": self.alert_backend,
            "prefer_free_sources": self.prefer_free_sources,
            "paid_providers_enabled": self.paid_providers_enabled,
        }

