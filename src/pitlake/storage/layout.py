"""Data lake directory layout."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pitlake.settings import ProjectSettings


@dataclass(frozen=True)
class LakeLayout:
    settings: ProjectSettings

    @property
    def collection_root(self) -> Path:
        return self.settings.data_lake_root / "collection"

    @property
    def control_root(self) -> Path:
        return self.collection_root / "control"

    @property
    def raw_root(self) -> Path:
        return self.collection_root / "raw_immutable"

    @property
    def metadata_root(self) -> Path:
        return self.collection_root / "metadata"

    @property
    def manifests_root(self) -> Path:
        return self.collection_root / "published_manifests"

    @property
    def quality_root(self) -> Path:
        return self.collection_root / "quality_reports"

    @property
    def staging_root(self) -> Path:
        return self.collection_root / "staging"

    @property
    def quarantine_root(self) -> Path:
        return self.collection_root / "quarantine"

    @property
    def logs_root(self) -> Path:
        return self.settings.logs_dir

    @property
    def backups_root(self) -> Path:
        return self.settings.local_backup_dir

    def required_directories(self) -> list[Path]:
        return [
            self.settings.data_lake_root,
            self.collection_root,
            self.control_root,
            self.raw_root,
            self.metadata_root,
            self.manifests_root,
            self.quality_root,
            self.staging_root,
            self.quarantine_root,
            self.logs_root,
            self.backups_root,
        ]

    def create(self) -> None:
        for path in self.required_directories():
            path.mkdir(parents=True, exist_ok=True)

