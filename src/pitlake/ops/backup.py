"""Local backup helpers for V0."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from pitlake.settings import ProjectSettings
from pitlake.utils import compact_timestamp


@dataclass(frozen=True)
class LocalBackupResult:
    backup_dir: Path
    copied_files: list[Path]


def backup_metadata_and_manifests(settings: ProjectSettings) -> LocalBackupResult:
    timestamp = compact_timestamp()
    backup_dir = settings.local_backup_dir / f"backup_{timestamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []

    if settings.metadata_db.exists():
        target = backup_dir / settings.metadata_db.name
        shutil.copy2(settings.metadata_db, target)
        copied.append(target)

    manifests_root = settings.data_lake_root / "collection" / "published_manifests"
    if manifests_root.exists():
        target_root = backup_dir / "published_manifests"
        shutil.copytree(manifests_root, target_root, dirs_exist_ok=True)
        copied.append(target_root)

    return LocalBackupResult(backup_dir=backup_dir, copied_files=copied)

