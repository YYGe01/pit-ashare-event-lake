"""Local backup helpers for V0."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.utils import compact_timestamp


@dataclass(frozen=True)
class LocalBackupResult:
    backup_dir: Path
    copied_files: list[Path]
    skipped: list[str]


def backup_collection_state(
    settings: ProjectSettings,
    *,
    target_root: Path | None = None,
    include_raw: bool = False,
) -> LocalBackupResult:
    timestamp = compact_timestamp()
    backup_root = target_root or _resolve_backup_root(settings)
    backup_dir = backup_root / f"backup_{timestamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []
    skipped: list[str] = []

    if settings.metadata_db.exists():
        target = backup_dir / settings.metadata_db.name
        shutil.copy2(settings.metadata_db, target)
        copied.append(target)
    else:
        skipped.append(f"metadata db not found: {settings.metadata_db}")

    layout = LakeLayout(settings)

    manifests_root = layout.manifests_root
    if manifests_root.exists():
        target_root = backup_dir / "published_manifests"
        shutil.copytree(manifests_root, target_root, dirs_exist_ok=True)
        copied.append(target_root)
    else:
        skipped.append(f"manifest root not found: {manifests_root}")

    quality_root = layout.quality_root
    if quality_root.exists():
        target_root = backup_dir / "quality_reports"
        shutil.copytree(quality_root, target_root, dirs_exist_ok=True)
        copied.append(target_root)

    reconciliation_root = layout.reconciliation_root
    if reconciliation_root.exists():
        target_root = backup_dir / "reconciliation_reports"
        shutil.copytree(reconciliation_root, target_root, dirs_exist_ok=True)
        copied.append(target_root)

    if include_raw:
        raw_root = layout.raw_root
        if raw_root.exists():
            target_root = backup_dir / "raw_immutable"
            shutil.copytree(raw_root, target_root, dirs_exist_ok=True)
            copied.append(target_root)
        else:
            skipped.append(f"raw root not found: {raw_root}")

    return LocalBackupResult(backup_dir=backup_dir, copied_files=copied, skipped=skipped)


def backup_metadata_and_manifests(settings: ProjectSettings) -> LocalBackupResult:
    return backup_collection_state(settings, include_raw=False)


def _resolve_backup_root(settings: ProjectSettings) -> Path:
    env_target = os.environ.get("PITLAKE_EXTERNAL_BACKUP_DIR")
    if env_target:
        return Path(env_target).expanduser().resolve()
    return settings.external_backup_dir or settings.local_backup_dir
