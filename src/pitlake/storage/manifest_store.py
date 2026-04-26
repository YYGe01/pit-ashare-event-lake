"""Collection manifest generation."""

from __future__ import annotations

from dataclasses import dataclass

from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore
from pitlake.utils import compact_timestamp, isoformat, sha256_json, write_json


@dataclass(frozen=True)
class ManifestStore:
    settings: ProjectSettings

    def generate_daily_manifest(
        self,
        *,
        manifest_date: str,
        metadata_store: MetadataStore,
        status: str = "complete",
    ) -> dict:
        layout = LakeLayout(self.settings)
        runs = metadata_store.fetch_runs_for_day(manifest_date)
        raw_objects = metadata_store.fetch_raw_objects_for_day(manifest_date)
        quality = metadata_store.fetch_quality_for_day(manifest_date)
        created_at = isoformat()

        datasets: dict[str, dict] = {}
        for raw in raw_objects:
            dataset = raw["logical_dataset"]
            current = datasets.setdefault(
                dataset,
                {
                    "logical_dataset": dataset,
                    "providers": set(),
                    "sources": set(),
                    "raw_object_count": 0,
                    "content_hashes": [],
                },
            )
            current["providers"].add(raw["provider_id"])
            current["sources"].add(raw["source_id"])
            current["raw_object_count"] += 1
            current["content_hashes"].append(raw["content_hash"])

        dataset_payload = []
        for dataset in sorted(datasets.values(), key=lambda item: item["logical_dataset"]):
            hashes = sorted(dataset.pop("content_hashes"))
            dataset["providers"] = sorted(dataset["providers"])
            dataset["sources"] = sorted(dataset["sources"])
            dataset["content_hash_root"] = sha256_json(hashes)
            dataset_payload.append(dataset)

        error_count = sum(1 for run in runs if run["status"] not in {"success", "complete"})
        new_item_count = sum(int(run.get("new_item_count") or 0) for run in runs)

        manifest_id = f"{manifest_date}-daily-{compact_timestamp()}"
        manifest = {
            "manifest_id": manifest_id,
            "manifest_type": "daily",
            "manifest_date": manifest_date,
            "created_at": created_at,
            "status": status,
            "summary": {
                "run_count": len(runs),
                "raw_object_count": len(raw_objects),
                "new_item_count": new_item_count,
                "error_count": error_count,
                "quality_check_count": len(quality),
            },
            "datasets": dataset_payload,
            "runs": runs,
            "raw_objects": raw_objects,
            "quality_checks": quality,
        }
        manifest_hash = sha256_json(manifest)
        hash_prefix = manifest_hash.split(":", 1)[1][:16]
        manifest["manifest_hash"] = manifest_hash

        manifest_dir = layout.manifests_root / f"dt={manifest_date}"
        manifest_path = manifest_dir / f"collection_manifest_{hash_prefix}.json"
        latest_path = manifest_dir / "latest_collection_manifest.json"
        manifest["manifest_path"] = manifest_path.relative_to(self.settings.data_lake_root).as_posix()
        write_json(manifest_path, manifest)
        write_json(latest_path, manifest)
        metadata_store.insert_manifest(manifest)
        return manifest

