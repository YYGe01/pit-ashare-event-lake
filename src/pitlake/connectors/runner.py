"""Connector dynamic loading and run orchestration."""

from __future__ import annotations

import importlib
import time
from dataclasses import dataclass
from typing import Any

from pitlake.connectors.base import BaseConnector, RunStats
from pitlake.control.contracts import ContractCatalog
from pitlake.control.registry import SourceRegistry
from pitlake.settings import ProjectSettings
from pitlake.storage.manifest_store import ManifestStore
from pitlake.storage.metadata_store import MetadataStore
from pitlake.storage.raw_store import RawStore


@dataclass(frozen=True)
class SourceRunResult:
    run_id: str
    source_id: str
    status: str
    stats: RunStats
    manifest: dict[str, Any] | None
    error_message: str | None = None
    attempts: int = 1


def load_connector_class(adapter_class: str) -> type[BaseConnector]:
    module_name, class_name = adapter_class.rsplit(".", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    if not issubclass(cls, BaseConnector):
        raise TypeError(f"{adapter_class} is not a BaseConnector subclass")
    return cls


class ConnectorRunner:
    """Run one configured source through ledger, connector, and manifest generation."""

    def __init__(self, settings: ProjectSettings) -> None:
        self.settings = settings
        self.metadata_store = MetadataStore(settings)
        self.raw_store = RawStore(settings)
        self.contracts = ContractCatalog.load(settings.config_dir / "dataset_contracts")
        self.sources = SourceRegistry.load(settings.config_dir)

    def run_source(
        self,
        *,
        source_id: str,
        trigger_type: str = "manual",
        options: dict[str, Any] | None = None,
        generate_manifest: bool = True,
        max_attempts: int = 1,
        retry_backoff_seconds: float = 0,
    ) -> SourceRunResult:
        source_config = self.sources.by_id()[source_id]
        contract = self.contracts.by_dataset()[source_config["logical_dataset"]]
        adapter_class = source_config["adapter_class"]
        connector_cls = load_connector_class(adapter_class)
        connector = connector_cls(
            settings=self.settings,
            source_config=source_config,
            contract=contract,
            raw_store=self.raw_store,
            metadata_store=self.metadata_store,
        )

        run_id = self.metadata_store.create_run(
            source_id=connector.source_id,
            provider_id=connector.provider_id,
            logical_dataset=connector.logical_dataset,
            connector_name=connector.connector_name,
            connector_version=connector.connector_version,
            trigger_type=trigger_type,
        )

        attempts = max(1, int(max_attempts))
        attempted_count = 0
        error_messages = []
        for attempt in range(1, attempts + 1):
            attempted_count = attempt
            try:
                stats = connector.collect(run_id=run_id, options=options or {})
                status = "success" if stats.error_count == 0 else "partial"
                error_message = None
                break
            except Exception as exc:
                error_messages.append(f"attempt {attempt}: {exc}")
                if attempt < attempts:
                    if retry_backoff_seconds > 0:
                        time.sleep(float(retry_backoff_seconds))
                    continue
                stats = RunStats(error_count=1)
                status = "failed"
                error_message = "; ".join(error_messages)

        self.metadata_store.finish_run(
            run_id,
            status=status,
            request_count=stats.request_count,
            success_count=stats.success_count,
            error_count=stats.error_count,
            new_item_count=stats.new_item_count,
            updated_item_count=stats.updated_item_count,
            duplicate_count=stats.duplicate_count,
            quarantine_count=stats.quarantine_count,
            error_message=error_message,
        )

        manifest = None
        if generate_manifest:
            manifest_date = options.get("manifest_date") if options else None
            if not manifest_date:
                from pitlake.utils import now_cn

                manifest_date = now_cn().date().isoformat()
            manifest = ManifestStore(self.settings).generate_daily_manifest(
                manifest_date=str(manifest_date),
                metadata_store=self.metadata_store,
                status="complete" if status == "success" else "partial",
            )

        return SourceRunResult(
            run_id=run_id,
            source_id=source_id,
            status=status,
            stats=stats,
            manifest=manifest,
            error_message=error_message,
            attempts=attempted_count,
        )
