from pathlib import Path

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.connectors.runner import ConnectorRunner
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore


class FlakyConnector(BaseConnector):
    attempts = 0

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, object] | None = None) -> RunStats:
        del run_id, options
        type(self).attempts += 1
        if type(self).attempts == 1:
            raise RuntimeError("transient connector failure")
        return RunStats(request_count=1, success_count=1, new_item_count=1)


def make_settings(tmp_path: Path) -> ProjectSettings:
    config_dir = tmp_path / "config"
    contract_dir = config_dir / "dataset_contracts"
    contract_dir.mkdir(parents=True)
    (contract_dir / "sample_dataset.yaml").write_text(
        """
logical_dataset: sample_dataset
contract_version: 1
primary_key_fields:
  - provider_id
  - item_id
required_fields:
  - provider_id
  - item_id
optional_fields: []
quality_rules: {}
""",
        encoding="utf-8",
    )
    (config_dir / "source_registry.yaml").write_text(
        """
sources:
  - source_id: flaky_source
    provider_id: sample_provider
    logical_dataset: sample_dataset
    source_type: python_api
    access_method: test
    auth_type: none
    priority: P0
    enabled: true
    adapter_class: pitlake.connectors.fake.FlakyConnector
""",
        encoding="utf-8",
    )
    return ProjectSettings(
        project_root=tmp_path,
        config_dir=config_dir,
        data_lake_root=tmp_path / "data_lake",
        metadata_db=tmp_path / "data_lake" / "collection" / "metadata" / "pitlake.sqlite",
        logs_dir=tmp_path / "data_lake" / "collection" / "logs",
        local_backup_dir=tmp_path / "data_lake" / "backups" / "local",
        timezone="Asia/Shanghai",
        metadata_backend="sqlite",
        raw_store="filesystem",
        alert_backend="local_report",
        prefer_free_sources=True,
        paid_providers_enabled=False,
    )


def test_runner_retries_uncaught_connector_exception(tmp_path: Path, monkeypatch) -> None:
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    MetadataStore(settings).init_schema()
    FlakyConnector.attempts = 0
    monkeypatch.setattr(
        "pitlake.connectors.runner.load_connector_class",
        lambda adapter_class: FlakyConnector,
    )

    result = ConnectorRunner(settings).run_source(
        source_id="flaky_source",
        generate_manifest=False,
        max_attempts=2,
    )

    assert result.status == "success"
    assert result.attempts == 2
    assert result.error_message is None
