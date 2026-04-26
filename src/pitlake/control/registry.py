"""Load and validate provider/source registries."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pitlake.exceptions import ConfigError, require_dependency

REQUIRED_PROVIDER_FIELDS = {
    "provider_id",
    "provider_name",
    "provider_type",
    "auth_method",
    "storage_permission",
}

REQUIRED_SOURCE_FIELDS = {
    "source_id",
    "provider_id",
    "logical_dataset",
    "source_type",
    "access_method",
    "auth_type",
    "priority",
    "enabled",
}


def load_yaml_file(path: Path) -> dict[str, Any]:
    yaml = require_dependency("yaml", "pyyaml")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


@dataclass(frozen=True)
class ProviderRegistry:
    providers: list[dict[str, Any]]
    path: Path

    @classmethod
    def load(cls, config_dir: str | Path) -> "ProviderRegistry":
        path = Path(config_dir) / "provider_registry.yaml"
        payload = load_yaml_file(path)
        return cls(providers=list(payload.get("providers", [])), path=path)

    def by_id(self) -> dict[str, dict[str, Any]]:
        return {provider["provider_id"]: provider for provider in self.providers}

    def validate(self) -> list[str]:
        errors: list[str] = []
        seen: set[str] = set()
        for index, provider in enumerate(self.providers):
            missing = REQUIRED_PROVIDER_FIELDS - provider.keys()
            if missing:
                errors.append(f"provider[{index}] missing fields: {sorted(missing)}")
            provider_id = provider.get("provider_id")
            if provider_id in seen:
                errors.append(f"duplicate provider_id: {provider_id}")
            if provider_id:
                seen.add(provider_id)
        return errors


@dataclass(frozen=True)
class SourceRegistry:
    sources: list[dict[str, Any]]
    path: Path

    @classmethod
    def load(cls, config_dir: str | Path) -> "SourceRegistry":
        path = Path(config_dir) / "source_registry.yaml"
        payload = load_yaml_file(path)
        return cls(sources=list(payload.get("sources", [])), path=path)

    def enabled_sources(self) -> list[dict[str, Any]]:
        return [source for source in self.sources if bool(source.get("enabled", False))]

    def by_id(self) -> dict[str, dict[str, Any]]:
        return {source["source_id"]: source for source in self.sources}

    def validate(
        self,
        provider_registry: ProviderRegistry,
        known_contracts: set[str],
    ) -> list[str]:
        errors: list[str] = []
        provider_ids = set(provider_registry.by_id())
        seen: set[str] = set()
        for index, source in enumerate(self.sources):
            missing = REQUIRED_SOURCE_FIELDS - source.keys()
            if missing:
                errors.append(f"source[{index}] missing fields: {sorted(missing)}")
            source_id = source.get("source_id")
            if source_id in seen:
                errors.append(f"duplicate source_id: {source_id}")
            if source_id:
                seen.add(source_id)
            provider_id = source.get("provider_id")
            if provider_id and provider_id not in provider_ids:
                errors.append(f"source {source_id} references unknown provider_id: {provider_id}")
            dataset = source.get("logical_dataset")
            if dataset and dataset not in known_contracts:
                errors.append(f"source {source_id} references unknown logical_dataset: {dataset}")
        return errors


def validate_control_plane(config_dir: str | Path) -> list[str]:
    from pitlake.control.contracts import ContractCatalog

    providers = ProviderRegistry.load(config_dir)
    sources = SourceRegistry.load(config_dir)
    contracts = ContractCatalog.load(Path(config_dir) / "dataset_contracts")

    errors = []
    errors.extend(providers.validate())
    errors.extend(contracts.validate())
    errors.extend(sources.validate(providers, set(contracts.by_dataset())))
    return errors


def assert_valid_control_plane(config_dir: str | Path) -> None:
    errors = validate_control_plane(config_dir)
    if errors:
        raise ConfigError("Invalid control-plane config:\n" + "\n".join(f"- {e}" for e in errors))

