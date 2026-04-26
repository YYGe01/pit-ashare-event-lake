"""Load dataset contracts that define minimal observed schemas."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pitlake.control.registry import load_yaml_file

REQUIRED_CONTRACT_FIELDS = {
    "logical_dataset",
    "contract_version",
    "primary_key_fields",
    "required_fields",
}


@dataclass(frozen=True)
class DatasetContract:
    logical_dataset: str
    contract_version: int
    primary_key_fields: list[str]
    required_fields: list[str]
    optional_fields: list[str]
    quality_rules: dict[str, Any]
    path: Path

    @classmethod
    def from_payload(cls, payload: dict[str, Any], path: Path) -> "DatasetContract":
        return cls(
            logical_dataset=str(payload["logical_dataset"]),
            contract_version=int(payload["contract_version"]),
            primary_key_fields=list(payload.get("primary_key_fields", [])),
            required_fields=list(payload.get("required_fields", [])),
            optional_fields=list(payload.get("optional_fields", [])),
            quality_rules=dict(payload.get("quality_rules", {})),
            path=path,
        )


@dataclass(frozen=True)
class ContractCatalog:
    contracts: list[DatasetContract]
    directory: Path

    @classmethod
    def load(cls, directory: str | Path) -> "ContractCatalog":
        root = Path(directory)
        contracts: list[DatasetContract] = []
        for path in sorted(root.glob("*.yaml")):
            payload = load_yaml_file(path)
            contracts.append(DatasetContract.from_payload(payload, path))
        return cls(contracts=contracts, directory=root)

    def by_dataset(self) -> dict[str, DatasetContract]:
        return {contract.logical_dataset: contract for contract in self.contracts}

    def validate(self) -> list[str]:
        errors: list[str] = []
        seen: set[str] = set()
        for contract in self.contracts:
            payload = load_yaml_file(contract.path)
            missing = REQUIRED_CONTRACT_FIELDS - payload.keys()
            if missing:
                errors.append(f"{contract.path.name} missing fields: {sorted(missing)}")
            if contract.logical_dataset in seen:
                errors.append(f"duplicate logical_dataset contract: {contract.logical_dataset}")
            seen.add(contract.logical_dataset)
            for field in contract.primary_key_fields:
                if field not in contract.required_fields and field not in contract.optional_fields:
                    errors.append(
                        f"{contract.logical_dataset} primary key field '{field}' is not declared"
                    )
        return errors

