"""Settings for quant_data_center."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _resolve_path(project_root: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (project_root / path).resolve()


@dataclass(frozen=True)
class TextEventClassifierSettings:
    """Configuration for single-document text event classification."""

    provider: str
    model: str
    api_key_env: str
    api_key_file: Path | None
    temperature: float
    max_tokens: int


@dataclass(frozen=True)
class QdcSettings:
    """Runtime paths and basic project options."""

    project_root: Path
    config_path: Path
    project_name: str
    timezone: str
    phase: str
    data_root: Path
    database_path: Path
    raw_root: Path
    parquet_root: Path
    qlib_root: Path
    logs_dir: Path
    database_backend: str
    file_format: str
    prefer_free_sources: bool
    paid_providers_enabled: bool
    raw_append_only: bool
    unknown_copyright_policy: str
    text_event_classifier: TextEventClassifierSettings
    universes: dict[str, list[str]]

    @classmethod
    def from_yaml(cls, config_path: str | Path) -> "QdcSettings":
        path = Path(config_path).resolve()
        yaml = __import__("yaml")
        payload: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        project_root = path.parent.parent.resolve()
        project = payload.get("project", {})
        paths = payload.get("paths", {})
        runtime = payload.get("runtime", {})
        policy = payload.get("policy", {})
        text_event_classifier = _parse_text_event_classifier_settings(
            project_root=project_root,
            payload=payload.get("llm", {}),
        )
        universes = _parse_universes(payload.get("universes", {}))

        required_paths = [
            "data_root",
            "database_path",
            "raw_root",
            "parquet_root",
            "qlib_root",
            "logs_dir",
        ]
        missing = [key for key in required_paths if key not in paths]
        if missing:
            raise ValueError(f"Missing required qdc path fields: {', '.join(missing)}")

        return cls(
            project_root=project_root,
            config_path=path,
            project_name=str(project.get("name", "quant_data_center")),
            timezone=str(project.get("timezone", "Asia/Shanghai")),
            phase=str(project.get("phase", "migration")),
            data_root=_resolve_path(project_root, paths["data_root"]),
            database_path=_resolve_path(project_root, paths["database_path"]),
            raw_root=_resolve_path(project_root, paths["raw_root"]),
            parquet_root=_resolve_path(project_root, paths["parquet_root"]),
            qlib_root=_resolve_path(project_root, paths["qlib_root"]),
            logs_dir=_resolve_path(project_root, paths["logs_dir"]),
            database_backend=str(runtime.get("database_backend", "duckdb")),
            file_format=str(runtime.get("file_format", "parquet")),
            prefer_free_sources=bool(policy.get("prefer_free_sources", True)),
            paid_providers_enabled=bool(policy.get("paid_providers_enabled", False)),
            raw_append_only=bool(policy.get("raw_append_only", True)),
            unknown_copyright_policy=str(
                policy.get("unknown_copyright_policy", "metadata_only")
            ),
            text_event_classifier=text_event_classifier,
            universes=universes,
        )

    def universe_symbols(self, universe: str) -> list[str]:
        universe_id = universe.strip()
        if not universe_id:
            return []
        if universe_id not in self.universes:
            available = ", ".join(sorted(self.universes)) or "<none>"
            raise ValueError(f"unknown qdc universe: {universe_id}; available: {available}")
        return list(self.universes[universe_id])

    def required_directories(self) -> list[Path]:
        return [
            self.data_root,
            self.raw_root,
            self.parquet_root / "bronze",
            self.parquet_root / "silver",
            self.parquet_root / "gold",
            self.qlib_root,
            self.logs_dir,
            self.database_path.parent,
        ]

    def as_dict(self) -> dict[str, Any]:
        return {
            "project_root": str(self.project_root),
            "config_path": str(self.config_path),
            "project_name": self.project_name,
            "timezone": self.timezone,
            "phase": self.phase,
            "data_root": str(self.data_root),
            "database_path": str(self.database_path),
            "raw_root": str(self.raw_root),
            "parquet_root": str(self.parquet_root),
            "qlib_root": str(self.qlib_root),
            "logs_dir": str(self.logs_dir),
            "database_backend": self.database_backend,
            "file_format": self.file_format,
            "prefer_free_sources": self.prefer_free_sources,
            "paid_providers_enabled": self.paid_providers_enabled,
            "raw_append_only": self.raw_append_only,
            "unknown_copyright_policy": self.unknown_copyright_policy,
            "text_event_classifier": {
                "provider": self.text_event_classifier.provider,
                "model": self.text_event_classifier.model,
                "api_key_env": self.text_event_classifier.api_key_env,
                "api_key_file": (
                    str(self.text_event_classifier.api_key_file)
                    if self.text_event_classifier.api_key_file
                    else None
                ),
                "temperature": self.text_event_classifier.temperature,
                "max_tokens": self.text_event_classifier.max_tokens,
            },
            "universes": {
                universe: {"symbol_count": len(symbols)}
                for universe, symbols in sorted(self.universes.items())
            },
        }


def _parse_universes(payload: Any) -> dict[str, list[str]]:
    if not payload:
        return {}
    if not isinstance(payload, dict):
        raise ValueError("qdc universes must be a mapping")

    universes: dict[str, list[str]] = {}
    for raw_name, raw_spec in payload.items():
        name = str(raw_name).strip()
        if not name:
            raise ValueError("qdc universe name must not be empty")
        if isinstance(raw_spec, dict):
            raw_symbols = raw_spec.get("symbols", [])
        else:
            raw_symbols = raw_spec
        if isinstance(raw_symbols, str):
            symbols = [item.strip() for item in raw_symbols.split(",") if item.strip()]
        else:
            symbols = [str(item).strip() for item in raw_symbols or [] if str(item).strip()]
        universes[name] = symbols
    return universes


def _parse_text_event_classifier_settings(
    *,
    project_root: Path,
    payload: Any,
) -> TextEventClassifierSettings:
    if payload and not isinstance(payload, dict):
        raise ValueError("qdc llm settings must be a mapping")
    llm = payload or {}
    raw_spec = llm.get("text_event", {})
    if raw_spec and not isinstance(raw_spec, dict):
        raise ValueError("qdc llm.text_event settings must be a mapping")
    spec = raw_spec or {}
    api_key_file = spec.get("api_key_file")
    return TextEventClassifierSettings(
        provider=str(spec.get("provider", "rule")).strip().lower() or "rule",
        model=str(spec.get("model", "deepseek/deepseek-v4-flash")).strip(),
        api_key_env=str(spec.get("api_key_env", "DEEPSEEK_API_KEY")).strip(),
        api_key_file=(
            _resolve_path(project_root, api_key_file)
            if api_key_file is not None and str(api_key_file).strip()
            else None
        ),
        temperature=float(spec.get("temperature", 0)),
        max_tokens=int(spec.get("max_tokens", 512)),
    )
