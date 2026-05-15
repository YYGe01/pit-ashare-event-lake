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
class DailyPipelineSettings:
    """Default options for the post-close daily pipeline."""

    universe: str | None
    source_id: str | None
    source_ids: list[str] | None
    all_market: bool | None
    skip_stock_basic_refresh: bool | None
    batch_size: int | None
    limit_tasks: int | None
    daily_parallelism: int | None
    provider_uri: str | None
    export_start: str | None
    market_name: str | None
    continue_on_failure: bool | None
    crawl_documents: bool | None
    crawl_source_id: str | None
    crawl_limit_tasks: int | None
    crawl_page_size: int | None
    crawl_max_pages: int | None
    crawl_pdf_limit: int | None
    crawl_parallelism: int | None
    crawl_request_timeout_seconds: float | None
    crawl_source_timeout_seconds: float | None
    crawl_instrument_parallelism: int | None
    crawl_instrument_limit: int | None
    crawl_interaction_schedule: str | None
    crawl_interaction_cold_no_data_days: int | None
    crawl_interaction_cold_check_interval_days: int | None
    crawl_interaction_cold_lookback_days: int | None
    crawl_interaction_unsupported_check_interval_days: int | None
    skip_crawl_pdf_download: bool | None
    skip_factors: bool | None
    skip_sync: bool | None
    skip_quality: bool | None
    skip_export: bool | None


@dataclass(frozen=True)
class QlibProviderSettings:
    """External Qlib provider consumed as the base market data layer."""

    provider_uri: str | None
    required_fields: list[str]


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
    use_environment_proxy: bool
    prefer_free_sources: bool
    paid_providers_enabled: bool
    raw_append_only: bool
    unknown_copyright_policy: str
    text_event_classifier: TextEventClassifierSettings
    daily_pipeline: DailyPipelineSettings
    qlib_provider: QlibProviderSettings
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
        daily_pipeline = _parse_daily_pipeline_settings(payload.get("daily_pipeline", {}))
        qlib_provider = _parse_qlib_provider_settings(payload.get("qlib_provider", {}))
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
            use_environment_proxy=_optional_bool(runtime.get("use_environment_proxy")) or False,
            prefer_free_sources=bool(policy.get("prefer_free_sources", True)),
            paid_providers_enabled=bool(policy.get("paid_providers_enabled", False)),
            raw_append_only=bool(policy.get("raw_append_only", True)),
            unknown_copyright_policy=str(
                policy.get("unknown_copyright_policy", "metadata_only")
            ),
            text_event_classifier=text_event_classifier,
            daily_pipeline=daily_pipeline,
            qlib_provider=qlib_provider,
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
            "use_environment_proxy": self.use_environment_proxy,
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
            "daily_pipeline": {
                "universe": self.daily_pipeline.universe,
                "source_id": self.daily_pipeline.source_id,
                "source_ids": self.daily_pipeline.source_ids,
                "all_market": self.daily_pipeline.all_market,
                "skip_stock_basic_refresh": self.daily_pipeline.skip_stock_basic_refresh,
                "batch_size": self.daily_pipeline.batch_size,
                "limit_tasks": self.daily_pipeline.limit_tasks,
                "daily_parallelism": self.daily_pipeline.daily_parallelism,
                "provider_uri": self.daily_pipeline.provider_uri,
                "export_start": self.daily_pipeline.export_start,
                "market_name": self.daily_pipeline.market_name,
                "continue_on_failure": self.daily_pipeline.continue_on_failure,
                "crawl_documents": self.daily_pipeline.crawl_documents,
                "crawl_source_id": self.daily_pipeline.crawl_source_id,
                "crawl_limit_tasks": self.daily_pipeline.crawl_limit_tasks,
                "crawl_page_size": self.daily_pipeline.crawl_page_size,
                "crawl_max_pages": self.daily_pipeline.crawl_max_pages,
                "crawl_pdf_limit": self.daily_pipeline.crawl_pdf_limit,
                "crawl_parallelism": self.daily_pipeline.crawl_parallelism,
                "crawl_request_timeout_seconds": (
                    self.daily_pipeline.crawl_request_timeout_seconds
                ),
                "crawl_source_timeout_seconds": self.daily_pipeline.crawl_source_timeout_seconds,
                "crawl_instrument_parallelism": self.daily_pipeline.crawl_instrument_parallelism,
                "crawl_instrument_limit": self.daily_pipeline.crawl_instrument_limit,
                "crawl_interaction_schedule": self.daily_pipeline.crawl_interaction_schedule,
                "crawl_interaction_cold_no_data_days": (
                    self.daily_pipeline.crawl_interaction_cold_no_data_days
                ),
                "crawl_interaction_cold_check_interval_days": (
                    self.daily_pipeline.crawl_interaction_cold_check_interval_days
                ),
                "crawl_interaction_cold_lookback_days": (
                    self.daily_pipeline.crawl_interaction_cold_lookback_days
                ),
                "crawl_interaction_unsupported_check_interval_days": (
                    self.daily_pipeline.crawl_interaction_unsupported_check_interval_days
                ),
                "skip_crawl_pdf_download": self.daily_pipeline.skip_crawl_pdf_download,
                "skip_factors": self.daily_pipeline.skip_factors,
                "skip_sync": self.daily_pipeline.skip_sync,
                "skip_quality": self.daily_pipeline.skip_quality,
                "skip_export": self.daily_pipeline.skip_export,
            },
            "qlib_provider": {
                "provider_uri": self.qlib_provider.provider_uri,
                "required_fields": self.qlib_provider.required_fields,
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


def _parse_daily_pipeline_settings(payload: Any) -> DailyPipelineSettings:
    if payload and not isinstance(payload, dict):
        raise ValueError("qdc daily_pipeline settings must be a mapping")
    spec = payload or {}
    return DailyPipelineSettings(
        universe=_optional_str(spec.get("universe")),
        source_id=_optional_str(spec.get("source_id")),
        source_ids=_optional_str_list(spec.get("source_ids")),
        all_market=_optional_bool(spec.get("all_market")),
        skip_stock_basic_refresh=_optional_bool(spec.get("skip_stock_basic_refresh")),
        batch_size=_optional_int(spec.get("batch_size")),
        limit_tasks=_optional_int(spec.get("limit_tasks")),
        daily_parallelism=_optional_int(spec.get("daily_parallelism")),
        provider_uri=_optional_str(spec.get("provider_uri")),
        export_start=_optional_str(spec.get("export_start")),
        market_name=_optional_str(spec.get("market_name")),
        continue_on_failure=_optional_bool(spec.get("continue_on_failure")),
        crawl_documents=_optional_bool(spec.get("crawl_documents")),
        crawl_source_id=_optional_str(spec.get("crawl_source_id")),
        crawl_limit_tasks=_optional_int(spec.get("crawl_limit_tasks")),
        crawl_page_size=_optional_int(spec.get("crawl_page_size")),
        crawl_max_pages=_optional_int(spec.get("crawl_max_pages")),
        crawl_pdf_limit=_optional_int(spec.get("crawl_pdf_limit")),
        crawl_parallelism=_optional_int(spec.get("crawl_parallelism")),
        crawl_request_timeout_seconds=_optional_float(
            spec.get("crawl_request_timeout_seconds")
        ),
        crawl_source_timeout_seconds=_optional_float(spec.get("crawl_source_timeout_seconds")),
        crawl_instrument_parallelism=_optional_int(spec.get("crawl_instrument_parallelism")),
        crawl_instrument_limit=_optional_int(spec.get("crawl_instrument_limit")),
        crawl_interaction_schedule=_optional_str(spec.get("crawl_interaction_schedule")),
        crawl_interaction_cold_no_data_days=_optional_int(
            spec.get("crawl_interaction_cold_no_data_days")
        ),
        crawl_interaction_cold_check_interval_days=_optional_int(
            spec.get("crawl_interaction_cold_check_interval_days")
        ),
        crawl_interaction_cold_lookback_days=_optional_int(
            spec.get("crawl_interaction_cold_lookback_days")
        ),
        crawl_interaction_unsupported_check_interval_days=_optional_int(
            spec.get("crawl_interaction_unsupported_check_interval_days")
        ),
        skip_crawl_pdf_download=_optional_bool(spec.get("skip_crawl_pdf_download")),
        skip_factors=_optional_bool(spec.get("skip_factors")),
        skip_sync=_optional_bool(spec.get("skip_sync")),
        skip_quality=_optional_bool(spec.get("skip_quality")),
        skip_export=_optional_bool(spec.get("skip_export")),
    )


def _parse_qlib_provider_settings(payload: Any) -> QlibProviderSettings:
    if payload and not isinstance(payload, dict):
        raise ValueError("qdc qlib_provider settings must be a mapping")
    spec = payload or {}
    required_fields = _optional_str_list(spec.get("required_fields")) or [
        "$close",
        "$volume",
        "$factor",
    ]
    return QlibProviderSettings(
        provider_uri=_optional_str(spec.get("provider_uri")),
        required_fields=[
            field if field.startswith("$") else f"${field}" for field in required_fields
        ],
    )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_str_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",")]
    else:
        items = [str(item).strip() for item in value or []]
    result = [item for item in items if item]
    return result or None


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    raise ValueError(f"invalid qdc boolean value: {value!r}")


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


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
