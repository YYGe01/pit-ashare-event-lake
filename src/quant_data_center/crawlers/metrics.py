"""Shared source-level crawler metrics."""

from __future__ import annotations

from typing import Any, Iterable


def build_document_source_metrics(
    *,
    provider_record_count: int,
    provider_record_keys: Iterable[Any],
    parsed_record_keys: Iterable[Any],
    mapped_source_record_ids: Iterable[Any],
) -> dict[str, float | int]:
    """Build comparable empty, duplicate, parse and mapping metrics for a source."""

    provider_keys = [_clean_key(value) for value in provider_record_keys]
    provider_keys = [value for value in provider_keys if value]
    parsed_keys = [_clean_key(value) for value in parsed_record_keys]
    parsed_keys = [value for value in parsed_keys if value]
    mapped_keys = {_clean_key(value) for value in mapped_source_record_ids}
    mapped_keys.discard("")

    unique_provider_count = len(set(provider_keys))
    unique_parsed_keys = set(parsed_keys)
    parsed_unique_count = len(unique_parsed_keys)
    mapped_source_count = len(mapped_keys & unique_parsed_keys) if unique_parsed_keys else 0
    duplicate_count = max(provider_record_count - unique_provider_count, 0)
    parse_failed_count = max(provider_record_count - len(parsed_keys), 0)
    mapping_failed_count = max(parsed_unique_count - mapped_source_count, 0)

    return {
        "empty_result_count": 1 if provider_record_count == 0 else 0,
        "empty_result_rate": _rate(1 if provider_record_count == 0 else 0, 1),
        "duplicate_record_count": duplicate_count,
        "duplicate_rate": _rate(duplicate_count, provider_record_count),
        "parse_failed_count": parse_failed_count,
        "parse_failed_rate": _rate(parse_failed_count, provider_record_count),
        "parsed_unique_record_count": parsed_unique_count,
        "mapped_source_record_count": mapped_source_count,
        "mapping_failed_count": mapping_failed_count,
        "mapping_rate": _rate(mapped_source_count, parsed_unique_count),
    }


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _clean_key(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
