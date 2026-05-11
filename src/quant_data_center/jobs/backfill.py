"""Backfill task planning helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta


@dataclass(frozen=True)
class BackfillTaskSpec:
    dataset: str
    source_id: str
    universe: str
    start_date: date
    end_date: date
    symbols: list[str]


def parse_date(value: str) -> date:
    text = value.strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return datetime.strptime(text, "%Y-%m-%d").date()


def parse_symbols(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def plan_backfill_tasks(
    *,
    dataset: str,
    source_id: str,
    universe: str,
    start_date: date,
    end_date: date,
    symbols: list[str] | None = None,
    batch_size: int = 0,
    chunk_days: int = 0,
) -> list[BackfillTaskSpec]:
    if start_date > end_date:
        raise ValueError("start date must be <= end date")
    if batch_size < 0:
        raise ValueError("batch_size must be >= 0")
    if chunk_days < 0:
        raise ValueError("chunk_days must be >= 0")

    date_ranges = _date_ranges(start_date=start_date, end_date=end_date, chunk_days=chunk_days)
    symbol_batches = _symbol_batches(symbols or [], batch_size=batch_size)
    tasks = []
    for range_start, range_end in date_ranges:
        for symbol_batch in symbol_batches:
            tasks.append(
                BackfillTaskSpec(
                    dataset=dataset,
                    source_id=source_id,
                    universe=universe,
                    start_date=range_start,
                    end_date=range_end,
                    symbols=symbol_batch,
                )
            )
    return tasks


def _date_ranges(*, start_date: date, end_date: date, chunk_days: int) -> list[tuple[date, date]]:
    if chunk_days <= 0:
        return [(start_date, end_date)]
    ranges = []
    current = start_date
    while current <= end_date:
        chunk_end = min(end_date, current + timedelta(days=chunk_days - 1))
        ranges.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return ranges


def _symbol_batches(symbols: list[str], *, batch_size: int) -> list[list[str]]:
    if not symbols:
        return [[]]
    if batch_size <= 0:
        return [symbols]
    return [symbols[index : index + batch_size] for index in range(0, len(symbols), batch_size)]
