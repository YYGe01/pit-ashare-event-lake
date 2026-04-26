"""SQL snippets for point-in-time replay."""

from __future__ import annotations


def observed_min_asof_query(logical_dataset: str, as_of_time: str) -> tuple[str, tuple[str, str]]:
    """Return the V0 as-of query for raw_item_version."""

    query = """
    select *
    from raw_item_version
    where logical_dataset = ?
      and first_seen_at <= ?
      and quality_status in ('pass', 'warning')
    order by source_item_key, first_seen_at
    """
    return query, (logical_dataset, as_of_time)

