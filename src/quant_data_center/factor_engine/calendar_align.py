"""Trade-day alignment helpers for event and text factors."""

from __future__ import annotations

from bisect import bisect_left
from datetime import date, datetime, timedelta
from typing import Any


class TradeDayAligner:
    """Align publish dates to the next known open trade date."""

    def __init__(self, open_trade_dates: list[date]) -> None:
        self.open_trade_dates = sorted(set(open_trade_dates))

    @classmethod
    def from_connection(cls, conn: Any) -> "TradeDayAligner":
        rows = conn.execute(
            """
            select trade_date
            from qdc_silver.trade_calendar
            where is_open = true
            order by trade_date
            """
        ).fetchall()
        return cls([parse_iso_date(row[0]) for row in rows])

    def align(self, publish_date: Any) -> str:
        current = parse_iso_date(publish_date)
        if not self.open_trade_dates:
            return current.isoformat()
        index = bisect_left(self.open_trade_dates, current)
        if index >= len(self.open_trade_dates):
            return current.isoformat()
        return self.open_trade_dates[index].isoformat()


def parse_iso_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    compact = text.replace("-", "").replace("/", "")[:8]
    return datetime.strptime(compact, "%Y%m%d").date()


def date_minus_days(value: str, days: int) -> str:
    return (parse_iso_date(value) - timedelta(days=days)).isoformat()
