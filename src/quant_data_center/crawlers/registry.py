"""Static crawler source registry for the first QDC crawler-lite phase."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class CrawlerSourceSpec:
    source_id: str
    source_type: str
    dataset: str
    base_url: str
    enabled: bool
    robots_url: str
    robots_status: str
    terms_review_status: str
    copyright_policy: str
    rate_limit_per_minute: int
    min_delay_seconds: float
    max_retry: int
    parser_version: str
    notes: str

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


DEFAULT_CRAWLER_SOURCES: dict[str, CrawlerSourceSpec] = {
    "cninfo_announcement": CrawlerSourceSpec(
        source_id="cninfo_announcement",
        source_type="announcement",
        dataset="announcement",
        base_url="https://www.cninfo.com.cn/",
        enabled=True,
        robots_url="https://www.cninfo.com.cn/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_public_pdf",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="cninfo_announcement_v1",
        notes="Daily CNINFO announcement list fetcher with public PDF retention and hash metadata.",
    ),
    "sina_finance_news": CrawlerSourceSpec(
        source_id="sina_finance_news",
        source_type="news",
        dataset="news",
        base_url="https://finance.sina.com.cn/",
        enabled=True,
        robots_url="https://finance.sina.com.cn/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_only",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="sina_finance_news_v1",
        notes="Daily metadata-only Sina finance rolling-news补位源; titles are mapped to known active A-share instruments.",
    ),
    "sse_announcement": CrawlerSourceSpec(
        source_id="sse_announcement",
        source_type="announcement",
        dataset="announcement",
        base_url="https://www.sse.com.cn/",
        enabled=True,
        robots_url="https://www.sse.com.cn/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_public_pdf",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="sse_announcement_v1",
        notes=(
            "Daily SSE announcement backup source; rows are accepted only when "
            "SSEDATE equals the crawl date."
        ),
    ),
    "eastmoney_roll_news": CrawlerSourceSpec(
        source_id="eastmoney_roll_news",
        source_type="news",
        dataset="news",
        base_url="https://roll.eastmoney.com/",
        enabled=True,
        robots_url="https://roll.eastmoney.com/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_only",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="eastmoney_roll_news_v1",
        notes=(
            "Daily Eastmoney rolling-news metadata source; rows must carry "
            "explicit YYYY-MM-DD HH:MM publish time for the crawl date."
        ),
    ),
    "nbd_company_news": CrawlerSourceSpec(
        source_id="nbd_company_news",
        source_type="news",
        dataset="news",
        base_url="https://www.nbd.com.cn/",
        enabled=True,
        robots_url="https://www.nbd.com.cn/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_only",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="nbd_company_news_v1",
        notes="Manual/company-news metadata source for dated public article samples; body text is not retained.",
    ),
}

CRAWL_DAILY_SOURCE_IDS = [
    "cninfo_announcement",
    "sse_announcement",
    "sina_finance_news",
    "eastmoney_roll_news",
    "nbd_company_news",
]


def crawler_source_spec(source_id: str) -> CrawlerSourceSpec:
    try:
        return DEFAULT_CRAWLER_SOURCES[source_id]
    except KeyError as exc:
        supported = ", ".join(sorted(DEFAULT_CRAWLER_SOURCES))
        raise ValueError(f"unsupported crawler source_id: {source_id}; supported: {supported}") from exc


def enabled_daily_source_specs(source_id: str | None = None) -> list[CrawlerSourceSpec]:
    if source_id:
        spec = crawler_source_spec(source_id)
        return [spec] if spec.enabled else []
    return [
        DEFAULT_CRAWLER_SOURCES[source_id]
        for source_id in CRAWL_DAILY_SOURCE_IDS
        if DEFAULT_CRAWLER_SOURCES[source_id].enabled
    ]
