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
        rate_limit_per_minute=120,
        min_delay_seconds=0.5,
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
        rate_limit_per_minute=120,
        min_delay_seconds=0.5,
        max_retry=3,
        parser_version="eastmoney_roll_news_v1",
        notes=(
            "Daily Eastmoney rolling-news metadata source; rows must carry "
            "explicit YYYY-MM-DD HH:MM publish time for the crawl date."
        ),
    ),
    "eastmoney_research_report": CrawlerSourceSpec(
        source_id="eastmoney_research_report",
        source_type="research_report",
        dataset="research_report",
        base_url="https://data.eastmoney.com/report/stock.jshtml",
        enabled=True,
        robots_url="https://data.eastmoney.com/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_only",
        rate_limit_per_minute=60,
        min_delay_seconds=1.0,
        max_retry=3,
        parser_version="eastmoney_research_report_v1",
        notes=(
            "Daily Eastmoney stock research-report metadata source; PDF URLs are retained "
            "but PDF bytes are not downloaded by default."
        ),
    ),
    "cninfo_investor_interaction": CrawlerSourceSpec(
        source_id="cninfo_investor_interaction",
        source_type="investor_interaction",
        dataset="investor_interaction",
        base_url="https://irm.cninfo.com.cn/",
        enabled=True,
        robots_url="https://irm.cninfo.com.cn/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_inline_preview",
        rate_limit_per_minute=60,
        min_delay_seconds=1.0,
        max_retry=3,
        parser_version="cninfo_investor_interaction_v1",
        notes=(
            "Daily CNINFO investor-interaction metadata source; public question/reply "
            "text is retained for factor explainability."
        ),
    ),
    "eastmoney_public_sentiment": CrawlerSourceSpec(
        source_id="eastmoney_public_sentiment",
        source_type="public_sentiment",
        dataset="public_sentiment",
        base_url="https://guba.eastmoney.com/rank/",
        enabled=True,
        robots_url="https://guba.eastmoney.com/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_only",
        rate_limit_per_minute=60,
        min_delay_seconds=1.0,
        max_retry=3,
        parser_version="eastmoney_public_sentiment_v1",
        notes=(
            "Daily Eastmoney public attention metadata source; stores rank, "
            "focus index, score and hot keyword metadata without post bodies."
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
    "sina": CrawlerSourceSpec(
        source_id="sina",
        source_type="news",
        dataset="news",
        base_url="https://finance.sina.com.cn/7x24/",
        enabled=True,
        robots_url="https://finance.sina.com.cn/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_inline_preview",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="vendor_news_v1",
        notes="Vendor-level Sina finance realtime feed via AkShare stock_info_global_sina.",
    ),
    "wallstreetcn": CrawlerSourceSpec(
        source_id="wallstreetcn",
        source_type="news",
        dataset="news",
        base_url="https://wallstreetcn.com/live/global",
        enabled=True,
        robots_url="https://wallstreetcn.com/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_inline_preview",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="vendor_news_v1",
        notes="Vendor-level Wallstreetcn quick-news feed using public live JSON metadata.",
    ),
    "10jqka": CrawlerSourceSpec(
        source_id="10jqka",
        source_type="news",
        dataset="news",
        base_url="https://news.10jqka.com.cn/realtimenews.html",
        enabled=True,
        robots_url="https://news.10jqka.com.cn/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_inline_preview",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="vendor_news_v1",
        notes="Vendor-level 10jqka finance realtime feed via AkShare stock_info_global_ths.",
    ),
    "eastmoney": CrawlerSourceSpec(
        source_id="eastmoney",
        source_type="news",
        dataset="news",
        base_url="https://kuaixun.eastmoney.com/7_24.html",
        enabled=True,
        robots_url="https://kuaixun.eastmoney.com/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_inline_preview",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="vendor_news_v1",
        notes="Vendor-level Eastmoney finance quick-news feed via AkShare stock_info_global_em.",
    ),
    "yuncaijing": CrawlerSourceSpec(
        source_id="yuncaijing",
        source_type="news",
        dataset="news",
        base_url="https://www.yuncaijing.com/insider/main.html",
        enabled=True,
        robots_url="https://www.yuncaijing.com/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_inline_preview",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="vendor_news_v1",
        notes="Vendor-level Yuncaijing quick-news list page parser for dated insider pages.",
    ),
    "fenghuang": CrawlerSourceSpec(
        source_id="fenghuang",
        source_type="news",
        dataset="news",
        base_url="https://finance.ifeng.com/",
        enabled=True,
        robots_url="https://finance.ifeng.com/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_inline_preview",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="vendor_news_v1",
        notes="Vendor-level Fenghuang finance 24h feed using public JSONP metadata.",
    ),
    "jinrongjie": CrawlerSourceSpec(
        source_id="jinrongjie",
        source_type="news",
        dataset="news",
        base_url="https://stock.jrj.com.cn/",
        enabled=True,
        robots_url="https://stock.jrj.com.cn/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_inline_preview",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="vendor_news_v1",
        notes="Vendor-level Jinrongjie dated stock-news JavaScript feed parser.",
    ),
    "cls": CrawlerSourceSpec(
        source_id="cls",
        source_type="news",
        dataset="news",
        base_url="https://www.cls.cn/telegraph",
        enabled=True,
        robots_url="https://www.cls.cn/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_inline_preview",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="vendor_news_v1",
        notes="Vendor-level CLS telegraph feed via AkShare stock_info_global_cls.",
    ),
    "yicai": CrawlerSourceSpec(
        source_id="yicai",
        source_type="news",
        dataset="news",
        base_url="https://www.yicai.com/brief/",
        enabled=True,
        robots_url="https://www.yicai.com/robots.txt",
        robots_status="manual_review_required",
        terms_review_status="manual_review_required",
        copyright_policy="metadata_and_inline_preview",
        rate_limit_per_minute=20,
        min_delay_seconds=3.0,
        max_retry=3,
        parser_version="vendor_news_v1",
        notes="Vendor-level Yicai brief feed using public getbrieflist JSON metadata.",
    ),
}

CRAWL_DAILY_SOURCE_IDS = [
    "cninfo_announcement",
    "sse_announcement",
    "sina_finance_news",
    "eastmoney_roll_news",
    "eastmoney_research_report",
    "cninfo_investor_interaction",
    "eastmoney_public_sentiment",
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
