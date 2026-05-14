from __future__ import annotations

import os

from quant_data_center.crawlers.date_scan import scan_rolling_date_window
from quant_data_center.crawlers.registry import crawler_source_spec
from quant_data_center.crawlers.runtime import call_with_proxy_policy


def test_rolling_date_scan_fast_seeks_to_target_window() -> None:
    calls: list[int] = []

    def fetch_page(page_num: int):
        calls.append(page_num)
        if page_num in {1, 2, 4, 8, 12, 13}:
            date_text = "2026-05-11"
        elif page_num in {14, 15, 16}:
            date_text = "2026-05-10"
        elif page_num in {17, 18}:
            date_text = "2026-05-09"
        else:
            date_text = "2026-05-11"
        rows = [
            {
                "id": f"news-{page_num}",
                "publish_time": f"{date_text} 10:00:00",
                "title": f"page {page_num}",
            }
        ]
        return rows, {"page_num": page_num, "news_count": len(rows)}

    result = scan_rolling_date_window(
        target_date="2026-05-10",
        fetch_page=fetch_page,
        publish_time_getter=lambda row: row["publish_time"],
        max_pages=20,
    )

    assert calls == [1, 2, 4, 8, 16, 12, 14, 13, 15, 17, 18]
    assert [row["id"] for row in result.target_rows] == [
        "news-14",
        "news-15",
        "news-16",
    ]
    assert result.manifest_fields["date_scan_complete"] is True
    assert result.manifest_fields["date_scan_stop_reason"] == "older_page_lookahead"
    assert result.manifest_fields["date_scan_fetch_strategy"] == (
        "exponential_probe_binary_seek_then_sequential_window"
    )
    assert result.manifest_fields["date_scan_sequential_start_page"] == 12
    assert result.manifest_fields["date_scan_probe_pages"] == [1, 2, 4, 8]
    assert result.manifest_fields["date_scan_first_target_page"] == 14
    assert result.manifest_fields["date_scan_last_target_page"] == 16


def test_eastmoney_roll_news_uses_fast_metadata_delay() -> None:
    spec = crawler_source_spec("eastmoney_roll_news")

    assert spec.copyright_policy == "metadata_only"
    assert spec.rate_limit_per_minute == 120
    assert spec.min_delay_seconds == 0.5


def test_call_with_proxy_policy_temporarily_removes_proxy_environment(monkeypatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:8888")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:8888")
    observed = {}

    def provider_call() -> str:
        observed["http"] = os.environ.get("HTTP_PROXY")
        observed["https"] = os.environ.get("HTTPS_PROXY")
        return "ok"

    assert call_with_proxy_policy(provider_call, use_environment_proxy=False) == "ok"

    assert observed == {"http": None, "https": None}
    assert os.environ["HTTP_PROXY"] == "http://127.0.0.1:8888"
    assert os.environ["HTTPS_PROXY"] == "http://127.0.0.1:8888"
