"""Public sentiment metadata crawlers."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import hashlib
import json
from datetime import datetime
from io import StringIO
from typing import Any

from quant_data_center.crawlers.metrics import build_document_source_metrics
from quant_data_center.crawlers.runtime import (
    call_with_proxy_policy,
    make_deadline,
    raise_if_deadline_exceeded,
    request_timeout,
    sleep_with_deadline,
)
from quant_data_center.factor_engine.text_events import (
    RISK_EVENTS,
    RuleBasedTextEventClassifier,
    event_matches_any,
)
from quant_data_center.factor_engine.text_factors import NEW_BUSINESS_KEYWORDS
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore
from quant_data_center.utils.instruments import normalize_instrument


SOURCE_ID = "eastmoney_public_sentiment"
EASTMONEY_STOCK_COMMENT_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EASTMONEY_GUBA_RANK_URL = "https://guba.eastmoney.com/rank/"
PARSER_VERSION = "eastmoney_public_sentiment_v1"
MAX_KEYWORD_INSTRUMENTS = 60
STOCK_COMMENT_PAGE_SIZE = 500


class EastmoneyPublicSentimentCrawler:
    """Fetch Eastmoney public attention metadata without retaining post bodies."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.objects = QdcObjectStore(settings)
        self.silver = SilverStore(settings)

    def crawl_date(
        self,
        *,
        source_id: str,
        crawl_date: str,
        page_size: int = 30,
        max_pages: int | None = None,
        min_delay_seconds: float = 3.0,
        request_timeout_seconds: float = 30.0,
        source_timeout_seconds: float | None = None,
        instrument_filter: list[str] | None = None,
    ) -> dict[str, Any]:
        if source_id != SOURCE_ID:
            raise ValueError(f"unsupported public sentiment source_id: {source_id}")
        akshare = __import__("akshare")
        requests = __import__("requests")
        deadline = make_deadline(source_timeout_seconds)
        observed_at = _timestamp()
        normalized_filter = (
            {normalize_instrument(value) for value in instrument_filter}
            if instrument_filter
            else None
        )
        fetch_result = _fetch_eastmoney_rows(
            akshare_module=akshare,
            requests_module=requests,
            crawl_date=crawl_date,
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=min_delay_seconds,
            request_timeout_seconds=request_timeout_seconds,
            deadline=deadline,
            use_environment_proxy=self.settings.use_environment_proxy,
        )
        provider_rows = fetch_result["rows"]
        stem = f"eastmoney_public_sentiment_{crawl_date}"
        raw_object_id = self.objects.put_json(
            dataset="public_sentiment",
            source_id=source_id,
            partition_value=crawl_date,
            stem=stem,
            payload={
                "function": "eastmoney_public_sentiment",
                "source_id": source_id,
                "params": {
                    "crawl_date": crawl_date,
                    "page_size": page_size,
                    "max_pages": max_pages,
                    "instrument_filter": instrument_filter or [],
                },
                "provider": fetch_result["provider"],
                "fetch_logs": fetch_result["fetch_logs"],
                "keyword_failures": fetch_result["keyword_failures"],
            },
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="public_sentiment",
            source_id=source_id,
            partition_value=crawl_date,
            stem=stem,
            records=provider_rows,
        )
        records = _normalize_public_sentiment(
            source_id=source_id,
            rows=provider_rows,
            crawl_date=crawl_date,
            instrument_filter=normalized_filter,
            observed_at=observed_at,
            raw_object_id=raw_object_id,
        )
        source_metrics = build_document_source_metrics(
            provider_record_count=len(provider_rows),
            provider_record_keys=(_provider_key(row) for row in provider_rows),
            parsed_record_keys=(
                _provider_key(row)
                for row in provider_rows
                if _is_parsable_row(row, crawl_date=crawl_date)
            ),
            mapped_source_record_ids=(_record_provider_key(record) for record in records),
        )
        document_bundle = self.objects.put_document_bundle(
            dataset="public_sentiment",
            source_id=source_id,
            partition_value=crawl_date,
            stem=stem,
            manifest={
                "function": "eastmoney_public_sentiment",
                "base_url": EASTMONEY_GUBA_RANK_URL,
                "accepted_date_rule": (
                    "stock_comment_em rows are accepted only when the provider trading "
                    "date equals crawl_date; publish_time is left empty when the source "
                    "only exposes a trading date"
                ),
                "copyright_policy": (
                    "metadata_only; stores ranking, focus index, scores and keyword "
                    "metadata only, without post bodies or personal information"
                ),
                "raw_object_id": raw_object_id,
                "provider_record_count": len(provider_rows),
                "keyword_failure_count": len(fetch_result["keyword_failures"]),
                "instrument_filter": instrument_filter or [],
                **source_metrics,
            },
            records=records,
        )
        row_count = self.silver.upsert_public_sentiment(records)
        return {
            "document_count": row_count,
            "raw_object_count": (
                2
                + int(document_bundle["records_object_id"] is not None)
                + int(bronze_object_id is not None)
            ),
            "raw_object_id": raw_object_id,
            "bronze_object_id": bronze_object_id,
            **document_bundle,
            "provider_record_count": len(provider_rows),
            "mapped_record_count": row_count,
            "keyword_failure_count": len(fetch_result["keyword_failures"]),
            **source_metrics,
            "observed_at": observed_at,
        }


def _fetch_eastmoney_rows(
    *,
    akshare_module: Any,
    requests_module: Any,
    crawl_date: str,
    page_size: int,
    max_pages: int | None,
    min_delay_seconds: float,
    request_timeout_seconds: float,
    deadline: float | None,
    use_environment_proxy: bool,
) -> dict[str, Any]:
    raise_if_deadline_exceeded(deadline, source_id=SOURCE_ID)
    comment_rows, comment_log = _fetch_stock_comment_rows(
        requests_module=requests_module,
        request_timeout_seconds=request_timeout_seconds,
        deadline=deadline,
        use_environment_proxy=use_environment_proxy,
    )
    hot_rank_rows: list[dict[str, Any]] = []
    hot_rank_log: dict[str, str] = {}
    if hasattr(akshare_module, "stock_hot_rank_em"):
        try:
            hot_rank_frame, hot_rank_log = _call_akshare(
                akshare_module.stock_hot_rank_em,
                use_environment_proxy=use_environment_proxy,
            )
            hot_rank_rows = _frame_rows(hot_rank_frame)
        except Exception as exc:
            hot_rank_log = {"error": str(exc)[:500], "stdout": "", "stderr": ""}
    hot_rank_by_instrument = {
        _instrument_or_empty(row.get("代码")): row for row in hot_rank_rows
    }
    rows = []
    for row in comment_rows:
        instrument = _instrument_or_empty(row.get("代码"))
        if not instrument:
            continue
        rows.append(_standardize_comment_row(row, hot_rank_by_instrument.get(instrument)))

    keyword_failures: list[dict[str, str]] = []
    keyword_limit = _keyword_limit(page_size=page_size, max_pages=max_pages)
    for index, row in enumerate(_keyword_target_rows(rows, limit=keyword_limit), start=1):
        raise_if_deadline_exceeded(deadline, source_id=SOURCE_ID)
        if index > 1 and min_delay_seconds > 0:
            sleep_with_deadline(min_delay_seconds, deadline=deadline, source_id=SOURCE_ID)
        if not hasattr(akshare_module, "stock_hot_keyword_em"):
            break
        try:
            keyword_frame, _keyword_log = _call_akshare(
                akshare_module.stock_hot_keyword_em,
                row["instrument"],
                use_environment_proxy=use_environment_proxy,
            )
            keywords = _keyword_records(_frame_rows(keyword_frame), crawl_date=crawl_date)
            row["keyword_time"] = keywords[0]["time"] if keywords else None
            row["keyword_text"] = "、".join(item["name"] for item in keywords)
            row["keyword_count"] = len(keywords)
            row["keyword_heat_sum"] = float(sum(item["heat"] for item in keywords))
            row["keywords_json"] = json.dumps(keywords, ensure_ascii=False, sort_keys=True)
        except Exception as exc:
            keyword_failures.append(
                {"instrument": str(row.get("instrument") or ""), "error": str(exc)[:500]}
            )
    return {
        "rows": rows,
        "provider": {
            "comment_row_count": len(comment_rows),
            "hot_rank_row_count": len(hot_rank_rows),
            "keyword_enriched_count": sum(1 for row in rows if row.get("keyword_text")),
        },
        "fetch_logs": {
            "stock_comment_datacenter": comment_log,
            "stock_hot_rank_em": hot_rank_log,
        },
        "keyword_failures": keyword_failures,
    }


def _fetch_stock_comment_rows(
    *,
    requests_module: Any,
    request_timeout_seconds: float,
    deadline: float | None,
    use_environment_proxy: bool,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    first_body = _request_stock_comment_page(
        requests_module=requests_module,
        page_number=1,
        request_timeout_seconds=request_timeout_seconds,
        deadline=deadline,
        use_environment_proxy=use_environment_proxy,
    )
    first_result = first_body.get("result") if isinstance(first_body, dict) else {}
    total_pages = _positive_int((first_result or {}).get("pages")) or 1
    total_count = _positive_int((first_result or {}).get("count"))
    rows = [
        _standardize_stock_comment_api_row(row)
        for row in ((first_result or {}).get("data") or [])
        if isinstance(row, dict)
    ]
    for page_number in range(2, total_pages + 1):
        body = _request_stock_comment_page(
            requests_module=requests_module,
            page_number=page_number,
            request_timeout_seconds=request_timeout_seconds,
            deadline=deadline,
            use_environment_proxy=use_environment_proxy,
        )
        result = body.get("result") if isinstance(body, dict) else {}
        rows.extend(
            _standardize_stock_comment_api_row(row)
            for row in ((result or {}).get("data") or [])
            if isinstance(row, dict)
        )
    return rows, {
        "provider": "eastmoney_datacenter_api",
        "page_size": str(STOCK_COMMENT_PAGE_SIZE),
        "page_count": str(total_pages),
        "provider_count": "" if total_count is None else str(total_count),
        "row_count": str(len(rows)),
    }


def _request_stock_comment_page(
    *,
    requests_module: Any,
    page_number: int,
    request_timeout_seconds: float,
    deadline: float | None,
    use_environment_proxy: bool,
) -> dict[str, Any]:
    raise_if_deadline_exceeded(deadline, source_id=SOURCE_ID)
    response = call_with_proxy_policy(
        requests_module.get,
        EASTMONEY_STOCK_COMMENT_URL,
        params=_stock_comment_params(page_number),
        headers=_stock_comment_headers(),
        timeout=request_timeout(
            deadline=deadline,
            default_seconds=request_timeout_seconds,
            source_id=SOURCE_ID,
        ),
        use_environment_proxy=use_environment_proxy,
    )
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict) or body.get("success") is False:
        raise ValueError(f"eastmoney stock comment response failed on page {page_number}")
    return body


def _stock_comment_params(page_number: int) -> dict[str, str]:
    return {
        "sortColumns": "SECURITY_CODE",
        "sortTypes": "1",
        "pageSize": str(STOCK_COMMENT_PAGE_SIZE),
        "pageNumber": str(page_number),
        "reportName": "RPT_DMSK_TS_STOCKNEW",
        "quoteColumns": (
            "f2~01~SECURITY_CODE~CLOSE_PRICE,f8~01~SECURITY_CODE~TURNOVERRATE,"
            "f3~01~SECURITY_CODE~CHANGE_RATE,f9~01~SECURITY_CODE~PE_DYNAMIC"
        ),
        "columns": "ALL",
        "filter": "",
        "token": "894050c76af8597a853f5b408b759f5d",
    }


def _stock_comment_headers() -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://data.eastmoney.com/stockcomment/",
        "User-Agent": "Mozilla/5.0",
    }


def _standardize_stock_comment_api_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "代码": row.get("SECURITY_CODE"),
        "名称": row.get("SECURITY_NAME_ABBR"),
        "交易日": row.get("TRADE_DATE"),
        "最新价": row.get("CLOSE_PRICE"),
        "涨跌幅": row.get("CHANGE_RATE"),
        "换手率": row.get("TURNOVERRATE"),
        "市盈率": row.get("PE_DYNAMIC"),
        "主力成本": row.get("PRIME_COST"),
        "机构参与度": row.get("ORG_PARTICIPATE"),
        "综合得分": row.get("TOTALSCORE"),
        "上升": row.get("RANK_UP"),
        "目前排名": row.get("RANK"),
        "关注指数": row.get("FOCUS"),
    }


def _standardize_comment_row(
    row: dict[str, Any],
    hot_rank: dict[str, Any] | None,
) -> dict[str, Any]:
    instrument = normalize_instrument(str(row.get("代码") or ""))
    return {
        "instrument": instrument,
        "source_sec_code": instrument[2:],
        "source_sec_name": _text(row.get("名称")),
        "trade_date": _date_text(row.get("交易日")),
        "latest_price": _number(row.get("最新价")),
        "pct_change": _number(row.get("涨跌幅")),
        "turnover_rate": _number(row.get("换手率")),
        "pe_ttm": _number(row.get("市盈率")),
        "main_cost": _number(row.get("主力成本")),
        "institution_participation": _number(row.get("机构参与度")),
        "composite_score": _number(row.get("综合得分")),
        "rank_change": _number(row.get("上升")),
        "current_rank": _number(row.get("目前排名")),
        "focus_index": _number(row.get("关注指数")),
        "guba_hot_rank": _number((hot_rank or {}).get("当前排名")),
        "guba_latest_price": _number((hot_rank or {}).get("最新价")),
        "guba_pct_change": _number((hot_rank or {}).get("涨跌幅")),
        "keyword_time": None,
        "keyword_text": "",
        "keyword_count": 0,
        "keyword_heat_sum": 0.0,
        "keywords_json": "[]",
        "raw_comment_json": json.dumps(row, ensure_ascii=False, sort_keys=True, default=str),
        "raw_hot_rank_json": json.dumps(
            hot_rank or {}, ensure_ascii=False, sort_keys=True, default=str
        ),
    }


def _normalize_public_sentiment(
    *,
    source_id: str,
    rows: list[dict[str, Any]],
    crawl_date: str,
    instrument_filter: set[str] | None,
    observed_at: str,
    raw_object_id: str,
) -> list[dict[str, Any]]:
    classifier = RuleBasedTextEventClassifier()
    records = []
    for row in rows:
        publish_date = _date_text(row.get("trade_date"))
        if publish_date != crawl_date:
            continue
        instrument = str(row.get("instrument") or "")
        if not instrument or (instrument_filter is not None and instrument not in instrument_filter):
            continue
        keyword_text = str(row.get("keyword_text") or "").strip()
        title = _title(row, keyword_text=keyword_text)
        result = classifier.classify(
            title=title,
            body=keyword_text or None,
            document_type="public_sentiment",
        )
        source_record_id = f"{source_id}|{publish_date}|{instrument}"
        records.append(
            {
                "public_sentiment_id": _record_id(source_record_id),
                "publish_date": publish_date,
                "publish_time": _timestamp_or_none(row.get("keyword_time")),
                "instrument": instrument,
                "title": title,
                "url": f"https://guba.eastmoney.com/rank/stock?code={instrument}",
                "source_id": source_id,
                "source_record_id": source_record_id,
                "source_sec_code": row.get("source_sec_code"),
                "source_sec_name": row.get("source_sec_name"),
                "platform": "eastmoney_guba",
                "sentiment_type": "attention_rank",
                "hot_rank": row.get("current_rank") or row.get("guba_hot_rank"),
                "hot_score": row.get("focus_index") or row.get("keyword_heat_sum") or 0.0,
                "rank_change": row.get("rank_change"),
                "keyword_text": keyword_text,
                "keyword_count": row.get("keyword_count") or 0,
                "risk_topic_count": 1.0 if event_matches_any(result, RISK_EVENTS) else 0.0,
                "new_business_topic_count": (
                    1.0 if _contains_new_business_topic(title, keyword_text) else 0.0
                ),
                "sentiment_score": result.sentiment_score,
                "observed_at": observed_at,
                "collect_time": observed_at,
                "raw_object_id": raw_object_id,
                "parser_version": PARSER_VERSION,
            }
        )
    return records


def _call_akshare(function: Any, *args: Any, use_environment_proxy: bool) -> tuple[Any, dict[str, str]]:
    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        result = call_with_proxy_policy(
            function,
            *args,
            use_environment_proxy=use_environment_proxy,
        )
    return result, {"stdout": stdout.getvalue()[-2000:], "stderr": stderr.getvalue()[-2000:]}


def _frame_rows(frame: Any) -> list[dict[str, Any]]:
    if hasattr(frame, "to_dict"):
        return [
            {str(key): _json_value(value) for key, value in row.items()}
            for row in frame.to_dict("records")
        ]
    return []


def _keyword_records(rows: list[dict[str, Any]], *, crawl_date: str) -> list[dict[str, Any]]:
    keywords = []
    for row in rows:
        time_text = _timestamp_or_none(row.get("时间"))
        if time_text and time_text[:10] != crawl_date:
            continue
        name = _text(row.get("概念名称"))
        if not name:
            continue
        keywords.append(
            {
                "time": time_text,
                "name": name,
                "code": _text(row.get("概念代码")),
                "heat": _number(row.get("热度")) or 0.0,
            }
        )
    keywords.sort(key=lambda item: item["heat"], reverse=True)
    return keywords


def _keyword_limit(*, page_size: int, max_pages: int | None) -> int:
    limit = max(0, int(page_size or 0))
    if limit <= 0:
        return 0
    if max_pages is not None:
        limit *= max(1, int(max_pages))
    return min(limit, MAX_KEYWORD_INSTRUMENTS)


def _keyword_target_rows(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    return sorted(rows, key=lambda row: (_rank_sort_value(row), row["instrument"]))[:limit]


def _rank_sort_value(row: dict[str, Any]) -> float:
    for field in ("guba_hot_rank", "current_rank"):
        value = row.get(field)
        if value is not None:
            return float(value)
    return 999999.0


def _provider_key(row: dict[str, Any]) -> str:
    return f"{row.get('trade_date') or ''}|{row.get('instrument') or ''}"


def _record_provider_key(record: dict[str, Any]) -> str:
    return f"{record.get('publish_date') or ''}|{record.get('instrument') or ''}"


def _is_parsable_row(row: dict[str, Any], *, crawl_date: str) -> bool:
    return bool(row.get("instrument") and _date_text(row.get("trade_date")) == crawl_date)


def _title(row: dict[str, Any], *, keyword_text: str) -> str:
    name = str(row.get("source_sec_name") or row.get("instrument") or "").strip()
    rank = row.get("current_rank") or row.get("guba_hot_rank")
    focus = row.get("focus_index")
    parts = [f"{name} 东方财富公开舆情"]
    if rank is not None:
        parts.append(f"人气排名 {int(float(rank))}")
    if focus is not None:
        parts.append(f"关注指数 {float(focus):.2f}")
    if keyword_text:
        parts.append(f"热门关键词 {keyword_text}")
    return "，".join(parts)


def _contains_new_business_topic(title: str, keyword_text: str) -> bool:
    text = f"{title or ''} {keyword_text or ''}".upper()
    return any(keyword.upper() in text for keyword in NEW_BUSINESS_KEYWORDS)


def _record_id(source_record_id: str) -> str:
    return hashlib.sha256(source_record_id.encode("utf-8")).hexdigest()[:32]


def _instrument_or_empty(value: Any) -> str:
    try:
        return normalize_instrument(str(value or ""))
    except ValueError:
        return ""


def _timestamp() -> str:
    return datetime.now().replace(microsecond=0).isoformat(" ")


def _timestamp_or_none(value: Any) -> str | None:
    text = _text(value)
    if not text:
        return None
    if len(text) == 10:
        return None
    return text.replace("T", " ")[:19]


def _date_text(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "date") and not isinstance(value, str):
        return value.date().isoformat()
    text = str(value).strip()
    if not text:
        return None
    return text.replace("/", "-")[:10]


def _text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none"}:
        return ""
    return text


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result != result:
        return None
    return result


def _positive_int(value: Any) -> int | None:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        return _json_value(value.item())
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, float) and value != value:
        return None
    return value
