"""CNINFO investor-interaction metadata crawler."""

from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

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
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore
from quant_data_center.utils.instruments import instrument_to_symbol, normalize_instrument


CNINFO_IR_KEYBOARD_URL = "https://irm.cninfo.com.cn/newircs/index/queryKeyboardInfo"
CNINFO_IR_QUESTION_URL = "https://irm.cninfo.com.cn/newircs/company/question"
CNINFO_IR_DETAIL_URL = "https://irm.cninfo.com.cn/ircs/question/questionDetail"
CNINFO_IR_REFERER = "https://irm.cninfo.com.cn/"
PARSER_VERSION = "cninfo_investor_interaction_v1"
DEFAULT_UNFILTERED_INSTRUMENT_LIMIT = 50


class CninfoInvestorInteractionCrawler:
    """Fetch CNINFO investor-interaction questions for one publish date."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.database = QdcDatabase(settings)
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
        requests = __import__("requests")
        deadline = make_deadline(source_timeout_seconds)
        observed_at = _timestamp()
        instruments, instrument_mode = self._resolve_instruments(instrument_filter)

        pages: list[dict[str, Any]] = []
        provider_rows: list[dict[str, Any]] = []
        org_failures: list[dict[str, str]] = []
        request_index = 0
        for instrument in instruments:
            raise_if_deadline_exceeded(deadline, source_id=source_id)
            symbol = instrument_to_symbol(instrument)
            try:
                org_id = _fetch_org_id(
                    requests=requests,
                    settings=self.settings,
                    symbol=symbol,
                    deadline=deadline,
                    source_id=source_id,
                    request_timeout_seconds=request_timeout_seconds,
                )
            except Exception as exc:
                org_failures.append({"instrument": instrument, "error": str(exc)[:300]})
                continue
            page_num = 1
            total_pages: int | None = None
            while total_pages is None or page_num <= total_pages:
                raise_if_deadline_exceeded(deadline, source_id=source_id)
                if request_index > 0 and min_delay_seconds > 0:
                    sleep_with_deadline(
                        min_delay_seconds, deadline=deadline, source_id=source_id
                    )
                params = _query_params(
                    symbol=symbol,
                    org_id=org_id,
                    crawl_date=crawl_date,
                    page_size=page_size,
                    page_num=page_num,
                )
                response = call_with_proxy_policy(
                    requests.post,
                    CNINFO_IR_QUESTION_URL,
                    headers=_headers(),
                    params=params,
                    timeout=request_timeout(
                        deadline=deadline,
                        default_seconds=request_timeout_seconds,
                        source_id=source_id,
                    ),
                    use_environment_proxy=self.settings.use_environment_proxy,
                )
                response.raise_for_status()
                body = response.json()
                rows = _extract_rows(body)
                for row in rows:
                    row["__instrument"] = instrument
                    row["__org_id"] = org_id
                provider_rows.extend(rows)
                total_pages = _total_pages(body, page_size=page_size)
                if max_pages is not None:
                    total_pages = min(total_pages, max(1, int(max_pages)))
                pages.append(
                    {
                        "instrument": instrument,
                        "stock_code": symbol,
                        "org_id": org_id,
                        "page_num": page_num,
                        "request": params,
                        "status_code": response.status_code,
                        "provider_record_count": len(rows),
                        "total_pages": total_pages,
                        "hits": body.get("total"),
                    }
                )
                request_index += 1
                if not rows:
                    break
                page_num += 1

        raw_object_id = self.objects.put_json(
            dataset="investor_interaction",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_investor_interaction_{crawl_date}",
            payload={
                "function": "cninfo_investor_interaction",
                "url": CNINFO_IR_QUESTION_URL,
                "params": {
                    "crawl_date": crawl_date,
                    "page_size": page_size,
                    "max_pages": max_pages,
                    "instrument_filter": instrument_filter or [],
                    "instrument_mode": instrument_mode,
                    "instrument_count": len(instruments),
                },
                "org_failures": org_failures,
                "pages": pages,
            },
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="investor_interaction",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_investor_interaction_{crawl_date}",
            records=_bronze_records(provider_rows),
        )
        records = _normalize_investor_interactions(
            source_id=source_id,
            rows=provider_rows,
            crawl_date=crawl_date,
            observed_at=observed_at,
            raw_object_id=raw_object_id,
        )
        source_metrics = build_document_source_metrics(
            provider_record_count=len(provider_rows),
            provider_record_keys=(_provider_key(row) for row in provider_rows),
            parsed_record_keys=(_provider_key(row) for row in provider_rows if _is_parsable_row(row)),
            mapped_source_record_ids=(record.get("source_record_id") for record in records),
        )
        document_bundle = self.objects.put_document_bundle(
            dataset="investor_interaction",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_investor_interaction_{crawl_date}",
            manifest={
                "function": "cninfo_investor_interaction",
                "url": CNINFO_IR_QUESTION_URL,
                "accepted_date_rule": "answer/update time is converted to Asia/Shanghai and must equal crawl_date; unanswered rows fall back to question time",
                "copyright_policy": "metadata_and_inline_preview; public question/reply text is retained for factor explainability",
                "raw_object_id": raw_object_id,
                "provider_record_count": len(provider_rows),
                "instrument_filter": instrument_filter or [],
                "instrument_mode": instrument_mode,
                "instrument_count": len(instruments),
                "org_failure_count": len(org_failures),
                **source_metrics,
            },
            records=records,
        )
        row_count = self.silver.upsert_investor_interactions(records)
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
            "instrument_count": len(instruments),
            "org_failure_count": len(org_failures),
            **source_metrics,
            "observed_at": observed_at,
        }

    def _resolve_instruments(self, instrument_filter: list[str] | None) -> tuple[list[str], str]:
        if instrument_filter:
            return sorted({normalize_instrument(value) for value in instrument_filter}), "explicit"
        instruments = self.database.stock_basic_instruments(active_only=True)
        max_instruments = _unfiltered_instrument_limit()
        return instruments[:max_instruments], f"stock_basic_active_first_{max_instruments}"


def _fetch_org_id(
    *,
    requests: Any,
    settings: QdcSettings,
    symbol: str,
    deadline: float | None,
    source_id: str,
    request_timeout_seconds: float,
) -> str:
    response = call_with_proxy_policy(
        requests.post,
        CNINFO_IR_KEYBOARD_URL,
        headers=_headers(),
        params={"_t": "1691144074"},
        data={"keyWord": symbol},
        timeout=request_timeout(
            deadline=deadline,
            default_seconds=request_timeout_seconds,
            source_id=source_id,
        ),
        use_environment_proxy=settings.use_environment_proxy,
    )
    response.raise_for_status()
    data = response.json()
    for item in data.get("data") or []:
        if str(item.get("stockCode") or "").zfill(6) == symbol:
            org_id = str(item.get("secid") or "").strip()
            if org_id:
                return org_id
    raise ValueError(f"missing CNINFO IR orgId for {symbol}")


def _query_params(
    *,
    symbol: str,
    org_id: str,
    crawl_date: str,
    page_size: int,
    page_num: int,
) -> dict[str, str]:
    return {
        "_t": "1691142650",
        "stockcode": symbol,
        "orgId": org_id,
        "pageSize": str(max(1, int(page_size))),
        "pageNum": str(page_num),
        "keyWord": "",
        "startDay": crawl_date,
        "endDay": crawl_date,
    }


def _headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": CNINFO_IR_REFERER,
        "Accept": "application/json,text/plain,*/*",
    }


def _extract_rows(body: Any) -> list[dict[str, Any]]:
    if not isinstance(body, dict):
        return []
    rows = body.get("rows") or []
    return [row for row in rows if isinstance(row, dict)]


def _total_pages(body: dict[str, Any], *, page_size: int) -> int:
    total_page = body.get("totalPage")
    if total_page not in (None, ""):
        return max(1, int(total_page or 1))
    total = body.get("total")
    if total:
        return max(1, math.ceil(int(total) / max(1, int(page_size))))
    return 1


def _bronze_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{str(key): _bronze_value(value) for key, value in row.items()} for row in rows]


def _bronze_value(value: Any) -> Any:
    if value is None:
        return value
    if isinstance(value, (bool, int, float, str)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _normalize_investor_interactions(
    *,
    source_id: str,
    rows: list[dict[str, Any]],
    crawl_date: str,
    observed_at: str,
    raw_object_id: str,
) -> list[dict[str, Any]]:
    classifier = RuleBasedTextEventClassifier()
    records: dict[str, dict[str, Any]] = {}
    for row in rows:
        question_text = _clean_text(row.get("mainContent"))
        question_time = _millis_time(row.get("pubDate"))
        stock_code = _clean_text(row.get("stockCode"))
        answer_text = _clean_text(row.get("attachedContent"))
        answer_time = _millis_time(row.get("attachedPubDate")) or (
            _millis_time(row.get("updateDate")) if answer_text else None
        )
        publish_time = answer_time or _millis_time(row.get("updateDate")) or question_time
        publish_date = publish_time[:10] if publish_time else None
        if not question_text or publish_date != crawl_date or not stock_code:
            continue
        try:
            instrument = normalize_instrument(stock_code)
        except ValueError:
            continue
        source_record_id = _provider_key(row)
        interaction_id = f"cninfo_ir_{_slug(source_record_id)}_{instrument}"
        classification = classifier.classify(
            title=question_text,
            body=answer_text,
            document_type="investor_interaction",
        )
        records[interaction_id] = {
            "investor_interaction_id": interaction_id,
            "publish_date": publish_date,
            "publish_time": publish_time,
            "instrument": instrument,
            "title": question_text,
            "url": f"{CNINFO_IR_DETAIL_URL}?questionId={source_record_id}",
            "source_id": source_id,
            "source_record_id": source_record_id,
            "source_sec_code": stock_code,
            "source_sec_name": _clean_text(row.get("companyShortName")),
            "question_text": question_text,
            "question_time": question_time,
            "answer_text": answer_text,
            "answer_time": answer_time,
            "reply_status": "replied" if answer_text else "pending",
            "reply_delay_hours": _reply_delay_hours(question_time, answer_time),
            "questioner": _clean_text(row.get("authorName")),
            "industry": _first_value(row.get("trade")),
            "channel": _channel_label(row.get("pubClient")),
            "topic_tags": _topic_tags(question_text, answer_text, classification),
            "sentiment_score": classification.sentiment_score,
            "observed_at": observed_at,
            "collect_time": observed_at,
            "raw_object_id": raw_object_id,
            "parser_version": PARSER_VERSION,
        }
    return list(records.values())


def _provider_key(row: dict[str, Any]) -> str:
    return _clean_text(row.get("indexId")) or _fallback_record_id(row)


def _is_parsable_row(row: dict[str, Any]) -> bool:
    return bool(
        _clean_text(row.get("mainContent"))
        and (_millis_time(row.get("attachedPubDate")) or _millis_time(row.get("updateDate")) or _millis_time(row.get("pubDate")))
        and _clean_text(row.get("stockCode"))
        and _provider_key(row)
    )


def _fallback_record_id(row: dict[str, Any]) -> str:
    values = [
        _clean_text(row.get("stockCode")),
        _clean_text(row.get("pubDate")),
        _clean_text(row.get("mainContent")),
    ]
    return "|".join(value for value in values if value)


def _millis_time(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        millis = int(float(value))
    except (TypeError, ValueError):
        return None
    parsed = datetime.fromtimestamp(millis / 1000, tz=timezone.utc).astimezone(
        ZoneInfo("Asia/Shanghai")
    )
    return parsed.replace(tzinfo=None, microsecond=0).isoformat(" ")


def _reply_delay_hours(question_time: str | None, answer_time: str | None) -> float | None:
    if not question_time or not answer_time:
        return None
    try:
        question = datetime.fromisoformat(question_time)
        answer = datetime.fromisoformat(answer_time)
    except ValueError:
        return None
    delay = (answer - question).total_seconds() / 3600
    return round(delay, 4) if delay >= 0 else None


def _topic_tags(question_text: str, answer_text: str | None, classification: Any) -> str:
    tags = list(classification.event_types)
    if event_matches_any(classification, RISK_EVENTS) and "risk" not in tags:
        tags.append("risk")
    if _contains_new_business_topic(question_text, answer_text):
        tags.append("new_business")
    return ",".join(dict.fromkeys(tags))


def _contains_new_business_topic(question_text: str, answer_text: str | None) -> bool:
    text = f"{question_text or ''} {answer_text or ''}".upper()
    return any(keyword.upper() in text for keyword in NEW_BUSINESS_KEYWORDS)


def _channel_label(value: Any) -> str | None:
    text = _clean_text(value)
    return {
        "2": "APP",
        "4": "website",
        "5": "wechat",
        "6": "website",
    }.get(text or "", text)


def _first_value(value: Any) -> str | None:
    if isinstance(value, (list, tuple)) and value:
        return _clean_text(value[0])
    return _clean_text(value)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _slug(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_.=-]+", "_", str(value or "").strip())
    return text[:96] or "unknown"


def _timestamp() -> str:
    return datetime.now().replace(microsecond=0).isoformat(" ")


def _unfiltered_instrument_limit() -> int:
    raw = os.environ.get("QDC_INVESTOR_INTERACTION_MAX_INSTRUMENTS")
    if not raw:
        return DEFAULT_UNFILTERED_INSTRUMENT_LIMIT
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_UNFILTERED_INSTRUMENT_LIMIT
