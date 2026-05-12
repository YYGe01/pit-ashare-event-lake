"""CNINFO announcement daily crawler."""

from __future__ import annotations

import html
import re
import time
from datetime import datetime
from typing import Any

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore
from quant_data_center.utils.instruments import normalize_instrument


CNINFO_QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_REFERER = (
    "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?"
    "lastPage=index&url=disclosure/list/search"
)
CNINFO_STATIC_ROOT = "https://static.cninfo.com.cn/"


class CninfoAnnouncementCrawler:
    """Fetch CNINFO announcement list pages for one disclosure date."""

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
    ) -> dict[str, Any]:
        requests = __import__("requests")
        pages = []
        announcements = []
        total_pages = 1
        page_num = 1
        while page_num <= total_pages:
            payload = _query_payload(crawl_date=crawl_date, page_num=page_num, page_size=page_size)
            response = requests.post(
                CNINFO_QUERY_URL,
                headers=_headers(),
                data=payload,
                timeout=30,
            )
            response.raise_for_status()
            body = response.json()
            page_announcements = list(body.get("announcements") or [])
            announcements.extend(page_announcements)
            total_pages = int(body.get("totalpages") or total_pages or 1)
            pages.append(
                {
                    "page_num": page_num,
                    "request": payload,
                    "status_code": response.status_code,
                    "total_pages": total_pages,
                    "total_record_num": body.get("totalRecordNum"),
                    "announcement_count": len(page_announcements),
                    "announcements": page_announcements,
                }
            )
            page_num += 1
            if max_pages is not None and page_num > max_pages:
                break
            if page_num <= total_pages and min_delay_seconds > 0:
                time.sleep(min_delay_seconds)

        raw_object_id = self.objects.put_json(
            dataset="announcement",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_announcement_{crawl_date}",
            payload={
                "function": "cninfo_his_announcement_query",
                "url": CNINFO_QUERY_URL,
                "params": {
                    "crawl_date": crawl_date,
                    "page_size": page_size,
                    "max_pages": max_pages,
                },
                "pages": pages,
            },
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="announcement",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_announcement_{crawl_date}",
            records=announcements,
        )
        records = _normalize_announcements(
            source_id=source_id,
            crawl_date=crawl_date,
            rows=announcements,
        )
        row_count = self.silver.upsert_announcements(records)
        return {
            "document_count": row_count,
            "raw_object_count": 1 + int(bronze_object_id is not None),
            "raw_object_id": raw_object_id,
            "bronze_object_id": bronze_object_id,
            "provider_record_count": len(announcements),
        }


def _query_payload(*, crawl_date: str, page_num: int, page_size: int) -> dict[str, str]:
    return {
        "pageNum": str(page_num),
        "pageSize": str(page_size),
        "column": "szse",
        "tabName": "fulltext",
        "plate": "",
        "stock": "",
        "searchkey": "",
        "secid": "",
        "category": "",
        "trade": "",
        "seDate": f"{crawl_date}~{crawl_date}",
        "sortName": "",
        "sortType": "",
        "isHLtitle": "true",
    }


def _headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": CNINFO_REFERER,
        "Origin": "https://www.cninfo.com.cn",
    }


def _normalize_announcements(
    *,
    source_id: str,
    crawl_date: str,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    records = []
    for row in rows:
        raw_code = row.get("secCode")
        raw_id = row.get("announcementId")
        raw_title = row.get("announcementTitle") or row.get("shortTitle")
        if raw_code is None or raw_id is None or raw_title is None:
            continue
        try:
            instrument = normalize_instrument(str(raw_code))
        except ValueError:
            continue
        publish_date = _announcement_date(row.get("announcementTime")) or crawl_date
        title = _clean_title(str(raw_title))
        adjunct_url = str(row.get("adjunctUrl") or "").strip()
        url = f"{CNINFO_STATIC_ROOT}{adjunct_url}" if adjunct_url else None
        records.append(
            {
                "announcement_id": f"cninfo_{raw_id}_{instrument}",
                "publish_date": publish_date,
                "instrument": instrument,
                "title": title,
                "url": url,
                "source_id": source_id,
            }
        )
    return list({str(record["announcement_id"]): record for record in records}.values())


def _announcement_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000).date().isoformat()
    except (TypeError, ValueError, OSError):
        return None


def _clean_title(value: str) -> str:
    text = html.unescape(value)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

