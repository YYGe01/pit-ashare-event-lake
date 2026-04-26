from pathlib import Path

import pytest

from pitlake.connectors.policy.csrc import CsrcPolicyConnector
from pitlake.connectors.policy.gov_cn import GovCnPolicyConnector
from pitlake.connectors.policy.pbc import PbcPolicyConnector
from pitlake.control.contracts import DatasetContract
from pitlake.control.registry import load_yaml_file
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore
from pitlake.storage.raw_store import RawStore


def make_settings(tmp_path: Path) -> ProjectSettings:
    return ProjectSettings(
        project_root=tmp_path,
        config_dir=Path("config").resolve(),
        data_lake_root=tmp_path / "data_lake",
        metadata_db=tmp_path / "data_lake" / "collection" / "metadata" / "pitlake.sqlite",
        logs_dir=tmp_path / "data_lake" / "collection" / "logs",
        local_backup_dir=tmp_path / "data_lake" / "backups" / "local",
        timezone="Asia/Shanghai",
        metadata_backend="sqlite",
        raw_store="filesystem",
        alert_backend="local_report",
        prefer_free_sources=True,
        paid_providers_enabled=False,
    )


class FakeResponse:
    status_code = 200
    apparent_encoding = "utf-8"
    headers = {"content-type": "text/html; charset=utf-8"}

    def __init__(self, html: str) -> None:
        self.content = html.encode("utf-8")
        self.encoding: str | None = None
        self.url = "https://example.invalid/list.html"

    @property
    def text(self) -> str:
        return self.content.decode(self.encoding or "utf-8")

    def raise_for_status(self) -> None:
        return None


@pytest.mark.parametrize(
    (
        "connector_cls",
        "source_id",
        "provider_id",
        "html",
        "expected_title",
        "expected_url_part",
        "expected_department",
        "expected_category",
        "expected_publish_time",
    ),
    [
        (
            CsrcPolicyConnector,
            "csrc_policy_news",
            "csrc",
            """
            <html><body><ul>
              <li>
                <a href="/csrc/c100028/c7628252/content.shtml">
                  中国证监会发布《上市公司董事会秘书监管规则》
                </a>
                <span class="time">2026-04-24</span>
              </li>
            </ul></body></html>
            """,
            "中国证监会发布《上市公司董事会秘书监管规则》",
            "https://www.csrc.gov.cn/csrc/c100028/c7628252/content.shtml",
            "CSRC",
            "csrc_news_policy_regulatory",
            "2026-04-24T00:00:00+08:00",
        ),
        (
            GovCnPolicyConnector,
            "gov_cn_policy",
            "gov_cn",
            """
            <html><body><ul>
              <li>
                <a href="./content/202604/content_7066483.htm">
                  国务院关于推进服务业扩能提质的意见
                </a>
                <span>2026-04-21</span>
              </li>
            </ul></body></html>
            """,
            "国务院关于推进服务业扩能提质的意见",
            "https://www.gov.cn/zhengce/content/202604/content_7066483.htm",
            "gov.cn",
            "central_policy",
            "2026-04-21T00:00:00+08:00",
        ),
        (
            PbcPolicyConnector,
            "pbc_policy_news",
            "pbc",
            """
            <html><body><ul>
              <li>
                <a href="/goutongjiaoliu/113456/113469/2026042314503995159/index.html"
                   title="中国人民银行公告〔2026〕第12号">
                  中国人民银行公告〔2026〕第12号
                </a>
                <span>2026-04-24</span>
              </li>
            </ul></body></html>
            """,
            "中国人民银行公告〔2026〕第12号",
            "http://www.pbc.gov.cn/goutongjiaoliu/113456/113469/2026042314503995159/index.html",
            "PBC",
            "monetary_policy_news",
            "2026-04-24T00:00:00+08:00",
        ),
    ],
)
def test_official_policy_html_connectors_collect_index_rows(
    tmp_path: Path,
    monkeypatch,
    connector_cls,
    source_id: str,
    provider_id: str,
    html: str,
    expected_title: str,
    expected_url_part: str,
    expected_department: str,
    expected_category: str,
    expected_publish_time: str,
) -> None:
    calls: dict[str, object] = {}

    def fake_get(url: str, headers: dict, timeout: int) -> FakeResponse:
        calls["url"] = url
        calls["headers"] = headers
        calls["timeout"] = timeout
        return FakeResponse(html)

    monkeypatch.setattr("requests.get", fake_get)
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/policy_regulatory_doc.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = connector_cls(
        settings=settings,
        source_config={
            "source_id": source_id,
            "provider_id": provider_id,
            "logical_dataset": "policy_regulatory_doc",
            "default_options": {"limit_items": 5, "timeout_seconds": 5},
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id=source_id,
        provider_id=provider_id,
        logical_dataset="policy_regulatory_doc",
        connector_name=connector.connector_name,
        connector_version=connector.connector_version,
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.request_count == 1
    assert stats.success_count == 1
    assert stats.error_count == 0
    assert stats.new_item_count == 1
    assert calls["timeout"] == 5
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select title, source_url, source_publish_time, observed_payload_json
            from raw_item_version
            where logical_dataset = 'policy_regulatory_doc'
            """
        ).fetchall()
        raw_count = conn.execute(
            "select count(*) as count from raw_object where source_id = ?",
            (source_id,),
        ).fetchone()["count"]

    assert raw_count == 1
    assert len(rows) == 1
    assert rows[0]["title"] == expected_title
    assert rows[0]["source_url"] == expected_url_part
    assert rows[0]["source_publish_time"] == expected_publish_time
    assert f'"source_department":"{expected_department}"' in rows[0]["observed_payload_json"]
    assert f'"category":"{expected_category}"' in rows[0]["observed_payload_json"]
