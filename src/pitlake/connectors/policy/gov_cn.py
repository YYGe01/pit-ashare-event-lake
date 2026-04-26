"""gov.cn central policy list connector."""

from __future__ import annotations

from pitlake.connectors.policy.html_list import OfficialPolicyHtmlListConnector


class GovCnPolicyConnector(OfficialPolicyHtmlListConnector):
    """Collect central policy index rows from gov.cn."""

    default_list_url = "https://www.gov.cn/zhengce/"
    default_source_department = "gov.cn"
    default_category = "central_policy"
    default_link_pattern = (
        r"https?://www\.gov\.cn/zhengce/(?:content/)?20\d{4}/content_\d+\.htm"
    )
    default_filename_prefix = "policy_gov_cn_list"
    default_check_name = "gov_cn_policy_html_list_request"
