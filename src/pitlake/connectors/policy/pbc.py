"""PBC official policy/news list connector."""

from __future__ import annotations

from pitlake.connectors.policy.html_list import OfficialPolicyHtmlListConnector


class PbcPolicyConnector(OfficialPolicyHtmlListConnector):
    """Collect PBC news/policy index rows from the official public website."""

    default_list_url = "http://www.pbc.gov.cn/goutongjiaoliu/113456/113469/index.html"
    default_source_department = "PBC"
    default_category = "monetary_policy_news"
    default_link_pattern = (
        r"https?://www\.pbc\.gov\.cn/goutongjiaoliu/113456/113469/20\d+/index\.html"
    )
    default_filename_prefix = "policy_pbc_list"
    default_check_name = "pbc_policy_html_list_request"
