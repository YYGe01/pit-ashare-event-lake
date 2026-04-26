"""CSRC official policy/regulatory list connector."""

from __future__ import annotations

from pitlake.connectors.policy.html_list import OfficialPolicyHtmlListConnector


class CsrcPolicyConnector(OfficialPolicyHtmlListConnector):
    """Collect CSRC news/policy index rows from the official public website."""

    default_list_url = "https://www.csrc.gov.cn/csrc/xwfb/index.shtml"
    default_source_department = "CSRC"
    default_category = "csrc_news_policy_regulatory"
    default_link_pattern = r"https?://www\.csrc\.gov\.cn/csrc/c100028/.*/content\.shtml"
    default_filename_prefix = "policy_csrc_list"
    default_check_name = "csrc_policy_html_list_request"
