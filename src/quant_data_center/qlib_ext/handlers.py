"""Qlib handlers that consume QDC-exported external factors."""

from __future__ import annotations

try:
    from qlib.contrib.data.handler import Alpha158
except ImportError as exc:  # pragma: no cover - exercised only without qlib installed.
    _QLIB_IMPORT_ERROR: ImportError | None = exc

    class Alpha158:  # type: ignore[no-redef]
        pass

else:
    _QLIB_IMPORT_ERROR = None


DEFAULT_EXTERNAL_FIELDS = (
    "news_count",
    "news_sentiment_mean",
    "news_positive_count",
    "news_negative_count",
    "news_growth_count",
    "news_risk_count",
    "news_financing_count",
    "news_weighted_sentiment_sum",
    "news_importance_sum",
    "news_contract_count",
    "news_buyback_count",
    "news_shareholder_change_count",
    "news_regulatory_count",
    "news_litigation_count",
    "news_performance_count",
    "announcement_count",
    "announcement_growth_count",
    "announcement_risk_count",
    "announcement_financing_count",
    "announcement_operation_count",
    "announcement_sentiment_mean",
    "announcement_positive_count",
    "announcement_negative_count",
    "announcement_weighted_sentiment_sum",
    "announcement_importance_sum",
    "announcement_contract_count",
    "announcement_buyback_count",
    "announcement_shareholder_change_count",
    "announcement_regulatory_count",
    "announcement_litigation_count",
    "announcement_performance_count",
    "research_report_count",
    "research_institution_count",
    "research_analyst_count",
    "research_rating_positive_count",
    "research_rating_neutral_count",
    "research_rating_negative_count",
    "research_risk_count",
    "research_topic_strength",
    "research_sentiment_mean",
    "question_count",
    "reply_count",
    "reply_delay_hours_mean",
    "risk_topic_count",
    "new_business_topic_count",
    "sentiment_mean",
    "public_sentiment_count",
    "public_sentiment_heat_mean",
    "public_sentiment_rank_best",
    "public_sentiment_keyword_count",
    "public_sentiment_risk_topic_count",
    "public_sentiment_new_business_topic_count",
    "public_sentiment_sentiment_mean",
)
DEFAULT_EXTERNAL_WINDOWS = (0, 3, 5)


class QdcAlpha158WithExternal(Alpha158):
    """Alpha158 plus stable daily factors exported by quant_data_center."""

    def __init__(
        self,
        *,
        external_fields: list[str] | tuple[str, ...] | None = None,
        external_windows: list[int] | tuple[int, ...] | None = None,
        **kwargs,
    ) -> None:
        if _QLIB_IMPORT_ERROR is not None:
            raise ImportError("QdcAlpha158WithExternal requires qlib to be installed") from (
                _QLIB_IMPORT_ERROR
            )
        self.external_fields = tuple(external_fields or DEFAULT_EXTERNAL_FIELDS)
        self.external_windows = tuple(external_windows or DEFAULT_EXTERNAL_WINDOWS)
        super().__init__(**kwargs)

    def get_feature_config(self):  # type: ignore[no-untyped-def]
        fields, names = super().get_feature_config()
        fields = list(fields)
        names = list(names)
        for raw_field in self.external_fields:
            field_expr = raw_field if raw_field.startswith("$") else f"${raw_field}"
            base_name = field_expr.removeprefix("$").upper()
            for window in self.external_windows:
                window = int(window)
                if window <= 0:
                    fields.append(field_expr)
                    names.append(f"QDC_{base_name}0")
                else:
                    fields.append(f"Mean({field_expr}, {window})")
                    names.append(f"QDC_{base_name}_MEAN{window}")
        return fields, names
