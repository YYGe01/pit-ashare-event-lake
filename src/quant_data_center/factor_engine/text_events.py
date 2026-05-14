"""Text event classification providers for unstructured QDC factors."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from quant_data_center.settings import TextEventClassifierSettings


VALID_DOCUMENT_TYPES = {"news", "announcement", "investor_interaction"}
VALID_EVENT_TYPES = {
    "growth",
    "risk",
    "financing",
    "operation",
    "contract",
    "buyback",
    "shareholder_reduce",
    "shareholder_increase",
    "control_change",
    "regulatory",
    "litigation",
    "performance_positive",
    "performance_negative",
    "dividend",
    "pledge",
    "guarantee",
}

GROWTH_EVENTS = {
    "growth",
    "contract",
    "buyback",
    "shareholder_increase",
    "performance_positive",
}
RISK_EVENTS = {
    "risk",
    "regulatory",
    "litigation",
    "shareholder_reduce",
    "performance_negative",
    "pledge",
    "guarantee",
}
FINANCING_EVENTS = {"financing"}
OPERATION_EVENTS = {
    "operation",
    "contract",
    "buyback",
    "shareholder_increase",
    "dividend",
    "control_change",
}
CONTRACT_EVENTS = {"contract"}
BUYBACK_EVENTS = {"buyback"}
SHAREHOLDER_CHANGE_EVENTS = {"shareholder_reduce", "shareholder_increase", "control_change"}
REGULATORY_EVENTS = {"regulatory"}
LITIGATION_EVENTS = {"litigation"}
PERFORMANCE_EVENTS = {"performance_positive", "performance_negative"}


@dataclass(frozen=True)
class TextEventRule:
    """A deterministic keyword rule for one event type."""

    event_type: str
    keywords: tuple[str, ...]
    polarity: float
    importance: float


@dataclass(frozen=True)
class TextEventResult:
    """Structured event result shared by rule and LLM providers."""

    provider: str
    document_type: str
    event_types: tuple[str, ...]
    sentiment_score: float
    importance_score: float
    matched_keywords: tuple[str, ...] = ()
    model: str | None = None
    evidence: str | None = None
    raw_response: str | None = None

    @property
    def weighted_sentiment(self) -> float:
        return self.sentiment_score * self.importance_score

    def to_dict(self, *, include_raw_response: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "provider": self.provider,
            "document_type": self.document_type,
            "event_types": list(self.event_types),
            "sentiment_score": self.sentiment_score,
            "importance_score": self.importance_score,
            "weighted_sentiment": self.weighted_sentiment,
            "matched_keywords": list(self.matched_keywords),
            "model": self.model,
            "evidence": self.evidence,
        }
        if include_raw_response:
            payload["raw_response"] = self.raw_response
        return payload


class TextEventClassifier(Protocol):
    """Provider contract for single-document text event classification."""

    def classify(
        self,
        *,
        title: str,
        body: str | None = None,
        document_type: str = "news",
    ) -> TextEventResult:
        """Classify one text document into structured event signals."""


RULES: tuple[TextEventRule, ...] = (
    TextEventRule(
        "performance_positive",
        ("预增", "业绩增长", "同比增长", "扭亏", "盈利增长", "净利润增长", "业绩快报"),
        1.0,
        0.75,
    ),
    TextEventRule(
        "performance_negative",
        ("预亏", "预减", "业绩下滑", "同比下降", "亏损", "净利润下降", "业绩承压"),
        -1.0,
        0.8,
    ),
    TextEventRule(
        "contract",
        ("中标", "签订合同", "重大合同", "订单", "采购协议", "框架协议", "战略合作"),
        1.0,
        0.7,
    ),
    TextEventRule(
        "growth",
        ("增长", "上升", "提升", "突破", "创新高", "获批", "扩产", "投产"),
        0.75,
        0.55,
    ),
    TextEventRule(
        "buyback",
        ("回购", "拟回购", "股份回购"),
        0.8,
        0.7,
    ),
    TextEventRule(
        "shareholder_increase",
        ("增持", "拟增持", "完成增持"),
        0.7,
        0.65,
    ),
    TextEventRule(
        "shareholder_reduce",
        ("减持", "拟减持", "计划减持", "被动减持"),
        -0.85,
        0.8,
    ),
    TextEventRule(
        "control_change",
        ("实控人变更", "控制权变更", "控股股东变更"),
        0.0,
        0.75,
    ),
    TextEventRule(
        "regulatory",
        ("问询函", "监管函", "立案调查", "行政处罚", "纪律处分", "警示函", "责令改正"),
        -0.9,
        0.9,
    ),
    TextEventRule(
        "litigation",
        ("诉讼", "仲裁", "起诉", "法院", "判决", "执行通知"),
        -0.75,
        0.75,
    ),
    TextEventRule(
        "pledge",
        ("质押", "冻结", "轮候冻结", "司法冻结"),
        -0.75,
        0.75,
    ),
    TextEventRule(
        "risk",
        ("风险", "退市", "ST", "违约", "债务逾期", "资金占用", "违规"),
        -0.85,
        0.8,
    ),
    TextEventRule(
        "financing",
        ("定增", "增发", "配股", "可转债", "融资", "募集资金", "发行股票"),
        0.0,
        0.6,
    ),
    TextEventRule(
        "dividend",
        ("权益分派", "分红", "派息", "现金红利"),
        0.3,
        0.55,
    ),
    TextEventRule(
        "operation",
        ("股权激励", "并购", "重组", "资产收购", "资产出售", "对外投资"),
        0.25,
        0.65,
    ),
    TextEventRule(
        "guarantee",
        ("担保", "对外担保", "违规担保"),
        -0.45,
        0.65,
    ),
)


class RuleBasedTextEventClassifier:
    """Deterministic keyword classifier used by production factor builds."""

    provider = "rule"

    def classify(
        self,
        *,
        title: str,
        body: str | None = None,
        document_type: str = "news",
    ) -> TextEventResult:
        document_type = _normalize_document_type(document_type)
        text = _document_text(title=title, body=body)
        normalized = text.upper()
        matched_rules: list[TextEventRule] = []
        matched_keywords: list[str] = []
        event_types: list[str] = []
        for rule in RULES:
            rule_hits = [keyword for keyword in rule.keywords if keyword.upper() in normalized]
            if not rule_hits:
                continue
            matched_rules.append(rule)
            matched_keywords.extend(rule_hits)
            if rule.event_type not in event_types:
                event_types.append(rule.event_type)

        if not matched_rules:
            return TextEventResult(
                provider=self.provider,
                document_type=document_type,
                event_types=(),
                sentiment_score=0.0,
                importance_score=0.0,
                matched_keywords=(),
            )

        weight_sum = sum(rule.importance for rule in matched_rules)
        sentiment_score = sum(rule.polarity * rule.importance for rule in matched_rules) / weight_sum
        importance_score = max(rule.importance for rule in matched_rules)
        if document_type == "announcement":
            importance_score = min(1.0, importance_score * 1.1)
        return TextEventResult(
            provider=self.provider,
            document_type=document_type,
            event_types=tuple(event_types),
            sentiment_score=_clamp(sentiment_score, -1.0, 1.0),
            importance_score=_clamp(importance_score, 0.0, 1.0),
            matched_keywords=tuple(dict.fromkeys(matched_keywords)),
            evidence=title.strip()[:240] or None,
        )


class LiteLlmTextEventClassifier:
    """Single-document LLM classifier backed by LiteLLM.

    This provider is intentionally not used by the bulk factor build path. It exists so a
    later API key/model can be smoke-tested and swapped in behind the same result contract.
    """

    provider = "llm"

    def __init__(
        self,
        *,
        settings: TextEventClassifierSettings | None = None,
        model: str | None = None,
        api_key_env: str | None = None,
        api_key_file: str | Path | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> None:
        self.model = model or (settings.model if settings else None) or "deepseek/deepseek-v4-flash"
        self.api_key_env = (
            api_key_env or (settings.api_key_env if settings else None) or "DEEPSEEK_API_KEY"
        )
        raw_api_key_file = api_key_file if api_key_file is not None else (
            settings.api_key_file if settings else None
        )
        self.api_key_file = Path(raw_api_key_file).expanduser() if raw_api_key_file else None
        self.temperature = (
            float(temperature)
            if temperature is not None
            else float(settings.temperature if settings else 0)
        )
        self.max_tokens = int(max_tokens if max_tokens is not None else (
            settings.max_tokens if settings else 512
        ))

    def classify(
        self,
        *,
        title: str,
        body: str | None = None,
        document_type: str = "news",
    ) -> TextEventResult:
        document_type = _normalize_document_type(document_type)
        api_key = self._load_api_key()
        try:
            from litellm import completion
        except ImportError as exc:  # pragma: no cover - depends on optional install.
            raise ImportError("provider=llm requires `pip install -e .[llm]`") from exc

        response = completion(
            model=self.model,
            api_key=api_key,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是A股公告和新闻事件抽取器。只输出严格 JSON，不要解释。"
                        "分数范围：sentiment_score 在 -1 到 1，importance_score 在 0 到 1。"
                    ),
                },
                {"role": "user", "content": _llm_prompt(title, body, document_type)},
            ],
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )
        content = _completion_content(response)
        parsed = _parse_json_object(content)
        return _result_from_mapping(
            parsed,
            provider=self.provider,
            document_type=document_type,
            model=self.model,
            raw_response=content,
        )

    def _load_api_key(self) -> str:
        if self.api_key_file and self.api_key_file.exists():
            api_key = self.api_key_file.read_text(encoding="utf-8").strip()
            if api_key:
                return api_key
        api_key = os.environ.get(self.api_key_env, "").strip()
        if api_key:
            return api_key
        locations = [f"env:{self.api_key_env}"]
        if self.api_key_file:
            locations.insert(0, f"file:{self.api_key_file}")
        raise ValueError(f"provider=llm requires an API key in {' or '.join(locations)}")


def build_text_event_classifier(
    provider: str | None = None,
    *,
    settings: TextEventClassifierSettings | None = None,
) -> TextEventClassifier:
    provider = (provider or (settings.provider if settings else "rule")).strip().lower()
    if provider == "rule":
        return RuleBasedTextEventClassifier()
    if provider == "llm":
        return LiteLlmTextEventClassifier(settings=settings)
    raise ValueError("text event provider must be one of: rule, llm")


def event_matches_any(result: TextEventResult, candidates: set[str]) -> bool:
    return any(event_type in candidates for event_type in result.event_types)


def _document_text(*, title: str, body: str | None) -> str:
    return " ".join(part.strip() for part in (title, body or "") if part and part.strip())


def _normalize_document_type(document_type: str) -> str:
    normalized = document_type.strip().lower()
    if normalized not in VALID_DOCUMENT_TYPES:
        supported = ", ".join(sorted(VALID_DOCUMENT_TYPES))
        raise ValueError(f"unsupported document_type: {document_type}; supported: {supported}")
    return normalized


def _llm_prompt(title: str, body: str | None, document_type: str) -> str:
    allowed = ", ".join(sorted(VALID_EVENT_TYPES))
    payload = {
        "document_type": document_type,
        "title": title,
        "body": body or "",
        "allowed_event_types": allowed,
        "output_schema": {
            "event_types": ["contract"],
            "sentiment_score": 0.6,
            "importance_score": 0.8,
            "matched_keywords": ["中标"],
            "evidence": "短证据句",
        },
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _completion_content(response: Any) -> str:
    try:
        content = response.choices[0].message.content
    except AttributeError:
        content = response["choices"][0]["message"]["content"]
    if not isinstance(content, str):
        raise ValueError("LLM response message content is not text")
    return content


def _parse_json_object(content: str) -> dict[str, Any]:
    stripped = content.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", stripped, re.DOTALL | re.IGNORECASE)
    if fenced:
        stripped = fenced.group(1).strip()
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise ValueError("LLM response is not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("LLM response JSON must be an object")
    return parsed


def _result_from_mapping(
    payload: dict[str, Any],
    *,
    provider: str,
    document_type: str,
    model: str | None,
    raw_response: str | None,
) -> TextEventResult:
    raw_event_types = payload.get("event_types") or []
    if isinstance(raw_event_types, str):
        raw_event_types = [raw_event_types]
    event_types = tuple(
        dict.fromkeys(
            event_type
            for event_type in (str(item).strip() for item in raw_event_types)
            if event_type in VALID_EVENT_TYPES
        )
    )
    raw_keywords = payload.get("matched_keywords") or []
    if isinstance(raw_keywords, str):
        raw_keywords = [raw_keywords]
    return TextEventResult(
        provider=provider,
        document_type=document_type,
        event_types=event_types,
        sentiment_score=_clamp(float(payload.get("sentiment_score") or 0.0), -1.0, 1.0),
        importance_score=_clamp(float(payload.get("importance_score") or 0.0), 0.0, 1.0),
        matched_keywords=tuple(str(item).strip() for item in raw_keywords if str(item).strip()),
        model=model,
        evidence=str(payload.get("evidence") or "").strip()[:240] or None,
        raw_response=raw_response,
    )


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))
