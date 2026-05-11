"""A-share instrument normalization helpers."""

from __future__ import annotations


def normalize_instrument(value: str) -> str:
    """Normalize common A-share code formats to Qlib-style SH/SZ/BJ instruments."""

    text = str(value).strip().upper().replace("_", ".")
    if not text:
        raise ValueError("empty instrument")
    if text.startswith(("SH", "SZ", "BJ")) and len(text) >= 8:
        return text[:2] + _digits(text[2:])
    if "." in text:
        code, suffix = text.split(".", 1)
        code = _digits(code)
        suffix = suffix.upper()
        if suffix in {"SH", "SSE"}:
            return f"SH{code}"
        if suffix in {"SZ", "SZSE"}:
            return f"SZ{code}"
        if suffix in {"BJ", "BSE"}:
            return f"BJ{code}"
    code = _digits(text)
    if code.startswith("6"):
        return f"SH{code}"
    if code.startswith(("0", "3")):
        return f"SZ{code}"
    if code.startswith(("4", "8", "9")):
        return f"BJ{code}"
    raise ValueError(f"unsupported A-share instrument: {value}")


def instrument_to_symbol(instrument: str) -> str:
    return normalize_instrument(instrument)[2:]


def instrument_exchange(instrument: str) -> str:
    prefix = normalize_instrument(instrument)[:2]
    if prefix == "SH":
        return "SSE"
    if prefix == "SZ":
        return "SZSE"
    if prefix == "BJ":
        return "BSE"
    return "UNKNOWN"


def instrument_to_akshare_daily_symbol(instrument: str) -> str:
    normalized = normalize_instrument(instrument)
    prefix = normalized[:2].lower()
    code = normalized[2:]
    if prefix == "sh":
        return f"sh{code}"
    if prefix == "sz":
        return f"sz{code}"
    if prefix == "bj":
        return f"bj{code}"
    return code


def _digits(value: str) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        raise ValueError(f"missing digits in instrument: {value}")
    return digits[-6:].zfill(6)
