"""Helpers for applying TLS options only when the Mongo URI requires them."""
from __future__ import annotations

from urllib.parse import parse_qs, urlparse


def mongo_tls_enabled(uri: str | None) -> bool:
    raw = (uri or "").strip()
    if not raw:
        return False
    lowered = raw.lower()
    if lowered.startswith("mongodb+srv://"):
        return True
    try:
        query = parse_qs(urlparse(raw).query)
    except Exception:
        return "tls=true" in lowered or "ssl=true" in lowered
    for key, values in query.items():
        if key.lower() not in {"tls", "ssl"}:
            continue
        for value in values:
            if str(value).strip().lower() in {"1", "true", "yes"}:
                return True
    return False


__all__ = ["mongo_tls_enabled"]
