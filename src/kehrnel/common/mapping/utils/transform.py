#src/mapper/utils/transform.py
"""
Small registry of helper functions you can reference from your YAML:

    transform: hl7_to_iso8601
"""
from __future__ import annotations
from typing import Any, Callable, Dict
import re
from datetime import datetime as _dt, timezone

REGISTRY: Dict[str, Callable[..., Any]] = {}

def register(fn=None, *, name: str | None = None):
    def decorator(func):
        REGISTRY[name or func.__name__] = func
        return func
    return decorator(fn) if fn else decorator

@register
def hl7_to_iso8601(value: str) -> str | None:
    """20220527104558+0000 → 2022-05-27T10:45:58+00:00"""
    if not value:
        return None
    v = str(value)
    if len(v) < 5:
        return None
    dt  = _dt.strptime(v[:-5], "%Y%m%d%H%M%S")
    tz  = v[-5:]  # +0000
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + tz[:3] + ":" + tz[3:]

@register
def space_to_T(value: str | None) -> str | None:
    return None if value is None else str(value).replace(" ", "T")

@register(name="to_int")
def _to_int(value) -> int | None:
    try:
        return None if value in ("", None) else int(float(value))
    except (ValueError, TypeError):
        return None

@register(name="int")  # alias
def _int_alias(value):
    return _to_int(value)

@register(name="to_float")
def _to_float(value) -> float | None:
    try:
        return None if value in ("", None) else float(value)
    except (ValueError, TypeError):
        return None

@register(name="float")  # alias
def _float_alias(value):
    return _to_float(value)

@register
def strip(value: Any) -> Any:
    return None if value is None else str(value).strip()

@register
def normalize_ws(value: Any) -> Any:
    return None if value is None else re.sub(r"\s+", " ", str(value)).strip()

@register
def wrap_date(value: str | dict | None) -> dict | None:
    """
    Normalize to Mongo Extended JSON date:
      "2025-02-11T07:44:13.593Z" → {"$date":"2025-02-11T07:44:13.593Z"}
    """
    if value is None:
        return None
    if isinstance(value, dict) and "$date" in value:
        return value
    v = str(value).strip()
    if not v:
        return None
    # If it's just a date-time without tz, assume UTC.
    if v.endswith("+00:00"):
        v = v[:-6] + "Z"
    elif not v.endswith("Z") and "T" in v and ("+" not in v):
        v = v + "Z"
    return {"$date": v}

def attach_to_jinja(env) -> None:
    for name, fn in REGISTRY.items():
        env.filters[name] = fn