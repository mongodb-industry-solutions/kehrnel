#src/mapper/transform.py
"""
Small registry of helper functions you can reference from your YAML:

    transform: hl7_to_iso8601
"""
from __future__ import annotations
from datetime import datetime as _dt

REGISTRY = {}


def register(fn=None, *, name: str | None = None):
    def decorator(func):
        REGISTRY[name or func.__name__] = func
        return func
    return decorator(fn) if fn else decorator


@register
def hl7_to_iso8601(value: str) -> str | None:
    """
    20220527104558+0000  →  2022-05-27T10:45:58+00:00
    """
    if not value:
        return None
    dt  = _dt.strptime(value[:-5], "%Y%m%d%H%M%S")
    tz  = value[-5:]                      # +0000
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + tz[:3] + ":" + tz[3:]


@register
def space_to_T(value: str | None) -> str | None:
    return None if value is None else value.replace(" ", "T")


@register
def to_int(value) -> int | None:
    try:
        return None if value in ("", None) else int(float(value))
    except (ValueError, TypeError):
        return None