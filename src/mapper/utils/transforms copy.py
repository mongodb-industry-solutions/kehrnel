# src/mapper/utils/transforms.py
from __future__ import annotations
import re
from typing import Any, Callable, Dict
from datetime import datetime as _dt

REGISTRY: Dict[str, Callable[..., Any]] = {}

def register(fn=None, *, name: str | None = None):
    """Decorator to register a transform (also doubles as a Jinja filter)."""
    def decorator(func):
        REGISTRY[name or func.__name__] = func
        return func
    return decorator(fn) if fn else decorator

# ───── Dates/Times ─────────────────────────────────────────────────────────

@register
def hl7_to_iso8601(value: str) -> str | None:
    """
    20220527104558+0000  →  2022-05-27T10:45:58+00:00
    """
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

# ───── Casting / numbers / booleans ────────────────────────────────────────

@register(name="to_int")
def _to_int(value) -> int | None:
    try:
        return None if value in ("", None) else int(float(value))
    except (ValueError, TypeError):
        return None

@register(name="to_float")
def _to_float(value) -> float | None:
    try:
        return None if value in ("", None) else float(value)
    except (ValueError, TypeError):
        return None

@register
def to_bool(value) -> bool | None:
    """
    Accepts common truthy/falsey strings/numbers in ES/EN.
    """
    if value is None:
        return None
    s = str(value).strip().lower()
    if s in {"1","true","t","yes","y","si","sí","verdadero"}:
        return True
    if s in {"0","false","f","no","n","falso"}:
        return False
    return None

# ───── Strings ─────────────────────────────────────────────────────────────

@register
def strip(value: Any) -> Any:
    return None if value is None else str(value).strip()

@register
def lower(value: Any) -> Any:
    return None if value is None else str(value).lower()

@register
def upper(value: Any) -> Any:
    return None if value is None else str(value).upper()

@register
def title(value: Any) -> Any:
    return None if value is None else str(value).title()

@register
def normalize_ws(value: Any) -> Any:
    return None if value is None else re.sub(r"\s+", " ", str(value)).strip()

@register
def slugify(value: Any) -> Any:
    if value is None:
        return None
    s = re.sub(r"[^\w.-]+", "_", str(value), flags=re.UNICODE)
    return s.strip("._") or "value"

@register
def empty_to_none(value: Any) -> Any:
    return None if value in ("", [], {}, ()) else value

# ───── Jinja integration ───────────────────────────────────────────────────

def attach_to_jinja(env) -> None:
    """
    Make every transform available as a Jinja filter with the same name.
    """
    for name, fn in REGISTRY.items():
        env.filters[name] = fn