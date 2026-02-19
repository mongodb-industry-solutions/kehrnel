# src/mapper/handlers/common.py
from __future__ import annotations
import re, datetime as dt
from typing import Any, Dict, List, Iterable, Optional, Tuple, Callable
from kehrnel.engine.common.mappings.utils.jinja_env import env as _JINJA

class HandlerCommon:
    # ── coercions ───────────────────────────────────────────────────────────
    @staticmethod
    def coerce(val: Any, how: Optional[str], date_fmt: Optional[str]=None) -> Any:
        if val is None: return None
        if how == "int":    return int(val)
        if how == "float":  return float(val)
        if how in {"date_iso","datetime_iso"}:
            if not date_fmt: return str(val)
            return dt.datetime.strptime(str(val), date_fmt).isoformat()
        return val

    # ── ranges: {"0..7": codeA, "8..10": codeB, ...} ────────────────────────
    @staticmethod
    def choose_by_ranges(n: Optional[int], mapping: Dict[str, Any], default: Any=None) -> Any:
        if n is None: return default
        for rng, code in mapping.items():
            m = re.match(r"^\s*(-?\d+)\s*\.\.\s*(-?\d+)\s*$", str(rng))
            if not m: continue
            lo, hi = int(m.group(1)), int(m.group(2))
            if lo <= n <= hi: return code
        return default

    # ── ${var} substitution ─────────────────────────────────────────────────
    @staticmethod
    def sub_vars(s: Any, vmap: Dict[str, Any]) -> Any:
        if not isinstance(s, str): return s
        return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", lambda m: str(vmap.get(m.group(1), "")), s)

    # ── tiny “when” evaluators (CSV DSL and XML/XPath) ──────────────────────
    @staticmethod
    def make_csv_when():
        rx = re.compile(r'^\s*([A-Za-z0-9_áéíóúñÁÉÍÓÚÑ]+)\s*(==|!=|~=|!~=)\s*"(.*)"\s*$')
        def _match_row(r: Dict[str, Any], cond: str) -> bool:
            m = rx.match(cond or "")
            if not m: return True
            col, op, rhs = m.groups(); val = str(r.get(col) or "")
            return (val == rhs) if op=="==" else \
                   (val != rhs) if op=="!=" else \
                   (re.search(rhs, val) is not None) if op=="~=" else \
                   (re.search(rhs, val) is None)
        return _match_row

    @staticmethod
    def render_template(tpl: str, ctx: Dict[str, Any]) -> str:
        return _JINJA.from_string(tpl).render(ctx)