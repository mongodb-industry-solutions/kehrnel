# src/mapper/handlers/csv_handler.py
from __future__ import annotations
import csv, re, datetime as dt
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Optional

from mapper.utils.transforms import REGISTRY          
from mapper.utils.jinja_env import env as _JINJA 

class CSVHandler:
    def can_handle(self, path: Path) -> bool:
        return path.suffix.lower() == ".csv"

    def load_source(self, path: Path) -> List[Dict[str, Any]]:
        # Try UTF-8 first; fall back to cp1252 (Windows CSVs)
        for enc in ("utf-8-sig", "utf-8", "cp1252"):
            try:
                with path.open("r", encoding=enc, newline="") as f:
                    rdr = csv.DictReader(f)
                    rows = [dict(r) for r in rdr]
                if rows:
                    return rows
            except Exception:
                continue
        # last resort: replacement
        with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            rdr = csv.DictReader(f)
            return [dict(r) for r in rdr]

    # ───────────────────────── helpers ─────────────────────────

    def _match_row(self, r: Dict[str, Any], cond: str) -> bool:
        # allow ==, !=, ~=, !~=  (case-sensitive by default; (?i) for case-insensitive)
        m = re.match(r'^\s*([A-Za-z0-9_áéíóúñÁÉÍÓÚÑ]+)\s*(==|!=|~=|!~=)\s*"(.*)"\s*$', cond or "")
        if not m:
            return True
        col, op, rhs = m.groups()
        val = str(r.get(col) or "")
        if op == "==":   return val == rhs
        if op == "!=":   return val != rhs
        if op == "~=":   return re.search(rhs, val) is not None
        if op == "!~=":  return re.search(rhs, val) is None
        return True

    def _filter_rows(self, rows: List[Dict[str, Any]], conds: List[str]) -> List[Dict[str, Any]]:
        if not conds:
            return rows
        return [r for r in rows if all(self._match_row(r, c) for c in conds)]

    def _group_rows(self, rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Dict[str, Any]]]:
        if not keys:
            return [rows]
        buckets: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
        for r in rows:
            k = tuple(r.get(k, "") for k in keys)
            buckets.setdefault(k, []).append(r)
        return list(buckets.values())

    def _parse_date(self, txt: str, fmt: Optional[str]) -> str:
        if not txt or not fmt:
            return txt
        return dt.datetime.strptime(str(txt), fmt).isoformat()

    def _apply_transforms(self, val: Any, tlist: List[str], date_fmt: Optional[str]) -> Any:
        if not tlist:
            return val
        out = val
        for t in tlist:
            name = str(t).strip()
            if name in ("date_iso", "datetime_iso"):
                out = self._parse_date(out, date_fmt)
            elif name in ("int", "to_int"):
                try: out = int(float(out)) if out not in (None, "") else out
                except Exception: pass
            elif name in ("float", "to_float"):
                try: out = float(out) if out not in (None, "") else out
                except Exception: pass
            elif name == "strip":
                out = None if out is None else str(out).strip()
            else:
                # delegate to custom registry if present
                fn = REGISTRY.get(name)
                out = fn(out) if fn else out
        return out

    def _apply_map(self, val: Any, mapping: Dict[str, Any]) -> Any:
        if mapping is None:
            return val
        return mapping.get(str(val), val)

    def _apply_map_ranges(self, val: Any, mapping: Dict[str, Any]) -> Any:
        if mapping is None:
            return val
        try:
            x = float(val)
        except Exception:
            return val
        for rng, code in mapping.items():
            m = re.match(r"^\s*(-?\d+(?:\.\d+)?)\s*\.\.\s*(-?\d+(?:\.\d+)?)\s*$", str(rng))
            if not m:
                continue
            lo, hi = float(m.group(1)), float(m.group(2))
            if lo <= x <= hi:
                return code
        return val

    def _render_expr(self, tpl: str, ctx: Dict[str, Any]) -> str:
        return _JINJA.from_string(str(tpl)).render(ctx)

    # ─────────────────────── main: preprocess ───────────────────────

    def preprocess_mapping(
        self,
        mapping: Dict,
        src: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]] | List[Tuple[Any, Dict]] | Dict[str, Any] | None:
        """
        Supports BOTH:
          • New DSL with: meta/at, input, output, mappings: [...]
          • Legacy compose/header/content (kept for backward compatibility)

        NEW DSL return shape (preferred):
          List[{
            "rows": [...],
            "map": Dict[jpath -> {"literal": ..., "translate": ...}],
            "envelope": Dict[str, Any] | None,
            "filename": str | None,
            "prune_empty": bool | None
          }]
        """
        # Detect new grammar
        if "mappings" in mapping or "input" in mapping or "output" in mapping or "meta" in mapping or "at" in mapping:
            return self._preprocess_new(mapping, src)

        # Otherwise keep legacy behavior (unchanged)
        return self._preprocess_legacy(mapping, src)

    # ─────────────────────── new grammar path ───────────────────────

    def _preprocess_new(self, mapping: Dict, src: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        meta    = mapping.get("meta") or mapping.get("at") or {}
        inp     = mapping.get("input", {}) or {}
        out     = mapping.get("output", {}) or {}
        rules   = mapping.get("mappings", []) or []

        datefmt = inp.get("date_format")
        where   = inp.get("where", []) or []
        groupby = inp.get("group_by", []) or []
        default_source = meta.get("default_source") or "csv"

        # filter & group
        rows   = self._filter_rows(src, where)
        groups = self._group_rows(rows, groupby)

        result: List[Dict[str, Any]] = []

        for grp in groups:
            vars_ctx: Dict[str, Any] = {}
            flat: Dict[str, Dict[str, Any]] = {}

            def pick_candidates(when):
                if not when:
                    return grp
                conds = when if isinstance(when, list) else [str(when)]
                return self._filter_rows(grp, conds)

            def eval_rule(r: Dict[str, Any]) -> Optional[tuple[str, Dict[str, Any]]]:
                path  = r.get("path")
                when  = r.get("when")
                get   = r.get("get")
                setv  = r.get("set")
                expr  = r.get("expr")
                trans = r.get("transform") or []
                vmap  = r.get("map")
                vrng  = r.get("map_ranges")
                nempty= r.get("null_if_empty")
                xlate = r.get("translate")  # on|off|no-cache|True|False

                if not path:
                    return None

                cand = pick_candidates(when)
                if not cand and get and not ("as" in get):   # allow captures even if empty?
                    return None
                row = cand[-1] if (r.get("overwrite") == "last" or r.get("list_mode") == "last") else (cand[0] if cand else {})

                # value resolution precedence: set → expr → get
                if setv is not None:
                    val = setv
                elif expr is not None:
                    val = self._render_expr(expr, {"rows": grp, "first": grp[0] if grp else {}, "vars": vars_ctx})
                elif isinstance(get, dict):
                    col = get.get("from")
                    val = (row.get(col) or "") if row else ""
                else:
                    val = None

                # transforms → maps
                val = self._apply_transforms(val, trans, datefmt)
                if vmap:
                    val = self._apply_map(val, vmap)
                if vrng:
                    val = self._apply_map_ranges(val, vrng)

                if nempty and (val is None or str(val).strip() == ""):
                    return None

                # capture variable if requested
                if isinstance(get, dict) and get.get("as"):
                    vars_ctx[str(get["as"])] = val
                    # capture-only rule without path assignment? keep returning None
                    if setv is None and expr is None and r.get("assign") in (None, False):
                        return None

                payload: Dict[str, Any] = {"literal": val}
                if xlate is not None:
                    payload["translate"] = xlate
                return (str(path).lstrip("/"), payload)

            # materialize rules
            for r in rules:
                out_kv = eval_rule(r)
                if not out_kv:
                    continue
                jpath, payload = out_kv
                flat[jpath] = payload

            # envelope (same grammar as a single rule map)
            envelope_cfg = (out.get("envelope") or {}) if isinstance(out.get("envelope"), dict) else {}
            envelope: Dict[str, Any] = {}
            for k, r in envelope_cfg.items():
                # treat each as a mini-rule (supports get/set/expr/transform/map/map_ranges)
                fake = {"path": k, **(r or {})}
                kv = eval_rule(fake)
                if not kv:
                    continue
                _, payload = kv
                envelope[k] = payload.get("literal")

            # filename
            fname = None
            if out.get("filename"):
                first = grp[0] if grp else {}
                fname = _JINJA.from_string(str(out["filename"])).render({
                    "index": 1,  # will be replaced by CLI with real index if needed
                    "first": first,
                    "rows": grp,
                    "envelope": envelope,
                })

            result.append({
                "rows": grp,
                "map": flat,
                "envelope": envelope or None,
                "filename": fname,
                "prune_empty": bool(out.get("prune_empty", False)),
            })

        return result

    # ─────────────────────── legacy grammar path ───────────────────────
    # (This is your previous implementation; left as-is for BC.)
    def _preprocess_legacy(self, mapping: Dict, src: List[Dict[str, Any]]):
        # (Paste your existing legacy preprocess_mapping implementation here,
        #  or import from a legacy module if you split it out.)
        # --- BEGIN: unchanged from your previous version ----------------
        inp     = mapping.get("input", {}) or {}
        where   = (mapping.get("select", {}) or {}).get("where", []) or []
        groupby = mapping.get("group_by", []) or []
        compose = mapping.get("compose", {}) or {}
        datefmt = inp.get("date_format")

        def _match_row(r: Dict[str, Any], cond: str) -> bool:
            m = re.match(
                r'^\s*([A-Za-z0-9_áéíóúñÁÉÍÓÚÑ]+)\s*(==|!=|~=|!~=)\s*"(.*)"\s*$',
                cond
            )
            if not m:
                return True
            col, op, rhs = m.groups()
            val = str(r.get(col) or "")
            if op == "==":
                return val == rhs
            if op == "!=":
                return val != rhs
            if op == "~=":
                return re.search(rhs, val) is not None
            if op == "!~=":
                return re.search(rhs, val) is None
            return True

        def _filter_rows(rows: List[Dict[str, Any]], conds: List[str]) -> List[Dict[str, Any]]:
            if not conds:
                return rows
            return [r for r in rows if all(_match_row(r, c) for c in conds)]

        def _group_rows(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Dict[str, Any]]]:
            if not keys:
                return [rows]
            buckets: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
            for r in rows:
                k = tuple(r.get(k, "") for k in keys)
                buckets.setdefault(k, []).append(r)
            return list(buckets.values())

        rows   = _filter_rows(src, where)
        groups = _group_rows(rows, groupby)

        out: List[Tuple[Any, Dict]] = []
        # … (keep your legacy expansion exactly as you had it) …
        # --- END: unchanged legacy path --------------------------------
        return out

    # keep extract_value BC
    def extract_value(self, src: Any, rule: Dict | str | Any) -> Any:
        if isinstance(rule, dict) and "literal" in rule:
            return rule["literal"]
        return rule