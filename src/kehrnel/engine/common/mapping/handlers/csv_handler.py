# src/mapper/handlers/csv_handler.py
from __future__ import annotations
import csv, re, datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional
from kehrnel.engine.common.mapping.utils.jinja_env import env as JINJA
from kehrnel.engine.common.mapping.utils.transform import REGISTRY as TREG
from kehrnel.engine.common.mapping.utils.expr import evaluate as eval_expr

class CSVHandler:
    def can_handle(self, path: Path) -> bool:
        return path.suffix.lower() == ".csv"

    def load_source(self, path: Path) -> List[Dict[str, Any]]:
        for enc in ("utf-8-sig","utf-8","cp1252"):
            try:
                with path.open("r", encoding=enc, newline="") as f:
                    rdr = csv.DictReader(f)
                    rows = [dict(r) for r in rdr]
                if rows:
                    return rows
            except Exception:
                pass
        with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            return [dict(r) for r in csv.DictReader(f)]

    def preprocess_mapping_new(self, mapping: Dict, src_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        meta    = mapping.get("meta") or {}
        inp     = mapping.get("input", {}) or {}
        out     = mapping.get("output", {}) or {}
        rules   = mapping.get("mappings") or {}

        # date format discovery (per default source or compatibility input key)
        default_src = meta.get("default_source","csv1")
        datefmt = (mapping.get("sources") or {}).get(default_src,{}).get("date_format") \
                  or inp.get("date_format")

        # select & group
        where   = inp.get("where", []) or []
        groupby = inp.get("group_by", []) or []
        rows    = [r for r in src_rows if all(eval_expr(c, row=r) for c in where)]
        groups  = self._group_by(rows, groupby)

        # normalize path-keyed rules to a list
        norm_rules: List[Dict[str, Any]] = []
        for path_key, rule in (rules or {}).items():
            r = dict(rule or {}); r["path"] = str(path_key)
            norm_rules.append(r)

        result: List[Dict[str, Any]] = []

        for grp in groups:
            flat: Dict[str, Dict[str, Any]] = {}
            first = grp[0] if grp else {}
            vars_ctx: Dict[str, Any] = {}

            def pick_rows(when):
                if not when:
                    return grp
                conds = when if isinstance(when, list) else [str(when)]
                return [r for r in grp if all(eval_expr(c, row=r, vars=vars_ctx) for c in conds)]

            def _apply_transforms(val: Any, trans: List[str]) -> Any:
                outv = val
                for t in (trans or []):
                    name = str(t).strip()
                    if name in ("date_iso","datetime_iso"):
                        if outv in (None,""):
                            continue
                        outv = dt.datetime.strptime(str(outv), datefmt).isoformat() if datefmt else str(outv)
                    elif name in ("int","to_int"):
                        try:
                            outv = None if outv in ("",None) else int(float(outv))
                        except:
                            pass
                    elif name in ("float","to_float"):
                        try:
                            outv = None if outv in ("",None) else float(outv)
                        except:
                            pass
                    elif name == "strip":
                        outv = None if outv is None else str(outv).strip()
                    else:
                        fn = TREG.get(name); outv = fn(outv) if fn else outv
                return outv

            def _map(val, mapping: Dict[str,Any]):
                if mapping is None:
                    return val
                for k,v in mapping.items():
                    if re.fullmatch(k, str(val)) or re.search(k, str(val)):
                        return v
                return mapping.get(str(val), val)

            def _map_ranges(val, mapping: Dict[str,Any]):
                if mapping is None:
                    return val
                try:
                    x = float(val)
                except:
                    return val
                for rng, code in mapping.items():
                    m = re.match(r"^\s*(-?\d+(?:\.\d+)?)\s*\.\.\s*(-?\d+(?:\.\d+)?)\s*$", str(rng))
                    if not m:
                        continue
                    lo, hi = float(m.group(1)), float(m.group(2))
                    if lo <= x <= hi:
                        return code
                return val

            # rules → flat path map
            for r in norm_rules:
                path  = r["path"].lstrip("/")
                when  = r.get("when")
                get   = r.get("get")
                setv  = r.get("set")
                expr  = r.get("expr")
                trans = r.get("transform") or []
                vmap  = r.get("map")
                vrng  = r.get("map_ranges")
                nempty= r.get("null_if_empty")
                xlate = r.get("translate")
                overwrite_last = r.get("overwrite") == "last"

                cand = pick_rows(when)
                if not cand:
                    # no candidates in this group → skip this rule safely
                    continue
                row = cand[-1] if overwrite_last else cand[0]

                if setv is not None:
                    val = setv
                elif expr is not None:
                    val = JINJA.from_string(str(expr)).render({"rows": grp, "first": first, "vars": vars_ctx})
                elif isinstance(get, dict):
                    col = get.get("column") or get.get("from")
                    val = (row.get(col) or "")
                else:
                    continue

                val = _apply_transforms(val, trans)
                if vmap: val = _map(val, vmap)
                if vrng: val = _map_ranges(val, vrng)
                if nempty and (val is None or str(val).strip() == ""):
                    continue

                flat[path] = {"literal": val} if xlate is None else {"literal": val, "translate": xlate}

            # DO NOT evaluate envelope here (it can reference {{ comp }})
            # Filename can be computed here (no comp dependency)
            fname = None
            if out.get("filename"):
                fname = JINJA.from_string(str(out["filename"])).render({
                    "index": 1, "first": first, "rows": grp
                })

            result.append({
                "rows": grp,
                "map": flat,
                "filename": fname,
                "prune_empty": bool(out.get("prune_empty", False))
            })

        return result

    def _group_by(self, rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Dict[str, Any]]]:
        if not keys:
            return [rows]
        buckets: Dict[tuple, List[Dict[str, Any]]] = {}
        for r in rows:
            k = tuple(r.get(k,"") for k in keys)
            buckets.setdefault(k, []).append(r)
        return list(buckets.values())