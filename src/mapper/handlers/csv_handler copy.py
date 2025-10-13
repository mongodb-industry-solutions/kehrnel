# src/mapper/handlers/csv_handler.py
from __future__ import annotations
import csv, re, datetime as dt
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Optional

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

    # --- helpers -------------------------------------------------------------

    def _eval_where(self, rows: List[Dict[str, Any]], where: List[str]) -> List[Dict[str, Any]]:
        if not where:
            return rows

        def match_row(r: Dict[str, Any], cond: str) -> bool:
            # allow ==, !=, ~=, !~=
            m = re.match(
                r'^\s*([A-Za-z0-9_áéíóúñÁÉÍÓÚÑ]+)\s*(==|!=|~=|!~=)\s*"(.*)"\s*$',
                cond
            )
            if not m:
                return True  # keep current fail-open behavior
            col, op, rhs = m.groups()
            val = str(r.get(col) or "")

            if op == "==":
                return val == rhs
            elif op == "!=":
                return val != rhs
            elif op == "~=":
                return re.search(rhs, val) is not None
            elif op == "!~=":
                return re.search(rhs, val) is None

        return [r for r in rows if all(match_row(r, c) for c in where)]

    def _group_rows(self, rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Dict[str, Any]]]:
        if not keys:
            return [rows]
        buckets: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
        for r in rows:
            k = tuple(r.get(k, "") for k in keys)
            buckets.setdefault(k, []).append(r)
        return list(buckets.values())

    def _parse_date(self, txt: str, fmt: Optional[str]) -> str:
        if not txt:
            return txt
        if not fmt:
            return txt
        return dt.datetime.strptime(txt, fmt).isoformat()

    # --- protocol methods ----------------------------------------------------

    def preprocess_mapping(
        self,
        mapping: Dict,
        src: List[Dict[str, Any]]
    ) -> List[Tuple[Any, Dict]]:
        """
        Turn the compose/header/content grammar into a flat mapping:
            expanded_map: Dict[str, Dict]  # jpath -> {"literal": <scalar|DV_*>}
        One expanded_map per grouped visit (group_by).
        """

        # ── read top-level knobs ─────────────────────────────────────────────
        inp     = mapping.get("input", {}) or {}
        where   = (mapping.get("select", {}) or {}).get("where", []) or []
        groupby = mapping.get("group_by", []) or []
        compose = mapping.get("compose", {}) or {}
        datefmt = inp.get("date_format")

        # ── helper: evaluate WHERE/WHEN (supports ==, !=, ~=, !~=) ──────────
        def _match_row(r: Dict[str, Any], cond: str) -> bool:
            m = re.match(
                r'^\s*([A-Za-z0-9_áéíóúñÁÉÍÓÚÑ]+)\s*(==|!=|~=|!~=)\s*"(.*)"\s*$',
                cond
            )
            if not m:
                return True  # fail-open like original behavior
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
        
        def _sub_vars(s: Any, vmap: Dict[str, Any]) -> Any:
            """Interpolate ${var} in strings."""
            if not isinstance(s, str):
                return s
            return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", lambda m: str(vmap.get(m.group(1), "")), s)

        # ── apply select.where; then group ───────────────────────────────────
        rows   = _filter_rows(src, where)
        groups = self._group_rows(rows, groupby)

        out: List[Tuple[Any, Dict]] = []

        for grp in groups:
            ctx = grp[0] if grp else {}
            expanded: Dict[str, Any] = {}

            # ── evaluate _vars once per group ───────────────────────────────
            vars_cfg = (mapping.get("_vars") or {})
            vars_vals: Dict[str, Any] = {}

            def _coerce_val(val: str, how: Optional[str]) -> Any:
                if how == "int":
                    return int(val)
                if how == "float":
                    return float(val)
                if how in {"date_iso","datetime_iso"}:
                    return self._parse_date(str(val), datefmt)
                return val

            def _first_last_from(which: str, cfg: Dict[str, Any]) -> Any:
                cond = cfg.get("when")
                candidates = _filter_rows(grp, [cond] if isinstance(cond, str) else (cond or []))
                if not candidates:
                    return None
                row = candidates[-1] if which == "last" else candidates[0]
                col = cfg.get(f"{which}_from") or cfg.get("from")
                raw = (row.get(col) or "").strip()
                if raw == "":
                    return None
                return _coerce_val(raw, cfg.get("coerce"))

            # compute simple vars (first_from/last_from)
            for vname, vcfg in vars_cfg.items():
                if isinstance(vcfg, dict) and ("first_from" in vcfg or "last_from" in vcfg or "from" in vcfg):
                    which = "last" if "last_from" in vcfg else ("first" if "first_from" in vcfg else "first")
                    vars_vals[vname] = _first_last_from(which, vcfg)

            # compute range-based vars
            for vname, vcfg in vars_cfg.items():
                r = vcfg.get("ranges")
                if not r:
                    continue
                # resolve the 'of' value (may reference ${var})
                of_expr = _sub_vars(r.get("of"), vars_vals)
                try:
                    n = int(of_expr) if not isinstance(of_expr, int) else of_expr
                except Exception:
                    # keep as is (let generator handle missing)
                    n = None
                chosen = None
                if isinstance(n, int):
                    for rng, code in (r.get("map") or {}).items():
                        m = re.match(r"^\s*(-?\d+)\s*\.\.\s*(-?\d+)\s*$", str(rng))
                        if not m:
                            continue
                        lo, hi = int(m.group(1)), int(m.group(2))
                        if lo <= n <= hi:
                            chosen = str(code).strip()
                            break
                vars_vals[vname] = chosen or r.get("default")

            # ── header → COMPOSITION-level fields ───────────────────────────
            header = compose.get("header", {}) or {}
            if header:
                # context.start_time → ISO string (generator wraps to DV_DATE_TIME)
                if "context.start_time" in header:
                    dtxt = header["context.start_time"]
                    if isinstance(dtxt, str) and dtxt.startswith("${") and dtxt.endswith("}"):
                        col = dtxt[2:-1]
                        dtxt = ctx.get(col, "")
                    lit = self._parse_date(str(dtxt), datefmt) if dtxt else ""
                    if lit:
                        expanded["context/start_time/value"] = {"literal": lit}

                # language / territory at COMPOSITION level
                if "language" in header:
                    expanded["language/code_string"] = {"literal": str(header["language"]).strip()}
                if "territory" in header:
                    expanded["territory/code_string"] = {"literal": str(header["territory"]).strip()}

                # category (map friendly strings to openehr codes)
                if "category" in header:
                    cat = str(header["category"]).strip().lower()
                    code = {"event": "433", "persistent": "431"}.get(cat, cat)
                    expanded["category/defining_code/code_string"] = {"literal": code}

            # ── content blocks (observation/evaluation/action/admin_entry) ──
            contents = compose.get("content", []) or []
            for block in contents:
                block_code = (block.get("observation") or block.get("evaluation")
                            or block.get("action") or block.get("admin_entry"))
                prefix = f"content[{block_code}]/" if block_code else ""

                for rule in (block.get("map") or []):
                    path   = rule.get("path")
                    when   = rule.get("when")
                    rtype  = rule.get("type")
                    frm    = rule.get("from")
                    coerce = rule.get("coerce")
                    choices= rule.get("choices") or {}
                    overwrite = rule.get("overwrite")
                    list_mode = rule.get("list_mode")  # new: first|last|only_one
                    null_if_empty = rule.get("null_if_empty")
                    from_var = rule.get("from_var")

                    if not path:
                        continue
                    full_path = f"{prefix}{path}" if prefix else path

                    # Determine candidate rows per rule (WHEN can be str or list)
                    candidates = grp
                    if when:
                        conds = when if isinstance(when, list) else [c.strip() for c in str(when).split(" and ")]
                        candidates = _filter_rows(grp, conds)
                        if not candidates:
                            continue

                    # choose row per list_mode / overwrite
                    if list_mode == "only_one" and len(candidates) != 1:
                        raise ValueError(f"Expected exactly one match for {full_path}, got {len(candidates)}")
                    if list_mode == "last" or overwrite == "last":
                        r0 = candidates[-1]
                    else:
                        r0 = candidates[0]

                    # variable-sourced rule
                    if from_var:
                        val = vars_vals.get(str(from_var), None)
                        if val is not None:
                            expanded[full_path] = {"literal": val}
                        continue

                    # literal-only rule (optionally gated by WHEN)
                    if "literal" in rule and rtype is None and frm is None:
                        lit = rule["literal"]
                        # allow ${var} interpolation inside literals
                        lit = _sub_vars(lit, vars_vals)
                        expanded[full_path] = {"literal": lit}
                        continue

                    # typed rules pulling from a source column
                    src_val = r0.get(frm) if isinstance(frm, str) else None
                    txt = (src_val or "").strip()

                    if null_if_empty and txt == "":
                        continue

                    if "code" in rule and rtype is None and frm is None:
                        expanded[full_path] = {"literal": str(rule["code"]).strip()}
                        continue

                    if "ranges" in rule and isinstance(frm, str):
                        if txt == "":
                            continue
                        try:
                            n = int(txt)
                        except ValueError:
                            continue  # not numeric; skip
                        chosen = None
                        for rng, code in (rule["ranges"] or {}).items():
                            m = re.match(r"^\s*(-?\d+)\s*\.\.\s*(-?\d+)\s*$", str(rng))
                            if not m:
                                continue
                            lo, hi = int(m.group(1)), int(m.group(2))
                            if lo <= n <= hi:
                                chosen = str(code).strip()
                                break
                        if chosen:
                            # only the code; generator will expand the DV_ORDINAL
                            expanded[full_path] = {"literal": chosen}
                            continue

                    if rtype == "ordinal":
                        if txt not in choices:
                            raise ValueError(
                                f"Response not in vocabulary for {full_path}: {txt}"
                            )
                        ch = choices[txt]
                        val = {
                            "_type": "DV_ORDINAL",
                            "value": ch.get("ordinal"),
                            "symbol": {
                                "_type": "DV_CODED_TEXT",
                                "value": txt,
                                "defining_code": {
                                    "_type": "CODE_PHRASE",
                                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "local"},
                                    "code_string": ch.get("code"),
                                },
                            },
                        }
                        expanded[full_path] = {"literal": val}
                        continue

                    if rtype == "count":
                        if txt == "":
                            continue
                        v = txt
                        if coerce == "int":
                            try:
                                v = int(txt)
                            except Exception:
                                raise ValueError(f"Cannot coerce to integer at {full_path}: {txt}")
                        expanded[full_path] = {"literal": {"_type": "DV_COUNT", "magnitude": v}}
                        continue

                    # generic scalar (dates/numbers/text)
                    if frm and txt != "":
                        if coerce in {"date_iso", "datetime_iso"}:
                            lit = self._parse_date(txt, datefmt)
                            expanded[full_path] = {"literal": lit}
                        else:
                            expanded[full_path] = {"literal": _sub_vars(txt, vars_vals)}
                        continue

            out.append((grp, expanded))

        return out

    def extract_value(self, src: Any, rule: Dict | str | Any) -> Any:
        # mapping_engine expects this; we only return the literal if present
        if isinstance(rule, dict) and "literal" in rule:
            return rule["literal"]
        return rule