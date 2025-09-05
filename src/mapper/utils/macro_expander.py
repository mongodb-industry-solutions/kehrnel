#src/mapper/utils/macro_expander.py
from __future__ import annotations
"""
Inject high-level shortcuts into the low-level JSON paths that
`mapping_engine.apply_mapping()` already understands.

Supported macros
----------------
 • code:     DV_CODED_TEXT → .../value/defining_code/code_string
 • term:     DV_CODED_TEXT → .../value/value
 • system:   DV_CODED_TEXT → .../value/defining_code/terminology_id/value
 • default:  value used if rule resolves to None / ""
 • null_flavour:  openehr coded null-flavour (e.g. 'unknown')
"""
from collections import defaultdict
from typing import Dict, Any, Tuple

DV_CT_SUFFIXES = {
    "code":   "value/defining_code/code_string",
    "term":   "value/value",
    "system": "value/defining_code/terminology_id/value",
}

def expand_macros(mapping: Dict[str, Any]) -> Dict[str, Any]:
    """Return a **new** mapping dict with macros expanded."""
    out: Dict[str, Any] = {}
    for json_path, rule in mapping.items():

        # keep untouched: meta keys & non-macro rules
        if json_path.startswith("_") or not isinstance(rule, dict):
            out[json_path] = rule
            continue

        # split macro keys (code/term/system/default/null_flavour) from others
        simple_keys, macro_keys = _split_keys(rule)

        # ── DV_CODED_TEXT helper ───────────────────────────────
        if macro_keys & {"code", "term", "system"}:
            for k in {"code", "term", "system"} & macro_keys:
                suffix = DV_CT_SUFFIXES[k]
                out[f"{json_path}/{suffix}"] = rule[k]

        # ── default / null_flavour  (handled at runtime) ───────
        if "default" in rule:
            simple_keys["default"] = rule["default"]
        if "null_flavour" in rule:
            simple_keys["null_flavour"] = rule["null_flavour"]

        # copy “normal” rule back (without the macro keys)
        if simple_keys:
            out[json_path] = simple_keys

    return out


def _split_keys(rule: Dict[str, Any]) -> Tuple[Dict[str, Any], set[str]]:
    """return normal_fields, macro_keys"""
    normal, macro = {}, set()
    for k, v in rule.items():
        if k in {"code", "term", "system", "default", "null_flavour"}:
            macro.add(k)
        else:
            normal[k] = v
    return normal, macro