#src/mapper/utils/macro_expander.py
from __future__ import annotations
from collections import defaultdict
from typing import Dict, Any, Tuple

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

DV_CT_SUFFIXES = {
    "code":   "value/defining_code/code_string",
    "term":   "value/value",
    "system": "value/defining_code/terminology_id/value",
}

DV_ORD_SUFFIXES = {
    "ordinal":       "value/value",  # the magnitude (int)
    "symbol_term":   "value/symbol/value",
    "symbol_code":   "value/symbol/defining_code/code_string",
    "symbol_system": "value/symbol/defining_code/terminology_id/value",
}

def expand_macros(mapping: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for json_path, rule in mapping.items():
        if json_path.startswith("_") or not isinstance(rule, dict):
            out[json_path] = rule
            continue

        simple_keys, macro_keys = _split_keys(rule)

        # DV_CODED_TEXT
        if macro_keys & {"code", "term", "system"}:
            for k in {"code", "term", "system"} & macro_keys:
                out[f"{json_path}/{DV_CT_SUFFIXES[k]}"] = rule[k]

        # DV_ORDINAL
        if macro_keys & {"ordinal", "symbol_term", "symbol_code", "symbol_system"}:
            # ensure the _type is set
            out[f"{json_path}/value/_type"] = "constant: DV_ORDINAL"
            for k in {"ordinal", "symbol_term", "symbol_code", "symbol_system"} & macro_keys:
                out[f"{json_path}/{DV_ORD_SUFFIXES[k]}"] = rule[k]

        # default/null_flavour passthrough
        if "default" in rule:
            simple_keys["default"] = rule["default"]
        if "null_flavour" in rule:
            simple_keys["null_flavour"] = rule["null_flavour"]

        if simple_keys:
            out[json_path] = simple_keys

    return out

def _split_keys(rule: Dict[str, Any]) -> Tuple[Dict[str, Any], set[str]]:
    normal, macro = {}, set()
    for k, v in rule.items():
        if k in {"code", "term", "system", "default", "null_flavour",
                 "ordinal", "symbol_term", "symbol_code", "symbol_system"}:
            macro.add(k)
        else:
            normal[k] = v
    return normal, macro