from __future__ import annotations

import copy
from typing import Dict, Optional, Tuple


def remap_fields_for_config(
    base_doc: Optional[dict],
    search_doc: Optional[dict],
    runtime_config: Optional[Dict],
) -> Tuple[Optional[dict], Optional[dict], str]:
    """
    Apply field remapping rules based on runtime_config.

    This is used by the API ingestion endpoints to adapt flattener outputs to the
    field names configured by the active persistence strategy runtime.

    Returns (base_doc, search_doc, path_field).
    """
    if not runtime_config:
        return base_doc, search_doc, "p"

    fields_cfg = runtime_config.get("fields", {}) if isinstance(runtime_config, dict) else {}
    comp_cfg = (
        fields_cfg.get("composition", {})
        or (runtime_config.get("composition_fields", {}) if isinstance(runtime_config, dict) else {})
        or {}
    )
    search_cfg = (
        fields_cfg.get("search", {})
        or (runtime_config.get("search_fields", {}) if isinstance(runtime_config, dict) else {})
        or {}
    )

    base = copy.deepcopy(base_doc) if base_doc else None
    search = copy.deepcopy(search_doc) if search_doc else None

    comp_map = {
        "ehr_id": comp_cfg.get("ehr_id"),
        "comp_id": comp_cfg.get("comp_id"),
        "tid": comp_cfg.get("template_id"),
        "v": comp_cfg.get("version"),
        "cn": comp_cfg.get("nodes"),
    }
    if base:
        for old, new in comp_map.items():
            if new and old in base:
                base[new] = base.pop(old)

        node_map = {
            "p": comp_cfg.get("path"),
            "ap": comp_cfg.get("archetype_path"),
            "anc": comp_cfg.get("ancestors"),
            "data": comp_cfg.get("data"),
        }
        node_key = comp_cfg.get("nodes", "cn")
        if node_key in base and isinstance(base[node_key], list):
            for n in base[node_key]:
                for old, new in node_map.items():
                    if new and old in n:
                        n[new] = n.pop(old)

    path_field = "p"
    if search:
        search_map = {
            "ehr_id": search_cfg.get("ehr_id"),
            "tid": search_cfg.get("template_id"),
            "sn": search_cfg.get("nodes"),
        }
        for old, new in search_map.items():
            if new and old in search:
                search[new] = search.pop(old)

        node_map = {
            "p": search_cfg.get("path"),
            "ap": search_cfg.get("archetype_path"),
            "anc": search_cfg.get("ancestors"),
            "data": search_cfg.get("data"),
        }
        node_key = search_cfg.get("nodes", "sn")
        if node_key in search and isinstance(search[node_key], list):
            for n in search[node_key]:
                for old, new in node_map.items():
                    if new and old in n:
                        n[new] = n.pop(old)

        path_field = search_cfg.get("path") or "p"

    return base, search, path_field

