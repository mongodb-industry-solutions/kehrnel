from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Set

from kehrnel.engine.strategies.openehr.rps_dual.config import (
    RPSDualConfig,
    build_coding_opts,
    build_flattener_config,
)
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener


def _normalize_mapping_payload(payload: Any) -> Any:
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except Exception:
            return payload
    return payload


def _shortcut_key_map(shortcuts: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not isinstance(shortcuts, dict):
        return {}
    if any(key in shortcuts for key in ("keys", "items", "values")):
        merged: Dict[str, str] = {}
        for key in ("items", "keys"):
            chunk = shortcuts.get(key)
            if isinstance(chunk, dict):
                merged.update({str(k): str(v) for k, v in chunk.items()})
        return merged
    return {str(k): str(v) for k, v in shortcuts.items()}


def _normalize_copy_expr(expr: Any) -> Optional[str]:
    if not isinstance(expr, str):
        return None
    text = expr.strip()
    if not text:
        return None
    if text == "p":
        return "p"
    text = text.replace("/", ".")
    while ".." in text:
        text = text.replace("..", ".")
    return text.strip(".") or None


def _dedupe_defs(defs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    out: List[Dict[str, Any]] = []
    for entry in defs:
        marker = json.dumps(entry, sort_keys=True)
        if marker in seen:
            continue
        seen.add(marker)
        out.append(entry)
    return out


def _merge_leaf(existing: Any, new_leaf: Dict[str, Any]) -> Any:
    if existing is None:
        return new_leaf
    if isinstance(existing, list):
        return _dedupe_defs([*(entry for entry in existing if isinstance(entry, dict)), new_leaf])
    if isinstance(existing, dict):
        if existing == new_leaf:
            return existing
        return _dedupe_defs([existing, new_leaf])
    return new_leaf


def _assign_field(tree: Dict[str, Any], parts: List[str], leaf: Dict[str, Any]) -> None:
    if not parts:
        return
    head = parts[0]
    if len(parts) == 1:
        tree[head] = _merge_leaf(tree.get(head), leaf)
        return

    node = tree.get(head)
    if not isinstance(node, dict) or node.get("type") != "document" or not isinstance(node.get("fields"), dict):
        node = {"type": "document", "fields": {}}
        tree[head] = node
    _assign_field(node["fields"], parts[1:], leaf)


def _metadata_id_def(strategy_cfg: RPSDualConfig, field_name: str) -> Dict[str, Any]:
    policy = "string"
    if field_name == strategy_cfg.fields.document.ehr_id:
        policy = str(strategy_cfg.ids.ehr_id or "string").strip().lower()
    if policy in {"uuid", "uuidbin", "uuid_bin"}:
        return {"type": "uuid"}
    if policy == "objectid":
        return {"type": "objectId"}
    return {"type": "token"}


def _archetype_id_def(strategy_cfg: RPSDualConfig) -> Dict[str, Any]:
    strategy = str(strategy_cfg.transform.coding.arcodes.strategy or "sequential").strip().lower()
    return {"type": "number"} if strategy == "sequential" else {"type": "token"}


def _apply_shortcuts(path: str, shortcut_keys: Dict[str, str], enabled: bool) -> str:
    if not enabled or not shortcut_keys:
        return path
    return ".".join(shortcut_keys.get(part, part) for part in path.split(".") if part)


def _leaf_defs_for_path(path: str, rm_type: Optional[str], strategy_cfg: RPSDualConfig) -> List[Dict[str, Any]]:
    clean_path = str(path or "").strip(".")
    if not clean_path:
        return []

    parts = clean_path.split(".")
    last = parts[-1]
    parent = parts[-2] if len(parts) > 1 else ""
    rm_upper = str(rm_type or "").strip().upper()

    if clean_path == "archetype_node_id":
        return [_archetype_id_def(strategy_cfg)]

    if last in {"code_string", "id", "units", "system_id"}:
        return [{"type": "token"}]

    if last == "magnitude":
        return [{"type": "number"}]

    if last in {"start_time", "end_time", "origin", "time"}:
        return [{"type": "date"}]

    if last == "value":
        if rm_upper in {"DV_DATE", "DV_DATE_TIME", "DV_TIME"}:
            return [{"type": "date"}]
        if rm_upper in {"DV_QUANTITY", "DV_COUNT", "DV_ORDINAL", "DV_PROPORTION"}:
            return [{"type": "number"}]
        if rm_upper in {"DV_TEXT", "DV_PARAGRAPH"}:
            return [{"type": "string", "analyzer": "lucene.standard"}]
        if rm_upper in {"DV_CODED_TEXT"}:
            return [{"type": "string", "analyzer": "lucene.standard"}]
        if rm_upper in {"DV_IDENTIFIER"}:
            return [{"type": "token"}]
        if parent in {"start_time", "end_time", "origin", "time"}:
            return [{"type": "date"}]
        if parent == "name":
            return [{"type": "string", "analyzer": "lucene.standard"}]
        if parent == "value":
            return [{"type": "token"}, {"type": "date"}]
        return [{"type": "string", "analyzer": "lucene.standard"}]

    if last == "name":
        return [{"type": "string", "analyzer": "lucene.standard"}]

    if rm_upper in {"DV_DATE", "DV_DATE_TIME", "DV_TIME"}:
        return [{"type": "date"}]
    if rm_upper in {"DV_QUANTITY", "DV_COUNT", "DV_ORDINAL", "DV_PROPORTION"}:
        return [{"type": "number"}]
    if rm_upper in {"DV_TEXT", "DV_PARAGRAPH"}:
        return [{"type": "string", "analyzer": "lucene.standard"}]

    return [{"type": "string", "analyzer": "lucene.standard"}]


def _legacy_rules(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rules = payload.get("rules")
    if not isinstance(rules, list):
        return []
    return [rule for rule in rules if isinstance(rule, dict)]


async def build_search_index_definition_from_mappings(
    strategy_cfg: RPSDualConfig,
    mappings_content: Any,
    *,
    db: Any = None,
    shortcuts: Optional[Dict[str, Any]] = None,
    include_stored_source: bool = True,
) -> Dict[str, Any]:
    payload = _normalize_mapping_payload(mappings_content)
    flattener = CompositionFlattener(
        db=db,
        config=build_flattener_config(strategy_cfg),
        mappings_path="unused",
        mappings_content=payload,
        coding_opts=build_coding_opts(strategy_cfg),
    )

    if flattener.catalog_mappings_spec:
        await flattener._load_mappings_from_catalog(flattener.catalog_mappings_spec)
    if flattener.apply_shortcuts and db is not None:
        await flattener._load_shortcuts_from_db()

    shortcut_keys = _shortcut_key_map(shortcuts)
    if shortcut_keys:
        flattener.shortcut_keys.update(shortcut_keys)
        flattener._refresh_codec()

    warnings: List[str] = []
    indexed_data_paths: Set[str] = set()
    template_ids: Set[str] = set()
    sources: Set[str] = set()

    data_fields: Dict[str, Any] = {}
    nodes_field = strategy_cfg.fields.document.sn
    path_field = strategy_cfg.fields.node.p
    data_field = strategy_cfg.fields.node.data

    def register_data_path(data_path: str, *, rm_type: Optional[str], source: str, template_id: Optional[str]) -> None:
        clean_path = str(data_path or "").strip(".")
        if not clean_path:
            warnings.append(f"Skipped empty data path from {source}")
            return
        slim_path = _apply_shortcuts(clean_path, flattener.shortcut_keys, flattener.apply_shortcuts)
        leaf_defs = _leaf_defs_for_path(clean_path, rm_type, strategy_cfg)
        if not leaf_defs:
            warnings.append(f"Could not infer Atlas Search type for {clean_path}")
            return
        indexed_data_paths.add(slim_path)
        if template_id:
            template_ids.add(str(template_id))
        sources.add(source)
        for leaf in leaf_defs:
            _assign_field(data_fields, slim_path.split("."), leaf)

    for template_id, fields in (flattener.template_fields or {}).items():
        template_ids.add(str(template_id))
        for field in fields or []:
            if not isinstance(field, dict):
                continue
            extract = field.get("extract") or field.get("valuePath") or flattener._infer_extract_from_path(field.get("path", ""))
            normalized = _normalize_copy_expr(f"data.{extract}" if extract else "data")
            if normalized == "data":
                warnings.append(f"Skipped broad analytics field without leaf extract for template {template_id}")
                continue
            if normalized and normalized.startswith("data."):
                register_data_path(
                    normalized[5:],
                    rm_type=field.get("rmType"),
                    source="analytics_fields",
                    template_id=str(template_id),
                )

    for rule in _legacy_rules(payload):
        rm_type = rule.get("rmType")
        template_ids_value = rule.get("template_ids")
        if isinstance(template_ids_value, list):
            for item in template_ids_value:
                if isinstance(item, str) and item.strip():
                    template_ids.add(item.strip())
        for expr in rule.get("copy") or []:
            normalized = _normalize_copy_expr(expr)
            if normalized in {None, "p"}:
                continue
            if not normalized.startswith("data."):
                warnings.append(f"Skipped unsupported legacy copy expression: {expr}")
                continue
            register_data_path(
                normalized[5:],
                rm_type=rm_type,
                source="legacy_rules",
                template_id=None,
            )

    root_fields: Dict[str, Any] = {
        strategy_cfg.fields.document.ehr_id: _metadata_id_def(strategy_cfg, strategy_cfg.fields.document.ehr_id),
        strategy_cfg.fields.document.tid: {"type": "token"},
        strategy_cfg.fields.document.sort_time: {"type": "date"},
        nodes_field: {
            "type": "embeddedDocuments",
            "fields": {
                path_field: {
                    "type": "string",
                    "analyzer": "lucene.keyword",
                },
                data_field: {
                    "type": "document",
                    "fields": data_fields,
                },
            },
        },
    }

    definition: Dict[str, Any] = {
        "mappings": {
            "dynamic": False,
            "fields": root_fields,
        }
    }
    if include_stored_source:
        definition["storedSource"] = {"include": [f"{nodes_field}.{path_field}"]}

    return {
        "definition": definition,
        "metadata": {
            "template_count": len(template_ids),
            "template_ids": sorted(template_ids),
            "data_field_count": len(indexed_data_paths),
            "data_paths": sorted(indexed_data_paths),
            "nodes_field": nodes_field,
            "path_field": path_field,
            "data_field": data_field,
            "sort_field": strategy_cfg.fields.document.sort_time,
            "sources": sorted(sources),
            "apply_shortcuts": bool(flattener.apply_shortcuts and flattener.shortcut_keys),
        },
        "warnings": warnings,
    }
