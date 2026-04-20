from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List


def safe_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def token_set(*values: Any) -> List[str]:
    tokens: List[str] = []
    for value in values:
        for item in safe_list(value):
            text = f"{item or ''}".strip()
            if not text:
                continue
            for token in re.split(r"[^a-zA-Z0-9]+", text.lower()):
                if token:
                    tokens.append(token)
    seen = set()
    ordered: List[str] = []
    for token in tokens:
        if token not in seen:
            seen.add(token)
            ordered.append(token)
    return ordered


def _uniq(values: Iterable[Any]) -> List[Any]:
    seen = set()
    ordered: List[Any] = []
    for value in values:
        key = repr(value)
        if key not in seen:
            seen.add(key)
            ordered.append(value)
    return ordered


def normalize_context_block(block: Any) -> Dict[str, Any]:
    if isinstance(block, str):
        return {
            "id": block,
            "title": block,
            "aliases": [],
            "tokens": token_set(block),
        }
    data = dict(block or {})
    block_id = data.get("id") or data.get("blockId") or data.get("name") or data.get("title") or "unknown_block"
    title = data.get("title") or data.get("name") or block_id
    aliases = _uniq(
        [
            *safe_list(data.get("aliases")),
            data.get("role"),
            data.get("attribute"),
            data.get("path"),
            data.get("source"),
            data.get("target"),
        ]
    )
    return {
        "id": block_id,
        "title": title,
        "aliases": [f"{item}".strip() for item in aliases if f"{item}".strip()],
        "tokens": token_set(block_id, title, aliases),
    }


def normalize_terminology_binding(binding: Any) -> Dict[str, Any]:
    if isinstance(binding, str):
        return {
            "system": "local",
            "display": binding,
            "code": None,
            "synonyms": [],
            "tokens": token_set(binding),
        }
    data = dict(binding or {})
    display = data.get("display") or data.get("name") or data.get("label") or data.get("code") or "binding"
    synonyms = _uniq([*safe_list(data.get("synonyms")), *safe_list(data.get("aliases"))])
    return {
        "system": data.get("system") or data.get("terminology") or "local",
        "display": display,
        "code": data.get("code") or data.get("value"),
        "synonyms": [f"{item}".strip() for item in synonyms if f"{item}".strip()],
        "tokens": token_set(display, data.get("code"), synonyms),
    }


def normalize_context_definition(definition: Dict[str, Any] | None) -> Dict[str, Any]:
    raw = dict(definition or {})
    definition_id = raw.get("id") or raw.get("_id") or raw.get("sourceId") or raw.get("name") or "unknown_definition"
    title = raw.get("title") or raw.get("name") or definition_id
    subject_kinds = [
        f"{item}".strip().lower()
        for item in safe_list(raw.get("subject_kinds") or raw.get("subjectKinds"))
        if f"{item}".strip()
    ]
    assertion_types = [
        f"{item}".strip().lower()
        for item in safe_list(raw.get("assertion_types") or raw.get("assertionTypes"))
        if f"{item}".strip()
    ]
    blocks = [normalize_context_block(block) for block in safe_list(raw.get("blocks"))]
    terminology = [normalize_terminology_binding(item) for item in safe_list(raw.get("terminology"))]
    tags = [f"{item}".strip().lower() for item in safe_list(raw.get("tags")) if f"{item}".strip()]
    output_families = [
        f"{item}".strip().lower()
        for item in safe_list(raw.get("output_families") or raw.get("outputFamilies"))
        if f"{item}".strip()
    ]
    summary = raw.get("summary") or raw.get("description") or ""
    resolution = raw.get("resolution") or {}
    retrieval = raw.get("retrieval") or {}
    relations = safe_list(raw.get("relations"))
    return {
        "id": definition_id,
        "title": title,
        "summary": summary,
        "status": raw.get("status") or "active",
        "subject_kinds": subject_kinds,
        "assertion_types": assertion_types,
        "blocks": blocks,
        "terminology": terminology,
        "tags": tags,
        "output_families": output_families,
        "resolution": resolution,
        "retrieval": retrieval,
        "relations": relations,
        "tokens": token_set(title, summary, tags, output_families),
        "raw": raw,
    }


def normalize_context_map(context_map: Dict[str, Any] | None) -> Dict[str, Any]:
    raw = dict(context_map or {})
    rules = [dict(rule or {}) for rule in safe_list(raw.get("rules"))]
    return {
        "id": raw.get("id") or raw.get("_id") or raw.get("name") or "unknown_context_map",
        "title": raw.get("title") or raw.get("name") or raw.get("id") or "Untitled Context Map",
        "source_type": raw.get("source_type") or raw.get("sourceType") or raw.get("source") or "unknown",
        "target_definition": raw.get("target_definition") or raw.get("targetDefinition") or raw.get("target") or None,
        "rules": rules,
        "terminology_bindings": [normalize_terminology_binding(item) for item in safe_list(raw.get("terminology_bindings"))],
        "notes": [f"{item}".strip() for item in safe_list(raw.get("notes")) if f"{item}".strip()],
        "raw": raw,
    }
