from __future__ import annotations

from typing import Any, Dict, List

from .models import normalize_context_definition, normalize_context_map, safe_list


def summarize_context_map(context_map: Dict[str, Any] | None, definitions: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    context_map = normalize_context_map(context_map)
    catalog = [normalize_context_definition(item) for item in safe_list(definitions)]
    target = context_map.get("target_definition")
    target_match = None
    for definition in catalog:
        if target and target in {definition["id"], definition["title"]}:
            target_match = definition
            break

    mapped_targets = [rule.get("target") for rule in context_map["rules"] if rule.get("target")]
    covered_blocks = []
    missing_blocks = []
    if target_match:
        for block in target_match.get("blocks", []):
            block_id = block.get("id")
            if any(block_id and block_id in f"{target_path}" for target_path in mapped_targets):
                covered_blocks.append(block_id)
            else:
                missing_blocks.append(block_id)

    required_rules = [rule for rule in context_map["rules"] if rule.get("required", True)]
    return {
        "id": context_map["id"],
        "title": context_map["title"],
        "sourceType": context_map["source_type"],
        "targetDefinition": target,
        "targetMatch": (
            {
                "id": target_match["id"],
                "title": target_match["title"],
                "subjectKinds": target_match["subject_kinds"],
            }
            if target_match
            else None
        ),
        "ruleCount": len(context_map["rules"]),
        "requiredRuleCount": len(required_rules),
        "terminologyBindingCount": len(context_map["terminology_bindings"]),
        "coveredBlocks": covered_blocks,
        "missingBlocks": missing_blocks,
        "ready": bool(target_match and not missing_blocks and context_map["rules"]),
        "notes": context_map["notes"],
    }
