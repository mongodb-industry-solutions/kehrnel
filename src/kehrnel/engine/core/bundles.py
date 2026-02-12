"""Bundle model and validation for portable strategy assets."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List


def compute_bundle_digest(bundle: Dict[str, Any]) -> str:
    payload = json.dumps(bundle or {}, sort_keys=True).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def validate_bundle(bundle: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(bundle, dict):
        return ["bundle must be an object"]
    bundle_id = bundle.get("bundle_id")
    if not bundle_id or not isinstance(bundle_id, str):
        errors.append("bundle_id is required")
    domain = bundle.get("domain")
    if not domain or not isinstance(domain, str):
        errors.append("domain is required")
    kind = bundle.get("kind")
    if kind != "slim_search_definition":
        errors.append("kind must be 'slim_search_definition'")
    payload = bundle.get("payload") or {}
    templates = payload.get("templates")
    if not templates or not isinstance(templates, list):
        errors.append("payload.templates must be a non-empty list")
    else:
        for idx, tpl in enumerate(templates):
            if not isinstance(tpl, dict):
                errors.append(f"templates[{idx}] must be an object")
                continue
            if not tpl.get("templateId"):
                errors.append(f"templates[{idx}].templateId is required")
            analytics = tpl.get("analytics_fields") or []
            rules = tpl.get("rules") or []
            if not analytics and not rules:
                errors.append(f"templates[{idx}] must have analytics_fields or rules")
            for ridx, rule in enumerate(rules):
                if not isinstance(rule, dict):
                    errors.append(f"templates[{idx}].rules[{ridx}] must be an object")
                    continue
                when = rule.get("when") or {}
                path_chain = when.get("pathChain") if isinstance(when, dict) else None
                if not path_chain or not isinstance(path_chain, list):
                    errors.append(f"templates[{idx}].rules[{ridx}].when.pathChain must be a non-empty list")
                copy_fields = rule.get("copy")
                if not copy_fields or not isinstance(copy_fields, list):
                    errors.append(f"templates[{idx}].rules[{ridx}].copy must be a non-empty list")
    return errors
