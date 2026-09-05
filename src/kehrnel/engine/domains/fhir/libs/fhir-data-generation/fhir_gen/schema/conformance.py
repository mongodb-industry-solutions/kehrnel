"""Fail-closed base-schema conformance for generated FHIR resources."""

from __future__ import annotations

from collections import Counter
from typing import Any

from jsonschema import Draft6Validator

from .registry import SchemaRegistry


def _remove_at_path(document: Any, path: list[Any]) -> bool:
    if not path:
        return False
    parent = document
    for part in path[:-1]:
        try:
            parent = parent[part]
        except (KeyError, IndexError, TypeError):
            return False
    key = path[-1]
    if isinstance(parent, dict) and key in parent:
        parent.pop(key, None)
        return True
    if isinstance(parent, list) and isinstance(key, int) and 0 <= key < len(parent):
        parent.pop(key)
        return True
    return False


def conform_resource_to_schema(
    resource: dict[str, Any],
    registry: SchemaRegistry,
    *,
    max_passes: int = 1_000,
) -> dict[str, Any]:
    """Remove invalid optional content and report any unrepairable error.

    This is a final guard for randomly selected optional/backbone fields. Resource
    enrichers should still emit the correct release shape. The guard never invents
    required content and therefore cannot conceal a missing required root field.
    """
    resource_type = str(resource.get("resourceType") or "")
    raw_schema = registry.parser().raw_schema
    if resource_type not in (raw_schema.get("definitions") or {}):
        return {
            "passed": False,
            "removals": {},
            "unresolved": [
                {
                    "path": "$",
                    "message": f"{resource_type} is absent from the active schema",
                }
            ],
        }
    validator = Draft6Validator(
        {
            "$schema": raw_schema.get("$schema"),
            "$ref": f"#/definitions/{resource_type}",
            "definitions": raw_schema["definitions"],
        }
    )
    removals: Counter[str] = Counter()
    unresolved: list[dict[str, str]] = []
    for _ in range(max_passes):
        errors = sorted(
            validator.iter_errors(resource),
            key=lambda error: len(list(error.absolute_path)),
            reverse=True,
        )
        if not errors:
            break
        error = errors[0]
        path = list(error.absolute_path)
        removed: list[str] = []
        if error.validator == "additionalProperties" and isinstance(
            error.instance, dict
        ):
            allowed = set((error.schema.get("properties") or {}).keys())
            for key in sorted(set(error.instance) - allowed):
                error.instance.pop(key, None)
                removed.append(".".join(str(part) for part in [*path, key]) or "$")
        elif error.validator == "required" and path:
            if _remove_at_path(resource, path):
                removed.append(".".join(str(part) for part in path) or "$")
        elif path and _remove_at_path(resource, path):
            removed.append(".".join(str(part) for part in path) or "$")

        if not removed:
            unresolved.append(
                {
                    "path": ".".join(str(part) for part in path) or "$",
                    "message": error.message,
                }
            )
            break
        removals.update(removed)
    else:
        unresolved.append(
            {
                "path": "$",
                "message": "Conformance guard exceeded its bounded iteration limit",
            }
        )
    return {
        "passed": not unresolved,
        "removals": dict(removals),
        "unresolved": unresolved,
    }
