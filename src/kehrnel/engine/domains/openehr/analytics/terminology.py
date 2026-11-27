"""Qualified coding identity; no inferred equivalence or hierarchy expansion."""

from copy import deepcopy
import json
import re

SYSTEMS = {
    "snomed-ct": "http://snomed.info/sct",
    "snomed ct": "http://snomed.info/sct",
    "snomedct": "http://snomed.info/sct",
    "http://snomed.info/sct": "http://snomed.info/sct",
    "loinc": "http://loinc.org",
    "http://loinc.org": "http://loinc.org",
    "local": "local",
}
IDENTITY_FIELDS = ("system", "code", "scope", "version")


def identity(value):
    if not isinstance(value, dict) or set(value) - set(IDENTITY_FIELDS):
        raise ValueError(
            "Choose a qualified code with its terminology system; local codes also require their source scope"
        )
    result = deepcopy(value)
    for key, maximum in (
        ("system", 512),
        ("code", 200),
        ("scope", 1024),
        ("version", 128),
    ):
        if key not in result:
            if key in ("system", "code"):
                raise ValueError("A coded filter requires system and code")
            continue
        if (
            not isinstance(result[key], str)
            or not result[key].strip()
            or len(result[key]) > maximum
        ):
            raise ValueError("Invalid coding " + key)
        result[key] = result[key].strip()
    raw = result["system"]
    match = re.fullmatch(r"(.+)\(([^()]+)\)", raw)
    if match:
        if result.get("version") and result["version"] != match[2]:
            raise ValueError("Conflicting terminology releases")
        result["system"], result["version"] = match.groups()
    result["system"] = SYSTEMS.get(result["system"].casefold(), result["system"])
    if result["system"] == "local" and not result.get("scope"):
        raise ValueError(
            "Local codes require their owning archetype or declared value-set scope"
        )
    return result


def project_coding(value, scope=None):
    code = value.get("defining_code", {})
    raw = code.get("terminology_id", {}).get("value")
    item = {"system": raw, "code": code.get("code_string")}
    if isinstance(raw, str) and raw.casefold().split("(", 1)[0] == "local" and scope:
        item["scope"] = scope
    result = identity(item)
    if raw != result["system"]:
        result["source_system"] = raw
    result["label"] = value.get("value", "")
    language = value.get("language", {}).get("code_string")
    if language:
        result["language"] = language
    return result


def normalize_recorded(value):
    """Preserve unresolved metadata codings for inspection, never invent a scope."""
    result = deepcopy(value)
    if not isinstance(result, dict):
        return result
    try:
        normalized = identity({k: result[k] for k in IDENTITY_FIELDS if k in result})
    except ValueError:
        result["terminology_state"] = "unqualified"
        return result
    if result.get("system") != normalized["system"]:
        result.setdefault("source_system", result.get("system"))
    result.update(normalized)
    return result


def binding_scope(path, template):
    from .source import segments, selector_parts

    archetypes = [
        selector_parts(selector)[0]
        for _, selector in segments(path)
        if selector and selector_parts(selector)[0].startswith("openEHR-")
    ]
    return archetypes[-1] if archetypes else "template:" + str(template)
