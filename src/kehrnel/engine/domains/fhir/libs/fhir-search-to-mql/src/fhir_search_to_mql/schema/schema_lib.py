"""
FHIR JSON Schema parsing — shared library for resource structure briefs.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from fhir_search_to_mql.schema.paths import indexes_dir, schema_json_path

_TYPE_SUFFIX_RE = re.compile(r"^(.+?)([A-Z][A-Za-z0-9]*)$")

PRIMITIVES = frozenset({
    "base64Binary", "boolean", "canonical", "code", "date", "dateTime",
    "decimal", "id", "instant", "integer", "integer64", "markdown", "oid",
    "positiveInt", "string", "time", "unsignedInt", "uri", "url", "uuid",
    "xhtml",
})

DATATYPE_EXTRACTOR_HINTS = {
    "HumanName": "HumanNameExtractor",
    "Identifier": "IdentifierExtractor",
    "CodeableConcept": "CodeableConceptExtractor",
    "Coding": "CodingExtractor",
    "Reference": "ReferenceExtractor",
    "Period": "PeriodExtractor",
    "CodeableReference": "CodeableReference (split .concept / .reference rules)",
    "ContactPoint": "ContactPointExtractor",
    "Address": "AddressExtractor",
    "Quantity": "QuantityExtractor",
    "Age": "AgeDurationExtractor",
    "Range": "RangeExtractor",
    "boolean": "DirectFieldExtractor (tokenType: boolean) or top-level scalar",
    "dateTime": "DirectFieldExtractor (datatype: date) or top-level date query",
    "instant": "DirectFieldExtractor (datatype: date)",
    "string": "DirectFieldExtractor + _lower companion for string search params",
}


@dataclass
class FieldRow:
    name: str
    ref: Optional[str]
    is_array: bool
    required: bool
    description: str


@dataclass
class ResourceBrief:
    resource: str
    fhir_version: str
    description: str
    required: List[str]
    fields: List[FieldRow]
    polymorphic: Dict[str, List[str]]
    backbone_elements: List[str]
    denorm_hints: List[Dict[str, str]]
    backbone_fields: Dict[str, List[Dict[str, Any]]]


def schema_path_for(version: str) -> Path:
    return schema_json_path(version)


def resources_index_path(version: str) -> Path:
    suffix = "r5" if version.upper().startswith("R5") else "r6"
    return indexes_dir() / f"resources.{suffix}.json"


def load_schema(version: str) -> Dict[str, Any]:
    with open(schema_path_for(version), encoding="utf-8") as f:
        return json.load(f)


def ref_name(ref: str) -> Optional[str]:
    if ref.startswith("#/definitions/"):
        return ref.rsplit("/", 1)[-1]
    return None


def extract_ref(prop: Dict[str, Any]) -> Optional[str]:
    if "$ref" in prop:
        return ref_name(prop["$ref"])
    if prop.get("type") == "array" and isinstance(prop.get("items"), dict):
        return extract_ref(prop["items"])
    if "allOf" in prop:
        for item in prop["allOf"]:
            r = extract_ref(item)
            if r:
                return r
    return None


def is_type_suffix(suffix: str, definitions: Dict[str, Any]) -> bool:
    if suffix in definitions or suffix in PRIMITIVES:
        return True
    if suffix.lower() in PRIMITIVES:
        return True
    camel = suffix[0].lower() + suffix[1:] if len(suffix) > 1 else suffix.lower()
    return camel in PRIMITIVES


def find_polymorphic(
    fields: Dict[str, FieldRow], definitions: Dict[str, Any]
) -> Dict[str, List[str]]:
    buckets: Dict[str, List[str]] = {}
    for fname in fields:
        if fname.startswith("_"):
            continue
        m = _TYPE_SUFFIX_RE.match(fname)
        if not m:
            continue
        prefix, suffix = m.group(1), m.group(2)
        if prefix and is_type_suffix(suffix, definitions):
            buckets.setdefault(prefix, []).append(fname)
    return {k: sorted(v) for k, v in buckets.items() if len(v) > 1}


def backbone_field_rows(
    def_name: str, definitions: Dict[str, Any]
) -> List[Dict[str, Any]]:
    if def_name not in definitions:
        return []
    props = definitions[def_name].get("properties", {})
    rows: List[Dict[str, Any]] = []
    for fname, prop in sorted(props.items()):
        if fname.startswith("_"):
            continue
        ref = extract_ref(prop)
        rows.append({
            "name": fname,
            "ref": ref,
            "array": prop.get("type") == "array" or "items" in prop,
        })
    return rows


def list_fhir_resources(version: str = "R5") -> List[str]:
    defs = load_schema(version)["definitions"]
    names: List[str] = []
    for name, raw in defs.items():
        if not isinstance(raw, dict):
            continue
        props = raw.get("properties") or {}
        if "resourceType" in props and name and name[0].isupper():
            names.append(name)
    return sorted(names)


def is_domain_resource(resource: str, version: str = "R5") -> bool:
    defs = load_schema(version)["definitions"]
    if resource not in defs:
        return False
    return "text" in (defs[resource].get("properties") or {})


def parse_definition(name: str, definitions: Dict[str, Any]) -> ResourceBrief:
    if name not in definitions:
        raise KeyError(f"Definition {name!r} not in schema")

    raw = definitions[name]
    props = raw.get("properties", {})
    required_list = list(raw.get("required", []))
    rows: Dict[str, FieldRow] = {}

    for fname, prop in props.items():
        if fname.startswith("_") and fname != "_id":
            continue
        ref = extract_ref(prop)
        rows[fname] = FieldRow(
            name=fname,
            ref=ref,
            is_array=prop.get("type") == "array" or "items" in prop,
            required=fname in required_list,
            description=(prop.get("description") or "")[:200],
        )

    poly = find_polymorphic(rows, definitions)
    backbone = sorted(
        n for n in definitions
        if n.startswith(f"{name}_") and isinstance(definitions.get(n), dict)
    )
    backbone_fields = {bb: backbone_field_rows(bb, definitions) for bb in backbone}

    hints: List[Dict[str, str]] = []
    seen: Set[str] = set()
    for fname, row in rows.items():
        if fname in poly or fname.startswith("_"):
            continue
        hint = DATATYPE_EXTRACTOR_HINTS.get(row.ref or "", "")
        if not hint or row.ref in seen:
            continue
        seen.add(row.ref or "")
        card = "1..*" if row.is_array else "0..1"
        hints.append({
            "field": fname,
            "type": row.ref or "primitive",
            "cardinality": card,
            "extractor": hint,
        })

    for bb in backbone:
        hints.append({
            "field": bb,
            "type": "BackboneElement",
            "cardinality": "0..*",
            "extractor": "see backbone_fields in index",
        })

    return ResourceBrief(
        resource=name,
        fhir_version="",
        description=(raw.get("description") or "")[:300],
        required=[f for f in required_list if not f.startswith("_")],
        fields=sorted(rows.values(), key=lambda r: r.name),
        polymorphic=poly,
        backbone_elements=backbone,
        denorm_hints=hints,
        backbone_fields=backbone_fields,
    )


def brief_to_dict(brief: ResourceBrief) -> Dict[str, Any]:
    return {
        "resource": brief.resource,
        "description": brief.description,
        "required": brief.required,
        "fields": [
            {
                "name": r.name,
                "ref": r.ref,
                "array": r.is_array,
                "required": r.required,
            }
            for r in brief.fields
        ],
        "polymorphic": brief.polymorphic,
        "backbone_elements": brief.backbone_elements,
        "backbone_fields": brief.backbone_fields,
        "denorm_hints": brief.denorm_hints,
    }


def build_brief(resource: str, version: str) -> ResourceBrief:
    definitions = load_schema(version)["definitions"]
    brief = parse_definition(resource, definitions)
    brief.fhir_version = version.upper()
    return brief


def load_resources_index(version: str = "R5") -> Optional[Dict[str, Any]]:
    path = resources_index_path(version)
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def get_resource_from_index(resource: str, version: str = "R5") -> Optional[Dict[str, Any]]:
    data = load_resources_index(version)
    if not data:
        return None
    return (data.get("resources") or {}).get(resource)


def build_resources_index(version: str = "R5") -> Dict[str, Any]:
    definitions = load_schema(version)["definitions"]
    resources: Dict[str, Any] = {}
    for name in list_fhir_resources(version):
        brief = parse_definition(name, definitions)
        resources[name] = brief_to_dict(brief)
    return {
        "fhir_version": version.upper(),
        "source": str(schema_path_for(version)),
        "resource_count": len(resources),
        "resources": resources,
    }
