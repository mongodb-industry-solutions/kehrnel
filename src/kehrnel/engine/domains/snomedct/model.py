"""Canonical SNOMED CT concept normalization."""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

TRUTHY_VALUES = {True, 1, "1", "true", "TRUE", "True", "yes", "YES"}
TYPE_FSN = "900000000000003001"
TYPE_SYNONYM = "900000000000013009"
PREFERRED_ACCEPTABILITY_ID = "900000000000548007"


def parse_release_date(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    match = re.match(r"^(\d{4})(\d{2})(\d{2})$", raw)
    if not match:
        return None
    year, month, day = match.groups()
    try:
        return datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
    except ValueError:
        return None


def normalize_active(value: Any) -> bool:
    return value in TRUTHY_VALUES


def normalize_text(value: Any) -> str:
    text = str(value or "")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def semantic_tag(term: Any) -> str:
    match = re.search(r"\(([^()]+)\)\s*$", str(term or ""))
    return match.group(1).strip() if match else ""


def slugify(value: Any) -> str:
    return normalize_text(value).replace(" ", "-")


def stringify(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def stringify_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [stringify(item) for item in value if stringify(item)]


def normalize_description(description: dict[str, Any], concept_id: str) -> dict[str, Any]:
    acceptability_map = description.get("acceptabilityMap")
    if not isinstance(acceptability_map, dict):
        acceptability_map = {}
    desc_id = stringify(description.get("descriptionId") or description.get("id"))
    return {
        **description,
        "id": desc_id,
        "descriptionId": desc_id,
        "conceptId": stringify(description.get("conceptId") or concept_id),
        "moduleId": stringify(description.get("moduleId")),
        "typeId": stringify(description.get("typeId")),
        "caseSignificanceId": stringify(description.get("caseSignificanceId")),
        "languageCode": stringify(description.get("languageCode")).lower(),
        "term": stringify(description.get("term")),
        "active": normalize_active(description.get("active")),
        "acceptabilityMap": {stringify(k): stringify(v) for k, v in acceptability_map.items() if stringify(k)},
    }


def normalize_relationship(relationship: dict[str, Any]) -> dict[str, Any]:
    rel_id = stringify(relationship.get("relationshipId") or relationship.get("id"))
    return {
        **relationship,
        "id": rel_id,
        "relationshipId": rel_id,
        "sourceId": stringify(relationship.get("sourceId")),
        "destinationId": stringify(relationship.get("destinationId")),
        "typeId": stringify(relationship.get("typeId")),
        "moduleId": stringify(relationship.get("moduleId")),
        "characteristicTypeId": stringify(relationship.get("characteristicTypeId")),
        "modifierId": stringify(relationship.get("modifierId")),
        "active": normalize_active(relationship.get("active")),
    }


def build_relationship_attribute_keys(relationships: list[dict[str, Any]] | None) -> list[str]:
    keys: set[str] = set()
    for relationship in relationships or []:
        if not normalize_active(relationship.get("active")):
            continue
        type_id = stringify(relationship.get("typeId"))
        destination_id = stringify(relationship.get("destinationId"))
        if type_id and destination_id:
            keys.add(f"{type_id}|{destination_id}")
    return sorted(keys)


def normalize_concept(
    concept: dict[str, Any],
    *,
    release_id: str,
    release_label: str | None = None,
    applied_at: datetime | None = None,
    include_descendants: bool = False,
) -> dict[str, Any]:
    """Normalize one official JSON concept into the kehrnel canonical model."""
    concept_id = stringify(concept.get("conceptId"))
    relationships = [
        normalize_relationship(item)
        for item in concept.get("relationships", []) or []
        if isinstance(item, dict)
    ]
    concrete_relationships = [
        normalize_relationship(item)
        for item in concept.get("concreteRelationships", []) or []
        if isinstance(item, dict)
    ]
    descriptions = [
        normalize_description(item, concept_id)
        for item in concept.get("descriptions", []) or []
        if isinstance(item, dict)
    ]
    release_date = parse_release_date(release_id)
    now = applied_at or datetime.now(timezone.utc)

    doc: dict[str, Any] = {
        **concept,
        "conceptId": concept_id,
        "effectiveTime": stringify(concept.get("effectiveTime")),
        "active": normalize_active(concept.get("active")),
        "moduleId": stringify(concept.get("moduleId")),
        "definitionStatusId": stringify(concept.get("definitionStatusId")),
        "memberOfRefsetIds": stringify_list(concept.get("memberOfRefsetIds")),
        "inferredParentIds": stringify_list(concept.get("inferredParentIds")),
        "inferredAncestorIds": stringify_list(concept.get("inferredAncestorIds")),
        "inferredChildIds": stringify_list(concept.get("inferredChildIds")),
        "descriptions": descriptions,
        "relationships": relationships,
        "concreteRelationships": concrete_relationships,
        "relationshipAttributeKeys": build_relationship_attribute_keys(relationships),
        "releaseId": stringify(release_id),
        "releaseDate": release_date,
        "releaseAppliedAt": now,
    }
    if release_label:
        doc["releaseLabel"] = release_label
    if not include_descendants:
        doc.pop("inferredDescendantIds", None)
    else:
        doc["inferredDescendantIds"] = stringify_list(concept.get("inferredDescendantIds"))
    return doc
