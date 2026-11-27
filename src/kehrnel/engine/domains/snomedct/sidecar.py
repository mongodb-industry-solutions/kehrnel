"""Term-level sidecar projection for SNOMED CT search."""

from __future__ import annotations

from typing import Any

from .model import (
    PREFERRED_ACCEPTABILITY_ID,
    TYPE_FSN,
    TYPE_SYNONYM,
    normalize_active,
    normalize_text,
    parse_release_date,
    semantic_tag,
    slugify,
)

DEFAULT_TOP_ROOT_IDS = [
    "123037004",
    "404684003",
    "71388002",
    "373873005",
    "272379006",
    "243796009",
    "260787004",
    "78621006",
    "362981000",
]

ROOT_AREA_TAGS = {
    "123037004": "body-structure",
    "404684003": "clinical-finding",
    "71388002": "procedure",
    "373873005": "product",
    "272379006": "event",
    "243796009": "situation",
    "260787004": "physical-object",
    "78621006": "observable",
    "362981000": "qualifier",
}


def is_preferred_description(description: dict[str, Any]) -> bool:
    acceptability_map = description.get("acceptabilityMap")
    if not isinstance(acceptability_map, dict):
        return False
    return any(str(value) == PREFERRED_ACCEPTABILITY_ID for value in acceptability_map.values())


def term_type(type_id: Any) -> str:
    raw = str(type_id or "")
    if raw == TYPE_FSN:
        return "fsn"
    if raw == TYPE_SYNONYM:
        return "synonym"
    return "description"


def compact_description(description: dict[str, Any]) -> dict[str, Any] | None:
    term = str(description.get("term") or "").strip()
    if not term:
        return None
    acceptability_map = description.get("acceptabilityMap")
    if not isinstance(acceptability_map, dict):
        acceptability_map = {}
    return {
        "descriptionId": str(description.get("descriptionId") or description.get("id") or "").strip(),
        "term": term,
        "normalized": normalize_text(term),
        "preferred": is_preferred_description(description),
        "typeId": str(description.get("typeId") or ""),
        "acceptabilityMap": acceptability_map,
        "acceptabilityIds": sorted({str(value) for value in acceptability_map.values() if str(value)}),
        "languageRefsetIds": sorted({str(key) for key in acceptability_map.keys() if str(key)}),
        "caseSignificanceId": str(description.get("caseSignificanceId") or ""),
    }


def sort_descriptions(description: dict[str, Any]) -> tuple[int, int, str]:
    preferred_rank = 0 if description.get("preferred") else 1
    return preferred_rank, len(description.get("term") or ""), str(description.get("term") or "")


def build_term_documents(
    concept: dict[str, Any],
    *,
    release_id: str,
    language_codes: list[str],
    top_root_ids: list[str] | None = None,
    max_synonyms: int = 10,
) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    concept_id = str(concept.get("conceptId") or "").strip()
    if not concept_id:
        return docs
    ancestors = [str(value) for value in concept.get("inferredAncestorIds", []) or [] if str(value)]
    parents = [str(value) for value in concept.get("inferredParentIds", []) or [] if str(value)]
    root_ids = top_root_ids or DEFAULT_TOP_ROOT_IDS
    top_roots = [value for value in ancestors if value in set(root_ids)]
    area_tags = [ROOT_AREA_TAGS[value] for value in top_roots if value in ROOT_AREA_TAGS]
    descriptions = [
        item
        for item in concept.get("descriptions", []) or []
        if isinstance(item, dict) and normalize_active(item.get("active"))
    ]

    for language_code in [code.lower() for code in language_codes]:
        compacted = [
            compact_description(item)
            for item in descriptions
            if str(item.get("languageCode") or "").lower() == language_code
        ]
        compacted = [item for item in compacted if item]
        if not compacted:
            continue
        fsns = sorted([item for item in compacted if item["typeId"] == TYPE_FSN], key=sort_descriptions)
        synonyms = sorted([item for item in compacted if item["typeId"] == TYPE_SYNONYM], key=sort_descriptions)
        all_sorted = sorted(compacted, key=sort_descriptions)
        preferred = synonyms[0] if synonyms else fsns[0] if fsns else all_sorted[0]
        fsn = fsns[0] if fsns else preferred
        display_term = preferred["term"]
        fsn_term = fsn["term"]
        tag = semantic_tag(fsn_term or display_term)
        tag_key = slugify(tag) if tag else ""
        dedupe = set()
        synonym_terms = []
        for item in synonyms:
            normalized = item["normalized"]
            if normalized in {normalize_text(display_term), normalize_text(fsn_term)} or normalized in dedupe:
                continue
            dedupe.add(normalized)
            synonym_terms.append(item["term"])
            if len(synonym_terms) >= max_synonyms:
                break
        semantic_area_tags = area_tags + ([tag_key] if tag_key else [])

        for index, description in enumerate(all_sorted):
            term = description["term"]
            normalized = description["normalized"]
            doc = {
                "_id": f"{release_id}|{concept_id}|{description.get('descriptionId') or index}|{language_code}",
                "releaseId": release_id,
                "releaseDate": parse_release_date(release_id),
                "conceptId": concept_id,
                "descriptionId": description.get("descriptionId") or None,
                "languageCode": language_code,
                "active": True,
                "conceptActive": normalize_active(concept.get("active")),
                "term": term,
                "matchedTerm": term,
                "displayTerm": term,
                "preferredTerm": display_term,
                "fsn": fsn_term,
                "normalizedTerm": normalized,
                "normalizedDisplay": normalized,
                "termType": term_type(description["typeId"]),
                "typeId": description["typeId"],
                "definitionStatusId": str(concept.get("definitionStatusId") or ""),
                "moduleId": str(concept.get("moduleId") or ""),
                "effectiveTime": str(concept.get("effectiveTime") or ""),
                "preferred": bool(description.get("preferred")),
                "isPreferred": bool(description.get("preferred")),
                "termRank": (30 if description.get("preferred") else 0)
                + (20 if description["typeId"] == TYPE_SYNONYM else 0)
                + (10 if description["typeId"] == TYPE_FSN else 0)
                + (4 if len(term) <= 28 else 0),
                "parentIds": parents,
                "ancestorIds": ancestors,
            }
            if description["acceptabilityIds"]:
                doc["acceptabilityIds"] = description["acceptabilityIds"]
            if description["languageRefsetIds"]:
                doc["languageRefsetIds"] = description["languageRefsetIds"]
            if description["caseSignificanceId"]:
                doc["caseSignificanceId"] = description["caseSignificanceId"]
            if tag:
                doc["semanticTag"] = tag
                doc["semanticTagKey"] = tag_key
            if synonym_terms:
                doc["synonyms"] = synonym_terms
            if top_roots:
                doc["topRoots"] = top_roots[:12]
            if semantic_area_tags:
                doc["areaTags"] = sorted(set(semantic_area_tags))[:12]
            doc["embedText"] = " | ".join([term, display_term, fsn_term, tag or ""]).strip(" |")
            docs.append(doc)
    return docs
