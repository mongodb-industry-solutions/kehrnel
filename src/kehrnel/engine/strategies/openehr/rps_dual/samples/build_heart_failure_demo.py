"""Build the deterministic openEHR heart-failure reference cohort.

The demo intentionally contains near misses.  It is designed to prove that a
cohort query matches the expanded SNOMED CT concept, terminology identifier,
clinical date, and repeated EVALUATION occurrence together.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import uuid

from kehrnel.engine.domains.openehr.templates.semantic_catalog import (
    QueryableField,
    compile_opt_semantic_catalog,
)
from kehrnel.engine.strategies.openehr.rps_dual.config import normalize_config
from kehrnel.engine.strategies.openehr.rps_dual.index_definition_builder import (
    build_search_index_definition_from_mappings,
)


SAMPLES_DIR = Path(__file__).resolve().parent
STRATEGY_DIR = SAMPLES_DIR.parent
REFERENCE_DIR = SAMPLES_DIR / "reference"
TEMPLATE_ID = "sample_laboratory_v0.4"
TEMPLATE_PATH = REFERENCE_DIR / "templates" / "sample_laboratory_v0_4.opt"
ACTIVE_MAPPINGS_PATH = STRATEGY_DIR / "ingest" / "config" / "flattener_mappings_f.jsonc"
REFERENCE_MAPPINGS_PATH = REFERENCE_DIR / "projection_mappings.json"
SHORTCUTS_PATH = STRATEGY_DIR / "bundles" / "shortcuts" / "shortcuts.json"
BUNDLED_SEARCH_INDEX_PATH = STRATEGY_DIR / "bundles" / "searchIndex" / "searchIndex.json"
REFERENCE_SEARCH_INDEX_PATH = REFERENCE_DIR / "search_index.definition.json"
HEART_FAILURE_CANONICAL_DIR = (
    REFERENCE_DIR / "canonical" / "sample_laboratory_v0_4_heart_failure"
)
HEART_FAILURE_ENVELOPES_PATH = (
    REFERENCE_DIR / "envelopes" / "sample_laboratory_v0_4_heart_failure.ndjson"
)
ADVERSARIAL_DIR = REFERENCE_DIR / "adversarial"
CORRELATION_FIXTURE_PATH = ADVERSARIAL_DIR / "split_evaluation_correlation.envelope.json"
ALL_ENVELOPES_PATH = REFERENCE_DIR / "envelopes" / "all.ndjson"
MANIFEST_PATH = REFERENCE_DIR / "manifest.json"

ROOT_ARCHETYPE = "openEHR-EHR-COMPOSITION.report-result.v1"
SECTION_ARCHETYPE = "openEHR-EHR-SECTION.problem_list.v0"
DIAGNOSIS_ARCHETYPE = "openEHR-EHR-EVALUATION.problem_diagnosis.v1"
SNOMED_CT = "SNOMED-CT"
HEART_FAILURE_CODES = frozenset({"84114007", "195111005"})
SINCE = "2025-01-01T00:00:00Z"


@dataclass(frozen=True)
class Diagnosis:
    code: str
    terminology_id: str
    recognised_at: str
    display: str


@dataclass(frozen=True)
class Scenario:
    case_id: str
    expected_match: bool
    reason: str
    diagnoses: tuple[Diagnosis, ...]


SCENARIOS = (
    Scenario(
        "hf-root-after-2025",
        True,
        "Root heart-failure concept in the requested terminology and period.",
        (Diagnosis("84114007", SNOMED_CT, "2025-02-12T09:00:00Z", "Heart failure"),),
    ),
    Scenario(
        "hf-expanded-after-2025",
        True,
        "Expanded heart-failure concept in the requested terminology and period.",
        (
            Diagnosis(
                "195111005",
                SNOMED_CT,
                "2026-01-15T14:30:00Z",
                "Expanded heart failure concept",
            ),
        ),
    ),
    Scenario(
        "wrong-concept-after-2025",
        False,
        "The date matches but the SNOMED CT concept is not in the expansion.",
        (
            Diagnosis(
                "38341003",
                SNOMED_CT,
                "2025-04-03T10:15:00Z",
                "Hypertensive disorder",
            ),
        ),
    ),
    Scenario(
        "wrong-terminology-after-2025",
        False,
        "The code string collides but belongs to a different terminology.",
        (Diagnosis("84114007", "LOCAL", "2025-05-04T11:00:00Z", "Local diagnosis"),),
    ),
    Scenario(
        "hf-before-2025",
        False,
        "The diagnosis matches but was clinically recognised before the period.",
        (Diagnosis("84114007", SNOMED_CT, "2024-12-31T23:59:59Z", "Heart failure"),),
    ),
)

CORRELATION_SCENARIO = Scenario(
    "split-evaluation-correlation-negative",
    False,
    (
        "Heart failure occurs before the period and a different diagnosis occurs "
        "after it; predicates must not be joined across EVALUATION instances."
    ),
    (
        Diagnosis("84114007", SNOMED_CT, "2024-10-01T08:00:00Z", "Heart failure"),
        Diagnosis(
            "38341003",
            SNOMED_CT,
            "2025-06-01T08:00:00Z",
            "Hypertensive disorder",
        ),
    ),
)


def _stable_uuid(kind: str, case_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"kehrnel:openehr:hf-demo:{kind}:{case_id}"))


def _evaluation(diagnosis: Diagnosis) -> dict[str, Any]:
    return {
        "_type": "EVALUATION",
        "name": {"_type": "DV_TEXT", "value": "Diagnosis"},
        "archetype_details": {
            "archetype_id": {"value": DIAGNOSIS_ARCHETYPE},
            "template_id": {"value": TEMPLATE_ID},
            "rm_version": "1.0.4",
        },
        "language": {
            "_type": "CODE_PHRASE",
            "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"},
            "code_string": "en",
        },
        "encoding": {
            "_type": "CODE_PHRASE",
            "terminology_id": {
                "_type": "TERMINOLOGY_ID",
                "value": "IANA_character-sets",
            },
            "code_string": "UTF-8",
        },
        "subject": {"_type": "PARTY_SELF"},
        "data": {
            "_type": "ITEM_TREE",
            "name": {"_type": "DV_TEXT", "value": "Structure"},
            "items": [
                {
                    "_type": "ELEMENT",
                    "name": {"_type": "DV_TEXT", "value": "Diagnosis"},
                    "value": {
                        "_type": "DV_CODED_TEXT",
                        "value": diagnosis.display,
                        "defining_code": {
                            "_type": "CODE_PHRASE",
                            "terminology_id": {
                                "_type": "TERMINOLOGY_ID",
                                "value": diagnosis.terminology_id,
                            },
                            "code_string": diagnosis.code,
                        },
                    },
                    "archetype_node_id": "at0002",
                },
                {
                    "_type": "ELEMENT",
                    "name": {
                        "_type": "DV_TEXT",
                        "value": "Date/time clinically recognised",
                    },
                    "value": {
                        "_type": "DV_DATE_TIME",
                        "value": diagnosis.recognised_at,
                    },
                    "archetype_node_id": "at0003",
                },
            ],
            "archetype_node_id": "at0001",
        },
        "archetype_node_id": DIAGNOSIS_ARCHETYPE,
    }


def build_envelope(scenario: Scenario) -> dict[str, Any]:
    composition_id = _stable_uuid("composition", scenario.case_id)
    ehr_id = _stable_uuid("ehr", scenario.case_id)
    committed_at = max(item.recognised_at for item in scenario.diagnoses)
    composition = {
        "_type": "COMPOSITION",
        "name": {"_type": "DV_TEXT", "value": "Sample clinical problem list"},
        "archetype_details": {
            "archetype_id": {"value": ROOT_ARCHETYPE},
            "template_id": {"value": TEMPLATE_ID},
            "rm_version": "1.0.4",
        },
        "language": {
            "_type": "CODE_PHRASE",
            "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"},
            "code_string": "en",
        },
        "territory": {
            "_type": "CODE_PHRASE",
            "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_3166-1"},
            "code_string": "ES",
        },
        "category": {
            "_type": "DV_CODED_TEXT",
            "value": "event",
            "defining_code": {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"},
                "code_string": "433",
            },
        },
        "composer": {"_type": "PARTY_IDENTIFIED", "name": "Synthetic clinician"},
        "context": {
            "_type": "EVENT_CONTEXT",
            "start_time": {"_type": "DV_DATE_TIME", "value": committed_at},
            "setting": {
                "_type": "DV_CODED_TEXT",
                "value": "other care",
                "defining_code": {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {
                        "_type": "TERMINOLOGY_ID",
                        "value": "openehr",
                    },
                    "code_string": "238",
                },
            },
        },
        "content": [
            {
                "_type": "SECTION",
                "name": {"_type": "DV_TEXT", "value": "Diagnosis"},
                "archetype_details": {
                    "archetype_id": {"value": SECTION_ARCHETYPE},
                    "template_id": {"value": TEMPLATE_ID},
                    "rm_version": "1.0.4",
                },
                "items": [_evaluation(item) for item in scenario.diagnoses],
                "archetype_node_id": SECTION_ARCHETYPE,
            }
        ],
        "archetype_node_id": ROOT_ARCHETYPE,
        "uid": {
            "_type": "OBJECT_VERSION_ID",
            "value": f"{composition_id}::sample.kehrnel::1",
        },
    }
    return {
        "_id": composition_id,
        "ehr_id": ehr_id,
        "composition_version": "1",
        "time_committed": committed_at,
        "template_name": TEMPLATE_ID,
        "template_id": TEMPLATE_ID,
        "archetype_node_id": ROOT_ARCHETYPE,
        "composition_date": committed_at,
        "demo_case": scenario.case_id,
        "expected_heart_failure_since_2025": scenario.expected_match,
        "canonicalJSON": composition,
    }


def _field(catalog: Any, label: str) -> QueryableField:
    matches = [
        field
        for field in catalog.fields
        if field.containing_archetype.archetype_id == DIAGNOSIS_ARCHETYPE
        and field.label == label
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected one {DIAGNOSIS_ARCHETYPE} field labelled {label!r}; "
            f"found {len(matches)}"
        )
    return matches[0]


def build_semantic_projection_fields() -> list[dict[str, str]]:
    """Derive the sidecar projection paths from the packaged OPT."""

    catalog = compile_opt_semantic_catalog(TEMPLATE_PATH)
    if catalog.template.template_id != TEMPLATE_ID:
        raise RuntimeError(
            f"OPT template id {catalog.template.template_id!r} does not match {TEMPLATE_ID!r}"
        )
    diagnosis = _field(catalog, "Diagnosis")
    recognised = _field(catalog, "Date/time clinically recognised")
    diagnosis_variant = next(
        variant for variant in diagnosis.value_variants if variant.rm_type == "DV_CODED_TEXT"
    )
    recognised_variant = next(
        variant for variant in recognised.value_variants if variant.rm_type == "DV_DATE_TIME"
    )
    diagnosis_facets = {item.facet: item.aql_path_suffix for item in diagnosis_variant.query_paths}
    recognised_facets = {
        item.facet: item.aql_path_suffix for item in recognised_variant.query_paths
    }

    return [
        {
            "name": "Diagnosis archetype anchor",
            "path": f"{diagnosis.containing_archetype.composition_aql_path}/archetype_node_id",
            "rmType": "EVALUATION",
        },
        {
            "name": "Diagnosis (Code)",
            "path": f"{diagnosis.composition_aql_path}{diagnosis_facets['code']}",
            "rmType": diagnosis_variant.rm_type,
        },
        {
            "name": "Diagnosis (Terminology identifier)",
            "path": f"{diagnosis.composition_aql_path}{diagnosis_facets['terminology']}",
            "rmType": diagnosis_variant.rm_type,
        },
        {
            "name": "Date/time clinically recognised (Value)",
            "path": f"{recognised.composition_aql_path}{recognised_facets['value']}",
            "rmType": recognised_variant.rm_type,
        },
    ]


def _read_json(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    return json.loads(text)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _merge_projection_fields(payload: dict[str, Any]) -> dict[str, Any]:
    semantic_fields = build_semantic_projection_fields()
    semantic_names = {field["name"] for field in semantic_fields}
    semantic_catalog = compile_opt_semantic_catalog(TEMPLATE_PATH)
    provenance = {
        "templateSourceDigest": semantic_catalog.template.source_digest,
        "catalogDigest": semantic_catalog.catalog_digest,
    }
    for template in payload.get("templates", []):
        if template.get("templateId") != TEMPLATE_ID:
            continue
        retained = [
            field
            for field in template.get("fields", [])
            if field.get("name") not in semantic_names
        ]
        template["fields"] = [*retained, *semantic_fields]
        template["provenance"] = provenance
        return payload
    raise RuntimeError(f"Projection mappings do not contain template {TEMPLATE_ID!r}")


async def build_reference_pack() -> None:
    HEART_FAILURE_CANONICAL_DIR.mkdir(parents=True, exist_ok=True)
    ADVERSARIAL_DIR.mkdir(parents=True, exist_ok=True)
    envelopes = [build_envelope(scenario) for scenario in SCENARIOS]
    for envelope in envelopes:
        _write_json(
            HEART_FAILURE_CANONICAL_DIR / f"{envelope['_id']}.json",
            envelope["canonicalJSON"],
        )
    HEART_FAILURE_ENVELOPES_PATH.write_text(
        "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in envelopes),
        encoding="utf-8",
    )
    _write_json(CORRELATION_FIXTURE_PATH, build_envelope(CORRELATION_SCENARIO))

    envelope_files = sorted(
        path
        for path in (REFERENCE_DIR / "envelopes").glob("*.ndjson")
        if path.name != ALL_ENVELOPES_PATH.name
    )
    ALL_ENVELOPES_PATH.write_text(
        "".join(path.read_text(encoding="utf-8") for path in envelope_files),
        encoding="utf-8",
    )

    active_mappings = _merge_projection_fields(_read_json(ACTIVE_MAPPINGS_PATH))
    _write_json(ACTIVE_MAPPINGS_PATH, active_mappings)
    reference_mappings = _merge_projection_fields(_read_json(REFERENCE_MAPPINGS_PATH))
    _write_json(REFERENCE_MAPPINGS_PATH, reference_mappings)

    index_result = await build_search_index_definition_from_mappings(
        normalize_config({}),
        reference_mappings,
        shortcuts=_read_json(SHORTCUTS_PATH),
    )
    if index_result["warnings"]:
        raise RuntimeError(f"Search-index generation warnings: {index_result['warnings']}")
    _write_json(REFERENCE_SEARCH_INDEX_PATH, index_result["definition"])
    _write_json(BUNDLED_SEARCH_INDEX_PATH, index_result["definition"])

    manifest = _read_json(MANIFEST_PATH)
    manifest["search_index_metadata"] = index_result["metadata"]
    manifest["heart_failure_demo"] = {
        "contract": "openehr-heart-failure-demo/v1",
        "template_id": TEMPLATE_ID,
        "root_concept": "84114007",
        "expanded_codes": sorted(HEART_FAILURE_CODES),
        "terminology_evidence": {
            "observation": "illustrative",
            "authoritative": False,
            "description": (
                "A deliberately small hand-curated fixture subset; a live run "
                "must replace it with a pinned terminology expansion receipt."
            ),
        },
        "terminology_id": SNOMED_CT,
        "clinically_recognised_since": SINCE,
        "scenario_count": len(SCENARIOS),
        "positive_case_ids": [item.case_id for item in SCENARIOS if item.expected_match],
        "negative_case_ids": [item.case_id for item in SCENARIOS if not item.expected_match],
        "canonical_dir": str(HEART_FAILURE_CANONICAL_DIR.relative_to(REFERENCE_DIR)),
        "envelope_file": str(HEART_FAILURE_ENVELOPES_PATH.relative_to(REFERENCE_DIR)),
        "query_file": "queries/cross_patient_heart_failure_since_2025.aql",
        "adversarial_correlation_fixture": {
            "case_id": CORRELATION_SCENARIO.case_id,
            "envelope_file": str(CORRELATION_FIXTURE_PATH.relative_to(REFERENCE_DIR)),
            "opt_conformant": False,
            "excluded_from_all_ndjson": True,
            "purpose": CORRELATION_SCENARIO.reason,
        },
    }
    _write_json(MANIFEST_PATH, manifest)


def main() -> int:
    asyncio.run(build_reference_pack())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
