from __future__ import annotations

from copy import deepcopy

import jsonschema
import pytest
from pydantic import ValidationError

from kehrnel.contracts import (
    IndustryOutcomeCatalog,
    InteractionRequirement,
    InteractionWriteTarget,
)
from kehrnel.contracts.schema_registry import contract_schemas


DIGEST = "sha256:" + "a" * 64


def _ref(
    kind: str,
    artifact_id: str,
    digest: str = DIGEST,
    *,
    version: str = "1.0.0",
) -> dict:
    return {
        "kind": kind,
        "id": artifact_id,
        "version": version,
        "digest": digest,
    }


def _catalog(role: str, *segments: str) -> dict:
    return {
        "role": role,
        "provenance": "mongodb_curated",
        "paths": [{"industry": "healthcare", "segments": list(segments)}],
        "tags": [],
    }


def _valid_catalog() -> dict:
    outcome_ref = _ref("outcome-pattern", "healthcare.longitudinal-record")
    architecture_ref = _ref(
        "reference-architecture", "healthcare.openehr-operational-record"
    )
    outcome = {
        "contract": "outcome-pattern/v1",
        "artifactRef": outcome_ref,
        "catalog": _catalog("outcome_pattern", "outcomes", "clinical-record"),
        "name": "Longitudinal clinical record",
        "summary": "Give care teams a governed longitudinal record at operational latency.",
        "industryExperienceIds": ["healthcare"],
        "domainIds": ["D01"],
        "scenarios": [
            {
                "scenarioId": "review-record",
                "name": "Review a patient's record",
                "actorRoles": ["clinician"],
                "trigger": "A clinician opens a care encounter.",
                "goal": "Assemble the latest longitudinal context.",
                "observableResult": "The record and its provenance appear together.",
                "freshnessExpectation": "Interactive current",
                "authorityInvariant": "The owning clinical service remains authoritative.",
                "agentExample": "Explain the record with evidence.",
                "successMeasures": ["Record is returned inside the latency target."],
                "interactionIds": ["retrieve-record", "record-decision"],
            }
        ],
        "sourceCategoryIds": ["healthcare.ehr"],
        "dataProductIds": ["P01", "P20"],
        "interactions": [
            {
                "interactionId": "retrieve-record",
                "name": "Retrieve record",
                "description": "Read the governed longitudinal record.",
                "effect": "retrieve",
                "autonomyModes": ["direct_application", "agent_assisted"],
                "readProductIds": ["P01"],
                "writeTargets": [],
                "approvalRequired": False,
                "auditRequired": True,
            },
            {
                "interactionId": "record-decision",
                "name": "Record decision",
                "description": "Persist a reviewed decision with its evidence.",
                "effect": "record_decision",
                "autonomyModes": ["direct_application"],
                "readProductIds": ["P01"],
                "writeTargets": [
                    {
                        "kind": "decision_trace",
                        "productId": "P20",
                        "authorityRequired": True,
                        "sourceOfTruth": True,
                    }
                ],
                "approvalRequired": True,
                "auditRequired": True,
            },
        ],
        "capabilityMaturity": "compilable",
        "supportStatus": "supported",
        "publicationStatus": "published",
        "evidenceRefs": [_ref("test-run", "outcome.longitudinal-record")],
    }
    architecture = {
        "contract": "reference-architecture/v1",
        "artifactRef": architecture_ref,
        "catalog": _catalog(
            "reference_architecture", "reference-architectures", "clinical-record"
        ),
        "name": "openEHR operational record",
        "summary": "Preserve canonical compositions and compile efficient access paths.",
        "architectureKind": "operational_data_layer",
        "outcomePatternRefs": [outcome_ref],
        "sourceCategoryRefs": [_ref("source-category", "healthcare.ehr")],
        "sourceProfileRefs": [],
        "dataProductIds": ["P01", "P20"],
        "capabilityIds": ["B01", "B02"],
        "consumptionKinds": ["openehr", "mql"],
        "interactionIds": ["retrieve-record", "record-decision"],
        "strategyRefs": [_ref("strategy", "openehr.rps_dual")],
        "mappingRefs": [],
        "semanticAssetRefs": [],
        "capabilityMaturity": "compilable",
        "supportStatus": "supported",
        "publicationStatus": "published",
        "evidenceRefs": [_ref("test-run", "architecture.openehr-operational")],
    }
    experience = {
        "contract": "demo-experience/v1",
        "artifactRef": _ref(
            "demo-experience", "healthcare.longitudinal-record-proof"
        ),
        "catalog": _catalog("demo_experience", "experiences", "clinical-record"),
        "name": "Longitudinal record proof",
        "summary": "Show the outcome, then reveal the architecture that produces it.",
        "outcomePatternRefs": [outcome_ref],
        "referenceArchitectureRefs": [architecture_ref],
        "interactionIds": ["retrieve-record"],
        "visualPatternId": "v01",
        "fixturePackRefs": [_ref("fixture-pack", "healthcare.record-demo")],
        "runEvidenceRefs": [_ref("simulation-run", "healthcare.record-demo.run")],
        "storyboardSteps": [
            {
                "stepId": "open-record",
                "title": "Open the longitudinal record",
                "actorAction": "Select a patient.",
                "visibleResult": "The canonical timeline loads with provenance.",
                "evidenceRefs": [],
            }
        ],
        "failureScenario": "A late source remains visibly stale instead of being hidden.",
        "capabilityMaturity": "simulated",
        "supportStatus": "evaluation_only",
        "publicationStatus": "draft",
        "evidenceRefs": [],
    }
    domains = [
        {
            "contract": "portfolio-domain/v1",
            "artifactRef": _ref("portfolio-domain", "D01"),
            "catalog": _catalog("portfolio_domain", "domains", "care-delivery"),
            "name": "Care delivery",
            "description": "Operational capabilities used while delivering care.",
            "primarySegments": ["Provider"],
            "industryExperienceIds": ["healthcare"],
        }
    ]
    data_products = [
        {
            "contract": "data-product/v1",
            "artifactRef": _ref("data-product", "P01"),
            "catalog": _catalog("data_product", "data-products", "clinical-record"),
            "name": "Clinical record",
            "description": "Longitudinal clinical facts and their provenance.",
            "representationOptions": ["openEHR compositions", "MongoDB documents"],
            "inputOutcomeRefs": [outcome_ref],
            "outputOutcomeRefs": [],
        },
        {
            "contract": "data-product/v1",
            "artifactRef": _ref("data-product", "P20"),
            "catalog": _catalog("data_product", "data-products", "decision-trace"),
            "name": "Decision trace",
            "description": "Evidence, approval, action, and outcome records.",
            "representationOptions": ["MongoDB documents"],
            "inputOutcomeRefs": [],
            "outputOutcomeRefs": [outcome_ref],
        },
    ]
    capabilities = [
        {
            "contract": "architecture-capability/v1",
            "artifactRef": _ref("architecture-capability", "B01"),
            "catalog": _catalog(
                "architecture_capability", "capabilities", "canonical-storage"
            ),
            "name": "Canonical storage",
            "description": "Preserve canonical records while serving operational access.",
            "generatedArtifacts": ["Collections", "validators", "indexes"],
            "reuseExamples": ["openEHR CDR", "FHIR store"],
            "coreOutcomeRefs": [outcome_ref],
            "optionalOutcomeRefs": [],
            "reference": "Kehrnel strategy portfolio",
            "scope": "Proposed module boundary",
        },
        {
            "contract": "architecture-capability/v1",
            "artifactRef": _ref("architecture-capability", "B02"),
            "catalog": _catalog(
                "architecture_capability", "capabilities", "decision-evidence"
            ),
            "name": "Decision evidence",
            "description": "Persist decisions with evidence and authority boundaries.",
            "generatedArtifacts": ["Decision trace collection"],
            "reuseExamples": ["Clinical review"],
            "coreOutcomeRefs": [],
            "optionalOutcomeRefs": [outcome_ref],
            "scope": "Proposed module boundary",
        },
    ]
    visual_patterns = [
        {
            "contract": "visual-pattern/v1",
            "artifactRef": _ref("visual-pattern", "v01"),
            "catalog": _catalog("visual_pattern", "visual-patterns", "timeline"),
            "name": "Evidence timeline",
            "description": "Expose source, freshness, and evidence on one timeline.",
            "screenComponents": ["Timeline", "evidence drawer"],
            "audienceInteraction": "Select an event to inspect its provenance.",
            "principleDemonstrated": "Operational answers remain evidence-linked.",
            "architectInspection": "Reveal source and strategy artifacts.",
            "operationExamples": ["Patient record review"],
        }
    ]
    return {
        "contract": "industry-outcome-catalog/v1",
        "artifactRef": _ref(
            "industry-outcome-catalog",
            "healthcare-industry-outcomes",
            version="0.1.0",
        ),
        "catalogId": "healthcare-industry-outcomes",
        "version": "0.1.0",
        "domains": domains,
        "dataProducts": data_products,
        "architectureCapabilities": capabilities,
        "visualPatterns": visual_patterns,
        "outcomes": [outcome],
        "referenceArchitectures": [architecture],
        "demoExperiences": [experience],
    }


def test_industry_outcome_catalog_round_trips_and_validates_json_schema():
    parsed = IndustryOutcomeCatalog.model_validate(_valid_catalog())

    assert parsed.outcomes[0].interactions[1].effect.value == "record_decision"
    assert parsed.reference_architectures[0].strategy_refs[0].id == "openehr.rps_dual"
    assert parsed.to_wire() == _valid_catalog()

    schema = contract_schemas()["industry-outcome-catalog.schema.json"]
    jsonschema.Draft202012Validator(schema).validate(parsed.to_wire())


def test_catalog_rejects_unresolved_or_stale_exact_references():
    payload = _valid_catalog()
    stale_ref = dict(
        payload["referenceArchitectures"][0]["outcomePatternRefs"][0]
    )
    stale_ref["digest"] = "sha256:" + "b" * 64
    payload["referenceArchitectures"][0]["outcomePatternRefs"] = [stale_ref]

    with pytest.raises(ValidationError, match="must resolve inside the catalog"):
        IndustryOutcomeCatalog.model_validate(payload)


def test_architecture_sources_and_products_must_be_covered_by_its_outcomes():
    payload = _valid_catalog()
    payload["referenceArchitectures"][0]["sourceCategoryRefs"].append(
        _ref("source-category", "healthcare.laboratory")
    )

    with pytest.raises(ValidationError, match="sourceCategoryRefs are not covered"):
        IndustryOutcomeCatalog.model_validate(payload)

    payload = _valid_catalog()
    extra_product = deepcopy(payload["dataProducts"][0])
    extra_product["artifactRef"] = _ref("data-product", "P99")
    extra_product["inputOutcomeRefs"] = []
    payload["dataProducts"].append(extra_product)
    payload["referenceArchitectures"][0]["dataProductIds"].append("P99")

    with pytest.raises(ValidationError, match="dataProductIds are not covered"):
        IndustryOutcomeCatalog.model_validate(payload)


def test_architecture_coverage_accepts_the_union_of_multiple_outcomes():
    payload = _valid_catalog()
    second_outcome = deepcopy(payload["outcomes"][0])
    second_ref = _ref(
        "outcome-pattern",
        "healthcare.laboratory-results",
        "sha256:" + "b" * 64,
    )
    second_outcome["artifactRef"] = second_ref
    second_outcome["sourceCategoryIds"] = ["healthcare.laboratory"]
    second_outcome["dataProductIds"] = ["P02"]
    second_outcome["scenarios"][0]["interactionIds"] = ["retrieve-lab-results"]
    second_outcome["interactions"] = [
        {
            "interactionId": "retrieve-lab-results",
            "name": "Retrieve laboratory results",
            "description": "Read governed laboratory results.",
            "effect": "retrieve",
            "autonomyModes": ["direct_application"],
            "readProductIds": ["P02"],
            "writeTargets": [],
            "approvalRequired": False,
            "auditRequired": True,
        }
    ]
    payload["outcomes"].append(second_outcome)

    second_product = deepcopy(payload["dataProducts"][0])
    second_product["artifactRef"] = _ref("data-product", "P02")
    second_product["inputOutcomeRefs"] = [second_ref]
    payload["dataProducts"].append(second_product)

    architecture = payload["referenceArchitectures"][0]
    architecture["outcomePatternRefs"].append(second_ref)
    architecture["sourceCategoryRefs"].append(
        _ref("source-category", "healthcare.laboratory")
    )
    architecture["dataProductIds"].append("P02")
    architecture["interactionIds"].append("retrieve-lab-results")
    payload["architectureCapabilities"][0]["optionalOutcomeRefs"].append(
        second_ref
    )

    parsed = IndustryOutcomeCatalog.model_validate(payload)

    assert len(parsed.reference_architectures[0].outcome_pattern_refs) == 2


def test_data_product_links_are_bidirectional_without_inventing_flow_direction():
    payload = _valid_catalog()
    payload["dataProducts"][0]["inputOutcomeRefs"] = []

    with pytest.raises(ValidationError, match="missing reverse input/output links"):
        IndustryOutcomeCatalog.model_validate(payload)

    payload = _valid_catalog()
    extra_product = deepcopy(payload["dataProducts"][0])
    extra_product["artifactRef"] = _ref("data-product", "P99")
    extra_product["inputOutcomeRefs"] = [
        payload["outcomes"][0]["artifactRef"]
    ]
    payload["dataProducts"].append(extra_product)

    with pytest.raises(ValidationError, match="does not declare the data product"):
        IndustryOutcomeCatalog.model_validate(payload)


def test_architecture_capabilities_must_relate_to_and_cover_its_outcomes():
    payload = _valid_catalog()
    for capability in payload["architectureCapabilities"]:
        if capability["artifactRef"]["id"] == "B02":
            capability["optionalOutcomeRefs"] = []
    payload["referenceArchitectures"][0]["capabilityIds"] = ["B02"]

    with pytest.raises(ValidationError, match="not related to any"):
        IndustryOutcomeCatalog.model_validate(payload)

    payload = _valid_catalog()
    second_outcome = deepcopy(payload["outcomes"][0])
    second_ref = _ref(
        "outcome-pattern",
        "healthcare.uncovered-outcome",
        "sha256:" + "b" * 64,
    )
    second_outcome["artifactRef"] = second_ref
    payload["outcomes"].append(second_outcome)
    for product in payload["dataProducts"]:
        product["inputOutcomeRefs"].append(second_ref)
    payload["referenceArchitectures"][0]["outcomePatternRefs"].append(second_ref)

    with pytest.raises(ValidationError, match="not covered by its selected capabilities"):
        IndustryOutcomeCatalog.model_validate(payload)


def test_catalog_rejects_demo_interaction_not_exposed_by_architecture():
    payload = _valid_catalog()
    payload["demoExperiences"][0]["interactionIds"] = ["record-decision"]
    payload["referenceArchitectures"][0]["interactionIds"] = ["retrieve-record"]

    with pytest.raises(ValidationError, match="unknown architecture interactions"):
        IndustryOutcomeCatalog.model_validate(payload)


def test_state_producing_interaction_requires_explicit_write_target():
    value = deepcopy(_valid_catalog()["outcomes"][0]["interactions"][1])
    value["writeTargets"] = []

    with pytest.raises(ValidationError, match="require at least one writeTarget"):
        InteractionRequirement.model_validate(value)


def test_outcome_rejects_interaction_product_outside_declared_products():
    payload = _valid_catalog()
    payload["outcomes"][0]["interactions"][0]["readProductIds"] = ["unknown.product"]

    with pytest.raises(ValidationError, match="unknown data products"):
        IndustryOutcomeCatalog.model_validate(payload)


def test_catalog_rejects_same_artifact_version_with_conflicting_digest():
    payload = _valid_catalog()
    duplicate = deepcopy(payload["outcomes"][0])
    duplicate["artifactRef"]["digest"] = "sha256:" + "b" * 64
    payload["outcomes"].append(duplicate)

    with pytest.raises(ValidationError, match="duplicate outcomes refs"):
        IndustryOutcomeCatalog.model_validate(payload)


def test_retrieve_and_subscribe_interactions_cannot_write():
    value = deepcopy(_valid_catalog()["outcomes"][0]["interactions"][0])
    value["writeTargets"] = [
        {
            "kind": "source_system",
            "productId": "ehr",
            "authorityRequired": True,
            "sourceOfTruth": False,
        }
    ]

    with pytest.raises(ValidationError, match="cannot declare writeTargets"):
        InteractionRequirement.model_validate(value)


def test_source_of_truth_writes_require_explicit_authority():
    with pytest.raises(ValidationError, match="require authorityRequired"):
        InteractionWriteTarget.model_validate(
            {
                "kind": "operational_product",
                "productId": "P01",
                "authorityRequired": False,
                "sourceOfTruth": True,
            }
        )


def test_scenario_interactions_must_resolve_inside_the_outcome():
    payload = _valid_catalog()
    payload["outcomes"][0]["scenarios"][0]["interactionIds"] = ["missing"]

    with pytest.raises(ValidationError, match="references unknown interactions"):
        IndustryOutcomeCatalog.model_validate(payload)


def test_promoted_outcome_requires_evidence():
    payload = _valid_catalog()
    payload["outcomes"][0]["evidenceRefs"] = []

    with pytest.raises(ValidationError, match="beyond specified maturity"):
        IndustryOutcomeCatalog.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("strategyRefs", "require strategyRefs"),
        ("evidenceRefs", "require evidenceRefs"),
    ],
)
def test_supported_compilable_architecture_requires_runtime_proof(field, message):
    payload = _valid_catalog()
    payload["referenceArchitectures"][0][field] = []

    with pytest.raises(ValidationError, match=message):
        IndustryOutcomeCatalog.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("fixturePackRefs", "require fixturePackRefs"),
        ("runEvidenceRefs", "require runEvidenceRefs"),
    ],
)
def test_simulated_demo_requires_fixture_and_run_evidence(field, message):
    payload = _valid_catalog()
    payload["demoExperiences"][0][field] = []

    with pytest.raises(ValidationError, match=message):
        IndustryOutcomeCatalog.model_validate(payload)


def test_demo_outcomes_must_be_delivered_by_a_referenced_architecture():
    payload = _valid_catalog()
    second_outcome = deepcopy(payload["outcomes"][0])
    second_ref = _ref(
        "outcome-pattern",
        "healthcare.second-outcome",
        "sha256:" + "b" * 64,
    )
    second_outcome["artifactRef"] = second_ref
    payload["outcomes"].append(second_outcome)
    for product in payload["dataProducts"]:
        product["inputOutcomeRefs"].append(second_ref)
    payload["demoExperiences"][0]["outcomePatternRefs"].append(second_ref)

    with pytest.raises(ValidationError, match="must be included"):
        IndustryOutcomeCatalog.model_validate(payload)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda value: value["outcomes"][0]["domainIds"].append("D99"),
            "domainIds contains undefined IDs",
        ),
        (
            lambda value: value["outcomes"][0]["dataProductIds"].append("P99"),
            "dataProductIds contains undefined IDs",
        ),
        (
            lambda value: value["referenceArchitectures"][0][
                "capabilityIds"
            ].append("B99"),
            "capabilityIds contains undefined IDs",
        ),
        (
            lambda value: value["demoExperiences"][0].update(
                {"visualPatternId": "v99"}
            ),
            "visualPatternId contains undefined IDs",
        ),
    ],
)
def test_opaque_catalog_ids_resolve_to_first_class_definitions(mutate, message):
    payload = _valid_catalog()
    mutate(payload)

    with pytest.raises(ValidationError, match=message):
        IndustryOutcomeCatalog.model_validate(payload)


def test_top_level_catalog_identity_is_exact():
    payload = _valid_catalog()
    payload["artifactRef"]["version"] = "0.2.0"

    with pytest.raises(ValidationError, match="artifactRef.version must equal version"):
        IndustryOutcomeCatalog.model_validate(payload)
