import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.contracts.artifacts import CatalogMetadata
from kehrnel.contracts.access_planning import AccessMechanism
from kehrnel.contracts.source_architecture import (
    CandidateStatus,
    SourceProfile,
    StrategyCompilationRequest,
)
from kehrnel.engine.core.source_profiles import (
    load_source_profile_catalog,
    source_profile_content_digest,
)
from kehrnel.engine.core.source_categories import (
    assert_source_catalog_links,
    load_source_category_catalog,
)
from kehrnel.engine.core.strategy_candidate_compiler import (
    StrategyCompilationError,
    compile_strategy_candidate,
)
from kehrnel.engine.core.access_planner import compile_architecture_proposal
from kehrnel.engine.core.strategy_portfolio import load_strategy_portfolio


ZERO_DIGEST = "sha256:" + "0" * 64
BUILDER_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "builder"
    / "healthcare-patient-context-proposal.json"
)


def _catalog_profile(profile_id: str):
    return next(
        profile
        for profile in load_source_profile_catalog().profiles
        if profile.artifact_ref.id == profile_id
    )


def _source(profile, instance_id: str = "source-1", ingestion_mode: str = "api"):
    return {
        "instanceId": instance_id,
        "profileRef": profile.artifact_ref.to_wire(),
        "ingestionMode": ingestion_mode,
    }


def _request(
    *,
    sources: list[dict],
    consumptions: list[dict],
    context: list[dict] | None = None,
    preferred: list[str] | None = None,
    discovered_profiles: list[SourceProfile] | None = None,
) -> StrategyCompilationRequest:
    return StrategyCompilationRequest.model_validate(
        {
            "contract": "strategy-compilation-request/v1",
            "blueprint": {
      "contract": "legacy-source-blueprint/internal",
                "blueprintId": "hospital-operational-space",
                "industry": "healthcare",
                "revision": 1,
                "sources": sources,
                "context": context or [],
                "consumptions": consumptions,
                "preferredStrategyIds": preferred or [],
            },
            "discoveredProfiles": [
                profile.to_wire() for profile in (discovered_profiles or [])
            ],
        }
    )


def _discovered_mongodb_profile() -> SourceProfile:
    payload = {
        "contract": "source-profile/v1",
        "artifactRef": {
            "kind": "source-profile",
            "id": "tenant.acme.legacy-clinical",
            "version": "1",
            "digest": ZERO_DIGEST,
        },
        "catalog": {
            "role": "source_profile",
            "provenance": "tenant_generated",
            "paths": [
                {
                    "industry": "healthcare",
                    "segments": ["source-systems", "tenant", "clinical-repository"],
                }
            ],
            "tags": ["discovered", "mongodb"],
        },
        "origin": "discovered",
        "technology": "mongodb",
        "categoryIds": ["healthcare.ehr"],
        "capabilityMaturity": "specified",
        "ingestionModes": ["snapshot", "cdc"],
        "structures": [
            {
                "name": "clinical_events",
                "kind": "collection",
                "primaryKeys": ["_id"],
                "fields": [
                    {"path": "_id", "dataType": "objectId", "nullable": False},
                    {"path": "patientKey", "dataType": "string", "nullable": False},
                    {"path": "code", "dataType": "string", "nullable": False},
                ],
                "polymorphic": True,
            }
        ],
        "semanticBindings": [
            {
                "sourcePath": "clinical_events.patientKey",
                "conceptId": "healthcare.person.local-id",
                "state": "inferred",
                "confidenceBasisPoints": 9400,
            },
            {
                "sourcePath": "clinical_events.code",
                "conceptId": "healthcare.observation.concept",
                "state": "inferred",
                "confidenceBasisPoints": 8700,
            },
        ],
        "mappingRoutes": [],
    }
    draft = SourceProfile.model_validate(payload)
    payload["artifactRef"]["digest"] = source_profile_content_digest(draft)
    return SourceProfile.model_validate(payload)


def test_catalog_separates_concrete_sources_from_target_strategies():
    profile_catalog = load_source_profile_catalog()
    categories = load_source_category_catalog()
    assert_source_catalog_links(categories, profile_catalog)
    profiles = profile_catalog.profiles
    by_id = {profile.artifact_ref.id: profile for profile in profiles}
    portfolio = load_strategy_portfolio()

    assert set(by_id) == {
        "exchange.fhir-r4-json",
        "exchange.openehr-json",
        "exchange.cdisc-dataset-json",
        "app.openmrs.mysql",
        "app.bahmni.reference",
    }
    assert by_id["app.openmrs.mysql"].catalog.role.value == "source_profile"
    assert by_id["app.openmrs.mysql"].category_ids == ["healthcare.ehr"]
    assert by_id["app.openmrs.mysql"].technology.value == "mysql"
    assert by_id["exchange.fhir-r4-json"].technology.value == "standard_exchange"
    cdisc = by_id["exchange.cdisc-dataset-json"]
    assert cdisc.artifact_ref.digest == source_profile_content_digest(cdisc)
    assert cdisc.capability_maturity.value == "specified"
    assert cdisc.category_ids == ["pharmaceutical.standards-exchange"]
    assert {mode.value for mode in cdisc.ingestion_modes} == {
        "snapshot",
        "batch",
        "api",
    }
    assert [interaction.interaction_id for interaction in cdisc.interactions] == [
        "dataset-snapshot-intake"
    ]
    assert [route.to_wire() for route in cdisc.mapping_routes] == [
        {
            "targetModel": "cdisc",
            "mode": "direct",
            "readiness": "available",
            "strategyIds": ["cdisc.sdr"],
        }
    ]
    assert portfolio.entry_for("fhir.resource_store").catalog.role.value == "target_strategy"
    assert {
        path.industry
        for path in portfolio.entry_for("fhir.resource_store").catalog.paths
    } == {"healthcare", "pharmaceutical"}
    category_ids = {category.artifact_ref.id for category in categories.categories}
    assert len(category_ids) == 31
    assert {
        "healthcare.ehr",
        "healthcare.patient-identity",
        "healthcare.laboratory",
        "healthcare.imaging",
        "pharmaceutical.edc",
        "pharmaceutical.ctms",
        "pharmaceutical.pharmacovigilance",
        "pharmaceutical.manufacturing",
    }.issubset(category_ids)


def test_tenant_extensions_must_pin_the_exact_base_artifact():
    with pytest.raises(ValueError, match="tenant_extended artifacts require extendsRef"):
        CatalogMetadata.model_validate(
            {
                "role": "target_strategy",
                "provenance": "tenant_extended",
                "paths": [
                    {
                        "industry": "healthcare",
                        "segments": ["target-strategies", "tenant"],
                    }
                ],
            }
        )

    metadata = CatalogMetadata.model_validate(
        {
            "role": "target_strategy",
            "provenance": "tenant_extended",
            "paths": [
                {
                    "industry": "healthcare",
                    "segments": ["target-strategies", "tenant"],
                }
            ],
            "extendsRef": {
                "kind": "strategy",
                "id": "openehr.rps_dual",
                "version": "0.2.0",
                "digest": ZERO_DIGEST,
            },
        }
    )

    assert metadata.extends_ref.id == "openehr.rps_dual"


def test_standards_native_openehr_source_compiles_to_supported_ibm_strategy():
    profile = _catalog_profile("exchange.openehr-json")
    request = _request(
        sources=[_source(profile)],
        consumptions=[
            {
                "consumptionId": "canonical-aql",
                "kind": "openehr",
                "protocol": "openEHR REST + AQL",
                "operations": ["commit-composition", "execute-aql"],
            },
            {
                "consumptionId": "agent-tools",
                "kind": "agent_tools",
                "operations": ["retrieve-patient-context"],
            },
        ],
        preferred=["openehr.rps_dual_ibm"],
    )

    first = compile_strategy_candidate(request)
    second = compile_strategy_candidate(request)

    assert first.status is CandidateStatus.READY
    assert first.candidate_digest == second.candidate_digest
    assert {selection.strategy_id for selection in first.strategy_selections} == {
        "openehr.rps_dual_ibm"
    }
    assert first.source_plans[0].mapping_mode.value == "direct"
    assert first.source_plans[0].integration_posture.value == (
        "selective_materialization"
    )
    assert {plan.consumption_id for plan in first.interface_plans} == {
        "canonical-aql",
        "agent-tools",
    }


def test_dataset_json_source_compiles_directly_to_supported_cdisc_strategy():
    profile = _catalog_profile("exchange.cdisc-dataset-json")
    request = _request(
        sources=[
            {
                **_source(profile),
                "interactionIds": ["dataset-snapshot-intake"],
            }
        ],
        consumptions=[
            {
                "consumptionId": "governed-study-query",
                "kind": "cdisc",
                "protocol": "Kehrnel cdisc-query/v1",
                "operations": ["query-published-study", "inspect-lineage"],
                "workload": {
                    "scope": "population",
                    "queryFeatures": ["point_lookup", "filter"],
                    "consistency": "snapshot",
                },
            },
            {
                "consumptionId": "governed-study-export",
                "kind": "analytics_export",
                "operations": ["export-reviewed-snapshot"],
                "workload": {
                    "scope": "population",
                    "queryFeatures": ["export"],
                    "consistency": "snapshot",
                },
            },
        ],
        preferred=["cdisc.sdr"],
    )

    proposal = compile_architecture_proposal(request)

    assert proposal.strategy_candidate.status is CandidateStatus.READY
    assert proposal.access_plan.status is CandidateStatus.READY
    assert proposal.strategy_candidate.source_plans[0].mapping_mode.value == "direct"
    assert {
        selection.strategy_id
        for selection in proposal.strategy_candidate.strategy_selections
    } == {"cdisc.sdr"}


def test_architecture_proposal_reuses_lowest_cost_access_structures():
    profile = _catalog_profile("exchange.openehr-json")
    request = _request(
        sources=[_source(profile)],
        consumptions=[
            {
                "consumptionId": "canonical-aql",
                "kind": "openehr",
                "operations": ["execute-aql"],
                "workload": {
                    "scope": "subject",
                    "queryFeatures": ["filter"],
                    "consistency": "strong",
                },
            },
            {
                "consumptionId": "agent-tools",
                "kind": "agent_tools",
                "operations": ["retrieve-patient-context"],
                "workload": {
                    "scope": "subject",
                    "queryFeatures": ["filter"],
                },
            },
        ],
        preferred=["openehr.rps_dual"],
    )

    first = compile_architecture_proposal(request)
    second = compile_architecture_proposal(request)

    assert first.proposal_digest == second.proposal_digest
    assert first.access_plan.status is CandidateStatus.READY
    assert first.requires_approval is False
    by_mechanism = {
        structure.mechanism: structure for structure in first.access_plan.structures
    }
    assert set(by_mechanism) == {
        AccessMechanism.DIRECT_DOCUMENT,
        AccessMechanism.TARGETED_INDEX,
    }
    assert by_mechanism[AccessMechanism.DIRECT_DOCUMENT].serves_consumption_ids == [
        "agent-tools",
        "canonical-aql",
    ]
    assert first.access_plan.projection_budget.derived_copy_count == 0
    assert {
        alternative.reason_code
        for alternative in first.access_plan.rejected_alternatives
    } == {"DERIVED_COPY_NOT_JUSTIFIED"}


def test_healthcare_architecture_proposal_matches_ui_golden_fixture():
    fixture = json.loads(BUILDER_FIXTURE.read_text(encoding="utf-8"))
    request = StrategyCompilationRequest.model_validate(fixture["request"])

    assert compile_architecture_proposal(request).to_wire() == fixture["response"]


def test_population_slo_justifies_one_rebuildable_projection():
    profile = _catalog_profile("exchange.openehr-json")
    proposal = compile_architecture_proposal(
        _request(
            sources=[_source(profile)],
            consumptions=[
                {
                    "consumptionId": "canonical-aql",
                    "kind": "openehr",
                    "operations": ["execute-aql"],
                },
                {
                    "consumptionId": "population-cohort",
                    "kind": "cohort",
                    "operations": ["find-population"],
                    "latencyTargetMs": 500,
                    "freshnessTargetSeconds": 60,
                    "workload": {
                        "scope": "population",
                        "queryFeatures": ["filter", "aggregation"],
                        "consistency": "bounded_staleness",
                        "expectedResultDocuments": 1000,
                    },
                },
            ],
            preferred=["openehr.rps_dual"],
        )
    )

    projection = next(
        structure
        for structure in proposal.access_plan.structures
        if structure.mechanism is AccessMechanism.MATERIALIZED_PROJECTION
    )
    assert projection.serves_consumption_ids == ["population-cohort"]
    assert projection.requires_derived_copy is True
    assert projection.rebuild_source == "authoritative:openehr.rps_dual"
    assert projection.freshness_target_seconds == 60
    assert proposal.access_plan.projection_budget.derived_copy_count == 1
    assert proposal.access_plan.status is CandidateStatus.REQUIRES_APPROVAL
    assert proposal.requires_approval is True
    assert "ACCESS_BENCHMARK_REQUIRED" in {
        issue.code for issue in proposal.access_plan.issues
    }


def test_strong_consistency_blocks_an_asynchronous_population_projection():
    profile = _catalog_profile("exchange.openehr-json")
    proposal = compile_architecture_proposal(
        _request(
            sources=[_source(profile)],
            consumptions=[
                {
                    "consumptionId": "canonical-aql",
                    "kind": "openehr",
                    "operations": ["execute-aql"],
                },
                {
                    "consumptionId": "strong-population-cohort",
                    "kind": "cohort",
                    "operations": ["find-population"],
                    "latencyTargetMs": 500,
                    "workload": {
                        "scope": "population",
                        "queryFeatures": ["aggregation"],
                        "consistency": "strong",
                        "expectedResultDocuments": 1000,
                    },
                },
            ],
            preferred=["openehr.rps_dual"],
        )
    )

    assert proposal.access_plan.status is CandidateStatus.BLOCKED
    assert "STRONG_CONSISTENCY_PROJECTION_CONFLICT" in {
        issue.code for issue in proposal.access_plan.issues
    }


def test_unproven_access_capability_is_visible_and_requires_approval():
    profile = _catalog_profile("exchange.openehr-json")
    proposal = compile_architecture_proposal(
        _request(
            sources=[_source(profile)],
            consumptions=[
                {
                    "consumptionId": "semantic-retrieval",
                    "kind": "vector_search",
                    "operations": ["retrieve-similar-context"],
                    "workload": {
                        "scope": "corpus",
                        "queryFeatures": ["semantic"],
                    },
                }
            ],
            preferred=["openehr.rps_dual"],
        )
    )

    assert proposal.requires_approval is True
    assert proposal.access_plan.status is CandidateStatus.REQUIRES_APPROVAL
    assert "ACCESS_IMPLEMENTATION_REQUIRED" in {
        issue.code for issue in proposal.access_plan.issues
    }
    vector = next(
        structure
        for structure in proposal.access_plan.structures
        if structure.mechanism is AccessMechanism.ATLAS_VECTOR_INDEX
    )
    assert vector.capability_maturity.value == "specified"
    assert vector.strategy_id == "openehr.rps_dual"


def test_protocol_neutral_agent_use_reuses_unambiguous_native_source_strategy():
    profile = _catalog_profile("exchange.openehr-json")
    candidate = compile_strategy_candidate(
        _request(
            sources=[_source(profile)],
            consumptions=[
                {
                    "consumptionId": "agent-tools",
                    "kind": "agent_tools",
                    "operations": ["retrieve-patient-context"],
                }
            ],
        )
    )

    assert candidate.status is CandidateStatus.READY
    assert candidate.strategy_selections[0].strategy_id == "openehr.rps_dual"
    assert candidate.strategy_selections[0].generated is False


def test_known_application_profile_is_not_claimed_executable_before_mapping_exists():
    profile = _catalog_profile("app.openmrs.mysql")
    candidate = compile_strategy_candidate(
        _request(
            sources=[_source(profile, ingestion_mode="cdc")],
            consumptions=[
                {
                    "consumptionId": "fhir-api",
                    "kind": "fhir",
                    "protocol": "FHIR R4",
                    "operations": ["search"],
                }
            ],
        )
    )

    assert candidate.status is CandidateStatus.BLOCKED
    assert candidate.source_plans[0].mapping_mode.value == "curated"
    assert candidate.source_plans[0].mapping_ref == "mapping.openmrs-to-fhir-r4/v1"
    assert "MAPPING_ROUTE_NOT_EXECUTABLE" in {issue.code for issue in candidate.issues}


def test_selected_source_interaction_surfaces_bounded_mapping_simulation():
    profile = _catalog_profile("app.openmrs.mysql")
    source = _source(profile, ingestion_mode="cdc")
    source["interactionIds"] = ["laboratory-result-observed"]
    candidate = compile_strategy_candidate(
        _request(
            sources=[source],
            consumptions=[
                {
                    "consumptionId": "canonical-aql",
                    "kind": "openehr",
                    "operations": ["commit-composition", "execute-aql"],
                }
            ],
        )
    )

    assert candidate.status is CandidateStatus.BLOCKED
    assert candidate.source_plans[0].interaction_ids == [
        "laboratory-result-observed"
    ]
    assert [
        ref.id for ref in candidate.source_plans[0].simulation_mapping_refs or []
    ] == ["mapping.openmrs-to-openehr.lab-observation"]
    assert "MAPPING_SIMULATION_AVAILABLE" in {
        issue.code for issue in candidate.issues
    }


def test_unknown_source_interaction_is_rejected_before_compilation():
    profile = _catalog_profile("app.openmrs.mysql")
    source = _source(profile, ingestion_mode="cdc")
    source["interactionIds"] = ["not-an-openmrs-interaction"]

    with pytest.raises(StrategyCompilationError, match="unknown interactions"):
        compile_strategy_candidate(
            _request(
                sources=[source],
                consumptions=[
                    {
                        "consumptionId": "canonical-aql",
                        "kind": "openehr",
                        "operations": ["execute-aql"],
                    }
                ],
            )
        )


def test_discovered_customer_repository_generates_reviewable_operational_candidate():
    profile = _discovered_mongodb_profile()
    candidate = compile_strategy_candidate(
        _request(
            sources=[_source(profile, ingestion_mode="cdc")],
            consumptions=[
                {
                    "consumptionId": "agent-tools",
                    "kind": "agent_tools",
                    "operations": ["search-events", "explain-evidence"],
                },
                {
                    "consumptionId": "operational-mql",
                    "kind": "mql",
                    "operations": ["patient-timeline"],
                },
            ],
            discovered_profiles=[profile],
        )
    )

    assert candidate.status is CandidateStatus.REQUIRES_APPROVAL
    assert candidate.strategy_selections[0].generated is True
    assert candidate.strategy_selections[0].strategy_id == (
        "generated.hospital-operational-space.operational"
    )
    assert candidate.source_plans[0].mapping_mode.value == "generated"
    assert {
        "DISCOVERED_SOURCE_APPROVAL_REQUIRED",
        "GENERATED_MAPPING_APPROVAL_REQUIRED",
        "SEMANTIC_BINDING_APPROVAL_REQUIRED",
    }.issubset({issue.code for issue in candidate.issues})


def test_multiple_patient_identity_sources_require_explicit_resolution_context():
    profile = _catalog_profile("app.openmrs.mysql")
    candidate = compile_strategy_candidate(
        _request(
            sources=[
                _source(profile, instance_id="hospital-a", ingestion_mode="cdc"),
                _source(profile, instance_id="hospital-b", ingestion_mode="cdc"),
            ],
            consumptions=[
                {"consumptionId": "fhir-api", "kind": "fhir", "operations": ["read"]}
            ],
        )
    )

    issue = next(
        item for item in candidate.issues if item.code == "IDENTITY_RESOLUTION_REQUIRED"
    )
    assert issue.refs == ["hospital-a", "hospital-b"]


def test_source_catalog_and_compiler_are_versioned_api_contracts(tmp_path):
    client = TestClient(create_app(str(tmp_path / "registry.json")))
    categories_response = client.get("/v1/source-categories")
    assert categories_response.status_code == 200
    assert len(categories_response.json()["categories"]) == 31
    catalog_response = client.get("/v1/source-profiles")

    assert catalog_response.status_code == 200
    catalog = catalog_response.json()
    assert catalog["contract"] == "source-profile-catalog/v1"

    open_ehr = next(
        profile
        for profile in catalog["profiles"]
        if profile["artifactRef"]["id"] == "exchange.openehr-json"
    )
    response = client.post(
        "/v1/internal/legacy/strategy-candidates/compile",
        json={
            "contract": "strategy-compilation-request/v1",
            "blueprint": {
                "contract": "legacy-source-blueprint/internal",
                "blueprintId": "api-proof",
                "industry": "healthcare",
                "sources": [
                    {
                        "instanceId": "ehr-feed",
                        "profileRef": open_ehr["artifactRef"],
                        "ingestionMode": "api",
                    }
                ],
                "consumptions": [
                    {
                        "consumptionId": "aql",
                        "kind": "openehr",
                        "operations": ["execute-aql"],
                    }
                ],
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["contract"] == "strategy-candidate/v1"
    assert response.json()["status"] == "ready"

    proposal_response = client.post(
        "/v1/internal/legacy/architecture-proposals/compile",
        json={
            "contract": "strategy-compilation-request/v1",
            "blueprint": {
                "contract": "legacy-source-blueprint/internal",
                "blueprintId": "api-proof",
                "industry": "healthcare",
                "sources": [
                    {
                        "instanceId": "ehr-feed",
                        "profileRef": open_ehr["artifactRef"],
                        "ingestionMode": "api",
                    }
                ],
                "consumptions": [
                    {
                        "consumptionId": "aql",
                        "kind": "openehr",
                        "operations": ["execute-aql"],
                    }
                ],
            },
        },
    )

    assert proposal_response.status_code == 200
    assert proposal_response.json()["contract"] == "architecture-proposal/v1"
    assert proposal_response.json()["accessPlan"]["contract"] == "access-plan/v1"
