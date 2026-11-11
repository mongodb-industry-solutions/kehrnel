from __future__ import annotations

from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.contracts.semantic_execution import (
    SemanticExecutionPlanningRequest,
)
from kehrnel.engine.core.semantic_execution_planner import (
    compile_semantic_execution_plan,
)
from kehrnel.engine.core.context_object_definitions import (
    load_context_object_definition_catalog,
)
from kehrnel.engine.core.source_profiles import load_source_profile_catalog


ZERO_DIGEST = "sha256:" + "0" * 64


def _context_object_ref():
    definition = load_context_object_definition_catalog().definitions[0]
    return definition.artifact_ref.to_wire()


def _profile(profile_id: str):
    return next(
        profile
        for profile in load_source_profile_catalog().profiles
        if profile.artifact_ref.id == profile_id
    )


def _source(profile, instance_id: str):
    return {
        "instanceId": instance_id,
        "profileRef": profile.artifact_ref.to_wire(),
        "ingestionMode": "api",
    }


def _request(*, sources, consumptions, requirements=None, preferred=None):
    return SemanticExecutionPlanningRequest.model_validate(
        {
            "contract": "semantic-execution-planning-request/v1",
            "strategyRequest": {
                "contract": "strategy-compilation-request/v1",
                "blueprint": {
      "contract": "legacy-source-blueprint/internal",
                    "blueprintId": "clinical-runtime",
                    "industry": "healthcare",
                    "sources": sources,
                    "consumptions": consumptions,
                    "preferredStrategyIds": preferred or [],
                },
            },
            "semanticRequirements": requirements or [],
        }
    )


def test_native_openehr_contract_does_not_require_a_context_object():
    profile = _profile("exchange.openehr-json")
    request = _request(
        sources=[_source(profile, "ehr")],
        consumptions=[
            {
                "consumptionId": "canonical-aql",
                "kind": "openehr",
                "protocol": "openEHR REST + AQL",
                "operations": ["execute-aql"],
            }
        ],
        preferred=["openehr.rps_dual"],
    )

    first = compile_semantic_execution_plan(request)
    second = compile_semantic_execution_plan(request)

    assert first.status.value == "ready"
    assert first.plan_digest == second.plan_digest
    assert first.consumption_plans[0].to_wire() == {
        "consumptionId": "canonical-aql",
        "mode": "native",
        "authorityIds": ["semantic:openehr:ehr"],
        "resolved": True,
        "rationale": (
            "Preserve the selected strategy's native semantic model and compile "
            "the consumption contract directly to its native binding."
        ),
    }
    assert first.subplans[0].binding_kind.value == "openehr_aql"
    assert first.subplans[0].preserves_native_semantics is True
    assert first.authorities[0].kind.value == "openehr"


def test_native_fhir_contract_uses_fhir_search_not_aql():
    profile = _profile("exchange.fhir-r4-json")
    plan = compile_semantic_execution_plan(
        _request(
            sources=[_source(profile, "fhir")],
            consumptions=[
                {
                    "consumptionId": "canonical-fhir-search",
                    "kind": "fhir",
                    "protocol": "FHIR R4 REST/Search",
                    "operations": ["search"],
                }
            ],
            preferred=["fhir.resource_store"],
        )
    )

    assert plan.status.value == "ready"
    assert plan.consumption_plans[0].mode.value == "native"
    assert plan.subplans[0].binding_kind.value == "fhir_search"
    assert all(item.binding_kind.value != "openehr_aql" for item in plan.subplans)


def test_cross_standard_contract_fails_closed_until_context_object_is_selected():
    fhir = _profile("exchange.fhir-r4-json")
    openehr = _profile("exchange.openehr-json")
    plan = compile_semantic_execution_plan(
        _request(
            sources=[_source(fhir, "fhir"), _source(openehr, "ehr")],
            consumptions=[
                {
                    "consumptionId": "patient-agent",
                    "kind": "agent_tools",
                    "operations": ["retrieve-longitudinal-context"],
                }
            ],
        )
    )

    assert plan.status.value == "blocked"
    assert plan.consumption_plans[0].mode.value == "composed"
    assert plan.consumption_plans[0].resolved is False
    assert {item.kind.value for item in plan.authorities} == {"fhir", "openehr"}
    assert "CONTEXT_OBJECT_REQUIRED" in {item.code for item in plan.issues}


def test_explicit_composed_contract_adds_context_object_authority():
    fhir = _profile("exchange.fhir-r4-json")
    openehr = _profile("exchange.openehr-json")
    plan = compile_semantic_execution_plan(
        _request(
            sources=[_source(fhir, "fhir"), _source(openehr, "ehr")],
            consumptions=[
                {
                    "consumptionId": "patient-agent",
                    "kind": "agent_tools",
                    "operations": ["retrieve-longitudinal-context"],
                }
            ],
            requirements=[
                {
                    "consumptionId": "patient-agent",
                    "mode": "composed",
                    "contextObjectRef": _context_object_ref(),
                }
            ],
        )
    )

    semantic = plan.consumption_plans[0]
    assert semantic.resolved is True
    assert semantic.context_object_ref is not None
    assert {item.kind.value for item in plan.authorities} == {
        "fhir",
        "openehr",
        "context_object",
    }
    assert "CONTEXT_OBJECT_REQUIRED" not in {item.code for item in plan.issues}


def test_unknown_context_object_definition_fails_closed():
    fhir = _profile("exchange.fhir-r4-json")
    openehr = _profile("exchange.openehr-json")
    plan = compile_semantic_execution_plan(
        _request(
            sources=[_source(fhir, "fhir"), _source(openehr, "ehr")],
            consumptions=[
                {
                    "consumptionId": "patient-agent",
                    "kind": "agent_tools",
                    "operations": ["retrieve-longitudinal-context"],
                }
            ],
            requirements=[
                {
                    "consumptionId": "patient-agent",
                    "mode": "composed",
                    "contextObjectRef": {
                        "kind": "context-object-definition",
                        "id": "unknown-context",
                        "version": "1.0.0",
                        "digest": ZERO_DIGEST,
                    },
                }
            ],
        )
    )

    assert plan.status.value == "blocked"
    assert plan.consumption_plans[0].resolved is False
    assert "CONTEXT_OBJECT_DEFINITION_NOT_FOUND" in {
        item.code for item in plan.issues
    }


def test_semantic_plan_is_available_through_the_core_api(tmp_path):
    profile = _profile("exchange.openehr-json")
    request = _request(
        sources=[_source(profile, "ehr")],
        consumptions=[
            {
                "consumptionId": "canonical-aql",
                "kind": "openehr",
                "operations": ["execute-aql"],
            }
        ],
    )
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    response = client.post(
        "/v1/internal/legacy/semantic-execution-plans/compile", json=request.to_wire()
    )

    assert response.status_code == 200
    assert response.json()["contract"] == "semantic-execution-plan/v1"
    assert response.json()["subplans"][0]["bindingKind"] == "openehr_aql"


def test_context_object_catalog_is_available_through_the_core_api(tmp_path):
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    response = client.get("/v1/context-object-definitions")

    assert response.status_code == 200
    payload = response.json()
    assert payload["contract"] == "context-object-definition-catalog/v1"
    assert payload["definitions"][0]["artifactRef"] == _context_object_ref()
    assert payload["definitions"][0]["runtimeBinding"] == "design_only"
