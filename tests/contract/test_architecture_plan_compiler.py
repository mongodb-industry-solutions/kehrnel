from copy import deepcopy
import json
from pathlib import Path

from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.contracts import ArchitectureResolutionRequest
from kehrnel.engine.core.architecture_plan_compiler import (
    _load_supported_strategy,
    _strategy_ref,
    compile_architecture_resolution,
    load_architecture_resolution_inputs,
)


FIXTURES = Path(__file__).parents[1] / "fixtures" / "architecture" / "v1"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _request(*, include_binding: bool = True, atlas_search: str = "available"):
    blueprint = _load("blueprint-greenfield-openehr.json")
    target = _load("target-capability-profile.json")
    next(
        item
        for item in target["capabilities"]
        if item["capabilityId"] == "atlas.search"
    )["availability"] = atlas_search
    bindings = []
    if include_binding:
        binding = _load("semantic-execution-binding-openehr.json")
        manifest = _load_supported_strategy("openehr.rps_dual")
        binding["strategyRef"] = _strategy_ref(manifest).to_wire()
        bindings.append(binding)
    return ArchitectureResolutionRequest(
        contract="architecture-resolution-request/v1",
        blueprint=blueprint,
        targetCapabilityProfile=target,
        semanticExecutionBindings=bindings,
    )


def _request_with_composition(*, supported_target: bool = True):
    payload = _request().to_wire()
    blueprint = payload["blueprint"]
    source_scope = blueprint["semanticAuthorities"][0]["scopeId"]
    target_scope = "derived-clinical-product"
    authority = deepcopy(blueprint["semanticAuthorities"][0])
    authority["scopeId"] = target_scope
    if not supported_target:
        authority["mode"] = "derived"
        authority["authorityRef"] = {
            "kind": "context-object-definition",
            "id": "derived-clinical-product",
            "version": "1.0.0",
            "digest": "sha256:" + "a" * 64,
        }
    blueprint["semanticAuthorities"].append(authority)
    repository_id = next(
        item["participantId"]
        for item in blueprint["participants"]
        if item["kind"] == "repository"
    )
    blueprint["dataPostures"].append({
        "scopeId": target_scope,
        "posture": "selective_materialization",
        "servingRepositoryRef": repository_id,
    })
    interaction = deepcopy(blueprint["interactions"][-1])
    interaction["interactionId"] = "query-derived-product"
    interaction["artifactRef"]["id"] = "query-derived-product"
    interaction["semanticScopeId"] = target_scope
    if not supported_target:
        interaction["interface"] = {"kind": "agent_tool"}
    blueprint["interactions"].append(interaction)
    # The fixture binds individual interactions. Extend that reviewed binding
    # to the new target interaction so this case exercises composition rather
    # than the independent incomplete-binding guard.
    payload["semanticExecutionBindings"][0]["queryInteractionIds"].append(
        "query-derived-product"
    )
    blueprint["dataProductCompositions"] = [{
        "compositionId": "compose-derived-product",
        "targetScopeId": target_scope,
        "inputScopeIds": [source_scope],
    }]
    return ArchitectureResolutionRequest.model_validate(payload)


def test_source_free_openehr_blueprint_compiles_into_three_linked_lenses():
    result = compile_architecture_resolution(_request())

    assert result.contract == "architecture-resolution-result/v1"
    assert result.resolved_plan.status.value == "ready"
    assert result.resolved_plan.requires_approval is False
    assert result.resolved_plan.preparation_plan_ref == (
        result.preparation_plan.artifact_ref
    )
    assert result.resolved_plan.runtime_plan_ref == result.runtime_plan.artifact_ref
    assert {item.kind.value for item in result.preparation_plan.stages} == {
        "authoritative_write",
        "materialize",
    }
    assert {item.interaction_id for item in result.runtime_plan.routes} == {
        "create-composition",
        "correct-composition",
        "query-cohort",
    }
    assert result.resolved_plan.query_plan_families[0].mechanism == (
        "atlas-search-then-authoritative-fetch"
    )
    assert all(
        item.compatible for item in result.resolved_plan.target_compatibility
    )


def test_explicit_data_product_composition_compiles_to_a_preparation_stage():
    result = compile_architecture_resolution(_request_with_composition())

    stage = next(
        item for item in result.preparation_plan.stages
        if item.kind.value == "compose"
    )
    assert stage.composition_id == "compose-derived-product"
    assert stage.interaction_ids == ["query-derived-product"]
    assert stage.input_resources == ["openehr-rps_dual-canonical"]
    assert stage.output_resources == [
        "openehr-rps_dual-composition-derived-clinical-product"
    ]
    assert not any(
        issue.code == "DATA_PRODUCT_COMPOSITION_STRATEGY_UNAVAILABLE"
        for issue in result.resolved_plan.issues
    )


def test_unplannable_composition_names_the_composition_in_a_blocking_issue():
    result = compile_architecture_resolution(
        _request_with_composition(supported_target=False)
    )

    issue = next(
        item for item in result.resolved_plan.issues
        if item.code == "DATA_PRODUCT_COMPOSITION_STRATEGY_UNAVAILABLE"
    )
    assert issue.severity.value == "blocking"
    assert issue.refs[0] == "compose-derived-product"
    assert "compose-derived-product" in issue.message
    assert not any(
        item.composition_id == "compose-derived-product"
        for item in result.preparation_plan.stages
    )


def test_resolution_is_deterministic_and_content_addressed():
    first = compile_architecture_resolution(_request())
    second = compile_architecture_resolution(_request())

    assert first.to_wire() == second.to_wire()
    assert first.resolved_plan.artifact_ref.digest.startswith("sha256:")
    assert first.preparation_plan.artifact_ref.digest.startswith("sha256:")
    assert first.runtime_plan.artifact_ref.digest.startswith("sha256:")


def test_unavailable_required_target_capability_blocks_resolution():
    result = compile_architecture_resolution(_request(atlas_search="unavailable"))

    assert result.resolved_plan.status.value == "blocked"
    assert result.resolved_plan.requires_approval is False
    assert any(
        issue.code == "TARGET_CAPABILITY_UNAVAILABLE"
        and issue.severity.value == "blocking"
        for issue in result.resolved_plan.issues
    )
    assessment = next(
        item
        for item in result.resolved_plan.target_compatibility
        if item.capability_id == "atlas.search"
    )
    assert assessment.compatible is False


def test_missing_semantic_binding_blocks_without_inventing_one():
    result = compile_architecture_resolution(_request(include_binding=False))

    assert result.resolved_plan.status.value == "blocked"
    assert result.resolved_plan.semantic_binding_refs == []
    assert result.preparation_plan.stages == []
    assert result.runtime_plan.routes == []
    assert any(
        issue.code == "SEMANTIC_EXECUTION_BINDING_REQUIRED"
        for issue in result.resolved_plan.issues
    )


def test_blueprint_change_changes_all_three_plan_identities():
    request = _request()
    first = compile_architecture_resolution(request)
    changed = deepcopy(request.to_wire())
    changed["blueprint"]["interactions"][1]["guarantees"]["freshnessTargetSeconds"] = 60
    changed_request = ArchitectureResolutionRequest.model_validate(changed)
    second = compile_architecture_resolution(changed_request)

    assert first.resolved_plan.artifact_ref.digest != second.resolved_plan.artifact_ref.digest
    assert first.preparation_plan.artifact_ref.digest != second.preparation_plan.artifact_ref.digest
    assert first.runtime_plan.artifact_ref.digest != second.runtime_plan.artifact_ref.digest


def test_resolution_endpoint_returns_the_three_plan_artifacts(tmp_path):
    client = TestClient(create_app(str(tmp_path / "registry.json")))
    response = client.post(
        "/v1/architecture-resolutions/compile",
        json=_request().to_wire(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["contract"] == "architecture-resolution-result/v1"
    assert body["resolvedPlan"]["status"] == "ready"
    assert body["preparationPlan"]["contract"] == "preparation-plan/v1"
    assert body["runtimePlan"]["contract"] == "runtime-execution-plan/v1"


def test_resolution_inputs_publish_reusable_reviewed_native_evidence(tmp_path):
    inputs = load_architecture_resolution_inputs()

    assert inputs.contract == "architecture-resolution-input-catalog/v1"
    assert inputs.target_capability_profiles[0].deployment_id == (
        "reference-atlas-evaluation"
    )
    bindings = {
        binding.strategy_ref.id: binding
        for binding in inputs.semantic_execution_bindings
    }
    assert set(bindings) == {"openehr.rps_dual", "fhir.resource_store"}
    assert bindings["openehr.rps_dual"].writer_interaction_ids == []
    assert bindings["openehr.rps_dual"].query_interaction_ids == []
    fhir = bindings["fhir.resource_store"]
    assert fhir.semantic_authority_ref.kind == "fhir-structure-definition"
    assert fhir.semantic_authority_ref.id == "fhir-r4-observation"
    assert fhir.path_bindings[0].semantic_path == "Observation"

    client = TestClient(create_app(str(tmp_path / "registry.json")))
    response = client.get("/v1/architecture-resolution-inputs")
    assert response.status_code == 200
    assert response.json()["contract"] == (
        "architecture-resolution-input-catalog/v1"
    )


def test_reusable_binding_covers_source_free_project_interactions():
    request = _request(include_binding=False)
    inputs = load_architecture_resolution_inputs()
    wire = request.to_wire()
    wire["targetCapabilityProfile"] = inputs.target_capability_profiles[0].to_wire()
    wire["blueprint"]["targetCapabilityProfileRef"] = (
        inputs.target_capability_profiles[0].artifact_ref.to_wire()
    )
    wire["blueprint"]["semanticAuthorities"][0]["authorityRef"] = (
        inputs.semantic_execution_bindings[0].semantic_authority_ref.to_wire()
    )
    wire["semanticExecutionBindings"] = [
        inputs.semantic_execution_bindings[0].to_wire()
    ]

    result = compile_architecture_resolution(
        ArchitectureResolutionRequest.model_validate(wire)
    )
    assert result.resolved_plan.status.value == "ready"
    assert {item.interaction_id for item in result.runtime_plan.routes} == {
        "create-composition",
        "correct-composition",
        "query-cohort",
    }


def test_reviewed_fhir_r4_observation_binding_resolves_native_reference_slice():
    inputs = load_architecture_resolution_inputs()
    target = inputs.target_capability_profiles[0]
    binding = next(
        item
        for item in inputs.semantic_execution_bindings
        if item.strategy_ref.id == "fhir.resource_store"
    )
    blueprint = _load("blueprint-greenfield-fhir.json")
    blueprint["targetCapabilityProfileRef"] = target.artifact_ref.to_wire()
    blueprint["semanticAuthorities"][0]["authorityRef"] = (
        binding.semantic_authority_ref.to_wire()
    )
    request = ArchitectureResolutionRequest(
        contract="architecture-resolution-request/v1",
        blueprint=blueprint,
        targetCapabilityProfile=target,
        semanticExecutionBindings=[binding],
    )

    result = compile_architecture_resolution(request)

    assert result.resolved_plan.status.value == "ready"
    assert result.resolved_plan.semantic_binding_refs == [binding.artifact_ref]
    assert result.resolved_plan.strategy_instances[0].strategy_ref.id == (
        "fhir.resource_store"
    )
    assert {route.interaction_id for route in result.runtime_plan.routes} == {
        "create-observation",
        "update-observation",
        "search-observation",
    }
