from copy import deepcopy
from pathlib import Path
import runpy

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.contracts import ArchitectureResolutionRequest
from kehrnel.contracts.implementation_package_build import ImplementationPackageBuildRequest
from kehrnel.engine.core.architecture_plan_compiler import (
    ArchitectureResolutionError, _load_supported_strategy,
    compile_architecture_resolution, load_architecture_resolution_inputs,
)
from kehrnel.engine.core.implementation_package_builder import (
    ImplementationPackageBuildError, build_implementation_package,
)

STARTER = Path(__file__).parents[2] / "examples/sdk/dicom_canonical_builder_journey.py"
BUILD = runpy.run_path(str(STARTER))["build_request"]


def _request():
    return BUILD()


def _compile(payload):
    return compile_architecture_resolution(ArchitectureResolutionRequest.model_validate(payload))


def _codes(result):
    return {issue.code for issue in result.resolved_plan.issues}


@pytest.mark.parametrize("delivery", ["customer_evaluation", "public_demonstration"])
def test_explicit_dicom_metadata_evaluation_compiles_three_honest_lenses(delivery):
    payload = _request().to_wire()
    payload["deliveryLevel"] = delivery
    result = _compile(payload)
    plan = result.resolved_plan
    assert plan.status.value == "requires_approval"
    assert plan.requires_approval is True
    assert _codes(result) == {"DICOM_PREVIEW_SELECTED"}
    assert result.preparation_plan.stages == []
    assert plan.materializations == []
    assert len(result.runtime_plan.routes) == 3
    assert len(plan.resource_ownership) == 4
    assert plan.preparation_plan_ref == result.preparation_plan.artifact_ref
    assert plan.runtime_plan_ref == result.runtime_plan.artifact_ref
    for family in plan.query_plan_families:
        assert family.mechanism == "dicom-bounded-metadata-query"
        assert family.runtime_constraints["operation"] == "dicom_query"
        assert family.runtime_constraints["authorizationFromActiveEnvironmentRequired"] is True
        assert family.runtime_constraints["canonicalPackageActivationSupported"] is False
    assert {item.capability_id for item in plan.target_compatibility} == {"mongodb.indexed_queries"}


def test_default_and_explicit_production_dicom_requests_fail_closed():
    payload = _request().to_wire()
    for delivery in (None, "production_implementation"):
        changed = deepcopy(payload)
        if delivery is None:
            del changed["deliveryLevel"]
        else:
            changed["deliveryLevel"] = delivery
        result = _compile(changed)
        assert result.resolved_plan.status.value == "blocked"
        assert "DICOM_PREVIEW_REQUIRES_EVALUATION" in _codes(result)
        assert result.runtime_plan.routes == []
        assert result.resolved_plan.strategy_instances == []


def test_implicit_preview_and_stale_strategy_pins_cannot_resolve():
    for change in ("missing", "version", "digest"):
        payload = _request().to_wire()
        if change == "missing":
            payload["blueprint"]["preferredFoundationRefs"] = []
        else:
            payload["blueprint"]["preferredFoundationRefs"][0][change] = (
                "9.9.9" if change == "version" else "sha256:" + "0" * 64)
        result = _compile(payload)
        assert result.resolved_plan.status.value == "blocked"
        assert result.runtime_plan.routes == []


@pytest.mark.parametrize("protocol,behavior", [
    ("DICOMweb", "query"), ("QIDO-RS", "read"),
    ("dicom-query/v1", "search"), ("dicom-query/v1", "export"),
])
def test_unimplemented_interactions_are_not_advertised_as_native_queries(protocol, behavior):
    payload = _request().to_wire()
    for interaction in payload["blueprint"]["interactions"]:
        interaction["interface"]["protocolVersion"] = protocol
        interaction["behavior"] = behavior
    result = _compile(payload)
    assert result.resolved_plan.status.value == "blocked"
    assert _codes(result) == {"DICOM_INTERACTION_UNSUPPORTED"}
    assert result.runtime_plan.routes == []


@pytest.mark.parametrize("mutation", ["missing", "path", "digest"])
def test_missing_or_modified_native_binding_cannot_produce_routes(mutation):
    payload = _request().to_wire()
    if mutation == "missing":
        payload["semanticExecutionBindings"] = []
    elif mutation == "path":
        payload["semanticExecutionBindings"][0]["pathBindings"][0]["physicalPath"] = "unreviewed.patient"
    else:
        payload["semanticExecutionBindings"][0]["artifactRef"]["digest"] = "sha256:" + "1" * 64
    result = _compile(payload)
    assert result.resolved_plan.status.value == "blocked"
    assert result.runtime_plan.routes == []
    assert result.resolved_plan.semantic_binding_refs == []


@pytest.mark.parametrize("availability", ["missing", "unavailable", "unknown"])
def test_required_regular_query_capability_is_not_inferred(availability):
    payload = _request().to_wire()
    target = payload["targetCapabilityProfile"]
    if availability == "missing":
        target["capabilities"] = [item for item in target["capabilities"]
                                  if item["capabilityId"] != "mongodb.indexed_queries"]
    else:
        next(item for item in target["capabilities"] if item["capabilityId"] == "mongodb.indexed_queries")["availability"] = availability
    result = _compile(payload)
    assert result.resolved_plan.status.value == "blocked"
    assert "TARGET_CAPABILITY_UNAVAILABLE" in _codes(result)


def test_search_unavailability_does_not_block_regular_metadata_queries():
    payload = _request().to_wire()
    next(item for item in payload["targetCapabilityProfile"]["capabilities"]
         if item["capabilityId"] == "atlas.search")["availability"] = "unavailable"
    result = _compile(payload)
    assert result.resolved_plan.status.value == "requires_approval"
    assert result.resolved_plan.materializations == []


def test_dicom_composition_remains_unimplemented_even_with_transform_capability():
    payload = _request().to_wire()
    authority = deepcopy(payload["blueprint"]["semanticAuthorities"][0])
    authority["scopeId"] = "published-imaging-product"
    payload["blueprint"]["semanticAuthorities"].append(authority)
    posture = deepcopy(payload["blueprint"]["dataPostures"][0])
    posture["scopeId"] = authority["scopeId"]
    payload["blueprint"]["dataPostures"].append(posture)
    payload["blueprint"]["interactions"][-1]["semanticScopeId"] = authority["scopeId"]
    payload["blueprint"]["dataProductCompositions"] = [{
        "compositionId": "publish-imaging-product", "targetScopeId": authority["scopeId"],
        "inputScopeIds": ["imaging-metadata"],
    }]
    result = _compile(payload)
    assert result.resolved_plan.status.value == "blocked"
    assert "DATA_PRODUCT_COMPOSITION_STRATEGY_UNAVAILABLE" in _codes(result)
    assert result.preparation_plan.stages == []


def test_intake_is_rejected_even_with_a_valid_record_authority_policy():
    import json
    payload = _request().to_wire()
    fixture = Path(__file__).parents[1] / "fixtures/architecture/v1/blueprint-greenfield-openehr.json"
    policy = json.loads(fixture.read_text())["recordAuthorityPolicies"][0]
    policy["scope"] = {"scopeId": "imaging-metadata", "domain": "dicom", "recordTypes": ["metadata"]}
    policy["authoritativeWriterRef"] = "metadata-viewer"
    policy["permittedWriterRefs"] = ["metadata-viewer", "research-agent"]
    payload["blueprint"]["recordAuthorityPolicies"] = [policy]
    for interaction in payload["blueprint"]["interactions"]:
        interaction["behavior"] = "create"
    result = _compile(payload)
    assert result.resolved_plan.status.value == "blocked"
    assert _codes(result) == {"DICOM_INTERACTION_UNSUPPORTED"}
    assert result.preparation_plan.stages == []
    assert result.runtime_plan.routes == []


def test_public_evidence_is_exact_and_does_not_promote_release_support():
    inputs = load_architecture_resolution_inputs()
    binding = next(item for item in inputs.semantic_execution_bindings if item.strategy_ref.id == "dicom.tiered_store")
    assert binding.semantic_authority_ref.kind == "dicom-metadata-contract"
    assert binding.strategy_ref.version == "0.5.0"
    assert {item.kind.value for item in binding.consumers} == {"query_planner", "executor"}
    with pytest.raises(ArchitectureResolutionError):
        _load_supported_strategy("dicom.tiered_store")


def test_resolution_is_deterministic_and_delivery_changes_plan_identity():
    request = _request()
    first = compile_architecture_resolution(request)
    assert first.to_wire() == compile_architecture_resolution(request).to_wire()
    changed = request.to_wire()
    changed["deliveryLevel"] = "public_demonstration"
    second = _compile(changed)
    assert first.resolved_plan.artifact_ref != second.resolved_plan.artifact_ref
    assert first.preparation_plan.artifact_ref != second.preparation_plan.artifact_ref
    assert first.runtime_plan.artifact_ref != second.runtime_plan.artifact_ref


def test_http_discovery_compilation_and_package_guard_match_sdk(tmp_path):
    client = TestClient(create_app(str(tmp_path / "registry.json")))
    response = client.get("/v1/architecture-resolution-inputs")
    assert response.status_code == 200
    assert any(item["strategyRef"]["id"] == "dicom.tiered_store"
               for item in response.json()["semanticExecutionBindings"])
    request = _request()
    expected = compile_architecture_resolution(request)
    response = client.post("/v1/architecture-resolutions/compile", json=request.to_wire())
    assert response.status_code == 200
    assert response.json() == expected.to_wire()
    build_request = ImplementationPackageBuildRequest(
        contract="implementation-package-build-request/v1", resolutionRequest=request,
        expectedResolvedPlanRef=expected.resolved_plan.artifact_ref,
        deliveryLevel="customer_evaluation")
    with pytest.raises(ImplementationPackageBuildError) as error:
        build_implementation_package(build_request)
    assert error.value.code == "IMPLEMENTATION_PACKAGE_NOT_READY"
    response = client.post("/v1/implementation-packages/build", json=build_request.to_wire())
    assert response.status_code == 422
    assert "IMPLEMENTATION_PACKAGE_NOT_READY" in response.text
