import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient
import pytest
from pydantic import ValidationError

from kehrnel.api.app import create_app
from kehrnel.contracts import ArchitectureResolutionRequest, ImplementationPackage
from kehrnel.contracts.implementation_package_build import (
    ImplementationPackageBuildRequest,
)
from kehrnel.contracts.implementation_package_activation import (
    ImplementationPackageActivationRequest,
)
from kehrnel.contracts.implementation_package_installation import (
    ImplementationPackageInstallationRequest,
)
from kehrnel.contracts.implementation_package_materialization import (
    ImplementationPackageMaterializationRequest,
)
from kehrnel.contracts.implementation_package_preparation import (
    ImplementationPackagePreparationReadinessRequest,
    StrategyActivationInput,
)
from kehrnel.contracts.implementation_package_preparation_execution import (
    ImplementationPackagePreparationExecutionRequest,
)
from kehrnel.contracts.implementation_package_runtime_execution import (
    ImplementationPackageRuntimeExecutionRequest,
)
from kehrnel.contracts.implementation_package_promotion import (
    ImplementationPackagePromotionRequest,
)
from kehrnel.contracts.implementation_package_operational_readiness import (
    ImplementationPackageOperationalReadinessRequest,
)
from kehrnel.contracts.implementation_package_deployment import (
    ImplementationPackageDeploymentRequest,
    ImplementationPackageRollbackRequest,
)
from kehrnel.contracts.implementation_package_observation import (
    ImplementationPackageOperationalObservationRequest,
    ImplementationPackageOperationalObservationTriggerRequest,
)
from kehrnel.contracts.implementation_package_proof import (
    ImplementationPackageProofRequest,
)
from kehrnel.engine.core.architecture_plan_compiler import (
    _load_supported_strategy,
    _strategy_ref,
    compile_architecture_resolution,
)
from kehrnel.engine.core.implementation_package_builder import (
    ImplementationPackageBuildError,
    build_implementation_package,
)
from kehrnel.engine.core.implementation_package_proof import (
    ImplementationPackageProofEvidenceStore,
    run_implementation_package_proof,
)
from kehrnel.engine.core.environment import EnvironmentRecord


FIXTURES = Path(__file__).parents[1] / "fixtures" / "architecture" / "v1"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _resolution_request(*, atlas_search: str = "available"):
    blueprint = _load("blueprint-greenfield-openehr.json")
    target = _load("target-capability-profile.json")
    next(
        item
        for item in target["capabilities"]
        if item["capabilityId"] == "atlas.search"
    )["availability"] = atlas_search
    binding = _load("semantic-execution-binding-openehr.json")
    manifest = _load_supported_strategy("openehr.rps_dual")
    binding["strategyRef"] = _strategy_ref(manifest).to_wire()
    return ArchitectureResolutionRequest(
        contract="architecture-resolution-request/v1",
        blueprint=blueprint,
        targetCapabilityProfile=target,
        semanticExecutionBindings=[binding],
    )


def _build_request(*, atlas_search: str = "available"):
    resolution_request = _resolution_request(atlas_search=atlas_search)
    resolution = compile_architecture_resolution(resolution_request)
    return ImplementationPackageBuildRequest(
        contract="implementation-package-build-request/v1",
        resolutionRequest=resolution_request,
        expectedResolvedPlanRef=resolution.resolved_plan.artifact_ref,
        deliveryLevel="customer_evaluation",
        lifecycleOwner="tenant-clinical-platform-team",
    )


def test_ready_resolution_builds_one_deterministic_portal_independent_package():
    request = _build_request()

    first = build_implementation_package(request)
    second = build_implementation_package(request)

    assert first.to_wire() == second.to_wire()
    assert first.package_digest == first.artifact_ref.digest
    assert first.portal_independent is True
    assert first.resolved_plan_ref == request.expected_resolved_plan_ref
    kinds = {item.kind for item in first.required_artifact_refs}
    assert {
        "component",
        "preparation-plan",
        "runtime-execution-plan",
        "semantic-execution-binding",
        "strategy",
        "target-capability-profile",
    } <= kinds


def test_stale_resolved_plan_precondition_fails_closed():
    request = _build_request()
    wire = request.to_wire()
    wire["expectedResolvedPlanRef"]["digest"] = "sha256:" + "0" * 64

    with pytest.raises(
        ImplementationPackageBuildError,
        match="no longer resolves to the expected plan",
    ) as raised:
        build_implementation_package(
            ImplementationPackageBuildRequest.model_validate(wire)
        )

    assert raised.value.code == "RESOLVED_PLAN_PRECONDITION_FAILED"
    assert raised.value.status == 409


def test_blocked_resolution_cannot_be_packaged():
    request = _build_request(atlas_search="unavailable")

    with pytest.raises(ImplementationPackageBuildError) as raised:
        build_implementation_package(request)

    assert raised.value.code == "IMPLEMENTATION_PACKAGE_NOT_READY"


def test_production_package_request_requires_a_lifecycle_owner():
    request = _build_request()
    wire = request.to_wire()
    wire["deliveryLevel"] = "production_implementation"
    wire.pop("lifecycleOwner")

    with pytest.raises(ValidationError, match="lifecycleOwner"):
        ImplementationPackageBuildRequest.model_validate(wire)


def test_package_endpoint_preserves_the_resolved_plan_identity(tmp_path):
    request = _build_request()
    registry_path = tmp_path / "registry.json"
    client = TestClient(create_app(str(registry_path)))

    response = client.post(
        "/v1/implementation-packages/build",
        json=request.to_wire(),
    )

    assert response.status_code == 200
    package = response.json()
    assert package["contract"] == "implementation-package/v1"
    assert package["resolvedPlanRef"] == request.expected_resolved_plan_ref.to_wire()
    assert package["packageDigest"] == package["artifactRef"]["digest"]
    restarted_client = TestClient(create_app(str(registry_path)))
    stored = restarted_client.get(
        "/v1/implementation-packages/"
        f"{package['artifactRef']['id']}/versions/{package['artifactRef']['version']}"
    )
    assert stored.status_code == 200
    assert stored.json() == package


def test_control_api_registry_advertises_package_build(tmp_path):
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    response = client.get("/v1/endpoints")

    assert response.status_code == 200
    assert response.json()["endpoints"]["build_implementation_package"] == (
        "/implementation-packages/build"
    )
    assert response.json()["endpoints"]["materialize_implementation_package"] == (
        "/environments/{env_id}/implementation-package-installations/materialize"
    )
    assert response.json()["endpoints"][
        "assess_implementation_package_preparation_readiness"
    ] == (
        "/environments/{env_id}/implementation-package-preparation/readiness"
    )
    assert response.json()["endpoints"]["activate_implementation_package"] == (
        "/environments/{env_id}/implementation-package-activations/run"
    )
    assert response.json()["endpoints"][
        "execute_implementation_package_preparation"
    ] == (
        "/environments/{env_id}/implementation-package-preparation-executions/run"
    )
    assert response.json()["endpoints"][
        "execute_implementation_package_runtime_contract"
    ] == (
        "/environments/{env_id}/implementation-package-runtime-executions/run"
    )
    assert response.json()["endpoints"]["promote_implementation_package"] == (
        "/environments/{env_id}/implementation-package-promotions/run"
    )
    assert response.json()["endpoints"][
        "assess_implementation_package_operational_readiness"
    ] == (
        "/environments/{env_id}/"
        "implementation-package-operational-readiness/assess"
    )
    assert response.json()["endpoints"]["deploy_implementation_package"] == (
        "/environments/{env_id}/implementation-package-deployments/run"
    )
    assert response.json()["endpoints"]["rollback_implementation_package"] == (
        "/environments/{env_id}/implementation-package-deployments/rollback"
    )
    assert response.json()["endpoints"][
        "observe_implementation_package_operation"
    ] == (
        "/environments/{env_id}/implementation-package-operational-observations/run"
    )
    assert response.json()["endpoints"][
        "trigger_implementation_package_operational_observation"
    ] == (
        "/environments/{env_id}/"
        "implementation-package-operational-observations/trigger"
    )
    assert response.json()["endpoints"][
        "get_implementation_package_operational_observation"
    ] == (
        "/environments/{env_id}/implementation-package-operational-observations/"
        "{observation_id}/versions/{version}"
    )
    assert response.json()["endpoints"][
        "list_implementation_package_operational_observations"
    ] == (
        "/environments/{env_id}/implementation-package-deployments/"
        "{deployment_id}/versions/{version}/operational-observations"
    )


def _proof_request(package=None):
    build_request = _build_request()
    return ImplementationPackageProofRequest(
        contract="implementation-package-proof-request/v1",
        packageBuildRequest=build_request,
        package=package or build_implementation_package(build_request),
    )


def test_static_package_proof_is_deterministic_retrievable_and_not_runtime_evidence():
    request = _proof_request()
    store = ImplementationPackageProofEvidenceStore(capacity=2)

    first = run_implementation_package_proof(request, store=store)
    second = run_implementation_package_proof(request, store=store)
    evidence = store.get(first.evidence_ref.id, first.evidence_ref.version)

    assert first.to_wire() == second.to_wire()
    assert first.state == "executed"
    assert first.contains_runtime_measurements is False
    assert evidence.status == "passed"
    assert evidence.contains_runtime_measurements is False
    assert evidence.artifact_ref == first.evidence_ref
    assert all(check.status == "passed" for check in evidence.checks)


def test_tampered_package_returns_failed_evidence_instead_of_a_runtime_claim():
    request = _proof_request()
    store = ImplementationPackageProofEvidenceStore()
    passing_run = run_implementation_package_proof(request, store=store)
    package = request.package.to_wire()
    package["compilerRef"]["digest"] = "sha256:" + "0" * 64
    tampered = ImplementationPackage.model_validate(package)

    run = run_implementation_package_proof(
        request.model_copy(update={"package": tampered}),
        store=store,
    )

    assert run.state == "failed"
    assert run.contains_runtime_measurements is False
    assert run.evidence_ref != passing_run.evidence_ref
    assert store.get(
        passing_run.evidence_ref.id,
        passing_run.evidence_ref.version,
    ).status == "passed"
    assert store.get(run.evidence_ref.id, run.evidence_ref.version).status == "failed"


def test_package_proof_api_runs_and_retrieves_durable_evidence(tmp_path):
    registry_path = tmp_path / "registry.json"
    client = TestClient(create_app(str(registry_path)))
    request = _proof_request()
    package_response = client.post(
        "/v1/implementation-packages/build",
        json=request.package_build_request.to_wire(),
    )
    assert package_response.status_code == 200
    request = _proof_request(ImplementationPackage.model_validate(package_response.json()))

    response = client.post(
        "/v1/implementation-package-proofs/run",
        json=request.to_wire(),
    )

    assert response.status_code == 200
    run = response.json()
    assert run["evidencePersistence"] == "durable"
    evidence_ref = run["evidenceRef"]
    restarted_client = TestClient(create_app(str(registry_path)))
    evidence_response = restarted_client.get(
        "/v1/implementation-package-proof-evidence/"
        f"{evidence_ref['id']}/versions/{evidence_ref['version']}"
    )
    assert evidence_response.status_code == 200
    evidence = evidence_response.json()
    assert evidence["artifactRef"] == evidence_ref
    assert evidence["status"] == "passed"
    assert evidence["containsRuntimeMeasurements"] is False
    assert evidence["evidencePersistence"] == "durable"


def test_qualified_package_can_only_be_staged_for_its_exact_sandbox_target(tmp_path):
    registry_path = tmp_path / "registry.json"
    app = create_app(str(registry_path))
    app.state.strategy_runtime.registry.upsert_environment(
        EnvironmentRecord(env_id="sandbox-a", name="Sandbox A")
    )
    client = TestClient(app)
    request = _proof_request()
    package_response = client.post(
        "/v1/implementation-packages/build",
        json=request.package_build_request.to_wire(),
    )
    assert package_response.status_code == 200
    package = ImplementationPackage.model_validate(package_response.json())
    proof_response = client.post(
        "/v1/implementation-package-proofs/run",
        json=_proof_request(package).to_wire(),
    )
    assert proof_response.status_code == 200
    evidence_ref = proof_response.json()["evidenceRef"]
    installation_request = ImplementationPackageInstallationRequest(
        contract="implementation-package-installation-request/v1",
        packageRef=package.artifact_ref,
        qualificationEvidenceRef=evidence_ref,
        targetCapabilityProfileRef=package.target_capability_profile_ref,
    )

    stage_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-installations/stage",
        json=installation_request.to_wire(),
    )

    assert stage_response.status_code == 200
    stage = stage_response.json()
    assert stage["state"] == "staged"
    assert stage["packageMetadataPersisted"] is True
    assert stage["artifactsMaterialized"] is False
    assert stage["preparationExecuted"] is False
    assert stage["runtimeExecuted"] is False
    assert stage["nextRequiredAction"] == "materialize_package_artifacts"

    materialization_request = ImplementationPackageMaterializationRequest(
        contract="implementation-package-materialization-request/v1",
        installationStageRef=stage["artifactRef"],
        expectedPackageRef=package.artifact_ref,
    )
    materialization_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-installations/materialize",
        json=materialization_request.to_wire(),
    )
    assert materialization_response.status_code == 200
    receipt = materialization_response.json()
    assert receipt["state"] == "materialized"
    assert receipt["artifactClosureComplete"] is True
    assert receipt["preparationExecuted"] is False
    assert receipt["runtimeExecuted"] is False
    assert receipt["nextRequiredAction"] == "assess_preparation_readiness"
    assert {item["artifactRef"]["digest"] for item in receipt["materializedArtifacts"]} == {
        item.digest for item in package.required_artifact_refs
    }

    readiness_request = ImplementationPackagePreparationReadinessRequest(
        contract="implementation-package-preparation-readiness-request/v1",
        materializationReceiptRef=receipt["artifactRef"],
        expectedPackageRef=package.artifact_ref,
    )
    blocked_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-preparation/readiness",
        json=readiness_request.to_wire(),
    )
    assert blocked_response.status_code == 200
    blocked = blocked_response.json()
    assert blocked["state"] == "blocked"
    assert blocked["preparationExecuted"] is False
    assert blocked["runtimeExecuted"] is False
    assert blocked["nextRequiredAction"] == "provide_activation_inputs"
    assert blocked["strategyRequirements"][0]["missingInputs"] == [
        "activation_input"
    ]

    resolution = compile_architecture_resolution(
        request.package_build_request.resolution_request
    )
    strategy = resolution.resolved_plan.strategy_instances[0]
    manifest = app.state.strategy_runtime.registry.get_manifest(strategy.strategy_ref.id)
    app.state.strategy_runtime.registry.upsert_environment(
        EnvironmentRecord(
            env_id="sandbox-a",
            name="Sandbox A",
            bindings_ref="env:sandbox-a",
        )
    )
    ready_request = ImplementationPackagePreparationReadinessRequest(
        contract="implementation-package-preparation-readiness-request/v1",
        materializationReceiptRef=receipt["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        activationInputs=[
            StrategyActivationInput(
                strategyInstanceId=strategy.strategy_instance_id,
                strategyRef=strategy.strategy_ref,
                domain=manifest.domain,
                configuration={"database": "sandbox_a_openehr"},
                approved=True,
            )
        ],
    )
    ready_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-preparation/readiness",
        json=ready_request.to_wire(),
    )
    assert ready_response.status_code == 200
    ready = ready_response.json()
    assert ready["state"] == "ready"
    assert ready["nextRequiredAction"] == "activate_strategies"
    assert ready["strategyRequirements"][0]["bindingsRef"] == "env:sandbox-a"
    assert ready["strategyRequirements"][0]["activationSpecDigest"].startswith(
        "sha256:"
    )
    assert ready["deferredStageIds"]
    assert ready["preparationExecuted"] is False

    activation_request = ImplementationPackageActivationRequest(
        contract="implementation-package-activation-request/v1",
        readinessRef=ready["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        activationInputs=ready_request.activation_inputs,
    )
    activation_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-activations/run",
        json=activation_request.to_wire(),
    )
    assert activation_response.status_code == 200
    activation_receipt = activation_response.json()
    assert activation_receipt["state"] == "activated"
    assert activation_receipt["preparationExecuted"] is False
    assert activation_receipt["runtimeExecuted"] is False
    assert activation_receipt["nextRequiredAction"] == "execute_preparation_plan"
    assert len(activation_receipt["strategyActivations"]) == 1
    activation_result = activation_receipt["strategyActivations"][0]
    assert activation_result["state"] == "active"

    # Repeating the exact request satisfies the same immutable desired state.
    replay_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-activations/run",
        json=activation_request.to_wire(),
    )
    assert replay_response.status_code == 200
    assert replay_response.json() == activation_receipt

    specification_ref = activation_result["activationSpecificationRef"]
    specification_response = client.get(
        "/v1/environments/sandbox-a/approved-strategy-activation-specifications/"
        f"{specification_ref['id']}/versions/{specification_ref['version']}"
    )
    assert specification_response.status_code == 200
    specification = specification_response.json()
    assert specification["artifactRef"] == specification_ref
    assert specification["configuration"] == {"database": "sandbox_a_openehr"}
    assert specification["bindingsRef"] == "env:sandbox-a"
    assert specification["approval"] == "approved"

    stored_activation_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-activations/"
        f"{activation_receipt['activationId']}/versions/"
        f"{activation_receipt['artifactRef']['version']}"
    )
    assert stored_activation_response.status_code == 200
    assert stored_activation_response.json() == activation_receipt

    dispatch_calls = []

    async def dispatch_preparation(env_id, operation, payload):
        dispatch_calls.append((env_id, operation, payload))
        assert payload["__activation_precondition"]["activationId"] == (
            activation_result["runtimeActivationId"]
        )
        if operation == "plan":
            return {"artifacts": {"collections": ["sandbox_a_compositions"]}}
        assert operation == "apply"
        assert payload["plan"]["artifacts"]["collections"] == [
            "sandbox_a_compositions"
        ]
        return {
            "created": ["sandbox_a_compositions"],
            "updated": [],
            "skipped": [],
            "warnings": [],
        }

    app.state.strategy_runtime.dispatch = dispatch_preparation
    execution_request = ImplementationPackagePreparationExecutionRequest(
        contract="implementation-package-preparation-execution-request/v1",
        activationReceiptRef=activation_receipt["artifactRef"],
        expectedPackageRef=package.artifact_ref,
    )
    execution_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-preparation-executions/run",
        json=execution_request.to_wire(),
    )
    assert execution_response.status_code == 200
    execution = execution_response.json()
    assert execution["state"] == "prepared"
    assert execution["preparationSetupExecuted"] is True
    assert execution["preparationDataExecution"] == "awaiting_trigger"
    assert execution["runtimeExecuted"] is False
    assert execution["nextRequiredAction"] == "execute_runtime_contract"
    assert len(dispatch_calls) == 2
    assert [call[1] for call in dispatch_calls] == ["plan", "apply"]
    assert execution["strategyExecutions"][0]["state"] == "applied"
    assert execution["strategyExecutions"][0]["createdArtifacts"] == [
        "sandbox_a_compositions"
    ]
    assert execution["strategyExecutions"][0]["stageReceipts"]
    assert all(
        item["setupState"] == "applied"
        and item["dataExecutionState"] == "awaiting_trigger"
        for item in execution["strategyExecutions"][0]["stageReceipts"]
    )

    replay_execution_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-preparation-executions/run",
        json=execution_request.to_wire(),
    )
    assert replay_execution_response.status_code == 200
    assert replay_execution_response.json() == execution
    assert len(dispatch_calls) == 2

    stored_execution_response = client.get(
        "/v1/environments/sandbox-a/"
        "implementation-package-preparation-executions/"
        f"{execution['executionId']}/versions/{execution['artifactRef']['version']}"
    )
    assert stored_execution_response.status_code == 200
    assert stored_execution_response.json() == execution

    runtime_dispatch_calls = []

    async def dispatch_runtime(env_id, operation, payload):
        runtime_dispatch_calls.append((env_id, operation, payload))
        assert payload["domain"].lower() == "openehr"
        assert payload["__activation_precondition"] == {
            "activationId": activation_result["runtimeActivationId"],
            "strategyId": activation_result["strategyRef"]["id"],
        }
        if operation == "extensions":
            assert payload["op"] == "operational_probe"
            assert payload["payload"] == {"resource_types": []}
            return {
                "state": "healthy",
                "schemaState": "in_sync",
                "indexState": "in_sync",
                "reconciliationDivergenceCount": 0,
                "checks": [
                    {
                        "checkId": check_id,
                        "state": "passed",
                        "detail": "The active test strategy evidence is in sync.",
                    }
                    for check_id in (
                        "runtime_connectivity",
                        "required_collections",
                        "storage_schema",
                        "managed_indexes",
                        "derived_projection",
                        "data_freshness",
                        "reconciliation_integrity",
                    )
                ],
            }
        if operation == "query":
            assert payload["query"] == {
                "aql": "SELECT e/ehr_id/value FROM EHR e LIMIT 2"
            }
            return {
                "rows": [{"ehr_id": "ehr-1"}, {"ehr_id": "ehr-2"}],
                "hasMore": False,
                "explain": {"timings": {"kehrnel_db_ms": 1.25}},
            }
        assert operation == "authoritative_write"
        assert payload["requesterRef"] == "clinical-app"
        assert payload["recordAuthorityPolicyRef"]["id"] == (
            "clinical-record-authority"
        )
        if payload["behavior"] == "update":
            assert payload["idempotencyKey"] == "correct-composition-001"
            assert payload["requestPayload"]["precedingVersionUid"] == (
                "composition-1::my-openehr-server::1"
            )
            assert payload["requestPayload"]["ifMatch"] == (
                '"composition-1::my-openehr-server::1"'
            )
            return {
                "resourceType": "openEHR COMPOSITION",
                "resourceId": "composition-1",
                "ehrId": payload["requestPayload"]["ehrId"],
                "versionUid": "composition-1::my-openehr-server::2",
                "version": "2",
                "etag": '"composition-1::my-openehr-server::2"',
                "changeType": "modification",
                "acknowledgement": "durable_authoritative",
            }
        assert payload["behavior"] == "create"
        assert payload["idempotencyKey"] == "create-composition-001"
        return {
            "resourceType": "openEHR COMPOSITION",
            "resourceId": "composition-1",
            "ehrId": payload["requestPayload"]["ehrId"],
            "versionUid": "composition-1::my-openehr-server::1",
            "version": "1",
            "etag": '"composition-1::my-openehr-server::1"',
            "changeType": "creation",
            "acknowledgement": "durable_authoritative",
        }

    app.state.strategy_runtime.dispatch = dispatch_runtime
    runtime_request = ImplementationPackageRuntimeExecutionRequest(
        contract="implementation-package-runtime-execution-request/v1",
        preparationExecutionRef=execution["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        interactionId="query-cohort",
        requestPayload={"aql": "SELECT e/ehr_id/value FROM EHR e LIMIT 2"},
    )
    runtime_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-runtime-executions/run",
        json=runtime_request.to_wire(),
    )
    assert runtime_response.status_code == 200, runtime_response.text
    runtime_run = runtime_response.json()
    assert runtime_run["contract"] == (
        "implementation-package-runtime-execution-response/v1"
    )
    assert runtime_run["result"] == {
        "rows": [{"ehr_id": "ehr-1"}, {"ehr_id": "ehr-2"}],
        "hasMore": False,
        "explain": {"timings": {"kehrnel_db_ms": 1.25}},
    }
    runtime_receipt = runtime_run["receipt"]
    assert runtime_receipt["behavior"] == "query"
    assert runtime_receipt["state"] == "executed"
    assert runtime_receipt["runtimeExecuted"] is True
    assert runtime_receipt["evidencePersistence"] == "durable"
    assert runtime_receipt["resultPersistence"] == "not_persisted"
    assert runtime_receipt["resultEvidence"]["containsResultPayload"] is False
    assert runtime_receipt["resultEvidence"]["itemCount"] == 2
    assert runtime_receipt["policyDecision"]["preparationExecutionRef"] == (
        execution["artifactRef"]
    )
    assert runtime_receipt["queryPlanTrace"]["interactionId"] == "query-cohort"
    assert runtime_receipt["queryPlanTrace"]["planFamilyId"] == (
        "query-cohort-eligible-plan"
    )
    assert runtime_receipt["queryPlanTrace"]["selectedMechanism"] == (
        "atlas-search-then-authoritative-fetch"
    )
    assert len(runtime_dispatch_calls) == 1

    stored_runtime_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-runtime-executions/"
        f"{runtime_receipt['executionId']}/versions/"
        f"{runtime_receipt['artifactRef']['version']}"
    )
    assert stored_runtime_response.status_code == 200
    assert stored_runtime_response.json() == runtime_receipt
    assert "result" not in stored_runtime_response.json()

    write_request = runtime_request.to_wire()
    write_request["interactionId"] = "create-composition"
    write_request["idempotencyKey"] = "create-composition-001"
    write_request["requestPayload"] = {
        "ehrId": "ehr-1",
        "composition": {
            "_type": "COMPOSITION",
            "archetype_details": {"template_id": {"value": "vital-signs"}},
        },
    }
    write_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-runtime-executions/run",
        json=write_request,
    )
    assert write_response.status_code == 200, write_response.text
    write_run = write_response.json()
    write_receipt = write_run["receipt"]
    assert write_receipt["behavior"] == "create"
    assert write_receipt.get("queryPlanTrace") is None
    assert write_receipt["writeAcknowledgement"]["versionUid"] == (
        "composition-1::my-openehr-server::1"
    )
    assert write_receipt["writeAcknowledgement"]["resourceId"] == "composition-1"
    assert write_receipt["writeAcknowledgement"]["acknowledgement"] == (
        "durable_authoritative"
    )
    assert write_receipt["writeAcknowledgement"]["recordAuthorityPolicyRef"]["id"] == (
        "clinical-record-authority"
    )
    assert len(runtime_dispatch_calls) == 2

    replay_write_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-runtime-executions/run",
        json=write_request,
    )
    assert replay_write_response.status_code == 200
    assert replay_write_response.json()["receipt"] == write_receipt
    assert replay_write_response.json()["result"]["writeAcknowledgement"]["replayed"] is True
    assert len(runtime_dispatch_calls) == 2

    conflicting_write = deepcopy(write_request)
    conflicting_write["requestPayload"]["ehrId"] = "ehr-2"
    conflicting_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-runtime-executions/run",
        json=conflicting_write,
    )
    assert conflicting_response.status_code == 409
    assert conflicting_response.json()["error"]["code"] == (
        "IMPLEMENTATION_PACKAGE_RUNTIME_IDEMPOTENCY_CONFLICT"
    )
    assert len(runtime_dispatch_calls) == 2

    correction_request = deepcopy(write_request)
    correction_request["interactionId"] = "correct-composition"
    correction_request["idempotencyKey"] = "correct-composition-001"
    correction_request["requestPayload"].update({
        "precedingVersionUid": "composition-1::my-openehr-server::1",
        "ifMatch": '"composition-1::my-openehr-server::1"',
    })
    correction_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-runtime-executions/run",
        json=correction_request,
    )
    assert correction_response.status_code == 200, correction_response.text
    correction_run = correction_response.json()
    correction_receipt = correction_run["receipt"]
    assert correction_receipt["behavior"] == "update"
    assert correction_receipt["writeAcknowledgement"]["versionUid"] == (
        "composition-1::my-openehr-server::2"
    )
    assert correction_receipt["writeAcknowledgement"]["changeType"] == (
        "modification"
    )
    assert len(runtime_dispatch_calls) == 3

    correction_replay = client.post(
        "/v1/environments/sandbox-a/implementation-package-runtime-executions/run",
        json=correction_request,
    )
    assert correction_replay.status_code == 200
    assert correction_replay.json()["receipt"] == correction_receipt
    assert correction_replay.json()["result"]["writeAcknowledgement"]["replayed"] is True
    assert len(runtime_dispatch_calls) == 3

    incomplete_promotion = ImplementationPackagePromotionRequest(
        contract="implementation-package-promotion-request/v1",
        preparationExecutionRef=execution["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        runtimeEvidenceRefs=[runtime_receipt["artifactRef"]],
        approval="approved",
    )
    incomplete_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-promotions/run",
        json=incomplete_promotion.to_wire(),
    )
    assert incomplete_response.status_code == 422
    assert incomplete_response.json()["error"]["code"] == (
        "IMPLEMENTATION_PACKAGE_PROMOTION_EVIDENCE_INCOMPLETE"
    )

    promotion_request = ImplementationPackagePromotionRequest(
        contract="implementation-package-promotion-request/v1",
        preparationExecutionRef=execution["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        runtimeEvidenceRefs=[
            correction_receipt["artifactRef"],
            runtime_receipt["artifactRef"],
            write_receipt["artifactRef"],
        ],
        approval="approved",
        releaseNotes="All package interactions reviewed in the tenant sandbox.",
    )
    promotion_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-promotions/run",
        json=promotion_request.to_wire(),
    )
    assert promotion_response.status_code == 200, promotion_response.text
    promotion = promotion_response.json()
    assert promotion["state"] == "promoted"
    assert promotion["promotionScope"] == "tenant_release_candidate"
    assert promotion["deploymentState"] == "not_deployed"
    assert promotion["nextRequiredAction"] == "review_operational_readiness"
    assert [item["interactionId"] for item in promotion["interactionEvidence"]] == [
        "correct-composition",
        "create-composition",
        "query-cohort",
    ]
    assert promotion["packageRef"] == package.artifact_ref.to_wire()

    replay_promotion_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-promotions/run",
        json=promotion_request.to_wire(),
    )
    assert replay_promotion_response.status_code == 200
    assert replay_promotion_response.json() == promotion

    stored_promotion_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-promotions/"
        f"{promotion['promotionId']}/versions/{promotion['artifactRef']['version']}"
    )
    assert stored_promotion_response.status_code == 200
    assert stored_promotion_response.json() == promotion

    blocked_operational_readiness = ImplementationPackageOperationalReadinessRequest(
        contract="implementation-package-operational-readiness-request/v1",
        promotionRef=promotion["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        reviewDecision="needs_changes",
        reviewNotes="Operational ownership still needs to be assigned.",
    )
    blocked_operational_readiness_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-operational-readiness/assess",
        json=blocked_operational_readiness.to_wire(),
    )
    assert blocked_operational_readiness_response.status_code == 200
    blocked_operational_readiness_receipt = (
        blocked_operational_readiness_response.json()
    )
    assert blocked_operational_readiness_receipt["state"] == "blocked"
    assert blocked_operational_readiness_receipt["deploymentState"] == (
        "not_deployed"
    )
    assert blocked_operational_readiness_receipt["nextRequiredAction"] == (
        "provide_operational_readiness_inputs"
    )

    operational_readiness_request = ImplementationPackageOperationalReadinessRequest(
        contract="implementation-package-operational-readiness-request/v1",
        promotionRef=promotion["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        operationalOwnerRef="team:clinical-platform-operations",
        monitoringOwnerRef="team:clinical-platform-observability",
        rollbackOwnerRef="team:clinical-platform-operations",
        reviewDecision="approved",
        reviewNotes="Owners reviewed the exact promoted package and its evidence.",
    )
    operational_readiness_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-operational-readiness/assess",
        json=operational_readiness_request.to_wire(),
    )
    assert operational_readiness_response.status_code == 200
    operational_readiness = operational_readiness_response.json()
    assert operational_readiness["state"] == "ready"
    assert operational_readiness["deploymentState"] == "not_deployed"
    assert operational_readiness["nextRequiredAction"] == (
        "deploy_release_candidate"
    )
    assert operational_readiness["promotionRef"] == promotion["artifactRef"]
    assert operational_readiness["packageRef"] == package.artifact_ref.to_wire()
    assert len(operational_readiness["checks"]) == 6
    assert all(check["state"] == "passed" for check in operational_readiness["checks"])

    replay_operational_readiness_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-operational-readiness/assess",
        json=operational_readiness_request.to_wire(),
    )
    assert replay_operational_readiness_response.status_code == 200
    assert replay_operational_readiness_response.json() == operational_readiness

    stored_operational_readiness_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-operational-readiness/"
        f"{operational_readiness['readinessId']}/versions/"
        f"{operational_readiness['artifactRef']['version']}"
    )
    assert stored_operational_readiness_response.status_code == 200
    assert stored_operational_readiness_response.json() == operational_readiness

    empty_deployment_status_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-deployments/active"
    )
    assert empty_deployment_status_response.status_code == 200
    assert empty_deployment_status_response.json() == {
        "contract": "implementation-package-deployment-status/v1",
        "environmentId": "sandbox-a",
        "state": "none",
        "trafficManagement": "external",
        "nextRequiredAction": "deploy_ready_release_candidate",
    }

    deployment_request = ImplementationPackageDeploymentRequest(
        contract="implementation-package-deployment-request/v1",
        deploymentRequestId="deploy-clinical-release-001",
        readinessRef=operational_readiness["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        deploymentOwnerRef="team:clinical-platform-operations",
        approval="approved",
        changeRef="change:clinical-release-001",
        deploymentNotes="Bind the reviewed package as the active Kehrnel release.",
    )
    deployment_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-deployments/run",
        json=deployment_request.to_wire(),
    )
    assert deployment_response.status_code == 200, deployment_response.text
    deployment = deployment_response.json()
    assert deployment["packageRef"] == package.artifact_ref.to_wire()
    assert deployment["readinessRef"] == operational_readiness["artifactRef"]
    assert deployment["operationState"] == "succeeded"
    assert deployment["recordedEffect"] == "active_release_binding_created"
    assert deployment["trafficManagement"] == "external"
    assert "previousDeploymentRef" not in deployment

    replay_deployment_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-deployments/run",
        json=deployment_request.to_wire(),
    )
    assert replay_deployment_response.status_code == 200
    assert replay_deployment_response.json() == deployment

    active_deployment_status_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-deployments/active"
    )
    assert active_deployment_status_response.status_code == 200
    active_status = active_deployment_status_response.json()
    assert active_status["state"] == "active"
    assert active_status["activeDeploymentRef"] == deployment["artifactRef"]
    assert active_status["activePackageRef"] == package.artifact_ref.to_wire()

    observation_request = ImplementationPackageOperationalObservationRequest(
        contract="implementation-package-operational-observation-request/v1",
        observationRequestId="observe-clinical-release-001",
        expectedDeploymentRef=deployment["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        observedByRef="team:clinical-platform-observability",
        observedAt=datetime(2026, 10, 4, 9, 0, tzinfo=timezone.utc),
    )
    observation_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-operational-observations/run",
        json=observation_request.to_wire(),
    )
    assert observation_response.status_code == 200, observation_response.text
    observation = observation_response.json()
    assert observation["activeDeploymentRef"] == deployment["artifactRef"]
    assert observation["activePackageRef"] == package.artifact_ref.to_wire()
    assert observation["healthState"] == "healthy"
    assert observation["driftState"] == "in_sync"
    assert observation["configurationDriftState"] == "in_sync"
    assert observation["externalTrafficObservation"] == "not_observed"
    assert observation["nextRequiredAction"] == "continue_monitoring"
    assert len(observation["checks"]) == 11
    assert all(check["state"] == "passed" for check in observation["checks"])
    assert len(observation["strategyProbes"]) == 1
    assert observation["strategyProbes"][0]["strategyInstanceId"] == (
        activation_result["strategyInstanceId"]
    )
    assert observation["strategyProbes"][0]["state"] == "healthy"

    replay_observation_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-operational-observations/run",
        json=observation_request.to_wire(),
    )
    assert replay_observation_response.status_code == 200
    assert replay_observation_response.json() == observation

    scheduled_request = ImplementationPackageOperationalObservationTriggerRequest(
        contract=(
            "implementation-package-operational-observation-trigger-request/v1"
        ),
        scheduleId="clinical-release-monitor",
        expectedDeploymentRef=deployment["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        observedByRef="team:clinical-platform-observability",
        scheduledFor=datetime(2026, 10, 4, 9, 1, tzinfo=timezone.utc),
    )
    scheduled_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-operational-observations/trigger",
        json=scheduled_request.to_wire(),
    )
    assert scheduled_response.status_code == 200, scheduled_response.text
    scheduled_observation = scheduled_response.json()
    assert scheduled_observation["observationRequestId"].startswith("scheduled.")
    assert scheduled_observation["observedAt"] == "2026-10-04T09:01:00Z"
    replay_scheduled_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-operational-observations/trigger",
        json=scheduled_request.to_wire(),
    )
    assert replay_scheduled_response.status_code == 200
    assert replay_scheduled_response.json() == scheduled_observation

    stored_observation_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-operational-observations/"
        f"{observation['observationId']}/versions/"
        f"{observation['artifactRef']['version']}"
    )
    assert stored_observation_response.status_code == 200
    assert stored_observation_response.json() == observation

    async def dispatch_drift_probe(env_id, operation, payload):
        assert env_id == "sandbox-a"
        assert operation == "extensions"
        assert payload["op"] == "operational_probe"
        return {
            "state": "degraded",
            "schemaState": "in_sync",
            "indexState": "drifted",
            "reconciliationDivergenceCount": 0,
            "checks": [
                {
                    "checkId": check_id,
                    "state": "failed" if check_id == "managed_indexes" else "passed",
                    "detail": "The managed index contract changed."
                    if check_id == "managed_indexes"
                    else "The strategy check passed.",
                }
                for check_id in (
                    "runtime_connectivity",
                    "required_collections",
                    "storage_schema",
                    "managed_indexes",
                    "derived_projection",
                    "data_freshness",
                    "reconciliation_integrity",
                )
            ],
        }

    app.state.strategy_runtime.dispatch = dispatch_drift_probe
    configuration_drift_request = ImplementationPackageOperationalObservationRequest(
        contract="implementation-package-operational-observation-request/v1",
        observationRequestId="observe-clinical-release-index-drift",
        expectedDeploymentRef=deployment["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        observedByRef="team:clinical-platform-observability",
        observedAt=datetime(2026, 10, 4, 9, 2, tzinfo=timezone.utc),
    )
    configuration_drift_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-operational-observations/run",
        json=configuration_drift_request.to_wire(),
    )
    assert configuration_drift_response.status_code == 200
    configuration_drift = configuration_drift_response.json()
    assert configuration_drift["healthState"] == "degraded"
    assert configuration_drift["driftState"] == "in_sync"
    assert configuration_drift["configurationDriftState"] == "drifted"
    assert configuration_drift["nextRequiredAction"] == "reconcile_strategy_drift"
    assert configuration_drift["strategyProbes"][0]["indexState"] == "drifted"

    history_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-deployments/"
        f"{deployment['deploymentId']}/versions/"
        f"{deployment['artifactRef']['version']}/operational-observations"
    )
    assert history_response.status_code == 200, history_response.text
    history = history_response.json()
    assert history["contract"] == (
        "implementation-package-operational-observation-history/v1"
    )
    assert history["expectedDeploymentRef"] == deployment["artifactRef"]
    assert history["expectedPackageRef"] == package.artifact_ref.to_wire()
    assert history["totalCount"] == 3
    assert history["returnedCount"] == 3
    assert history["truncated"] is False
    assert [item["observationId"] for item in history["observations"]] == [
        configuration_drift["observationId"],
        scheduled_observation["observationId"],
        observation["observationId"],
    ]
    assert history["healthTrend"] == "degraded"
    assert history["configurationTrend"] == "degraded"

    bounded_history_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-deployments/"
        f"{deployment['deploymentId']}/versions/"
        f"{deployment['artifactRef']['version']}/operational-observations?limit=1"
    )
    assert bounded_history_response.status_code == 200
    bounded_history = bounded_history_response.json()
    assert bounded_history["totalCount"] == 3
    assert bounded_history["returnedCount"] == 1
    assert bounded_history["truncated"] is True
    assert bounded_history["healthTrend"] == "insufficient_history"

    wrong_observer = observation_request.to_wire()
    wrong_observer["observationRequestId"] = "observe-clinical-release-wrong-owner"
    wrong_observer["observedByRef"] = "team:not-the-monitoring-owner"
    wrong_observer_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-operational-observations/run",
        json=wrong_observer,
    )
    assert wrong_observer_response.status_code == 403
    assert wrong_observer_response.json()["error"]["code"] == (
        "IMPLEMENTATION_PACKAGE_OBSERVATION_OWNER_MISMATCH"
    )

    stored_deployment_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-deployments/"
        f"{deployment['deploymentId']}/versions/"
        f"{deployment['artifactRef']['version']}"
    )
    assert stored_deployment_response.status_code == 200
    assert stored_deployment_response.json() == deployment

    wrong_rollback_owner = ImplementationPackageRollbackRequest(
        contract="implementation-package-rollback-request/v1",
        rollbackRequestId="rollback-clinical-release-wrong-owner",
        deploymentRef=deployment["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        executedByRef="team:not-the-rollback-owner",
        approval="approved",
        reason="Exercise the authorization boundary.",
    )
    wrong_owner_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-deployments/rollback",
        json=wrong_rollback_owner.to_wire(),
    )
    assert wrong_owner_response.status_code == 403
    assert wrong_owner_response.json()["error"]["code"] == (
        "IMPLEMENTATION_PACKAGE_ROLLBACK_OWNER_MISMATCH"
    )

    rollback_request = ImplementationPackageRollbackRequest(
        contract="implementation-package-rollback-request/v1",
        rollbackRequestId="rollback-clinical-release-001",
        deploymentRef=deployment["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        executedByRef="team:clinical-platform-operations",
        approval="approved",
        reason="Rollback drill for the reviewed tenant release.",
    )
    rollback_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-deployments/rollback",
        json=rollback_request.to_wire(),
    )
    assert rollback_response.status_code == 200, rollback_response.text
    rollback = rollback_response.json()
    assert rollback["deploymentRef"] == deployment["artifactRef"]
    assert rollback["operationState"] == "succeeded"
    assert rollback["recordedEffect"] == "active_release_binding_reverted"
    assert rollback["nextRequiredAction"] == "review_new_release_candidate"

    replay_rollback_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-deployments/rollback",
        json=rollback_request.to_wire(),
    )
    assert replay_rollback_response.status_code == 200
    assert replay_rollback_response.json() == rollback

    rolled_back_status_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-deployments/active"
    )
    assert rolled_back_status_response.status_code == 200
    assert rolled_back_status_response.json()["state"] == "none"

    drift_request = ImplementationPackageOperationalObservationRequest(
        contract="implementation-package-operational-observation-request/v1",
        observationRequestId="observe-clinical-release-after-rollback",
        expectedDeploymentRef=deployment["artifactRef"],
        expectedPackageRef=package.artifact_ref,
        observedByRef="team:clinical-platform-observability",
        observedAt=datetime(2026, 10, 4, 9, 5, tzinfo=timezone.utc),
    )
    drift_response = client.post(
        "/v1/environments/sandbox-a/"
        "implementation-package-operational-observations/run",
        json=drift_request.to_wire(),
    )
    assert drift_response.status_code == 200, drift_response.text
    drift = drift_response.json()
    assert drift["healthState"] == "unavailable"
    assert drift["driftState"] == "release_absent"
    assert drift["nextRequiredAction"] == "deploy_release_candidate"
    assert "activeDeploymentRef" not in drift
    assert "activePackageRef" not in drift

    stored_rollback_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-rollbacks/"
        f"{rollback['rollbackId']}/versions/{rollback['artifactRef']['version']}"
    )
    assert stored_rollback_response.status_code == 200
    assert stored_rollback_response.json() == rollback

    stale_activation = activation_request.to_wire()
    stale_activation["activationInputs"][0]["configuration"]["database"] = (
        "different_database"
    )
    stale_activation_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-activations/run",
        json=stale_activation,
    )
    assert stale_activation_response.status_code == 409
    assert stale_activation_response.json()["error"]["code"] == (
        "IMPLEMENTATION_PACKAGE_APPROVAL_STALE"
    )

    unsafe = ready_request.to_wire()
    unsafe["activationInputs"][0]["configuration"]["password"] = "not-allowed"
    unsafe_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-preparation/readiness",
        json=unsafe,
    )
    assert unsafe_response.status_code == 422
    assert unsafe_response.json()["error"]["code"] == (
        "IMPLEMENTATION_PACKAGE_SECRET_IN_CONFIGURATION"
    )

    stored_readiness_response = client.get(
        "/v1/environments/sandbox-a/implementation-package-preparation-readiness/"
        f"{ready['readinessId']}/versions/{ready['artifactRef']['version']}"
    )
    assert stored_readiness_response.status_code == 200
    assert stored_readiness_response.json() == ready

    restarted_client = TestClient(create_app(str(registry_path)))
    stored_response = restarted_client.get(
        "/v1/environments/sandbox-a/implementation-package-installations/"
        f"{stage['installationId']}/versions/{stage['artifactRef']['version']}"
    )
    assert stored_response.status_code == 200
    assert stored_response.json() == stage
    stored_receipt_response = restarted_client.get(
        "/v1/environments/sandbox-a/implementation-package-materializations/"
        f"{receipt['materializationId']}/versions/{receipt['artifactRef']['version']}"
    )
    assert stored_receipt_response.status_code == 200
    assert stored_receipt_response.json() == receipt

    mismatched = installation_request.to_wire()
    mismatched["targetCapabilityProfileRef"]["digest"] = "sha256:" + "0" * 64
    mismatch_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-installations/stage",
        json=mismatched,
    )
    assert mismatch_response.status_code == 409
    assert mismatch_response.json()["error"]["code"] == (
        "IMPLEMENTATION_PACKAGE_TARGET_MISMATCH"
    )

    stale_materialization = materialization_request.to_wire()
    stale_materialization["expectedPackageRef"]["digest"] = "sha256:" + "0" * 64
    stale_response = client.post(
        "/v1/environments/sandbox-a/implementation-package-installations/materialize",
        json=stale_materialization,
    )
    assert stale_response.status_code == 409
    assert stale_response.json()["error"]["code"] == (
        "IMPLEMENTATION_PACKAGE_PRECONDITION_FAILED"
    )


def test_missing_package_proof_evidence_is_explicit(tmp_path):
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    response = client.get(
        "/v1/implementation-package-proof-evidence/missing/versions/1.0.0"
    )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == (
        "IMPLEMENTATION_PACKAGE_PROOF_EVIDENCE_NOT_FOUND"
    )
