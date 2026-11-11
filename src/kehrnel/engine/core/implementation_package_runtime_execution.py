"""Execute one query interaction from the exact activated package revision."""

from __future__ import annotations

import hashlib
import json
from uuid import uuid4

from fastapi.encoders import jsonable_encoder

from kehrnel.contracts.architecture_foundation import (
    AcknowledgementLevel,
    InteractionBehavior,
    RuntimeQueryPlanTrace,
)
from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_runtime_execution import (
    ImplementationPackageRuntimeExecutionReceipt,
    ImplementationPackageRuntimeExecutionRequest,
    ImplementationPackageRuntimeExecutionResponse,
    RuntimePolicyDecision,
    RuntimeResultEvidence,
    RuntimeWriteAcknowledgement,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.implementation_artifact_store import (
    FileImplementationArtifactStore,
    ImplementationArtifactNotFound,
)


class ImplementationPackageRuntimeExecutionError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _digest(payload) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _runtime_result_digest(payload) -> str:
    """Digest the JSON response boundary, including legitimate finite floats.

    Immutable architecture artifacts use the stricter canonical subset, which
    deliberately rejects floats. Runtime results can contain measurements and
    query timings, so evidence hashes the JSON-encoded response instead.
    """

    encoded = json.dumps(
        jsonable_encoder(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _result_type(result) -> str:
    if result is None:
        return "null"
    if isinstance(result, dict):
        return "object"
    if isinstance(result, list):
        return "array"
    return "scalar"


def _item_count(result) -> int | None:
    if isinstance(result, list):
        return len(result)
    if not isinstance(result, dict):
        return None
    for key in ("rows", "items", "resources", "entries", "entry"):
        value = result.get(key)
        if isinstance(value, list):
            return len(value)
    return None


async def execute_implementation_package_runtime_contract(
    environment_id: str,
    request: ImplementationPackageRuntimeExecutionRequest,
    *,
    store: FileImplementationArtifactStore,
    runtime,
) -> ImplementationPackageRuntimeExecutionResponse:
    """Execute one governed route and persist evidence without retaining result data."""

    preparation = store.get_preparation_execution(request.preparation_execution_ref)
    if preparation.environment_id != environment_id:
        raise ImplementationPackageRuntimeExecutionError(
            "IMPLEMENTATION_PACKAGE_RUNTIME_SCOPE_MISMATCH",
            "The preparation execution belongs to a different environment.",
            status=409,
        )
    if preparation.package_ref != request.expected_package_ref:
        raise ImplementationPackageRuntimeExecutionError(
            "IMPLEMENTATION_PACKAGE_RUNTIME_PRECONDITION_FAILED",
            "The prepared package differs from the expected package.",
            status=409,
        )

    activation = store.get_activation_receipt(preparation.activation_receipt_ref)
    if activation.environment_id != environment_id or (
        activation.package_ref != preparation.package_ref
    ):
        raise ImplementationPackageRuntimeExecutionError(
            "IMPLEMENTATION_PACKAGE_RUNTIME_ACTIVATION_MISMATCH",
            "The preparation receipt is not backed by this package activation.",
            status=409,
        )

    assembly = store.find_package_assembly(
        preparation.package_ref.id,
        preparation.package_ref.version,
    )
    resolution = assembly.resolution
    blueprint = assembly.package_build_request.resolution_request.blueprint
    runtime_plan = resolution.runtime_plan
    interaction = next(
        (
            candidate
            for candidate in blueprint.interactions
            if candidate.interaction_id == request.interaction_id
        ),
        None,
    )
    route = next(
        (
            candidate
            for candidate in runtime_plan.routes
            if candidate.interaction_id == request.interaction_id
        ),
        None,
    )
    if route is None:
        raise ImplementationPackageRuntimeExecutionError(
            "IMPLEMENTATION_PACKAGE_RUNTIME_ROUTE_NOT_FOUND",
            f"The package has no runtime route for interaction '{request.interaction_id}'.",
            status=404,
        )
    if interaction is None or interaction.behavior is not route.behavior:
        raise ImplementationPackageRuntimeExecutionError(
            "IMPLEMENTATION_PACKAGE_RUNTIME_INTERACTION_MISMATCH",
            "The runtime route does not match its reviewed interaction contract.",
            status=409,
        )
    supported_write = route.behavior in {
        InteractionBehavior.CREATE,
        InteractionBehavior.UPDATE,
    }
    supported_query = route.behavior in {
        InteractionBehavior.QUERY,
        InteractionBehavior.SEARCH,
    }
    if not supported_query and not supported_write:
        raise ImplementationPackageRuntimeExecutionError(
            "IMPLEMENTATION_PACKAGE_RUNTIME_BEHAVIOR_NOT_SUPPORTED",
            "This runtime executes governed query, search, create and update interactions only.",
        )

    plan_family = None
    if supported_query:
        plan_family = next(
            (
                candidate
                for candidate in resolution.resolved_plan.query_plan_families
                if candidate.plan_family_id == route.plan_family_id
            ),
            None,
        )
        if plan_family is None or (
            request.interaction_id not in plan_family.interaction_ids
            or plan_family.strategy_instance_id != route.strategy_instance_id
            or plan_family.semantic_binding_ref != route.semantic_binding_ref
        ):
            raise ImplementationPackageRuntimeExecutionError(
                "IMPLEMENTATION_PACKAGE_RUNTIME_PLAN_MISMATCH",
                "The selected route is not backed by a coherent query-plan family.",
                status=409,
            )

    strategy_activation = next(
        (
            candidate
            for candidate in activation.strategy_activations
            if candidate.strategy_instance_id == route.strategy_instance_id
        ),
        None,
    )
    if strategy_activation is None:
        raise ImplementationPackageRuntimeExecutionError(
            "IMPLEMENTATION_PACKAGE_RUNTIME_STRATEGY_NOT_ACTIVE",
            "The route's exact strategy instance is not active for this package.",
            status=409,
        )

    authority_policy = None
    semantic_authority = None
    if supported_write:
        authority_policy = next(
            (
                candidate
                for candidate in blueprint.record_authority_policies
                if candidate.scope.scope_id == interaction.semantic_scope_id
            ),
            None,
        )
        semantic_authority = next(
            (
                candidate
                for candidate in blueprint.semantic_authorities
                if candidate.scope_id == interaction.semantic_scope_id
            ),
            None,
        )
        requester_ref = interaction.roles.requester_ref
        if (
            authority_policy is None
            or semantic_authority is None
            or requester_ref not in authority_policy.permitted_writer_refs
        ):
            raise ImplementationPackageRuntimeExecutionError(
                "IMPLEMENTATION_PACKAGE_RUNTIME_AUTHORITY_DENIED",
                "The write is not backed by a coherent semantic and record authority policy.",
                status=403,
            )
        if interaction.guarantees.idempotency_required and not request.idempotency_key:
            raise ImplementationPackageRuntimeExecutionError(
                "IMPLEMENTATION_PACKAGE_RUNTIME_IDEMPOTENCY_REQUIRED",
                "This interaction requires an idempotencyKey.",
            )
        if (
            interaction.guarantees.acknowledgement
            is not AcknowledgementLevel.DURABLE_AUTHORITATIVE
        ):
            raise ImplementationPackageRuntimeExecutionError(
                "IMPLEMENTATION_PACKAGE_RUNTIME_WRITE_GUARANTEE_UNSUPPORTED",
                "Authoritative writes require durable_authoritative acknowledgement.",
            )

    request_digest = _digest(request.request_payload)
    if supported_write and request.idempotency_key:
        idempotency_material = {
            "environmentId": environment_id,
            "packageRef": preparation.package_ref.to_wire(),
            "interactionId": request.interaction_id,
            "idempotencyKey": request.idempotency_key,
        }
        execution_id = (
            "package-runtime-execution."
            + _digest(idempotency_material).split(":", 1)[1]
        )
        try:
            existing = store.find_runtime_execution(
                execution_id,
                preparation.package_ref.version,
            )
        except ImplementationArtifactNotFound:
            existing = None
        if existing is not None:
            acknowledgement = existing.write_acknowledgement
            if acknowledgement is None or acknowledgement.request_digest != request_digest:
                raise ImplementationPackageRuntimeExecutionError(
                    "IMPLEMENTATION_PACKAGE_RUNTIME_IDEMPOTENCY_CONFLICT",
                    "The idempotency key is already bound to a different request.",
                    status=409,
                )
            replayed = acknowledgement.model_copy(update={"replayed": True})
            return ImplementationPackageRuntimeExecutionResponse(
                contract="implementation-package-runtime-execution-response/v1",
                receipt=existing,
                result={"writeAcknowledgement": replayed.to_wire()},
                resultPersistence="not_persisted",
            )
    else:
        execution_id = "package-runtime-execution." + uuid4().hex
    policy_payload = {
        "decision": "allow",
        "scope": "execute_activated_package_interaction",
        "packageRef": preparation.package_ref.to_wire(),
        "preparationExecutionRef": preparation.artifact_ref.to_wire(),
        "interactionId": request.interaction_id,
    }
    policy_digest = _digest(policy_payload)
    policy_decision = RuntimePolicyDecision(
        **policy_payload,
        artifactRef=ArtifactRef(
            kind="policy-decision",
            id=f"{execution_id}.policy",
            version=preparation.package_ref.version,
            digest=policy_digest,
        ),
        decisionDigest=policy_digest,
    )

    activation_precondition = {
        "activationId": strategy_activation.runtime_activation_id,
        "strategyId": strategy_activation.strategy_ref.id,
    }
    try:
        if supported_query:
            result = await runtime.dispatch(
                environment_id,
                "query",
                {
                    "domain": strategy_activation.domain,
                    "query": request.request_payload,
                    "__activation_precondition": activation_precondition,
                },
            )
        else:
            result = await runtime.dispatch(
                environment_id,
                "authoritative_write",
                {
                    "domain": strategy_activation.domain,
                    "behavior": route.behavior.value,
                    "requestPayload": request.request_payload,
                    "idempotencyKey": request.idempotency_key,
                    "interactionId": request.interaction_id,
                    "semanticScopeId": interaction.semantic_scope_id,
                    "requesterRef": interaction.roles.requester_ref,
                    "providerRef": interaction.roles.provider_ref,
                    "recordAuthorityPolicyRef": authority_policy.artifact_ref.to_wire(),
                    "__activation_precondition": activation_precondition,
                },
            )
    except Exception as exc:
        raise ImplementationPackageRuntimeExecutionError(
            "IMPLEMENTATION_PACKAGE_RUNTIME_EXECUTION_FAILED",
            f"Runtime execution failed for interaction '{request.interaction_id}': {exc}",
            status=getattr(exc, "status", 502),
        ) from exc

    result_digest = _runtime_result_digest(result)
    trace = None
    write_acknowledgement = None
    if supported_query:
        trace_payload = {
            "contract": "runtime-query-plan-trace/v1",
            "traceId": f"query-trace.{uuid4().hex}",
            "resolvedPlanRef": resolution.resolved_plan.artifact_ref.to_wire(),
            "planFamilyId": plan_family.plan_family_id,
            "interactionId": request.interaction_id,
            "semanticBindingRef": route.semantic_binding_ref.to_wire(),
            "targetCapabilityProfileRef": runtime_plan.target_capability_profile_ref.to_wire(),
            "queryFingerprint": request_digest,
            "selectedMechanism": plan_family.mechanism,
            "fallbackUsed": False,
            "policyDecisionRef": policy_decision.artifact_ref.to_wire(),
            "executorRef": route.executor_ref.to_wire(),
        }
        trace = RuntimeQueryPlanTrace(
            **trace_payload,
            traceDigest=_digest(trace_payload),
        )
    else:
        if not isinstance(result, dict):
            raise ImplementationPackageRuntimeExecutionError(
                "IMPLEMENTATION_PACKAGE_RUNTIME_WRITE_ACK_INVALID",
                "The strategy did not return a structured authoritative-write acknowledgement.",
                status=502,
            )
        required_result_fields = {
            "resourceType",
            "versionUid",
            "changeType",
            "etag",
        }
        missing_result_fields = sorted(required_result_fields - set(result))
        resource_id = result.get("resourceId") or result.get("ehrId")
        if not isinstance(resource_id, str) or not resource_id:
            missing_result_fields.append("resourceId")
        if missing_result_fields:
            raise ImplementationPackageRuntimeExecutionError(
                "IMPLEMENTATION_PACKAGE_RUNTIME_WRITE_ACK_INVALID",
                "The strategy write acknowledgement is incomplete: "
                + ", ".join(missing_result_fields),
                status=502,
            )
        write_acknowledgement = RuntimeWriteAcknowledgement(
            behavior=route.behavior,
            semanticAuthorityRef=semantic_authority.authority_ref,
            recordAuthorityPolicyRef=authority_policy.artifact_ref,
            requesterRef=interaction.roles.requester_ref,
            providerRef=interaction.roles.provider_ref,
            acknowledgement=interaction.guarantees.acknowledgement,
            consistency=interaction.guarantees.consistency,
            requestDigest=request_digest,
            idempotencyKeyDigest=_digest(request.idempotency_key),
            resourceType=result["resourceType"],
            resourceId=resource_id,
            versionUid=result["versionUid"],
            changeType=result["changeType"],
            etag=result["etag"],
            replayed=False,
        )
    result_evidence = RuntimeResultEvidence(
        resultDigest=result_digest,
        resultType=_result_type(result),
        itemCount=_item_count(result),
        containsResultPayload=False,
    )
    receipt_payload = {
        "contract": "implementation-package-runtime-execution-receipt/v1",
        "executionId": execution_id,
        "environmentId": environment_id,
        "packageRef": preparation.package_ref.to_wire(),
        "preparationExecutionRef": preparation.artifact_ref.to_wire(),
        "resolvedPlanRef": resolution.resolved_plan.artifact_ref.to_wire(),
        "runtimePlanRef": runtime_plan.artifact_ref.to_wire(),
        "interactionId": request.interaction_id,
        "behavior": route.behavior.value,
        "routeId": route.route_id,
        "strategyInstanceId": route.strategy_instance_id,
        "policyDecision": policy_decision.to_wire(),
        "resultEvidence": result_evidence.to_wire(),
        "state": "executed",
        "runtimeExecuted": True,
        "evidencePersistence": "durable",
        "resultPersistence": "not_persisted",
        "nextRequiredAction": "review_runtime_evidence",
    }
    if trace is not None:
        receipt_payload["queryPlanTrace"] = trace.to_wire()
    if write_acknowledgement is not None:
        receipt_payload["writeAcknowledgement"] = write_acknowledgement.to_wire()
    execution_digest = _digest(receipt_payload)
    receipt = ImplementationPackageRuntimeExecutionReceipt(
        **receipt_payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-runtime-execution-receipt",
            id=execution_id,
            version=preparation.package_ref.version,
            digest=execution_digest,
        ),
        executionDigest=execution_digest,
    )
    store.put_runtime_execution(receipt)
    return ImplementationPackageRuntimeExecutionResponse(
        contract="implementation-package-runtime-execution-response/v1",
        receipt=receipt,
        result=result,
        resultPersistence="not_persisted",
    )


__all__ = [
    "ImplementationPackageRuntimeExecutionError",
    "execute_implementation_package_runtime_contract",
]
