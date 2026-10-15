"""Deploy and roll back exact operationally-ready package release bindings."""

from __future__ import annotations

import hashlib

from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_deployment import (
    ImplementationPackageDeploymentReceipt,
    ImplementationPackageDeploymentRequest,
    ImplementationPackageDeploymentStatus,
    ImplementationPackageRollbackReceipt,
    ImplementationPackageRollbackRequest,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.implementation_artifact_store import (
    FileImplementationArtifactStore,
    ImplementationArtifactNotFound,
)


class ImplementationPackageDeploymentError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _digest(payload) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _operation_id(prefix: str, environment_id: str, request_id: str, ref: ArtifactRef) -> str:
    identity = _digest(
        {
            "environmentId": environment_id,
            "requestId": request_id,
            "sourceRef": ref.to_wire(),
        }
    )
    return f"{prefix}." + identity.removeprefix("sha256:")


def deploy_implementation_package(
    environment_id: str,
    request: ImplementationPackageDeploymentRequest,
    *,
    store: FileImplementationArtifactStore,
) -> ImplementationPackageDeploymentReceipt:
    """Bind one ready package as the active Kehrnel release for an environment."""

    readiness = store.get_operational_readiness(request.readiness_ref)
    if readiness.environment_id != environment_id:
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_DEPLOYMENT_SCOPE_MISMATCH",
            "The readiness review belongs to a different environment.",
            status=409,
        )
    if readiness.package_ref != request.expected_package_ref:
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_DEPLOYMENT_PRECONDITION_FAILED",
            "The operationally-ready package differs from the expected package.",
            status=409,
        )
    if readiness.state != "ready" or readiness.deployment_state != "not_deployed":
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_NOT_OPERATIONALLY_READY",
            "Only an approved, undeployed operational-readiness receipt can be deployed.",
        )

    promotion = store.get_promotion(readiness.promotion_ref)
    if (
        promotion.environment_id != environment_id
        or promotion.package_ref != readiness.package_ref
        or promotion.state != "promoted"
    ):
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_DEPLOYMENT_PROMOTION_MISMATCH",
            "The readiness review no longer resolves to the exact promoted package.",
            status=409,
        )
    preparation = store.get_preparation_execution(
        promotion.preparation_execution_ref
    )
    if (
        preparation.environment_id != environment_id
        or preparation.package_ref != readiness.package_ref
        or preparation.state != "prepared"
    ):
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_DEPLOYMENT_PREPARATION_MISMATCH",
            "The promoted package does not have matching completed preparation evidence.",
            status=409,
        )
    for reference in readiness.runtime_evidence_refs:
        evidence = store.get_runtime_execution(reference)
        if (
            evidence.environment_id != environment_id
            or evidence.package_ref != readiness.package_ref
        ):
            raise ImplementationPackageDeploymentError(
                "IMPLEMENTATION_PACKAGE_DEPLOYMENT_EVIDENCE_MISMATCH",
                "Runtime evidence does not belong to this package and environment.",
                status=409,
            )

    deployment_id = _operation_id(
        "package-deployment",
        environment_id,
        request.deployment_request_id,
        readiness.artifact_ref,
    )
    current = store.get_active_deployment_ref(environment_id)
    replaying_current_request = (
        current is not None
        and current.id == deployment_id
        and current.version == readiness.package_ref.version
    )
    if current != request.expected_current_deployment_ref and not replaying_current_request:
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_ACTIVE_RELEASE_CHANGED",
            "The active deployment changed after review. Refresh before deploying.",
            status=409,
        )
    if replaying_current_request:
        existing = store.get_deployment(current)
        if (
            existing.readiness_ref == readiness.artifact_ref
            and existing.package_ref == readiness.package_ref
            and existing.deployment_owner_ref == request.deployment_owner_ref
            and existing.change_ref == request.change_ref
            and existing.deployment_notes == request.deployment_notes
        ):
            return existing
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_DEPLOYMENT_IDEMPOTENCY_CONFLICT",
            "The deployment request identifier was already used with different inputs.",
            status=409,
        )
    owners = (
        readiness.operational_owner_ref,
        readiness.monitoring_owner_ref,
        readiness.rollback_owner_ref,
    )
    if any(owner is None for owner in owners):
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_DEPLOYMENT_OWNERSHIP_INCOMPLETE",
            "Operational, monitoring and rollback owners must be assigned.",
        )

    payload = {
        "contract": "implementation-package-deployment-receipt/v1",
        "deploymentId": deployment_id,
        "deploymentRequestId": request.deployment_request_id,
        "environmentId": environment_id,
        "readinessRef": readiness.artifact_ref.to_wire(),
        "promotionRef": promotion.artifact_ref.to_wire(),
        "packageRef": readiness.package_ref.to_wire(),
        "preparationExecutionRef": preparation.artifact_ref.to_wire(),
        **(
            {"previousDeploymentRef": current.to_wire()}
            if current is not None
            else {}
        ),
        "deploymentOwnerRef": request.deployment_owner_ref,
        "operationalOwnerRef": readiness.operational_owner_ref,
        "monitoringOwnerRef": readiness.monitoring_owner_ref,
        "rollbackOwnerRef": readiness.rollback_owner_ref,
        **({"changeRef": request.change_ref} if request.change_ref else {}),
        **(
            {"deploymentNotes": request.deployment_notes}
            if request.deployment_notes
            else {}
        ),
        "operationState": "succeeded",
        "deploymentScope": "tenant_environment_release",
        "recordedEffect": "active_release_binding_created",
        "trafficManagement": "external",
        "evidencePersistence": "durable",
        "nextRequiredAction": "monitor_release",
    }
    deployment_digest = _digest(payload)
    receipt = ImplementationPackageDeploymentReceipt(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-deployment-receipt",
            id=deployment_id,
            version=readiness.package_ref.version,
            digest=deployment_digest,
        ),
        deploymentDigest=deployment_digest,
    )
    store.bind_active_deployment(
        receipt,
        expected_current_ref=request.expected_current_deployment_ref,
    )
    return receipt


def rollback_implementation_package(
    environment_id: str,
    request: ImplementationPackageRollbackRequest,
    *,
    store: FileImplementationArtifactStore,
) -> ImplementationPackageRollbackReceipt:
    """Revert the active release binding to its recorded predecessor, if any."""

    deployment = store.get_deployment(request.deployment_ref)
    if deployment.environment_id != environment_id:
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_ROLLBACK_SCOPE_MISMATCH",
            "The deployment belongs to a different environment.",
            status=409,
        )
    if deployment.package_ref != request.expected_package_ref:
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_ROLLBACK_PRECONDITION_FAILED",
            "The deployed package differs from the expected package.",
            status=409,
        )
    if request.executed_by_ref != deployment.rollback_owner_ref:
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_ROLLBACK_OWNER_MISMATCH",
            "Rollback must be executed by the owner recorded in operational readiness.",
            status=403,
        )
    rollback_id = _operation_id(
        "package-rollback",
        environment_id,
        request.rollback_request_id,
        deployment.artifact_ref,
    )
    try:
        existing = store.find_rollback(rollback_id, deployment.package_ref.version)
    except ImplementationArtifactNotFound:
        existing = None
    if existing is not None:
        if (
            existing.deployment_ref == deployment.artifact_ref
            and existing.package_ref == deployment.package_ref
            and existing.executed_by_ref == request.executed_by_ref
            and existing.reason == request.reason
        ):
            return existing
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_ROLLBACK_IDEMPOTENCY_CONFLICT",
            "The rollback request identifier was already used with different inputs.",
            status=409,
        )

    active = store.get_active_deployment_ref(environment_id)
    if active != deployment.artifact_ref:
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_DEPLOYMENT_NOT_ACTIVE",
            "Only the currently active release binding can be rolled back.",
            status=409,
        )

    restored = None
    if deployment.previous_deployment_ref is not None:
        restored = store.get_deployment(deployment.previous_deployment_ref)
        if restored.environment_id != environment_id:
            raise ImplementationPackageDeploymentError(
                "IMPLEMENTATION_PACKAGE_ROLLBACK_TARGET_MISMATCH",
                "The recorded previous deployment belongs to another environment.",
                status=409,
            )

    payload = {
        "contract": "implementation-package-rollback-receipt/v1",
        "rollbackId": rollback_id,
        "rollbackRequestId": request.rollback_request_id,
        "environmentId": environment_id,
        "deploymentRef": deployment.artifact_ref.to_wire(),
        "packageRef": deployment.package_ref.to_wire(),
        **(
            {
                "restoredDeploymentRef": restored.artifact_ref.to_wire(),
                "restoredPackageRef": restored.package_ref.to_wire(),
            }
            if restored is not None
            else {}
        ),
        "executedByRef": request.executed_by_ref,
        "rollbackOwnerRef": deployment.rollback_owner_ref,
        "reason": request.reason,
        "operationState": "succeeded",
        "deploymentScope": "tenant_environment_release",
        "recordedEffect": "active_release_binding_reverted",
        "trafficManagement": "external",
        "evidencePersistence": "durable",
        "nextRequiredAction": (
            "monitor_restored_release"
            if restored is not None
            else "review_new_release_candidate"
        ),
    }
    rollback_digest = _digest(payload)
    receipt = ImplementationPackageRollbackReceipt(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-rollback-receipt",
            id=rollback_id,
            version=deployment.package_ref.version,
            digest=rollback_digest,
        ),
        rollbackDigest=rollback_digest,
    )
    store.rollback_active_deployment(
        receipt,
        expected_active_ref=deployment.artifact_ref,
        restored_deployment_ref=(
            restored.artifact_ref if restored is not None else None
        ),
    )
    return receipt


def get_implementation_package_deployment_status(
    environment_id: str,
    *,
    store: FileImplementationArtifactStore,
) -> ImplementationPackageDeploymentStatus:
    active = store.get_active_deployment(environment_id)
    if active is None:
        return ImplementationPackageDeploymentStatus(
            contract="implementation-package-deployment-status/v1",
            environmentId=environment_id,
            state="none",
            nextRequiredAction="deploy_ready_release_candidate",
        )
    if active.environment_id != environment_id:
        raise ImplementationPackageDeploymentError(
            "IMPLEMENTATION_PACKAGE_ACTIVE_RELEASE_SCOPE_MISMATCH",
            "The active release binding belongs to a different environment.",
            status=409,
        )
    return ImplementationPackageDeploymentStatus(
        contract="implementation-package-deployment-status/v1",
        environmentId=environment_id,
        state="active",
        activeDeploymentRef=active.artifact_ref,
        activePackageRef=active.package_ref,
        activeReadinessRef=active.readiness_ref,
        nextRequiredAction="monitor_or_rollback",
    )


__all__ = [
    "ImplementationPackageDeploymentError",
    "deploy_implementation_package",
    "get_implementation_package_deployment_status",
    "rollback_implementation_package",
]
