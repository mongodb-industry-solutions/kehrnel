"""Assess whether a promoted package is ready to enter deployment."""

from __future__ import annotations

import hashlib

from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_operational_readiness import (
    ImplementationPackageOperationalReadinessReceipt,
    ImplementationPackageOperationalReadinessRequest,
    OperationalReadinessCheck,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.implementation_artifact_store import (
    FileImplementationArtifactStore,
    ImplementationArtifactNotFound,
)


class ImplementationPackageOperationalReadinessError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _digest(payload) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _readiness_id(environment_id: str, payload: dict) -> str:
    identity = _digest({"environmentId": environment_id, **payload})
    return "package-operational-readiness." + identity.removeprefix("sha256:")


def _check(
    check_id: str,
    passed: bool,
    passed_detail: str,
    blocked_detail: str,
    *,
    evidence_ref=None,
):
    return OperationalReadinessCheck(
        checkId=check_id,
        state="passed" if passed else "blocked",
        detail=passed_detail if passed else blocked_detail,
        evidenceRef=evidence_ref,
    )


def assess_implementation_package_operational_readiness(
    environment_id: str,
    request: ImplementationPackageOperationalReadinessRequest,
    *,
    store: FileImplementationArtifactStore,
) -> ImplementationPackageOperationalReadinessReceipt:
    """Create a durable, non-deploying operational review of one promotion."""

    promotion = store.get_promotion(request.promotion_ref)
    if promotion.environment_id != environment_id:
        raise ImplementationPackageOperationalReadinessError(
            "IMPLEMENTATION_PACKAGE_OPERATIONAL_READINESS_SCOPE_MISMATCH",
            "The promotion belongs to a different environment.",
            status=409,
        )
    if promotion.package_ref != request.expected_package_ref:
        raise ImplementationPackageOperationalReadinessError(
            "IMPLEMENTATION_PACKAGE_OPERATIONAL_READINESS_PRECONDITION_FAILED",
            "The promoted package differs from the expected package.",
            status=409,
        )

    # Re-read the referenced runtime receipts so a missing or corrupt durable
    # artifact fails closed instead of being treated as operations-ready.
    for reference in promotion.runtime_evidence_refs:
        evidence = store.get_runtime_execution(reference)
        if (
            evidence.environment_id != environment_id
            or evidence.package_ref != promotion.package_ref
        ):
            raise ImplementationPackageOperationalReadinessError(
                "IMPLEMENTATION_PACKAGE_OPERATIONAL_READINESS_EVIDENCE_MISMATCH",
                "Promotion evidence does not belong to this package and environment.",
                status=409,
            )

    checks = [
        _check(
            "promotion_integrity",
            promotion.state == "promoted"
            and promotion.deployment_state == "not_deployed",
            "The immutable tenant release candidate is approved and not deployed.",
            "The referenced artifact is not an undeployed approved release candidate.",
            evidence_ref=promotion.artifact_ref,
        ),
        _check(
            "runtime_evidence_available",
            bool(promotion.runtime_evidence_refs),
            "All promoted runtime evidence remains durably available.",
            "Durable runtime evidence is missing.",
            evidence_ref=promotion.artifact_ref,
        ),
        _check(
            "operational_owner_assigned",
            bool(request.operational_owner_ref),
            "An operational owner is assigned.",
            "Assign an operational owner before deployment.",
        ),
        _check(
            "monitoring_owner_assigned",
            bool(request.monitoring_owner_ref),
            "A monitoring owner is assigned.",
            "Assign a monitoring owner before deployment.",
        ),
        _check(
            "rollback_owner_assigned",
            bool(request.rollback_owner_ref),
            "A rollback owner is assigned.",
            "Assign a rollback owner before deployment.",
        ),
        _check(
            "operational_review_approved",
            request.review_decision == "approved",
            "The operator approved this exact candidate for deployment.",
            "The operational review still requires changes.",
        ),
    ]
    state = (
        "ready" if all(check.state == "passed" for check in checks) else "blocked"
    )
    identity_payload = {
        "promotionRef": promotion.artifact_ref.to_wire(),
        "packageRef": promotion.package_ref.to_wire(),
        "runtimeEvidenceRefs": [ref.to_wire() for ref in promotion.runtime_evidence_refs],
        "operationalOwnerRef": request.operational_owner_ref,
        "monitoringOwnerRef": request.monitoring_owner_ref,
        "rollbackOwnerRef": request.rollback_owner_ref,
        "reviewDecision": request.review_decision,
        "reviewNotes": request.review_notes,
        "checks": [check.to_wire() for check in checks],
    }
    readiness_id = _readiness_id(environment_id, identity_payload)
    try:
        return store.find_operational_readiness(
            readiness_id,
            promotion.package_ref.version,
        )
    except ImplementationArtifactNotFound:
        pass

    payload = {
        "contract": "implementation-package-operational-readiness-receipt/v1",
        "readinessId": readiness_id,
        "environmentId": environment_id,
        **identity_payload,
        "state": state,
        "deploymentState": "not_deployed",
        "evidencePersistence": "durable",
        "nextRequiredAction": (
            "deploy_release_candidate"
            if state == "ready"
            else "provide_operational_readiness_inputs"
        ),
    }
    readiness_digest = _digest(payload)
    receipt = ImplementationPackageOperationalReadinessReceipt(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-operational-readiness-receipt",
            id=readiness_id,
            version=promotion.package_ref.version,
            digest=readiness_digest,
        ),
        readinessDigest=readiness_digest,
    )
    store.put_operational_readiness(receipt)
    return receipt


__all__ = [
    "ImplementationPackageOperationalReadinessError",
    "assess_implementation_package_operational_readiness",
]
