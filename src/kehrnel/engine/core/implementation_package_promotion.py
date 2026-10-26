"""Approve a proven implementation package as a tenant release candidate."""

from __future__ import annotations

import hashlib

from kehrnel.contracts.architecture_foundation import InteractionBehavior
from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_promotion import (
    ImplementationPackagePromotionReceipt,
    ImplementationPackagePromotionRequest,
    InteractionPromotionEvidence,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.implementation_artifact_store import (
    FileImplementationArtifactStore,
    ImplementationArtifactNotFound,
)


class ImplementationPackagePromotionError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _digest(payload) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _promotion_id(environment_id: str, payload: dict) -> str:
    identity = _digest({"environmentId": environment_id, **payload})
    return "package-promotion." + identity.removeprefix("sha256:")


def promote_implementation_package(
    environment_id: str,
    request: ImplementationPackagePromotionRequest,
    *,
    store: FileImplementationArtifactStore,
) -> ImplementationPackagePromotionReceipt:
    """Promote only when every executable interaction has durable evidence.

    Promotion is deliberately not deployment. It records the exact package,
    preparation execution and runtime receipts a human approved, so an
    operational workflow can subsequently deploy that immutable candidate.
    """

    preparation = store.get_preparation_execution(request.preparation_execution_ref)
    if preparation.environment_id != environment_id:
        raise ImplementationPackagePromotionError(
            "IMPLEMENTATION_PACKAGE_PROMOTION_SCOPE_MISMATCH",
            "The preparation execution belongs to a different environment.",
            status=409,
        )
    if preparation.package_ref != request.expected_package_ref:
        raise ImplementationPackagePromotionError(
            "IMPLEMENTATION_PACKAGE_PROMOTION_PRECONDITION_FAILED",
            "The prepared package differs from the expected package.",
            status=409,
        )

    assembly = store.find_package_assembly(
        preparation.package_ref.id,
        preparation.package_ref.version,
    )
    if assembly.package_ref != preparation.package_ref:
        raise ImplementationPackagePromotionError(
            "IMPLEMENTATION_PACKAGE_ASSEMBLY_MISMATCH",
            "The stored package assembly belongs to a different package.",
            status=409,
        )

    executable_behaviors = {
        InteractionBehavior.QUERY,
        InteractionBehavior.SEARCH,
        InteractionBehavior.CREATE,
        InteractionBehavior.UPDATE,
    }
    required_routes = {
        route.interaction_id: route
        for route in assembly.resolution.runtime_plan.routes
        if route.behavior in executable_behaviors
    }
    if not required_routes:
        raise ImplementationPackagePromotionError(
            "IMPLEMENTATION_PACKAGE_PROMOTION_EVIDENCE_INCOMPLETE",
            "The package has no executable interactions to prove.",
        )

    receipts = []
    for reference in request.runtime_evidence_refs:
        receipt = store.get_runtime_execution(reference)
        if receipt.environment_id != environment_id:
            raise ImplementationPackagePromotionError(
                "IMPLEMENTATION_PACKAGE_PROMOTION_SCOPE_MISMATCH",
                "Runtime evidence belongs to a different environment.",
                status=409,
            )
        if (
            receipt.package_ref != preparation.package_ref
            or receipt.preparation_execution_ref != preparation.artifact_ref
        ):
            raise ImplementationPackagePromotionError(
                "IMPLEMENTATION_PACKAGE_PROMOTION_EVIDENCE_MISMATCH",
                "Runtime evidence does not prove this prepared package revision.",
                status=409,
            )
        route = required_routes.get(receipt.interaction_id)
        if route is None or route.behavior != receipt.behavior:
            raise ImplementationPackagePromotionError(
                "IMPLEMENTATION_PACKAGE_PROMOTION_EVIDENCE_MISMATCH",
                "Runtime evidence does not match an executable package interaction.",
                status=409,
            )
        receipts.append(receipt)

    receipts_by_interaction = {receipt.interaction_id: receipt for receipt in receipts}
    if len(receipts_by_interaction) != len(receipts):
        raise ImplementationPackagePromotionError(
            "IMPLEMENTATION_PACKAGE_PROMOTION_EVIDENCE_MISMATCH",
            "Only one runtime receipt may prove each interaction.",
            status=409,
        )
    missing = sorted(set(required_routes) - set(receipts_by_interaction))
    if missing:
        raise ImplementationPackagePromotionError(
            "IMPLEMENTATION_PACKAGE_PROMOTION_EVIDENCE_INCOMPLETE",
            "Runtime evidence is missing for interactions: " + ", ".join(missing),
        )

    interaction_evidence = []
    for interaction_id in sorted(required_routes):
        receipt = receipts_by_interaction[interaction_id]
        interaction_evidence.append(
            InteractionPromotionEvidence(
                interactionId=interaction_id,
                behavior=receipt.behavior,
                runtimeExecutionRef=receipt.artifact_ref,
                evidenceKind=(
                    "query_plan_trace"
                    if receipt.behavior
                    in {InteractionBehavior.QUERY, InteractionBehavior.SEARCH}
                    else "write_acknowledgement"
                ),
                resultDigest=receipt.result_evidence.result_digest,
            )
        )

    runtime_refs = [item.runtime_execution_ref for item in interaction_evidence]
    identity_payload = {
        "packageRef": preparation.package_ref.to_wire(),
        "preparationExecutionRef": preparation.artifact_ref.to_wire(),
        "runtimeEvidenceRefs": [ref.to_wire() for ref in runtime_refs],
        "approval": request.approval,
        "releaseNotes": request.release_notes,
    }
    promotion_id = _promotion_id(environment_id, identity_payload)
    try:
        return store.find_promotion(promotion_id, preparation.package_ref.version)
    except ImplementationArtifactNotFound:
        pass

    payload = {
        "contract": "implementation-package-promotion-receipt/v1",
        "promotionId": promotion_id,
        "environmentId": environment_id,
        **identity_payload,
        "interactionEvidence": [item.to_wire() for item in interaction_evidence],
        "state": "promoted",
        "promotionScope": "tenant_release_candidate",
        "deploymentState": "not_deployed",
        "evidencePersistence": "durable",
        "nextRequiredAction": "review_operational_readiness",
    }
    promotion_digest = _digest(payload)
    receipt = ImplementationPackagePromotionReceipt(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-promotion-receipt",
            id=promotion_id,
            version=preparation.package_ref.version,
            digest=promotion_digest,
        ),
        promotionDigest=promotion_digest,
    )
    store.put_promotion(receipt)
    return receipt


__all__ = [
    "ImplementationPackagePromotionError",
    "promote_implementation_package",
]
