"""Contracts for approving an exact, proven implementation package revision."""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import Field, model_validator

from .architecture_foundation import InteractionBehavior
from .artifacts import ArtifactRef, ContractModel, Sha256Digest


class ImplementationPackagePromotionRequest(ContractModel):
    contract: Literal["implementation-package-promotion-request/v1"]
    preparation_execution_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    runtime_evidence_refs: List[ArtifactRef] = Field(min_length=1)
    approval: Literal["approved"]
    release_notes: Optional[str] = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ImplementationPackagePromotionRequest":
        if self.preparation_execution_ref.kind != (
            "implementation-package-preparation-execution-receipt"
        ):
            raise ValueError(
                "preparationExecutionRef.kind must be "
                "'implementation-package-preparation-execution-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        if any(
            ref.kind != "implementation-package-runtime-execution-receipt"
            for ref in self.runtime_evidence_refs
        ):
            raise ValueError(
                "runtimeEvidenceRefs must contain only "
                "implementation-package-runtime-execution-receipt references"
            )
        identities = [
            (ref.kind, ref.id, ref.version, ref.digest)
            for ref in self.runtime_evidence_refs
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("runtimeEvidenceRefs must be unique")
        return self


class InteractionPromotionEvidence(ContractModel):
    interaction_id: str = Field(min_length=1)
    behavior: InteractionBehavior
    runtime_execution_ref: ArtifactRef
    evidence_kind: Literal["query_plan_trace", "write_acknowledgement"]
    result_digest: Sha256Digest

    @model_validator(mode="after")
    def evidence_is_coherent(self) -> "InteractionPromotionEvidence":
        if self.runtime_execution_ref.kind != (
            "implementation-package-runtime-execution-receipt"
        ):
            raise ValueError(
                "runtimeExecutionRef.kind must be "
                "'implementation-package-runtime-execution-receipt'"
            )
        return self


class ImplementationPackagePromotionReceipt(ContractModel):
    contract: Literal["implementation-package-promotion-receipt/v1"]
    artifact_ref: ArtifactRef
    promotion_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    package_ref: ArtifactRef
    preparation_execution_ref: ArtifactRef
    runtime_evidence_refs: List[ArtifactRef] = Field(min_length=1)
    interaction_evidence: List[InteractionPromotionEvidence] = Field(min_length=1)
    approval: Literal["approved"]
    release_notes: Optional[str] = Field(default=None, max_length=2000)
    state: Literal["promoted"]
    promotion_scope: Literal["tenant_release_candidate"]
    deployment_state: Literal["not_deployed"]
    evidence_persistence: Literal["durable"] = "durable"
    next_required_action: Literal["review_operational_readiness"]
    promotion_digest: Sha256Digest

    @model_validator(mode="after")
    def receipt_is_coherent(self) -> "ImplementationPackagePromotionReceipt":
        if self.artifact_ref.kind != "implementation-package-promotion-receipt":
            raise ValueError(
                "artifactRef.kind must be 'implementation-package-promotion-receipt'"
            )
        if self.artifact_ref.digest != self.promotion_digest:
            raise ValueError("artifactRef.digest must equal promotionDigest")
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.preparation_execution_ref.kind != (
            "implementation-package-preparation-execution-receipt"
        ):
            raise ValueError(
                "preparationExecutionRef.kind must be "
                "'implementation-package-preparation-execution-receipt'"
            )
        evidence_ids = [item.interaction_id for item in self.interaction_evidence]
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("interactionEvidence must contain unique interactionIds")
        evidence_refs = {
            (ref.kind, ref.id, ref.version, ref.digest)
            for ref in (item.runtime_execution_ref for item in self.interaction_evidence)
        }
        runtime_refs = {
            (ref.kind, ref.id, ref.version, ref.digest)
            for ref in self.runtime_evidence_refs
        }
        if evidence_refs != runtime_refs:
            raise ValueError(
                "runtimeEvidenceRefs must exactly match interactionEvidence references"
            )
        return self


__all__ = [
    "ImplementationPackagePromotionReceipt",
    "ImplementationPackagePromotionRequest",
    "InteractionPromotionEvidence",
]
