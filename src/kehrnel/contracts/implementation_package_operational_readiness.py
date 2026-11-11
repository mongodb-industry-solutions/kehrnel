"""Contracts for reviewing a promoted package before deployment."""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import Field, model_validator

from .artifacts import ArtifactRef, ContractModel, Sha256Digest


OperationalReadinessCheckId = Literal[
    "promotion_integrity",
    "runtime_evidence_available",
    "operational_owner_assigned",
    "monitoring_owner_assigned",
    "rollback_owner_assigned",
    "operational_review_approved",
]


class ImplementationPackageOperationalReadinessRequest(ContractModel):
    contract: Literal["implementation-package-operational-readiness-request/v1"]
    promotion_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    operational_owner_ref: Optional[str] = Field(
        default=None, min_length=1, max_length=256
    )
    monitoring_owner_ref: Optional[str] = Field(
        default=None, min_length=1, max_length=256
    )
    rollback_owner_ref: Optional[str] = Field(
        default=None, min_length=1, max_length=256
    )
    review_decision: Literal["approved", "needs_changes"]
    review_notes: Optional[str] = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ImplementationPackageOperationalReadinessRequest":
        if self.promotion_ref.kind != "implementation-package-promotion-receipt":
            raise ValueError(
                "promotionRef.kind must be 'implementation-package-promotion-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        return self


class OperationalReadinessCheck(ContractModel):
    check_id: OperationalReadinessCheckId
    state: Literal["passed", "blocked"]
    detail: str = Field(min_length=1)
    evidence_ref: Optional[ArtifactRef] = None


class ImplementationPackageOperationalReadinessReceipt(ContractModel):
    contract: Literal["implementation-package-operational-readiness-receipt/v1"]
    artifact_ref: ArtifactRef
    readiness_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    promotion_ref: ArtifactRef
    package_ref: ArtifactRef
    runtime_evidence_refs: List[ArtifactRef] = Field(min_length=1)
    operational_owner_ref: Optional[str] = None
    monitoring_owner_ref: Optional[str] = None
    rollback_owner_ref: Optional[str] = None
    review_decision: Literal["approved", "needs_changes"]
    review_notes: Optional[str] = Field(default=None, max_length=2000)
    checks: List[OperationalReadinessCheck] = Field(min_length=1)
    state: Literal["ready", "blocked"]
    deployment_state: Literal["not_deployed"]
    evidence_persistence: Literal["durable"] = "durable"
    next_required_action: Literal[
        "deploy_release_candidate",
        "provide_operational_readiness_inputs",
    ]
    readiness_digest: Sha256Digest

    @model_validator(mode="after")
    def receipt_is_coherent(self) -> "ImplementationPackageOperationalReadinessReceipt":
        if self.artifact_ref.kind != (
            "implementation-package-operational-readiness-receipt"
        ):
            raise ValueError(
                "artifactRef.kind must be "
                "'implementation-package-operational-readiness-receipt'"
            )
        if self.artifact_ref.digest != self.readiness_digest:
            raise ValueError("artifactRef.digest must equal readinessDigest")
        if self.promotion_ref.kind != "implementation-package-promotion-receipt":
            raise ValueError(
                "promotionRef.kind must be 'implementation-package-promotion-receipt'"
            )
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if any(
            ref.kind != "implementation-package-runtime-execution-receipt"
            for ref in self.runtime_evidence_refs
        ):
            raise ValueError(
                "runtimeEvidenceRefs must contain only "
                "implementation-package-runtime-execution-receipt references"
            )
        check_ids = [check.check_id for check in self.checks]
        if len(check_ids) != len(set(check_ids)):
            raise ValueError("checks must contain unique checkIds")
        expected_state = (
            "ready"
            if self.checks and all(check.state == "passed" for check in self.checks)
            else "blocked"
        )
        if self.state != expected_state:
            raise ValueError("state must reflect the readiness checks")
        expected_action = (
            "deploy_release_candidate"
            if self.state == "ready"
            else "provide_operational_readiness_inputs"
        )
        if self.next_required_action != expected_action:
            raise ValueError("nextRequiredAction must reflect state")
        return self


__all__ = [
    "ImplementationPackageOperationalReadinessReceipt",
    "ImplementationPackageOperationalReadinessRequest",
    "OperationalReadinessCheck",
    "OperationalReadinessCheckId",
]
