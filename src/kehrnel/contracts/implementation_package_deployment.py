"""Contracts for deploying and rolling back an operationally-ready package."""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import Field, model_validator

from .artifacts import ArtifactRef, ContractModel, Sha256Digest


class ImplementationPackageDeploymentRequest(ContractModel):
    contract: Literal["implementation-package-deployment-request/v1"]
    deployment_request_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    readiness_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    expected_current_deployment_ref: Optional[ArtifactRef] = None
    deployment_owner_ref: str = Field(min_length=1, max_length=256)
    approval: Literal["approved"]
    change_ref: Optional[str] = Field(default=None, min_length=1, max_length=256)
    deployment_notes: Optional[str] = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ImplementationPackageDeploymentRequest":
        if self.readiness_ref.kind != (
            "implementation-package-operational-readiness-receipt"
        ):
            raise ValueError(
                "readinessRef.kind must be "
                "'implementation-package-operational-readiness-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        if (
            self.expected_current_deployment_ref is not None
            and self.expected_current_deployment_ref.kind
            != "implementation-package-deployment-receipt"
        ):
            raise ValueError(
                "expectedCurrentDeploymentRef.kind must be "
                "'implementation-package-deployment-receipt'"
            )
        return self


class ImplementationPackageDeploymentReceipt(ContractModel):
    contract: Literal["implementation-package-deployment-receipt/v1"]
    artifact_ref: ArtifactRef
    deployment_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    deployment_request_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    readiness_ref: ArtifactRef
    promotion_ref: ArtifactRef
    package_ref: ArtifactRef
    preparation_execution_ref: ArtifactRef
    previous_deployment_ref: Optional[ArtifactRef] = None
    deployment_owner_ref: str = Field(min_length=1, max_length=256)
    operational_owner_ref: str = Field(min_length=1, max_length=256)
    monitoring_owner_ref: str = Field(min_length=1, max_length=256)
    rollback_owner_ref: str = Field(min_length=1, max_length=256)
    change_ref: Optional[str] = Field(default=None, max_length=256)
    deployment_notes: Optional[str] = Field(default=None, max_length=2000)
    operation_state: Literal["succeeded"]
    deployment_scope: Literal["tenant_environment_release"]
    recorded_effect: Literal["active_release_binding_created"]
    traffic_management: Literal["external"]
    evidence_persistence: Literal["durable"] = "durable"
    next_required_action: Literal["monitor_release"]
    deployment_digest: Sha256Digest

    @model_validator(mode="after")
    def receipt_is_coherent(self) -> "ImplementationPackageDeploymentReceipt":
        if self.artifact_ref.kind != "implementation-package-deployment-receipt":
            raise ValueError(
                "artifactRef.kind must be 'implementation-package-deployment-receipt'"
            )
        if self.artifact_ref.digest != self.deployment_digest:
            raise ValueError("artifactRef.digest must equal deploymentDigest")
        if self.readiness_ref.kind != (
            "implementation-package-operational-readiness-receipt"
        ):
            raise ValueError(
                "readinessRef.kind must be "
                "'implementation-package-operational-readiness-receipt'"
            )
        if self.promotion_ref.kind != "implementation-package-promotion-receipt":
            raise ValueError(
                "promotionRef.kind must be 'implementation-package-promotion-receipt'"
            )
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.preparation_execution_ref.kind != (
            "implementation-package-preparation-execution-receipt"
        ):
            raise ValueError(
                "preparationExecutionRef.kind must be "
                "'implementation-package-preparation-execution-receipt'"
            )
        if (
            self.previous_deployment_ref is not None
            and self.previous_deployment_ref.kind
            != "implementation-package-deployment-receipt"
        ):
            raise ValueError(
                "previousDeploymentRef.kind must be "
                "'implementation-package-deployment-receipt'"
            )
        return self


class ImplementationPackageRollbackRequest(ContractModel):
    contract: Literal["implementation-package-rollback-request/v1"]
    rollback_request_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    deployment_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    executed_by_ref: str = Field(min_length=1, max_length=256)
    approval: Literal["approved"]
    reason: str = Field(min_length=1, max_length=2000)

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ImplementationPackageRollbackRequest":
        if self.deployment_ref.kind != "implementation-package-deployment-receipt":
            raise ValueError(
                "deploymentRef.kind must be 'implementation-package-deployment-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        return self


class ImplementationPackageRollbackReceipt(ContractModel):
    contract: Literal["implementation-package-rollback-receipt/v1"]
    artifact_ref: ArtifactRef
    rollback_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    rollback_request_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    deployment_ref: ArtifactRef
    package_ref: ArtifactRef
    restored_deployment_ref: Optional[ArtifactRef] = None
    restored_package_ref: Optional[ArtifactRef] = None
    executed_by_ref: str = Field(min_length=1, max_length=256)
    rollback_owner_ref: str = Field(min_length=1, max_length=256)
    reason: str = Field(min_length=1, max_length=2000)
    operation_state: Literal["succeeded"]
    deployment_scope: Literal["tenant_environment_release"]
    recorded_effect: Literal["active_release_binding_reverted"]
    traffic_management: Literal["external"]
    evidence_persistence: Literal["durable"] = "durable"
    next_required_action: Literal[
        "monitor_restored_release",
        "review_new_release_candidate",
    ]
    rollback_digest: Sha256Digest

    @model_validator(mode="after")
    def receipt_is_coherent(self) -> "ImplementationPackageRollbackReceipt":
        if self.artifact_ref.kind != "implementation-package-rollback-receipt":
            raise ValueError(
                "artifactRef.kind must be 'implementation-package-rollback-receipt'"
            )
        if self.artifact_ref.digest != self.rollback_digest:
            raise ValueError("artifactRef.digest must equal rollbackDigest")
        if self.deployment_ref.kind != "implementation-package-deployment-receipt":
            raise ValueError(
                "deploymentRef.kind must be 'implementation-package-deployment-receipt'"
            )
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if (self.restored_deployment_ref is None) != (
            self.restored_package_ref is None
        ):
            raise ValueError(
                "restoredDeploymentRef and restoredPackageRef must be present together"
            )
        if (
            self.restored_deployment_ref is not None
            and self.restored_deployment_ref.kind
            != "implementation-package-deployment-receipt"
        ):
            raise ValueError(
                "restoredDeploymentRef.kind must be "
                "'implementation-package-deployment-receipt'"
            )
        expected_action = (
            "monitor_restored_release"
            if self.restored_deployment_ref is not None
            else "review_new_release_candidate"
        )
        if self.next_required_action != expected_action:
            raise ValueError("nextRequiredAction must reflect the rollback result")
        return self


class ImplementationPackageDeploymentStatus(ContractModel):
    contract: Literal["implementation-package-deployment-status/v1"]
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    state: Literal["active", "none"]
    active_deployment_ref: Optional[ArtifactRef] = None
    active_package_ref: Optional[ArtifactRef] = None
    active_readiness_ref: Optional[ArtifactRef] = None
    traffic_management: Literal["external"] = "external"
    next_required_action: Literal[
        "monitor_or_rollback",
        "deploy_ready_release_candidate",
    ]

    @model_validator(mode="after")
    def status_is_coherent(self) -> "ImplementationPackageDeploymentStatus":
        references = (
            self.active_deployment_ref,
            self.active_package_ref,
            self.active_readiness_ref,
        )
        if self.state == "active" and any(ref is None for ref in references):
            raise ValueError("active deployment status requires all active references")
        if self.state == "none" and any(ref is not None for ref in references):
            raise ValueError("empty deployment status cannot contain active references")
        expected_action = (
            "monitor_or_rollback"
            if self.state == "active"
            else "deploy_ready_release_candidate"
        )
        if self.next_required_action != expected_action:
            raise ValueError("nextRequiredAction must reflect deployment state")
        return self


__all__ = [
    "ImplementationPackageDeploymentReceipt",
    "ImplementationPackageDeploymentRequest",
    "ImplementationPackageDeploymentStatus",
    "ImplementationPackageRollbackReceipt",
    "ImplementationPackageRollbackRequest",
]
