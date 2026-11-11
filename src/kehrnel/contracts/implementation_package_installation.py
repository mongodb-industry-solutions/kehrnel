"""Contracts for staging a qualified package in one tenant sandbox."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from .artifacts import ArtifactRef, ContractModel, Sha256Digest


class ImplementationPackageInstallationRequest(ContractModel):
    contract: Literal["implementation-package-installation-request/v1"]
    package_ref: ArtifactRef
    qualification_evidence_ref: ArtifactRef
    target_capability_profile_ref: ArtifactRef
    installation_mode: Literal["tenant_sandbox"] = "tenant_sandbox"

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ImplementationPackageInstallationRequest":
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if (
            self.qualification_evidence_ref.kind
            != "implementation-package-proof-evidence"
        ):
            raise ValueError(
                "qualificationEvidenceRef.kind must be "
                "'implementation-package-proof-evidence'"
            )
        if self.target_capability_profile_ref.kind != "target-capability-profile":
            raise ValueError(
                "targetCapabilityProfileRef.kind must be 'target-capability-profile'"
            )
        return self


class ImplementationPackageInstallationStage(ContractModel):
    contract: Literal["implementation-package-installation-stage/v1"]
    artifact_ref: ArtifactRef
    installation_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    environment_id: str = Field(min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    installation_mode: Literal["tenant_sandbox"]
    package_ref: ArtifactRef
    resolved_plan_ref: ArtifactRef
    qualification_evidence_ref: ArtifactRef
    target_capability_profile_ref: ArtifactRef
    state: Literal["staged"] = "staged"
    package_metadata_persisted: Literal[True] = True
    artifacts_materialized: Literal[False] = False
    preparation_executed: Literal[False] = False
    runtime_executed: Literal[False] = False
    next_required_action: Literal["materialize_package_artifacts"] = (
        "materialize_package_artifacts"
    )
    stage_digest: Sha256Digest

    @model_validator(mode="after")
    def stage_is_coherent(self) -> "ImplementationPackageInstallationStage":
        if self.artifact_ref.kind != "implementation-package-installation-stage":
            raise ValueError(
                "artifactRef.kind must be 'implementation-package-installation-stage'"
            )
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.resolved_plan_ref.kind != "resolved-architecture-plan":
            raise ValueError("resolvedPlanRef.kind must be 'resolved-architecture-plan'")
        if (
            self.qualification_evidence_ref.kind
            != "implementation-package-proof-evidence"
        ):
            raise ValueError(
                "qualificationEvidenceRef.kind must be "
                "'implementation-package-proof-evidence'"
            )
        if self.target_capability_profile_ref.kind != "target-capability-profile":
            raise ValueError(
                "targetCapabilityProfileRef.kind must be 'target-capability-profile'"
            )
        if self.artifact_ref.digest != self.stage_digest:
            raise ValueError("artifactRef.digest must equal stageDigest")
        return self


__all__ = [
    "ImplementationPackageInstallationRequest",
    "ImplementationPackageInstallationStage",
]
