"""Contracts for retaining and materializing an Implementation Package closure."""

from __future__ import annotations

from typing import List, Literal

from pydantic import Field, model_validator

from .architecture_foundation import ArchitectureResolutionResult
from .artifacts import ArtifactRef, ContractModel, Sha256Digest
from .implementation_package_build import ImplementationPackageBuildRequest


class ImplementationPackageAssembly(ContractModel):
    """Immutable build inputs and outputs needed to reproduce package artifacts."""

    contract: Literal["implementation-package-assembly/v1"]
    artifact_ref: ArtifactRef
    package_ref: ArtifactRef
    package_build_request: ImplementationPackageBuildRequest
    resolution: ArchitectureResolutionResult
    assembly_digest: Sha256Digest

    @model_validator(mode="after")
    def assembly_is_coherent(self) -> "ImplementationPackageAssembly":
        if self.artifact_ref.kind != "implementation-package-assembly":
            raise ValueError(
                "artifactRef.kind must be 'implementation-package-assembly'"
            )
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.artifact_ref.digest != self.assembly_digest:
            raise ValueError("artifactRef.digest must equal assemblyDigest")
        if self.resolution.resolved_plan.artifact_ref != (
            self.package_build_request.expected_resolved_plan_ref
        ):
            raise ValueError("assembly resolution does not match its build request")
        return self


class ImplementationPackageMaterializationRequest(ContractModel):
    contract: Literal["implementation-package-materialization-request/v1"]
    installation_stage_ref: ArtifactRef
    expected_package_ref: ArtifactRef

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ImplementationPackageMaterializationRequest":
        if self.installation_stage_ref.kind != "implementation-package-installation-stage":
            raise ValueError(
                "installationStageRef.kind must be "
                "'implementation-package-installation-stage'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        return self


class MaterializedArtifact(ContractModel):
    artifact_ref: ArtifactRef
    source: Literal[
        "resolution",
        "build-input",
        "strategy-registry",
        "resolved-component",
    ]


class ImplementationPackageMaterializationReceipt(ContractModel):
    contract: Literal["implementation-package-materialization-receipt/v1"]
    artifact_ref: ArtifactRef
    materialization_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$"
    )
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    installation_stage_ref: ArtifactRef
    package_ref: ArtifactRef
    target_capability_profile_ref: ArtifactRef
    state: Literal["materialized"] = "materialized"
    materialized_artifacts: List[MaterializedArtifact] = Field(min_length=1)
    artifact_closure_complete: Literal[True] = True
    preparation_executed: Literal[False] = False
    runtime_executed: Literal[False] = False
    next_required_action: Literal["assess_preparation_readiness"] = (
        "assess_preparation_readiness"
    )
    materialization_digest: Sha256Digest

    @model_validator(mode="after")
    def receipt_is_coherent(self) -> "ImplementationPackageMaterializationReceipt":
        if self.artifact_ref.kind != "implementation-package-materialization-receipt":
            raise ValueError(
                "artifactRef.kind must be "
                "'implementation-package-materialization-receipt'"
            )
        if self.installation_stage_ref.kind != "implementation-package-installation-stage":
            raise ValueError(
                "installationStageRef.kind must be "
                "'implementation-package-installation-stage'"
            )
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.target_capability_profile_ref.kind != "target-capability-profile":
            raise ValueError(
                "targetCapabilityProfileRef.kind must be 'target-capability-profile'"
            )
        if self.artifact_ref.digest != self.materialization_digest:
            raise ValueError("artifactRef.digest must equal materializationDigest")
        refs = [item.artifact_ref for item in self.materialized_artifacts]
        if len(refs) != len(set(refs)):
            raise ValueError("materializedArtifacts must contain unique artifact refs")
        return self


__all__ = [
    "ImplementationPackageAssembly",
    "ImplementationPackageMaterializationReceipt",
    "ImplementationPackageMaterializationRequest",
    "MaterializedArtifact",
]
