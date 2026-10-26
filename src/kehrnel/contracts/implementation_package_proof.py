"""Contracts for bounded, deterministic Implementation Package qualification."""

from __future__ import annotations

from typing import List, Literal

from pydantic import Field, model_validator

from .architecture_foundation import ImplementationPackage
from .artifacts import ArtifactRef, ContractModel, Sha256Digest
from .implementation_package_build import ImplementationPackageBuildRequest


class ImplementationPackageProofRequest(ContractModel):
    contract: Literal["implementation-package-proof-request/v1"]
    package_build_request: ImplementationPackageBuildRequest
    package: ImplementationPackage
    proof_kind: Literal["static_package_qualification"] = (
        "static_package_qualification"
    )


class ImplementationPackageProofCheck(ContractModel):
    check_id: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9._-]*$")
    status: Literal["passed", "failed"]
    summary: str = Field(min_length=1)
    evidence_digest: Sha256Digest


class ImplementationPackageProofEvidence(ContractModel):
    contract: Literal["implementation-package-proof-evidence/v1"]
    artifact_ref: ArtifactRef
    package_ref: ArtifactRef
    resolved_plan_ref: ArtifactRef
    compiler_ref: ArtifactRef
    proof_kind: Literal["static_package_qualification"]
    status: Literal["passed", "failed"]
    checks: List[ImplementationPackageProofCheck] = Field(min_length=1)
    contains_runtime_measurements: Literal[False] = False
    evidence_persistence: Literal["ephemeral", "durable"]
    evidence_digest: Sha256Digest

    @model_validator(mode="after")
    def evidence_is_coherent(self) -> "ImplementationPackageProofEvidence":
        if self.artifact_ref.kind != "implementation-package-proof-evidence":
            raise ValueError(
                "artifactRef.kind must be 'implementation-package-proof-evidence'"
            )
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.resolved_plan_ref.kind != "resolved-architecture-plan":
            raise ValueError(
                "resolvedPlanRef.kind must be 'resolved-architecture-plan'"
            )
        expected = (
            "passed" if all(check.status == "passed" for check in self.checks) else "failed"
        )
        if self.status != expected:
            raise ValueError("status must reflect every proof check")
        if self.artifact_ref.digest != self.evidence_digest:
            raise ValueError("artifactRef.digest must equal evidenceDigest")
        check_ids = [check.check_id for check in self.checks]
        if len(check_ids) != len(set(check_ids)):
            raise ValueError("checkId values must be unique")
        return self


class ImplementationPackageProofRun(ContractModel):
    contract: Literal["implementation-package-proof-run/v1"]
    artifact_ref: ArtifactRef
    proof_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    package_ref: ArtifactRef
    evidence_ref: ArtifactRef
    state: Literal["executed", "failed"]
    proof_kind: Literal["static_package_qualification"]
    contains_runtime_measurements: Literal[False] = False
    evidence_persistence: Literal["ephemeral", "durable"]
    run_digest: Sha256Digest

    @model_validator(mode="after")
    def run_is_coherent(self) -> "ImplementationPackageProofRun":
        if self.artifact_ref.kind != "implementation-package-proof-run":
            raise ValueError(
                "artifactRef.kind must be 'implementation-package-proof-run'"
            )
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.evidence_ref.kind != "implementation-package-proof-evidence":
            raise ValueError(
                "evidenceRef.kind must be 'implementation-package-proof-evidence'"
            )
        if self.artifact_ref.digest != self.run_digest:
            raise ValueError("artifactRef.digest must equal runDigest")
        return self


__all__ = [
    "ImplementationPackageProofCheck",
    "ImplementationPackageProofEvidence",
    "ImplementationPackageProofRequest",
    "ImplementationPackageProofRun",
]
