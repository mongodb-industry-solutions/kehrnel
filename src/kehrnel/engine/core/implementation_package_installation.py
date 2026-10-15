"""Stage one qualified Implementation Package for a tenant sandbox."""

from __future__ import annotations

import hashlib

from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_installation import (
    ImplementationPackageInstallationRequest,
    ImplementationPackageInstallationStage,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.implementation_artifact_store import (
    FileImplementationArtifactStore,
)


class ImplementationPackageInstallationError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _digest(payload: dict) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def stage_implementation_package_installation(
    environment_id: str,
    request: ImplementationPackageInstallationRequest,
    *,
    store: FileImplementationArtifactStore,
) -> ImplementationPackageInstallationStage:
    """Persist a tenant-scoped installation stage without claiming execution."""

    package = store.get_package(request.package_ref)
    evidence = store.get_evidence(request.qualification_evidence_ref)
    if evidence.status != "passed":
        raise ImplementationPackageInstallationError(
            "IMPLEMENTATION_PACKAGE_NOT_QUALIFIED",
            "Only a package with passing static qualification can be staged.",
        )
    if evidence.package_ref != package.artifact_ref:
        raise ImplementationPackageInstallationError(
            "IMPLEMENTATION_PACKAGE_EVIDENCE_MISMATCH",
            "The qualification evidence belongs to a different package.",
            status=409,
        )
    if package.target_capability_profile_ref != request.target_capability_profile_ref:
        raise ImplementationPackageInstallationError(
            "IMPLEMENTATION_PACKAGE_TARGET_MISMATCH",
            "The requested sandbox target differs from the package target.",
            status=409,
        )

    identity_digest = _digest(
        {
            "environmentId": environment_id,
            "packageRef": package.artifact_ref.to_wire(),
            "qualificationEvidenceRef": evidence.artifact_ref.to_wire(),
            "targetCapabilityProfileRef": package.target_capability_profile_ref.to_wire(),
            "installationMode": request.installation_mode,
        }
    )
    installation_id = (
        "sandbox-installation."
        f"{identity_digest.removeprefix('sha256:')}"
    )
    payload = {
        "contract": "implementation-package-installation-stage/v1",
        "installationId": installation_id,
        "environmentId": environment_id,
        "installationMode": request.installation_mode,
        "packageRef": package.artifact_ref.to_wire(),
        "resolvedPlanRef": package.resolved_plan_ref.to_wire(),
        "qualificationEvidenceRef": evidence.artifact_ref.to_wire(),
        "targetCapabilityProfileRef": package.target_capability_profile_ref.to_wire(),
        "state": "staged",
        "packageMetadataPersisted": True,
        "artifactsMaterialized": False,
        "preparationExecuted": False,
        "runtimeExecuted": False,
        "nextRequiredAction": "materialize_package_artifacts",
    }
    stage_digest = _digest(payload)
    stage = ImplementationPackageInstallationStage(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-installation-stage",
            id=installation_id,
            version=package.artifact_ref.version,
            digest=stage_digest,
        ),
        stageDigest=stage_digest,
    )
    store.put_installation_stage(stage)
    return stage


__all__ = [
    "ImplementationPackageInstallationError",
    "stage_implementation_package_installation",
]
