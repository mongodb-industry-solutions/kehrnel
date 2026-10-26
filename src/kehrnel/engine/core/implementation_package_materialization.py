"""Materialize an exact staged package artifact closure for one tenant."""

from __future__ import annotations

import hashlib

from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_materialization import (
    ImplementationPackageAssembly,
    ImplementationPackageMaterializationReceipt,
    ImplementationPackageMaterializationRequest,
    MaterializedArtifact,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.architecture_plan_compiler import _load_supported_strategy, _strategy_ref
from kehrnel.engine.core.implementation_artifact_store import FileImplementationArtifactStore


class ImplementationPackageMaterializationError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _digest(payload: dict) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def build_package_assembly(
    package,
    build_request,
    resolution,
) -> ImplementationPackageAssembly:
    payload = {
        "contract": "implementation-package-assembly/v1",
        "packageRef": package.artifact_ref.to_wire(),
        "packageBuildRequest": build_request.to_wire(),
        "resolution": resolution.to_wire(),
    }
    digest = _digest(payload)
    return ImplementationPackageAssembly(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-assembly",
            id=package.artifact_ref.id,
            version=package.artifact_ref.version,
            digest=digest,
        ),
        assemblyDigest=digest,
    )


def _assembly_artifacts(assembly: ImplementationPackageAssembly) -> dict[ArtifactRef, str]:
    resolution = assembly.resolution
    request = assembly.package_build_request.resolution_request
    artifacts: dict[ArtifactRef, str] = {
        resolution.preparation_plan.artifact_ref: "resolution",
        resolution.runtime_plan.artifact_ref: "resolution",
        request.target_capability_profile.artifact_ref: "build-input",
    }
    artifacts.update(
        (binding.artifact_ref, "build-input")
        for binding in request.semantic_execution_bindings
    )
    for strategy in resolution.resolved_plan.strategy_instances:
        installed_ref = _strategy_ref(_load_supported_strategy(strategy.strategy_ref.id))
        if installed_ref != strategy.strategy_ref:
            raise ImplementationPackageMaterializationError(
                "IMPLEMENTATION_PACKAGE_STRATEGY_MISMATCH",
                f"Installed strategy '{strategy.strategy_ref.id}' differs from the package.",
                status=409,
            )
        artifacts[strategy.strategy_ref] = "strategy-registry"
    for route in resolution.runtime_plan.routes:
        artifacts[route.planner_ref] = "resolved-component"
        artifacts[route.executor_ref] = "resolved-component"
        if route.policy_ref is not None:
            artifacts[route.policy_ref] = "resolved-component"
    for strategy in resolution.resolved_plan.strategy_instances:
        for reference in strategy.dependency_refs:
            artifacts.setdefault(reference, "resolved-component")
    return artifacts


def materialize_implementation_package(
    environment_id: str,
    request: ImplementationPackageMaterializationRequest,
    *,
    store: FileImplementationArtifactStore,
) -> ImplementationPackageMaterializationReceipt:
    stage = store.get_installation_stage(request.installation_stage_ref)
    if stage.environment_id != environment_id:
        raise ImplementationPackageMaterializationError(
            "IMPLEMENTATION_PACKAGE_INSTALLATION_SCOPE_MISMATCH",
            "The installation stage belongs to a different environment.",
            status=409,
        )
    if stage.package_ref != request.expected_package_ref:
        raise ImplementationPackageMaterializationError(
            "IMPLEMENTATION_PACKAGE_PRECONDITION_FAILED",
            "The staged package differs from the expected package.",
            status=409,
        )
    package = store.get_package(stage.package_ref)
    assembly = store.find_package_assembly(package.artifact_ref.id, package.artifact_ref.version)
    if assembly.package_ref != package.artifact_ref:
        raise ImplementationPackageMaterializationError(
            "IMPLEMENTATION_PACKAGE_ASSEMBLY_MISMATCH",
            "The stored package assembly belongs to a different package.",
            status=409,
        )
    available = _assembly_artifacts(assembly)
    required = set(package.required_artifact_refs)
    missing = sorted(
        required - set(available),
        key=lambda ref: (ref.kind, ref.id, ref.version, ref.digest),
    )
    if missing:
        raise ImplementationPackageMaterializationError(
            "IMPLEMENTATION_PACKAGE_ARTIFACT_UNAVAILABLE",
            "Package artifact closure is incomplete: "
            + ", ".join(f"{ref.kind}:{ref.id}@{ref.version}" for ref in missing),
            status=409,
        )
    materialized = [
        MaterializedArtifact(artifactRef=ref, source=available[ref])
        for ref in sorted(
            required,
            key=lambda item: (item.kind, item.id, item.version, item.digest),
        )
    ]
    identity = _digest(
        {
            "environmentId": environment_id,
            "installationStageRef": stage.artifact_ref.to_wire(),
            "packageRef": package.artifact_ref.to_wire(),
            "materializedArtifactRefs": [item.artifact_ref.to_wire() for item in materialized],
        }
    )
    materialization_id = "package-materialization." + identity.removeprefix("sha256:")
    payload = {
        "contract": "implementation-package-materialization-receipt/v1",
        "materializationId": materialization_id,
        "environmentId": environment_id,
        "installationStageRef": stage.artifact_ref.to_wire(),
        "packageRef": package.artifact_ref.to_wire(),
        "targetCapabilityProfileRef": package.target_capability_profile_ref.to_wire(),
        "state": "materialized",
        "materializedArtifacts": [item.to_wire() for item in materialized],
        "artifactClosureComplete": True,
        "preparationExecuted": False,
        "runtimeExecuted": False,
        "nextRequiredAction": "assess_preparation_readiness",
    }
    receipt_digest = _digest(payload)
    receipt = ImplementationPackageMaterializationReceipt(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-materialization-receipt",
            id=materialization_id,
            version=package.artifact_ref.version,
            digest=receipt_digest,
        ),
        materializationDigest=receipt_digest,
    )
    store.put_materialization_receipt(receipt)
    return receipt


__all__ = [
    "ImplementationPackageMaterializationError",
    "build_package_assembly",
    "materialize_implementation_package",
]
