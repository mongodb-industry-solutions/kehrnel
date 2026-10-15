"""Deterministically package one ready canonical architecture resolution."""

from __future__ import annotations

import hashlib
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from kehrnel.contracts.architecture_foundation import (
    CandidateStatus,
    ImplementationPackage,
)
from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_build import (
    ImplementationPackageBuildRequest,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.contracts.schema_registry import published_contract_digest
from kehrnel.engine.core.architecture_plan_compiler import (
    compile_architecture_resolution,
)


class ImplementationPackageBuildError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _digest(payload: dict) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _compiler_version() -> str:
    try:
        return version("kehrnel-core")
    except PackageNotFoundError:
        return "development"


def compiler_artifact_ref() -> ArtifactRef:
    """Identify the exact compiler sources and published contract set in use."""

    compiler_path = Path(__file__).with_name("architecture_plan_compiler.py")
    sources = {}
    for path in sorted((compiler_path, Path(__file__)), key=lambda item: item.name):
        sources[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    payload = {
        "compiler": "kehrnel-architecture-resolution",
        "version": _compiler_version(),
        "sources": sources,
        "publishedContractDigest": published_contract_digest(),
    }
    return ArtifactRef(
        kind="compiler",
        id="kehrnel-architecture-resolution",
        version=_compiler_version(),
        digest=_digest(payload),
    )


def _required_artifact_refs(resolution) -> list[ArtifactRef]:
    resolved = resolution.resolved_plan
    refs = {
        resolution.preparation_plan.artifact_ref,
        resolution.runtime_plan.artifact_ref,
        resolved.target_capability_profile_ref,
        *resolved.semantic_binding_refs,
    }
    for strategy in resolved.strategy_instances:
        refs.add(strategy.strategy_ref)
        refs.update(strategy.binding_refs)
        refs.update(strategy.dependency_refs)
    for stage in resolution.preparation_plan.stages:
        refs.update(stage.binding_refs)
    for route in resolution.runtime_plan.routes:
        refs.add(route.semantic_binding_ref)
        refs.add(route.planner_ref)
        refs.add(route.executor_ref)
        if route.policy_ref is not None:
            refs.add(route.policy_ref)
    return sorted(
        refs,
        key=lambda item: (item.kind, item.id, item.version, item.digest),
    )


def build_implementation_package(
    request: ImplementationPackageBuildRequest,
) -> ImplementationPackage:
    """Resolve, precondition-check and package an architecture without deploying it."""

    resolution = compile_architecture_resolution(request.resolution_request)
    resolved = resolution.resolved_plan
    if resolved.artifact_ref != request.expected_resolved_plan_ref:
        raise ImplementationPackageBuildError(
            "RESOLVED_PLAN_PRECONDITION_FAILED",
            "The architecture no longer resolves to the expected plan.",
            status=409,
        )
    if resolved.status is not CandidateStatus.READY:
        raise ImplementationPackageBuildError(
            "IMPLEMENTATION_PACKAGE_NOT_READY",
            "Only a ready architecture resolution can become an implementation package.",
        )

    compiler_ref = compiler_artifact_ref()
    required_refs = _required_artifact_refs(resolution)
    payload = {
        "contract": "implementation-package/v1",
        "blueprintRef": resolved.blueprint_ref.to_wire(),
        "resolvedPlanRef": resolved.artifact_ref.to_wire(),
        "targetCapabilityProfileRef": resolved.target_capability_profile_ref.to_wire(),
        "requiredArtifactRefs": [item.to_wire() for item in required_refs],
        "compilerRef": compiler_ref.to_wire(),
        "runtimeDependencies": [
            item.to_wire() for item in request.runtime_dependencies
        ],
        "deliveryLevel": request.delivery_level.value,
        "portalIndependent": True,
    }
    if request.lifecycle_owner:
        payload["lifecycleOwner"] = request.lifecycle_owner
    package_digest = _digest(payload)
    artifact_ref = ArtifactRef(
        kind="implementation-package",
        id=resolved.blueprint_ref.blueprint_id,
        version=f"{resolved.blueprint_ref.revision}.0.0",
        digest=package_digest,
    )
    return ImplementationPackage(
        **payload,
        artifactRef=artifact_ref,
        packageDigest=package_digest,
    )


__all__ = [
    "ImplementationPackageBuildError",
    "build_implementation_package",
    "compiler_artifact_ref",
]
