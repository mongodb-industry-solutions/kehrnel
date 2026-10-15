"""Run and retain bounded static qualification for Implementation Packages."""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from threading import RLock

from kehrnel.contracts.architecture_foundation import ImplementationPackage
from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_proof import (
    ImplementationPackageProofCheck,
    ImplementationPackageProofEvidence,
    ImplementationPackageProofRequest,
    ImplementationPackageProofRun,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.implementation_package_builder import (
    ImplementationPackageBuildError,
    build_implementation_package,
    compiler_artifact_ref,
)


class ImplementationPackageProofEvidenceNotFound(LookupError):
    pass


def _digest(payload: dict) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _package_content_digest(package: ImplementationPackage) -> str:
    payload = package.to_wire()
    payload.pop("artifactRef", None)
    payload.pop("packageDigest", None)
    return _digest(payload)


def _check(check_id: str, passed: bool, summary: str, observed: dict):
    return ImplementationPackageProofCheck(
        checkId=check_id,
        status="passed" if passed else "failed",
        summary=summary,
        evidenceDigest=_digest(
            {
                "checkId": check_id,
                "status": "passed" if passed else "failed",
                "observed": observed,
            }
        ),
    )


class ImplementationPackageProofEvidenceStore:
    """Small in-memory proof store used by deterministic unit-level callers."""

    persistence = "ephemeral"

    def __init__(self, *, capacity: int = 256):
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._items: OrderedDict[
            tuple[str, str], ImplementationPackageProofEvidence
        ] = OrderedDict()
        self._lock = RLock()

    def put(self, evidence: ImplementationPackageProofEvidence) -> None:
        key = (evidence.artifact_ref.id, evidence.artifact_ref.version)
        with self._lock:
            existing = self._items.get(key)
            if existing is not None and existing.evidence_digest != evidence.evidence_digest:
                raise ValueError(
                    "proof evidence identity is immutable and cannot be overwritten"
                )
            self._items[key] = evidence
            self._items.move_to_end(key)
            while len(self._items) > self._capacity:
                self._items.popitem(last=False)

    def get(self, evidence_id: str, version: str) -> ImplementationPackageProofEvidence:
        key = (evidence_id, version)
        with self._lock:
            evidence = self._items.get(key)
            if evidence is None:
                raise ImplementationPackageProofEvidenceNotFound(
                    f"Proof evidence '{evidence_id}@{version}' is unavailable or expired."
                )
            self._items.move_to_end(key)
            return evidence


PROOF_EVIDENCE_STORE = ImplementationPackageProofEvidenceStore()


def run_implementation_package_proof(
    request: ImplementationPackageProofRequest,
    *,
    store: ImplementationPackageProofEvidenceStore = PROOF_EVIDENCE_STORE,
) -> ImplementationPackageProofRun:
    """Qualify package identity without installing it or claiming runtime evidence."""

    package = request.package
    evidence_persistence = getattr(store, "persistence", "ephemeral")
    expected_content_digest = _package_content_digest(package)
    resolved_plan_pinned = (
        request.package_build_request.expected_resolved_plan_ref
        == package.resolved_plan_ref
    )
    artifact_closure_pinned = bool(package.required_artifact_refs) and all(
        reference.digest.startswith("sha256:")
        for reference in package.required_artifact_refs
    )
    current_compiler_ref = compiler_artifact_ref()
    compiler_contract_current = package.compiler_ref == current_compiler_ref
    checks = [
        _check(
            "package-content-addressed",
            expected_content_digest
            == package.package_digest
            == package.artifact_ref.digest,
            "The package content, packageDigest and artifactRef digest agree."
            if expected_content_digest
            == package.package_digest
            == package.artifact_ref.digest
            else "The package content does not match its declared digest.",
            {
                "computedDigest": expected_content_digest,
                "packageDigest": package.package_digest,
                "artifactDigest": package.artifact_ref.digest,
            },
        ),
        _check(
            "resolved-plan-pinned",
            resolved_plan_pinned,
            "The package pins the exact resolved plan expected by its build request."
            if resolved_plan_pinned
            else "The package and build request refer to different resolved plans.",
            {
                "requestDigest": request.package_build_request.expected_resolved_plan_ref.digest,
                "packageDigest": package.resolved_plan_ref.digest,
            },
        ),
        _check(
            "artifact-closure-pinned",
            artifact_closure_pinned,
            "Every required package artifact is pinned by content digest."
            if artifact_closure_pinned
            else "The package has no required artifact closure.",
            {
                "artifactCount": len(package.required_artifact_refs),
                "artifactDigests": sorted(
                    reference.digest for reference in package.required_artifact_refs
                ),
            },
        ),
        _check(
            "compiler-contract-current",
            compiler_contract_current,
            "The package pins the current compiler source and published contract set."
            if compiler_contract_current
            else "The package was built with a different compiler or contract set.",
            {
                "packageCompilerDigest": package.compiler_ref.digest,
                "currentCompilerDigest": current_compiler_ref.digest,
            },
        ),
    ]

    canonical_package = None
    rebuild_error = None
    try:
        canonical_package = build_implementation_package(request.package_build_request)
    except ImplementationPackageBuildError as exc:
        rebuild_error = exc.code
    checks.append(
        _check(
            "canonical-rebuild",
            canonical_package is not None
            and canonical_package.to_wire() == package.to_wire(),
            "Rebuilding from the canonical request reproduces the package byte-for-byte."
            if canonical_package is not None
            and canonical_package.to_wire() == package.to_wire()
            else "The canonical request does not reproduce this package.",
            {
                "submittedPackageDigest": package.package_digest,
                "rebuiltPackageDigest": (
                    canonical_package.package_digest if canonical_package else None
                ),
                "rebuildErrorCode": rebuild_error,
            },
        )
    )

    status = "passed" if all(check.status == "passed" for check in checks) else "failed"
    evidence_payload = {
        "contract": "implementation-package-proof-evidence/v1",
        "packageRef": package.artifact_ref.to_wire(),
        "resolvedPlanRef": package.resolved_plan_ref.to_wire(),
        "compilerRef": package.compiler_ref.to_wire(),
        "proofKind": request.proof_kind,
        "status": status,
        "checks": [check.to_wire() for check in checks],
        "containsRuntimeMeasurements": False,
        "evidencePersistence": evidence_persistence,
    }
    evidence_digest = _digest(evidence_payload)
    evidence_id = (
        f"{package.artifact_ref.id}.static-qualification."
        f"{evidence_digest.removeprefix('sha256:')}"
    )
    evidence_ref = ArtifactRef(
        kind="implementation-package-proof-evidence",
        id=evidence_id,
        version=package.artifact_ref.version,
        digest=evidence_digest,
    )
    evidence = ImplementationPackageProofEvidence(
        **evidence_payload,
        artifactRef=evidence_ref,
        evidenceDigest=evidence_digest,
    )
    store.put(evidence)

    run_payload = {
        "contract": "implementation-package-proof-run/v1",
        "proofId": evidence_id,
        "packageRef": package.artifact_ref.to_wire(),
        "evidenceRef": evidence_ref.to_wire(),
        "state": "executed" if status == "passed" else "failed",
        "proofKind": request.proof_kind,
        "containsRuntimeMeasurements": False,
        "evidencePersistence": evidence_persistence,
    }
    run_digest = _digest(run_payload)
    return ImplementationPackageProofRun(
        **run_payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-proof-run",
            id=run_payload["proofId"],
            version=package.artifact_ref.version,
            digest=run_digest,
        ),
        runDigest=run_digest,
    )


__all__ = [
    "ImplementationPackageProofEvidenceNotFound",
    "ImplementationPackageProofEvidenceStore",
    "PROOF_EVIDENCE_STORE",
    "run_implementation_package_proof",
]
