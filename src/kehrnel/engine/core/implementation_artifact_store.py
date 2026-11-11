"""Durable, content-addressed storage for implementation lifecycle artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from threading import RLock
from typing import TypeVar

from pydantic import BaseModel

from kehrnel.contracts.architecture_foundation import ImplementationPackage
from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_installation import (
    ImplementationPackageInstallationStage,
)
from kehrnel.contracts.implementation_package_activation import (
    ApprovedStrategyActivationSpecification,
    ImplementationPackageActivationReceipt,
)
from kehrnel.contracts.implementation_package_materialization import (
    ImplementationPackageAssembly,
    ImplementationPackageMaterializationReceipt,
)
from kehrnel.contracts.implementation_package_preparation import (
    ImplementationPackagePreparationReadiness,
)
from kehrnel.contracts.implementation_package_preparation_execution import (
    ImplementationPackagePreparationExecutionReceipt,
)
from kehrnel.contracts.implementation_package_runtime_execution import (
    ImplementationPackageRuntimeExecutionReceipt,
)
from kehrnel.contracts.implementation_package_promotion import (
    ImplementationPackagePromotionReceipt,
)
from kehrnel.contracts.implementation_package_operational_readiness import (
    ImplementationPackageOperationalReadinessReceipt,
)
from kehrnel.contracts.implementation_package_deployment import (
    ImplementationPackageDeploymentReceipt,
    ImplementationPackageRollbackReceipt,
)
from kehrnel.contracts.implementation_package_observation import (
    ImplementationPackageOperationalObservationReceipt,
)
from kehrnel.contracts.implementation_package_proof import (
    ImplementationPackageProofEvidence,
)
from kehrnel.contracts.package_digest import canonical_json_bytes


StoredModel = TypeVar("StoredModel", bound=BaseModel)


class ImplementationArtifactNotFound(LookupError):
    pass


class ImplementationArtifactConflict(ValueError):
    pass


class FileImplementationArtifactStore:
    """Atomically retain immutable implementation artifacts across restarts."""

    persistence = "durable"

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()

    @staticmethod
    def _key(reference: ArtifactRef) -> str:
        identity = f"{reference.kind}\0{reference.id}\0{reference.version}"
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    def _path(self, reference: ArtifactRef) -> Path:
        return self.root / reference.kind / f"{self._key(reference)}.json"

    def _put(self, artifact: BaseModel) -> None:
        reference = artifact.artifact_ref
        path = self._path(reference)
        payload = artifact.to_wire()
        encoded = canonical_json_bytes(payload)
        with self._lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                existing = path.read_bytes()
                if existing != encoded:
                    raise ImplementationArtifactConflict(
                        f"Artifact '{reference.id}@{reference.version}' is immutable."
                    )
                return
            temporary = path.with_suffix(".tmp")
            temporary.write_bytes(encoded)
            temporary.replace(path)

    def _get(
        self,
        reference: ArtifactRef,
        model: type[StoredModel],
    ) -> StoredModel:
        path = self._path(reference)
        with self._lock:
            if not path.exists():
                raise ImplementationArtifactNotFound(
                    f"Artifact '{reference.id}@{reference.version}' is not stored."
                )
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                artifact = model.model_validate(payload)
            except Exception as exc:
                raise ImplementationArtifactConflict(
                    f"Stored artifact '{reference.id}@{reference.version}' is invalid."
                ) from exc
            if artifact.artifact_ref != reference:
                raise ImplementationArtifactConflict(
                    f"Stored artifact '{reference.id}@{reference.version}' does not match its reference."
                )
            return artifact

    def _find(
        self,
        kind: str,
        artifact_id: str,
        version: str,
        model: type[StoredModel],
    ) -> StoredModel:
        directory = self.root / kind
        if directory.exists():
            with self._lock:
                for path in directory.glob("*.json"):
                    try:
                        payload = json.loads(path.read_text(encoding="utf-8"))
                        reference = ArtifactRef.model_validate(payload.get("artifactRef"))
                    except Exception as exc:
                        raise ImplementationArtifactConflict(
                            f"Stored {kind} index contains an invalid artifact."
                        ) from exc
                    if reference.id == artifact_id and reference.version == version:
                        return self._get(reference, model)
        raise ImplementationArtifactNotFound(
            f"Artifact '{artifact_id}@{version}' is not stored."
        )

    def put_package(self, package: ImplementationPackage) -> None:
        self._put(package)

    def get_package(self, reference: ArtifactRef) -> ImplementationPackage:
        return self._get(reference, ImplementationPackage)

    def find_package(self, package_id: str, version: str) -> ImplementationPackage:
        return self._find(
            "implementation-package",
            package_id,
            version,
            ImplementationPackage,
        )

    def put_evidence(self, evidence: ImplementationPackageProofEvidence) -> None:
        self._put(evidence)

    def get_evidence(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackageProofEvidence:
        return self._get(reference, ImplementationPackageProofEvidence)

    # The proof runner intentionally depends on this minimal store protocol.
    def put(self, evidence: ImplementationPackageProofEvidence) -> None:
        self.put_evidence(evidence)

    def get(self, evidence_id: str, version: str) -> ImplementationPackageProofEvidence:
        return self._find(
            "implementation-package-proof-evidence",
            evidence_id,
            version,
            ImplementationPackageProofEvidence,
        )

    def put_package_assembly(self, assembly: ImplementationPackageAssembly) -> None:
        self._put(assembly)

    def get_package_assembly(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackageAssembly:
        return self._get(reference, ImplementationPackageAssembly)

    def find_package_assembly(
        self,
        package_id: str,
        version: str,
    ) -> ImplementationPackageAssembly:
        return self._find(
            "implementation-package-assembly",
            package_id,
            version,
            ImplementationPackageAssembly,
        )

    def put_installation_stage(
        self,
        stage: ImplementationPackageInstallationStage,
    ) -> None:
        self._put(stage)

    def get_installation_stage(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackageInstallationStage:
        return self._get(reference, ImplementationPackageInstallationStage)

    def find_installation_stage(
        self,
        installation_id: str,
        version: str,
    ) -> ImplementationPackageInstallationStage:
        return self._find(
            "implementation-package-installation-stage",
            installation_id,
            version,
            ImplementationPackageInstallationStage,
        )

    def put_materialization_receipt(
        self,
        receipt: ImplementationPackageMaterializationReceipt,
    ) -> None:
        self._put(receipt)

    def get_materialization_receipt(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackageMaterializationReceipt:
        return self._get(reference, ImplementationPackageMaterializationReceipt)

    def find_materialization_receipt(
        self,
        materialization_id: str,
        version: str,
    ) -> ImplementationPackageMaterializationReceipt:
        return self._find(
            "implementation-package-materialization-receipt",
            materialization_id,
            version,
            ImplementationPackageMaterializationReceipt,
        )

    def put_preparation_readiness(
        self,
        readiness: ImplementationPackagePreparationReadiness,
    ) -> None:
        self._put(readiness)

    def get_preparation_readiness(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackagePreparationReadiness:
        return self._get(reference, ImplementationPackagePreparationReadiness)

    def find_preparation_readiness(
        self,
        readiness_id: str,
        version: str,
    ) -> ImplementationPackagePreparationReadiness:
        return self._find(
            "implementation-package-preparation-readiness",
            readiness_id,
            version,
            ImplementationPackagePreparationReadiness,
        )

    def put_activation_specification(
        self,
        specification: ApprovedStrategyActivationSpecification,
    ) -> None:
        self._put(specification)

    def get_activation_specification(
        self,
        reference: ArtifactRef,
    ) -> ApprovedStrategyActivationSpecification:
        return self._get(reference, ApprovedStrategyActivationSpecification)

    def find_activation_specification(
        self,
        specification_id: str,
        version: str,
    ) -> ApprovedStrategyActivationSpecification:
        return self._find(
            "approved-strategy-activation-specification",
            specification_id,
            version,
            ApprovedStrategyActivationSpecification,
        )

    def put_activation_receipt(
        self,
        receipt: ImplementationPackageActivationReceipt,
    ) -> None:
        self._put(receipt)

    def get_activation_receipt(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackageActivationReceipt:
        return self._get(reference, ImplementationPackageActivationReceipt)

    def find_activation_receipt(
        self,
        activation_id: str,
        version: str,
    ) -> ImplementationPackageActivationReceipt:
        return self._find(
            "implementation-package-activation-receipt",
            activation_id,
            version,
            ImplementationPackageActivationReceipt,
        )

    def put_preparation_execution(
        self,
        receipt: ImplementationPackagePreparationExecutionReceipt,
    ) -> None:
        self._put(receipt)

    def get_preparation_execution(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackagePreparationExecutionReceipt:
        return self._get(
            reference,
            ImplementationPackagePreparationExecutionReceipt,
        )

    def find_preparation_execution(
        self,
        execution_id: str,
        version: str,
    ) -> ImplementationPackagePreparationExecutionReceipt:
        return self._find(
            "implementation-package-preparation-execution-receipt",
            execution_id,
            version,
            ImplementationPackagePreparationExecutionReceipt,
        )

    def put_runtime_execution(
        self,
        receipt: ImplementationPackageRuntimeExecutionReceipt,
    ) -> None:
        self._put(receipt)

    def get_runtime_execution(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackageRuntimeExecutionReceipt:
        return self._get(reference, ImplementationPackageRuntimeExecutionReceipt)

    def find_runtime_execution(
        self,
        execution_id: str,
        version: str,
    ) -> ImplementationPackageRuntimeExecutionReceipt:
        return self._find(
            "implementation-package-runtime-execution-receipt",
            execution_id,
            version,
            ImplementationPackageRuntimeExecutionReceipt,
        )

    def put_promotion(
        self,
        receipt: ImplementationPackagePromotionReceipt,
    ) -> None:
        self._put(receipt)

    def get_promotion(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackagePromotionReceipt:
        return self._get(reference, ImplementationPackagePromotionReceipt)

    def find_promotion(
        self,
        promotion_id: str,
        version: str,
    ) -> ImplementationPackagePromotionReceipt:
        return self._find(
            "implementation-package-promotion-receipt",
            promotion_id,
            version,
            ImplementationPackagePromotionReceipt,
        )

    def put_operational_readiness(
        self,
        receipt: ImplementationPackageOperationalReadinessReceipt,
    ) -> None:
        self._put(receipt)

    def get_operational_readiness(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackageOperationalReadinessReceipt:
        return self._get(
            reference,
            ImplementationPackageOperationalReadinessReceipt,
        )

    def find_operational_readiness(
        self,
        readiness_id: str,
        version: str,
    ) -> ImplementationPackageOperationalReadinessReceipt:
        return self._find(
            "implementation-package-operational-readiness-receipt",
            readiness_id,
            version,
            ImplementationPackageOperationalReadinessReceipt,
        )

    def put_deployment(
        self,
        receipt: ImplementationPackageDeploymentReceipt,
    ) -> None:
        self._put(receipt)

    def get_deployment(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackageDeploymentReceipt:
        return self._get(reference, ImplementationPackageDeploymentReceipt)

    def find_deployment(
        self,
        deployment_id: str,
        version: str,
    ) -> ImplementationPackageDeploymentReceipt:
        return self._find(
            "implementation-package-deployment-receipt",
            deployment_id,
            version,
            ImplementationPackageDeploymentReceipt,
        )

    def put_rollback(
        self,
        receipt: ImplementationPackageRollbackReceipt,
    ) -> None:
        self._put(receipt)

    def get_rollback(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackageRollbackReceipt:
        return self._get(reference, ImplementationPackageRollbackReceipt)

    def find_rollback(
        self,
        rollback_id: str,
        version: str,
    ) -> ImplementationPackageRollbackReceipt:
        return self._find(
            "implementation-package-rollback-receipt",
            rollback_id,
            version,
            ImplementationPackageRollbackReceipt,
        )

    def put_operational_observation(
        self,
        receipt: ImplementationPackageOperationalObservationReceipt,
    ) -> None:
        self._put(receipt)

    def get_operational_observation(
        self,
        reference: ArtifactRef,
    ) -> ImplementationPackageOperationalObservationReceipt:
        return self._get(
            reference,
            ImplementationPackageOperationalObservationReceipt,
        )

    def find_operational_observation(
        self,
        observation_id: str,
        version: str,
    ) -> ImplementationPackageOperationalObservationReceipt:
        return self._find(
            "implementation-package-operational-observation-receipt",
            observation_id,
            version,
            ImplementationPackageOperationalObservationReceipt,
        )

    def list_operational_observations(
        self,
        environment_id: str,
        expected_deployment_ref: ArtifactRef,
        *,
        limit: int = 20,
    ) -> tuple[list[ImplementationPackageOperationalObservationReceipt], int]:
        """Return immutable observations for one release baseline, newest first."""

        bounded_limit = max(1, min(limit, 50))
        directory = self.root / "implementation-package-operational-observation-receipt"
        observations: list[ImplementationPackageOperationalObservationReceipt] = []
        with self._lock:
            if directory.exists():
                for path in directory.glob("*.json"):
                    try:
                        payload = json.loads(path.read_text(encoding="utf-8"))
                        receipt = ImplementationPackageOperationalObservationReceipt.model_validate(
                            payload
                        )
                    except Exception as exc:
                        raise ImplementationArtifactConflict(
                            "Stored operational observation history contains an invalid artifact."
                        ) from exc
                    if (
                        receipt.environment_id == environment_id
                        and receipt.expected_deployment_ref == expected_deployment_ref
                    ):
                        observations.append(receipt)
        observations.sort(key=lambda receipt: receipt.observed_at, reverse=True)
        return observations[:bounded_limit], len(observations)

    def _active_deployment_path(self, environment_id: str) -> Path:
        key = hashlib.sha256(environment_id.encode("utf-8")).hexdigest()
        return self.root / "environment-release-bindings" / f"{key}.json"

    def get_active_deployment_ref(self, environment_id: str) -> ArtifactRef | None:
        path = self._active_deployment_path(environment_id)
        with self._lock:
            if not path.exists():
                return None
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if payload.get("environmentId") != environment_id:
                    raise ValueError("environment identity does not match")
                return ArtifactRef.model_validate(payload.get("activeDeploymentRef"))
            except Exception as exc:
                raise ImplementationArtifactConflict(
                    f"Active deployment binding for environment '{environment_id}' is invalid."
                ) from exc

    def get_active_deployment(
        self,
        environment_id: str,
    ) -> ImplementationPackageDeploymentReceipt | None:
        reference = self.get_active_deployment_ref(environment_id)
        return self.get_deployment(reference) if reference is not None else None

    def bind_active_deployment(
        self,
        receipt: ImplementationPackageDeploymentReceipt,
        *,
        expected_current_ref: ArtifactRef | None,
    ) -> None:
        """Persist a deployment event and atomically advance its environment binding."""

        path = self._active_deployment_path(receipt.environment_id)
        with self._lock:
            current = self.get_active_deployment_ref(receipt.environment_id)
            if current == receipt.artifact_ref:
                self._put(receipt)
                return
            if current != expected_current_ref:
                raise ImplementationArtifactConflict(
                    "The active deployment changed after it was reviewed. Refresh and retry "
                    "with the current deployment reference."
                )
            self._put(receipt)
            path.parent.mkdir(parents=True, exist_ok=True)
            encoded = canonical_json_bytes(
                {
                    "environmentId": receipt.environment_id,
                    "activeDeploymentRef": receipt.artifact_ref.to_wire(),
                }
            )
            temporary = path.with_suffix(".tmp")
            temporary.write_bytes(encoded)
            temporary.replace(path)

    def rollback_active_deployment(
        self,
        receipt: ImplementationPackageRollbackReceipt,
        *,
        expected_active_ref: ArtifactRef,
        restored_deployment_ref: ArtifactRef | None,
    ) -> None:
        """Persist rollback evidence and atomically revert the environment binding."""

        path = self._active_deployment_path(receipt.environment_id)
        with self._lock:
            current = self.get_active_deployment_ref(receipt.environment_id)
            desired = restored_deployment_ref
            if current == desired:
                try:
                    existing = self.get_rollback(receipt.artifact_ref)
                except ImplementationArtifactNotFound:
                    existing = None
                if existing == receipt:
                    return
            if current != expected_active_ref:
                raise ImplementationArtifactConflict(
                    "Only the currently active deployment can be rolled back. Refresh the "
                    "environment release status before retrying."
                )
            self._put(receipt)
            if restored_deployment_ref is None:
                path.unlink(missing_ok=True)
                return
            encoded = canonical_json_bytes(
                {
                    "environmentId": receipt.environment_id,
                    "activeDeploymentRef": restored_deployment_ref.to_wire(),
                }
            )
            temporary = path.with_suffix(".tmp")
            temporary.write_bytes(encoded)
            temporary.replace(path)


__all__ = [
    "FileImplementationArtifactStore",
    "ImplementationArtifactConflict",
    "ImplementationArtifactNotFound",
]
