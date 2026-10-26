"""Contracts for immutable package activation specifications and receipts."""

from __future__ import annotations

from typing import Dict, List, Literal, Any

from pydantic import Field, model_validator

from .artifacts import ArtifactRef, ContractModel, Sha256Digest
from .implementation_package_preparation import StrategyActivationInput


class ImplementationPackageActivationRequest(ContractModel):
    contract: Literal["implementation-package-activation-request/v1"]
    readiness_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    activation_inputs: List[StrategyActivationInput] = Field(min_length=1)

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ImplementationPackageActivationRequest":
        if self.readiness_ref.kind != "implementation-package-preparation-readiness":
            raise ValueError(
                "readinessRef.kind must be "
                "'implementation-package-preparation-readiness'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        identities = [item.strategy_instance_id for item in self.activation_inputs]
        if len(identities) != len(set(identities)):
            raise ValueError("activationInputs must contain unique strategyInstanceIds")
        return self


class ApprovedStrategyActivationSpecification(ContractModel):
    """Immutable, non-secret tenant approval consumed by the runtime."""

    contract: Literal["approved-strategy-activation-specification/v1"]
    artifact_ref: ArtifactRef
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    package_ref: ArtifactRef
    readiness_ref: ArtifactRef
    strategy_instance_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    strategy_ref: ArtifactRef
    domain: str = Field(min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    configuration: Dict[str, Any]
    bindings_ref: str = Field(min_length=1)
    approval: Literal["approved"]
    approved_input_digest: Sha256Digest
    specification_digest: Sha256Digest

    @model_validator(mode="after")
    def specification_is_coherent(
        self,
    ) -> "ApprovedStrategyActivationSpecification":
        if self.artifact_ref.kind != "approved-strategy-activation-specification":
            raise ValueError(
                "artifactRef.kind must be "
                "'approved-strategy-activation-specification'"
            )
        if self.artifact_ref.digest != self.specification_digest:
            raise ValueError("artifactRef.digest must equal specificationDigest")
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.readiness_ref.kind != "implementation-package-preparation-readiness":
            raise ValueError(
                "readinessRef.kind must be "
                "'implementation-package-preparation-readiness'"
            )
        if self.strategy_ref.kind != "strategy":
            raise ValueError("strategyRef.kind must be 'strategy'")
        return self


class StrategyActivationSatisfaction(ContractModel):
    strategy_instance_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    strategy_ref: ArtifactRef
    activation_specification_ref: ArtifactRef
    domain: str = Field(min_length=1)
    runtime_activation_id: str = Field(min_length=1)
    state: Literal["active"]


class ImplementationPackageActivationReceipt(ContractModel):
    contract: Literal["implementation-package-activation-receipt/v1"]
    artifact_ref: ArtifactRef
    activation_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$"
    )
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    package_ref: ArtifactRef
    readiness_ref: ArtifactRef
    state: Literal["activated"]
    strategy_activations: List[StrategyActivationSatisfaction] = Field(min_length=1)
    preparation_executed: Literal[False] = False
    runtime_executed: Literal[False] = False
    next_required_action: Literal["execute_preparation_plan"]
    activation_digest: Sha256Digest

    @model_validator(mode="after")
    def receipt_is_coherent(self) -> "ImplementationPackageActivationReceipt":
        if self.artifact_ref.kind != "implementation-package-activation-receipt":
            raise ValueError(
                "artifactRef.kind must be 'implementation-package-activation-receipt'"
            )
        if self.artifact_ref.digest != self.activation_digest:
            raise ValueError("artifactRef.digest must equal activationDigest")
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.readiness_ref.kind != "implementation-package-preparation-readiness":
            raise ValueError(
                "readinessRef.kind must be "
                "'implementation-package-preparation-readiness'"
            )
        return self


__all__ = [
    "ApprovedStrategyActivationSpecification",
    "ImplementationPackageActivationReceipt",
    "ImplementationPackageActivationRequest",
    "StrategyActivationSatisfaction",
]
