"""Contracts for reviewing tenant inputs before package preparation executes."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import Field, model_validator

from .artifacts import ArtifactRef, ContractModel, Sha256Digest


class StrategyActivationInput(ContractModel):
    """Tenant-supplied, non-secret activation input for one resolved strategy."""

    strategy_instance_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    strategy_ref: ArtifactRef
    domain: str = Field(min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    configuration: Dict[str, Any]
    bindings_ref: Optional[str] = Field(default=None, min_length=1)
    approved: bool = False

    @model_validator(mode="after")
    def strategy_ref_is_coherent(self) -> "StrategyActivationInput":
        if self.strategy_ref.kind != "strategy":
            raise ValueError("strategyRef.kind must be 'strategy'")
        return self


class ImplementationPackagePreparationReadinessRequest(ContractModel):
    contract: Literal["implementation-package-preparation-readiness-request/v1"]
    materialization_receipt_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    activation_inputs: List[StrategyActivationInput] = Field(default_factory=list)

    @model_validator(mode="after")
    def request_is_coherent(
        self,
    ) -> "ImplementationPackagePreparationReadinessRequest":
        if (
            self.materialization_receipt_ref.kind
            != "implementation-package-materialization-receipt"
        ):
            raise ValueError(
                "materializationReceiptRef.kind must be "
                "'implementation-package-materialization-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        identities = [item.strategy_instance_id for item in self.activation_inputs]
        if len(identities) != len(set(identities)):
            raise ValueError("activationInputs must contain unique strategyInstanceIds")
        return self


class StrategyPreparationRequirement(ContractModel):
    strategy_instance_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    strategy_ref: ArtifactRef
    state: Literal["ready", "blocked"]
    activation_spec_digest: Optional[Sha256Digest] = None
    bindings_ref: Optional[str] = None
    configuration_keys: List[str] = Field(default_factory=list)
    validation_errors: List[str] = Field(default_factory=list)
    missing_inputs: List[
        Literal[
            "activation_input",
            "strategy_reference",
            "domain",
            "configuration",
            "database",
            "bindings_ref",
            "approval",
        ]
    ] = Field(default_factory=list)


class ImplementationPackagePreparationReadiness(ContractModel):
    contract: Literal["implementation-package-preparation-readiness/v1"]
    artifact_ref: ArtifactRef
    readiness_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$"
    )
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    materialization_receipt_ref: ArtifactRef
    package_ref: ArtifactRef
    preparation_plan_ref: ArtifactRef
    state: Literal["ready", "blocked"]
    strategy_requirements: List[StrategyPreparationRequirement] = Field(
        min_length=1
    )
    deferred_stage_ids: List[str] = Field(default_factory=list)
    preparation_executed: Literal[False] = False
    runtime_executed: Literal[False] = False
    next_required_action: Literal[
        "provide_activation_inputs", "activate_strategies"
    ]
    readiness_digest: Sha256Digest

    @model_validator(mode="after")
    def readiness_is_coherent(self) -> "ImplementationPackagePreparationReadiness":
        if self.artifact_ref.kind != "implementation-package-preparation-readiness":
            raise ValueError(
                "artifactRef.kind must be "
                "'implementation-package-preparation-readiness'"
            )
        if (
            self.materialization_receipt_ref.kind
            != "implementation-package-materialization-receipt"
        ):
            raise ValueError(
                "materializationReceiptRef.kind must be "
                "'implementation-package-materialization-receipt'"
            )
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.preparation_plan_ref.kind != "preparation-plan":
            raise ValueError("preparationPlanRef.kind must be 'preparation-plan'")
        if self.artifact_ref.digest != self.readiness_digest:
            raise ValueError("artifactRef.digest must equal readinessDigest")
        blocked = any(item.state == "blocked" for item in self.strategy_requirements)
        if blocked != (self.state == "blocked"):
            raise ValueError("readiness state must match strategy requirements")
        expected_action = (
            "provide_activation_inputs" if blocked else "activate_strategies"
        )
        if self.next_required_action != expected_action:
            raise ValueError("nextRequiredAction does not match readiness state")
        return self


__all__ = [
    "ImplementationPackagePreparationReadiness",
    "ImplementationPackagePreparationReadinessRequest",
    "StrategyActivationInput",
    "StrategyPreparationRequirement",
]
