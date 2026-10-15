"""Contracts for applying a package's preparation setup to one tenant target."""

from __future__ import annotations

from typing import List, Literal

from pydantic import Field, model_validator

from .architecture_foundation import PreparationStageKind, PreparationTrigger
from .artifacts import ArtifactRef, ContractModel, Sha256Digest


class ImplementationPackagePreparationExecutionRequest(ContractModel):
    contract: Literal["implementation-package-preparation-execution-request/v1"]
    activation_receipt_ref: ArtifactRef
    expected_package_ref: ArtifactRef

    @model_validator(mode="after")
    def request_is_coherent(
        self,
    ) -> "ImplementationPackagePreparationExecutionRequest":
        if self.activation_receipt_ref.kind != "implementation-package-activation-receipt":
            raise ValueError(
                "activationReceiptRef.kind must be "
                "'implementation-package-activation-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        return self


class PreparationStageExecutionReceipt(ContractModel):
    """Evidence that setup for one trigger-driven preparation stage is installed."""

    stage_id: str = Field(min_length=1)
    kind: PreparationStageKind
    trigger: PreparationTrigger
    strategy_instance_id: str = Field(min_length=1)
    setup_state: Literal["applied"]
    data_execution_state: Literal["awaiting_trigger"]
    plan_digest: Sha256Digest
    apply_result_digest: Sha256Digest


class StrategyPreparationExecutionReceipt(ContractModel):
    strategy_instance_id: str = Field(min_length=1)
    strategy_ref: ArtifactRef
    activation_specification_ref: ArtifactRef
    runtime_activation_id: str = Field(min_length=1)
    state: Literal["applied"]
    plan_digest: Sha256Digest
    apply_result_digest: Sha256Digest
    created_artifacts: List[str] = Field(default_factory=list)
    updated_artifacts: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    stage_receipts: List[PreparationStageExecutionReceipt] = Field(
        default_factory=list
    )


class ImplementationPackagePreparationExecutionReceipt(ContractModel):
    contract: Literal["implementation-package-preparation-execution-receipt/v1"]
    artifact_ref: ArtifactRef
    execution_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    package_ref: ArtifactRef
    activation_receipt_ref: ArtifactRef
    preparation_plan_ref: ArtifactRef
    state: Literal["prepared"]
    strategy_executions: List[StrategyPreparationExecutionReceipt] = Field(
        min_length=1
    )
    preparation_setup_executed: Literal[True] = True
    preparation_data_execution: Literal["awaiting_trigger"] = "awaiting_trigger"
    runtime_executed: Literal[False] = False
    next_required_action: Literal["execute_runtime_contract"]
    execution_digest: Sha256Digest

    @model_validator(mode="after")
    def receipt_is_coherent(
        self,
    ) -> "ImplementationPackagePreparationExecutionReceipt":
        if self.artifact_ref.kind != "implementation-package-preparation-execution-receipt":
            raise ValueError(
                "artifactRef.kind must be "
                "'implementation-package-preparation-execution-receipt'"
            )
        if self.artifact_ref.digest != self.execution_digest:
            raise ValueError("artifactRef.digest must equal executionDigest")
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.activation_receipt_ref.kind != "implementation-package-activation-receipt":
            raise ValueError(
                "activationReceiptRef.kind must be "
                "'implementation-package-activation-receipt'"
            )
        if self.preparation_plan_ref.kind != "preparation-plan":
            raise ValueError("preparationPlanRef.kind must be 'preparation-plan'")
        strategy_ids = [item.strategy_instance_id for item in self.strategy_executions]
        if len(strategy_ids) != len(set(strategy_ids)):
            raise ValueError("strategyExecutions must contain unique strategyInstanceIds")
        stage_ids = [
            stage.stage_id
            for execution in self.strategy_executions
            for stage in execution.stage_receipts
        ]
        if len(stage_ids) != len(set(stage_ids)):
            raise ValueError("stageReceipts must contain unique stageIds")
        return self


__all__ = [
    "ImplementationPackagePreparationExecutionReceipt",
    "ImplementationPackagePreparationExecutionRequest",
    "PreparationStageExecutionReceipt",
    "StrategyPreparationExecutionReceipt",
]
