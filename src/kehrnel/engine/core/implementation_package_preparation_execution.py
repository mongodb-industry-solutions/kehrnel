"""Apply the exact preparation setup of an activated Implementation Package."""

from __future__ import annotations

import hashlib

from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_preparation_execution import (
    ImplementationPackagePreparationExecutionReceipt,
    ImplementationPackagePreparationExecutionRequest,
    PreparationStageExecutionReceipt,
    StrategyPreparationExecutionReceipt,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.implementation_artifact_store import (
    FileImplementationArtifactStore,
    ImplementationArtifactNotFound,
)


class ImplementationPackagePreparationExecutionError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _digest(payload) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _execution_id(
    environment_id: str,
    activation_receipt_ref: ArtifactRef,
    package_ref: ArtifactRef,
) -> str:
    identity = _digest(
        {
            "environmentId": environment_id,
            "activationReceiptRef": activation_receipt_ref.to_wire(),
            "packageRef": package_ref.to_wire(),
        }
    )
    return "package-preparation-execution." + identity.removeprefix("sha256:")


def _string_list(value) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


async def execute_implementation_package_preparation(
    environment_id: str,
    request: ImplementationPackagePreparationExecutionRequest,
    *,
    store: FileImplementationArtifactStore,
    runtime,
) -> ImplementationPackagePreparationExecutionReceipt:
    """Apply each activated strategy's setup and retain stage-level evidence.

    Strategy ``plan``/``apply`` installs collections, indexes and other static
    prerequisites. Trigger-driven ingestion and authoritative writes remain
    dormant until their declared event occurs; this receipt deliberately does
    not claim that tenant data has moved or that a runtime contract has run.
    """

    activation = store.get_activation_receipt(request.activation_receipt_ref)
    if activation.environment_id != environment_id:
        raise ImplementationPackagePreparationExecutionError(
            "IMPLEMENTATION_PACKAGE_ACTIVATION_SCOPE_MISMATCH",
            "The package activation belongs to a different environment.",
            status=409,
        )
    if activation.package_ref != request.expected_package_ref:
        raise ImplementationPackagePreparationExecutionError(
            "IMPLEMENTATION_PACKAGE_PRECONDITION_FAILED",
            "The activated package differs from the expected package.",
            status=409,
        )

    execution_id = _execution_id(
        environment_id,
        activation.artifact_ref,
        activation.package_ref,
    )
    try:
        return store.find_preparation_execution(
            execution_id,
            activation.package_ref.version,
        )
    except ImplementationArtifactNotFound:
        pass

    assembly = store.find_package_assembly(
        activation.package_ref.id,
        activation.package_ref.version,
    )
    if assembly.package_ref != activation.package_ref:
        raise ImplementationPackagePreparationExecutionError(
            "IMPLEMENTATION_PACKAGE_ASSEMBLY_MISMATCH",
            "The stored package assembly belongs to a different package.",
            status=409,
        )
    readiness = store.get_preparation_readiness(activation.readiness_ref)
    preparation_plan = assembly.resolution.preparation_plan
    if readiness.preparation_plan_ref != preparation_plan.artifact_ref:
        raise ImplementationPackagePreparationExecutionError(
            "IMPLEMENTATION_PACKAGE_PREPARATION_PLAN_MISMATCH",
            "The approved readiness does not reference this preparation plan.",
            status=409,
        )

    stages_by_strategy = {}
    for stage in preparation_plan.stages:
        stages_by_strategy.setdefault(stage.strategy_instance_id, []).append(stage)

    executions: list[StrategyPreparationExecutionReceipt] = []
    for strategy_activation in sorted(
        activation.strategy_activations,
        key=lambda item: item.strategy_instance_id,
    ):
        precondition = {"activationId": strategy_activation.runtime_activation_id}
        payload = {
            "domain": strategy_activation.domain,
            "__activation_precondition": precondition,
        }
        try:
            plan = await runtime.dispatch(environment_id, "plan", payload)
            if isinstance(plan, dict) and plan.get("artifacts", {}).get("plan_error"):
                raise ValueError(plan["artifacts"]["plan_error"])
            result = await runtime.dispatch(
                environment_id,
                "apply",
                {**payload, "plan": plan},
            )
        except Exception as exc:
            raise ImplementationPackagePreparationExecutionError(
                "IMPLEMENTATION_PACKAGE_PREPARATION_EXECUTION_FAILED",
                "Preparation setup failed for strategy "
                f"'{strategy_activation.strategy_instance_id}': {exc}",
                status=getattr(exc, "status", 502),
            ) from exc

        result = result if isinstance(result, dict) else {}
        skipped = result.get("skipped") or []
        if skipped:
            raise ImplementationPackagePreparationExecutionError(
                "IMPLEMENTATION_PACKAGE_PREPARATION_INCOMPLETE",
                "Preparation setup skipped required work for strategy "
                f"'{strategy_activation.strategy_instance_id}'.",
                status=409,
            )
        plan_digest = _digest(plan)
        result_digest = _digest(result)
        stage_receipts = [
            PreparationStageExecutionReceipt(
                stageId=stage.stage_id,
                kind=stage.kind,
                trigger=stage.trigger,
                strategyInstanceId=stage.strategy_instance_id,
                setupState="applied",
                dataExecutionState="awaiting_trigger",
                planDigest=plan_digest,
                applyResultDigest=result_digest,
            )
            for stage in sorted(
                stages_by_strategy.get(strategy_activation.strategy_instance_id, []),
                key=lambda item: item.stage_id,
            )
        ]
        executions.append(
            StrategyPreparationExecutionReceipt(
                strategyInstanceId=strategy_activation.strategy_instance_id,
                strategyRef=strategy_activation.strategy_ref,
                activationSpecificationRef=(
                    strategy_activation.activation_specification_ref
                ),
                runtimeActivationId=strategy_activation.runtime_activation_id,
                state="applied",
                planDigest=plan_digest,
                applyResultDigest=result_digest,
                createdArtifacts=_string_list(result.get("created")),
                updatedArtifacts=_string_list(result.get("updated")),
                warnings=_string_list(result.get("warnings")),
                stageReceipts=stage_receipts,
            )
        )

    payload = {
        "contract": "implementation-package-preparation-execution-receipt/v1",
        "executionId": execution_id,
        "environmentId": environment_id,
        "packageRef": activation.package_ref.to_wire(),
        "activationReceiptRef": activation.artifact_ref.to_wire(),
        "preparationPlanRef": preparation_plan.artifact_ref.to_wire(),
        "state": "prepared",
        "strategyExecutions": [item.to_wire() for item in executions],
        "preparationSetupExecuted": True,
        "preparationDataExecution": "awaiting_trigger",
        "runtimeExecuted": False,
        "nextRequiredAction": "execute_runtime_contract",
    }
    execution_digest = _digest(payload)
    receipt = ImplementationPackagePreparationExecutionReceipt(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-preparation-execution-receipt",
            id=execution_id,
            version=activation.package_ref.version,
            digest=execution_digest,
        ),
        executionDigest=execution_digest,
    )
    store.put_preparation_execution(receipt)
    return receipt


__all__ = [
    "ImplementationPackagePreparationExecutionError",
    "execute_implementation_package_preparation",
]
