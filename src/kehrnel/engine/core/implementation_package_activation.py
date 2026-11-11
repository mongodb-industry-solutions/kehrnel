"""Activate the exact approved strategies of one materialized package."""

from __future__ import annotations

import hashlib

from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_activation import (
    ApprovedStrategyActivationSpecification,
    ImplementationPackageActivationReceipt,
    ImplementationPackageActivationRequest,
    StrategyActivationSatisfaction,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.implementation_artifact_store import (
    FileImplementationArtifactStore,
)
from kehrnel.engine.core.implementation_package_preparation import activation_input_digest
from kehrnel.strategy_sdk import StrategyBindings


class ImplementationPackageActivationError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _digest(payload: dict) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _approved_specification(
    *,
    environment_id: str,
    package_ref: ArtifactRef,
    readiness_ref: ArtifactRef,
    activation_input,
    bindings_ref: str,
    approved_input_digest: str,
) -> ApprovedStrategyActivationSpecification:
    payload = {
        "contract": "approved-strategy-activation-specification/v1",
        "environmentId": environment_id,
        "packageRef": package_ref.to_wire(),
        "readinessRef": readiness_ref.to_wire(),
        "strategyInstanceId": activation_input.strategy_instance_id,
        "strategyRef": activation_input.strategy_ref.to_wire(),
        "domain": activation_input.domain,
        "configuration": activation_input.configuration,
        "bindingsRef": bindings_ref,
        "approval": "approved",
        "approvedInputDigest": approved_input_digest,
    }
    specification_digest = _digest(payload)
    specification_id = (
        "approved-strategy-activation."
        + specification_digest.removeprefix("sha256:")
    )
    return ApprovedStrategyActivationSpecification(
        **payload,
        artifactRef=ArtifactRef(
            kind="approved-strategy-activation-specification",
            id=specification_id,
            version=package_ref.version,
            digest=specification_digest,
        ),
        specificationDigest=specification_digest,
    )


async def activate_implementation_package_strategies(
    environment_id: str,
    request: ImplementationPackageActivationRequest,
    *,
    store: FileImplementationArtifactStore,
    runtime,
) -> ImplementationPackageActivationReceipt:
    """Persist reviewed specs and idempotently satisfy their runtime state."""

    readiness = store.get_preparation_readiness(request.readiness_ref)
    if readiness.environment_id != environment_id:
        raise ImplementationPackageActivationError(
            "IMPLEMENTATION_PACKAGE_PREPARATION_SCOPE_MISMATCH",
            "The preparation readiness belongs to a different environment.",
            status=409,
        )
    if readiness.package_ref != request.expected_package_ref:
        raise ImplementationPackageActivationError(
            "IMPLEMENTATION_PACKAGE_PRECONDITION_FAILED",
            "The ready package differs from the expected package.",
            status=409,
        )
    if readiness.state != "ready":
        raise ImplementationPackageActivationError(
            "IMPLEMENTATION_PACKAGE_PREPARATION_NOT_READY",
            "Every strategy must pass preparation readiness before activation.",
            status=409,
        )
    environment = runtime.registry.get_environment(environment_id)
    if environment is None:
        raise ImplementationPackageActivationError(
            "IMPLEMENTATION_ENVIRONMENT_NOT_FOUND",
            f"Environment '{environment_id}' does not exist.",
            status=404,
        )

    requirements = {
        item.strategy_instance_id: item for item in readiness.strategy_requirements
    }
    supplied = {
        item.strategy_instance_id: item for item in request.activation_inputs
    }
    if set(supplied) != set(requirements):
        raise ImplementationPackageActivationError(
            "IMPLEMENTATION_PACKAGE_ACTIVATION_INPUT_MISMATCH",
            "Activation inputs must exactly match every strategy in the ready package.",
            status=409,
        )

    specifications: list[ApprovedStrategyActivationSpecification] = []
    for instance_id in sorted(requirements):
        requirement = requirements[instance_id]
        item = supplied[instance_id]
        bindings_ref = (
            (item.bindings_ref or "").strip()
            or (getattr(environment, "bindings_ref", None) or "").strip()
        )
        if not item.approved or not bindings_ref:
            raise ImplementationPackageActivationError(
                "IMPLEMENTATION_PACKAGE_APPROVAL_STALE",
                "Activation approval or its secret binding reference is no longer valid.",
                status=409,
            )
        candidate_digest = activation_input_digest(item, bindings_ref=bindings_ref)
        if (
            requirement.state != "ready"
            or requirement.strategy_ref != item.strategy_ref
            or requirement.activation_spec_digest != candidate_digest
            or requirement.bindings_ref != bindings_ref
        ):
            raise ImplementationPackageActivationError(
                "IMPLEMENTATION_PACKAGE_APPROVAL_STALE",
                f"Activation input for '{instance_id}' changed after readiness approval.",
                status=409,
            )
        specification = _approved_specification(
            environment_id=environment_id,
            package_ref=readiness.package_ref,
            readiness_ref=readiness.artifact_ref,
            activation_input=item,
            bindings_ref=bindings_ref,
            approved_input_digest=candidate_digest,
        )
        store.put_activation_specification(specification)
        specifications.append(specification)

    satisfactions: list[StrategyActivationSatisfaction] = []
    for specification in specifications:
        activation = await runtime.activate(
            environment_id,
            specification.strategy_ref.id,
            specification.strategy_ref.version,
            specification.configuration,
            StrategyBindings(),
            allow_plaintext_bindings=False,
            domain=specification.domain,
            reason="implementation-package-activation",
            bindings_ref=specification.bindings_ref,
        )
        if not activation.activation_id:
            raise ImplementationPackageActivationError(
                "IMPLEMENTATION_PACKAGE_ACTIVATION_FAILED",
                f"Strategy '{specification.strategy_instance_id}' has no activation identity.",
                status=500,
            )
        satisfactions.append(
            StrategyActivationSatisfaction(
                strategyInstanceId=specification.strategy_instance_id,
                strategyRef=specification.strategy_ref,
                activationSpecificationRef=specification.artifact_ref,
                domain=activation.domain,
                runtimeActivationId=activation.activation_id,
                state="active",
            )
        )

    payload = {
        "contract": "implementation-package-activation-receipt/v1",
        "environmentId": environment_id,
        "packageRef": readiness.package_ref.to_wire(),
        "readinessRef": readiness.artifact_ref.to_wire(),
        "state": "activated",
        "strategyActivations": [item.to_wire() for item in satisfactions],
        "preparationExecuted": False,
        "runtimeExecuted": False,
        "nextRequiredAction": "execute_preparation_plan",
    }
    activation_digest = _digest(payload)
    activation_id = "package-activation." + activation_digest.removeprefix("sha256:")
    receipt = ImplementationPackageActivationReceipt(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-activation-receipt",
            id=activation_id,
            version=readiness.package_ref.version,
            digest=activation_digest,
        ),
        activationId=activation_id,
        activationDigest=activation_digest,
    )
    store.put_activation_receipt(receipt)
    return receipt


__all__ = [
    "ImplementationPackageActivationError",
    "activate_implementation_package_strategies",
]
