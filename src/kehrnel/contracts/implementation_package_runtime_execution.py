"""Contracts for executing one governed interaction from an activated package."""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import Field, JsonValue, model_validator

from .architecture_foundation import (
    AcknowledgementLevel,
    ConsistencyRequirement,
    InteractionBehavior,
    RuntimeQueryPlanTrace,
)
from .artifacts import ArtifactRef, ContractModel, Sha256Digest


class ImplementationPackageRuntimeExecutionRequest(ContractModel):
    contract: Literal["implementation-package-runtime-execution-request/v1"]
    preparation_execution_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    interaction_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    request_payload: dict[str, JsonValue]
    idempotency_key: Optional[str] = Field(default=None, min_length=8, max_length=256)

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ImplementationPackageRuntimeExecutionRequest":
        if self.preparation_execution_ref.kind != (
            "implementation-package-preparation-execution-receipt"
        ):
            raise ValueError(
                "preparationExecutionRef.kind must be "
                "'implementation-package-preparation-execution-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        return self


class RuntimePolicyDecision(ContractModel):
    artifact_ref: ArtifactRef
    decision: Literal["allow"]
    scope: Literal["execute_activated_package_interaction"]
    package_ref: ArtifactRef
    preparation_execution_ref: ArtifactRef
    interaction_id: str = Field(min_length=1)
    decision_digest: Sha256Digest

    @model_validator(mode="after")
    def decision_is_coherent(self) -> "RuntimePolicyDecision":
        if self.artifact_ref.kind != "policy-decision":
            raise ValueError("artifactRef.kind must be 'policy-decision'")
        if self.artifact_ref.digest != self.decision_digest:
            raise ValueError("artifactRef.digest must equal decisionDigest")
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.preparation_execution_ref.kind != (
            "implementation-package-preparation-execution-receipt"
        ):
            raise ValueError(
                "preparationExecutionRef.kind must be "
                "'implementation-package-preparation-execution-receipt'"
            )
        return self


class RuntimeResultEvidence(ContractModel):
    result_digest: Sha256Digest
    result_type: Literal["object", "array", "scalar", "null"]
    item_count: int | None = Field(default=None, ge=0)
    contains_result_payload: Literal[False] = False


class RuntimeWriteAcknowledgement(ContractModel):
    behavior: Literal[InteractionBehavior.CREATE, InteractionBehavior.UPDATE]
    semantic_authority_ref: ArtifactRef
    record_authority_policy_ref: ArtifactRef
    requester_ref: str = Field(min_length=1)
    provider_ref: str = Field(min_length=1)
    acknowledgement: AcknowledgementLevel
    consistency: ConsistencyRequirement
    request_digest: Sha256Digest
    idempotency_key_digest: Sha256Digest
    resource_type: str = Field(min_length=1)
    resource_id: str = Field(min_length=1)
    version_uid: str = Field(min_length=1)
    change_type: Literal["creation", "modification"]
    etag: str = Field(min_length=1)
    replayed: bool = False

    @model_validator(mode="after")
    def acknowledgement_is_authoritative(self) -> "RuntimeWriteAcknowledgement":
        if self.record_authority_policy_ref.kind != "record-authority-policy":
            raise ValueError(
                "recordAuthorityPolicyRef.kind must be 'record-authority-policy'"
            )
        if self.acknowledgement is not AcknowledgementLevel.DURABLE_AUTHORITATIVE:
            raise ValueError(
                "authoritative writes require durable_authoritative acknowledgement"
            )
        return self


class ImplementationPackageRuntimeExecutionReceipt(ContractModel):
    contract: Literal["implementation-package-runtime-execution-receipt/v1"]
    artifact_ref: ArtifactRef
    execution_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    package_ref: ArtifactRef
    preparation_execution_ref: ArtifactRef
    resolved_plan_ref: ArtifactRef
    runtime_plan_ref: ArtifactRef
    interaction_id: str = Field(min_length=1)
    behavior: InteractionBehavior
    route_id: str = Field(min_length=1)
    strategy_instance_id: str = Field(min_length=1)
    policy_decision: RuntimePolicyDecision
    query_plan_trace: Optional[RuntimeQueryPlanTrace] = None
    write_acknowledgement: Optional[RuntimeWriteAcknowledgement] = None
    result_evidence: RuntimeResultEvidence
    state: Literal["executed"]
    runtime_executed: Literal[True] = True
    evidence_persistence: Literal["durable"] = "durable"
    result_persistence: Literal["not_persisted"] = "not_persisted"
    next_required_action: Literal["review_runtime_evidence"]
    execution_digest: Sha256Digest

    @model_validator(mode="after")
    def receipt_is_coherent(self) -> "ImplementationPackageRuntimeExecutionReceipt":
        if self.artifact_ref.kind != "implementation-package-runtime-execution-receipt":
            raise ValueError(
                "artifactRef.kind must be "
                "'implementation-package-runtime-execution-receipt'"
            )
        if self.artifact_ref.digest != self.execution_digest:
            raise ValueError("artifactRef.digest must equal executionDigest")
        if self.package_ref.kind != "implementation-package":
            raise ValueError("packageRef.kind must be 'implementation-package'")
        if self.runtime_plan_ref.kind != "runtime-execution-plan":
            raise ValueError("runtimePlanRef.kind must be 'runtime-execution-plan'")
        if self.behavior in {
            InteractionBehavior.QUERY,
            InteractionBehavior.SEARCH,
        }:
            if self.query_plan_trace is None or self.write_acknowledgement is not None:
                raise ValueError("query execution requires only queryPlanTrace")
            if self.query_plan_trace.interaction_id != self.interaction_id:
                raise ValueError("queryPlanTrace interactionId must match interactionId")
            if self.query_plan_trace.policy_decision_ref != self.policy_decision.artifact_ref:
                raise ValueError("queryPlanTrace must reference the embedded policy decision")
            if self.query_plan_trace.resolved_plan_ref != self.resolved_plan_ref:
                raise ValueError("queryPlanTrace resolvedPlanRef must match resolvedPlanRef")
        elif self.behavior in {InteractionBehavior.CREATE, InteractionBehavior.UPDATE}:
            if self.write_acknowledgement is None or self.query_plan_trace is not None:
                raise ValueError("authoritative write requires only writeAcknowledgement")
            if self.write_acknowledgement.behavior is not self.behavior:
                raise ValueError("writeAcknowledgement behavior must match behavior")
        else:
            raise ValueError("runtime receipt behavior is not executable")
        return self


class ImplementationPackageRuntimeExecutionResponse(ContractModel):
    contract: Literal["implementation-package-runtime-execution-response/v1"]
    receipt: ImplementationPackageRuntimeExecutionReceipt
    result: JsonValue
    result_persistence: Literal["not_persisted"] = "not_persisted"


__all__ = [
    "ImplementationPackageRuntimeExecutionReceipt",
    "ImplementationPackageRuntimeExecutionRequest",
    "ImplementationPackageRuntimeExecutionResponse",
    "RuntimePolicyDecision",
    "RuntimeResultEvidence",
    "RuntimeWriteAcknowledgement",
]
