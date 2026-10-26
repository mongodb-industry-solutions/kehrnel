"""Deterministic, workload-aware access planning contracts.

These contracts describe why a MongoDB access structure is proposed before
any collection, index, projection, or export is created.  They are deliberately
separate from model-generated advice: an agent may propose changes to the
inputs, while Core owns the reproducible plan and its digest.
"""

from __future__ import annotations

from enum import Enum
from typing import List, Literal, Optional

from pydantic import Field, model_validator

from .artifacts import CapabilityMaturity, ContractModel, ObservationKind, Sha256Digest
from .source_architecture import CandidateStatus, CompilationIssue, StrategyCandidate


class AccessMechanism(str, Enum):
    DIRECT_DOCUMENT = "direct_document"
    TARGETED_INDEX = "targeted_index"
    COMPUTED_FIELD = "computed_field"
    ATLAS_SEARCH_INDEX = "atlas_search_index"
    ATLAS_VECTOR_INDEX = "atlas_vector_index"
    MATERIALIZED_PROJECTION = "materialized_projection"
    CHANGE_STREAM = "change_stream"
    ISOLATED_EXPORT = "isolated_export"


class AccessConsistency(str, Enum):
    AUTHORITATIVE = "authoritative"
    SYNCHRONOUS_DERIVED = "synchronous_derived"
    ASYNCHRONOUS_DERIVED = "asynchronous_derived"
    SNAPSHOT = "snapshot"


class AccessStructurePlan(ContractModel):
    access_structure_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$"
    )
    mechanism: AccessMechanism
    strategy_id: str = Field(min_length=1)
    serves_consumption_ids: List[str] = Field(min_length=1)
    source_instance_ids: List[str] = Field(default_factory=list)
    rationale: str = Field(min_length=1)
    consistency: AccessConsistency
    requires_derived_copy: bool = False
    rebuild_source: Optional[str] = None
    retirement_condition: Optional[str] = None
    freshness_target_seconds: Optional[int] = Field(default=None, ge=0)
    capability_maturity: CapabilityMaturity
    observation_kind: ObservationKind

    @model_validator(mode="after")
    def structure_is_rebuildable_and_referenced(self) -> "AccessStructurePlan":
        if len(self.serves_consumption_ids) != len(set(self.serves_consumption_ids)):
            raise ValueError("access structure contains duplicate consumption references")
        if len(self.source_instance_ids) != len(set(self.source_instance_ids)):
            raise ValueError("access structure contains duplicate source references")
        if self.mechanism is not AccessMechanism.DIRECT_DOCUMENT:
            if not self.rebuild_source:
                raise ValueError("derived access structures require rebuildSource")
            if not self.retirement_condition:
                raise ValueError("derived access structures require retirementCondition")
        if self.requires_derived_copy and self.consistency is AccessConsistency.AUTHORITATIVE:
            raise ValueError("a derived copy cannot use authoritative consistency")
        return self


class RejectedAccessAlternative(ContractModel):
    alternative_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$"
    )
    mechanism: AccessMechanism
    consumption_id: str = Field(min_length=1)
    reason_code: str = Field(min_length=1)
    explanation: str = Field(min_length=1)


class ProjectionBudget(ContractModel):
    observation_kind: Literal[ObservationKind.ESTIMATED]
    access_structure_count: int = Field(ge=0)
    index_like_structure_count: int = Field(ge=0)
    computed_field_count: int = Field(ge=0)
    derived_copy_count: int = Field(ge=0)
    external_export_count: int = Field(ge=0)
    serving_consumption_count: int = Field(ge=0)


class AccessPlan(ContractModel):
    contract: Literal["access-plan/v1"]
    blueprint_id: str = Field(min_length=1)
    blueprint_revision: int = Field(ge=1)
    candidate_digest: Sha256Digest
    status: CandidateStatus
    requires_approval: bool
    access_plan_digest: Sha256Digest
    structures: List[AccessStructurePlan] = Field(min_length=1)
    rejected_alternatives: List[RejectedAccessAlternative] = Field(default_factory=list)
    projection_budget: ProjectionBudget
    issues: List[CompilationIssue] = Field(default_factory=list)


class ArchitectureProposal(ContractModel):
    """The complete explainable proposal consumed by the visual Builder."""

    contract: Literal["architecture-proposal/v1"]
    proposal_origin: Literal["deterministic_core"]
    blueprint_id: str = Field(min_length=1)
    blueprint_revision: int = Field(ge=1)
    proposal_digest: Sha256Digest
    requires_approval: bool
    strategy_candidate: StrategyCandidate
    access_plan: AccessPlan

    @model_validator(mode="after")
    def proposal_parts_refer_to_same_blueprint(self) -> "ArchitectureProposal":
        if (
            self.strategy_candidate.blueprint_id != self.blueprint_id
            or self.access_plan.blueprint_id != self.blueprint_id
        ):
            raise ValueError("architecture proposal parts must use the same blueprintId")
        if (
            self.strategy_candidate.blueprint_revision != self.blueprint_revision
            or self.access_plan.blueprint_revision != self.blueprint_revision
        ):
            raise ValueError("architecture proposal parts must use the same blueprintRevision")
        if self.access_plan.candidate_digest != self.strategy_candidate.candidate_digest:
            raise ValueError("access plan must reference the embedded Strategy Candidate")
        return self
