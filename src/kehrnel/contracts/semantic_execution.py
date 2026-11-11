"""Contracts for choosing semantic authority and runtime execution bindings.

These contracts deliberately do not make ContextObjects a universal canonical
model.  A consumption contract may preserve a native semantic authority, use a
derived ContextObject view, or compose several authorities into a purpose-built
ContextObject.
"""

from __future__ import annotations

from enum import Enum
import hashlib
from typing import List, Literal, Optional

from pydantic import Field, model_validator

from .artifacts import ArtifactRef, ContractModel, Sha256Digest
from .package_digest import canonical_json_bytes
from .source_architecture import (
    CandidateStatus,
    CompilationIssue,
    StrategyCompilationRequest,
)


class SemanticMode(str, Enum):
    """How a consumption contract obtains its semantic model."""

    NATIVE = "native"
    DERIVED = "derived"
    COMPOSED = "composed"


class SemanticAuthorityKind(str, Enum):
    OPENEHR = "openehr"
    FHIR = "fhir"
    CDISC = "cdisc"
    CONTEXT_OBJECT = "context_object"
    SOURCE_SCHEMA = "source_schema"


class SemanticAuthorityRole(str, Enum):
    AUTHORITATIVE = "authoritative"
    TARGET_SEMANTICS = "target_semantics"
    COMPOSITION = "composition"


class ExecutionBindingKind(str, Enum):
    OPENEHR_AQL = "openehr_aql"
    FHIR_SEARCH = "fhir_search"
    CDISC_QUERY = "cdisc_query"
    MONGODB_MQL = "mongodb_mql"
    SERVICE = "service"
    STREAM = "stream"
    EXPORT = "export"


class ContextObjectDefinitionStatus(str, Enum):
    AVAILABLE = "available"
    GUIDED = "guided"
    PLANNED = "planned"


class ContextObjectRuntimeBinding(str, Enum):
    EXECUTABLE = "executable"
    DESIGN_ONLY = "design_only"


class ContextObjectDefinitionSummary(ContractModel):
    """Pinned semantic-composition definition offered to architecture authors."""

    artifact_ref: ArtifactRef
    name: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    subject: str = Field(min_length=1)
    result_shape: str = Field(min_length=1)
    supported_authority_kinds: List[SemanticAuthorityKind] = Field(min_length=1)
    output_families: List[str] = Field(min_length=1)
    status: ContextObjectDefinitionStatus
    runtime_binding: ContextObjectRuntimeBinding

    @model_validator(mode="after")
    def definition_is_coherent(self) -> "ContextObjectDefinitionSummary":
        if self.artifact_ref.kind != "context-object-definition":
            raise ValueError("artifactRef.kind must be 'context-object-definition'")
        if len(self.supported_authority_kinds) != len(
            set(self.supported_authority_kinds)
        ):
            raise ValueError("supportedAuthorityKinds must be unique")
        if SemanticAuthorityKind.CONTEXT_OBJECT in self.supported_authority_kinds:
            raise ValueError("a ContextObject cannot list itself as a source authority")
        return self


class ContextObjectDefinitionCatalog(ContractModel):
    contract: Literal["context-object-definition-catalog/v1"]
    definitions: List[ContextObjectDefinitionSummary] = Field(default_factory=list)


class ConsumptionSemanticRequirement(ContractModel):
    """Optional semantic-mode decision supplied for one consumption contract."""

    consumption_id: str = Field(min_length=1)
    mode: SemanticMode
    context_object_ref: Optional[ArtifactRef] = None

    @model_validator(mode="after")
    def mode_has_the_right_contract(self) -> "ConsumptionSemanticRequirement":
        if self.mode is SemanticMode.NATIVE and self.context_object_ref is not None:
            raise ValueError("native semantic mode cannot reference a ContextObject")
        if self.mode in {SemanticMode.DERIVED, SemanticMode.COMPOSED}:
            if self.context_object_ref is None:
                raise ValueError(
                    f"{self.mode.value} semantic mode requires contextObjectRef"
                )
            if self.context_object_ref.kind != "context-object-definition":
                raise ValueError(
                    "contextObjectRef.kind must be 'context-object-definition'"
                )
        return self


class SemanticExecutionPlanningRequest(ContractModel):
    contract: Literal["semantic-execution-planning-request/v1"]
    strategy_request: StrategyCompilationRequest
    semantic_requirements: List[ConsumptionSemanticRequirement] = Field(
        default_factory=list
    )

    @model_validator(mode="after")
    def requirements_reference_consumptions(self) -> "SemanticExecutionPlanningRequest":
        requirement_ids = [item.consumption_id for item in self.semantic_requirements]
        if len(requirement_ids) != len(set(requirement_ids)):
            raise ValueError("semanticRequirements must reference unique consumptions")
        known = {
            item.consumption_id
            for item in self.strategy_request.blueprint.consumptions
        }
        missing = sorted(set(requirement_ids) - known)
        if missing:
            raise ValueError(
                f"semanticRequirements reference unknown consumptions: {missing}"
            )
        return self


class SemanticAuthorityPlan(ContractModel):
    authority_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._:-]*$"
    )
    kind: SemanticAuthorityKind
    role: SemanticAuthorityRole
    source_instance_ids: List[str] = Field(default_factory=list)
    source_profile_refs: List[ArtifactRef] = Field(default_factory=list)
    definition_refs: List[ArtifactRef] = Field(default_factory=list)

    @model_validator(mode="after")
    def references_are_unique(self) -> "SemanticAuthorityPlan":
        if len(self.source_instance_ids) != len(set(self.source_instance_ids)):
            raise ValueError("semantic authority sourceInstanceIds must be unique")
        identities = [
            (ref.kind, ref.id, ref.version, ref.digest)
            for ref in [*self.source_profile_refs, *self.definition_refs]
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("semantic authority artifact references must be unique")
        return self


class ConsumptionSemanticPlan(ContractModel):
    consumption_id: str = Field(min_length=1)
    mode: SemanticMode
    authority_ids: List[str] = Field(min_length=1)
    context_object_ref: Optional[ArtifactRef] = None
    resolved: bool
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def mode_is_coherent(self) -> "ConsumptionSemanticPlan":
        if len(self.authority_ids) != len(set(self.authority_ids)):
            raise ValueError("consumption semantic authorityIds must be unique")
        if self.mode is SemanticMode.NATIVE and self.context_object_ref is not None:
            raise ValueError("native semantic plans cannot use a ContextObject")
        if (
            self.mode in {SemanticMode.DERIVED, SemanticMode.COMPOSED}
            and self.resolved
            and self.context_object_ref is None
        ):
            raise ValueError("a resolved derived or composed plan requires contextObjectRef")
        return self


class ExecutionSubplan(ContractModel):
    subplan_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._:-]*$"
    )
    consumption_id: str = Field(min_length=1)
    binding_kind: ExecutionBindingKind
    strategy_id: str = Field(min_length=1)
    semantic_mode: SemanticMode
    authority_ids: List[str] = Field(min_length=1)
    depends_on: List[str] = Field(default_factory=list)
    preserves_native_semantics: bool


class SemanticExecutionPlan(ContractModel):
    """Design-time execution envelope; native subplans remain native."""

    contract: Literal["semantic-execution-plan/v1"]
    blueprint_id: str = Field(min_length=1)
    blueprint_revision: int = Field(ge=1)
    strategy_candidate_digest: Sha256Digest
    status: CandidateStatus
    requires_approval: bool
    plan_digest: Sha256Digest
    authorities: List[SemanticAuthorityPlan] = Field(min_length=1)
    consumption_plans: List[ConsumptionSemanticPlan] = Field(min_length=1)
    subplans: List[ExecutionSubplan] = Field(min_length=1)
    issues: List[CompilationIssue] = Field(default_factory=list)

    @model_validator(mode="after")
    def internal_references_are_resolved(self) -> "SemanticExecutionPlan":
        authority_ids = [item.authority_id for item in self.authorities]
        if len(authority_ids) != len(set(authority_ids)):
            raise ValueError("semantic execution plan contains duplicate authorityId values")
        known_authorities = set(authority_ids)
        consumption_ids = [item.consumption_id for item in self.consumption_plans]
        if len(consumption_ids) != len(set(consumption_ids)):
            raise ValueError("semantic execution plan contains duplicate consumptionId values")
        for item in [*self.consumption_plans, *self.subplans]:
            missing = sorted(set(item.authority_ids) - known_authorities)
            if missing:
                raise ValueError(
                    f"semantic execution plan references unknown authorities: {missing}"
                )
        known_consumptions = set(consumption_ids)
        for subplan in self.subplans:
            if subplan.consumption_id not in known_consumptions:
                raise ValueError(
                    f"execution subplan references unknown consumption: {subplan.consumption_id}"
                )
        return self


def semantic_execution_plan_content_digest(payload: dict) -> str:
    """Return the content digest for an unsigned semantic execution plan."""

    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
