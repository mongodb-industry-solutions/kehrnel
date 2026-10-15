"""Canonical contracts for source-optional architecture compilation.

The participant/interaction Blueprint is the only target architecture model.
Older prototype source/consumption documents are migration inputs, not a
parallel public contract. These models do not deploy or execute architecture.
"""

from __future__ import annotations

from copy import deepcopy
from enum import Enum
import hashlib
from typing import List, Literal, Optional

from pydantic import Field, JsonValue, model_validator

from .artifacts import (
    ArtifactRef,
    ContractModel,
    ObservationKind,
    Sha256Digest,
    Visibility,
)
from .package_digest import canonical_json_bytes
from .semantic_execution import SemanticMode
from .source_architecture import (
    ArchitectureConstraints,
    CandidateStatus,
    CompilationIssue,
    ConsistencyRequirement,
    IntegrationPosture,
    IssueSeverity,
)


NORMALIZED_ID = r"^[a-z0-9][a-z0-9._-]*$"


def _content_digest(value: ContractModel | dict, *, exclude: set[str] | None = None) -> str:
    if isinstance(value, ContractModel):
        payload = value.to_wire()
    else:
        payload = deepcopy(value)
    for key in exclude or set():
        payload.pop(key, None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _unique(values: List[str], label: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")


class ParticipantKind(str, Enum):
    APPLICATION = "application"
    SYSTEM = "system"
    SERVICE = "service"
    AGENT = "agent"
    REPOSITORY = "repository"
    DEVICE = "device"
    EXTERNAL_AUTHORITY = "external_authority"


class ParticipantLifecycle(str, Enum):
    EXISTING = "existing"
    PLANNED = "planned"


class InteractionBehavior(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    COMMAND = "command"
    READ = "read"
    SEARCH = "search"
    QUERY = "query"
    SUBSCRIBE = "subscribe"
    PUBLISH = "publish"
    EXPORT = "export"


class InteractionPattern(str, Enum):
    REQUEST_RESPONSE = "request_response"
    PUBLISH_SUBSCRIBE = "publish_subscribe"
    STREAM = "stream"
    BATCH = "batch"


class InterfaceKind(str, Enum):
    REST = "rest"
    FHIR = "fhir"
    OPENEHR = "openehr"
    AQL = "aql"
    MQL = "mql"
    SQL = "sql"
    EVENT = "event"
    AGENT_TOOL = "agent_tool"
    NATURAL_LANGUAGE = "natural_language"


class AcknowledgementLevel(str, Enum):
    ACCEPTED = "accepted"
    DURABLE_AUTHORITATIVE = "durable_authoritative"
    DERIVED_VISIBLE = "derived_visible"


class RecordConflictPolicy(str, Enum):
    REJECT = "reject"
    RECONCILE = "reconcile"
    APPEND_DISTINCT = "append_distinct"
    REVIEWED_MERGE = "reviewed_merge"


class MaterializationKind(str, Enum):
    PROJECTION = "projection"
    INDEX = "index"
    SEARCH_INDEX = "search_index"
    VECTOR_INDEX = "vector_index"
    CACHE = "cache"
    EXPORT = "export"


class BindingConsumerKind(str, Enum):
    PREPARATION = "preparation"
    MATERIALIZATION = "materialization"
    QUERY_PLANNER = "query_planner"
    EXECUTOR = "executor"


class PreparationStageKind(str, Enum):
    VALIDATE = "validate"
    MAP = "map"
    ENRICH = "enrich"
    COMPOSE = "compose"
    AUTHORITATIVE_WRITE = "authoritative_write"
    MATERIALIZE = "materialize"
    INDEX = "index"
    REBUILD = "rebuild"


class PreparationTrigger(str, Enum):
    SYNCHRONOUS_WRITE = "synchronous_write"
    ASYNCHRONOUS_EVENT = "asynchronous_event"
    BATCH = "batch"


class CapabilityAvailability(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class DeliveryLevel(str, Enum):
    PUBLIC_DEMONSTRATION = "public_demonstration"
    CUSTOMER_EVALUATION = "customer_evaluation"
    PRODUCTION_IMPLEMENTATION = "production_implementation"


class RuntimeDependencyKind(str, Enum):
    TERMINOLOGY = "terminology"
    IDENTITY = "identity"
    CONNECTOR = "connector"
    POLICY = "policy"
    OBSERVABILITY = "observability"
    EXTERNAL_SERVICE = "external_service"


class WorkloadShape(str, Enum):
    POINT_LOOKUP = "point_lookup"
    PATIENT_BOUNDED = "patient_bounded"
    INDEX_RANGE = "index_range"
    COHORT_SELECTIVE = "cohort_selective"
    COHORT_BROAD = "cohort_broad"
    FULL_SCAN = "full_scan"
    SEARCH = "search"
    VECTOR_SEARCH = "vector_search"
    AGGREGATION = "aggregation"
    GRAPH_TRAVERSAL = "graph_traversal"
    EVENT_TIMELINE = "event_timeline"
    SEMANTIC_RESOLUTION = "semantic_resolution"
    NATURAL_LANGUAGE = "natural_language"
    WRITE = "write"
    BULK_INGEST = "bulk_ingest"


class PerformanceModelKind(str, Enum):
    ANALYTICAL = "analytical"
    CALIBRATED = "calibrated"
    ANALOGOUS = "analogous"


class EvidenceGrade(str, Enum):
    A_DIRECTLY_MEASURED = "a_directly_measured"
    B_CALIBRATED = "b_calibrated"
    C_ANALOGOUS = "c_analogous"
    D_ANALYTICAL = "d_analytical"
    E_UNVALIDATED = "e_unvalidated"


class BlueprintRevisionRef(ContractModel):
    blueprint_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    revision: int = Field(ge=1)
    digest: Sha256Digest


class ParticipantSpec(ContractModel):
    participant_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    label: str = Field(min_length=1)
    kind: ParticipantKind
    lifecycle: ParticipantLifecycle
    source_profile_ref: Optional[ArtifactRef] = None

    @model_validator(mode="after")
    def source_profile_has_the_right_kind(self) -> "ParticipantSpec":
        if (
            self.source_profile_ref is not None
            and self.source_profile_ref.kind != "source-profile"
        ):
            raise ValueError("sourceProfileRef.kind must be 'source-profile'")
        return self


class InteractionRoleBindings(ContractModel):
    requester_ref: Optional[str] = Field(default=None, pattern=NORMALIZED_ID)
    provider_ref: Optional[str] = Field(default=None, pattern=NORMALIZED_ID)
    publisher_ref: Optional[str] = Field(default=None, pattern=NORMALIZED_ID)
    subscriber_ref: Optional[str] = Field(default=None, pattern=NORMALIZED_ID)


class InteractionInterface(ContractModel):
    kind: InterfaceKind
    protocol_version: Optional[str] = Field(default=None, min_length=1)
    capability_ref: Optional[ArtifactRef] = None

    @model_validator(mode="after")
    def standard_interfaces_pin_capability(self) -> "InteractionInterface":
        if self.kind is InterfaceKind.FHIR and self.capability_ref is None:
            raise ValueError("FHIR interfaces require capabilityRef")
        return self


class InteractionGuarantees(ContractModel):
    acknowledgement: AcknowledgementLevel = AcknowledgementLevel.ACCEPTED
    consistency: ConsistencyRequirement = ConsistencyRequirement.STRONG
    latency_target_ms: Optional[int] = Field(default=None, gt=0)
    freshness_target_seconds: Optional[int] = Field(default=None, ge=0)
    idempotency_required: bool = False

    @model_validator(mode="after")
    def derived_visibility_has_a_freshness_contract(self) -> "InteractionGuarantees":
        if (
            self.acknowledgement is AcknowledgementLevel.DERIVED_VISIBLE
            and self.freshness_target_seconds is None
        ):
            raise ValueError(
                "derived_visible acknowledgement requires freshnessTargetSeconds"
            )
        return self


class ProjectInteractionContract(ContractModel):
    contract: Literal["interaction-contract/v1"]
    artifact_ref: ArtifactRef
    interaction_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    behavior: InteractionBehavior
    pattern: InteractionPattern
    roles: InteractionRoleBindings
    interface: InteractionInterface
    semantic_scope_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    input_contract_ref: Optional[ArtifactRef] = None
    result_contract_ref: Optional[ArtifactRef] = None
    error_contract_ref: Optional[ArtifactRef] = None
    authorization_policy_ref: Optional[ArtifactRef] = None
    workload_ref: ArtifactRef
    guarantees: InteractionGuarantees = Field(default_factory=InteractionGuarantees)

    @model_validator(mode="after")
    def interaction_is_coherent(self) -> "ProjectInteractionContract":
        if self.artifact_ref.kind != "interaction-contract":
            raise ValueError("artifactRef.kind must be 'interaction-contract'")
        if self.workload_ref.kind != "workload-profile":
            raise ValueError("workloadRef.kind must be 'workload-profile'")
        if self.pattern is InteractionPattern.REQUEST_RESPONSE:
            if not self.roles.requester_ref or not self.roles.provider_ref:
                raise ValueError(
                    "request_response interactions require requesterRef and providerRef"
                )
        else:
            if not self.roles.publisher_ref or not self.roles.subscriber_ref:
                raise ValueError(
                    f"{self.pattern.value} interactions require publisherRef and subscriberRef"
                )
        if self.behavior in {
            InteractionBehavior.PUBLISH,
            InteractionBehavior.SUBSCRIBE,
        } and self.pattern is InteractionPattern.REQUEST_RESPONSE:
            raise ValueError("publish and subscribe cannot use request_response")
        return self


class AuthorityScope(ContractModel):
    scope_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    domain: str = Field(min_length=1)
    record_types: List[str] = Field(default_factory=list)
    paths: List[str] = Field(default_factory=list)
    lifecycle_states: List[str] = Field(default_factory=list)


class RecordAuthorityPolicy(ContractModel):
    contract: Literal["record-authority-policy/v1"]
    artifact_ref: ArtifactRef
    scope: AuthorityScope
    authoritative_writer_ref: str = Field(min_length=1, pattern=NORMALIZED_ID)
    permitted_writer_refs: List[str] = Field(min_length=1)
    conflict_policy: RecordConflictPolicy
    version_policy_ref: ArtifactRef
    correction_behavior: str = Field(min_length=1)
    provenance_required: bool = True

    @model_validator(mode="after")
    def authority_is_coherent(self) -> "RecordAuthorityPolicy":
        if self.artifact_ref.kind != "record-authority-policy":
            raise ValueError("artifactRef.kind must be 'record-authority-policy'")
        _unique(self.permitted_writer_refs, "permittedWriterRefs")
        if self.authoritative_writer_ref not in self.permitted_writer_refs:
            raise ValueError(
                "authoritativeWriterRef must be included in permittedWriterRefs"
            )
        return self


class SemanticAuthoritySelection(ContractModel):
    scope_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    mode: SemanticMode
    authority_ref: ArtifactRef

    @model_validator(mode="after")
    def authority_matches_mode(self) -> "SemanticAuthoritySelection":
        is_context_object = self.authority_ref.kind == "context-object-definition"
        if self.mode is SemanticMode.NATIVE and is_context_object:
            raise ValueError("native semantic mode cannot use a ContextObject authority")
        if self.mode in {SemanticMode.DERIVED, SemanticMode.COMPOSED} and not is_context_object:
            raise ValueError(
                f"{self.mode.value} semantic mode requires a ContextObject authority"
            )
        return self


class DataPostureSelection(ContractModel):
    scope_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    posture: IntegrationPosture
    serving_repository_ref: Optional[str] = Field(default=None, pattern=NORMALIZED_ID)


class DataProductComposition(ContractModel):
    """Explicit lineage between semantic data-product scopes.

    This is separate from SemanticMode.COMPOSED: that mode governs meaning;
    this contract names the products supplying another product's data.
    """

    composition_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    target_scope_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    input_scope_ids: List[str] = Field(min_length=1)

    @model_validator(mode="after")
    def composition_is_coherent(self) -> "DataProductComposition":
        _unique(self.input_scope_ids, "composition inputScopeIds")
        if self.target_scope_id in self.input_scope_ids:
            raise ValueError("a data product cannot compose itself")
        return self


WRITE_BEHAVIORS = {
    InteractionBehavior.CREATE,
    InteractionBehavior.UPDATE,
    InteractionBehavior.DELETE,
    InteractionBehavior.COMMAND,
    InteractionBehavior.PUBLISH,
}


class SolutionBlueprint(ContractModel):
    contract: Literal["solution-blueprint/v1"]
    blueprint_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    industry: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    revision: int = Field(default=1, ge=1)
    objective: str = Field(min_length=1)
    participants: List[ParticipantSpec] = Field(min_length=1)
    interactions: List[ProjectInteractionContract] = Field(min_length=1)
    semantic_authorities: List[SemanticAuthoritySelection] = Field(min_length=1)
    record_authority_policies: List[RecordAuthorityPolicy] = Field(default_factory=list)
    data_postures: List[DataPostureSelection] = Field(min_length=1)
    data_product_compositions: List[DataProductComposition] = Field(default_factory=list)
    constraints: ArchitectureConstraints = Field(default_factory=ArchitectureConstraints)
    target_capability_profile_ref: ArtifactRef
    preferred_foundation_refs: List[ArtifactRef] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    decisions: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def blueprint_is_coherent(self) -> "SolutionBlueprint":
        if self.target_capability_profile_ref.kind != "target-capability-profile":
            raise ValueError(
                "targetCapabilityProfileRef.kind must be 'target-capability-profile'"
            )
        participant_ids = [item.participant_id for item in self.participants]
        _unique(participant_ids, "participantId values")
        known_participants = set(participant_ids)

        interaction_ids = [item.interaction_id for item in self.interactions]
        _unique(interaction_ids, "interactionId values")
        interaction_refs = [item.artifact_ref.id for item in self.interactions]
        _unique(interaction_refs, "interaction artifactRef.id values")
        foundation_refs = [
            (item.kind, item.id, item.version, item.digest)
            for item in self.preferred_foundation_refs
        ]
        if len(foundation_refs) != len(set(foundation_refs)):
            raise ValueError("preferredFoundationRefs must be unique")

        authority_scopes = [item.scope_id for item in self.semantic_authorities]
        _unique(authority_scopes, "semantic authority scopeId values")
        known_scopes = set(authority_scopes)
        policy_scopes = [item.scope.scope_id for item in self.record_authority_policies]
        _unique(policy_scopes, "record authority scopeId values")
        policies = {
            item.scope.scope_id: item for item in self.record_authority_policies
        }
        posture_scopes = [item.scope_id for item in self.data_postures]
        _unique(posture_scopes, "data posture scopeId values")
        composition_ids = [item.composition_id for item in self.data_product_compositions]
        _unique(composition_ids, "data product compositionId values")
        composition_targets = [item.target_scope_id for item in self.data_product_compositions]
        _unique(composition_targets, "data product composition targetScopeId values")

        composition_graph: dict[str, set[str]] = {}
        for composition in self.data_product_compositions:
            referenced_scopes = {composition.target_scope_id, *composition.input_scope_ids}
            missing_scopes = sorted(referenced_scopes - known_scopes)
            if missing_scopes:
                raise ValueError(
                    f"data product composition '{composition.composition_id}' references unknown semantic scopes: {missing_scopes}"
                )
            composition_graph[composition.target_scope_id] = set(composition.input_scope_ids)

        def visit(scope_id: str, visiting: set[str], visited: set[str]) -> None:
            if scope_id in visiting:
                raise ValueError("data product compositions must be acyclic")
            if scope_id in visited:
                return
            visiting.add(scope_id)
            for input_scope_id in composition_graph.get(scope_id, set()):
                visit(input_scope_id, visiting, visited)
            visiting.remove(scope_id)
            visited.add(scope_id)

        visited_scopes: set[str] = set()
        for target_scope_id in sorted(composition_graph):
            visit(target_scope_id, set(), visited_scopes)

        for policy in self.record_authority_policies:
            writers = {
                policy.authoritative_writer_ref,
                *policy.permitted_writer_refs,
            }
            missing = sorted(writers - known_participants)
            if missing:
                raise ValueError(
                    f"record authority policy '{policy.scope.scope_id}' references unknown participants: {missing}"
                )
        for posture in self.data_postures:
            if posture.scope_id not in known_scopes:
                raise ValueError(
                    f"data posture references unknown semantic scope: {posture.scope_id}"
                )
            if (
                posture.serving_repository_ref is not None
                and posture.serving_repository_ref not in known_participants
            ):
                raise ValueError(
                    f"data posture references unknown serving repository: {posture.serving_repository_ref}"
                )

        for interaction in self.interactions:
            role_refs = {
                value
                for value in interaction.roles.to_wire().values()
                if value is not None
            }
            missing = sorted(role_refs - known_participants)
            if missing:
                raise ValueError(
                    f"interaction '{interaction.interaction_id}' references unknown participants: {missing}"
                )
            if interaction.semantic_scope_id not in known_scopes:
                raise ValueError(
                    f"interaction '{interaction.interaction_id}' references unknown semantic scope '{interaction.semantic_scope_id}'"
                )
            if interaction.behavior in WRITE_BEHAVIORS:
                policy = policies.get(interaction.semantic_scope_id)
                if policy is None:
                    raise ValueError(
                        f"write interaction '{interaction.interaction_id}' requires a record authority policy"
                    )
                writer_ref = (
                    interaction.roles.publisher_ref
                    if interaction.behavior is InteractionBehavior.PUBLISH
                    else interaction.roles.requester_ref
                )
                if writer_ref not in policy.permitted_writer_refs:
                    raise ValueError(
                        f"write interaction '{interaction.interaction_id}' uses writer '{writer_ref}' outside permittedWriterRefs"
                    )
        return self


def solution_blueprint_content_digest(blueprint: SolutionBlueprint | dict) -> str:
    payload = blueprint.to_wire() if isinstance(blueprint, ContractModel) else deepcopy(blueprint)
    compositions = payload.get("dataProductCompositions", [])
    for composition in compositions:
        composition["inputScopeIds"] = sorted(composition.get("inputScopeIds", []))
    payload["dataProductCompositions"] = sorted(
        compositions,
        key=lambda item: (item.get("targetScopeId", ""), item.get("compositionId", "")),
    )
    return _content_digest(payload)


class SemanticPathBinding(ContractModel):
    semantic_path: str = Field(min_length=1)
    physical_path: str = Field(min_length=1)
    value_type: str = Field(min_length=1)
    repeated: bool = False
    correlation_key: Optional[str] = None
    unit_policy: Optional[str] = None
    temporal_policy: Optional[str] = None
    index_refs: List[str] = Field(default_factory=list)
    reconstruction_rule: Optional[str] = None


class SemanticBindingConsumer(ContractModel):
    consumer_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    kind: BindingConsumerKind
    component_ref: ArtifactRef


class SemanticExecutionBindingV1(ContractModel):
    contract: Literal["semantic-execution-binding/v1"]
    artifact_ref: ArtifactRef
    semantic_authority_ref: ArtifactRef
    strategy_ref: ArtifactRef
    path_bindings: List[SemanticPathBinding] = Field(min_length=1)
    consumers: List[SemanticBindingConsumer] = Field(min_length=1)
    writer_interaction_ids: List[str] = Field(default_factory=list)
    query_interaction_ids: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def binding_is_coherent(self) -> "SemanticExecutionBindingV1":
        if self.artifact_ref.kind != "semantic-execution-binding":
            raise ValueError("artifactRef.kind must be 'semantic-execution-binding'")
        _unique(
            [item.semantic_path for item in self.path_bindings],
            "semanticPath values",
        )
        _unique([item.consumer_id for item in self.consumers], "binding consumerId values")
        _unique(self.writer_interaction_ids, "writerInteractionIds")
        _unique(self.query_interaction_ids, "queryInteractionIds")
        return self

    def covers_interaction(self, interaction_id: str, *, write: bool) -> bool:
        """Return whether this reviewed binding applies to an interaction.

        An empty writer or query list deliberately means that the binding is
        reusable for every interaction of that behaviour in its semantic
        authority scope.  Non-empty lists narrow the reviewed binding to the
        named project interactions.
        """

        declared = (
            self.writer_interaction_ids if write else self.query_interaction_ids
        )
        return not declared or interaction_id in declared


class TargetCapability(ContractModel):
    capability_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    availability: CapabilityAvailability
    version: Optional[str] = None
    constraints: dict[str, JsonValue] = Field(default_factory=dict)


class TargetTopology(ContractModel):
    deployment_model: str = Field(min_length=1)
    regions: List[str] = Field(default_factory=list)
    shard_count: Optional[int] = Field(default=None, gt=0)
    search_node_count: Optional[int] = Field(default=None, ge=0)


class TargetCapabilityProfile(ContractModel):
    contract: Literal["target-capability-profile/v1"]
    artifact_ref: ArtifactRef
    deployment_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    mongodb_version: str = Field(min_length=1)
    topology: TargetTopology
    capabilities: List[TargetCapability] = Field(min_length=1)

    @model_validator(mode="after")
    def target_is_coherent(self) -> "TargetCapabilityProfile":
        if self.artifact_ref.kind != "target-capability-profile":
            raise ValueError("artifactRef.kind must be 'target-capability-profile'")
        _unique(
            [item.capability_id for item in self.capabilities],
            "target capabilityId values",
        )
        _unique(self.topology.regions, "target topology regions")
        return self


class TargetCapabilityRequirement(ContractModel):
    capability_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    required: bool = True
    minimum_version: Optional[str] = None
    rationale: str = Field(min_length=1)


class TargetCompatibilityAssessment(ContractModel):
    capability_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    required: bool
    availability: CapabilityAvailability
    compatible: bool
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def unavailable_required_capability_is_not_compatible(
        self,
    ) -> "TargetCompatibilityAssessment":
        if (
            self.required
            and self.availability is not CapabilityAvailability.AVAILABLE
            and self.compatible
        ):
            raise ValueError(
                "a required unavailable or unknown capability cannot be compatible"
            )
        return self


class PreparationStage(ContractModel):
    stage_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    kind: PreparationStageKind
    trigger: PreparationTrigger
    strategy_instance_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    interaction_ids: List[str] = Field(min_length=1)
    binding_refs: List[ArtifactRef] = Field(default_factory=list)
    input_resources: List[str] = Field(default_factory=list)
    output_resources: List[str] = Field(min_length=1)
    freshness_target_seconds: Optional[int] = Field(default=None, ge=0)
    failure_behavior: str = Field(min_length=1)
    composition_id: Optional[str] = Field(default=None, pattern=NORMALIZED_ID)

    @model_validator(mode="after")
    def stage_is_coherent(self) -> "PreparationStage":
        _unique(self.interaction_ids, "preparation stage interactionIds")
        _unique(self.input_resources, "preparation stage inputResources")
        _unique(self.output_resources, "preparation stage outputResources")
        for reference in self.binding_refs:
            if reference.kind != "semantic-execution-binding":
                raise ValueError(
                    "preparation stage bindingRefs must reference semantic-execution-binding artifacts"
                )
        if self.kind is PreparationStageKind.COMPOSE and self.composition_id is None:
            raise ValueError("compose preparation stages require compositionId")
        if self.kind is not PreparationStageKind.COMPOSE and self.composition_id is not None:
            raise ValueError("compositionId is only valid for compose preparation stages")
        return self


class PreparationPlan(ContractModel):
    contract: Literal["preparation-plan/v1"]
    artifact_ref: ArtifactRef
    blueprint_ref: BlueprintRevisionRef
    target_capability_profile_ref: ArtifactRef
    stages: List[PreparationStage] = Field(default_factory=list)
    issues: List[CompilationIssue] = Field(default_factory=list)

    @model_validator(mode="after")
    def preparation_plan_is_coherent(self) -> "PreparationPlan":
        if self.artifact_ref.kind != "preparation-plan":
            raise ValueError("artifactRef.kind must be 'preparation-plan'")
        if self.target_capability_profile_ref.kind != "target-capability-profile":
            raise ValueError(
                "targetCapabilityProfileRef.kind must be 'target-capability-profile'"
            )
        _unique([item.stage_id for item in self.stages], "preparation stageId values")
        return self


class RuntimeRoute(ContractModel):
    route_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    interaction_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    behavior: InteractionBehavior
    strategy_instance_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    semantic_binding_ref: ArtifactRef
    plan_family_id: Optional[str] = Field(default=None, pattern=NORMALIZED_ID)
    planner_ref: ArtifactRef
    executor_ref: ArtifactRef
    policy_ref: Optional[ArtifactRef] = None
    fallback_behavior: str = Field(min_length=1)

    @model_validator(mode="after")
    def route_is_coherent(self) -> "RuntimeRoute":
        if self.semantic_binding_ref.kind != "semantic-execution-binding":
            raise ValueError(
                "semanticBindingRef.kind must be 'semantic-execution-binding'"
            )
        return self


class RuntimeExecutionPlan(ContractModel):
    contract: Literal["runtime-execution-plan/v1"]
    artifact_ref: ArtifactRef
    blueprint_ref: BlueprintRevisionRef
    target_capability_profile_ref: ArtifactRef
    routes: List[RuntimeRoute] = Field(default_factory=list)
    issues: List[CompilationIssue] = Field(default_factory=list)

    @model_validator(mode="after")
    def runtime_plan_is_coherent(self) -> "RuntimeExecutionPlan":
        if self.artifact_ref.kind != "runtime-execution-plan":
            raise ValueError("artifactRef.kind must be 'runtime-execution-plan'")
        if self.target_capability_profile_ref.kind != "target-capability-profile":
            raise ValueError(
                "targetCapabilityProfileRef.kind must be 'target-capability-profile'"
            )
        _unique([item.route_id for item in self.routes], "runtime routeId values")
        _unique(
            [item.interaction_id for item in self.routes],
            "runtime route interactionId values",
        )
        return self


class ResourceOwnership(ContractModel):
    resource_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    resource_kind: str = Field(min_length=1)
    owner_strategy_instance_id: str = Field(min_length=1, pattern=NORMALIZED_ID)


class ResolvedStrategyInstance(ContractModel):
    strategy_instance_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    strategy_ref: ArtifactRef
    configuration_digest: Sha256Digest
    interaction_ids: List[str] = Field(default_factory=list)
    binding_refs: List[ArtifactRef] = Field(default_factory=list)
    dependency_refs: List[ArtifactRef] = Field(default_factory=list)
    capability_requirements: List[TargetCapabilityRequirement] = Field(
        default_factory=list
    )

    @model_validator(mode="after")
    def refs_are_coherent(self) -> "ResolvedStrategyInstance":
        _unique(self.interaction_ids, "strategy interactionIds")
        _unique(
            [item.capability_id for item in self.capability_requirements],
            "strategy capability requirement ids",
        )
        for reference in self.binding_refs:
            if reference.kind != "semantic-execution-binding":
                raise ValueError(
                    "strategy bindingRefs must reference semantic-execution-binding artifacts"
                )
        return self


class MaterializationMaintenance(ContractModel):
    update_mode: Literal["synchronous", "asynchronous", "snapshot"]
    rebuild_source: str = Field(min_length=1)
    deletion_behavior: str = Field(min_length=1)
    recovery_behavior: str = Field(min_length=1)
    retirement_condition: str = Field(min_length=1)
    freshness_target_seconds: Optional[int] = Field(default=None, ge=0)


class MaterializationPlanV1(ContractModel):
    materialization_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    kind: MaterializationKind
    owner_strategy_instance_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    serves_interaction_ids: List[str] = Field(min_length=1)
    rationale: str = Field(min_length=1)
    maintenance: MaterializationMaintenance


class QueryPlanFamily(ContractModel):
    plan_family_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    interaction_ids: List[str] = Field(min_length=1)
    strategy_instance_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    semantic_binding_ref: ArtifactRef
    mechanism: str = Field(min_length=1)
    runtime_constraints: dict[str, JsonValue] = Field(default_factory=dict)
    fallback_plan_family_ids: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def binding_kind_is_correct(self) -> "QueryPlanFamily":
        if self.semantic_binding_ref.kind != "semantic-execution-binding":
            raise ValueError(
                "semanticBindingRef.kind must be 'semantic-execution-binding'"
            )
        _unique(self.interaction_ids, "query-plan interactionIds")
        _unique(self.fallback_plan_family_ids, "fallbackPlanFamilyIds")
        return self


class ResolvedArchitecturePlan(ContractModel):
    contract: Literal["resolved-architecture-plan/v1"]
    artifact_ref: ArtifactRef
    blueprint_ref: BlueprintRevisionRef
    target_capability_profile_ref: ArtifactRef
    status: CandidateStatus
    requires_approval: bool
    strategy_instances: List[ResolvedStrategyInstance] = Field(default_factory=list)
    semantic_binding_refs: List[ArtifactRef] = Field(default_factory=list)
    resource_ownership: List[ResourceOwnership] = Field(default_factory=list)
    materializations: List[MaterializationPlanV1] = Field(default_factory=list)
    query_plan_families: List[QueryPlanFamily] = Field(default_factory=list)
    target_compatibility: List[TargetCompatibilityAssessment] = Field(
        default_factory=list
    )
    preparation_plan_ref: Optional[ArtifactRef] = None
    runtime_plan_ref: Optional[ArtifactRef] = None
    issues: List[CompilationIssue] = Field(default_factory=list)

    @model_validator(mode="after")
    def resolved_plan_is_coherent(self) -> "ResolvedArchitecturePlan":
        if self.artifact_ref.kind != "resolved-architecture-plan":
            raise ValueError("artifactRef.kind must be 'resolved-architecture-plan'")
        if self.target_capability_profile_ref.kind != "target-capability-profile":
            raise ValueError(
                "targetCapabilityProfileRef.kind must be 'target-capability-profile'"
            )
        instance_ids = [item.strategy_instance_id for item in self.strategy_instances]
        _unique(instance_ids, "strategyInstanceId values")
        known_instances = set(instance_ids)
        _unique(
            [item.materialization_id for item in self.materializations],
            "materializationId values",
        )
        _unique(
            [item.plan_family_id for item in self.query_plan_families],
            "planFamilyId values",
        )
        _unique(
            [item.capability_id for item in self.target_compatibility],
            "target compatibility capabilityId values",
        )
        for item in self.resource_ownership:
            if item.owner_strategy_instance_id not in known_instances:
                raise ValueError(
                    f"resource '{item.resource_id}' references unknown strategy instance"
                )
        for item in self.materializations:
            if item.owner_strategy_instance_id not in known_instances:
                raise ValueError(
                    f"materialization '{item.materialization_id}' references unknown strategy instance"
                )
        for item in self.query_plan_families:
            if item.strategy_instance_id not in known_instances:
                raise ValueError(
                    f"query plan family '{item.plan_family_id}' references unknown strategy instance"
                )
        blocking = any(issue.severity is IssueSeverity.BLOCKING for issue in self.issues)
        incompatible_required = [
            item.capability_id
            for item in self.target_compatibility
            if item.required and not item.compatible
        ]
        if incompatible_required and self.status is not CandidateStatus.BLOCKED:
            raise ValueError(
                "required target capabilities are incompatible; resolved plan must be blocked: "
                f"{sorted(incompatible_required)}"
            )
        if self.status is CandidateStatus.BLOCKED and not blocking:
            raise ValueError("blocked resolved plans require a blocking issue")
        if self.status is not CandidateStatus.BLOCKED:
            if blocking:
                raise ValueError("non-blocked resolved plans cannot contain blocking issues")
            if not self.strategy_instances:
                raise ValueError("non-blocked resolved plans require strategyInstances")
        return self


def resolved_architecture_plan_content_digest(
    plan: ResolvedArchitecturePlan | dict,
) -> str:
    return _content_digest(plan)


class ArchitectureResolutionRequest(ContractModel):
    contract: Literal["architecture-resolution-request/v1"]
    blueprint: SolutionBlueprint
    target_capability_profile: TargetCapabilityProfile
    semantic_execution_bindings: List[SemanticExecutionBindingV1] = Field(
        default_factory=list
    )

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ArchitectureResolutionRequest":
        if (
            self.blueprint.target_capability_profile_ref
            != self.target_capability_profile.artifact_ref
        ):
            raise ValueError(
                "blueprint targetCapabilityProfileRef must match targetCapabilityProfile.artifactRef"
            )
        identities = [item.artifact_ref for item in self.semantic_execution_bindings]
        if len(identities) != len(set(identities)):
            raise ValueError("semanticExecutionBindings must be unique")
        return self


class ArchitectureResolutionInputCatalog(ContractModel):
    """Reviewed inputs that a client may pin into a resolution request."""

    contract: Literal["architecture-resolution-input-catalog/v1"]
    target_capability_profiles: List[TargetCapabilityProfile] = Field(min_length=1)
    semantic_execution_bindings: List[SemanticExecutionBindingV1] = Field(
        default_factory=list
    )

    @model_validator(mode="after")
    def inputs_are_unique(self) -> "ArchitectureResolutionInputCatalog":
        target_refs = [item.artifact_ref for item in self.target_capability_profiles]
        binding_refs = [item.artifact_ref for item in self.semantic_execution_bindings]
        if len(target_refs) != len(set(target_refs)):
            raise ValueError("targetCapabilityProfiles must be unique")
        if len(binding_refs) != len(set(binding_refs)):
            raise ValueError("semanticExecutionBindings must be unique")
        return self


class ArchitectureResolutionResult(ContractModel):
    contract: Literal["architecture-resolution-result/v1"]
    resolved_plan: ResolvedArchitecturePlan
    preparation_plan: PreparationPlan
    runtime_plan: RuntimeExecutionPlan

    @model_validator(mode="after")
    def result_is_coherent(self) -> "ArchitectureResolutionResult":
        blueprint_refs = {
            (
                self.resolved_plan.blueprint_ref.blueprint_id,
                self.resolved_plan.blueprint_ref.revision,
                self.resolved_plan.blueprint_ref.digest,
            ),
            (
                self.preparation_plan.blueprint_ref.blueprint_id,
                self.preparation_plan.blueprint_ref.revision,
                self.preparation_plan.blueprint_ref.digest,
            ),
            (
                self.runtime_plan.blueprint_ref.blueprint_id,
                self.runtime_plan.blueprint_ref.revision,
                self.runtime_plan.blueprint_ref.digest,
            ),
        }
        if len(blueprint_refs) != 1:
            raise ValueError("all resolution artifacts must reference one blueprint revision")
        if self.resolved_plan.preparation_plan_ref != self.preparation_plan.artifact_ref:
            raise ValueError("resolved plan preparationPlanRef does not match preparationPlan")
        if self.resolved_plan.runtime_plan_ref != self.runtime_plan.artifact_ref:
            raise ValueError("resolved plan runtimePlanRef does not match runtimePlan")
        target_refs = {
            self.resolved_plan.target_capability_profile_ref,
            self.preparation_plan.target_capability_profile_ref,
            self.runtime_plan.target_capability_profile_ref,
        }
        if len(target_refs) != 1:
            raise ValueError("all resolution artifacts must reference one target profile")
        return self


class RuntimeQueryPlanTrace(ContractModel):
    contract: Literal["runtime-query-plan-trace/v1"]
    trace_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    resolved_plan_ref: ArtifactRef
    plan_family_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    interaction_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    semantic_binding_ref: ArtifactRef
    target_capability_profile_ref: ArtifactRef
    query_fingerprint: Sha256Digest
    selected_mechanism: str = Field(min_length=1)
    fallback_used: bool = False
    observed_freshness_seconds: Optional[int] = Field(default=None, ge=0)
    policy_decision_ref: ArtifactRef
    executor_ref: ArtifactRef
    trace_digest: Sha256Digest

    @model_validator(mode="after")
    def trace_refs_are_coherent(self) -> "RuntimeQueryPlanTrace":
        if self.resolved_plan_ref.kind != "resolved-architecture-plan":
            raise ValueError("resolvedPlanRef.kind must be 'resolved-architecture-plan'")
        if self.semantic_binding_ref.kind != "semantic-execution-binding":
            raise ValueError(
                "semanticBindingRef.kind must be 'semantic-execution-binding'"
            )
        if self.target_capability_profile_ref.kind != "target-capability-profile":
            raise ValueError(
                "targetCapabilityProfileRef.kind must be 'target-capability-profile'"
            )
        return self


class RuntimeDependency(ContractModel):
    dependency_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    kind: RuntimeDependencyKind
    artifact_ref: Optional[ArtifactRef] = None
    required_at_runtime: bool = True
    purpose: str = Field(min_length=1)


class ImplementationPackage(ContractModel):
    contract: Literal["implementation-package/v1"]
    artifact_ref: ArtifactRef
    blueprint_ref: BlueprintRevisionRef
    resolved_plan_ref: ArtifactRef
    target_capability_profile_ref: ArtifactRef
    required_artifact_refs: List[ArtifactRef] = Field(min_length=1)
    compiler_ref: ArtifactRef
    runtime_dependencies: List[RuntimeDependency] = Field(default_factory=list)
    delivery_level: DeliveryLevel
    lifecycle_owner: Optional[str] = None
    portal_independent: Literal[True] = True
    package_digest: Sha256Digest

    @model_validator(mode="after")
    def package_is_coherent(self) -> "ImplementationPackage":
        if self.artifact_ref.kind != "implementation-package":
            raise ValueError("artifactRef.kind must be 'implementation-package'")
        if self.resolved_plan_ref.kind != "resolved-architecture-plan":
            raise ValueError("resolvedPlanRef.kind must be 'resolved-architecture-plan'")
        if self.target_capability_profile_ref.kind != "target-capability-profile":
            raise ValueError(
                "targetCapabilityProfileRef.kind must be 'target-capability-profile'"
            )
        identities = [
            (item.kind, item.id, item.version, item.digest)
            for item in self.required_artifact_refs
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("requiredArtifactRefs must be unique")
        _unique(
            [item.dependency_id for item in self.runtime_dependencies],
            "runtime dependencyId values",
        )
        if (
            self.delivery_level is DeliveryLevel.PRODUCTION_IMPLEMENTATION
            and not self.lifecycle_owner
        ):
            raise ValueError(
                "production implementations require lifecycleOwner"
            )
        return self


class DatasetCharacteristics(ContractModel):
    record_count: int = Field(ge=0)
    subject_count: Optional[int] = Field(default=None, ge=0)
    logical_bytes: Optional[int] = Field(default=None, ge=0)
    physical_bytes: Optional[int] = Field(default=None, ge=0)
    average_record_bytes: Optional[int] = Field(default=None, ge=0)
    cardinality_notes: List[str] = Field(default_factory=list)
    skew_notes: List[str] = Field(default_factory=list)


class WorkloadProfile(ContractModel):
    contract: Literal["workload-profile/v1"]
    artifact_ref: ArtifactRef
    workload_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    shape: WorkloadShape
    dataset: DatasetCharacteristics
    expected_result_count: Optional[int] = Field(default=None, ge=0)
    result_bytes: Optional[int] = Field(default=None, ge=0)
    predicate_count: Optional[int] = Field(default=None, ge=0)
    indexed_predicate_count: Optional[int] = Field(default=None, ge=0)
    selectivity_parts_per_million: Optional[int] = Field(
        default=None, ge=0, le=1_000_000
    )
    shard_fanout: Optional[int] = Field(default=None, ge=1)
    concurrency: int = Field(default=1, ge=1)
    requests_per_second: Optional[int] = Field(default=None, ge=0)
    writes_per_second: Optional[int] = Field(default=None, ge=0)
    latency_target_ms: Optional[int] = Field(default=None, gt=0)
    freshness_target_seconds: Optional[int] = Field(default=None, ge=0)

    @model_validator(mode="after")
    def workload_is_coherent(self) -> "WorkloadProfile":
        if self.artifact_ref.kind != "workload-profile":
            raise ValueError("artifactRef.kind must be 'workload-profile'")
        if (
            self.indexed_predicate_count is not None
            and self.predicate_count is not None
            and self.indexed_predicate_count > self.predicate_count
        ):
            raise ValueError("indexedPredicateCount cannot exceed predicateCount")
        return self


class BenchmarkRunConditions(ContractModel):
    run_id: str = Field(min_length=1, pattern=NORMALIZED_ID)
    started_at: str = Field(min_length=1)
    duration_seconds: int = Field(gt=0)
    warm_cache: bool
    concurrency: int = Field(ge=1)
    software_versions: dict[str, str] = Field(default_factory=dict)
    configuration_digest: Sha256Digest
    methodology: str = Field(min_length=1)


class LatencyPercentileMeasurement(ContractModel):
    percentile_basis_points: int = Field(ge=1, le=10_000)
    latency_microseconds: int = Field(ge=0)


class BenchmarkEvidence(ContractModel):
    contract: Literal["benchmark-evidence/v1"]
    artifact_ref: ArtifactRef
    observation_kind: Literal[ObservationKind.MEASURED]
    resolved_plan_ref: ArtifactRef
    workload_ref: ArtifactRef
    target_capability_profile_ref: ArtifactRef
    dataset: DatasetCharacteristics
    conditions: BenchmarkRunConditions
    latency_percentiles: List[LatencyPercentileMeasurement] = Field(min_length=2)
    throughput_per_second: int = Field(ge=0)
    error_count: int = Field(ge=0)
    cpu_peak_basis_points: Optional[int] = Field(default=None, ge=0, le=10_000)
    memory_peak_bytes: Optional[int] = Field(default=None, ge=0)
    storage_bytes: Optional[int] = Field(default=None, ge=0)
    cache_hit_basis_points: Optional[int] = Field(default=None, ge=0, le=10_000)
    visibility: Visibility
    reviewed: bool

    @model_validator(mode="after")
    def evidence_is_coherent(self) -> "BenchmarkEvidence":
        if self.artifact_ref.kind != "benchmark-evidence":
            raise ValueError("artifactRef.kind must be 'benchmark-evidence'")
        if self.resolved_plan_ref.kind != "resolved-architecture-plan":
            raise ValueError("resolvedPlanRef.kind must be 'resolved-architecture-plan'")
        if self.workload_ref.kind != "workload-profile":
            raise ValueError("workloadRef.kind must be 'workload-profile'")
        if self.target_capability_profile_ref.kind != "target-capability-profile":
            raise ValueError(
                "targetCapabilityProfileRef.kind must be 'target-capability-profile'"
            )
        percentiles = [item.percentile_basis_points for item in self.latency_percentiles]
        _unique([str(item) for item in percentiles], "latency percentile values")
        if not {5000, 9500}.issubset(set(percentiles)):
            raise ValueError("benchmark evidence requires P50 and P95 measurements")
        ordered = sorted(
            self.latency_percentiles,
            key=lambda item: item.percentile_basis_points,
        )
        latencies = [item.latency_microseconds for item in ordered]
        if latencies != sorted(latencies):
            raise ValueError("latency percentiles must be monotonic")
        return self


class PerformanceModel(ContractModel):
    contract: Literal["performance-model/v1"]
    artifact_ref: ArtifactRef
    kind: PerformanceModelKind
    model_version: str = Field(min_length=1)
    applicable_strategy_ids: List[str] = Field(min_length=1)
    applicable_workload_shapes: List[WorkloadShape] = Field(min_length=1)
    benchmark_evidence_refs: List[ArtifactRef] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    validity_bounds: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def model_is_coherent(self) -> "PerformanceModel":
        if self.artifact_ref.kind != "performance-model":
            raise ValueError("artifactRef.kind must be 'performance-model'")
        _unique(self.applicable_strategy_ids, "applicableStrategyIds")
        _unique(
            [item.value for item in self.applicable_workload_shapes],
            "applicableWorkloadShapes",
        )
        for reference in self.benchmark_evidence_refs:
            if reference.kind != "benchmark-evidence":
                raise ValueError(
                    "benchmarkEvidenceRefs must reference benchmark-evidence artifacts"
                )
        if (
            self.kind in {PerformanceModelKind.CALIBRATED, PerformanceModelKind.ANALOGOUS}
            and not self.benchmark_evidence_refs
        ):
            raise ValueError(
                f"{self.kind.value} performance models require benchmark evidence"
            )
        return self


class EstimateRange(ContractModel):
    lower: int = Field(ge=0)
    upper: int = Field(ge=0)

    @model_validator(mode="after")
    def range_is_ordered(self) -> "EstimateRange":
        if self.lower > self.upper:
            raise ValueError("estimate lower bound cannot exceed upper bound")
        return self


class PerformanceForecast(ContractModel):
    contract: Literal["performance-forecast/v1"]
    artifact_ref: ArtifactRef
    observation_kind: Literal[ObservationKind.ESTIMATED]
    resolved_plan_ref: ArtifactRef
    workload_ref: ArtifactRef
    target_capability_profile_ref: ArtifactRef
    performance_model_ref: ArtifactRef
    evidence_grade: EvidenceGrade
    benchmark_evidence_refs: List[ArtifactRef] = Field(default_factory=list)
    p50_latency_microseconds: EstimateRange
    p95_latency_microseconds: EstimateRange
    p99_latency_microseconds: Optional[EstimateRange] = None
    throughput_per_second: Optional[EstimateRange] = None
    storage_bytes: Optional[EstimateRange] = None
    memory_bytes: Optional[EstimateRange] = None
    monthly_cost_micros: Optional[EstimateRange] = None
    matching_conditions: List[str] = Field(default_factory=list)
    non_matching_conditions: List[str] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    required_tests: List[str] = Field(default_factory=list)
    is_guarantee: Literal[False] = False

    @model_validator(mode="after")
    def forecast_is_coherent(self) -> "PerformanceForecast":
        if self.artifact_ref.kind != "performance-forecast":
            raise ValueError("artifactRef.kind must be 'performance-forecast'")
        expected_kinds = {
            "resolved_plan_ref": (self.resolved_plan_ref, "resolved-architecture-plan"),
            "workload_ref": (self.workload_ref, "workload-profile"),
            "target_capability_profile_ref": (
                self.target_capability_profile_ref,
                "target-capability-profile",
            ),
            "performance_model_ref": (self.performance_model_ref, "performance-model"),
        }
        for name, (reference, kind) in expected_kinds.items():
            if reference.kind != kind:
                raise ValueError(f"{name} must reference a {kind} artifact")
        for reference in self.benchmark_evidence_refs:
            if reference.kind != "benchmark-evidence":
                raise ValueError(
                    "benchmarkEvidenceRefs must reference benchmark-evidence artifacts"
                )
        if self.evidence_grade in {
            EvidenceGrade.A_DIRECTLY_MEASURED,
            EvidenceGrade.B_CALIBRATED,
            EvidenceGrade.C_ANALOGOUS,
        } and not self.benchmark_evidence_refs:
            raise ValueError(
                f"evidence grade {self.evidence_grade.value} requires benchmark evidence"
            )
        if self.evidence_grade in {
            EvidenceGrade.D_ANALYTICAL,
            EvidenceGrade.E_UNVALIDATED,
        } and not self.required_tests:
            raise ValueError(
                f"evidence grade {self.evidence_grade.value} requires requiredTests"
            )
        if self.p50_latency_microseconds.lower > self.p95_latency_microseconds.lower:
            raise ValueError("P50 lower bound cannot exceed P95 lower bound")
        if self.p50_latency_microseconds.upper > self.p95_latency_microseconds.upper:
            raise ValueError("P50 upper bound cannot exceed P95 upper bound")
        if self.p99_latency_microseconds is not None:
            if self.p95_latency_microseconds.lower > self.p99_latency_microseconds.lower:
                raise ValueError("P95 lower bound cannot exceed P99 lower bound")
            if self.p95_latency_microseconds.upper > self.p99_latency_microseconds.upper:
                raise ValueError("P95 upper bound cannot exceed P99 upper bound")
        return self


def performance_forecast_content_digest(
    forecast: PerformanceForecast | dict,
) -> str:
    return _content_digest(forecast)
