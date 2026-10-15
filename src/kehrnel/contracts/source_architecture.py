"""Source-aware contracts for compiling customer data into Kehrnel strategies.

The contracts deliberately separate where data originates, what it means, and
how it must be consumed.  Known product profiles and tenant-discovered profiles
therefore enter the compiler through the same boundary.
"""

from __future__ import annotations

from copy import deepcopy
from enum import Enum
import hashlib
import re
from typing import List, Literal, Optional

from pydantic import Field, JsonValue, field_validator, model_validator

from .artifacts import (
    ArtifactProvenance,
    ArtifactRef,
    CapabilityMaturity,
    CatalogArtifactRole,
    CatalogMetadata,
    ContractModel,
    Sha256Digest,
)
from .package_digest import canonical_json_bytes

class SourceProfileOrigin(str, Enum):
    KNOWN = "known"
    DISCOVERED = "discovered"


class SourceTechnology(str, Enum):
    MONGODB = "mongodb"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    APPLICATION_API = "application_api"
    EVENT_STREAM = "event_stream"
    FILE = "file"
    STANDARD_EXCHANGE = "standard_exchange"
    COMPOSITE_APPLICATION = "composite_application"


class SourceStructureKind(str, Enum):
    COLLECTION = "collection"
    TABLE = "table"
    RESOURCE = "resource"
    MESSAGE = "message"
    OBJECT = "object"
    FILE = "file"


class IngestionMode(str, Enum):
    SNAPSHOT = "snapshot"
    BATCH = "batch"
    CDC = "cdc"
    POLLING = "polling"
    EVENT_STREAM = "event_stream"
    API = "api"


class IntegrationPosture(str, Enum):
    """How much responsibility MongoDB assumes for one source domain."""

    FEDERATED_ACCESS = "federated_access"
    SELECTIVE_MATERIALIZATION = "selective_materialization"
    OPERATIONAL_REPLICA = "operational_replica"
    MONGODB_SYSTEM_OF_RECORD = "mongodb_system_of_record"


class SemanticBindingState(str, Enum):
    DECLARED = "declared"
    INFERRED = "inferred"
    CONFIRMED = "confirmed"


class MappingMode(str, Enum):
    DIRECT = "direct"
    CURATED = "curated"
    GENERATED = "generated"


class MappingReadiness(str, Enum):
    AVAILABLE = "available"
    PROPOSED = "proposed"


class ContextKind(str, Enum):
    TERMINOLOGY = "terminology"
    IDENTITY_RESOLUTION = "identity_resolution"
    DOCUMENTS = "documents"
    PROTOCOLS = "protocols"
    ENRICHMENT = "enrichment"
    DECISION_TRACES = "decision_traces"
    EPISODIC_MEMORY = "episodic_memory"
    PROCEDURAL_MEMORY = "procedural_memory"
    VECTORIZATION = "vectorization"
    RELATIONSHIP_GRAPH = "relationship_graph"


class ConsumptionKind(str, Enum):
    FHIR = "fhir"
    OPENEHR = "openehr"
    CDISC = "cdisc"
    MQL = "mql"
    APPLICATION_API = "application_api"
    COHORT = "cohort"
    SEARCH = "search"
    VECTOR_SEARCH = "vector_search"
    GRAPH = "graph"
    AGENT_TOOLS = "agent_tools"
    ANALYTICS_EXPORT = "analytics_export"
    EVENT_STREAM = "event_stream"


class WorkloadScope(str, Enum):
    RECORD = "record"
    SUBJECT = "subject"
    POPULATION = "population"
    CORPUS = "corpus"
    STREAM = "stream"


class QueryFeature(str, Enum):
    POINT_LOOKUP = "point_lookup"
    FILTER = "filter"
    AGGREGATION = "aggregation"
    FULL_TEXT = "full_text"
    SEMANTIC = "semantic"
    GRAPH_TRAVERSAL = "graph_traversal"
    EXPORT = "export"


class ConsistencyRequirement(str, Enum):
    STRONG = "strong"
    BOUNDED_STALENESS = "bounded_staleness"
    EVENTUAL = "eventual"
    SNAPSHOT = "snapshot"


class CandidateStatus(str, Enum):
    READY = "ready"
    REQUIRES_APPROVAL = "requires_approval"
    BLOCKED = "blocked"


class IssueSeverity(str, Enum):
    INFO = "info"
    REVIEW = "review"
    BLOCKING = "blocking"


class BlueprintOverrideOperation(str, Enum):
    """Restricted JSON Patch operations accepted at the Builder boundary."""

    ADD = "add"
    REMOVE = "remove"
    REPLACE = "replace"


class BlueprintAssumptionDecisionStatus(str, Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class BlueprintInstantiationStatus(str, Enum):
    INSTANTIATED = "instantiated"
    NOT_INSTANTIABLE = "not_instantiable"


class SemanticBinding(ContractModel):
    source_path: str = Field(min_length=1)
    concept_id: str = Field(min_length=1)
    state: SemanticBindingState
    confidence_basis_points: Optional[int] = Field(default=None, ge=0, le=10_000)
    value_set_ref: Optional[str] = None

    @model_validator(mode="after")
    def inferred_bindings_have_confidence(self) -> "SemanticBinding":
        if (
            self.state is SemanticBindingState.INFERRED
            and self.confidence_basis_points is None
        ):
            raise ValueError(
                "inferred semantic bindings require confidenceBasisPoints"
            )
        return self


class SourceField(ContractModel):
    path: str = Field(min_length=1)
    data_type: str = Field(min_length=1)
    nullable: bool = True
    repeated: bool = False


class SourceStructure(ContractModel):
    name: str = Field(min_length=1)
    kind: SourceStructureKind
    primary_keys: List[str] = Field(default_factory=list)
    fields: List[SourceField] = Field(default_factory=list)
    polymorphic: bool = False

    @model_validator(mode="after")
    def fields_are_unique(self) -> "SourceStructure":
        paths = [field.path for field in self.fields]
        if len(paths) != len(set(paths)):
            raise ValueError(f"source structure '{self.name}' contains duplicate field paths")
        return self


class SourceInteraction(ContractModel):
    interaction_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    operation: Literal["create", "update", "delete", "event"]
    structure_refs: List[str] = Field(min_length=1)
    event_name: Optional[str] = None


class SourceCategoryInteraction(ContractModel):
    """Product-neutral business interaction that a category must represent."""

    interaction_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$"
    )
    label: str = Field(min_length=1)
    description: str = Field(min_length=1)
    semantic_concept_ids: List[str] = Field(default_factory=list)


class SourceCategory(ContractModel):
    """Industry category learned from, but not identified with, products."""

    contract: Literal["source-category/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    capability_maturity: CapabilityMaturity = CapabilityMaturity.SPECIFIED
    semantic_domains: List[str] = Field(min_length=1)
    functional_interactions: List[SourceCategoryInteraction] = Field(min_length=1)
    reference_profile_refs: List[ArtifactRef] = Field(default_factory=list)

    @model_validator(mode="after")
    def source_category_is_coherent(self) -> "SourceCategory":
        if self.artifact_ref.kind != "source-category":
            raise ValueError(
                "source category artifactRef.kind must be 'source-category'"
            )
        if self.catalog.role is not CatalogArtifactRole.SOURCE_CATEGORY:
            raise ValueError("source category catalog.role must be 'source_category'")
        interaction_ids = [
            interaction.interaction_id for interaction in self.functional_interactions
        ]
        if len(interaction_ids) != len(set(interaction_ids)):
            raise ValueError("source category contains duplicate interactionId values")
        invalid_refs = [
            reference.id
            for reference in self.reference_profile_refs
            if reference.kind != "source-profile"
        ]
        if invalid_refs:
            raise ValueError(
                f"referenceProfileRefs must reference source profiles: {invalid_refs}"
            )
        return self


class SourceCategoryCatalog(ContractModel):
    contract: Literal["source-category-catalog/v1"]
    categories: List[SourceCategory] = Field(min_length=1)

    @model_validator(mode="after")
    def categories_are_unique(self) -> "SourceCategoryCatalog":
        refs = [
            (category.artifact_ref.id, category.artifact_ref.version)
            for category in self.categories
        ]
        if len(refs) != len(set(refs)):
            raise ValueError("source category catalog contains duplicate id/version pairs")
        return self


class MappingRoute(ContractModel):
    target_model: Literal["openehr", "fhir", "cdisc", "custom_operational"]
    mode: MappingMode
    readiness: MappingReadiness
    strategy_ids: List[str] = Field(default_factory=list)
    mapping_ref: Optional[str] = None

    @model_validator(mode="after")
    def non_direct_routes_identify_mapping(self) -> "MappingRoute":
        if self.mode is MappingMode.CURATED and not self.mapping_ref:
            raise ValueError("curated mapping routes require mappingRef")
        return self


class SourceProfile(ContractModel):
    contract: Literal["source-profile/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    origin: SourceProfileOrigin
    technology: SourceTechnology
    product_name: Optional[str] = None
    product_version: Optional[str] = None
    category_ids: List[str] = Field(min_length=1)
    capability_maturity: CapabilityMaturity = CapabilityMaturity.SPECIFIED
    ingestion_modes: List[IngestionMode] = Field(min_length=1)
    structures: List[SourceStructure] = Field(min_length=1)
    interactions: List[SourceInteraction] = Field(default_factory=list)
    semantic_bindings: List[SemanticBinding] = Field(default_factory=list)
    mapping_routes: List[MappingRoute] = Field(default_factory=list)

    @model_validator(mode="after")
    def source_profile_is_coherent(self) -> "SourceProfile":
        if self.artifact_ref.kind != "source-profile":
            raise ValueError("source profile artifactRef.kind must be 'source-profile'")
        if self.catalog.role is not CatalogArtifactRole.SOURCE_PROFILE:
            raise ValueError("source profile catalog.role must be 'source_profile'")
        names = [structure.name for structure in self.structures]
        if len(names) != len(set(names)):
            raise ValueError("source profile contains duplicate structure names")
        if len(self.category_ids) != len(set(self.category_ids)):
            raise ValueError("source profile contains duplicate categoryIds")
        known_structures = set(names)
        for interaction in self.interactions:
            missing = sorted(set(interaction.structure_refs) - known_structures)
            if missing:
                raise ValueError(
                    f"interaction '{interaction.interaction_id}' references unknown structures: {missing}"
                )
        if self.origin is SourceProfileOrigin.DISCOVERED and not self.semantic_bindings:
            raise ValueError("discovered source profiles require semanticBindings")
        if self.origin is SourceProfileOrigin.DISCOVERED and self.catalog.provenance not in {
            ArtifactProvenance.TENANT_GENERATED,
            ArtifactProvenance.TENANT_EXTENDED,
        }:
            raise ValueError(
                "discovered source profiles must be tenant_generated or tenant_extended"
            )
        if self.origin is SourceProfileOrigin.KNOWN and self.catalog.provenance in {
            ArtifactProvenance.TENANT_GENERATED,
            ArtifactProvenance.TENANT_EXTENDED,
        }:
            raise ValueError("known source profiles cannot have tenant provenance")
        return self


class SourceProfileCatalog(ContractModel):
    contract: Literal["source-profile-catalog/v1"]
    profiles: List[SourceProfile] = Field(min_length=1)

    @model_validator(mode="after")
    def profiles_are_unique(self) -> "SourceProfileCatalog":
        refs = [
            (profile.artifact_ref.id, profile.artifact_ref.version)
            for profile in self.profiles
        ]
        if len(refs) != len(set(refs)):
            raise ValueError("source profile catalog contains duplicate id/version pairs")
        return self


class SourceInstanceSpec(ContractModel):
    instance_id: str = Field(min_length=1)
    profile_ref: ArtifactRef
    ingestion_mode: IngestionMode
    integration_posture: IntegrationPosture = IntegrationPosture.SELECTIVE_MATERIALIZATION
    interaction_ids: Optional[List[str]] = None
    site_id: Optional[str] = None
    tenant_key: Optional[str] = None
    region: Optional[str] = None

    @model_validator(mode="after")
    def profile_reference_kind(self) -> "SourceInstanceSpec":
        if self.profile_ref.kind != "source-profile":
            raise ValueError("profileRef.kind must be 'source-profile'")
        if self.interaction_ids is not None:
            if not self.interaction_ids:
                raise ValueError("interactionIds must be omitted or non-empty")
            if len(self.interaction_ids) != len(set(self.interaction_ids)):
                raise ValueError("source interactionIds must be unique")
        return self


class ContextRequirement(ContractModel):
    context_id: str = Field(min_length=1)
    kind: ContextKind
    required: bool = True
    applies_to_source_ids: List[str] = Field(default_factory=list)
    concept_ids: List[str] = Field(default_factory=list)
    provider_ref: Optional[str] = None


class WorkloadEnvelope(ContractModel):
    """The workload facts that can justify a physical access decision."""

    scope: WorkloadScope = WorkloadScope.SUBJECT
    query_features: List[QueryFeature] = Field(default_factory=list)
    consistency: ConsistencyRequirement = ConsistencyRequirement.STRONG
    peak_requests_per_second: Optional[int] = Field(default=None, ge=0)
    expected_result_documents: Optional[int] = Field(default=None, ge=0)

    @model_validator(mode="after")
    def query_features_are_unique(self) -> "WorkloadEnvelope":
        if len(self.query_features) != len(set(self.query_features)):
            raise ValueError("workload queryFeatures must be unique")
        return self


class ConsumptionRequirement(ContractModel):
    consumption_id: str = Field(min_length=1)
    kind: ConsumptionKind
    protocol: Optional[str] = None
    operations: List[str] = Field(default_factory=list)
    latency_target_ms: Optional[int] = Field(default=None, gt=0)
    freshness_target_seconds: Optional[int] = Field(default=None, ge=0)
    workload: WorkloadEnvelope = Field(default_factory=WorkloadEnvelope)


class ArchitectureConstraints(ContractModel):
    tenancy_mode: Literal["single_tenant", "multi_tenant"] = "single_tenant"
    regions: List[str] = Field(default_factory=list)
    estimated_documents: Optional[int] = Field(default=None, ge=0)
    peak_reads_per_second: Optional[int] = Field(default=None, ge=0)
    peak_writes_per_second: Optional[int] = Field(default=None, ge=0)
    retention_days: Optional[int] = Field(default=None, gt=0)


_JSON_POINTER_PATTERN = re.compile(r"^(?:/(?:[^~/]|~[01])*)+$")
_BLUEPRINT_OVERRIDE_ROOTS = {
    "objective",
    "participants",
    "interactions",
    "semanticAuthorities",
    "recordAuthorityPolicies",
    "dataPostures",
    "constraints",
    "targetCapabilityProfileRef",
    "preferredFoundationRefs",
    "assumptions",
    "decisions",
}

_LEGACY_BLUEPRINT_OVERRIDE_ROOTS = {
    "industry",
    "sources",
    "context",
    "consumptions",
    "constraints",
    "preferredStrategyIds",
}


def _validate_json_pointer(value: str, *, allow_root: bool) -> str:
    if value == "" and allow_root:
        return value
    if not _JSON_POINTER_PATTERN.fullmatch(value):
        raise ValueError(
            "must be an RFC 6901 JSON Pointer with valid '~0' and '~1' escapes"
        )
    return value


def _validate_blueprint_structure(
    *,
    sources: List[SourceInstanceSpec],
    context: List[ContextRequirement],
    consumptions: List[ConsumptionRequirement],
) -> None:
    source_ids = [source.instance_id for source in sources]
    if len(source_ids) != len(set(source_ids)):
        raise ValueError("solution blueprint contains duplicate source instanceId values")
    context_ids = [item.context_id for item in context]
    if len(context_ids) != len(set(context_ids)):
        raise ValueError("solution blueprint contains duplicate contextId values")
    consumption_ids = [item.consumption_id for item in consumptions]
    if len(consumption_ids) != len(set(consumption_ids)):
        raise ValueError("solution blueprint contains duplicate consumptionId values")
    known_sources = set(source_ids)
    for item in context:
        missing = sorted(set(item.applies_to_source_ids) - known_sources)
        if missing:
            raise ValueError(
                f"context requirement '{item.context_id}' references unknown sources: {missing}"
            )


class BlueprintAssumption(ContractModel):
    """A visible premise inherited from a reusable architecture template."""

    assumption_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$"
    )
    statement: str = Field(min_length=1)
    applies_to_paths: List[str] = Field(default_factory=list)
    evidence_refs: List[ArtifactRef] = Field(default_factory=list)

    @field_validator("applies_to_paths")
    @classmethod
    def paths_are_json_pointers(cls, values: List[str]) -> List[str]:
        if len(values) != len(set(values)):
            raise ValueError("assumption appliesToPaths must be unique")
        return [
            _validate_json_pointer(value, allow_root=True) for value in values
        ]

    @model_validator(mode="after")
    def evidence_is_unique(self) -> "BlueprintAssumption":
        identities = [
            (reference.kind, reference.id, reference.version, reference.digest)
            for reference in self.evidence_refs
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("assumption evidenceRefs must be unique")
        return self


class BlueprintAssumptionDecision(ContractModel):
    """A customer decision about one exact template assumption."""

    assumption_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$"
    )
    status: BlueprintAssumptionDecisionStatus
    rationale: str = Field(min_length=1)


class BlueprintOverride(ContractModel):
    """One explicit, reviewable delta from an immutable blueprint template.

    This intentionally implements only the ``add``, ``remove`` and ``replace``
    subset of JSON Patch.  Null add/replace values are excluded because the
    common wire serializer omits ``None``; callers should remove an optional
    field instead of producing an ambiguous patch.
    """

    override_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$"
    )
    operation: BlueprintOverrideOperation
    path: str = Field(min_length=1)
    value: Optional[JsonValue] = None
    reason: str = Field(min_length=1)
    assumption_ids: List[str] = Field(default_factory=list)

    @field_validator("path")
    @classmethod
    def path_is_editable_json_pointer(cls, value: str) -> str:
        pointer = _validate_json_pointer(value, allow_root=False)
        root = pointer.split("/", 2)[1]
        if root not in _BLUEPRINT_OVERRIDE_ROOTS:
            raise ValueError(
                "override path must target an editable canonical Blueprint field"
            )
        return pointer

    @model_validator(mode="after")
    def operation_has_coherent_value(self) -> "BlueprintOverride":
        if self.operation in {
            BlueprintOverrideOperation.ADD,
            BlueprintOverrideOperation.REPLACE,
        } and self.value is None:
            raise ValueError("add and replace overrides require a non-null value")
        if self.operation is BlueprintOverrideOperation.REMOVE and self.value is not None:
            raise ValueError("remove overrides cannot include a value")
        if self.value is not None:
            try:
                canonical_json_bytes({"value": self.value})
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "override value must use Kehrnel's canonical JSON subset"
                ) from exc
        if len(self.assumption_ids) != len(set(self.assumption_ids)):
            raise ValueError("override assumptionIds must be unique")
        invalid_assumption_ids = [
            assumption_id
            for assumption_id in self.assumption_ids
            if not re.fullmatch(r"^[a-z0-9][a-z0-9._-]*$", assumption_id)
        ]
        if invalid_assumption_ids:
            raise ValueError(
                "override assumptionIds must use normalized identifiers: "
                f"{invalid_assumption_ids}"
            )
        return self


class LegacyBlueprintOverride(BlueprintOverride):
    """Restricted patch used only by the legacy source gallery adapter."""

    @field_validator("path")
    @classmethod
    def path_is_editable_json_pointer(cls, value: str) -> str:
        pointer = _validate_json_pointer(value, allow_root=False)
        root = pointer.split("/", 2)[1]
        if root not in _LEGACY_BLUEPRINT_OVERRIDE_ROOTS:
            raise ValueError("override path must target an editable legacy Blueprint field")
        return pointer


class SolutionBlueprintTemplate(ContractModel):
    """Immutable, content-addressed defaults for creating a customer blueprint.

    A Reference Architecture may be broader than this executable hand-off.  It
    is instantiable only when it points to one of these templates; the service
    must never infer missing source, ingestion, workload, or consumption facts
    from category-level portfolio metadata.
    """

    contract: Literal["solution-blueprint-template/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    industry: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    sources: List[SourceInstanceSpec] = Field(min_length=1)
    context: List[ContextRequirement] = Field(default_factory=list)
    consumptions: List[ConsumptionRequirement] = Field(min_length=1)
    constraints: ArchitectureConstraints = Field(default_factory=ArchitectureConstraints)
    preferred_strategy_ids: List[str] = Field(default_factory=list)
    assumptions: List[BlueprintAssumption] = Field(default_factory=list)

    @model_validator(mode="after")
    def template_is_coherent(self) -> "SolutionBlueprintTemplate":
        if self.artifact_ref.kind != "solution-blueprint-template":
            raise ValueError(
                "solution blueprint template artifactRef.kind must be "
                "'solution-blueprint-template'"
            )
        if self.catalog.role is not CatalogArtifactRole.SOLUTION_BLUEPRINT_TEMPLATE:
            raise ValueError(
                "solution blueprint template catalog.role must be "
                "'solution_blueprint_template'"
            )
        _validate_blueprint_structure(
            sources=self.sources,
            context=self.context,
            consumptions=self.consumptions,
        )
        if len(self.preferred_strategy_ids) != len(set(self.preferred_strategy_ids)):
            raise ValueError("template preferredStrategyIds must be unique")
        assumption_ids = [item.assumption_id for item in self.assumptions]
        if len(assumption_ids) != len(set(assumption_ids)):
            raise ValueError("template assumptionId values must be unique")
        return self


class SolutionBlueprintTemplateCatalog(ContractModel):
    """Portable collection of immutable Builder hand-off templates."""

    contract: Literal["solution-blueprint-template-catalog/v1"]
    templates: List[SolutionBlueprintTemplate] = Field(min_length=1)

    @model_validator(mode="after")
    def template_versions_are_unique(self) -> "SolutionBlueprintTemplateCatalog":
        identities = [
            (template.artifact_ref.id, template.artifact_ref.version)
            for template in self.templates
        ]
        if len(identities) != len(set(identities)):
            raise ValueError(
                "solution blueprint template catalog contains duplicate "
                "id/version pairs"
            )
        return self


class LegacySourceBlueprint(ContractModel):
    """Obsolete source/consumption prototype retained for migration tooling only."""
    contract: Literal["legacy-source-blueprint/internal"]
    blueprint_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    industry: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    revision: int = Field(default=1, ge=1)
    sources: List[SourceInstanceSpec] = Field(min_length=1)
    context: List[ContextRequirement] = Field(default_factory=list)
    consumptions: List[ConsumptionRequirement] = Field(min_length=1)
    constraints: ArchitectureConstraints = Field(default_factory=ArchitectureConstraints)
    preferred_strategy_ids: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def references_are_unique_and_resolved(self) -> "LegacySourceBlueprint":
        _validate_blueprint_structure(
            sources=self.sources,
            context=self.context,
            consumptions=self.consumptions,
        )
        if len(self.preferred_strategy_ids) != len(set(self.preferred_strategy_ids)):
            raise ValueError("Blueprint preferredStrategyIds must be unique")
        return self


def legacy_source_blueprint_content_digest(
    blueprint: LegacySourceBlueprint | dict,
) -> str:
    """Digest the complete canonical wire representation of a Blueprint."""

    if isinstance(blueprint, LegacySourceBlueprint):
        payload = blueprint.to_wire()
    else:
        payload = deepcopy(blueprint)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class ReferenceArchitectureInstantiationRequest(ContractModel):
    """Create a reviewable Blueprint from one exact Reference Architecture."""

    contract: Literal["reference-architecture-instantiation-request/v1"]
    reference_architecture_ref: ArtifactRef
    blueprint_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    revision: int = Field(default=1, ge=1)
    assumption_decisions: List[BlueprintAssumptionDecision] = Field(
        default_factory=list
    )
    overrides: List[LegacyBlueprintOverride] = Field(default_factory=list)

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ReferenceArchitectureInstantiationRequest":
        if self.reference_architecture_ref.kind != "reference-architecture":
            raise ValueError(
                "referenceArchitectureRef.kind must be 'reference-architecture'"
            )
        decision_ids = [item.assumption_id for item in self.assumption_decisions]
        if len(decision_ids) != len(set(decision_ids)):
            raise ValueError("assumptionDecisions must reference unique assumptionIds")
        override_ids = [item.override_id for item in self.overrides]
        if len(override_ids) != len(set(override_ids)):
            raise ValueError("overrideId values must be unique")
        return self


class StrategyCompilationRequest(ContractModel):
    contract: Literal["strategy-compilation-request/v1"]
    blueprint: LegacySourceBlueprint
    discovered_profiles: List[SourceProfile] = Field(default_factory=list)

    @model_validator(mode="after")
    def inline_profiles_are_discovered(self) -> "StrategyCompilationRequest":
        invalid = [
            profile.artifact_ref.id
            for profile in self.discovered_profiles
            if profile.origin is not SourceProfileOrigin.DISCOVERED
        ]
        if invalid:
            raise ValueError(
                f"discoveredProfiles may contain only discovered source profiles: {invalid}"
            )
        return self


class CompilationIssue(ContractModel):
    code: str = Field(min_length=1)
    severity: IssueSeverity
    message: str = Field(min_length=1)
    refs: List[str] = Field(default_factory=list)


class ReferenceArchitectureInstantiationResult(ContractModel):
    """Truthful result of the non-executing gallery-to-Builder hand-off."""

    contract: Literal["reference-architecture-instantiation-result/v1"]
    status: BlueprintInstantiationStatus
    request_digest: Sha256Digest
    derivation_digest: Sha256Digest
    requested_blueprint_id: str = Field(
        min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$"
    )
    requested_revision: int = Field(ge=1)
    reference_architecture_ref: ArtifactRef
    blueprint_template_ref: Optional[ArtifactRef] = None
    blueprint: Optional[LegacySourceBlueprint] = None
    assumptions: List[BlueprintAssumption] = Field(default_factory=list)
    assumption_decisions: List[BlueprintAssumptionDecision] = Field(
        default_factory=list
    )
    overrides: List[LegacyBlueprintOverride] = Field(default_factory=list)
    requires_review: bool
    issues: List[CompilationIssue] = Field(default_factory=list)

    @model_validator(mode="after")
    def result_is_coherent(self) -> "ReferenceArchitectureInstantiationResult":
        if self.reference_architecture_ref.kind != "reference-architecture":
            raise ValueError(
                "referenceArchitectureRef.kind must be 'reference-architecture'"
            )
        if (
            self.blueprint_template_ref is not None
            and self.blueprint_template_ref.kind != "solution-blueprint-template"
        ):
            raise ValueError(
                "blueprintTemplateRef.kind must be 'solution-blueprint-template'"
            )

        assumption_ids = [item.assumption_id for item in self.assumptions]
        if len(assumption_ids) != len(set(assumption_ids)):
            raise ValueError("result assumptionId values must be unique")
        known_assumptions = set(assumption_ids)
        decision_ids = [item.assumption_id for item in self.assumption_decisions]
        if len(decision_ids) != len(set(decision_ids)):
            raise ValueError("result assumptionDecisions must be unique")
        unknown_decisions = sorted(set(decision_ids) - known_assumptions)
        if unknown_decisions:
            raise ValueError(
                "assumptionDecisions reference unknown assumptions: "
                f"{unknown_decisions}"
            )

        override_ids = [item.override_id for item in self.overrides]
        if len(override_ids) != len(set(override_ids)):
            raise ValueError("result overrideId values must be unique")
        unknown_override_assumptions = sorted(
            {
                assumption_id
                for override in self.overrides
                for assumption_id in override.assumption_ids
            }
            - known_assumptions
        )
        if unknown_override_assumptions:
            raise ValueError(
                "overrides reference unknown assumptions: "
                f"{unknown_override_assumptions}"
            )

        blocking = any(
            issue.severity is IssueSeverity.BLOCKING for issue in self.issues
        )
        if self.status is BlueprintInstantiationStatus.INSTANTIATED:
            if self.blueprint_template_ref is None or self.blueprint is None:
                raise ValueError(
                    "instantiated results require blueprintTemplateRef and blueprint"
                )
            if self.blueprint.blueprint_id != self.requested_blueprint_id:
                raise ValueError(
                    "instantiated blueprintId must match requestedBlueprintId"
                )
            if self.blueprint.revision != self.requested_revision:
                raise ValueError(
                    "instantiated blueprint revision must match requestedRevision"
                )
            if blocking:
                raise ValueError("instantiated results cannot contain blocking issues")
        else:
            if self.blueprint is not None:
                raise ValueError(
                    "not_instantiable results cannot contain a blueprint"
                )
            if not blocking:
                raise ValueError(
                    "not_instantiable results require at least one blocking issue"
                )

        unresolved_assumptions = known_assumptions - set(decision_ids)
        has_review_issue = any(
            issue.severity is IssueSeverity.REVIEW for issue in self.issues
        )
        if (
            unresolved_assumptions or self.overrides or has_review_issue
        ) and not self.requires_review:
            raise ValueError(
                "unresolved assumptions, overrides, and review issues require review"
            )
        return self


class SourceCompilationPlan(ContractModel):
    source_instance_id: str
    source_profile_ref: ArtifactRef
    ingestion_mode: IngestionMode
    integration_posture: IntegrationPosture
    interaction_ids: Optional[List[str]] = None
    target_model: Literal["openehr", "fhir", "cdisc", "custom_operational"]
    mapping_mode: MappingMode
    mapping_ref: Optional[str] = None
    simulation_mapping_refs: Optional[List[ArtifactRef]] = None
    strategy_id: str


class StrategySelection(ContractModel):
    strategy_id: str
    generated: bool = False
    reason: str
    source_instance_ids: List[str] = Field(default_factory=list)
    consumption_ids: List[str] = Field(default_factory=list)


class InterfacePlan(ContractModel):
    consumption_id: str
    kind: ConsumptionKind
    protocol: Optional[str] = None
    operations: List[str] = Field(default_factory=list)
    strategy_id: str


class StrategyCandidate(ContractModel):
    contract: Literal["strategy-candidate/v1"]
    blueprint_id: str
    blueprint_revision: int
    status: CandidateStatus
    candidate_digest: Sha256Digest
    requires_approval: bool
    source_plans: List[SourceCompilationPlan]
    strategy_selections: List[StrategySelection]
    interface_plans: List[InterfacePlan]
    issues: List[CompilationIssue] = Field(default_factory=list)
