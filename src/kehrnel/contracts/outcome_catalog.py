"""Contracts for the industry outcome, architecture, and demo catalog.

The catalog sits above executable :class:`SolutionBlueprint` instances.  It
describes why an architecture exists, which reusable Kehrnel assets prove it,
and how a user can experience the outcome before opening it in the Builder.

References intentionally form a directed graph::

    OutcomePattern <- ReferenceArchitecture <- DemoExperience

Reverse navigation is derived by catalog readers.  Keeping the authored graph
acyclic avoids circular content-addressed references between artifacts.
"""

from __future__ import annotations

from enum import Enum
from typing import List, Literal, Optional

from pydantic import Field, model_validator

from .artifacts import (
    ArtifactRef,
    CapabilityMaturity,
    CatalogArtifactRole,
    CatalogMetadata,
    ContractModel,
    PublicationStatus,
    SupportStatus,
)
from .source_architecture import ConsumptionKind


_ID_PATTERN = r"^[a-z0-9][a-z0-9._-]*$"
_PROMOTED_OUTCOME_MATURITIES = {
    CapabilityMaturity.SIMULATED,
    CapabilityMaturity.COMPILABLE,
    CapabilityMaturity.PILOT_VALIDATED,
    CapabilityMaturity.PRODUCTION_VALIDATED,
}
_COMPILABLE_MATURITIES = {
    CapabilityMaturity.COMPILABLE,
    CapabilityMaturity.PILOT_VALIDATED,
    CapabilityMaturity.PRODUCTION_VALIDATED,
}
_SIMULATED_MATURITIES = {
    CapabilityMaturity.SIMULATED,
    CapabilityMaturity.COMPILABLE,
    CapabilityMaturity.PILOT_VALIDATED,
    CapabilityMaturity.PRODUCTION_VALIDATED,
}


class InteractionEffect(str, Enum):
    """The observable effect an application, workflow, or agent may cause."""

    RETRIEVE = "retrieve"
    SUBSCRIBE = "subscribe"
    ENRICH = "enrich"
    COMMAND = "command"
    COORDINATE = "coordinate"
    RECORD_DECISION = "record_decision"
    REMEMBER = "remember"
    IMPROVE = "improve"


class AutonomyMode(str, Enum):
    """Who selects and executes the next operation.

    Autonomy is deliberately independent from write access.  A direct
    application may write authoritative data, while an agent may remain
    read-only.
    """

    DIRECT_APPLICATION = "direct_application"
    AGENT_ASSISTED = "agent_assisted"
    BOUNDED_AUTONOMOUS = "bounded_autonomous"
    CONTINUOUS_AGENT = "continuous_agent"


class WriteTargetKind(str, Enum):
    SOURCE_SYSTEM = "source_system"
    OPERATIONAL_PRODUCT = "operational_product"
    WORKFLOW_STATE = "workflow_state"
    DERIVED_ARTIFACT = "derived_artifact"
    DECISION_TRACE = "decision_trace"
    MEMORY = "memory"


class ReferenceArchitectureKind(str, Enum):
    CANONICAL_REPOSITORY = "canonical_repository"
    OPERATIONAL_DATA_LAYER = "operational_data_layer"
    EVENT_DRIVEN = "event_driven"
    AGENTIC_CONTEXT = "agentic_context"
    HYBRID = "hybrid"


class InteractionWriteTarget(ContractModel):
    """A durable effect produced by one interaction requirement."""

    kind: WriteTargetKind
    product_id: str = Field(min_length=1, pattern=r"^\S+$")
    authority_required: bool = False
    source_of_truth: bool = False

    @model_validator(mode="after")
    def authoritative_writes_require_authority(self) -> "InteractionWriteTarget":
        if self.source_of_truth and not self.authority_required:
            raise ValueError("sourceOfTruth write targets require authorityRequired")
        return self


class InteractionRequirement(ContractModel):
    """What a consumer can do, not merely which protocol it can call."""

    interaction_id: str = Field(min_length=1, pattern=_ID_PATTERN)
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    effect: InteractionEffect
    autonomy_modes: List[AutonomyMode] = Field(min_length=1)
    read_product_ids: List[str] = Field(default_factory=list)
    write_targets: List[InteractionWriteTarget] = Field(default_factory=list)
    approval_required: bool = False
    audit_required: bool = True

    @model_validator(mode="after")
    def interaction_is_coherent(self) -> "InteractionRequirement":
        if len(self.autonomy_modes) != len(set(self.autonomy_modes)):
            raise ValueError("interaction autonomyModes must be unique")
        if len(self.read_product_ids) != len(set(self.read_product_ids)):
            raise ValueError("interaction readProductIds must be unique")
        write_keys = [
            (target.kind, target.product_id) for target in self.write_targets
        ]
        if len(write_keys) != len(set(write_keys)):
            raise ValueError("interaction writeTargets must be unique")
        effects_that_produce_state = {
            InteractionEffect.ENRICH,
            InteractionEffect.COMMAND,
            InteractionEffect.COORDINATE,
            InteractionEffect.RECORD_DECISION,
            InteractionEffect.REMEMBER,
            InteractionEffect.IMPROVE,
        }
        if self.effect in effects_that_produce_state and not self.write_targets:
            raise ValueError(
                f"'{self.effect.value}' interactions require at least one writeTarget"
            )
        if self.effect in {
            InteractionEffect.RETRIEVE,
            InteractionEffect.SUBSCRIBE,
        } and self.write_targets:
            raise ValueError(
                f"'{self.effect.value}' interactions cannot declare writeTargets"
            )
        return self


class OutcomeScenario(ContractModel):
    """A functional story that can be understood before viewing implementation."""

    scenario_id: str = Field(min_length=1, pattern=_ID_PATTERN)
    name: str = Field(min_length=1)
    actor_roles: List[str] = Field(min_length=1)
    trigger: str = Field(min_length=1)
    goal: str = Field(min_length=1)
    observable_result: str = Field(min_length=1)
    freshness_expectation: str = Field(min_length=1)
    authority_invariant: str = Field(min_length=1)
    agent_example: Optional[str] = None
    failure_case: Optional[str] = None
    success_measures: List[str] = Field(min_length=1)
    interaction_ids: List[str] = Field(min_length=1)

    @model_validator(mode="after")
    def lists_are_unique(self) -> "OutcomeScenario":
        if len(self.actor_roles) != len(set(self.actor_roles)):
            raise ValueError("scenario actorRoles must be unique")
        if len(self.success_measures) != len(set(self.success_measures)):
            raise ValueError("scenario successMeasures must be unique")
        if len(self.interaction_ids) != len(set(self.interaction_ids)):
            raise ValueError("scenario interactionIds must be unique")
        return self


class OutcomePattern(ContractModel):
    """A reusable industry outcome, independent of one physical implementation."""

    contract: Literal["outcome-pattern/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    name: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    industry_experience_ids: List[str] = Field(min_length=1)
    domain_ids: List[str] = Field(min_length=1)
    scenarios: List[OutcomeScenario] = Field(min_length=1)
    source_category_ids: List[str] = Field(min_length=1)
    data_product_ids: List[str] = Field(min_length=1)
    interactions: List[InteractionRequirement] = Field(min_length=1)
    capability_maturity: CapabilityMaturity = CapabilityMaturity.NOT_ASSESSED
    support_status: SupportStatus = SupportStatus.EVALUATION_ONLY
    publication_status: PublicationStatus = PublicationStatus.DRAFT
    evidence_refs: List[ArtifactRef] = Field(default_factory=list)

    @model_validator(mode="after")
    def outcome_is_coherent(self) -> "OutcomePattern":
        if self.artifact_ref.kind != "outcome-pattern":
            raise ValueError("outcome pattern artifactRef.kind must be 'outcome-pattern'")
        if self.catalog.role is not CatalogArtifactRole.OUTCOME_PATTERN:
            raise ValueError("outcome pattern catalog.role must be 'outcome_pattern'")
        self._require_unique("industryExperienceIds", self.industry_experience_ids)
        self._require_unique("domainIds", self.domain_ids)
        self._require_unique("sourceCategoryIds", self.source_category_ids)
        self._require_unique("dataProductIds", self.data_product_ids)
        self._require_unique(
            "scenarioId values", [scenario.scenario_id for scenario in self.scenarios]
        )
        self._require_unique(
            "interactionId values",
            [interaction.interaction_id for interaction in self.interactions],
        )
        known_interactions = {
            interaction.interaction_id for interaction in self.interactions
        }
        for scenario in self.scenarios:
            missing_interactions = sorted(
                set(scenario.interaction_ids) - known_interactions
            )
            if missing_interactions:
                raise ValueError(
                    f"scenario '{scenario.scenario_id}' references unknown "
                    f"interactions: {missing_interactions}"
                )
        known_products = set(self.data_product_ids)
        for interaction in self.interactions:
            referenced_products = set(interaction.read_product_ids) | {
                target.product_id
                for target in interaction.write_targets
                if target.kind is not WriteTargetKind.SOURCE_SYSTEM
            }
            missing = sorted(referenced_products - known_products)
            if missing:
                raise ValueError(
                    f"interaction '{interaction.interaction_id}' references unknown "
                    f"data products: {missing}"
                )
        if (
            self.capability_maturity in _PROMOTED_OUTCOME_MATURITIES
            and not self.evidence_refs
        ):
            raise ValueError(
                "outcome patterns beyond specified maturity require evidenceRefs"
            )
        return self

    @staticmethod
    def _require_unique(label: str, values: List[str]) -> None:
        if len(values) != len(set(values)):
            raise ValueError(f"outcome pattern {label} must be unique")


class PortfolioDomainDefinition(ContractModel):
    """Stable definition of a portfolio domain used to group outcomes."""

    contract: Literal["portfolio-domain/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    primary_segments: List[str] = Field(min_length=1)
    industry_experience_ids: List[str] = Field(min_length=1)

    @model_validator(mode="after")
    def domain_is_coherent(self) -> "PortfolioDomainDefinition":
        if self.artifact_ref.kind != "portfolio-domain":
            raise ValueError(
                "portfolio domain artifactRef.kind must be 'portfolio-domain'"
            )
        if self.catalog.role is not CatalogArtifactRole.PORTFOLIO_DOMAIN:
            raise ValueError(
                "portfolio domain catalog.role must be 'portfolio_domain'"
            )
        self._require_unique("primarySegments", self.primary_segments)
        self._require_unique("industryExperienceIds", self.industry_experience_ids)
        return self

    @staticmethod
    def _require_unique(label: str, values: List[str]) -> None:
        if len(values) != len(set(values)):
            raise ValueError(f"portfolio domain {label} must be unique")


class DataProductDefinition(ContractModel):
    """Reusable logical data product independent of one storage projection."""

    contract: Literal["data-product/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    representation_options: List[str] = Field(min_length=1)
    input_outcome_refs: List[ArtifactRef] = Field(default_factory=list)
    output_outcome_refs: List[ArtifactRef] = Field(default_factory=list)
    owner: Optional[str] = None

    @model_validator(mode="after")
    def product_is_coherent(self) -> "DataProductDefinition":
        if self.artifact_ref.kind != "data-product":
            raise ValueError("data product artifactRef.kind must be 'data-product'")
        if self.catalog.role is not CatalogArtifactRole.DATA_PRODUCT:
            raise ValueError("data product catalog.role must be 'data_product'")
        if len(self.representation_options) != len(set(self.representation_options)):
            raise ValueError("data product representationOptions must be unique")
        self._require_outcome_refs("inputOutcomeRefs", self.input_outcome_refs)
        self._require_outcome_refs("outputOutcomeRefs", self.output_outcome_refs)
        return self

    @staticmethod
    def _require_outcome_refs(label: str, refs: List[ArtifactRef]) -> None:
        invalid = [reference.id for reference in refs if reference.kind != "outcome-pattern"]
        if invalid:
            raise ValueError(f"{label} must reference outcome-pattern artifacts: {invalid}")
        identities = [
            (reference.id, reference.version) for reference in refs
        ]
        if len(identities) != len(set(identities)):
            raise ValueError(f"data product {label} must be unique")


class ArchitectureCapabilityDefinition(ContractModel):
    """Reusable architecture capability and the artifacts it is expected to emit."""

    contract: Literal["architecture-capability/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    generated_artifacts: List[str] = Field(min_length=1)
    reuse_examples: List[str] = Field(min_length=1)
    core_outcome_refs: List[ArtifactRef] = Field(default_factory=list)
    optional_outcome_refs: List[ArtifactRef] = Field(default_factory=list)
    reference: Optional[str] = None
    scope: str = Field(min_length=1)

    @model_validator(mode="after")
    def capability_is_coherent(self) -> "ArchitectureCapabilityDefinition":
        if self.artifact_ref.kind != "architecture-capability":
            raise ValueError(
                "architecture capability artifactRef.kind must be "
                "'architecture-capability'"
            )
        if self.catalog.role is not CatalogArtifactRole.ARCHITECTURE_CAPABILITY:
            raise ValueError(
                "architecture capability catalog.role must be "
                "'architecture_capability'"
            )
        self._require_unique("generatedArtifacts", self.generated_artifacts)
        self._require_unique("reuseExamples", self.reuse_examples)
        self._require_outcome_refs("coreOutcomeRefs", self.core_outcome_refs)
        self._require_outcome_refs("optionalOutcomeRefs", self.optional_outcome_refs)
        core = {(ref.id, ref.version) for ref in self.core_outcome_refs}
        optional = {(ref.id, ref.version) for ref in self.optional_outcome_refs}
        overlap = sorted(core & optional)
        if overlap:
            raise ValueError(
                "architecture capability outcomes cannot be both core and optional: "
                f"{overlap}"
            )
        return self

    @staticmethod
    def _require_unique(label: str, values: List[str]) -> None:
        if len(values) != len(set(values)):
            raise ValueError(f"architecture capability {label} must be unique")

    @staticmethod
    def _require_outcome_refs(label: str, refs: List[ArtifactRef]) -> None:
        invalid = [reference.id for reference in refs if reference.kind != "outcome-pattern"]
        if invalid:
            raise ValueError(f"{label} must reference outcome-pattern artifacts: {invalid}")
        identities = [(reference.id, reference.version) for reference in refs]
        if len(identities) != len(set(identities)):
            raise ValueError(f"architecture capability {label} must be unique")


class VisualPatternDefinition(ContractModel):
    """Reusable experience grammar for demonstrating an outcome truthfully."""

    contract: Literal["visual-pattern/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    screen_components: List[str] = Field(min_length=1)
    audience_interaction: str = Field(min_length=1)
    principle_demonstrated: str = Field(min_length=1)
    architect_inspection: str = Field(min_length=1)
    operation_examples: List[str] = Field(min_length=1)

    @model_validator(mode="after")
    def visual_pattern_is_coherent(self) -> "VisualPatternDefinition":
        if self.artifact_ref.kind != "visual-pattern":
            raise ValueError("visual pattern artifactRef.kind must be 'visual-pattern'")
        if self.catalog.role is not CatalogArtifactRole.VISUAL_PATTERN:
            raise ValueError("visual pattern catalog.role must be 'visual_pattern'")
        if len(self.screen_components) != len(set(self.screen_components)):
            raise ValueError("visual pattern screenComponents must be unique")
        if len(self.operation_examples) != len(set(self.operation_examples)):
            raise ValueError("visual pattern operationExamples must be unique")
        return self


class ReferenceArchitecture(ContractModel):
    """A proven or proposed arrangement of reusable Kehrnel assets."""

    contract: Literal["reference-architecture/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    name: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    architecture_kind: ReferenceArchitectureKind
    outcome_pattern_refs: List[ArtifactRef] = Field(min_length=1)
    source_category_refs: List[ArtifactRef] = Field(min_length=1)
    source_profile_refs: List[ArtifactRef] = Field(default_factory=list)
    data_product_ids: List[str] = Field(min_length=1)
    capability_ids: List[str] = Field(min_length=1)
    consumption_kinds: List[ConsumptionKind] = Field(min_length=1)
    interaction_ids: List[str] = Field(min_length=1)
    strategy_refs: List[ArtifactRef] = Field(default_factory=list)
    mapping_refs: List[ArtifactRef] = Field(default_factory=list)
    semantic_asset_refs: List[ArtifactRef] = Field(default_factory=list)
    blueprint_template_ref: Optional[ArtifactRef] = None
    capability_maturity: CapabilityMaturity = CapabilityMaturity.NOT_ASSESSED
    support_status: SupportStatus = SupportStatus.EVALUATION_ONLY
    publication_status: PublicationStatus = PublicationStatus.DRAFT
    evidence_refs: List[ArtifactRef] = Field(default_factory=list)

    @model_validator(mode="after")
    def architecture_is_coherent(self) -> "ReferenceArchitecture":
        if self.artifact_ref.kind != "reference-architecture":
            raise ValueError(
                "reference architecture artifactRef.kind must be 'reference-architecture'"
            )
        if self.catalog.role is not CatalogArtifactRole.REFERENCE_ARCHITECTURE:
            raise ValueError(
                "reference architecture catalog.role must be 'reference_architecture'"
        )
        self._require_ref_kind("outcomePatternRefs", self.outcome_pattern_refs, "outcome-pattern")
        self._require_ref_kind("sourceCategoryRefs", self.source_category_refs, "source-category")
        self._require_ref_kind("sourceProfileRefs", self.source_profile_refs, "source-profile")
        self._require_ref_kind("strategyRefs", self.strategy_refs, "strategy")
        self._require_ref_kind("mappingRefs", self.mapping_refs, "mapping")
        if (
            self.blueprint_template_ref is not None
            and self.blueprint_template_ref.kind != "solution-blueprint-template"
        ):
            raise ValueError(
                "blueprintTemplateRef must reference a "
                "'solution-blueprint-template' artifact"
            )
        self._require_unique_refs("outcomePatternRefs", self.outcome_pattern_refs)
        self._require_unique_refs("sourceCategoryRefs", self.source_category_refs)
        self._require_unique_refs("sourceProfileRefs", self.source_profile_refs)
        self._require_unique_refs("strategyRefs", self.strategy_refs)
        self._require_unique_refs("mappingRefs", self.mapping_refs)
        self._require_unique_refs("semanticAssetRefs", self.semantic_asset_refs)
        self._require_unique("dataProductIds", self.data_product_ids)
        self._require_unique("capabilityIds", self.capability_ids)
        self._require_unique("consumptionKinds", self.consumption_kinds)
        self._require_unique("interactionIds", self.interaction_ids)
        requires_runtime_proof = (
            self.support_status is SupportStatus.SUPPORTED
            or self.capability_maturity in _COMPILABLE_MATURITIES
        )
        if requires_runtime_proof and not self.strategy_refs:
            raise ValueError(
                "supported or compilable reference architectures require strategyRefs"
            )
        if requires_runtime_proof and not self.evidence_refs:
            raise ValueError(
                "supported or compilable reference architectures require evidenceRefs"
            )
        return self

    @staticmethod
    def _require_ref_kind(label: str, refs: List[ArtifactRef], kind: str) -> None:
        invalid = [reference.id for reference in refs if reference.kind != kind]
        if invalid:
            raise ValueError(f"{label} must reference '{kind}' artifacts: {invalid}")

    @staticmethod
    def _require_unique_refs(label: str, refs: List[ArtifactRef]) -> None:
        keys = [
            (reference.kind, reference.id, reference.version)
            for reference in refs
        ]
        if len(keys) != len(set(keys)):
            raise ValueError(f"reference architecture {label} must be unique")

    @staticmethod
    def _require_unique(label: str, values: List[object]) -> None:
        if len(values) != len(set(values)):
            raise ValueError(f"reference architecture {label} must be unique")


class DemoStoryboardStep(ContractModel):
    step_id: str = Field(min_length=1, pattern=_ID_PATTERN)
    title: str = Field(min_length=1)
    actor_action: str = Field(min_length=1)
    visible_result: str = Field(min_length=1)
    evidence_refs: List[ArtifactRef] = Field(default_factory=list)


class DemoExperience(ContractModel):
    """A truthful visual proof of an outcome and its underlying architecture."""

    contract: Literal["demo-experience/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    name: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    outcome_pattern_refs: List[ArtifactRef] = Field(min_length=1)
    reference_architecture_refs: List[ArtifactRef] = Field(min_length=1)
    interaction_ids: List[str] = Field(min_length=1)
    visual_pattern_id: str = Field(min_length=1, pattern=_ID_PATTERN)
    fixture_pack_refs: List[ArtifactRef] = Field(default_factory=list)
    run_evidence_refs: List[ArtifactRef] = Field(default_factory=list)
    storyboard_steps: List[DemoStoryboardStep] = Field(min_length=1)
    failure_scenario: Optional[str] = None
    capability_maturity: CapabilityMaturity = CapabilityMaturity.NOT_ASSESSED
    support_status: SupportStatus = SupportStatus.EVALUATION_ONLY
    publication_status: PublicationStatus = PublicationStatus.DRAFT
    evidence_refs: List[ArtifactRef] = Field(default_factory=list)

    @model_validator(mode="after")
    def experience_is_coherent(self) -> "DemoExperience":
        if self.artifact_ref.kind != "demo-experience":
            raise ValueError("demo experience artifactRef.kind must be 'demo-experience'")
        if self.catalog.role is not CatalogArtifactRole.DEMO_EXPERIENCE:
            raise ValueError("demo experience catalog.role must be 'demo_experience'")
        self._require_ref_kind(
            "outcomePatternRefs", self.outcome_pattern_refs, "outcome-pattern"
        )
        self._require_ref_kind(
            "referenceArchitectureRefs",
            self.reference_architecture_refs,
            "reference-architecture",
        )
        self._require_ref_kind("fixturePackRefs", self.fixture_pack_refs, "fixture-pack")
        invalid_run_evidence = [
            reference.id
            for reference in self.run_evidence_refs
            if reference.kind
            not in {"simulation-run", "demo-run", "runtime-evidence", "test-run"}
        ]
        if invalid_run_evidence:
            raise ValueError(
                "runEvidenceRefs must reference simulation-run, demo-run, "
                "runtime-evidence, or test-run artifacts: "
                f"{invalid_run_evidence}"
            )
        self._require_unique_refs("outcomePatternRefs", self.outcome_pattern_refs)
        self._require_unique_refs(
            "referenceArchitectureRefs", self.reference_architecture_refs
        )
        self._require_unique_refs("fixturePackRefs", self.fixture_pack_refs)
        self._require_unique_refs("runEvidenceRefs", self.run_evidence_refs)
        if len(self.interaction_ids) != len(set(self.interaction_ids)):
            raise ValueError("demo experience interactionIds must be unique")
        step_ids = [step.step_id for step in self.storyboard_steps]
        if len(step_ids) != len(set(step_ids)):
            raise ValueError("demo experience storyboard stepIds must be unique")
        if self.capability_maturity in _SIMULATED_MATURITIES:
            if not self.fixture_pack_refs:
                raise ValueError(
                    "simulated or higher demo experiences require fixturePackRefs"
                )
            if not self.run_evidence_refs:
                raise ValueError(
                    "simulated or higher demo experiences require runEvidenceRefs"
                )
        return self

    @staticmethod
    def _require_ref_kind(label: str, refs: List[ArtifactRef], kind: str) -> None:
        invalid = [reference.id for reference in refs if reference.kind != kind]
        if invalid:
            raise ValueError(f"{label} must reference '{kind}' artifacts: {invalid}")

    @staticmethod
    def _require_unique_refs(label: str, refs: List[ArtifactRef]) -> None:
        keys = [
            (reference.kind, reference.id, reference.version)
            for reference in refs
        ]
        if len(keys) != len(set(keys)):
            raise ValueError(f"demo experience {label} must be unique")


class IndustryOutcomeCatalog(ContractModel):
    """Versioned, portable catalog consumed by the gallery and Builder."""

    contract: Literal["industry-outcome-catalog/v1"]
    artifact_ref: ArtifactRef
    catalog_id: str = Field(min_length=1, pattern=_ID_PATTERN)
    version: str = Field(min_length=1, pattern=r"^\S+$")
    domains: List[PortfolioDomainDefinition] = Field(min_length=1)
    data_products: List[DataProductDefinition] = Field(min_length=1)
    architecture_capabilities: List[ArchitectureCapabilityDefinition] = Field(
        min_length=1
    )
    visual_patterns: List[VisualPatternDefinition] = Field(min_length=1)
    outcomes: List[OutcomePattern] = Field(min_length=1)
    reference_architectures: List[ReferenceArchitecture] = Field(default_factory=list)
    demo_experiences: List[DemoExperience] = Field(default_factory=list)

    @model_validator(mode="after")
    def references_are_unique_and_resolved(self) -> "IndustryOutcomeCatalog":
        if self.artifact_ref.kind != "industry-outcome-catalog":
            raise ValueError(
                "industry outcome catalog artifactRef.kind must be "
                "'industry-outcome-catalog'"
            )
        if self.artifact_ref.id != self.catalog_id:
            raise ValueError("industry outcome catalog artifactRef.id must equal catalogId")
        if self.artifact_ref.version != self.version:
            raise ValueError(
                "industry outcome catalog artifactRef.version must equal version"
            )

        domain_index = self._definition_id_index("domains", self.domains)
        data_product_index = self._definition_id_index(
            "dataProducts", self.data_products
        )
        capability_index = self._definition_id_index(
            "architectureCapabilities", self.architecture_capabilities
        )
        visual_pattern_index = self._definition_id_index(
            "visualPatterns", self.visual_patterns
        )
        outcome_index = self._index_unique("outcomes", self.outcomes)
        architecture_index = self._index_unique(
            "referenceArchitectures", self.reference_architectures
        )
        self._index_unique("demoExperiences", self.demo_experiences)

        for definition in self.data_products:
            self._require_local_refs(
                f"data product '{definition.artifact_ref.id}' inputOutcomeRefs",
                definition.input_outcome_refs,
                outcome_index,
            )
            self._require_local_refs(
                f"data product '{definition.artifact_ref.id}' outputOutcomeRefs",
                definition.output_outcome_refs,
                outcome_index,
            )
            for outcome_ref in (
                *definition.input_outcome_refs,
                *definition.output_outcome_refs,
            ):
                outcome = outcome_index[self._ref_key(outcome_ref)]
                if definition.artifact_ref.id not in outcome.data_product_ids:
                    raise ValueError(
                        f"data product '{definition.artifact_ref.id}' references "
                        f"outcome '{outcome_ref.id}' but that outcome does not "
                        "declare the data product"
                    )
        for definition in self.architecture_capabilities:
            self._require_local_refs(
                f"architecture capability '{definition.artifact_ref.id}' coreOutcomeRefs",
                definition.core_outcome_refs,
                outcome_index,
            )
            self._require_local_refs(
                f"architecture capability '{definition.artifact_ref.id}' optionalOutcomeRefs",
                definition.optional_outcome_refs,
                outcome_index,
            )

        for outcome in self.outcomes:
            self._require_defined_ids(
                f"outcome '{outcome.artifact_ref.id}' domainIds",
                outcome.domain_ids,
                domain_index,
            )
            self._require_defined_ids(
                f"outcome '{outcome.artifact_ref.id}' dataProductIds",
                outcome.data_product_ids,
                data_product_index,
            )
            outcome_key = self._ref_key(outcome.artifact_ref)
            missing_product_links = sorted(
                product_id
                for product_id in outcome.data_product_ids
                if outcome_key
                not in {
                    self._ref_key(reference)
                    for reference in (
                        *data_product_index[product_id].input_outcome_refs,
                        *data_product_index[product_id].output_outcome_refs,
                    )
                }
            )
            if missing_product_links:
                raise ValueError(
                    f"outcome '{outcome.artifact_ref.id}' dataProductIds are missing "
                    "reverse input/output links: "
                    f"{missing_product_links}"
                )

        for architecture in self.reference_architectures:
            self._require_local_refs(
                f"reference architecture '{architecture.artifact_ref.id}' outcomePatternRefs",
                architecture.outcome_pattern_refs,
                outcome_index,
            )
            referenced_outcomes = [
                outcome_index[self._ref_key(reference)]
                for reference in architecture.outcome_pattern_refs
            ]
            covered_source_categories = {
                category_id
                for outcome in referenced_outcomes
                for category_id in outcome.source_category_ids
            }
            uncovered_source_categories = sorted(
                {
                    reference.id
                    for reference in architecture.source_category_refs
                }
                - covered_source_categories
            )
            if uncovered_source_categories:
                raise ValueError(
                    f"reference architecture '{architecture.artifact_ref.id}' "
                    "sourceCategoryRefs are not covered by its referenced outcomes: "
                    f"{uncovered_source_categories}"
                )
            self._require_defined_ids(
                f"reference architecture '{architecture.artifact_ref.id}' dataProductIds",
                architecture.data_product_ids,
                data_product_index,
            )
            covered_data_products = {
                product_id
                for outcome in referenced_outcomes
                for product_id in outcome.data_product_ids
            }
            uncovered_data_products = sorted(
                set(architecture.data_product_ids) - covered_data_products
            )
            if uncovered_data_products:
                raise ValueError(
                    f"reference architecture '{architecture.artifact_ref.id}' "
                    "dataProductIds are not covered by its referenced outcomes: "
                    f"{uncovered_data_products}"
                )
            self._require_defined_ids(
                f"reference architecture '{architecture.artifact_ref.id}' capabilityIds",
                architecture.capability_ids,
                capability_index,
            )
            architecture_outcome_keys = {
                self._ref_key(reference)
                for reference in architecture.outcome_pattern_refs
            }
            covered_by_capabilities: set[tuple[str, str, str, str]] = set()
            for capability_id in architecture.capability_ids:
                capability = capability_index[capability_id]
                capability_outcomes = {
                    self._ref_key(reference)
                    for reference in (
                        *capability.core_outcome_refs,
                        *capability.optional_outcome_refs,
                    )
                }
                matching_outcomes = architecture_outcome_keys & capability_outcomes
                if not matching_outcomes:
                    raise ValueError(
                        f"reference architecture '{architecture.artifact_ref.id}' "
                        f"capability '{capability_id}' is not related to any of its "
                        "referenced outcomes"
                    )
                covered_by_capabilities.update(matching_outcomes)
            uncovered_by_capabilities = sorted(
                f"{kind}:{artifact_id}@{version}"
                for kind, artifact_id, version, digest in (
                    architecture_outcome_keys - covered_by_capabilities
                )
            )
            if uncovered_by_capabilities:
                raise ValueError(
                    f"reference architecture '{architecture.artifact_ref.id}' "
                    "outcomes are not covered by its selected capabilities: "
                    f"{uncovered_by_capabilities}"
                )
            interaction_ids = {
                interaction.interaction_id
                for reference in architecture.outcome_pattern_refs
                for interaction in outcome_index[self._ref_key(reference)].interactions
            }
            missing_interactions = sorted(
                set(architecture.interaction_ids) - interaction_ids
            )
            if missing_interactions:
                raise ValueError(
                    f"reference architecture '{architecture.artifact_ref.id}' references "
                    f"unknown outcome interactions: {missing_interactions}"
                )

        for experience in self.demo_experiences:
            self._require_local_refs(
                f"demo experience '{experience.artifact_ref.id}' outcomePatternRefs",
                experience.outcome_pattern_refs,
                outcome_index,
            )
            self._require_local_refs(
                f"demo experience '{experience.artifact_ref.id}' referenceArchitectureRefs",
                experience.reference_architecture_refs,
                architecture_index,
            )
            self._require_defined_ids(
                f"demo experience '{experience.artifact_ref.id}' visualPatternId",
                [experience.visual_pattern_id],
                visual_pattern_index,
            )
            architecture_outcomes = {
                self._ref_key(outcome_ref)
                for architecture_ref in experience.reference_architecture_refs
                for outcome_ref in architecture_index[
                    self._ref_key(architecture_ref)
                ].outcome_pattern_refs
            }
            extra_outcomes = sorted(
                f"{reference.id}@{reference.version}"
                for reference in experience.outcome_pattern_refs
                if self._ref_key(reference) not in architecture_outcomes
            )
            if extra_outcomes:
                raise ValueError(
                    f"demo experience '{experience.artifact_ref.id}' outcomePatternRefs "
                    "must be included in its referenced architectures: "
                    f"{extra_outcomes}"
                )
            architecture_interactions = {
                interaction_id
                for reference in experience.reference_architecture_refs
                for interaction_id in architecture_index[
                    self._ref_key(reference)
                ].interaction_ids
            }
            missing_interactions = sorted(
                set(experience.interaction_ids) - architecture_interactions
            )
            if missing_interactions:
                raise ValueError(
                    f"demo experience '{experience.artifact_ref.id}' references "
                    f"unknown architecture interactions: {missing_interactions}"
                )
        return self

    @staticmethod
    def _definition_id_index(label: str, values: List[object]) -> dict[str, object]:
        index: dict[str, object] = {}
        for value in values:
            artifact_ref = value.artifact_ref  # type: ignore[attr-defined]
            if artifact_ref.id in index:
                raise ValueError(
                    f"industry outcome catalog contains duplicate {label} IDs: "
                    f"{artifact_ref.id}"
                )
            index[artifact_ref.id] = value
        return index

    @staticmethod
    def _require_defined_ids(
        label: str, values: List[str], index: dict[str, object]
    ) -> None:
        missing = sorted(set(values) - set(index))
        if missing:
            raise ValueError(f"{label} contains undefined IDs: {missing}")

    @classmethod
    def _index_unique(
        cls, label: str, values: List[object]
    ) -> dict[tuple[str, str, str, str], object]:
        index: dict[tuple[str, str, str, str], object] = {}
        identities: set[tuple[str, str, str]] = set()
        for value in values:
            artifact_ref = value.artifact_ref  # type: ignore[attr-defined]
            identity = (
                artifact_ref.kind,
                artifact_ref.id,
                artifact_ref.version,
            )
            if identity in identities:
                raise ValueError(f"industry outcome catalog contains duplicate {label} refs")
            identities.add(identity)
            key = cls._ref_key(artifact_ref)
            index[key] = value
        return index

    @classmethod
    def _require_local_refs(
        cls,
        label: str,
        refs: List[ArtifactRef],
        index: dict[tuple[str, str, str, str], object],
    ) -> None:
        missing = [
            f"{reference.id}@{reference.version}"
            for reference in refs
            if cls._ref_key(reference) not in index
        ]
        if missing:
            raise ValueError(f"{label} must resolve inside the catalog: {missing}")

    @staticmethod
    def _ref_key(reference: ArtifactRef) -> tuple[str, str, str, str]:
        return (
            reference.kind,
            reference.id,
            reference.version,
            reference.digest,
        )
