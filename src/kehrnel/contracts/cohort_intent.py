"""Protocol-neutral contracts for governed clinical cohort resolution.

Natural-language parsing stops at :class:`ClinicalCohortIntent`.  Terminology
codes and physical paths are deliberately absent from an unresolved mention.
They are added later through independently evidenced terminology and structural
binding receipts.
"""

from __future__ import annotations

import hashlib
from enum import Enum
from typing import Any, Literal, Optional, Union

from pydantic import Field, model_validator

from .artifacts import ArtifactRef, ContractModel, Sha256Digest
from .package_digest import canonical_json_bytes


CLINICAL_COHORT_INTENT_CONTRACT = "clinical-cohort-intent/v1"
COHORT_RESOLUTION_CONTRACT = "clinical-cohort-resolution/v1"
MAX_COHORT_NODES = 64
MAX_COHORT_DEPTH = 8
MAX_TERMINOLOGY_EXPANSION_CODES = 512


class ConceptRelationship(str, Enum):
    EXACT = "exact"
    DESCENDANT = "descendant"
    DESCENDANT_OR_SELF = "descendant_or_self"


class CohortBooleanOperator(str, Enum):
    ALL = "all"
    ANY = "any"
    NOT = "not"


class CohortComparisonOperator(str, Enum):
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"


class ResolutionStatus(str, Enum):
    RESOLVED = "resolved"
    CLARIFICATION_REQUIRED = "clarification_required"
    UNSUPPORTED = "unsupported"


class ResolutionDecision(str, Enum):
    READY = "ready"
    CLARIFICATION_REQUIRED = "clarification_required"
    UNSUPPORTED = "unsupported"


class StructuralResolverKind(str, Enum):
    NATIVE_STRATEGY = "native_strategy"
    COMPILED_SEMANTIC_PACK = "compiled_semantic_pack"
    FEDERATED = "federated"


class TerminologyCandidateAuthority(str, Enum):
    EXPLICIT_CODE = "explicit_code"
    ACCEPTED_BINDING = "accepted_binding"
    PROVIDER_VERIFIED = "provider_verified"
    INFERRED = "inferred"
    MODEL_PROPOSED = "model_proposed"


class TerminologyExpressionLanguage(str, Enum):
    CODE = "code"
    ECL = "ecl"
    VALUE_SET = "value_set"


class ExplicitCoding(ContractModel):
    system: str = Field(min_length=1, max_length=2_048)
    code: str = Field(min_length=1, max_length=256)
    version: Optional[str] = Field(default=None, max_length=512)
    display: Optional[str] = Field(default=None, max_length=512)


class ConceptMention(ContractModel):
    mention_id: str = Field(min_length=1, max_length=128)
    text: Optional[str] = Field(default=None, min_length=1, max_length=1_024)
    semantic_category: str = Field(min_length=1, max_length=128)
    requested_relationship: ConceptRelationship = ConceptRelationship.EXACT
    terminology_system_hint: Optional[str] = Field(default=None, max_length=2_048)
    value_set_hint: Optional[str] = Field(default=None, max_length=2_048)
    explicit_coding: Optional[ExplicitCoding] = None

    @model_validator(mode="after")
    def has_text_or_explicit_coding(self) -> "ConceptMention":
        if not self.text and self.explicit_coding is None:
            raise ValueError("a concept mention requires text or an explicit coding")
        if self.explicit_coding and self.terminology_system_hint:
            if self.explicit_coding.system != self.terminology_system_hint:
                raise ValueError("explicit coding and terminology system hint disagree")
        return self


class TemporalConstraint(ContractModel):
    semantic_role: str = Field(min_length=1, max_length=128)
    operator: CohortComparisonOperator
    value: Any
    timezone: Optional[str] = Field(default=None, max_length=128)


class ScalarConstraint(ContractModel):
    operator: CohortComparisonOperator
    value: Any = None
    unit: Optional[str] = Field(default=None, max_length=128)


class CohortPredicate(ContractModel):
    node_type: Literal["predicate"] = "predicate"
    predicate_id: str = Field(min_length=1, max_length=128)
    semantic_role: str = Field(min_length=1, max_length=128)
    concept: Optional[ConceptMention] = None
    comparison: Optional[ScalarConstraint] = None
    temporal: list[TemporalConstraint] = Field(default_factory=list, max_length=8)
    correlation_group: Optional[str] = Field(default=None, max_length=128)

    @model_validator(mode="after")
    def has_a_constraint(self) -> "CohortPredicate":
        if self.concept is None and self.comparison is None and not self.temporal:
            raise ValueError("a cohort predicate requires a concept, comparison, or temporal constraint")
        return self


class CohortGroup(ContractModel):
    node_type: Literal["group"] = "group"
    group_id: str = Field(min_length=1, max_length=128)
    operator: CohortBooleanOperator
    children: list["CohortExpression"] = Field(min_length=1, max_length=MAX_COHORT_NODES)

    @model_validator(mode="after")
    def not_has_one_child(self) -> "CohortGroup":
        if self.operator is CohortBooleanOperator.NOT and len(self.children) != 1:
            raise ValueError("a NOT group must contain exactly one child")
        return self


CohortExpression = Union[CohortPredicate, CohortGroup]


class CohortResultShape(ContractModel):
    kind: Literal["pseudonymous_patient_list", "patient_count"] = "pseudonymous_patient_list"
    limit: int = Field(default=50, ge=1, le=1_000)
    include_evidence: bool = True


class ClinicalCohortIntent(ContractModel):
    contract: Literal["clinical-cohort-intent/v1"] = CLINICAL_COHORT_INTENT_CONTRACT
    intent_id: str = Field(min_length=1, max_length=128)
    interaction_profile: Literal["clinical_cohort"] = "clinical_cohort"
    scope: Literal["cross_patient"] = "cross_patient"
    source_language: str = Field(default="en", min_length=2, max_length=32)
    criteria: CohortExpression
    result: CohortResultShape = Field(default_factory=CohortResultShape)

    @model_validator(mode="after")
    def tree_is_bounded_and_identifiable(self) -> "ClinicalCohortIntent":
        node_ids: list[str] = []
        mention_ids: list[str] = []

        def visit(node: CohortExpression, depth: int) -> None:
            if depth > MAX_COHORT_DEPTH:
                raise ValueError(f"cohort criteria depth must not exceed {MAX_COHORT_DEPTH}")
            node_ids.append(node.predicate_id if isinstance(node, CohortPredicate) else node.group_id)
            if isinstance(node, CohortPredicate) and node.concept is not None:
                mention_ids.append(node.concept.mention_id)
            if isinstance(node, CohortGroup):
                for child in node.children:
                    visit(child, depth + 1)

        visit(self.criteria, 1)
        if len(node_ids) > MAX_COHORT_NODES:
            raise ValueError(f"cohort criteria must not exceed {MAX_COHORT_NODES} nodes")
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("cohort criteria node identifiers must be unique")
        if len(mention_ids) != len(set(mention_ids)):
            raise ValueError("cohort concept mention identifiers must be unique")
        return self


class TerminologyCandidate(ContractModel):
    system: str = Field(min_length=1, max_length=2_048)
    code: str = Field(min_length=1, max_length=256)
    display: str = Field(min_length=1, max_length=512)
    provider_id: str = Field(min_length=1, max_length=128)
    version: Optional[str] = Field(default=None, max_length=512)
    score_basis_points: int = Field(ge=0, le=10_000)
    authority: TerminologyCandidateAuthority
    verified: bool = False
    match_methods: list[str] = Field(default_factory=list, max_length=16)
    evidence: dict[str, Any] = Field(default_factory=dict)


class TerminologyExpansion(ContractModel):
    """Complete, version-pinned code set authorized for query compilation."""

    provider_id: str = Field(min_length=1, max_length=128)
    system: str = Field(min_length=1, max_length=2_048)
    version: str = Field(min_length=1, max_length=512)
    expression_language: TerminologyExpressionLanguage
    expression: str = Field(min_length=1, max_length=2_048)
    value_set_uri: Optional[str] = Field(default=None, max_length=2_048)
    value_set_version: Optional[str] = Field(default=None, max_length=512)
    codes: list[str] = Field(min_length=1, max_length=MAX_TERMINOLOGY_EXPANSION_CODES)
    truncated: Literal[False] = False
    expansion_digest: Sha256Digest

    @model_validator(mode="after")
    def is_complete_and_canonical(self) -> "TerminologyExpansion":
        if self.codes != sorted(set(self.codes)):
            raise ValueError("terminology expansion codes must be sorted and unique")
        if bool(self.value_set_uri) != bool(self.value_set_version):
            raise ValueError("valueSetUri and valueSetVersion must be supplied together")
        if self.expression_language is TerminologyExpressionLanguage.VALUE_SET and not self.value_set_uri:
            raise ValueError("a value-set expansion requires valueSetUri and valueSetVersion")
        if self.expression_language is not TerminologyExpressionLanguage.VALUE_SET and self.value_set_uri:
            raise ValueError("valueSetUri is only valid for value-set expansions")
        expected = terminology_expansion_digest(
            provider_id=self.provider_id,
            system=self.system,
            version=self.version,
            expression_language=self.expression_language,
            expression=self.expression,
            value_set_uri=self.value_set_uri,
            value_set_version=self.value_set_version,
            codes=self.codes,
        )
        if self.expansion_digest != expected:
            raise ValueError("terminology expansion digest does not match its content")
        return self


def terminology_expansion_digest(
    *,
    provider_id: str,
    system: str,
    version: str,
    expression_language: TerminologyExpressionLanguage | str,
    expression: str,
    value_set_uri: Optional[str],
    value_set_version: Optional[str],
    codes: list[str],
) -> str:
    payload = {
        "providerId": provider_id,
        "system": system,
        "version": version,
        "expressionLanguage": TerminologyExpressionLanguage(expression_language).value,
        "expression": expression,
        "valueSetUri": value_set_uri,
        "valueSetVersion": value_set_version,
        "codes": sorted(set(codes)),
        "truncated": False,
    }
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def create_terminology_expansion(
    *,
    provider_id: str,
    system: str,
    version: str,
    expression_language: TerminologyExpressionLanguage | str,
    expression: str,
    codes: list[str],
    value_set_uri: Optional[str] = None,
    value_set_version: Optional[str] = None,
) -> TerminologyExpansion:
    canonical_codes = sorted(set(codes))
    digest = terminology_expansion_digest(
        provider_id=provider_id,
        system=system,
        version=version,
        expression_language=expression_language,
        expression=expression,
        value_set_uri=value_set_uri,
        value_set_version=value_set_version,
        codes=canonical_codes,
    )
    return TerminologyExpansion(
        provider_id=provider_id,
        system=system,
        version=version,
        expression_language=expression_language,
        expression=expression,
        value_set_uri=value_set_uri,
        value_set_version=value_set_version,
        codes=canonical_codes,
        expansion_digest=digest,
    )


class TerminologyGroundingReceipt(ContractModel):
    mention_id: str = Field(min_length=1, max_length=128)
    status: ResolutionStatus
    selected: Optional[TerminologyCandidate] = None
    expansion: Optional[TerminologyExpansion] = None
    candidates: list[TerminologyCandidate] = Field(default_factory=list, max_length=20)
    ambiguity_margin_basis_points: Optional[int] = Field(default=None, ge=0, le=10_000)
    requires_confirmation: bool = False
    reason: str = Field(min_length=1, max_length=1_024)

    @model_validator(mode="after")
    def selected_candidate_is_safe(self) -> "TerminologyGroundingReceipt":
        if self.status is ResolutionStatus.RESOLVED:
            if self.selected is None or not self.selected.verified:
                raise ValueError("resolved terminology requires a verified selected candidate")
            if self.requires_confirmation:
                raise ValueError("resolved terminology cannot require confirmation")
        elif self.selected is not None:
            raise ValueError("unresolved terminology cannot publish a selected candidate")
        if self.status is not ResolutionStatus.RESOLVED and self.expansion is not None:
            raise ValueError("unresolved terminology cannot publish an expansion")
        if self.expansion is not None and self.selected is not None:
            if (
                self.expansion.provider_id != self.selected.provider_id
                or self.expansion.system != self.selected.system
                or self.expansion.version != self.selected.version
                or self.selected.code not in self.expansion.codes
            ):
                raise ValueError("terminology expansion does not match its selected candidate")
        return self


class StructuralBinding(ContractModel):
    predicate_id: str = Field(min_length=1, max_length=128)
    semantic_role: str = Field(min_length=1, max_length=128)
    binding_id: str = Field(min_length=1, max_length=512)
    model_kind: str = Field(min_length=1, max_length=128)
    details: dict[str, Any] = Field(default_factory=dict)


class StructuralBindingReceipt(ContractModel):
    status: ResolutionStatus
    resolver_kind: StructuralResolverKind
    resolver_id: str = Field(min_length=1, max_length=256)
    strategy_id: str = Field(min_length=1, max_length=256)
    strategy_version: Optional[str] = Field(default=None, max_length=256)
    catalog_ref: Optional[ArtifactRef] = None
    bindings: list[StructuralBinding] = Field(default_factory=list, max_length=MAX_COHORT_NODES)
    alternatives: list[dict[str, Any]] = Field(default_factory=list, max_length=20)
    reason: str = Field(min_length=1, max_length=1_024)


class ResolutionRoute(ContractModel):
    strategy_id: str = Field(min_length=1, max_length=256)
    structural_resolver_kind: StructuralResolverKind
    structural_resolver_id: str = Field(min_length=1, max_length=256)
    terminology_provider_ids: list[str] = Field(default_factory=list)
    semantic_pack_ref: Optional[ArtifactRef] = None

    @model_validator(mode="after")
    def semantic_pack_is_declared_when_required(self) -> "ResolutionRoute":
        if self.structural_resolver_kind is StructuralResolverKind.COMPILED_SEMANTIC_PACK and self.semantic_pack_ref is None:
            raise ValueError("compiled semantic-pack routes require semanticPackRef")
        return self


class ClinicalCohortResolution(ContractModel):
    contract: Literal["clinical-cohort-resolution/v1"] = COHORT_RESOLUTION_CONTRACT
    decision: ResolutionDecision
    intent: ClinicalCohortIntent
    route: ResolutionRoute
    terminology: list[TerminologyGroundingReceipt] = Field(default_factory=list)
    structural: Optional[StructuralBindingReceipt] = None
    reasons: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def ready_requires_all_bindings(self) -> "ClinicalCohortResolution":
        expected_mentions: list[str] = []

        def visit(node: CohortExpression) -> None:
            if isinstance(node, CohortPredicate):
                if node.concept is not None:
                    expected_mentions.append(node.concept.mention_id)
                return
            for child in node.children:
                visit(child)

        visit(self.intent.criteria)
        receipt_mentions = [item.mention_id for item in self.terminology]
        if sorted(receipt_mentions) != sorted(expected_mentions):
            raise ValueError("terminology receipts must exactly match intent concept mentions")
        routed_providers = set(self.route.terminology_provider_ids)
        if routed_providers and any(
            item.selected is not None
            and item.selected.provider_id not in routed_providers
            for item in self.terminology
        ):
            raise ValueError("terminology receipts must use a provider selected by the route")
        if self.structural is not None:
            if self.structural.strategy_id != self.route.strategy_id:
                raise ValueError("structural receipt strategy must match the resolution route")
            if self.structural.resolver_id != self.route.structural_resolver_id:
                raise ValueError("structural receipt resolver must match the resolution route")
            if self.structural.resolver_kind is not self.route.structural_resolver_kind:
                raise ValueError("structural receipt kind must match the resolution route")
            if self.route.semantic_pack_ref != self.structural.catalog_ref and (
                self.route.structural_resolver_kind
                is StructuralResolverKind.COMPILED_SEMANTIC_PACK
            ):
                raise ValueError("semantic-pack structural receipt must match the routed pack")
        if self.decision is ResolutionDecision.READY:
            if any(item.status is not ResolutionStatus.RESOLVED for item in self.terminology):
                raise ValueError("a ready cohort resolution requires every terminology mention to be resolved")
            if any(item.expansion is None for item in self.terminology):
                raise ValueError("a ready cohort resolution requires every terminology expansion")
            if self.structural is None or self.structural.status is not ResolutionStatus.RESOLVED:
                raise ValueError("a ready cohort resolution requires resolved structural bindings")
        return self


CohortGroup.model_rebuild()
