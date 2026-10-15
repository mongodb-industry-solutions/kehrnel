from __future__ import annotations

import pytest

from kehrnel.contracts import (
    ClinicalCohortIntent,
    CohortPredicate,
    ConceptMention,
    ResolutionDecision,
    ResolutionRoute,
    ResolutionStatus,
    StructuralBinding,
    StructuralBindingReceipt,
    StructuralResolverKind,
    TerminologyCandidate,
    create_terminology_expansion,
)
from kehrnel.engine.semantics import (
    CohortResolutionOrchestrator,
    ResolutionPolicy,
    ResolutionRegistry,
)


def _intent() -> ClinicalCohortIntent:
    return ClinicalCohortIntent(
        intent_id="heart-failure-cohort",
        criteria=CohortPredicate(
            predicate_id="p-condition",
            semantic_role="condition",
            concept=ConceptMention(
                mention_id="m-heart-failure",
                text="heart failure",
                semantic_category="clinical_condition",
                requested_relationship="descendant_or_self",
                terminology_system_hint="http://snomed.info/sct",
            ),
            temporal=[
                {
                    "semanticRole": "clinical_onset",
                    "operator": "gte",
                    "value": "2025-01-01",
                }
            ],
            correlation_group="condition-occurrence",
        ),
    )


def _candidate(code: str, score: int, *, verified: bool = True, authority: str = "provider_verified"):
    return TerminologyCandidate(
        system="http://snomed.info/sct",
        code=code,
        display="Heart failure",
        provider_id="snomed-production",
        version="20260601",
        score_basis_points=score,
        authority=authority,
        verified=verified,
        match_methods=["provider-search", "lookup-verification"],
    )


class CandidateProvider:
    def __init__(self, candidates):
        self.values = candidates

    async def candidates(self, mention, *, provider_ids, context):
        assert mention.text == "heart failure"
        assert provider_ids == ["snomed-production"]
        return self.values

    async def expand(self, mention, selected, *, provider_ids, context):
        expression = (
            f"<< {selected.code}"
            if mention.requested_relationship.value == "descendant_or_self"
            else selected.code
        )
        return create_terminology_expansion(
            provider_id=selected.provider_id,
            system=selected.system,
            version=selected.version,
            expression_language=("ecl" if expression.startswith("<<") else "code"),
            expression=expression,
            codes=[selected.code],
        )


class NativeResolver:
    resolver_id = "fhir-native-cohort.v1"

    def __init__(self):
        self.calls = 0

    async def bind(self, intent, terminology, *, route, context):
        self.calls += 1
        assert terminology[0].selected.code == "84114007"
        return StructuralBindingReceipt(
            status="resolved",
            resolver_kind="native_strategy",
            resolver_id=self.resolver_id,
            strategy_id=route.strategy_id,
            bindings=[
                StructuralBinding(
                    predicate_id="p-condition",
                    semantic_role="condition",
                    binding_id="fhir-r4:Condition.code",
                    model_kind="fhir_search_parameter",
                    details={"resourceType": "Condition", "searchParameter": "code"},
                )
            ],
            reason="The active FHIR strategy exposes the required governed search parameter.",
        )


def _route() -> ResolutionRoute:
    return ResolutionRoute(
        strategy_id="fhir.resource_store",
        structural_resolver_kind=StructuralResolverKind.NATIVE_STRATEGY,
        structural_resolver_id="fhir-native-cohort.v1",
        terminology_provider_ids=["snomed-production"],
    )


@pytest.mark.asyncio
async def test_verified_terminology_then_native_structural_binding_is_ready():
    provider = CandidateProvider([_candidate("84114007", 9_800), _candidate("195111005", 7_100)])
    resolver = NativeResolver()
    registry = ResolutionRegistry()
    registry.register(resolver)
    result = await CohortResolutionOrchestrator(
        terminology_provider=provider,
        structural_resolvers=registry,
    ).resolve(_intent(), _route())

    assert result.decision is ResolutionDecision.READY
    assert result.terminology[0].selected.code == "84114007"
    assert result.terminology[0].expansion.expression == "<< 84114007"
    assert result.terminology[0].expansion.expansion_digest.startswith("sha256:")
    assert result.structural.bindings[0].binding_id == "fhir-r4:Condition.code"
    assert resolver.calls == 1


@pytest.mark.asyncio
async def test_ambiguity_stops_before_any_strategy_path_is_bound():
    provider = CandidateProvider([_candidate("84114007", 9_500), _candidate("195111005", 9_100)])
    resolver = NativeResolver()
    registry = ResolutionRegistry()
    registry.register(resolver)
    result = await CohortResolutionOrchestrator(
        terminology_provider=provider,
        structural_resolvers=registry,
        policy=ResolutionPolicy(minimum_margin_basis_points=800),
    ).resolve(_intent(), _route())

    assert result.decision is ResolutionDecision.CLARIFICATION_REQUIRED
    assert result.structural is None
    assert result.terminology[0].requires_confirmation is True
    assert resolver.calls == 0


@pytest.mark.asyncio
async def test_model_proposal_cannot_cross_the_execution_boundary():
    provider = CandidateProvider(
        [_candidate("84114007", 9_900, verified=True, authority="model_proposed")]
    )
    resolver = NativeResolver()
    registry = ResolutionRegistry()
    registry.register(resolver)
    result = await CohortResolutionOrchestrator(
        terminology_provider=provider,
        structural_resolvers=registry,
    ).resolve(_intent(), _route())

    assert result.decision is ResolutionDecision.CLARIFICATION_REQUIRED
    assert result.terminology[0].status is ResolutionStatus.CLARIFICATION_REQUIRED
    assert resolver.calls == 0


@pytest.mark.asyncio
async def test_no_terminology_candidate_is_unsupported_and_fail_closed():
    resolver = NativeResolver()
    registry = ResolutionRegistry()
    registry.register(resolver)
    result = await CohortResolutionOrchestrator(
        terminology_provider=CandidateProvider([]),
        structural_resolvers=registry,
    ).resolve(_intent(), _route())

    assert result.decision is ResolutionDecision.UNSUPPORTED
    assert result.structural is None
    assert resolver.calls == 0
