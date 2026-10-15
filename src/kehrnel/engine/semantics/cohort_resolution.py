"""Governed orchestration from a typed cohort intent to safe bindings.

The orchestrator intentionally knows neither FHIR paths nor openEHR archetypes.
Known strategies register native structural resolvers; custom projects register
resolvers backed by a compiled, approved semantic pack.  Terminology candidate
recall may use search or a model, but only verified candidates can cross the
execution boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from kehrnel.contracts.cohort_intent import (
    ClinicalCohortIntent,
    ClinicalCohortResolution,
    CohortGroup,
    CohortPredicate,
    ConceptMention,
    ResolutionDecision,
    ResolutionRoute,
    ResolutionStatus,
    StructuralBindingReceipt,
    TerminologyCandidate,
    TerminologyCandidateAuthority,
    TerminologyExpansion,
    TerminologyGroundingReceipt,
)


class TerminologyCandidateProvider(Protocol):
    async def candidates(
        self,
        mention: ConceptMention,
        *,
        provider_ids: Sequence[str],
        context: dict[str, Any],
    ) -> Sequence[TerminologyCandidate]: ...

    async def expand(
        self,
        mention: ConceptMention,
        selected: TerminologyCandidate,
        *,
        provider_ids: Sequence[str],
        context: dict[str, Any],
    ) -> TerminologyExpansion: ...


class StructuralResolver(Protocol):
    resolver_id: str

    async def bind(
        self,
        intent: ClinicalCohortIntent,
        terminology: Sequence[TerminologyGroundingReceipt],
        *,
        route: ResolutionRoute,
        context: dict[str, Any],
    ) -> StructuralBindingReceipt: ...


@dataclass(frozen=True)
class ResolutionPolicy:
    minimum_verified_score_basis_points: int = 9_000
    minimum_margin_basis_points: int = 800
    maximum_candidates: int = 10

    def __post_init__(self) -> None:
        if not 0 <= self.minimum_verified_score_basis_points <= 10_000:
            raise ValueError("minimum_verified_score_basis_points must be between zero and 10000")
        if not 0 <= self.minimum_margin_basis_points <= 10_000:
            raise ValueError("minimum_margin_basis_points must be between zero and 10000")
        if not 1 <= self.maximum_candidates <= 20:
            raise ValueError("maximum_candidates must be between one and twenty")


class ResolutionRegistry:
    """Explicit resolver registry; there is no cross-strategy fallback."""

    def __init__(self) -> None:
        self._resolvers: dict[str, StructuralResolver] = {}

    def register(self, resolver: StructuralResolver) -> None:
        resolver_id = str(resolver.resolver_id).strip()
        if not resolver_id:
            raise ValueError("a structural resolver requires a stable resolver_id")
        if resolver_id in self._resolvers:
            raise ValueError(f"structural resolver {resolver_id!r} is already registered")
        self._resolvers[resolver_id] = resolver

    def require(self, route: ResolutionRoute) -> StructuralResolver:
        resolver = self._resolvers.get(route.structural_resolver_id)
        if resolver is None:
            raise LookupError(
                f"structural resolver {route.structural_resolver_id!r} is not registered"
            )
        return resolver


def _concept_mentions(intent: ClinicalCohortIntent) -> list[ConceptMention]:
    mentions: list[ConceptMention] = []

    def visit(node: CohortPredicate | CohortGroup) -> None:
        if isinstance(node, CohortPredicate):
            if node.concept is not None:
                mentions.append(node.concept)
            return
        for child in node.children:
            visit(child)

    visit(intent.criteria)
    return mentions


def _ranked_unique(
    candidates: Sequence[TerminologyCandidate],
    maximum: int,
) -> list[TerminologyCandidate]:
    best: dict[tuple[str, str, str | None], TerminologyCandidate] = {}
    for candidate in candidates:
        key = (candidate.system, candidate.code, candidate.version)
        current = best.get(key)
        if current is None or (candidate.verified, candidate.score_basis_points) > (current.verified, current.score_basis_points):
            best[key] = candidate
    return sorted(
        best.values(),
        key=lambda item: (item.verified, item.score_basis_points, item.authority.value, item.system, item.code),
        reverse=True,
    )[:maximum]


def adjudicate_terminology_candidates(
    mention: ConceptMention,
    candidates: Sequence[TerminologyCandidate],
    policy: ResolutionPolicy,
) -> TerminologyGroundingReceipt:
    ranked = _ranked_unique(candidates, policy.maximum_candidates)
    verified = [item for item in ranked if item.verified]
    if not verified:
        status = ResolutionStatus.CLARIFICATION_REQUIRED if ranked else ResolutionStatus.UNSUPPORTED
        return TerminologyGroundingReceipt(
            mention_id=mention.mention_id,
            status=status,
            candidates=ranked,
            requires_confirmation=bool(ranked),
            reason=(
                "Candidate meanings exist, but none has been verified by an authoritative terminology provider."
                if ranked
                else "No terminology candidate could be found for the mention."
            ),
        )

    top = verified[0]
    runner_up = verified[1] if len(verified) > 1 else None
    margin = top.score_basis_points - runner_up.score_basis_points if runner_up else 10_000
    inferred_only = top.authority in {
        TerminologyCandidateAuthority.INFERRED,
        TerminologyCandidateAuthority.MODEL_PROPOSED,
    }
    if (
        top.score_basis_points < policy.minimum_verified_score_basis_points
        or margin < policy.minimum_margin_basis_points
        or inferred_only
    ):
        return TerminologyGroundingReceipt(
            mention_id=mention.mention_id,
            status=ResolutionStatus.CLARIFICATION_REQUIRED,
            candidates=ranked,
            ambiguity_margin_basis_points=max(0, margin),
            requires_confirmation=True,
            reason="The authoritative candidates are too weak or too close to select without confirmation.",
        )
    return TerminologyGroundingReceipt(
        mention_id=mention.mention_id,
        status=ResolutionStatus.RESOLVED,
        selected=top,
        candidates=ranked,
        ambiguity_margin_basis_points=max(0, margin),
        reason="The top candidate is provider-verified and satisfies the configured score and margin gates.",
    )


class CohortResolutionOrchestrator:
    def __init__(
        self,
        *,
        terminology_provider: TerminologyCandidateProvider,
        structural_resolvers: ResolutionRegistry,
        policy: ResolutionPolicy | None = None,
    ) -> None:
        self.terminology_provider = terminology_provider
        self.structural_resolvers = structural_resolvers
        self.policy = policy or ResolutionPolicy()

    async def resolve(
        self,
        intent: ClinicalCohortIntent,
        route: ResolutionRoute,
        *,
        context: dict[str, Any] | None = None,
    ) -> ClinicalCohortResolution:
        runtime_context = dict(context or {})
        receipts: list[TerminologyGroundingReceipt] = []
        for mention in _concept_mentions(intent):
            candidates = await self.terminology_provider.candidates(
                mention,
                provider_ids=route.terminology_provider_ids,
                context=runtime_context,
            )
            receipt = adjudicate_terminology_candidates(mention, candidates, self.policy)
            if receipt.status is ResolutionStatus.RESOLVED:
                assert receipt.selected is not None
                expansion = await self.terminology_provider.expand(
                    mention,
                    receipt.selected,
                    provider_ids=route.terminology_provider_ids,
                    context=runtime_context,
                )
                receipt = TerminologyGroundingReceipt.model_validate(
                    {**receipt.to_wire(), "expansion": expansion.to_wire()}
                )
            receipts.append(receipt)

        unresolved = [item for item in receipts if item.status is not ResolutionStatus.RESOLVED]
        if unresolved:
            decision = (
                ResolutionDecision.CLARIFICATION_REQUIRED
                if any(item.status is ResolutionStatus.CLARIFICATION_REQUIRED for item in unresolved)
                else ResolutionDecision.UNSUPPORTED
            )
            return ClinicalCohortResolution(
                decision=decision,
                intent=intent,
                route=route,
                terminology=receipts,
                reasons=[item.reason for item in unresolved],
            )

        resolver = self.structural_resolvers.require(route)
        structural = await resolver.bind(
            intent,
            receipts,
            route=route,
            context=runtime_context,
        )
        if structural.status is not ResolutionStatus.RESOLVED:
            decision = (
                ResolutionDecision.CLARIFICATION_REQUIRED
                if structural.status is ResolutionStatus.CLARIFICATION_REQUIRED
                else ResolutionDecision.UNSUPPORTED
            )
            return ClinicalCohortResolution(
                decision=decision,
                intent=intent,
                route=route,
                terminology=receipts,
                structural=structural,
                reasons=[structural.reason],
            )
        return ClinicalCohortResolution(
            decision=ResolutionDecision.READY,
            intent=intent,
            route=route,
            terminology=receipts,
            structural=structural,
            reasons=["Terminology and structural bindings passed deterministic execution gates."],
        )
