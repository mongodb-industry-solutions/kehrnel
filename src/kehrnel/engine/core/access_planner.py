"""Deterministic consumption-aware planning for MongoDB access structures."""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict

from kehrnel.contracts.access_planning import (
    AccessConsistency,
    AccessMechanism,
    AccessPlan,
    AccessStructurePlan,
    ArchitectureProposal,
    ProjectionBudget,
    RejectedAccessAlternative,
)
from kehrnel.contracts.artifacts import CapabilityMaturity, ObservationKind
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.contracts.source_architecture import (
    CandidateStatus,
    CompilationIssue,
    ConsistencyRequirement,
    ConsumptionKind,
    ConsumptionRequirement,
    IssueSeverity,
    QueryFeature,
    StrategyCandidate,
    StrategyCompilationRequest,
    WorkloadScope,
)
from kehrnel.engine.core.strategy_candidate_compiler import compile_strategy_candidate


_COMPILED_MECHANISMS: dict[str, frozenset[AccessMechanism]] = {
    "openehr.rps_dual": frozenset(
        {
            AccessMechanism.DIRECT_DOCUMENT,
            AccessMechanism.TARGETED_INDEX,
            AccessMechanism.ATLAS_SEARCH_INDEX,
            AccessMechanism.MATERIALIZED_PROJECTION,
        }
    ),
    "openehr.rps_dual_ibm": frozenset(
        {
            AccessMechanism.DIRECT_DOCUMENT,
            AccessMechanism.TARGETED_INDEX,
            AccessMechanism.ATLAS_SEARCH_INDEX,
            AccessMechanism.MATERIALIZED_PROJECTION,
        }
    ),
    "fhir.resource_store": frozenset(
        {
            AccessMechanism.DIRECT_DOCUMENT,
            AccessMechanism.TARGETED_INDEX,
            AccessMechanism.COMPUTED_FIELD,
            AccessMechanism.ATLAS_VECTOR_INDEX,
        }
    ),
    "cdisc.sdr": frozenset(
        {
            AccessMechanism.DIRECT_DOCUMENT,
            AccessMechanism.TARGETED_INDEX,
            AccessMechanism.ATLAS_SEARCH_INDEX,
            AccessMechanism.ATLAS_VECTOR_INDEX,
            AccessMechanism.MATERIALIZED_PROJECTION,
            AccessMechanism.ISOLATED_EXPORT,
        }
    ),
}

_INDEX_LIKE = {
    AccessMechanism.TARGETED_INDEX,
    AccessMechanism.ATLAS_SEARCH_INDEX,
    AccessMechanism.ATLAS_VECTOR_INDEX,
}

_DERIVED_COPIES = {
    AccessMechanism.MATERIALIZED_PROJECTION,
    AccessMechanism.ISOLATED_EXPORT,
}


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _digest(payload: object) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _operation_tokens(consumption: ConsumptionRequirement) -> set[str]:
    return {
        token
        for operation in consumption.operations
        for token in re.split(r"[^a-z0-9]+", operation.lower())
        if token
    }


def _requires_population_projection(consumption: ConsumptionRequirement) -> bool:
    workload = consumption.workload
    tokens = _operation_tokens(consumption)
    broad_scope = workload.scope in {WorkloadScope.POPULATION, WorkloadScope.CORPUS}
    explicitly_cross_record = bool(
        tokens & {"cohort", "population", "cross", "patients", "subjects"}
    )
    demanding_latency = (
        consumption.latency_target_ms is not None
        and consumption.latency_target_ms <= 1_000
    )
    broad_result = (
        workload.expected_result_documents is not None
        and workload.expected_result_documents >= 1_000
    )
    return (broad_scope or explicitly_cross_record) and (
        demanding_latency
        or broad_result
        or QueryFeature.AGGREGATION in workload.query_features
    )


def _required_mechanisms(
    consumption: ConsumptionRequirement,
) -> list[AccessMechanism]:
    """Choose the lowest useful rung of the projection-economy ladder."""

    mechanisms: list[AccessMechanism] = [AccessMechanism.DIRECT_DOCUMENT]
    features = set(consumption.workload.query_features)
    tokens = _operation_tokens(consumption)

    if consumption.kind is ConsumptionKind.FHIR:
        if "search" in tokens or QueryFeature.FILTER in features:
            mechanisms.extend(
                [AccessMechanism.COMPUTED_FIELD, AccessMechanism.TARGETED_INDEX]
            )
    elif consumption.kind in {
        ConsumptionKind.OPENEHR,
        ConsumptionKind.CDISC,
        ConsumptionKind.MQL,
        ConsumptionKind.APPLICATION_API,
        ConsumptionKind.COHORT,
        ConsumptionKind.AGENT_TOOLS,
    }:
        if (
            consumption.kind
            in {
                ConsumptionKind.OPENEHR,
                ConsumptionKind.CDISC,
                ConsumptionKind.COHORT,
                ConsumptionKind.AGENT_TOOLS,
            }
            or features
            or "query" in tokens
            or "search" in tokens
        ):
            mechanisms.append(AccessMechanism.TARGETED_INDEX)

    if consumption.kind is ConsumptionKind.SEARCH or QueryFeature.FULL_TEXT in features:
        mechanisms.append(AccessMechanism.ATLAS_SEARCH_INDEX)
    if (
        consumption.kind is ConsumptionKind.VECTOR_SEARCH
        or QueryFeature.SEMANTIC in features
    ):
        mechanisms.append(AccessMechanism.ATLAS_VECTOR_INDEX)
    if consumption.kind is ConsumptionKind.GRAPH or QueryFeature.GRAPH_TRAVERSAL in features:
        mechanisms.append(AccessMechanism.TARGETED_INDEX)
    if _requires_population_projection(consumption):
        mechanisms.append(AccessMechanism.MATERIALIZED_PROJECTION)
    if consumption.kind is ConsumptionKind.ANALYTICS_EXPORT or QueryFeature.EXPORT in features:
        mechanisms.append(AccessMechanism.ISOLATED_EXPORT)
    if consumption.kind is ConsumptionKind.EVENT_STREAM:
        mechanisms.append(AccessMechanism.CHANGE_STREAM)

    return list(dict.fromkeys(mechanisms))


def _consistency(mechanism: AccessMechanism) -> AccessConsistency:
    if mechanism is AccessMechanism.DIRECT_DOCUMENT:
        return AccessConsistency.AUTHORITATIVE
    if mechanism in {AccessMechanism.TARGETED_INDEX, AccessMechanism.COMPUTED_FIELD}:
        return AccessConsistency.SYNCHRONOUS_DERIVED
    if mechanism is AccessMechanism.ISOLATED_EXPORT:
        return AccessConsistency.SNAPSHOT
    return AccessConsistency.ASYNCHRONOUS_DERIVED


def _rationale(mechanism: AccessMechanism) -> str:
    return {
        AccessMechanism.DIRECT_DOCUMENT: (
            "Preserve the authoritative industry record and serve access directly when its document boundary is sufficient."
        ),
        AccessMechanism.TARGETED_INDEX: (
            "Support declared filters, joins, graph keys, or bounded queries without creating another data copy."
        ),
        AccessMechanism.COMPUTED_FIELD: (
            "Maintain a small co-located, rebuildable field required by the canonical query contract."
        ),
        AccessMechanism.ATLAS_SEARCH_INDEX: (
            "Provide the explicitly requested lexical or full-text retrieval path over governed fields."
        ),
        AccessMechanism.ATLAS_VECTOR_INDEX: (
            "Provide the explicitly requested semantic retrieval path with versioned embeddings and index configuration."
        ),
        AccessMechanism.MATERIALIZED_PROJECTION: (
            "Meet a declared broad-scope aggregation or cohort workload that direct access and indexes alone cannot safely promise."
        ),
        AccessMechanism.CHANGE_STREAM: (
            "Publish declared operational changes without introducing a separate analytical copy."
        ),
        AccessMechanism.ISOLATED_EXPORT: (
            "Create a bounded snapshot for an explicitly requested external analytical consumer."
        ),
    }[mechanism]


def _alternative_for(
    consumption: ConsumptionRequirement,
    mechanisms: set[AccessMechanism],
) -> RejectedAccessAlternative:
    if AccessMechanism.MATERIALIZED_PROJECTION in mechanisms:
        mechanism = AccessMechanism.DIRECT_DOCUMENT
        reason_code = "DIRECT_ONLY_INSUFFICIENT_FOR_WORKLOAD"
        explanation = (
            "Direct access remains authoritative, but it is not sufficient by itself for the declared broad-scope workload and SLO."
        )
    elif AccessMechanism.ATLAS_VECTOR_INDEX in mechanisms:
        mechanism = AccessMechanism.ATLAS_SEARCH_INDEX
        reason_code = "LEXICAL_SEARCH_DOES_NOT_SATISFY_SEMANTIC_RETRIEVAL"
        explanation = (
            "Lexical search remains available as a separate use, but it does not replace the requested semantic retrieval behavior."
        )
    elif AccessMechanism.ATLAS_SEARCH_INDEX in mechanisms:
        mechanism = AccessMechanism.ATLAS_VECTOR_INDEX
        reason_code = "VECTOR_INDEX_NOT_JUSTIFIED"
        explanation = (
            "The declared workload asks for lexical retrieval and provides no semantic-search requirement that would justify embeddings."
        )
    else:
        mechanism = AccessMechanism.MATERIALIZED_PROJECTION
        reason_code = "DERIVED_COPY_NOT_JUSTIFIED"
        explanation = (
            "The declared workload can use the authoritative document and lower-cost access structures; another data copy is not justified."
        )
    return RejectedAccessAlternative(
        alternativeId=f"alternative.{_slug(consumption.consumption_id)}.{mechanism.value.replace('_', '-')}",
        mechanism=mechanism,
        consumptionId=consumption.consumption_id,
        reasonCode=reason_code,
        explanation=explanation,
    )


def compile_access_plan(
    request: StrategyCompilationRequest,
    candidate: StrategyCandidate | None = None,
) -> AccessPlan:
    """Build a non-executable, explainable access plan for a Strategy Candidate."""

    candidate = candidate or compile_strategy_candidate(request)
    if (
        candidate.blueprint_id != request.blueprint.blueprint_id
        or candidate.blueprint_revision != request.blueprint.revision
    ):
        raise ValueError("Strategy Candidate does not belong to the supplied blueprint")

    interfaces = {
        interface.consumption_id: interface for interface in candidate.interface_plans
    }
    source_ids_by_strategy: dict[str, set[str]] = defaultdict(set)
    for source_plan in candidate.source_plans:
        source_ids_by_strategy[source_plan.strategy_id].add(
            source_plan.source_instance_id
        )

    structure_consumptions: dict[tuple[str, AccessMechanism], set[str]] = defaultdict(set)
    structure_freshness: dict[tuple[str, AccessMechanism], list[int]] = defaultdict(list)
    alternatives: list[RejectedAccessAlternative] = []
    issues: list[CompilationIssue] = []

    for consumption in request.blueprint.consumptions:
        interface = interfaces[consumption.consumption_id]
        mechanisms = set(_required_mechanisms(consumption))
        if AccessMechanism.MATERIALIZED_PROJECTION in mechanisms:
            if consumption.workload.consistency is ConsistencyRequirement.STRONG:
                issues.append(
                    CompilationIssue(
                        code="STRONG_CONSISTENCY_PROJECTION_CONFLICT",
                        severity=IssueSeverity.BLOCKING,
                        message=(
                            f"Consumption '{consumption.consumption_id}' requires strong consistency, "
                            "which the proposed asynchronous materialized projection cannot guarantee."
                        ),
                        refs=[consumption.consumption_id],
                    )
                )
            else:
                issues.append(
                    CompilationIssue(
                        code="ACCESS_BENCHMARK_REQUIRED",
                        severity=IssueSeverity.REVIEW,
                        message=(
                            f"The materialized projection for '{consumption.consumption_id}' "
                            "is justified by static workload rules and requires benchmark evidence before release."
                        ),
                        refs=[consumption.consumption_id, interface.strategy_id],
                    )
                )
        for mechanism in mechanisms:
            key = (interface.strategy_id, mechanism)
            structure_consumptions[key].add(consumption.consumption_id)
            if consumption.freshness_target_seconds is not None:
                structure_freshness[key].append(consumption.freshness_target_seconds)
        alternatives.append(_alternative_for(consumption, mechanisms))

    structures: list[AccessStructurePlan] = []
    for strategy_id, mechanism in sorted(
        structure_consumptions,
        key=lambda item: (item[0], item[1].value),
    ):
        supported = mechanism in _COMPILED_MECHANISMS.get(strategy_id, frozenset())
        maturity = (
            CapabilityMaturity.COMPILABLE
            if supported
            else CapabilityMaturity.SPECIFIED
        )
        if not supported and mechanism is not AccessMechanism.DIRECT_DOCUMENT:
            issues.append(
                CompilationIssue(
                    code="ACCESS_IMPLEMENTATION_REQUIRED",
                    severity=IssueSeverity.REVIEW,
                    message=(
                        f"Strategy '{strategy_id}' does not yet publish executable evidence for "
                        f"the '{mechanism.value}' access mechanism."
                    ),
                    refs=[strategy_id, mechanism.value],
                )
            )

        derived_copy = mechanism in _DERIVED_COPIES
        freshness_values = structure_freshness[(strategy_id, mechanism)]
        structures.append(
            AccessStructurePlan(
                accessStructureId=(
                    f"access.{_slug(strategy_id)}.{mechanism.value.replace('_', '-')}"
                ),
                mechanism=mechanism,
                strategyId=strategy_id,
                servesConsumptionIds=sorted(structure_consumptions[(strategy_id, mechanism)]),
                sourceInstanceIds=sorted(source_ids_by_strategy[strategy_id]),
                rationale=_rationale(mechanism),
                consistency=_consistency(mechanism),
                requiresDerivedCopy=derived_copy,
                rebuildSource=(
                    None
                    if mechanism is AccessMechanism.DIRECT_DOCUMENT
                    else f"authoritative:{strategy_id}"
                ),
                retirementCondition=(
                    None
                    if mechanism is AccessMechanism.DIRECT_DOCUMENT
                    else "Retire when no approved consumption profile references this access structure."
                ),
                freshnessTargetSeconds=(
                    min(freshness_values) if freshness_values else None
                ),
                capabilityMaturity=maturity,
                observationKind=ObservationKind.ESTIMATED,
            )
        )

    if candidate.status is CandidateStatus.BLOCKED or any(
        issue.severity is IssueSeverity.BLOCKING for issue in issues
    ):
        status = CandidateStatus.BLOCKED
    elif candidate.requires_approval or issues:
        status = CandidateStatus.REQUIRES_APPROVAL
    else:
        status = CandidateStatus.READY

    budget = ProjectionBudget(
        observationKind=ObservationKind.ESTIMATED,
        accessStructureCount=len(structures),
        indexLikeStructureCount=sum(
            structure.mechanism in _INDEX_LIKE for structure in structures
        ),
        computedFieldCount=sum(
            structure.mechanism is AccessMechanism.COMPUTED_FIELD
            for structure in structures
        ),
        derivedCopyCount=sum(
            structure.mechanism in _DERIVED_COPIES for structure in structures
        ),
        externalExportCount=sum(
            structure.mechanism is AccessMechanism.ISOLATED_EXPORT
            for structure in structures
        ),
        servingConsumptionCount=len(request.blueprint.consumptions),
    )
    unsigned = {
        "contract": "access-plan/v1",
        "blueprintId": request.blueprint.blueprint_id,
        "blueprintRevision": request.blueprint.revision,
        "candidateDigest": candidate.candidate_digest,
        "status": status.value,
        "requiresApproval": status is not CandidateStatus.READY,
        "structures": [structure.to_wire() for structure in structures],
        "rejectedAlternatives": [alternative.to_wire() for alternative in alternatives],
        "projectionBudget": budget.to_wire(),
        "issues": [issue.to_wire() for issue in issues],
    }
    return AccessPlan(**unsigned, accessPlanDigest=_digest(unsigned))


def compile_architecture_proposal(
    request: StrategyCompilationRequest,
) -> ArchitectureProposal:
    """Compile strategy selection and access planning into one Builder proposal."""

    candidate = compile_strategy_candidate(request)
    access_plan = compile_access_plan(request, candidate)
    unsigned = {
        "contract": "architecture-proposal/v1",
        "proposalOrigin": "deterministic_core",
        "blueprintId": request.blueprint.blueprint_id,
        "blueprintRevision": request.blueprint.revision,
        "requiresApproval": candidate.requires_approval or access_plan.requires_approval,
        "strategyCandidate": candidate.to_wire(),
        "accessPlan": access_plan.to_wire(),
    }
    return ArchitectureProposal(**unsigned, proposalDigest=_digest(unsigned))
