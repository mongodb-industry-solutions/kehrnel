"""Resolve semantic authority without forcing every source through ContextObjects."""

from __future__ import annotations

import re

from kehrnel.contracts.semantic_execution import (
    ConsumptionSemanticPlan,
    ExecutionBindingKind,
    ExecutionSubplan,
    SemanticAuthorityKind,
    SemanticAuthorityPlan,
    SemanticAuthorityRole,
    SemanticExecutionPlan,
    SemanticExecutionPlanningRequest,
    SemanticMode,
    semantic_execution_plan_content_digest,
)
from kehrnel.contracts.source_architecture import (
    CandidateStatus,
    CompilationIssue,
    ConsumptionKind,
    IssueSeverity,
    MappingMode,
)
from kehrnel.engine.core.strategy_candidate_compiler import (
    compile_strategy_candidate,
)
from kehrnel.engine.core.source_profiles import (
    load_source_profile_catalog,
    source_profile_index,
)
from kehrnel.engine.core.context_object_definitions import (
    context_object_definition_index,
    load_context_object_definition_catalog,
)


_NATIVE_TARGETS = {
    "openehr": SemanticAuthorityKind.OPENEHR,
    "fhir": SemanticAuthorityKind.FHIR,
    "cdisc": SemanticAuthorityKind.CDISC,
}

_STANDARD_CONSUMPTION_TARGETS = {
    ConsumptionKind.OPENEHR: "openehr",
    ConsumptionKind.FHIR: "fhir",
    ConsumptionKind.CDISC: "cdisc",
}

_BINDINGS = {
    ConsumptionKind.OPENEHR: ExecutionBindingKind.OPENEHR_AQL,
    ConsumptionKind.FHIR: ExecutionBindingKind.FHIR_SEARCH,
    ConsumptionKind.CDISC: ExecutionBindingKind.CDISC_QUERY,
    ConsumptionKind.MQL: ExecutionBindingKind.MONGODB_MQL,
    ConsumptionKind.EVENT_STREAM: ExecutionBindingKind.STREAM,
    ConsumptionKind.ANALYTICS_EXPORT: ExecutionBindingKind.EXPORT,
}


def _identifier(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9._-]+", "-", value.lower()).strip("-")
    return normalized or "unnamed"


def _semantic_sources(candidate, consumption_kind: ConsumptionKind, strategy_id: str):
    target = _STANDARD_CONSUMPTION_TARGETS.get(consumption_kind)
    if target is not None:
        return [plan for plan in candidate.source_plans if plan.target_model == target]

    # Purpose-built contracts are assumed to consume the Blueprint as a whole.
    # Keep every distinct source/target authority so a cross-standard contract
    # cannot accidentally collapse onto whichever strategy happened to be first.
    unique = {}
    for plan in candidate.source_plans:
        key = (plan.source_instance_id, plan.target_model)
        unique.setdefault(key, plan)
    if unique:
        return list(unique.values())
    return [plan for plan in candidate.source_plans if plan.strategy_id == strategy_id]


def _authorities_for(plan, profile) -> list[SemanticAuthorityPlan]:
    targets = []
    if plan.target_model in _NATIVE_TARGETS:
        targets.append(plan.target_model)
    else:
        targets.extend(
            route.target_model
            for route in profile.mapping_routes
            if route.target_model in _NATIVE_TARGETS
            and route.mode is MappingMode.DIRECT
            and route.readiness.value == "available"
        )
    if not targets:
        targets.append("source_schema")

    authorities = []
    for target in sorted(set(targets)):
        kind = _NATIVE_TARGETS.get(
            target, SemanticAuthorityKind.SOURCE_SCHEMA
        )
        role = (
            SemanticAuthorityRole.AUTHORITATIVE
            if target != plan.target_model or plan.mapping_mode is MappingMode.DIRECT
            else SemanticAuthorityRole.TARGET_SEMANTICS
        )
        authorities.append(
            SemanticAuthorityPlan(
                authority_id=(
                    f"semantic:{kind.value}:{_identifier(plan.source_instance_id)}"
                ),
                kind=kind,
                role=role,
                source_instance_ids=[plan.source_instance_id],
                source_profile_refs=[plan.source_profile_ref],
            )
        )
    return authorities


def _inferred_mode(consumption_kind: ConsumptionKind, authorities) -> SemanticMode:
    if consumption_kind in _STANDARD_CONSUMPTION_TARGETS:
        return SemanticMode.NATIVE
    kinds = {authority.kind for authority in authorities}
    if len(kinds) == 1 and not kinds.intersection(
        {SemanticAuthorityKind.SOURCE_SCHEMA, SemanticAuthorityKind.CONTEXT_OBJECT}
    ):
        return SemanticMode.NATIVE
    return SemanticMode.COMPOSED


def compile_semantic_execution_plan(
    request: SemanticExecutionPlanningRequest,
) -> SemanticExecutionPlan:
    """Compile semantic modes and native execution bindings for a Blueprint.

    This is a deterministic design-time plan. It carries native AQL and FHIR
    Search bindings as native subplans and introduces a ContextObject only when
    a requirement explicitly asks for one or cross-authority composition needs
    one.
    """

    candidate = compile_strategy_candidate(request.strategy_request)
    profiles = source_profile_index(load_source_profile_catalog())
    profiles.update(
        {
            (
                profile.artifact_ref.id,
                profile.artifact_ref.version,
                profile.artifact_ref.digest,
            ): profile
            for profile in request.strategy_request.discovered_profiles
        }
    )
    requirements = {
        item.consumption_id: item for item in request.semantic_requirements
    }
    context_object_definitions = context_object_definition_index(
        load_context_object_definition_catalog()
    )
    consumptions = {
        item.consumption_id: item
        for item in request.strategy_request.blueprint.consumptions
    }

    authority_index: dict[str, SemanticAuthorityPlan] = {}
    consumption_plans: list[ConsumptionSemanticPlan] = []
    subplans: list[ExecutionSubplan] = []
    issues = list(candidate.issues)

    for interface in candidate.interface_plans:
        consumption = consumptions[interface.consumption_id]
        source_plans = _semantic_sources(
            candidate, consumption.kind, interface.strategy_id
        )
        native_authorities = []
        for plan in source_plans:
            profile = profiles[
                (
                    plan.source_profile_ref.id,
                    plan.source_profile_ref.version,
                    plan.source_profile_ref.digest,
                )
            ]
            native_authorities.extend(_authorities_for(plan, profile))
        native_authorities = list(
            {item.authority_id: item for item in native_authorities}.values()
        )
        for authority in native_authorities:
            authority_index.setdefault(authority.authority_id, authority)

        requested = requirements.get(consumption.consumption_id)
        mode = requested.mode if requested else _inferred_mode(
            consumption.kind, native_authorities
        )
        context_object_ref = requested.context_object_ref if requested else None
        native_kinds = {authority.kind for authority in native_authorities}
        native_available = bool(native_kinds) and not native_kinds.intersection(
            {SemanticAuthorityKind.SOURCE_SCHEMA, SemanticAuthorityKind.CONTEXT_OBJECT}
        )
        if consumption.kind not in _STANDARD_CONSUMPTION_TARGETS:
            native_available = native_available and len(native_kinds) == 1

        resolved = True
        rationale: str
        if mode is SemanticMode.NATIVE:
            resolved = native_available
            rationale = (
                "Preserve the selected strategy's native semantic model and "
                "compile the consumption contract directly to its native binding."
            )
            if not resolved:
                issues.append(
                    CompilationIssue(
                        code="NATIVE_SEMANTICS_UNAVAILABLE",
                        severity=IssueSeverity.BLOCKING,
                        message=(
                            f"Consumption '{consumption.consumption_id}' cannot use "
                            "native mode because its sources do not resolve to one "
                            "established semantic authority."
                        ),
                        refs=[consumption.consumption_id],
                    )
                )
        else:
            definition = None
            if context_object_ref is not None:
                definition = context_object_definitions.get(
                    (
                        context_object_ref.id,
                        context_object_ref.version,
                        context_object_ref.digest,
                    )
                )
            resolved = definition is not None
            rationale = (
                "Expose a generated ContextObject view over one native authority."
                if mode is SemanticMode.DERIVED
                else "Compose the required source authorities into a purpose-built ContextObject."
            )
            if context_object_ref is None:
                issues.append(
                    CompilationIssue(
                        code="CONTEXT_OBJECT_REQUIRED",
                        severity=IssueSeverity.BLOCKING,
                        message=(
                            f"Consumption '{consumption.consumption_id}' requires a "
                            f"{mode.value} ContextObject semantic contract."
                        ),
                        refs=[consumption.consumption_id],
                    )
                )
            elif definition is None:
                issues.append(
                    CompilationIssue(
                        code="CONTEXT_OBJECT_DEFINITION_NOT_FOUND",
                        severity=IssueSeverity.BLOCKING,
                        message=(
                            f"Consumption '{consumption.consumption_id}' references "
                            "a ContextObject definition that is not present in Core's "
                            "versioned catalog."
                        ),
                        refs=[
                            consumption.consumption_id,
                            f"{context_object_ref.id}@{context_object_ref.version}",
                        ],
                    )
                )
            else:
                supported = set(definition.supported_authority_kinds)
                unsupported = sorted(
                    (kind.value for kind in native_kinds - supported)
                )
                if unsupported:
                    resolved = False
                    issues.append(
                        CompilationIssue(
                            code="CONTEXT_OBJECT_AUTHORITY_MISMATCH",
                            severity=IssueSeverity.BLOCKING,
                            message=(
                                f"ContextObject definition '{definition.name}' does not "
                                "support semantic authorities: " + ", ".join(unsupported)
                            ),
                            refs=[
                                consumption.consumption_id,
                                f"{context_object_ref.id}@{context_object_ref.version}",
                            ],
                        )
                    )

        authority_ids = [item.authority_id for item in native_authorities]
        if context_object_ref is not None:
            context_authority = SemanticAuthorityPlan(
                authority_id=f"semantic:context-object:{_identifier(consumption.consumption_id)}",
                kind=SemanticAuthorityKind.CONTEXT_OBJECT,
                role=SemanticAuthorityRole.COMPOSITION,
                source_instance_ids=sorted(
                    {source_id for item in native_authorities for source_id in item.source_instance_ids}
                ),
                definition_refs=[context_object_ref],
            )
            authority_index.setdefault(context_authority.authority_id, context_authority)
            authority_ids.append(context_authority.authority_id)

        consumption_plans.append(
            ConsumptionSemanticPlan(
                consumption_id=consumption.consumption_id,
                mode=mode,
                authority_ids=authority_ids,
                context_object_ref=context_object_ref,
                resolved=resolved,
                rationale=rationale,
            )
        )
        subplans.append(
            ExecutionSubplan(
                subplan_id=f"execute:{_identifier(consumption.consumption_id)}",
                consumption_id=consumption.consumption_id,
                binding_kind=_BINDINGS.get(
                    consumption.kind, ExecutionBindingKind.SERVICE
                ),
                strategy_id=interface.strategy_id,
                semantic_mode=mode,
                authority_ids=authority_ids,
                preserves_native_semantics=(mode is SemanticMode.NATIVE),
            )
        )

    blocking = any(issue.severity is IssueSeverity.BLOCKING for issue in issues)
    review = any(issue.severity is IssueSeverity.REVIEW for issue in issues)
    if blocking:
        status = CandidateStatus.BLOCKED
    elif review or candidate.status is CandidateStatus.REQUIRES_APPROVAL:
        status = CandidateStatus.REQUIRES_APPROVAL
    else:
        status = CandidateStatus.READY

    unsigned = {
        "contract": "semantic-execution-plan/v1",
        "blueprintId": candidate.blueprint_id,
        "blueprintRevision": candidate.blueprint_revision,
        "strategyCandidateDigest": candidate.candidate_digest,
        "status": status.value,
        "requiresApproval": status is not CandidateStatus.READY,
        "authorities": [item.to_wire() for item in authority_index.values()],
        "consumptionPlans": [item.to_wire() for item in consumption_plans],
        "subplans": [item.to_wire() for item in subplans],
        "issues": [item.to_wire() for item in issues],
    }
    return SemanticExecutionPlan(
        **unsigned,
        planDigest=semantic_execution_plan_content_digest(unsigned),
    )
