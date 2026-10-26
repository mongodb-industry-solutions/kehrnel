"""Deterministic preview compiler for source-aware strategy candidates.

This compiler selects existing supported physical strategies where possible
and emits reviewable mapping candidates for unfamiliar sources.  It never
generates executable code or activates a runtime.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from typing import Iterable

from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.contracts.source_architecture import (
    CandidateStatus,
    CompilationIssue,
    ConsumptionKind,
    InterfacePlan,
    IssueSeverity,
    MappingMode,
    MappingReadiness,
    SemanticBindingState,
    SourceCompilationPlan,
    SourceInstanceSpec,
    SourceProfile,
    SourceProfileOrigin,
    StrategyCandidate,
    StrategyCompilationRequest,
    StrategySelection,
)
from kehrnel.engine.core.source_profiles import (
    load_source_profile_catalog,
    source_profile_content_digest,
    source_profile_index,
)
from kehrnel.engine.core.mappings import load_mapping_catalog
from kehrnel.engine.core.source_categories import (
    assert_source_catalog_links,
    load_source_category_catalog,
)
from kehrnel.engine.core.strategy_portfolio import load_strategy_portfolio


_STANDARD_TARGETS = {
    ConsumptionKind.OPENEHR: "openehr",
    ConsumptionKind.FHIR: "fhir",
    ConsumptionKind.CDISC: "cdisc",
}

_TARGET_STRATEGIES = {
    "openehr": ("openehr.rps_dual", "openehr.rps_dual_ibm"),
    "fhir": ("fhir.resource_store",),
    "cdisc": ("cdisc.sdr",),
}


class StrategyCompilationError(ValueError):
    """The blueprint cannot be compiled because its inputs are inconsistent."""


def _generated_strategy_id(blueprint_id: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", blueprint_id.lower()).strip("-")
    return f"generated.{normalized or 'customer'}.operational"


def _select_strategy(
    target_model: str,
    preferred: Iterable[str],
) -> tuple[str, bool]:
    if target_model == "custom_operational":
        return "", True
    supported = _TARGET_STRATEGIES[target_model]
    preferred_set = list(preferred)
    chosen = next((item for item in preferred_set if item in supported), supported[0])
    return chosen, False


def _resolve_profiles(request: StrategyCompilationRequest) -> dict[tuple[str, str, str], SourceProfile]:
    profile_catalog = load_source_profile_catalog()
    category_catalog = load_source_category_catalog()
    assert_source_catalog_links(category_catalog, profile_catalog)
    known_category_ids = {
        category.artifact_ref.id for category in category_catalog.categories
    }
    known = source_profile_index(profile_catalog)
    known_versions = {(key[0], key[1]) for key in known}
    for profile in request.discovered_profiles:
        missing_categories = sorted(set(profile.category_ids) - known_category_ids)
        if missing_categories:
            raise StrategyCompilationError(
                f"discovered source profile references unknown categories: {missing_categories}"
            )
        expected_digest = source_profile_content_digest(profile)
        if profile.artifact_ref.digest != expected_digest:
            raise StrategyCompilationError(
                "discovered source profile digest mismatch for "
                f"{profile.artifact_ref.id}@{profile.artifact_ref.version}"
            )
        key = (
            profile.artifact_ref.id,
            profile.artifact_ref.version,
            profile.artifact_ref.digest,
        )
        if (key[0], key[1]) in known_versions:
            raise StrategyCompilationError(
                "discovered source profile cannot shadow catalog profile "
                f"{profile.artifact_ref.id}@{profile.artifact_ref.version}"
            )
        known[key] = profile
    return known


def _route_for(profile: SourceProfile, target_model: str, strategy_id: str):
    for route in profile.mapping_routes:
        if route.target_model != target_model:
            continue
        if route.strategy_ids and strategy_id not in route.strategy_ids:
            continue
        return route
    return None


def _profile_has_person_identity(profile: SourceProfile) -> bool:
    return any(
        binding.concept_id.startswith("healthcare.person.")
        for binding in profile.semantic_bindings
    )


def _simulation_mapping_refs(
    source: SourceInstanceSpec,
    profile: SourceProfile,
    target_model: str,
    strategy_id: str,
):
    if not source.interaction_ids:
        return []
    requested = set(source.interaction_ids)
    return [
        mapping.artifact_ref
        for mapping in load_mapping_catalog().mappings
        if mapping.source.profile_ref == profile.artifact_ref
        and requested.issubset(set(mapping.source.interaction_ids))
        and mapping.target.model == target_model
        and strategy_id in mapping.target.strategy_ids
    ]


def _infer_target_from_sources(
    resolved_sources: list[tuple[SourceInstanceSpec, SourceProfile]],
    preferred_strategy_ids: Iterable[str],
) -> str | None:
    """Reuse an unambiguous source route when the Use is protocol-neutral."""

    preferred = list(preferred_strategy_ids)
    for strategy_id in preferred:
        target = next(
            (
                target_model
                for target_model, strategy_ids in _TARGET_STRATEGIES.items()
                if strategy_id in strategy_ids
            ),
            None,
        )
        if target and all(
            _route_for(profile, target, strategy_id) is not None
            for _, profile in resolved_sources
        ):
            return target

    available_targets: set[str] | None = None
    for _, profile in resolved_sources:
        profile_targets = {
            route.target_model
            for route in profile.mapping_routes
            if route.readiness is MappingReadiness.AVAILABLE
            and route.target_model in _TARGET_STRATEGIES
        }
        available_targets = (
            profile_targets
            if available_targets is None
            else available_targets & profile_targets
        )
    if available_targets and len(available_targets) == 1:
        return next(iter(available_targets))
    return None


def compile_strategy_candidate(request: StrategyCompilationRequest) -> StrategyCandidate:
    """Compile a blueprint into an immutable, non-executable strategy candidate."""

    profiles = _resolve_profiles(request)
    portfolio = load_strategy_portfolio()
    release_blocking = set(portfolio.release_blocking_ids)
    issues: list[CompilationIssue] = []
    resolved_sources: list[tuple[SourceInstanceSpec, SourceProfile]] = []

    for source in request.blueprint.sources:
        key = (source.profile_ref.id, source.profile_ref.version, source.profile_ref.digest)
        profile = profiles.get(key)
        if profile is None:
            raise StrategyCompilationError(
                "source profile reference is not available: "
                f"{source.profile_ref.id}@{source.profile_ref.version}"
            )
        if source.ingestion_mode not in profile.ingestion_modes:
            raise StrategyCompilationError(
                f"source '{source.instance_id}' does not support ingestion mode "
                f"'{source.ingestion_mode.value}'"
            )
        if source.interaction_ids is not None:
            known_interactions = {
                interaction.interaction_id for interaction in profile.interactions
            }
            missing_interactions = sorted(
                set(source.interaction_ids) - known_interactions
            )
            if missing_interactions:
                raise StrategyCompilationError(
                    f"source '{source.instance_id}' requests unknown interactions: "
                    f"{missing_interactions}"
                )
        resolved_sources.append((source, profile))

    target_models: list[str] = []
    for consumption in request.blueprint.consumptions:
        target = _STANDARD_TARGETS.get(consumption.kind)
        if target and target not in target_models:
            target_models.append(target)
    if not target_models:
        inferred_target = _infer_target_from_sources(
            resolved_sources,
            request.blueprint.preferred_strategy_ids,
        )
        target_models.append(inferred_target or "custom_operational")

    target_strategy: dict[str, str] = {}
    generated_strategy_id = _generated_strategy_id(request.blueprint.blueprint_id)
    for target in target_models:
        selected, generated = _select_strategy(
            target,
            request.blueprint.preferred_strategy_ids,
        )
        if generated:
            selected = generated_strategy_id
        elif selected not in release_blocking:
            raise StrategyCompilationError(
                f"compiler selected non-supported strategy '{selected}'"
            )
        target_strategy[target] = selected

    source_plans: list[SourceCompilationPlan] = []
    selections_by_strategy: dict[str, dict[str, object]] = {}
    for target in target_models:
        strategy_id = target_strategy[target]
        selections_by_strategy[strategy_id] = {
            "generated": target == "custom_operational",
            "source_ids": [],
            "consumption_ids": [],
            "reason": (
                "No standard persistence protocol was requested; generate a purpose-built operational strategy."
                if target == "custom_operational"
                else f"Required by the {target} consumption contract."
            ),
        }

        for source, profile in resolved_sources:
            route = _route_for(profile, target, strategy_id)
            simulation_refs = _simulation_mapping_refs(
                source, profile, target, strategy_id
            )
            if route is not None:
                mapping_mode = route.mode
                mapping_ref = route.mapping_ref
                if route.readiness is MappingReadiness.PROPOSED:
                    issues.append(
                        CompilationIssue(
                            code="MAPPING_ROUTE_NOT_EXECUTABLE",
                            severity=IssueSeverity.BLOCKING,
                            message=(
                                f"Mapping from '{profile.artifact_ref.id}' to '{target}' is "
                                "specified but not executable."
                            ),
                            refs=[source.instance_id, route.mapping_ref or target],
                        )
                    )
                    if simulation_refs:
                        issues.append(
                            CompilationIssue(
                                code="MAPPING_SIMULATION_AVAILABLE",
                                severity=IssueSeverity.INFO,
                                message=(
                                    "A bounded mapping simulation is available for the "
                                    "selected source interactions; it does not make the "
                                    "production route executable."
                                ),
                                refs=[
                                    source.instance_id,
                                    *[
                                        f"{ref.id}@{ref.version}"
                                        for ref in simulation_refs
                                    ],
                                ],
                            )
                        )
            else:
                mapping_mode = MappingMode.GENERATED
                mapping_ref = None
                if not profile.semantic_bindings:
                    issues.append(
                        CompilationIssue(
                            code="SEMANTIC_BINDINGS_REQUIRED",
                            severity=IssueSeverity.BLOCKING,
                            message=(
                                f"Source '{source.instance_id}' has no semantic bindings from "
                                "which a mapping can be generated."
                            ),
                            refs=[source.instance_id],
                        )
                    )
                else:
                    issues.append(
                        CompilationIssue(
                            code="GENERATED_MAPPING_APPROVAL_REQUIRED",
                            severity=IssueSeverity.REVIEW,
                            message=(
                                f"The generated mapping from '{source.instance_id}' to '{target}' "
                                "requires human approval and deterministic validation."
                            ),
                            refs=[source.instance_id, strategy_id],
                        )
                    )

            inferred = [
                binding.source_path
                for binding in profile.semantic_bindings
                if binding.state is SemanticBindingState.INFERRED
            ]
            if inferred:
                issues.append(
                    CompilationIssue(
                        code="SEMANTIC_BINDING_APPROVAL_REQUIRED",
                        severity=IssueSeverity.REVIEW,
                        message=(
                            f"Source '{source.instance_id}' contains inferred semantic bindings "
                            "that must be confirmed before activation."
                        ),
                        refs=[source.instance_id, *inferred],
                    )
                )
            if profile.origin is SourceProfileOrigin.DISCOVERED:
                issues.append(
                    CompilationIssue(
                        code="DISCOVERED_SOURCE_APPROVAL_REQUIRED",
                        severity=IssueSeverity.REVIEW,
                        message=(
                            f"Discovered source profile '{profile.artifact_ref.id}' is tenant-owned "
                            "and requires approval before it can become an executable strategy."
                        ),
                        refs=[source.instance_id, profile.artifact_ref.id],
                    )
                )

            source_plans.append(
                SourceCompilationPlan(
                    source_instance_id=source.instance_id,
                    source_profile_ref=profile.artifact_ref,
                    ingestion_mode=source.ingestion_mode,
                    integration_posture=source.integration_posture,
                    interaction_ids=source.interaction_ids,
                    target_model=target,
                    mapping_mode=mapping_mode,
                    mapping_ref=mapping_ref,
                    simulation_mapping_refs=simulation_refs or None,
                    strategy_id=strategy_id,
                )
            )
            selections_by_strategy[strategy_id]["source_ids"].append(source.instance_id)

    identity_sources = [
        source.instance_id
        for source, profile in resolved_sources
        if _profile_has_person_identity(profile)
    ]
    has_identity_context = any(
        item.kind.value == "identity_resolution" for item in request.blueprint.context
    )
    if len(identity_sources) > 1 and not has_identity_context:
        issues.append(
            CompilationIssue(
                code="IDENTITY_RESOLUTION_REQUIRED",
                severity=IssueSeverity.BLOCKING,
                message=(
                    "Multiple sources carry person identity semantics; declare an identity-resolution "
                    "context before merging their records."
                ),
                refs=identity_sources,
            )
        )

    interface_plans: list[InterfacePlan] = []
    default_strategy = target_strategy[target_models[0]]
    for consumption in request.blueprint.consumptions:
        target = _STANDARD_TARGETS.get(consumption.kind)
        strategy_id = target_strategy.get(target, default_strategy)
        interface_plans.append(
            InterfacePlan(
                consumption_id=consumption.consumption_id,
                kind=consumption.kind,
                protocol=consumption.protocol,
                operations=consumption.operations,
                strategy_id=strategy_id,
            )
        )
        selections_by_strategy[strategy_id]["consumption_ids"].append(
            consumption.consumption_id
        )

    strategy_selections = [
        StrategySelection(
            strategy_id=strategy_id,
            generated=bool(data["generated"]),
            reason=str(data["reason"]),
            source_instance_ids=sorted(set(data["source_ids"])),
            consumption_ids=sorted(set(data["consumption_ids"])),
        )
        for strategy_id, data in selections_by_strategy.items()
    ]

    # Repeated findings are collapsed without losing deterministic ordering.
    unique_issues: list[CompilationIssue] = []
    seen_issues: set[tuple[str, tuple[str, ...]]] = set()
    for issue in issues:
        key = (issue.code, tuple(issue.refs))
        if key not in seen_issues:
            seen_issues.add(key)
            unique_issues.append(issue)

    blocking = any(issue.severity is IssueSeverity.BLOCKING for issue in unique_issues)
    review = any(issue.severity is IssueSeverity.REVIEW for issue in unique_issues)
    if blocking:
        status = CandidateStatus.BLOCKED
    elif review or any(selection.generated for selection in strategy_selections):
        status = CandidateStatus.REQUIRES_APPROVAL
    else:
        status = CandidateStatus.READY

    unsigned = {
        "contract": "strategy-candidate/v1",
        "blueprintId": request.blueprint.blueprint_id,
        "blueprintRevision": request.blueprint.revision,
        "status": status.value,
        "requiresApproval": status is not CandidateStatus.READY,
        "sourcePlans": [plan.to_wire() for plan in source_plans],
        "strategySelections": [selection.to_wire() for selection in strategy_selections],
        "interfacePlans": [plan.to_wire() for plan in interface_plans],
        "issues": [issue.to_wire() for issue in unique_issues],
    }
    candidate_digest = "sha256:" + hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    return StrategyCandidate(
        **unsigned,
        candidateDigest=candidate_digest,
    )
