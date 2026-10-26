"""Deterministic canonical ``solution-blueprint/v1`` architecture resolution.

The resolver selects reviewed strategy packages and compiles three linked,
non-deploying artifacts: the resolved overview, the preparation plan, and the
runtime execution plan.  It never invents semantic bindings, activates a
strategy, or treats an unavailable target capability as an approval question.
"""

from __future__ import annotations

from collections import defaultdict
import hashlib
from pathlib import Path

from kehrnel.contracts import (
    ArchitectureResolutionInputCatalog,
    ArchitectureResolutionRequest,
    ArchitectureResolutionResult,
    ArtifactRef,
    BindingConsumerKind,
    BlueprintRevisionRef,
    CandidateStatus,
    CapabilityAvailability,
    CompilationIssue,
    InteractionBehavior,
    InterfaceKind,
    IssueSeverity,
    MaterializationKind,
    MaterializationMaintenance,
    MaterializationPlanV1,
    PreparationPlan,
    PreparationStage,
    PreparationStageKind,
    PreparationTrigger,
    QueryPlanFamily,
    ResolvedArchitecturePlan,
    ResolvedStrategyInstance,
    ResourceOwnership,
    RuntimeExecutionPlan,
    RuntimeRoute,
    SemanticExecutionBindingV1,
    TargetCapabilityRequirement,
    TargetCapability,
    TargetCapabilityProfile,
    TargetCompatibilityAssessment,
    TargetTopology,
    solution_blueprint_content_digest,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.pack_loader import (
    iter_strategy_manifest_paths,
    load_strategy_manifest,
)
from kehrnel.engine.core.strategy_portfolio import load_strategy_portfolio


STRATEGY_ROOT = Path(__file__).resolve().parents[1] / "strategies"
WRITE_BEHAVIORS = {
    InteractionBehavior.CREATE,
    InteractionBehavior.UPDATE,
    InteractionBehavior.DELETE,
    InteractionBehavior.COMMAND,
    InteractionBehavior.PUBLISH,
}
QUERY_BEHAVIORS = {
    InteractionBehavior.READ,
    InteractionBehavior.SEARCH,
    InteractionBehavior.QUERY,
}


class ArchitectureResolutionError(ValueError):
    """The resolution request is inconsistent before compilation can begin."""


def _digest(value: dict) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _artifact_ref(kind: str, artifact_id: str, payload: dict) -> ArtifactRef:
    return ArtifactRef(
        kind=kind,
        id=artifact_id,
        version="1.0.0",
        digest=_digest(payload),
    )


def _component_ref(strategy_id: str, component: str) -> ArtifactRef:
    component_id = f"{strategy_id}.{component}"
    return _artifact_ref(
        "component",
        component_id,
        {"strategyId": strategy_id, "component": component},
    )


def _strategy_id_for_interface(kind: InterfaceKind, preferred: set[str]) -> str | None:
    if kind in {InterfaceKind.OPENEHR, InterfaceKind.AQL}:
        supported = ("openehr.rps_dual", "openehr.rps_dual_ibm")
        return next((item for item in supported if item in preferred), supported[0])
    if kind is InterfaceKind.FHIR:
        return "fhir.resource_store"
    return None


def _load_supported_strategy(strategy_id: str):
    portfolio = load_strategy_portfolio()
    entry = portfolio.entry_for(strategy_id)
    if entry is None or not entry.release_blocking:
        raise ArchitectureResolutionError(
            f"strategy '{strategy_id}' is not in the supported release portfolio"
        )
    for path in iter_strategy_manifest_paths(STRATEGY_ROOT):
        try:
            if f'"id": "{strategy_id}"' not in path.read_text(encoding="utf-8"):
                continue
        except OSError:
            continue
        return load_strategy_manifest(path, strategy_id=strategy_id)
    raise ArchitectureResolutionError(
        f"supported strategy package '{strategy_id}' is not installed"
    )


def _strategy_ref(manifest) -> ArtifactRef:
    if not manifest.package_digest:
        raise ArchitectureResolutionError(
            f"strategy package '{manifest.id}' has no package digest"
        )
    return ArtifactRef(
        kind="strategy",
        id=manifest.id,
        version=manifest.version,
        digest=manifest.package_digest,
    )


def _strategy_supports_composition(manifest) -> bool:
    """Return whether a reviewed strategy can build a derived data product."""

    return bool(
        {"transform", "semantic_materialization"}
        & set(manifest.capabilities or [])
    )


def _binding_for(
    bindings: list[SemanticExecutionBindingV1],
    *,
    authority_ref: ArtifactRef,
    strategy_ref: ArtifactRef,
) -> SemanticExecutionBindingV1 | None:
    return next(
        (
            binding
            for binding in bindings
            if binding.semantic_authority_ref == authority_ref
            and binding.strategy_ref == strategy_ref
        ),
        None,
    )


def load_architecture_resolution_inputs() -> ArchitectureResolutionInputCatalog:
    """Publish content-addressed, reviewed inputs for architecture resolution.

    This is reference evidence for design-time evaluation. A private tenant
    portal should publish additional profiles derived from the actual target;
    the public Builder must never present this reference profile as observed
    tenant state.
    """

    target_payload = {
        "deploymentId": "reference-atlas-evaluation",
        "mongodbVersion": "8.0",
        "topology": {
            "deploymentModel": "atlas-dedicated-reference",
            "regions": [],
            "shardCount": 1,
            "searchNodeCount": 1,
        },
        "capabilities": [
            {
                "capabilityId": "mongodb.transactions",
                "availability": "available",
                "version": "4.0",
                "constraints": {},
            },
            {
                "capabilityId": "atlas.search",
                "availability": "available",
                "constraints": {},
            },
            {
                "capabilityId": "change.streams",
                "availability": "available",
                "constraints": {},
            },
        ],
    }
    target = TargetCapabilityProfile(
        contract="target-capability-profile/v1",
        artifactRef=_artifact_ref(
            "target-capability-profile",
            "reference-atlas-evaluation",
            target_payload,
        ),
        **target_payload,
    )

    manifest = _load_supported_strategy("openehr.rps_dual")
    strategy_ref = _strategy_ref(manifest)
    authority_payload = {
        "standard": "openEHR",
        "scope": "clinical-record",
        "status": "reviewed",
    }
    authority_ref = _artifact_ref(
        "openehr-template", "builder-clinical-record", authority_payload
    )
    binding_payload = {
        "contract": "semantic-execution-binding/v1",
        "semanticAuthorityRef": authority_ref.to_wire(),
        "strategyRef": strategy_ref.to_wire(),
        "pathBindings": [
            {
                "semanticPath": "openEHR.composition",
                "physicalPath": "canonicalCompositions",
                "valueType": "DOCUMENT",
                "repeated": True,
                "indexRefs": ["rps-dual-canonical", "rps-dual-search"],
                "reconstructionRule": "Reconstruct the canonical openEHR composition through the reviewed RPS Dual binding.",
            }
        ],
        "consumers": [
            {
                "consumerId": "rps-dual-preparation",
                "kind": "preparation",
                "componentRef": _component_ref(
                    "openehr.rps_dual", "preparation"
                ).to_wire(),
            },
            {
                "consumerId": "rps-dual-query-planner",
                "kind": "query_planner",
                "componentRef": _component_ref(
                    "openehr.rps_dual", "query-planner"
                ).to_wire(),
            },
            {
                "consumerId": "rps-dual-executor",
                "kind": "executor",
                "componentRef": _component_ref(
                    "openehr.rps_dual", "executor"
                ).to_wire(),
            },
        ],
        "writerInteractionIds": [],
        "queryInteractionIds": [],
    }
    openehr_binding = SemanticExecutionBindingV1(
        **binding_payload,
        artifactRef=_artifact_ref(
            "semantic-execution-binding",
            "openehr-rps-dual-clinical-record",
            binding_payload,
        ),
    )

    fhir_manifest = _load_supported_strategy("fhir.resource_store")
    fhir_strategy_ref = _strategy_ref(fhir_manifest)
    fhir_authority_payload = {
        "standard": "HL7 FHIR",
        "release": "R4",
        "version": "4.0.1",
        "resourceType": "Observation",
        "status": "reviewed-bounded",
    }
    fhir_authority_ref = _artifact_ref(
        "fhir-structure-definition",
        "fhir-r4-observation",
        fhir_authority_payload,
    )
    fhir_binding_payload = {
        "contract": "semantic-execution-binding/v1",
        "semanticAuthorityRef": fhir_authority_ref.to_wire(),
        "strategyRef": fhir_strategy_ref.to_wire(),
        "pathBindings": [
            {
                "semanticPath": "Observation",
                "physicalPath": "Observation",
                "valueType": "FHIR_R4_RESOURCE",
                "repeated": True,
                "correlationKey": "subject.reference",
                "indexRefs": [
                    "fhir-observation-id",
                    "fhir-observation-subject-code-date",
                ],
                "reconstructionRule": (
                    "Return canonical FHIR R4 Observation JSON after removing "
                    "strategy-owned operational fields."
                ),
            }
        ],
        "consumers": [
            {
                "consumerId": "fhir-resource-store-preparation",
                "kind": "preparation",
                "componentRef": _component_ref(
                    "fhir.resource_store", "preparation"
                ).to_wire(),
            },
            {
                "consumerId": "fhir-search-planner",
                "kind": "query_planner",
                "componentRef": _component_ref(
                    "fhir.resource_store", "query-planner"
                ).to_wire(),
            },
            {
                "consumerId": "fhir-search-executor",
                "kind": "executor",
                "componentRef": _component_ref(
                    "fhir.resource_store", "executor"
                ).to_wire(),
            },
        ],
        "writerInteractionIds": [],
        "queryInteractionIds": [],
    }
    fhir_binding = SemanticExecutionBindingV1(
        **fhir_binding_payload,
        artifactRef=_artifact_ref(
            "semantic-execution-binding",
            "fhir-r4-observation-resource-store",
            fhir_binding_payload,
        ),
    )
    return ArchitectureResolutionInputCatalog(
        contract="architecture-resolution-input-catalog/v1",
        targetCapabilityProfiles=[target],
        semanticExecutionBindings=[openehr_binding, fhir_binding],
    )


def _capability_requirements(
    strategy_id: str,
    interactions,
) -> list[TargetCapabilityRequirement]:
    requirements: list[TargetCapabilityRequirement] = []
    if strategy_id.startswith("openehr.") and any(
        item.behavior in WRITE_BEHAVIORS for item in interactions
    ):
        requirements.append(
            TargetCapabilityRequirement(
                capabilityId="mongodb.transactions",
                required=True,
                minimumVersion="4.0",
                rationale="Atomic openEHR contribution and version writes require transactions.",
            )
        )
    if strategy_id.startswith("openehr.") and any(
        item.behavior in QUERY_BEHAVIORS
        and "cohort" in item.workload_ref.id.lower()
        for item in interactions
    ):
        requirements.append(
            TargetCapabilityRequirement(
                capabilityId="atlas.search",
                required=True,
                rationale="The selected population-cohort plan uses the RPS Dual Search projection.",
            )
        )
    return requirements


def _assess_target(requirements, target) -> list[TargetCompatibilityAssessment]:
    capabilities = {item.capability_id: item for item in target.capabilities}
    assessments: list[TargetCompatibilityAssessment] = []
    for requirement in requirements:
        capability = capabilities.get(requirement.capability_id)
        availability = (
            capability.availability
            if capability is not None
            else CapabilityAvailability.UNKNOWN
        )
        compatible = availability is CapabilityAvailability.AVAILABLE
        assessments.append(
            TargetCompatibilityAssessment(
                capabilityId=requirement.capability_id,
                required=requirement.required,
                availability=availability,
                compatible=compatible,
                rationale=(
                    "The target declares this required capability available."
                    if compatible
                    else "The target does not declare this required capability available."
                ),
            )
        )
    return assessments


def compile_architecture_resolution(
    request: ArchitectureResolutionRequest,
) -> ArchitectureResolutionResult:
    """Resolve the canonical Blueprint without activating or deploying anything."""

    blueprint = request.blueprint
    blueprint_ref = BlueprintRevisionRef(
        blueprintId=blueprint.blueprint_id,
        revision=blueprint.revision,
        digest=solution_blueprint_content_digest(blueprint),
    )
    preferred = {item.id for item in blueprint.preferred_foundation_refs}
    authorities = {item.scope_id: item for item in blueprint.semantic_authorities}
    interactions_by_strategy: dict[str, list] = defaultdict(list)
    issues: list[CompilationIssue] = []

    for interaction in blueprint.interactions:
        strategy_id = _strategy_id_for_interface(interaction.interface.kind, preferred)
        if strategy_id is None:
            issues.append(
                CompilationIssue(
                    code="INTERFACE_STRATEGY_UNAVAILABLE",
                    severity=IssueSeverity.BLOCKING,
                    message=(
                        f"No supported strategy resolves interface '{interaction.interface.kind.value}'."
                    ),
                    refs=[interaction.interaction_id, interaction.interface.kind.value],
                )
            )
            continue
        interactions_by_strategy[strategy_id].append(interaction)

    strategy_instances: list[ResolvedStrategyInstance] = []
    selected_bindings: dict[tuple[str, str], SemanticExecutionBindingV1] = {}
    all_requirements: list[TargetCapabilityRequirement] = []
    strategy_refs: dict[str, ArtifactRef] = {}
    strategy_manifests = {}

    for strategy_id in sorted(interactions_by_strategy):
        manifest = _load_supported_strategy(strategy_id)
        strategy_manifests[strategy_id] = manifest
        strategy_ref = _strategy_ref(manifest)
        strategy_refs[strategy_id] = strategy_ref
        interactions = interactions_by_strategy[strategy_id]
        requirements = _capability_requirements(strategy_id, interactions)
        all_requirements.extend(requirements)

        binding_refs: list[ArtifactRef] = []
        for scope_id in sorted({item.semantic_scope_id for item in interactions}):
            authority = authorities[scope_id]
            binding = _binding_for(
                request.semantic_execution_bindings,
                authority_ref=authority.authority_ref,
                strategy_ref=strategy_ref,
            )
            if binding is None:
                issues.append(
                    CompilationIssue(
                        code="SEMANTIC_EXECUTION_BINDING_REQUIRED",
                        severity=IssueSeverity.BLOCKING,
                        message=(
                            f"Strategy '{strategy_id}' has no reviewed execution binding for semantic scope '{scope_id}'."
                        ),
                        refs=[scope_id, strategy_id],
                    )
                )
                continue
            scoped_interaction_ids = {
                item.interaction_id
                for item in interactions
                if item.semantic_scope_id == scope_id
            }
            missing_ids = sorted(
                item.interaction_id
                for item in interactions
                if item.semantic_scope_id == scope_id
                and not binding.covers_interaction(
                    item.interaction_id,
                    write=item.behavior in WRITE_BEHAVIORS,
                )
            )
            if missing_ids:
                issues.append(
                    CompilationIssue(
                        code="SEMANTIC_EXECUTION_BINDING_INCOMPLETE",
                        severity=IssueSeverity.BLOCKING,
                        message=(
                            f"Execution binding '{binding.artifact_ref.id}' does not cover all interactions in scope '{scope_id}'."
                        ),
                        refs=[binding.artifact_ref.id, *missing_ids],
                    )
                )
                continue
            selected_bindings[(scope_id, strategy_id)] = binding
            binding_refs.append(binding.artifact_ref)

        configuration_payload = {
            "strategyRef": strategy_ref.to_wire(),
            "interactionIds": sorted(item.interaction_id for item in interactions),
            "bindingRefs": [item.to_wire() for item in binding_refs],
            "targetCapabilityProfileRef": request.target_capability_profile.artifact_ref.to_wire(),
        }
        strategy_instances.append(
            ResolvedStrategyInstance(
                strategyInstanceId=strategy_id.replace(".", "-"),
                strategyRef=strategy_ref,
                configurationDigest=_digest(configuration_payload),
                interactionIds=sorted(item.interaction_id for item in interactions),
                bindingRefs=binding_refs,
                dependencyRefs=[],
                capabilityRequirements=requirements,
            )
        )

    unique_requirements = {
        item.capability_id: item for item in all_requirements
    }
    target_compatibility = _assess_target(
        [unique_requirements[key] for key in sorted(unique_requirements)],
        request.target_capability_profile,
    )
    for assessment in target_compatibility:
        if assessment.required and not assessment.compatible:
            issues.append(
                CompilationIssue(
                    code="TARGET_CAPABILITY_UNAVAILABLE",
                    severity=IssueSeverity.BLOCKING,
                    message=(
                        f"Required capability '{assessment.capability_id}' is not available on target "
                        f"'{request.target_capability_profile.deployment_id}'."
                    ),
                    refs=[
                        assessment.capability_id,
                        request.target_capability_profile.deployment_id,
                    ],
                )
            )

    preparation_stages: list[PreparationStage] = []
    runtime_routes: list[RuntimeRoute] = []
    materializations: list[MaterializationPlanV1] = []
    query_plan_families: list[QueryPlanFamily] = []
    resource_ownership: list[ResourceOwnership] = []

    for strategy_id in sorted(interactions_by_strategy):
        instance_id = strategy_id.replace(".", "-")
        interactions = interactions_by_strategy[strategy_id]
        resource_prefix = strategy_id.replace(".", "-")
        canonical_resource = f"{resource_prefix}-canonical"
        resource_ownership.append(
            ResourceOwnership(
                resourceId=canonical_resource,
                resourceKind="collection-family",
                ownerStrategyInstanceId=instance_id,
            )
        )
        for interaction in sorted(interactions, key=lambda item: item.interaction_id):
            binding = selected_bindings.get(
                (interaction.semantic_scope_id, strategy_id)
            )
            if binding is None:
                continue
            if interaction.behavior in WRITE_BEHAVIORS:
                preparation_stages.append(
                    PreparationStage(
                        stageId=f"{interaction.interaction_id}-authoritative-write",
                        kind=PreparationStageKind.AUTHORITATIVE_WRITE,
                        trigger=PreparationTrigger.SYNCHRONOUS_WRITE,
                        strategyInstanceId=instance_id,
                        interactionIds=[interaction.interaction_id],
                        bindingRefs=[binding.artifact_ref],
                        inputResources=[],
                        outputResources=[canonical_resource],
                        freshnessTargetSeconds=interaction.guarantees.freshness_target_seconds,
                        failureBehavior="Fail the interaction without acknowledging an authoritative write.",
                    )
                )

            plan_family_id = None
            if interaction.behavior in QUERY_BEHAVIORS:
                cohort = "cohort" in interaction.workload_ref.id.lower()
                mechanism = (
                    "atlas-search-then-authoritative-fetch"
                    if strategy_id.startswith("openehr.") and cohort
                    else "native-indexed-query"
                )
                plan_family_id = f"{interaction.interaction_id}-eligible-plan"
                query_plan_families.append(
                    QueryPlanFamily(
                        planFamilyId=plan_family_id,
                        interactionIds=[interaction.interaction_id],
                        strategyInstanceId=instance_id,
                        semanticBindingRef=binding.artifact_ref,
                        mechanism=mechanism,
                        runtimeConstraints={
                            "requestTimePlannerRequired": True,
                            "targetCapabilityProfileDigest": request.target_capability_profile.artifact_ref.digest,
                        },
                        fallbackPlanFamilyIds=[],
                    )
                )
                if mechanism.startswith("atlas-search"):
                    derived_resource = f"{resource_prefix}-cohort-search"
                    resource_ownership.append(
                        ResourceOwnership(
                            resourceId=derived_resource,
                            resourceKind="search-index",
                            ownerStrategyInstanceId=instance_id,
                        )
                    )
                    materializations.append(
                        MaterializationPlanV1(
                            materializationId=f"{interaction.interaction_id}-search",
                            kind=MaterializationKind.SEARCH_INDEX,
                            ownerStrategyInstanceId=instance_id,
                            servesInteractionIds=[interaction.interaction_id],
                            rationale="Provide the compiled population-query access path while preserving authoritative records.",
                            maintenance=MaterializationMaintenance(
                                updateMode="asynchronous",
                                rebuildSource=canonical_resource,
                                deletionBehavior="Propagate authoritative deletion or tombstone semantics.",
                                recoveryBehavior="Rebuild from the authoritative collection family.",
                                retirementCondition="No active interaction uses this plan family.",
                                freshnessTargetSeconds=interaction.guarantees.freshness_target_seconds,
                            ),
                        )
                    )
                    writer_ids = sorted(
                        item.interaction_id
                        for item in interactions
                        if item.behavior in WRITE_BEHAVIORS
                        and item.semantic_scope_id == interaction.semantic_scope_id
                    )
                    if writer_ids:
                        preparation_stages.append(
                            PreparationStage(
                                stageId=f"{interaction.interaction_id}-search-materialization",
                                kind=PreparationStageKind.MATERIALIZE,
                                trigger=PreparationTrigger.ASYNCHRONOUS_EVENT,
                                strategyInstanceId=instance_id,
                                interactionIds=writer_ids,
                                bindingRefs=[binding.artifact_ref],
                                inputResources=[canonical_resource],
                                outputResources=[derived_resource],
                                freshnessTargetSeconds=interaction.guarantees.freshness_target_seconds,
                                failureBehavior="Keep the authoritative write and expose derived freshness as stale until recovery succeeds.",
                            )
                        )

            runtime_routes.append(
                RuntimeRoute(
                    routeId=f"{interaction.interaction_id}-runtime",
                    interactionId=interaction.interaction_id,
                    behavior=interaction.behavior,
                    strategyInstanceId=instance_id,
                    semanticBindingRef=binding.artifact_ref,
                    planFamilyId=plan_family_id,
                    plannerRef=_component_ref(strategy_id, "query-planner"),
                    executorRef=_component_ref(strategy_id, "executor"),
                    policyRef=interaction.authorization_policy_ref,
                    fallbackBehavior=(
                        "Fail closed when no eligible runtime plan satisfies policy, freshness, and target capability constraints."
                    ),
                )
            )

    scope_strategies: dict[str, set[str]] = defaultdict(set)
    for strategy_id, strategy_interactions in interactions_by_strategy.items():
        for interaction in strategy_interactions:
            if (interaction.semantic_scope_id, strategy_id) in selected_bindings:
                scope_strategies[interaction.semantic_scope_id].add(strategy_id)

    for composition in blueprint.data_product_compositions:
        target_candidates = sorted(
            strategy_id
            for strategy_id in scope_strategies.get(composition.target_scope_id, set())
            if _strategy_supports_composition(strategy_manifests[strategy_id])
        )
        input_strategy_ids = {
            scope_id: sorted(scope_strategies.get(scope_id, set()))
            for scope_id in composition.input_scope_ids
        }
        missing_inputs = sorted(
            scope_id for scope_id, strategy_ids in input_strategy_ids.items()
            if not strategy_ids
        )
        if len(target_candidates) != 1 or missing_inputs:
            reasons = []
            if not target_candidates:
                reasons.append(
                    f"no resolved composition-capable strategy serves target scope '{composition.target_scope_id}'"
                )
            elif len(target_candidates) > 1:
                reasons.append(
                    f"multiple composition-capable strategies serve target scope '{composition.target_scope_id}'"
                )
            if missing_inputs:
                reasons.append(
                    "no resolved strategy serves input scope(s) " + ", ".join(missing_inputs)
                )
            issues.append(
                CompilationIssue(
                    code="DATA_PRODUCT_COMPOSITION_STRATEGY_UNAVAILABLE",
                    severity=IssueSeverity.BLOCKING,
                    message=(
                        f"Data-product composition '{composition.composition_id}' cannot be planned: "
                        + "; ".join(reasons)
                        + "."
                    ),
                    refs=[
                        composition.composition_id,
                        composition.target_scope_id,
                        *composition.input_scope_ids,
                    ],
                )
            )
            continue

        target_strategy_id = target_candidates[0]
        target_instance_id = target_strategy_id.replace(".", "-")
        target_interactions = sorted(
            (
                item
                for item in interactions_by_strategy[target_strategy_id]
                if item.semantic_scope_id == composition.target_scope_id
            ),
            key=lambda item: item.interaction_id,
        )
        binding_refs = {
            selected_bindings[(composition.target_scope_id, target_strategy_id)].artifact_ref
        }
        input_resources = []
        for input_scope_id in composition.input_scope_ids:
            input_strategy_id = input_strategy_ids[input_scope_id][0]
            binding_refs.add(
                selected_bindings[(input_scope_id, input_strategy_id)].artifact_ref
            )
            input_resources.append(
                f"{input_strategy_id.replace('.', '-')}-canonical"
            )
        output_resource = f"{target_instance_id}-composition-{composition.target_scope_id}"
        resource_ownership.append(
            ResourceOwnership(
                resourceId=output_resource,
                resourceKind="derived-data-product",
                ownerStrategyInstanceId=target_instance_id,
            )
        )
        freshness_targets = [
            item.guarantees.freshness_target_seconds
            for item in target_interactions
            if item.guarantees.freshness_target_seconds is not None
        ]
        preparation_stages.append(
            PreparationStage(
                stageId=f"{composition.composition_id}-compose",
                kind=PreparationStageKind.COMPOSE,
                trigger=PreparationTrigger.ASYNCHRONOUS_EVENT,
                strategyInstanceId=target_instance_id,
                interactionIds=[item.interaction_id for item in target_interactions],
                bindingRefs=sorted(
                    binding_refs,
                    key=lambda reference: (
                        reference.id,
                        reference.version,
                        reference.digest,
                    ),
                ),
                inputResources=sorted(set(input_resources)),
                outputResources=[output_resource],
                freshnessTargetSeconds=(
                    min(freshness_targets) if freshness_targets else None
                ),
                failureBehavior=(
                    "Keep source products authoritative and mark the derived "
                    "product stale until composition succeeds."
                ),
                compositionId=composition.composition_id,
            )
        )

    blocking = any(issue.severity is IssueSeverity.BLOCKING for issue in issues)
    review = any(issue.severity is IssueSeverity.REVIEW for issue in issues)
    status = (
        CandidateStatus.BLOCKED
        if blocking
        else CandidateStatus.REQUIRES_APPROVAL
        if review
        else CandidateStatus.READY
    )

    common_identity = {
        "blueprintRef": blueprint_ref.to_wire(),
        "targetCapabilityProfileRef": request.target_capability_profile.artifact_ref.to_wire(),
    }
    preparation_payload = {
        "contract": "preparation-plan/v1",
        **common_identity,
        "stages": [item.to_wire() for item in preparation_stages],
        "issues": [item.to_wire() for item in issues],
    }
    preparation_ref = _artifact_ref(
        "preparation-plan", blueprint.blueprint_id, preparation_payload
    )
    preparation_plan = PreparationPlan(
        **preparation_payload,
        artifactRef=preparation_ref,
    )

    runtime_payload = {
        "contract": "runtime-execution-plan/v1",
        **common_identity,
        "routes": [item.to_wire() for item in runtime_routes],
        "issues": [item.to_wire() for item in issues],
    }
    runtime_ref = _artifact_ref(
        "runtime-execution-plan", blueprint.blueprint_id, runtime_payload
    )
    runtime_plan = RuntimeExecutionPlan(
        **runtime_payload,
        artifactRef=runtime_ref,
    )

    resolved_payload = {
        "contract": "resolved-architecture-plan/v1",
        **common_identity,
        "status": status.value,
        "requiresApproval": status is CandidateStatus.REQUIRES_APPROVAL,
        "strategyInstances": [item.to_wire() for item in strategy_instances],
        "semanticBindingRefs": [
            item.to_wire()
            for item in sorted(
                {binding.artifact_ref for binding in selected_bindings.values()},
                key=lambda reference: (
                    reference.id,
                    reference.version,
                    reference.digest,
                ),
            )
        ],
        "resourceOwnership": [item.to_wire() for item in resource_ownership],
        "materializations": [item.to_wire() for item in materializations],
        "queryPlanFamilies": [item.to_wire() for item in query_plan_families],
        "targetCompatibility": [item.to_wire() for item in target_compatibility],
        "preparationPlanRef": preparation_ref.to_wire(),
        "runtimePlanRef": runtime_ref.to_wire(),
        "issues": [item.to_wire() for item in issues],
    }
    resolved_ref = _artifact_ref(
        "resolved-architecture-plan", blueprint.blueprint_id, resolved_payload
    )
    resolved_plan = ResolvedArchitecturePlan(
        **resolved_payload,
        artifactRef=resolved_ref,
    )

    return ArchitectureResolutionResult(
        contract="architecture-resolution-result/v1",
        resolvedPlan=resolved_plan,
        preparationPlan=preparation_plan,
        runtimePlan=runtime_plan,
    )
