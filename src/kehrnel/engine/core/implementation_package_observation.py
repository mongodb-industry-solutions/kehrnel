"""Observe health and release drift for one exact active package binding."""

from __future__ import annotations

import hashlib
from typing import Any

from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_observation import (
    ImplementationPackageOperationalObservationHistory,
    ImplementationPackageOperationalObservationReceipt,
    ImplementationPackageOperationalObservationRequest,
    ImplementationPackageOperationalObservationTriggerRequest,
    OperationalObservationCheck,
    StrategyOperationalProbeCheck,
    StrategyOperationalProbeEvidence,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.implementation_artifact_store import (
    FileImplementationArtifactStore,
    ImplementationArtifactNotFound,
)


class ImplementationPackageOperationalObservationError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _trend(values: list[str], rank: dict[str, int]) -> str:
    if len(values) < 2:
        return "insufficient_history"
    change = rank[values[0]] - rank[values[1]]
    return "improved" if change > 0 else "degraded" if change < 0 else "unchanged"


def get_implementation_package_observation_history(
    environment_id: str,
    expected_deployment_ref: ArtifactRef,
    *,
    store: FileImplementationArtifactStore,
    limit: int = 20,
) -> ImplementationPackageOperationalObservationHistory:
    deployment = store.get_deployment(expected_deployment_ref)
    if deployment.environment_id != environment_id:
        raise ImplementationPackageOperationalObservationError(
            "IMPLEMENTATION_PACKAGE_OBSERVATION_SCOPE_MISMATCH",
            "The expected deployment belongs to a different environment.",
            status=409,
        )
    observations, total_count = store.list_operational_observations(
        environment_id,
        expected_deployment_ref,
        limit=limit,
    )
    health_values = [observation.health_state for observation in observations]
    configuration_values = [
        observation.configuration_drift_state for observation in observations
    ]
    return ImplementationPackageOperationalObservationHistory(
        contract="implementation-package-operational-observation-history/v1",
        environmentId=environment_id,
        expectedDeploymentRef=expected_deployment_ref,
        expectedPackageRef=deployment.package_ref,
        observations=observations,
        totalCount=total_count,
        returnedCount=len(observations),
        truncated=total_count > len(observations),
        healthTrend=_trend(
            health_values,
            {"unavailable": 0, "degraded": 1, "healthy": 2},
        ),
        configurationTrend=_trend(
            configuration_values,
            {"drifted": 0, "unavailable": 1, "in_sync": 2},
        ),
    )


def _digest(payload) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _operation_id(
    environment_id: str,
    request_id: str,
    deployment_ref: ArtifactRef,
) -> str:
    identity = _digest(
        {
            "environmentId": environment_id,
            "requestId": request_id,
            "expectedDeploymentRef": deployment_ref.to_wire(),
        }
    )
    return "package-operational-observation." + identity.removeprefix("sha256:")


def _scheduled_request_id(
    environment_id: str,
    request: ImplementationPackageOperationalObservationTriggerRequest,
) -> str:
    identity = _digest(
        {
            "environmentId": environment_id,
            "scheduleId": request.schedule_id,
            "scheduledFor": request.scheduled_for.isoformat(),
            "expectedDeploymentRef": request.expected_deployment_ref.to_wire(),
            "expectedPackageRef": request.expected_package_ref.to_wire(),
        }
    )
    return "scheduled." + identity.removeprefix("sha256:")


def _check(check_id: str, passed: bool | None, detail: str) -> OperationalObservationCheck:
    return OperationalObservationCheck(
        checkId=check_id,
        state="unavailable" if passed is None else "passed" if passed else "failed",
        detail=detail,
    )


_STRATEGY_PROBE_CHECK_IDS = (
    "runtime_connectivity",
    "required_collections",
    "storage_schema",
    "managed_indexes",
    "derived_projection",
    "data_freshness",
    "reconciliation_integrity",
)


def _unavailable_probe(
    activation,
    detail: str,
) -> StrategyOperationalProbeEvidence:
    checks = [
        StrategyOperationalProbeCheck(
            checkId=check_id,
            state="unavailable",
            detail=detail,
        )
        for check_id in _STRATEGY_PROBE_CHECK_IDS
    ]
    payload = {
        "contract": "strategy-operational-probe-evidence/v1",
        "strategyInstanceId": activation.strategy_instance_id,
        "strategyRef": activation.strategy_ref.to_wire(),
        "activationSpecificationRef": activation.activation_specification_ref.to_wire(),
        "runtimeActivationId": activation.runtime_activation_id,
        "domain": activation.domain,
        "probeOperation": "operational_probe",
        "state": "unavailable",
        "schemaState": "unavailable",
        "indexState": "unavailable",
        "checks": [check.to_wire() for check in checks],
    }
    return StrategyOperationalProbeEvidence(
        **payload,
        evidenceDigest=_digest(payload),
    )


def _probe_evidence(activation, raw: Any) -> StrategyOperationalProbeEvidence:
    if not isinstance(raw, dict):
        return _unavailable_probe(
            activation,
            "The strategy returned no structured operational evidence.",
        )
    try:
        checks = [StrategyOperationalProbeCheck.model_validate(item) for item in raw.get("checks", [])]
        payload = {
            "contract": "strategy-operational-probe-evidence/v1",
            "strategyInstanceId": activation.strategy_instance_id,
            "strategyRef": activation.strategy_ref.to_wire(),
            "activationSpecificationRef": activation.activation_specification_ref.to_wire(),
            "runtimeActivationId": activation.runtime_activation_id,
            "domain": activation.domain,
            "probeOperation": "operational_probe",
            "state": raw.get("state"),
            "schemaState": raw.get("schemaState"),
            "indexState": raw.get("indexState"),
            "freshnessTargetSeconds": raw.get("freshnessTargetSeconds"),
            "observedFreshnessSeconds": raw.get("observedFreshnessSeconds"),
            "reconciliationDivergenceCount": raw.get(
                "reconciliationDivergenceCount"
            ),
            "checks": [check.to_wire() for check in checks],
        }
        return StrategyOperationalProbeEvidence(
            **payload,
            evidenceDigest=_digest(payload),
        )
    except Exception:
        return _unavailable_probe(
            activation,
            "The strategy returned invalid operational evidence.",
        )


async def _observe_strategies(
    environment_id: str,
    active,
    *,
    store: FileImplementationArtifactStore,
    runtime,
) -> list[StrategyOperationalProbeEvidence]:
    try:
        preparation = store.get_preparation_execution(active.preparation_execution_ref)
        activation_receipt = store.get_activation_receipt(
            preparation.activation_receipt_ref
        )
        readiness = store.get_operational_readiness(active.readiness_ref)
        assembly = store.find_package_assembly(
            active.package_ref.id,
            active.package_ref.version,
        )
    except Exception:
        # The surrounding lifecycle-integrity checks report the missing artifact.
        # An observation must still be recordable, even when there is no trustworthy
        # activation list from which to construct a strategy-specific probe.
        return []
    resource_types: dict[str, set[str]] = {}
    interaction_by_id = {
        interaction.interaction_id: interaction
        for interaction in assembly.package_build_request.resolution_request.blueprint.interactions
    }
    freshness_targets: dict[str, int] = {}
    for strategy in assembly.resolution.resolved_plan.strategy_instances:
        values = [
            interaction_by_id[interaction_id].guarantees.freshness_target_seconds
            for interaction_id in strategy.interaction_ids
            if interaction_id in interaction_by_id
            and interaction_by_id[interaction_id].guarantees.freshness_target_seconds
            is not None
        ]
        if values:
            freshness_targets[strategy.strategy_instance_id] = min(values)
    for reference in readiness.runtime_evidence_refs:
        evidence = store.get_runtime_execution(reference)
        acknowledgement = evidence.write_acknowledgement
        if acknowledgement is not None and acknowledgement.resource_type:
            resource_type = acknowledgement.resource_type.strip()
            if resource_type and not resource_type.lower().startswith("openehr"):
                resource_types.setdefault(evidence.strategy_instance_id, set()).add(
                    resource_type
                )

    probes: list[StrategyOperationalProbeEvidence] = []
    for activation in activation_receipt.strategy_activations:
        try:
            raw = await runtime.dispatch(
                environment_id,
                "extensions",
                {
                    "domain": activation.domain,
                    "op": "operational_probe",
                    "payload": {
                      "resource_types": sorted(
                            resource_types.get(activation.strategy_instance_id, set())
                        ),
                        **(
                            {
                                "freshness_target_seconds": freshness_targets[
                                    activation.strategy_instance_id
                                ]
                            }
                            if activation.strategy_instance_id in freshness_targets
                            else {}
                        ),
                    },
                    "__activation_precondition": {
                        "activationId": activation.runtime_activation_id,
                        "strategyId": activation.strategy_ref.id,
                    },
                },
            )
            probes.append(_probe_evidence(activation, raw))
        except Exception:
            probes.append(
                _unavailable_probe(
                    activation,
                    "The active strategy could not complete its read-only operational probe.",
                )
            )
    return probes


async def observe_implementation_package_operation(
    environment_id: str,
    request: ImplementationPackageOperationalObservationRequest,
    *,
    store: FileImplementationArtifactStore,
    runtime,
) -> ImplementationPackageOperationalObservationReceipt:
    """Record a payload-safe control-plane health snapshot for an active release."""

    expected = store.get_deployment(request.expected_deployment_ref)
    if expected.environment_id != environment_id:
        raise ImplementationPackageOperationalObservationError(
            "IMPLEMENTATION_PACKAGE_OBSERVATION_SCOPE_MISMATCH",
            "The expected deployment belongs to a different environment.",
            status=409,
        )
    if expected.package_ref != request.expected_package_ref:
        raise ImplementationPackageOperationalObservationError(
            "IMPLEMENTATION_PACKAGE_OBSERVATION_PRECONDITION_FAILED",
            "The expected deployment does not bind the expected package.",
            status=409,
        )
    if request.observed_by_ref != expected.monitoring_owner_ref:
        raise ImplementationPackageOperationalObservationError(
            "IMPLEMENTATION_PACKAGE_OBSERVATION_OWNER_MISMATCH",
            "Operational observation must be recorded by the monitoring owner.",
            status=403,
        )

    observation_id = _operation_id(
        environment_id,
        request.observation_request_id,
        expected.artifact_ref,
    )
    try:
        existing = store.find_operational_observation(
            observation_id,
            expected.package_ref.version,
        )
    except ImplementationArtifactNotFound:
        existing = None
    if existing is not None:
        if (
            existing.expected_deployment_ref == expected.artifact_ref
            and existing.expected_package_ref == expected.package_ref
            and existing.observed_by_ref == request.observed_by_ref
            and existing.observed_at == request.observed_at
        ):
            return existing
        raise ImplementationPackageOperationalObservationError(
            "IMPLEMENTATION_PACKAGE_OBSERVATION_IDEMPOTENCY_CONFLICT",
            "The observation request identifier was already used with different inputs.",
            status=409,
        )

    active_ref = store.get_active_deployment_ref(environment_id)
    active = store.get_deployment(active_ref) if active_ref is not None else None
    if active is None:
        drift_state = "release_absent"
    elif active.artifact_ref != expected.artifact_ref:
        drift_state = "release_changed"
    else:
        drift_state = "in_sync"

    deployment_integrity = False
    readiness_integrity = False
    preparation_integrity = False
    runtime_integrity = False
    if active is not None:
        deployment_integrity = (
            active.environment_id == environment_id
            and active.package_ref.kind == "implementation-package"
            and active.readiness_ref.kind
            == "implementation-package-operational-readiness-receipt"
        )
        try:
            readiness = store.get_operational_readiness(active.readiness_ref)
            readiness_integrity = (
                readiness.environment_id == environment_id
                and readiness.package_ref == active.package_ref
                and readiness.state == "ready"
            )
        except ImplementationArtifactNotFound:
            readiness = None
        try:
            preparation = store.get_preparation_execution(
                active.preparation_execution_ref
            )
            preparation_integrity = (
                preparation.environment_id == environment_id
                and preparation.package_ref == active.package_ref
                and preparation.state == "prepared"
            )
        except ImplementationArtifactNotFound:
            preparation_integrity = False
        if readiness is not None and readiness.runtime_evidence_refs:
            try:
                runtime_integrity = all(
                    (
                        evidence.environment_id == environment_id
                        and evidence.package_ref == active.package_ref
                        and evidence.state == "executed"
                    )
                    for evidence in (
                        store.get_runtime_execution(reference)
                        for reference in readiness.runtime_evidence_refs
                    )
                )
            except ImplementationArtifactNotFound:
                runtime_integrity = False

    strategy_probes = (
        await _observe_strategies(
            environment_id,
            active,
            store=store,
            runtime=runtime,
        )
        if active is not None
        else []
    )
    strategy_health = (
        None
        if active is None or not strategy_probes
        else all(probe.state == "healthy" for probe in strategy_probes)
    )
    configuration_drift_state = (
        "unavailable"
        if not strategy_probes
        else "drifted"
        if any(
            probe.schema_state == "drifted" or probe.index_state == "drifted"
            for probe in strategy_probes
        )
        else "unavailable"
        if any(
            probe.schema_state == "unavailable" or probe.index_state == "unavailable"
            for probe in strategy_probes
        )
        else "in_sync"
    )
    def probe_check_state(check_id: str) -> bool | None:
        states = [
            check.state
            for probe in strategy_probes
            for check in probe.checks
            if check.check_id == check_id
        ]
        if not states or any(state == "unavailable" for state in states):
            return None
        return all(state == "passed" for state in states)

    freshness_health = probe_check_state("data_freshness")
    reconciliation_health = probe_check_state("reconciliation_integrity")
    checks = [
        _check(
            "active_release_present",
            active is not None if active is not None else None,
            "The environment has an active Kehrnel release binding."
            if active is not None
            else "The environment has no active Kehrnel release binding.",
        ),
        _check(
            "expected_deployment_active",
            active is not None and active.artifact_ref == expected.artifact_ref,
            "The observed deployment is the exact expected deployment."
            if active is not None and active.artifact_ref == expected.artifact_ref
            else "The active deployment differs from the expected deployment.",
        ),
        _check(
            "expected_package_active",
            active is not None and active.package_ref == expected.package_ref,
            "The observed package is the exact expected package."
            if active is not None and active.package_ref == expected.package_ref
            else "The active package differs from the expected package.",
        ),
        _check(
            "deployment_artifact_integrity",
            deployment_integrity if active is not None else None,
            "The active deployment receipt is available and internally coherent."
            if deployment_integrity
            else "No coherent active deployment receipt was available.",
        ),
        _check(
            "readiness_artifact_integrity",
            readiness_integrity if active is not None else None,
            "The active deployment resolves to its approved readiness evidence."
            if readiness_integrity
            else "The active deployment did not resolve to coherent readiness evidence.",
        ),
        _check(
            "preparation_artifact_integrity",
            preparation_integrity if active is not None else None,
            "The active deployment resolves to completed preparation evidence."
            if preparation_integrity
            else "The active deployment did not resolve to completed preparation evidence.",
        ),
        _check(
            "runtime_evidence_integrity",
            runtime_integrity if active is not None else None,
            "Every promoted runtime route resolves to payload-safe durable evidence."
            if runtime_integrity
            else "Runtime evidence for the active deployment is unavailable or incomplete.",
        ),
        _check(
            "strategy_runtime_health",
            strategy_health,
            "Every activated strategy completed its read-only runtime probe."
            if strategy_health is True
            else "One or more activated strategies could not prove runtime health.",
        ),
        _check(
            "schema_index_drift",
            None
            if configuration_drift_state == "unavailable"
            else configuration_drift_state == "in_sync",
            "Observed collections, schemas and managed indexes match the active strategy contract."
            if configuration_drift_state == "in_sync"
            else "Observed collections, schemas or managed indexes are drifted or unavailable.",
        ),
        _check(
            "data_freshness",
            freshness_health,
            "Every activated strategy satisfies its declared freshness boundary."
            if freshness_health is True
            else "Freshness is outside its declared boundary or cannot be proven.",
        ),
        _check(
            "reconciliation_integrity",
            reconciliation_health,
            "Every activated derived representation is reconciled with its authority."
            if reconciliation_health is True
            else "Reconciliation is incomplete, divergent or cannot be proven.",
        ),
    ]
    all_passed = all(check.state == "passed" for check in checks)
    health_state = (
        "unavailable"
        if drift_state == "release_absent"
        else "healthy"
        if all_passed and drift_state == "in_sync"
        else "degraded"
    )
    if drift_state == "release_absent":
        next_required_action = "deploy_release_candidate"
    elif drift_state == "release_changed":
        next_required_action = "review_release_drift"
    elif configuration_drift_state == "drifted":
        next_required_action = "reconcile_strategy_drift"
    elif any(probe.state != "healthy" for probe in strategy_probes):
        next_required_action = "investigate_strategy_health"
    else:
        next_required_action = "continue_monitoring"
    payload = {
        "contract": "implementation-package-operational-observation-receipt/v1",
        "observationId": observation_id,
        "observationRequestId": request.observation_request_id,
        "environmentId": environment_id,
        "expectedDeploymentRef": expected.artifact_ref.to_wire(),
        "expectedPackageRef": expected.package_ref.to_wire(),
        **(
            {
                "activeDeploymentRef": active.artifact_ref.to_wire(),
                "activePackageRef": active.package_ref.to_wire(),
            }
            if active is not None
            else {}
        ),
        "observedByRef": request.observed_by_ref,
        "observedAt": request.observed_at.isoformat(),
        "monitoringOwnerRef": expected.monitoring_owner_ref,
        "checks": [check.to_wire() for check in checks],
        "strategyProbes": [probe.to_wire() for probe in strategy_probes],
        "healthState": health_state,
        "driftState": drift_state,
        "configurationDriftState": configuration_drift_state,
        "observationScope": "kehrnel_release_binding",
        "externalTrafficObservation": "not_observed",
        "evidencePersistence": "durable",
        "nextRequiredAction": next_required_action,
    }
    observation_digest = _digest(payload)
    receipt = ImplementationPackageOperationalObservationReceipt(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-operational-observation-receipt",
            id=observation_id,
            version=expected.package_ref.version,
            digest=observation_digest,
        ),
        observationDigest=observation_digest,
    )
    store.put_operational_observation(receipt)
    return receipt


async def trigger_implementation_package_operational_observation(
    environment_id: str,
    request: ImplementationPackageOperationalObservationTriggerRequest,
    *,
    store: FileImplementationArtifactStore,
    runtime,
) -> ImplementationPackageOperationalObservationReceipt:
    """Run one retry-safe scheduler slot through the canonical observation path."""

    return await observe_implementation_package_operation(
        environment_id,
        ImplementationPackageOperationalObservationRequest(
            contract="implementation-package-operational-observation-request/v1",
            observationRequestId=_scheduled_request_id(environment_id, request),
            expectedDeploymentRef=request.expected_deployment_ref,
            expectedPackageRef=request.expected_package_ref,
            observedByRef=request.observed_by_ref,
            observedAt=request.scheduled_for,
        ),
        store=store,
        runtime=runtime,
    )


__all__ = [
    "ImplementationPackageOperationalObservationError",
    "observe_implementation_package_operation",
    "trigger_implementation_package_operational_observation",
]
