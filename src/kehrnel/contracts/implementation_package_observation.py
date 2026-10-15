"""Operational health and release-drift evidence for active packages."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import Field, model_validator

from .artifacts import ArtifactRef, ContractModel, Sha256Digest


OperationalObservationCheckId = Literal[
    "active_release_present",
    "expected_deployment_active",
    "expected_package_active",
    "deployment_artifact_integrity",
    "readiness_artifact_integrity",
    "preparation_artifact_integrity",
    "runtime_evidence_integrity",
    "strategy_runtime_health",
    "schema_index_drift",
    "data_freshness",
    "reconciliation_integrity",
]

StrategyOperationalProbeCheckId = Literal[
    "runtime_connectivity",
    "required_collections",
    "storage_schema",
    "managed_indexes",
    "derived_projection",
    "data_freshness",
    "reconciliation_integrity",
]


class OperationalObservationCheck(ContractModel):
    check_id: OperationalObservationCheckId
    state: Literal["passed", "failed", "unavailable"]
    detail: str = Field(min_length=1, max_length=1000)


class StrategyOperationalProbeCheck(ContractModel):
    check_id: StrategyOperationalProbeCheckId
    state: Literal["passed", "failed", "unavailable"]
    detail: str = Field(min_length=1, max_length=1000)


class StrategyOperationalProbeEvidence(ContractModel):
    contract: Literal["strategy-operational-probe-evidence/v1"]
    strategy_instance_id: str = Field(min_length=1)
    strategy_ref: ArtifactRef
    activation_specification_ref: ArtifactRef
    runtime_activation_id: str = Field(min_length=1)
    domain: str = Field(min_length=1)
    probe_operation: Literal["operational_probe"] = "operational_probe"
    state: Literal["healthy", "degraded", "unavailable"]
    schema_state: Literal["in_sync", "drifted", "unavailable"]
    index_state: Literal["in_sync", "drifted", "unavailable"]
    freshness_target_seconds: Optional[int] = Field(default=None, ge=0)
    observed_freshness_seconds: Optional[int] = Field(default=None, ge=0)
    reconciliation_divergence_count: Optional[int] = Field(default=None, ge=0)
    checks: list[StrategyOperationalProbeCheck] = Field(min_length=7, max_length=7)
    evidence_digest: Sha256Digest

    @model_validator(mode="after")
    def evidence_is_coherent(self) -> "StrategyOperationalProbeEvidence":
        if self.strategy_ref.kind != "strategy":
            raise ValueError("strategyRef.kind must be 'strategy'")
        if self.activation_specification_ref.kind != (
            "approved-strategy-activation-specification"
        ):
            raise ValueError(
                "activationSpecificationRef.kind must be "
                "'approved-strategy-activation-specification'"
            )
        expected_ids = {
            "runtime_connectivity",
            "required_collections",
            "storage_schema",
            "managed_indexes",
            "derived_projection",
            "data_freshness",
            "reconciliation_integrity",
        }
        if {check.check_id for check in self.checks} != expected_ids:
            raise ValueError("checks must contain every strategy probe check once")
        all_passed = all(check.state == "passed" for check in self.checks)
        all_unavailable = all(check.state == "unavailable" for check in self.checks)
        expected_state = (
            "healthy" if all_passed else "unavailable" if all_unavailable else "degraded"
        )
        if self.state != expected_state:
            raise ValueError("state must reflect strategy probe checks")
        check_by_id = {check.check_id: check for check in self.checks}
        schema_checks = [
            check_by_id["required_collections"],
            check_by_id["storage_schema"],
        ]
        expected_schema_state = (
            "drifted"
            if any(check.state == "failed" for check in schema_checks)
            else "in_sync"
            if all(check.state == "passed" for check in schema_checks)
            else "unavailable"
        )
        if self.schema_state != expected_schema_state:
            raise ValueError("schemaState must reflect schema probe checks")
        index_check = check_by_id["managed_indexes"]
        expected_index_state = {
            "passed": "in_sync",
            "failed": "drifted",
            "unavailable": "unavailable",
        }[index_check.state]
        if self.index_state != expected_index_state:
            raise ValueError("indexState must reflect the managed index check")
        freshness_check = check_by_id["data_freshness"]
        if (
            freshness_check.state == "passed"
            and self.freshness_target_seconds is not None
            and (
                self.observed_freshness_seconds is None
                or self.observed_freshness_seconds > self.freshness_target_seconds
            )
        ):
            raise ValueError(
                "a passed freshness check must satisfy the declared freshness target"
            )
        reconciliation_check = check_by_id["reconciliation_integrity"]
        if (
            reconciliation_check.state == "passed"
            and self.reconciliation_divergence_count != 0
        ):
            raise ValueError(
                "a passed reconciliation check requires zero observed divergence"
            )
        return self


class ImplementationPackageOperationalObservationRequest(ContractModel):
    contract: Literal["implementation-package-operational-observation-request/v1"]
    observation_request_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    expected_deployment_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    observed_by_ref: str = Field(min_length=1, max_length=256)
    observed_at: datetime

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ImplementationPackageOperationalObservationRequest":
        if self.expected_deployment_ref.kind != "implementation-package-deployment-receipt":
            raise ValueError(
                "expectedDeploymentRef.kind must be "
                "'implementation-package-deployment-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        return self


class ImplementationPackageOperationalObservationTriggerRequest(ContractModel):
    """One deterministic monitoring slot invoked by an external scheduler."""

    contract: Literal[
        "implementation-package-operational-observation-trigger-request/v1"
    ]
    schedule_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    expected_deployment_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    observed_by_ref: str = Field(min_length=1, max_length=256)
    scheduled_for: datetime

    @model_validator(mode="after")
    def trigger_is_coherent(
        self,
    ) -> "ImplementationPackageOperationalObservationTriggerRequest":
        if self.expected_deployment_ref.kind != "implementation-package-deployment-receipt":
            raise ValueError(
                "expectedDeploymentRef.kind must be "
                "'implementation-package-deployment-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        if self.scheduled_for.tzinfo is None or self.scheduled_for.utcoffset() is None:
            raise ValueError("scheduledFor must include a timezone")
        return self


class ImplementationPackageOperationalObservationReceipt(ContractModel):
    contract: Literal["implementation-package-operational-observation-receipt/v1"]
    artifact_ref: ArtifactRef
    observation_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    observation_request_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    expected_deployment_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    active_deployment_ref: Optional[ArtifactRef] = None
    active_package_ref: Optional[ArtifactRef] = None
    observed_by_ref: str = Field(min_length=1, max_length=256)
    observed_at: datetime
    monitoring_owner_ref: str = Field(min_length=1, max_length=256)
    checks: list[OperationalObservationCheck] = Field(min_length=11, max_length=11)
    strategy_probes: list[StrategyOperationalProbeEvidence] = Field(
        default_factory=list
    )
    health_state: Literal["healthy", "degraded", "unavailable"]
    drift_state: Literal["in_sync", "release_changed", "release_absent"]
    configuration_drift_state: Literal["in_sync", "drifted", "unavailable"]
    observation_scope: Literal["kehrnel_release_binding"]
    external_traffic_observation: Literal["not_observed"]
    evidence_persistence: Literal["durable"] = "durable"
    next_required_action: Literal[
        "continue_monitoring",
        "review_release_drift",
        "deploy_release_candidate",
        "investigate_strategy_health",
        "reconcile_strategy_drift",
    ]
    observation_digest: Sha256Digest

    @model_validator(mode="after")
    def receipt_is_coherent(self) -> "ImplementationPackageOperationalObservationReceipt":
        if self.artifact_ref.kind != "implementation-package-operational-observation-receipt":
            raise ValueError(
                "artifactRef.kind must be "
                "'implementation-package-operational-observation-receipt'"
            )
        if self.artifact_ref.digest != self.observation_digest:
            raise ValueError("artifactRef.digest must equal observationDigest")
        if self.expected_deployment_ref.kind != "implementation-package-deployment-receipt":
            raise ValueError(
                "expectedDeploymentRef.kind must be "
                "'implementation-package-deployment-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        if (self.active_deployment_ref is None) != (self.active_package_ref is None):
            raise ValueError(
                "activeDeploymentRef and activePackageRef must be present together"
            )
        if (
            self.active_deployment_ref is not None
            and self.active_deployment_ref.kind
            != "implementation-package-deployment-receipt"
        ):
            raise ValueError(
                "activeDeploymentRef.kind must be "
                "'implementation-package-deployment-receipt'"
            )
        if self.active_package_ref is not None and self.active_package_ref.kind != "implementation-package":
            raise ValueError("activePackageRef.kind must be 'implementation-package'")

        expected_ids = {
            "active_release_present",
            "expected_deployment_active",
            "expected_package_active",
            "deployment_artifact_integrity",
            "readiness_artifact_integrity",
            "preparation_artifact_integrity",
            "runtime_evidence_integrity",
            "strategy_runtime_health",
            "schema_index_drift",
            "data_freshness",
            "reconciliation_integrity",
        }
        if {check.check_id for check in self.checks} != expected_ids:
            raise ValueError("checks must contain every operational observation check once")
        all_passed = all(check.state == "passed" for check in self.checks)
        expected_health = (
            "unavailable"
            if self.drift_state == "release_absent"
            else "healthy"
            if all_passed and self.drift_state == "in_sync"
            else "degraded"
        )
        if self.health_state != expected_health:
            raise ValueError("healthState must reflect checks and driftState")
        if self.drift_state == "release_absent":
            if self.strategy_probes:
                raise ValueError("release_absent observations cannot contain strategy probes")
            expected_configuration_drift = "unavailable"
        elif not self.strategy_probes:
            expected_configuration_drift = "unavailable"
        elif any(probe.schema_state == "drifted" or probe.index_state == "drifted" for probe in self.strategy_probes):
            expected_configuration_drift = "drifted"
        elif any(probe.schema_state == "unavailable" or probe.index_state == "unavailable" for probe in self.strategy_probes):
            expected_configuration_drift = "unavailable"
        else:
            expected_configuration_drift = "in_sync"
        if self.configuration_drift_state != expected_configuration_drift:
            raise ValueError("configurationDriftState must reflect strategy probe evidence")
        if self.drift_state == "release_absent":
            expected_action = "deploy_release_candidate"
        elif self.drift_state == "release_changed":
            expected_action = "review_release_drift"
        elif self.configuration_drift_state == "drifted":
            expected_action = "reconcile_strategy_drift"
        elif not self.strategy_probes:
            expected_action = "investigate_strategy_health"
        elif any(probe.state != "healthy" for probe in self.strategy_probes):
            expected_action = "investigate_strategy_health"
        else:
            expected_action = "continue_monitoring"
        if self.next_required_action != expected_action:
            raise ValueError("nextRequiredAction must reflect driftState")
        return self


class ImplementationPackageOperationalObservationHistory(ContractModel):
    """Bounded, newest-first observation history for one exact deployment."""

    contract: Literal["implementation-package-operational-observation-history/v1"]
    environment_id: str = Field(
        min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$"
    )
    expected_deployment_ref: ArtifactRef
    expected_package_ref: ArtifactRef
    observations: list[ImplementationPackageOperationalObservationReceipt] = Field(
        max_length=50
    )
    total_count: int = Field(ge=0)
    returned_count: int = Field(ge=0, le=50)
    truncated: bool
    health_trend: Literal[
        "insufficient_history", "unchanged", "improved", "degraded"
    ]
    configuration_trend: Literal[
        "insufficient_history", "unchanged", "improved", "degraded"
    ]

    @model_validator(mode="after")
    def history_is_coherent(self) -> "ImplementationPackageOperationalObservationHistory":
        if self.expected_deployment_ref.kind != "implementation-package-deployment-receipt":
            raise ValueError(
                "expectedDeploymentRef.kind must be "
                "'implementation-package-deployment-receipt'"
            )
        if self.expected_package_ref.kind != "implementation-package":
            raise ValueError("expectedPackageRef.kind must be 'implementation-package'")
        if self.returned_count != len(self.observations):
            raise ValueError("returnedCount must equal the number of observations")
        if self.total_count < self.returned_count:
            raise ValueError("totalCount cannot be smaller than returnedCount")
        if self.truncated != (self.total_count > self.returned_count):
            raise ValueError("truncated must reflect totalCount and returnedCount")
        for observation in self.observations:
            if observation.environment_id != self.environment_id:
                raise ValueError("history observations must belong to the environment")
            if observation.expected_deployment_ref != self.expected_deployment_ref:
                raise ValueError("history observations must share the deployment baseline")
            if observation.expected_package_ref != self.expected_package_ref:
                raise ValueError("history observations must share the package baseline")
        if self.observations != sorted(
            self.observations,
            key=lambda observation: observation.observed_at,
            reverse=True,
        ):
            raise ValueError("history observations must be newest first")
        expected_health_trend = _trend(
            [observation.health_state for observation in self.observations],
            {"unavailable": 0, "degraded": 1, "healthy": 2},
        )
        expected_configuration_trend = _trend(
            [
                observation.configuration_drift_state
                for observation in self.observations
            ],
            {"drifted": 0, "unavailable": 1, "in_sync": 2},
        )
        if self.health_trend != expected_health_trend:
            raise ValueError("healthTrend must compare the latest two observations")
        if self.configuration_trend != expected_configuration_trend:
            raise ValueError(
                "configurationTrend must compare the latest two observations"
            )
        return self


def _trend(values: list[str], rank: dict[str, int]) -> str:
    if len(values) < 2:
        return "insufficient_history"
    change = rank[values[0]] - rank[values[1]]
    return "improved" if change > 0 else "degraded" if change < 0 else "unchanged"


__all__ = [
    "ImplementationPackageOperationalObservationHistory",
    "ImplementationPackageOperationalObservationReceipt",
    "ImplementationPackageOperationalObservationRequest",
    "ImplementationPackageOperationalObservationTriggerRequest",
    "OperationalObservationCheck",
    "OperationalObservationCheckId",
    "StrategyOperationalProbeCheck",
    "StrategyOperationalProbeCheckId",
    "StrategyOperationalProbeEvidence",
]
