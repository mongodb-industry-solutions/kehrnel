"""Assess tenant readiness before an Implementation Package may prepare data."""

from __future__ import annotations

import hashlib
from typing import Any

from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.implementation_package_preparation import (
    ImplementationPackagePreparationReadiness,
    ImplementationPackagePreparationReadinessRequest,
    StrategyActivationInput,
    StrategyPreparationRequirement,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.implementation_artifact_store import (
    FileImplementationArtifactStore,
)
from kehrnel.engine.core.config import validate_config


class ImplementationPackagePreparationError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 422):
        self.code = code
        self.status = status
        super().__init__(message)


def _digest(payload: dict) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


_SECRET_KEY_PARTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "connection_string",
    "connectionstring",
)


def _contains_secret_like_key(value: Any) -> bool:
    if isinstance(value, dict):
        for key, nested in value.items():
            normalized = str(key).strip().lower()
            if (
                normalized == "uri"
                or normalized.endswith("_uri")
                or any(part in normalized for part in _SECRET_KEY_PARTS)
            ):
                return True
            if _contains_secret_like_key(nested):
                return True
    elif isinstance(value, list):
        return any(_contains_secret_like_key(item) for item in value)
    return False


def _require_exact_materialization(
    environment_id: str,
    request: ImplementationPackagePreparationReadinessRequest,
    store: FileImplementationArtifactStore,
):
    receipt = store.get_materialization_receipt(request.materialization_receipt_ref)
    if receipt.environment_id != environment_id:
        raise ImplementationPackagePreparationError(
            "IMPLEMENTATION_PACKAGE_MATERIALIZATION_SCOPE_MISMATCH",
            "The materialization receipt belongs to a different environment.",
            status=409,
        )
    if receipt.package_ref != request.expected_package_ref:
        raise ImplementationPackagePreparationError(
            "IMPLEMENTATION_PACKAGE_PRECONDITION_FAILED",
            "The materialized package differs from the expected package.",
            status=409,
        )
    if not receipt.artifact_closure_complete:
        raise ImplementationPackagePreparationError(
            "IMPLEMENTATION_PACKAGE_ARTIFACT_CLOSURE_INCOMPLETE",
            "Preparation readiness requires a complete materialized artifact closure.",
            status=409,
        )
    package = store.get_package(receipt.package_ref)
    assembly = store.find_package_assembly(
        package.artifact_ref.id, package.artifact_ref.version
    )
    if assembly.package_ref != package.artifact_ref:
        raise ImplementationPackagePreparationError(
            "IMPLEMENTATION_PACKAGE_ASSEMBLY_MISMATCH",
            "The stored package assembly belongs to a different package.",
            status=409,
        )
    return receipt, package, assembly


def activation_input_digest(
    item: StrategyActivationInput,
    *,
    bindings_ref: str,
) -> str:
    return _digest(
        {
            "strategyInstanceId": item.strategy_instance_id,
            "strategyRef": item.strategy_ref.to_wire(),
            "domain": item.domain,
            "configuration": item.configuration,
            "bindingsRef": bindings_ref,
            "approved": item.approved,
        }
    )


def assess_implementation_package_preparation_readiness(
    environment_id: str,
    request: ImplementationPackagePreparationReadinessRequest,
    *,
    store: FileImplementationArtifactStore,
    runtime,
) -> ImplementationPackagePreparationReadiness:
    """Fail closed on missing tenant activation inputs without executing work."""

    receipt, package, assembly = _require_exact_materialization(
        environment_id, request, store
    )
    environment = runtime.registry.get_environment(environment_id)
    if environment is None:
        raise ImplementationPackagePreparationError(
            "IMPLEMENTATION_ENVIRONMENT_NOT_FOUND",
            f"Environment '{environment_id}' does not exist.",
            status=404,
        )
    resolved = assembly.resolution.resolved_plan
    preparation_plan_ref = assembly.resolution.preparation_plan.artifact_ref
    expected = {
        item.strategy_instance_id: item for item in resolved.strategy_instances
    }
    supplied = {item.strategy_instance_id: item for item in request.activation_inputs}
    unexpected = sorted(set(supplied) - set(expected))
    if unexpected:
        raise ImplementationPackagePreparationError(
            "IMPLEMENTATION_PACKAGE_ACTIVATION_INPUT_UNKNOWN",
            "Activation inputs reference strategy instances outside the package: "
            + ", ".join(unexpected),
        )

    requirements: list[StrategyPreparationRequirement] = []
    for instance_id in sorted(expected):
        strategy = expected[instance_id]
        item = supplied.get(instance_id)
        missing: list[str] = []
        bindings_ref = ""
        activation_digest = None
        configuration_keys: list[str] = []
        validation_errors: list[str] = []
        if item is None:
            missing.append("activation_input")
        else:
            if item.strategy_ref != strategy.strategy_ref:
                missing.append("strategy_reference")
            if not item.domain.strip():
                missing.append("domain")
            configuration_keys = sorted(item.configuration)
            manifest = runtime.registry.get_manifest(strategy.strategy_ref.id)
            if manifest is None or manifest.version != strategy.strategy_ref.version:
                missing.append("strategy_reference")
            elif runtime._uses_mongodb(manifest):
                database = item.configuration.get("database") or item.configuration.get(
                    "database_name"
                )
                if not isinstance(database, str) or not database.strip():
                    missing.append("database")
            if _contains_secret_like_key(item.configuration):
                raise ImplementationPackagePreparationError(
                    "IMPLEMENTATION_PACKAGE_SECRET_IN_CONFIGURATION",
                    "Activation configuration must not contain credentials or connection "
                    "URIs. Store them behind bindingsRef.",
                )
            bindings_ref = (
                (item.bindings_ref or "").strip()
                or (getattr(environment, "bindings_ref", None) or "").strip()
            )
            if not bindings_ref:
                missing.append("bindings_ref")
            if not item.approved:
                missing.append("approval")
            if manifest is not None and "database" not in missing:
                try:
                    merged = {}
                    defaults = manifest.default_config or runtime._load_defaults_from_entrypoint(
                        manifest
                    )
                    if defaults:
                        merged = runtime._deep_merge(merged, defaults)
                    merged = runtime._deep_merge(merged, item.configuration)
                    schema = (
                        manifest.config_schema
                        or runtime._load_schema_from_entrypoint(manifest)
                        or {}
                    )
                    validate_config(schema, merged)
                    runtime._validate_pack_config(manifest, merged)
                except Exception as exc:
                    validation_errors.append(str(exc))
            if validation_errors:
                missing.append("configuration")
            if not missing:
                activation_digest = activation_input_digest(
                    item, bindings_ref=bindings_ref
                )
        requirements.append(
            StrategyPreparationRequirement(
                strategyInstanceId=instance_id,
                strategyRef=strategy.strategy_ref,
                state="blocked" if missing else "ready",
                activationSpecDigest=activation_digest,
                bindingsRef=bindings_ref or None,
                configurationKeys=configuration_keys,
                validationErrors=validation_errors,
                missingInputs=missing,
            )
        )

    blocked = any(item.state == "blocked" for item in requirements)
    state = "blocked" if blocked else "ready"
    deferred_stage_ids = sorted(
        stage.stage_id for stage in assembly.resolution.preparation_plan.stages
    )
    identity = _digest(
        {
            "environmentId": environment_id,
            "materializationReceiptRef": receipt.artifact_ref.to_wire(),
            "packageRef": package.artifact_ref.to_wire(),
            "preparationPlanRef": preparation_plan_ref.to_wire(),
            "state": state,
            "strategyRequirements": [item.to_wire() for item in requirements],
            "deferredStageIds": deferred_stage_ids,
        }
    )
    readiness_id = "package-preparation-readiness." + identity.removeprefix("sha256:")
    payload = {
        "contract": "implementation-package-preparation-readiness/v1",
        "readinessId": readiness_id,
        "environmentId": environment_id,
        "materializationReceiptRef": receipt.artifact_ref.to_wire(),
        "packageRef": package.artifact_ref.to_wire(),
        "preparationPlanRef": preparation_plan_ref.to_wire(),
        "state": state,
        "strategyRequirements": [item.to_wire() for item in requirements],
        "deferredStageIds": deferred_stage_ids,
        "preparationExecuted": False,
        "runtimeExecuted": False,
        "nextRequiredAction": (
            "provide_activation_inputs" if blocked else "activate_strategies"
        ),
    }
    readiness_digest = _digest(payload)
    readiness = ImplementationPackagePreparationReadiness(
        **payload,
        artifactRef=ArtifactRef(
            kind="implementation-package-preparation-readiness",
            id=readiness_id,
            version=package.artifact_ref.version,
            digest=readiness_digest,
        ),
        readinessDigest=readiness_digest,
    )
    store.put_preparation_readiness(readiness)
    return readiness


__all__ = [
    "ImplementationPackagePreparationError",
    "activation_input_digest",
    "assess_implementation_package_preparation_readiness",
]
