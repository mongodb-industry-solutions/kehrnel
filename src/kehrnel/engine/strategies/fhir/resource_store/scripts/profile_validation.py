"""Optional, fail-closed FHIR profile-validation integration.

FHIR profile validation is delegated to a configured validation adapter rather
than approximated inside the persistence strategy.  Selecting profiles and
enforcing them are separate, explicit choices: FHIR Core remains usable with no
IG, while ``mode=required`` rejects writes unless the configured validator has
validated every resource governed by an active profile.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.fhir import implementation_guides

PROFILE_VALIDATION_CONTRACT_VERSION = "fhir-profile-validation.v1"
_ERROR_SEVERITIES = frozenset({"error", "fatal"})


def _config(config: dict[str, Any]) -> dict[str, Any]:
    ig = config.get("implementation_guides")
    if not isinstance(ig, dict):
        return {"mode": "disabled", "binding": "validation_engine"}
    validation = ig.get("profile_validation")
    if not isinstance(validation, dict):
        return {"mode": "disabled", "binding": "validation_engine"}
    return {
        "mode": str(validation.get("mode") or "disabled").strip().lower(),
        "binding": str(validation.get("binding") or "validation_engine").strip(),
        "fail_on_warning": bool(validation.get("fail_on_warning", False)),
    }


def validate_profile_config(
    config: dict[str, Any], adapters: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Validate cross-field profile-enforcement configuration."""

    validation = _config(config)
    if validation["mode"] not in {"disabled", "required"}:
        raise ValueError(
            "implementation_guides.profile_validation.mode must be 'disabled' or 'required'"
        )
    if not validation["binding"]:
        raise ValueError("implementation_guides.profile_validation.binding is required")

    active_profiles = implementation_guides.resolve_active_profiles(config)
    if validation["mode"] == "required" and not active_profiles:
        raise ValueError(
            "Profile validation cannot be required without at least one active profile"
        )
    if adapters is not None and validation["mode"] == "required":
        adapter = adapters.get(validation["binding"])
        if adapter is None or not callable(getattr(adapter, "validate", None)):
            raise ValueError(
                "Required FHIR profile validation adapter is not available at binding "
                f"{validation['binding']!r}"
            )
    return validation


def describe_profile_validation(
    config: dict[str, Any], adapters: dict[str, Any] | None = None
) -> dict[str, Any]:
    validation = validate_profile_config(config, None)
    active_profiles = implementation_guides.resolve_active_profiles(config)
    adapter_available = bool(
        adapters
        and callable(getattr(adapters.get(validation["binding"]), "validate", None))
    )
    return {
        "contract_version": PROFILE_VALIDATION_CONTRACT_VERSION,
        "mode": validation["mode"],
        "binding": validation["binding"],
        "active_profile_count": len(active_profiles),
        "adapter_available": adapter_available,
        "enforced": validation["mode"] == "required" and adapter_available,
        "fail_on_warning": validation["fail_on_warning"],
    }


def _normalize_adapter_findings(
    findings: list[Any], resources: list[dict[str, Any]], *, fail_on_warning: bool
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for raw in findings:
        if not isinstance(raw, dict):
            continue
        raw_index = raw.get("index", raw.get("dataset_index", 0))
        try:
            index = int(raw_index)
        except (TypeError, ValueError):
            index = 0
        if not 0 <= index < len(resources):
            index = 0
        resource = resources[index]
        severity = str(raw.get("severity") or "error").strip().lower()
        if fail_on_warning and severity == "warning":
            severity = "error"
        item: dict[str, Any] = {
            "index": index,
            "severity": severity,
            "code": str(raw.get("code") or "FHIR_PROFILE_VALIDATION_FINDING"),
            "message": str(
                raw.get("message")
                or raw.get("details")
                or "FHIR profile validation finding"
            ),
            "resource_type": resource.get("resourceType"),
            "resource_id": resource.get("id"),
            "source": "profile-validator",
        }
        path = raw.get("path") or raw.get("location")
        if path:
            item["path"] = str(path)
        profile = raw.get("profile")
        if profile:
            item["profile"] = str(profile)
        normalized.append(item)
    return normalized


async def validate_profiles(
    ctx: StrategyContext,
    config: dict[str, Any],
    resources: list[dict[str, Any]],
    *,
    resource_indexes: list[int] | None = None,
) -> dict[str, Any]:
    """Validate the active profile subset and return persistence findings."""

    validation = validate_profile_config(config, None)
    active_profiles = implementation_guides.resolve_active_profiles(config)
    source_indexes = resource_indexes or list(range(len(resources)))
    if len(source_indexes) != len(resources):
        raise ValueError("resource_indexes must align with resources")
    base_report: dict[str, Any] = {
        "contract_version": PROFILE_VALIDATION_CONTRACT_VERSION,
        "mode": validation["mode"],
        "binding": validation["binding"],
        "active_profiles": [item.get("url") for item in active_profiles],
        "enforced": False,
        "checked": 0,
        "passed": 0,
        "failed": 0,
        "failed_resource_indexes": [],
        "findings": [],
    }
    if validation["mode"] == "disabled" or not active_profiles:
        return base_report

    adapter = (ctx.adapters or {}).get(validation["binding"])
    if adapter is None or not callable(getattr(adapter, "validate", None)):
        raise KehrnelError(
            code="FHIR_PROFILE_VALIDATOR_UNAVAILABLE",
            status=503,
            message="Profile enforcement is required but its validation adapter is unavailable",
            details={"binding": validation["binding"]},
        )

    profiles_by_type: dict[str, set[str]] = defaultdict(set)
    for profile in active_profiles:
        resource_type = str(profile.get("type") or "").strip()
        url = str(profile.get("url") or "").strip()
        if resource_type and url:
            profiles_by_type[resource_type].add(url)

    checked_resources: list[dict[str, Any]] = []
    checked_indexes: list[int] = []
    preflight_findings: list[dict[str, Any]] = []
    for position, resource in enumerate(resources):
        index = source_indexes[position]
        resource_type = str(resource.get("resourceType") or "")
        required = profiles_by_type.get(resource_type)
        if not required:
            continue
        checked_resources.append(resource)
        checked_indexes.append(index)
        declared = {
            str(value)
            for value in ((resource.get("meta") or {}).get("profile") or [])
            if str(value)
        }
        if required.isdisjoint(declared):
            preflight_findings.append(
                {
                    "index": index,
                    "severity": "error",
                    "code": "FHIR_ACTIVE_PROFILE_NOT_DECLARED",
                    "message": (
                        f"{resource_type}/{resource.get('id')} must declare one of the active "
                        "profiles in meta.profile"
                    ),
                    "resource_type": resource_type,
                    "resource_id": resource.get("id"),
                    "required_profiles": sorted(required),
                    "source": "kehrnel-profile-preflight",
                }
            )

    if preflight_findings:
        failed_indexes = {item["index"] for item in preflight_findings}
    else:
        failed_indexes = set()

    adapter_findings: list[dict[str, Any]] = []
    if checked_resources:
        inspected = implementation_guides.inspect_configured_implementation_guides(
            config
        )
        ig_config = config.get("implementation_guides") or {}
        package_sources = [
            {"source": item.get("source"), "sha256": item.get("sha256")}
            for item in (ig_config.get("packages") or [])
            if isinstance(item, dict) and item.get("enabled", True)
        ]
        result = await adapter.validate(
            snapshot={
                "domain": "fhir",
                "contract_version": PROFILE_VALIDATION_CONTRACT_VERSION,
                "fhir_release": config.get("schema_version"),
                "packages": [item.get("package") for item in inspected],
                "package_sources": package_sources,
                "active_profiles": active_profiles,
            },
            datasets=[
                {
                    "name": f"{item.get('resourceType')}/{item.get('id')}",
                    "resource": item,
                }
                for item in checked_resources
            ],
            options={
                "profile_urls": [item.get("url") for item in active_profiles],
                "fail_on_warning": validation["fail_on_warning"],
            },
        )
        raw_findings = result.get("findings") if isinstance(result, dict) else None
        adapter_findings = _normalize_adapter_findings(
            raw_findings if isinstance(raw_findings, list) else [],
            checked_resources,
            fail_on_warning=validation["fail_on_warning"],
        )
        for finding in adapter_findings:
            local_index = int(finding["index"])
            finding["index"] = checked_indexes[local_index]
            if finding["severity"] in _ERROR_SEVERITIES:
                failed_indexes.add(checked_indexes[local_index])

    findings = preflight_findings + adapter_findings
    return {
        **base_report,
        "enforced": True,
        "checked": len(checked_resources),
        "passed": len(checked_resources) - len(failed_indexes),
        "failed": len(failed_indexes),
        "failed_resource_indexes": sorted(failed_indexes),
        "findings": findings,
    }
