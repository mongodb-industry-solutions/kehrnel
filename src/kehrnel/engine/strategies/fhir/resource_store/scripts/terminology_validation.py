"""Tenant-routed terminology validation for canonical FHIR resources.

This is deliberately independent from structural/profile validation. It only
validates Coding-shaped values for explicitly configured systems, and records
the provider/release evidence returned by the shared terminology gateway.
"""

from __future__ import annotations

from typing import Any

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.fhir import implementation_guides

CONTRACT_VERSION = "fhir-terminology-validation.v1"
_MODES = {"disabled", "advisory", "required"}


def _config(config: dict[str, Any]) -> dict[str, Any]:
    raw = config.get("terminology_validation")
    if not isinstance(raw, dict):
        raw = {}
    return {
        "mode": str(raw.get("mode") or "disabled").strip().lower(),
        "binding": str(raw.get("binding") or "terminology").strip(),
        "systems": sorted(
            {str(value).strip() for value in raw.get("systems") or [] if str(value).strip()}
        ),
        "max_codings_per_resource": max(
            1, min(int(raw.get("max_codings_per_resource") or 100), 1000)
        ),
    }


def validate_config(
    config: dict[str, Any], adapters: dict[str, Any] | None = None
) -> dict[str, Any]:
    settings = _config(config)
    if settings["mode"] not in _MODES:
        raise ValueError("terminology_validation.mode must be disabled, advisory, or required")
    if not settings["binding"]:
        raise ValueError("terminology_validation.binding is required")
    if settings["mode"] != "disabled" and not settings["systems"]:
        raise ValueError(
            "terminology_validation.systems must name at least one canonical system when validation is enabled"
        )
    if adapters is not None and settings["mode"] == "required":
        adapter = adapters.get(settings["binding"])
        if adapter is None or not callable(getattr(adapter, "execute", None)):
            raise ValueError(
                "Required terminology adapter is not available at binding "
                f"{settings['binding']!r}"
            )
    return settings


def describe(
    config: dict[str, Any], adapters: dict[str, Any] | None = None
) -> dict[str, Any]:
    settings = validate_config(config, None)
    adapter_available = bool(
        adapters
        and callable(getattr(adapters.get(settings["binding"]), "execute", None))
    )
    binding_rules = _active_binding_rules(config)
    return {
        "contract_version": CONTRACT_VERSION,
        **settings,
        "adapter_available": adapter_available,
        "enforced": settings["mode"] == "required" and adapter_available,
        "active_binding_count": len(binding_rules),
        "required_binding_count": sum(
            1 for rule in binding_rules if rule.get("strength") == "required"
        ),
    }


def _coding_values(
    value: Any,
    *,
    systems: set[str] | None,
    path: str = "",
    limit: int,
) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def visit(item: Any, current_path: str) -> None:
        if len(found) >= limit:
            return
        if isinstance(item, list):
            for index, child in enumerate(item):
                visit(child, f"{current_path}[{index}]")
            return
        if not isinstance(item, dict):
            return
        system = str(item.get("system") or "").strip()
        code = str(item.get("code") or "").strip()
        if code and (systems is None or system in systems):
            coding = {"code": code, "path": current_path or "$"}
            if system:
                coding["system"] = system
            for key in ("version", "display"):
                if item.get(key) is not None:
                    coding[key] = item[key]
            found.append(coding)
        for key, child in item.items():
            next_path = f"{current_path}.{key}" if current_path else str(key)
            visit(child, next_path)

    visit(value, path)
    return found


def _active_binding_rules(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Resolve bindings only from profiles explicitly selected by the tenant."""

    active_profiles = implementation_guides.resolve_active_profiles(config)
    active_urls = {
        str(profile.get("url") or "").strip()
        for profile in active_profiles
        if str(profile.get("url") or "").strip()
    }
    if not active_urls:
        return []
    inspected = implementation_guides.inspect_configured_implementation_guides(config)
    rules: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for package in inspected:
        for binding in (package.get("inventory") or {}).get("terminology_bindings") or []:
            profile_url = str(binding.get("profile_url") or "").strip()
            if profile_url not in active_urls:
                continue
            key = (
                profile_url,
                str(binding.get("element_id") or binding.get("path") or ""),
                str(binding.get("value_set") or ""),
                str(binding.get("value_set_version") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            rules.append(dict(binding))
    return sorted(
        rules,
        key=lambda item: (
            str(item.get("resource_type") or ""),
            str(item.get("path") or item.get("element_id") or ""),
            str(item.get("value_set") or ""),
        ),
    )


def _bound_codings(
    resource: dict[str, Any],
    rule: dict[str, Any],
    *,
    systems: set[str],
    limit: int,
) -> list[dict[str, Any]]:
    """Find Coding values below a simple StructureDefinition element path."""

    resource_type = str(resource.get("resourceType") or "")
    declared_path = str(rule.get("path") or rule.get("element_id") or "").strip()
    parts = declared_path.split(".") if declared_path else []
    if parts and parts[0].split(":", 1)[0] == resource_type:
        parts = parts[1:]
    nodes: list[tuple[Any, str]] = [(resource, "")]
    for raw_part in parts:
        part = raw_part.split(":", 1)[0]
        expanded: list[tuple[Any, str]] = []
        choice_prefix = part[:-3] if part.endswith("[x]") else None
        for node, node_path in nodes:
            if not isinstance(node, dict):
                continue
            keys = (
                [key for key in node if key == choice_prefix or key.startswith(choice_prefix or "\0")]
                if choice_prefix is not None
                else [part]
            )
            for key in keys:
                if key not in node:
                    continue
                value = node[key]
                path = f"{node_path}.{key}" if node_path else key
                if isinstance(value, list):
                    expanded.extend((item, f"{path}[{index}]") for index, item in enumerate(value))
                else:
                    expanded.append((value, path))
        nodes = expanded
        if not nodes:
            break
    found: list[dict[str, Any]] = []
    for value, path in nodes:
        if isinstance(value, str) and value.strip():
            found.append({"code": value.strip(), "path": path})
            if len(found) >= limit:
                break
            continue
        found.extend(
            _coding_values(
                value,
                systems=None,
                path=path,
                limit=max(0, limit - len(found)),
            )
        )
        if len(found) >= limit:
            break
    return found


async def validate_resources(
    ctx: StrategyContext,
    config: dict[str, Any],
    resources: list[dict[str, Any]],
    *,
    resource_indexes: list[int] | None = None,
) -> dict[str, Any]:
    settings = validate_config(config, None)
    source_indexes = resource_indexes or list(range(len(resources)))
    if len(source_indexes) != len(resources):
        raise ValueError("resource_indexes must align with resources")
    report: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "mode": settings["mode"],
        "systems": settings["systems"],
        "enforced": False,
        "checked": 0,
        "valid": 0,
        "invalid": 0,
        "failed_resource_indexes": [],
        "findings": [],
        "evidence": [],
        "binding_checks": 0,
        "binding_passed": 0,
        "binding_failed": 0,
    }
    if settings["mode"] == "disabled":
        return report

    adapter = (ctx.adapters or {}).get(settings["binding"])
    if adapter is None or not callable(getattr(adapter, "execute", None)):
        raise KehrnelError(
            code="FHIR_TERMINOLOGY_VALIDATOR_UNAVAILABLE",
            status=503,
            message="FHIR terminology validation is enabled but the tenant terminology gateway is unavailable",
            details={"binding": settings["binding"]},
        )

    failed_indexes: set[int] = set()
    severity = "error" if settings["mode"] == "required" else "warning"
    systems = set(settings["systems"])
    binding_rules = _active_binding_rules(config)
    bound_locations: set[tuple[int, str, str, str]] = set()
    for position, resource in enumerate(resources):
        input_index = source_indexes[position]
        for rule in binding_rules:
            if str(rule.get("resource_type") or "") != str(resource.get("resourceType") or ""):
                continue
            codings = _bound_codings(
                resource,
                rule,
                systems=systems,
                limit=settings["max_codings_per_resource"],
            )
            if not codings:
                continue
            report["binding_checks"] += 1
            binding_results: list[dict[str, Any]] = []
            binding_errors: list[KehrnelError] = []
            for coding in codings:
                bound_locations.add(
                    (
                        input_index,
                        coding["path"],
                        str(coding.get("system") or ""),
                        coding["code"],
                    )
                )
                report["checked"] += 1
                request = {
                    key: coding[key]
                    for key in ("system", "version", "code", "display")
                    if key in coding
                }
                request["url"] = rule["value_set"]
                if rule.get("value_set_version"):
                    request["value_set_version"] = rule["value_set_version"]
                try:
                    result = await adapter.execute("validate-code", request)
                    binding_results.append(result)
                    report["evidence"].append(
                        {
                            "index": input_index,
                            "path": coding["path"],
                            "system": coding.get("system"),
                            "code": coding["code"],
                            "valueSet": rule["value_set"],
                            "valueSetVersion": rule.get("value_set_version"),
                            "bindingStrength": rule.get("strength"),
                            "profile": rule.get("profile_url"),
                            "valid": bool(result.get("valid")),
                            "provider": result.get("provider"),
                            "version": result.get("version"),
                            "releaseId": result.get("releaseId"),
                        }
                    )
                except KehrnelError as exc:
                    binding_errors.append(exc)
            if any(bool(result.get("valid")) for result in binding_results):
                report["binding_passed"] += 1
                report["valid"] += 1
                continue
            report["binding_failed"] += 1
            report["invalid"] += 1
            binding_severity = (
                "error"
                if settings["mode"] == "required" and rule.get("strength") == "required"
                else "warning"
            )
            if binding_severity == "error":
                failed_indexes.add(input_index)
            first_error = binding_errors[0] if binding_errors else None
            report["findings"].append(
                {
                    "index": input_index,
                    "severity": binding_severity,
                    "code": (
                        first_error.code
                        if first_error is not None
                        else "FHIR_TERMINOLOGY_BINDING_NOT_SATISFIED"
                    ),
                    "message": (
                        str(first_error)
                        if first_error is not None
                        else f"No coding at {rule.get('path') or rule.get('element_id')} satisfies {rule['value_set']}"
                    ),
                    "path": rule.get("path") or rule.get("element_id"),
                    "resource_type": resource.get("resourceType"),
                    "resource_id": resource.get("id"),
                    "profile": rule.get("profile_url"),
                    "binding_strength": rule.get("strength"),
                    "value_set": rule.get("value_set"),
                    "value_set_version": rule.get("value_set_version"),
                    "source": "terminology-gateway",
                    **({"details": first_error.details} if first_error is not None else {}),
                }
            )
        codings = _coding_values(
            resource,
            systems=systems,
            limit=settings["max_codings_per_resource"],
        )
        for coding in codings:
            if (input_index, coding["path"], coding["system"], coding["code"]) in bound_locations:
                continue
            report["checked"] += 1
            request = {key: coding[key] for key in ("system", "version", "code", "display") if key in coding}
            try:
                result = await adapter.execute("validate-code", request)
                evidence = {
                    "index": input_index,
                    "path": coding["path"],
                    "system": coding["system"],
                    "code": coding["code"],
                    "valid": bool(result.get("valid")),
                    "provider": result.get("provider"),
                    "version": result.get("version"),
                    "releaseId": result.get("releaseId"),
                }
                report["evidence"].append(evidence)
                if evidence["valid"]:
                    report["valid"] += 1
                    continue
                report["invalid"] += 1
                failed_indexes.add(input_index)
                report["findings"].append(
                    {
                        "index": input_index,
                        "severity": severity,
                        "code": "FHIR_TERMINOLOGY_CODE_INVALID",
                        "message": result.get("message") or f"Coding {coding['system']}|{coding['code']} is not valid",
                        "path": coding["path"],
                        "resource_type": resource.get("resourceType"),
                        "resource_id": resource.get("id"),
                        "provider": result.get("provider"),
                        "source": "terminology-gateway",
                    }
                )
            except KehrnelError as exc:
                report["invalid"] += 1
                failed_indexes.add(input_index)
                report["findings"].append(
                    {
                        "index": input_index,
                        "severity": severity,
                        "code": exc.code,
                        "message": str(exc),
                        "path": coding["path"],
                        "resource_type": resource.get("resourceType"),
                        "resource_id": resource.get("id"),
                        "source": "terminology-gateway",
                        "details": exc.details,
                    }
                )

    report["enforced"] = settings["mode"] == "required"
    report["failed_resource_indexes"] = sorted(failed_indexes)
    return report
