"""Tenant-scoped terminology routing across native and external providers.

The gateway deliberately owns routing, normalization, and evidence only. Native
terminology semantics remain in the strategy that owns the data model; remote
FHIR terminology semantics remain in the configured external server.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse
from uuid import uuid4

from kehrnel.engine.core.errors import KehrnelError

SNOMED_SYSTEM_URI = "http://snomed.info/sct"
_PROVIDER_TYPES = {"kehrnel-strategy", "fhir-terminology"}
_NATIVE_OPERATIONS = {"capabilities", "lookup", "validate-code", "subsumes", "expand", "search", "ecl", "ground"}
_FHIR_OPERATIONS = {"lookup", "validate-code", "subsumes", "expand", "translate"}
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_PROVIDER_RESPONSE_BYTES = 8 * 1024 * 1024


def _digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _clean_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _validate_external_url(provider: dict[str, Any]) -> str:
    base_url = str(provider.get("base_url") or provider.get("baseUrl") or "").strip().rstrip("/")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        raise KehrnelError(
            code="TERMINOLOGY_PROVIDER_CONFIG_INVALID",
            status=400,
            message=f"Provider {provider.get('id')!r} requires an absolute http(s) base_url without embedded credentials.",
        )
    if parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"} and not provider.get("allow_insecure_http"):
        raise KehrnelError(
            code="TERMINOLOGY_PROVIDER_CONFIG_INVALID",
            status=400,
            message=f"Provider {provider.get('id')!r} must use HTTPS unless allow_insecure_http is explicitly enabled.",
        )
    return base_url


def _normalize_auth(value: Any, provider_id: str) -> dict[str, Any] | None:
    if value in (None, {}):
        return None
    if not isinstance(value, dict):
        raise KehrnelError(
            code="TERMINOLOGY_PROVIDER_CONFIG_INVALID",
            status=400,
            message=f"Provider {provider_id!r} auth must be an object.",
        )
    auth_type = str(value.get("type") or "").strip().lower()
    if auth_type not in {"bearer-env", "api-key-env", "bearer-hdl", "api-key-hdl"}:
        raise KehrnelError(
            code="TERMINOLOGY_PROVIDER_CONFIG_INVALID",
            status=400,
            message=(
                f"Provider {provider_id!r} auth.type must be bearer-env, api-key-env, "
                "bearer-hdl, or api-key-hdl."
            ),
        )
    normalized = {"type": auth_type}
    if auth_type.endswith("-env"):
        environment_variable = str(value.get("environment_variable") or value.get("environmentVariable") or "").strip()
        if not _ENV_NAME.match(environment_variable):
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_CONFIG_INVALID",
                status=400,
                message=f"Provider {provider_id!r} auth requires a valid environment_variable reference.",
            )
        normalized["environment_variable"] = environment_variable
    else:
        secret_ref = str(value.get("secret_ref") or value.get("secretRef") or provider_id).strip()
        if secret_ref != provider_id:
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_CONFIG_INVALID",
                status=400,
                message=f"Provider {provider_id!r} auth.secret_ref must match its provider id.",
            )
        normalized["secret_ref"] = secret_ref
    if auth_type.startswith("api-key-"):
        header = str(value.get("header") or "X-API-Key").strip()
        if not re.match(r"^[A-Za-z0-9-]+$", header):
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_CONFIG_INVALID",
                status=400,
                message=f"Provider {provider_id!r} auth.header is invalid.",
            )
        normalized["header"] = header
    return normalized


def normalize_terminology_config(value: Any) -> dict[str, Any]:
    """Validate the non-secret terminology registry stored in environment metadata."""
    if value in (None, {}):
        return {"providers": [], "routes": []}
    if not isinstance(value, dict):
        raise KehrnelError(code="TERMINOLOGY_CONFIG_INVALID", status=400, message="terminology configuration must be an object")
    providers: list[dict[str, Any]] = []
    provider_ids: set[str] = set()
    for raw in value.get("providers") or []:
        if not isinstance(raw, dict):
            raise KehrnelError(code="TERMINOLOGY_PROVIDER_CONFIG_INVALID", status=400, message="Each terminology provider must be an object.")
        forbidden_secret_fields = {
            key for key in raw if str(key).lower() in {"secret", "token", "api_key", "apikey", "authorization", "headers"}
        }
        if forbidden_secret_fields:
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_CONFIG_INVALID",
                status=400,
                message="Terminology provider credentials must be referenced through auth, not stored in environment metadata.",
                details={"forbidden_fields": sorted(forbidden_secret_fields)},
            )
        provider_id = str(raw.get("id") or "").strip()
        provider_type = str(raw.get("type") or "").strip().lower()
        if not provider_id or not re.match(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$", provider_id):
            raise KehrnelError(code="TERMINOLOGY_PROVIDER_CONFIG_INVALID", status=400, message="Each terminology provider requires a stable id.")
        if provider_id in provider_ids:
            raise KehrnelError(code="TERMINOLOGY_PROVIDER_CONFIG_INVALID", status=400, message=f"Duplicate terminology provider id {provider_id!r}.")
        if provider_type not in _PROVIDER_TYPES:
            raise KehrnelError(code="TERMINOLOGY_PROVIDER_CONFIG_INVALID", status=400, message=f"Unsupported terminology provider type {provider_type!r}.")
        systems = _clean_list(raw.get("systems"))
        operations = _clean_list(raw.get("operations"))
        allowed_operations = (
            _NATIVE_OPERATIONS
            if provider_type == "kehrnel-strategy"
            else _FHIR_OPERATIONS
        )
        unknown_operations = sorted(set(operations) - allowed_operations)
        if unknown_operations:
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_CONFIG_INVALID",
                status=400,
                message=f"Provider {provider_id!r} declares unsupported operations.",
                details={"unsupported_operations": unknown_operations},
            )
        normalized: dict[str, Any] = {
            "id": provider_id,
            "type": provider_type,
            "label": str(raw.get("label") or provider_id).strip(),
            "systems": systems,
            "operations": operations or sorted(allowed_operations),
            "enabled": bool(raw.get("enabled", True)),
        }
        if provider_type == "kehrnel-strategy":
            domain = str(raw.get("domain") or "snomedct").strip().lower()
            if domain != "snomedct":
                raise KehrnelError(
                    code="TERMINOLOGY_PROVIDER_CONFIG_INVALID",
                    status=400,
                    message="The current native terminology provider supports domain 'snomedct' only.",
                )
            normalized["domain"] = domain
            normalized["systems"] = systems or [SNOMED_SYSTEM_URI]
        else:
            normalized["base_url"] = _validate_external_url(raw)
            normalized["timeout_seconds"] = max(1.0, min(float(raw.get("timeout_seconds") or 20), 120.0))
            auth = _normalize_auth(raw.get("auth"), provider_id)
            if auth:
                normalized["auth"] = auth
            normalized["allow_insecure_http"] = bool(raw.get("allow_insecure_http", False))
        providers.append(normalized)
        provider_ids.add(provider_id)

    routes: list[dict[str, Any]] = []
    route_selectors: dict[tuple[str, str, str], str] = {}
    for raw in value.get("routes") or []:
        if not isinstance(raw, dict):
            raise KehrnelError(code="TERMINOLOGY_CONFIG_INVALID", status=400, message="Each terminology route must be an object.")
        provider_id = str(raw.get("provider_id") or raw.get("providerId") or "").strip()
        system = str(raw.get("system") or "").strip()
        value_set = str(raw.get("value_set") or raw.get("valueSet") or "").strip()
        if (not system and not value_set) or provider_id not in provider_ids:
            raise KehrnelError(
                code="TERMINOLOGY_CONFIG_INVALID",
                status=400,
                message="Each terminology route requires a system or value_set and an existing provider_id.",
            )
        route = {"provider_id": provider_id}
        if system:
            route["system"] = system
        if raw.get("version"):
            route["version"] = str(raw["version"]).strip()
        if value_set:
            route["value_set"] = value_set
        selector = (
            str(route.get("value_set") or ""),
            str(route.get("system") or ""),
            str(route.get("version") or ""),
        )
        existing_provider = route_selectors.get(selector)
        if existing_provider and existing_provider != provider_id:
            raise KehrnelError(
                code="TERMINOLOGY_ROUTE_AMBIGUOUS",
                status=409,
                message="A terminology route selector cannot point to more than one provider.",
                details={
                    "selector": {
                        "value_set": selector[0] or None,
                        "system": selector[1] or None,
                        "version": selector[2] or None,
                    },
                    "providers": [existing_provider, provider_id],
                },
            )
        if existing_provider == provider_id:
            continue
        route_selectors[selector] = provider_id
        routes.append(route)

    default_provider_id = str(value.get("default_provider_id") or value.get("defaultProviderId") or "").strip() or None
    if default_provider_id and default_provider_id not in provider_ids:
        raise KehrnelError(code="TERMINOLOGY_CONFIG_INVALID", status=400, message="default_provider_id must reference an existing provider.")
    return {"providers": providers, "routes": routes, "default_provider_id": default_provider_id}


def redact_terminology_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return configuration safe for operational and UI APIs."""
    redacted = deepcopy(config)
    for provider in redacted.get("providers") or []:
        auth = provider.get("auth")
        if isinstance(auth, dict):
            if str(auth.get("type") or "").endswith("-env"):
                auth["configured"] = bool(os.getenv(str(auth.get("environment_variable") or "")))
            else:
                auth["configured"] = None
    return redacted


def _parameter(name: str, value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"name": name, "valueBoolean": value}
    if isinstance(value, int):
        return {"name": name, "valueInteger": value}
    if name in {"system", "url", "target", "targetsystem"}:
        return {"name": name, "valueUri": str(value)}
    if name in {"code", "codeA", "codeB", "displayLanguage"}:
        return {"name": name, "valueCode": str(value)}
    if name == "date":
        return {"name": name, "valueDateTime": str(value)}
    return {"name": name, "valueString": str(value)}


def _fhir_parameters(operation: str, payload: dict[str, Any]) -> dict[str, Any]:
    names_by_operation = {
        "lookup": ("system", "version", "code", "date", "displayLanguage"),
        "validate-code": (
            ("url", "valueSetVersion", "system", "systemVersion", "code", "display", "date", "abstract", "displayLanguage")
            if payload.get("url")
            else ("system", "version", "code", "display", "date", "abstract", "displayLanguage")
        ),
        "subsumes": ("system", "version", "codeA", "codeB"),
        "expand": ("url", "valueSetVersion", "filter", "offset", "count", "includeDesignations", "activeOnly"),
        "translate": ("url", "conceptMapVersion", "system", "version", "code", "target", "targetsystem", "reverse"),
    }
    aliases = {
        "codeA": ("codeA", "code_a"),
        "codeB": ("codeB", "code_b"),
        "displayLanguage": ("displayLanguage", "display_language", "language"),
        "valueSetVersion": ("valueSetVersion", "value_set_version"),
        "systemVersion": ("systemVersion", "system_version", "version"),
        "includeDesignations": ("includeDesignations", "include_designations"),
        "activeOnly": ("activeOnly", "active_only"),
        "conceptMapVersion": ("conceptMapVersion", "concept_map_version"),
        "targetsystem": ("targetsystem", "target_system"),
    }
    parameters: list[dict[str, Any]] = []
    for name in names_by_operation[operation]:
        keys = aliases.get(name, (name,))
        value = next((payload.get(key) for key in keys if payload.get(key) is not None), None)
        if value is not None:
            parameters.append(_parameter(name, value))
    return {"resourceType": "Parameters", "parameter": parameters}


def _parameter_values(resource: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for parameter in resource.get("parameter") or []:
        if not isinstance(parameter, dict) or not parameter.get("name"):
            continue
        value = next((v for k, v in parameter.items() if k.startswith("value")), None)
        if value is None and isinstance(parameter.get("resource"), dict):
            value = parameter["resource"]
        values[str(parameter["name"])] = value
    return values


Transport = Callable[[str, dict[str, Any], dict[str, str], float], Awaitable[dict[str, Any]]]
MetadataTransport = Callable[[str, dict[str, str], float], Awaitable[dict[str, Any]]]


async def _default_transport(url: str, body: dict[str, Any], headers: dict[str, str], timeout: float) -> dict[str, Any]:
    def send() -> dict[str, Any]:
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            method="POST",
            headers={"Accept": "application/fhir+json", "Content-Type": "application/fhir+json", **headers},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read(_MAX_PROVIDER_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                details = json.loads(raw.decode("utf-8"))
            except Exception:
                details = {"status": exc.code}
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_REJECTED",
                status=502,
                message=f"External terminology provider returned HTTP {exc.code}.",
                details=details if isinstance(details, dict) else {"response": details},
            ) from exc
        except Exception as exc:
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_UNAVAILABLE",
                status=502,
                message="External terminology provider is unavailable.",
                details={"reason": str(exc)},
            ) from exc
        if len(raw) > _MAX_PROVIDER_RESPONSE_BYTES:
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_RESPONSE_TOO_LARGE",
                status=502,
                message="External terminology provider response exceeds the configured safety limit.",
                details={"max_bytes": _MAX_PROVIDER_RESPONSE_BYTES},
            )
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise KehrnelError(code="TERMINOLOGY_PROVIDER_INVALID_RESPONSE", status=502, message="External terminology provider returned invalid JSON.") from exc
        if not isinstance(parsed, dict):
            raise KehrnelError(code="TERMINOLOGY_PROVIDER_INVALID_RESPONSE", status=502, message="External terminology provider response must be a FHIR resource.")
        return parsed

    return await asyncio.to_thread(send)


async def _default_metadata_transport(
    url: str, headers: dict[str, str], timeout: float
) -> dict[str, Any]:
    """Fetch a FHIR CapabilityStatement without exposing provider credentials."""

    def send() -> dict[str, Any]:
        request = urllib.request.Request(
            url,
            method="GET",
            headers={"Accept": "application/fhir+json", **headers},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read(_MAX_PROVIDER_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                details = json.loads(raw.decode("utf-8"))
            except Exception:
                details = {"status": exc.code}
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_REJECTED",
                status=502,
                message=f"External terminology provider returned HTTP {exc.code} for metadata.",
                details=details if isinstance(details, dict) else {"response": details},
            ) from exc
        except Exception as exc:
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_UNAVAILABLE",
                status=502,
                message="External terminology provider metadata is unavailable.",
                details={"reason": str(exc)},
            ) from exc
        if len(raw) > _MAX_PROVIDER_RESPONSE_BYTES:
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_RESPONSE_TOO_LARGE",
                status=502,
                message="External terminology provider metadata exceeds the configured safety limit.",
                details={"max_bytes": _MAX_PROVIDER_RESPONSE_BYTES},
            )
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_INVALID_RESPONSE",
                status=502,
                message="External terminology provider returned invalid metadata JSON.",
            ) from exc
        if not isinstance(parsed, dict):
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_INVALID_RESPONSE",
                status=502,
                message="External terminology provider metadata must be a FHIR resource.",
            )
        return parsed

    return await asyncio.to_thread(send)


def _fhir_capability_summary(resource: dict[str, Any]) -> dict[str, Any]:
    if resource.get("resourceType") != "CapabilityStatement":
        raise KehrnelError(
            code="TERMINOLOGY_PROVIDER_INVALID_RESPONSE",
            status=502,
            message="FHIR terminology metadata endpoint did not return a CapabilityStatement.",
            details={"resourceType": resource.get("resourceType")},
        )
    operations: set[str] = set()
    resources: set[str] = set()
    for rest in resource.get("rest") or []:
        if not isinstance(rest, dict):
            continue
        for operation in rest.get("operation") or []:
            if isinstance(operation, dict) and operation.get("name"):
                operations.add(str(operation["name"]).lstrip("$").lower())
        for declared in rest.get("resource") or []:
            if not isinstance(declared, dict):
                continue
            resource_type = str(declared.get("type") or "").strip()
            if resource_type:
                resources.add(resource_type)
            for operation in declared.get("operation") or []:
                if isinstance(operation, dict) and operation.get("name"):
                    operations.add(str(operation["name"]).lstrip("$").lower())
    software = resource.get("software") if isinstance(resource.get("software"), dict) else {}
    implementation = (
        resource.get("implementation")
        if isinstance(resource.get("implementation"), dict)
        else {}
    )
    return {
        "resourceType": "CapabilityStatement",
        "status": resource.get("status"),
        "fhirVersion": resource.get("fhirVersion"),
        "formats": [str(value) for value in resource.get("format") or []],
        "software": {
            "name": software.get("name"),
            "version": software.get("version"),
        },
        "implementation": {
            "description": implementation.get("description"),
            "url": implementation.get("url"),
        },
        "resources": sorted(resources),
        "operations": sorted(operations),
        "digest": _digest(resource),
    }


class TerminologyService:
    """Resolve one tenant's request to one explicit terminology provider."""

    def __init__(
        self,
        runtime: Any,
        env_id: str,
        *,
        transport: Transport | None = None,
        metadata_transport: MetadataTransport | None = None,
    ):
        self.runtime = runtime
        self.env_id = env_id
        self.transport = transport or _default_transport
        self.metadata_transport = metadata_transport or _default_metadata_transport
        environment = runtime.get_environment(env_id)
        if not environment:
            raise KehrnelError(code="ENVIRONMENT_NOT_FOUND", status=404, message=f"Environment {env_id} not found")
        raw = (environment.metadata or {}).get("terminology") or {}
        self.config = normalize_terminology_config(raw)
        self._ensure_native_snomed_provider()

    def _ensure_native_snomed_provider(self) -> None:
        activation = self.runtime.registry.get_activation(self.env_id, "snomedct")
        has_native = any(
            provider.get("type") == "kehrnel-strategy" and SNOMED_SYSTEM_URI in provider.get("systems", [])
            for provider in self.config["providers"]
        )
        if activation and not has_native:
            self.config["providers"].append(
                {
                    "id": "snomed-local",
                    "type": "kehrnel-strategy",
                    "label": "Kehrnel SNOMED CT",
                    "domain": "snomedct",
                    "systems": [SNOMED_SYSTEM_URI],
                    "operations": sorted(_NATIVE_OPERATIONS),
                    "enabled": True,
                    "inferred": True,
                }
            )

    def _provider(self, payload: dict[str, Any], operation: str) -> dict[str, Any]:
        explicit_id = str(payload.get("provider_id") or payload.get("providerId") or "").strip()
        providers = {provider["id"]: provider for provider in self.config["providers"] if provider.get("enabled", True)}
        if explicit_id:
            provider = providers.get(explicit_id)
            if not provider:
                raise KehrnelError(code="TERMINOLOGY_PROVIDER_NOT_FOUND", status=404, message=f"Terminology provider {explicit_id!r} is not configured.")
            requested_system = str(payload.get("system") or "").strip()
            declared_systems = provider.get("systems") or []
            if requested_system and declared_systems and requested_system not in declared_systems:
                raise KehrnelError(
                    code="TERMINOLOGY_SYSTEM_UNSUPPORTED",
                    status=422,
                    message=f"Provider {explicit_id!r} does not declare support for system {requested_system!r}.",
                    details={"provider_id": explicit_id, "supported_systems": declared_systems},
                )
            return self._require_operation(provider, operation)

        system = str(payload.get("system") or "").strip()
        version = str(payload.get("version") or "").strip()
        value_set = str(payload.get("url") or payload.get("value_set") or payload.get("valueSet") or "").strip()
        routes = self.config.get("routes") or []
        for route in routes:
            if not value_set or route.get("value_set") != value_set:
                continue
            if route.get("system") and route["system"] != system:
                continue
            if route.get("version") and route["version"] != version:
                continue
            provider = providers.get(route["provider_id"])
            if provider:
                return self._require_operation(provider, operation)
        for route in routes:
            if not system or route.get("system") != system or route.get("value_set"):
                continue
            if route.get("version") and route["version"] != version:
                continue
            provider = providers.get(route["provider_id"])
            if provider:
                return self._require_operation(provider, operation)

        matches = [provider for provider in providers.values() if system and system in provider.get("systems", [])]
        if len(matches) == 1:
            return self._require_operation(matches[0], operation)
        if len(matches) > 1:
            raise KehrnelError(
                code="TERMINOLOGY_ROUTE_AMBIGUOUS",
                status=409,
                message=f"Multiple providers support system {system!r}; configure an exact route or provide provider_id.",
                details={"providers": [provider["id"] for provider in matches]},
            )
        if not system:
            operation_matches = [
                provider
                for provider in providers.values()
                if operation in provider.get("operations", [])
            ]
            if len(operation_matches) == 1:
                return operation_matches[0]
            if len(operation_matches) > 1:
                raise KehrnelError(
                    code="TERMINOLOGY_ROUTE_AMBIGUOUS",
                    status=409,
                    message=(
                        f"Multiple providers can execute {operation!r} without a code-system route; "
                        "configure an exact route or provide provider_id."
                    ),
                    details={"providers": [provider["id"] for provider in operation_matches]},
                )
        default_id = self.config.get("default_provider_id")
        if default_id and default_id in providers and (not system or system in providers[default_id].get("systems", [])):
            return self._require_operation(providers[default_id], operation)
        raise KehrnelError(
            code="TERMINOLOGY_ROUTE_NOT_FOUND",
            status=422,
            message=f"No terminology provider is configured for system {system!r}.",
            details={"system": system, "operation": operation},
        )

    @staticmethod
    def _require_operation(provider: dict[str, Any], operation: str) -> dict[str, Any]:
        if operation not in provider.get("operations", []):
            raise KehrnelError(
                code="TERMINOLOGY_OPERATION_UNSUPPORTED",
                status=422,
                message=f"Provider {provider['id']!r} does not declare operation {operation!r}.",
            )
        return provider

    async def capabilities(self, *, include_readiness: bool = False) -> dict[str, Any]:
        providers = []
        for provider in self.config["providers"]:
            item = redact_terminology_config({"providers": [provider]}).get("providers", [provider])[0]
            if provider["type"] == "kehrnel-strategy" and include_readiness:
                item["runtime"] = await self.runtime.dispatch(
                    self.env_id,
                    "op",
                    {
                        "domain": provider["domain"],
                        "op": "snomed_capabilities",
                        "payload": {"include_readiness": True},
                    },
                )
            providers.append(item)
        return {
            "ok": True,
            "environmentId": self.env_id,
            "providers": providers,
            "routes": self.config.get("routes") or [],
            "defaultProviderId": self.config.get("default_provider_id"),
        }

    async def provider_capabilities(self, provider_id: str) -> dict[str, Any]:
        """Inspect one provider using its native capability contract or FHIR metadata."""

        provider = next(
            (
                item
                for item in self.config["providers"]
                if item.get("id") == provider_id and item.get("enabled", True)
            ),
            None,
        )
        if provider is None:
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_NOT_FOUND",
                status=404,
                message=f"Terminology provider {provider_id!r} is not configured or enabled.",
            )
        started = time.perf_counter()
        if provider["type"] == "kehrnel-strategy":
            capability = await self.runtime.dispatch(
                self.env_id,
                "op",
                {
                    "domain": provider["domain"],
                    "op": "snomed_capabilities",
                    "payload": {"include_readiness": True},
                },
            )
            advertised = set(capability.get("operations") or [])
            advertised.add("capabilities")
            summary = capability
        else:
            response = await self.metadata_transport(
                f"{provider['base_url']}/metadata",
                self._auth_headers(provider),
                float(provider.get("timeout_seconds") or 20),
            )
            summary = _fhir_capability_summary(response)
            advertised = set(summary["operations"])
        declared = set(provider.get("operations") or [])
        return {
            "ok": True,
            "healthy": True,
            "environmentId": self.env_id,
            "provider": redact_terminology_config({"providers": [provider]})["providers"][0],
            "capabilities": summary,
            "declaredOperations": sorted(declared),
            "advertisedOperations": sorted(advertised),
            "missingDeclaredOperations": sorted(declared - advertised),
            "evidence": {
                "checkedAt": datetime.now(timezone.utc).isoformat(),
                "latencyMs": round((time.perf_counter() - started) * 1000, 3),
                "capabilityDigest": _digest(summary),
            },
        }

    async def execute(self, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        operation = str(operation).strip().lower().replace("_", "-")
        provider = self._provider(payload, operation)
        correlation_id = str(payload.get("correlation_id") or uuid4())
        started = time.perf_counter()
        try:
            if provider["type"] == "kehrnel-strategy":
                result = await self._execute_native(provider, operation, payload)
            else:
                result = await self._execute_fhir(provider, operation, payload)
        except KehrnelError as exc:
            exc.details = {
                **(exc.details or {}),
                "terminology": {
                    "correlationId": correlation_id,
                    "operation": operation,
                    "providerId": provider["id"],
                    "providerType": provider["type"],
                },
            }
            raise
        normalized = dict(result or {})
        normalized.setdefault("ok", True)
        normalized["provider"] = {
            "id": provider["id"],
            "type": provider["type"],
            "label": provider.get("label"),
        }
        normalized["evidence"] = {
            "correlationId": correlation_id,
            "operation": operation,
            "providerId": provider["id"],
            "providerType": provider["type"],
            "system": normalized.get("system") or payload.get("system"),
            "version": normalized.get("version") or payload.get("version"),
            "releaseId": normalized.get("releaseId"),
            "executedAt": datetime.now(timezone.utc).isoformat(),
            "latencyMs": round((time.perf_counter() - started) * 1000, 3),
            "requestDigest": _digest({"operation": operation, "payload": payload}),
            "resultDigest": _digest(normalized),
            "cacheStatus": "not-used",
        }
        return normalized

    async def _execute_native(self, provider: dict[str, Any], operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        op_map = {
            "capabilities": "snomed_capabilities",
            "lookup": "snomed_lookup",
            "validate-code": "snomed_validate_code",
            "subsumes": "snomed_subsumes",
            "expand": "snomed_expand_value_set",
            "search": "snomed_search",
            "ecl": "snomed_ecl",
            "ground": "snomed_ground_note",
        }
        if operation == "validate-code" and payload.get("url"):
            op_map["validate-code"] = "snomed_validate_value_set"
        native_payload = dict(payload)
        if operation == "lookup" and native_payload.get("code") and not native_payload.get("concept_id"):
            native_payload["concept_id"] = native_payload["code"]
        if operation == "expand" and native_payload.get("count") is not None and native_payload.get("limit") is None:
            native_payload["limit"] = native_payload["count"]
        result = await self.runtime.dispatch(
            self.env_id,
            "op",
            {"domain": provider["domain"], "op": op_map[operation], "payload": native_payload},
        )
        if isinstance(result, dict):
            result.setdefault("system", payload.get("system") or SNOMED_SYSTEM_URI)
            release_id = result.get("releaseId") or payload.get("release_id")
            if not release_id and isinstance(result.get("concept"), dict):
                release_id = result["concept"].get("releaseId")
            if release_id:
                result.setdefault("releaseId", release_id)
                result.setdefault("version", payload.get("version") or f"{SNOMED_SYSTEM_URI}/version/{release_id}")
        return result

    def _auth_headers(self, provider: dict[str, Any]) -> dict[str, str]:
        auth = provider.get("auth")
        if not isinstance(auth, dict):
            return {}
        auth_type = str(auth.get("type") or "")
        env_name = str(auth.get("environment_variable") or "")
        if auth_type.endswith("-env"):
            secret = os.getenv(env_name)
            secret_details = {"environment_variable": env_name}
        else:
            try:
                from kehrnel.engine.core.integrations.hdl.bindings_resolver import resolve_hdl_terminology_secret

                secret = resolve_hdl_terminology_secret(
                    env_id=self.env_id,
                    provider_id=str(auth.get("secret_ref") or provider["id"]),
                )
            except Exception as exc:
                raise KehrnelError(
                    code="TERMINOLOGY_PROVIDER_CREDENTIAL_UNAVAILABLE",
                    status=503,
                    message=f"Encrypted HDL credential for provider {provider['id']!r} is not available.",
                    details={"provider_id": provider["id"], "reason": str(exc)},
                ) from exc
            secret_details = {"provider_id": provider["id"]}
        if not secret:
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_CREDENTIAL_UNAVAILABLE",
                status=503,
                message=f"Credential reference for provider {provider['id']!r} is not available.",
                details=secret_details,
            )
        if auth_type.startswith("bearer-"):
            return {"Authorization": f"Bearer {secret}"}
        return {str(auth.get("header") or "X-API-Key"): secret}

    async def _execute_fhir(self, provider: dict[str, Any], operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        resource = (
            "ValueSet"
            if operation == "expand" or (operation == "validate-code" and payload.get("url"))
            else "ConceptMap"
            if operation == "translate"
            else "CodeSystem"
        )
        url = f"{provider['base_url']}/{resource}/${operation}"
        response = await self.transport(
            url,
            _fhir_parameters(operation, payload),
            self._auth_headers(provider),
            float(provider.get("timeout_seconds") or 20),
        )
        if response.get("resourceType") == "OperationOutcome":
            raise KehrnelError(
                code="TERMINOLOGY_PROVIDER_REJECTED",
                status=422,
                message="External terminology provider rejected the operation.",
                details=response,
            )
        values = _parameter_values(response) if response.get("resourceType") == "Parameters" else {}
        if operation == "validate-code":
            return {
                "valid": bool(values.get("result")),
                "message": values.get("message"),
                "display": values.get("display"),
                "system": payload.get("system"),
                "version": payload.get("version"),
                "code": payload.get("code"),
                "response": response,
            }
        if operation == "subsumes":
            return {
                "valid": values.get("outcome") is not None,
                "outcome": values.get("outcome"),
                "system": payload.get("system"),
                "version": payload.get("version"),
                "codeA": payload.get("codeA") or payload.get("code_a"),
                "codeB": payload.get("codeB") or payload.get("code_b"),
                "response": response,
            }
        if operation == "expand":
            value_set = response if response.get("resourceType") == "ValueSet" else values.get("return")
            return {"valueSet": value_set, "response": response}
        if operation == "translate":
            return {
                "matched": bool(values.get("result")),
                "message": values.get("message"),
                "system": payload.get("system"),
                "version": payload.get("version"),
                "code": payload.get("code"),
                "target": payload.get("target"),
                "targetSystem": payload.get("target_system") or payload.get("targetsystem"),
                "response": response,
            }
        return {"parameters": values, "response": response}


class RuntimeTerminologyAdapter:
    """Strategy-facing adapter over the tenant terminology gateway."""

    def __init__(self, runtime: Any, env_id: str):
        self.runtime = runtime
        self.env_id = env_id

    async def execute(self, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await TerminologyService(self.runtime, self.env_id).execute(
            operation, payload
        )

    async def capabilities(self, *, include_readiness: bool = False) -> dict[str, Any]:
        return await TerminologyService(self.runtime, self.env_id).capabilities(
            include_readiness=include_readiness
        )
