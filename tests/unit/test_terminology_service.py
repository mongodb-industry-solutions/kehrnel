from types import SimpleNamespace

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.terminology.service import SNOMED_SYSTEM_URI, TerminologyService, normalize_terminology_config


class _Registry:
    def __init__(self, native=False):
        self.native = native

    def get_activation(self, env_id, domain=None):
        if self.native and domain == "snomedct":
            return SimpleNamespace(strategy_id="snomedct.mongodb")
        return None


class _Runtime:
    def __init__(self, terminology=None, native=False):
        self.registry = _Registry(native=native)
        self.environment = SimpleNamespace(metadata={"terminology": terminology or {}})
        self.calls = []

    def get_environment(self, env_id):
        return self.environment if env_id == "tenant-a" else None

    async def dispatch(self, env_id, op, payload):
        self.calls.append((env_id, op, payload))
        if payload["op"] == "snomed_capabilities":
            return {
                "ok": True,
                "operations": [
                    "lookup",
                    "validate-code",
                    "subsumes",
                    "expand",
                    "search",
                    "ecl",
                    "ground",
                ],
            }
        if payload["op"] == "snomed_validate_code":
            return {"ok": True, "valid": True, "code": payload["payload"]["code"]}
        return {"ok": True}


def test_terminology_config_keeps_only_secret_references():
    config = normalize_terminology_config(
        {
            "providers": [
                {
                    "id": "external",
                    "type": "fhir-terminology",
                    "base_url": "https://terminology.example/fhir",
                    "systems": ["http://loinc.org"],
                    "auth": {"type": "bearer-env", "environment_variable": "CUSTOMER_TERMINOLOGY_TOKEN"},
                }
            ],
            "routes": [{"system": "http://loinc.org", "provider_id": "external"}],
        }
    )

    assert config["providers"][0]["auth"] == {
        "type": "bearer-env",
        "environment_variable": "CUSTOMER_TERMINOLOGY_TOKEN",
    }
    assert "token" not in config["providers"][0]


def test_terminology_config_rejects_credentials_in_provider_url():
    with pytest.raises(KehrnelError, match="without embedded credentials"):
        normalize_terminology_config(
            {
                "providers": [
                    {
                        "id": "external",
                        "type": "fhir-terminology",
                        "base_url": "https://user:secret@terminology.example/fhir",
                    }
                ]
            }
        )


def test_terminology_config_rejects_duplicate_exact_routes():
    with pytest.raises(KehrnelError) as exc_info:
        normalize_terminology_config(
            {
                "providers": [
                    {"id": "one", "type": "fhir-terminology", "base_url": "https://one.example/fhir"},
                    {"id": "two", "type": "fhir-terminology", "base_url": "https://two.example/fhir"},
                ],
                "routes": [
                    {"system": "http://loinc.org", "provider_id": "one"},
                    {"system": "http://loinc.org", "provider_id": "two"},
                ],
            }
        )

    assert exc_info.value.code == "TERMINOLOGY_ROUTE_AMBIGUOUS"


@pytest.mark.asyncio
async def test_native_snomed_activation_is_an_explicit_inferred_provider():
    runtime = _Runtime(native=True)
    service = TerminologyService(runtime, "tenant-a")

    result = await service.execute("validate-code", {"system": SNOMED_SYSTEM_URI, "code": "73211009"})

    assert result["valid"] is True
    assert result["provider"]["id"] == "snomed-local"
    assert runtime.calls[0][2]["domain"] == "snomedct"
    assert runtime.calls[0][2]["op"] == "snomed_validate_code"


@pytest.mark.asyncio
async def test_single_native_provider_can_expand_ecl_without_redundant_system():
    runtime = _Runtime(native=True)

    result = await TerminologyService(runtime, "tenant-a").execute(
        "expand", {"expression": "<< 73211009", "offset": 10, "count": 25}
    )

    assert result["provider"]["id"] == "snomed-local"
    assert runtime.calls[0][2]["op"] == "snomed_expand_value_set"
    assert runtime.calls[0][2]["payload"]["offset"] == 10
    assert runtime.calls[0][2]["payload"]["limit"] == 25
    assert result["evidence"]["requestDigest"]
    assert result["evidence"]["resultDigest"]
    assert result["evidence"]["cacheStatus"] == "not-used"


@pytest.mark.asyncio
async def test_native_provider_capability_discovery_includes_gateway_contract():
    runtime = _Runtime(native=True)

    result = await TerminologyService(runtime, "tenant-a").provider_capabilities(
        "snomed-local"
    )

    assert result["healthy"] is True
    assert "capabilities" in result["advertisedOperations"]
    assert result["missingDeclaredOperations"] == []


@pytest.mark.asyncio
async def test_external_fhir_provider_receives_standard_parameters(monkeypatch):
    monkeypatch.setenv("LOINC_TOKEN", "secret-value")
    runtime = _Runtime(
        terminology={
            "providers": [
                {
                    "id": "loinc",
                    "type": "fhir-terminology",
                    "base_url": "https://tx.example/fhir",
                    "systems": ["http://loinc.org"],
                    "auth": {"type": "bearer-env", "environment_variable": "LOINC_TOKEN"},
                }
            ]
        }
    )
    captured = {}

    async def transport(url, body, headers, timeout):
        captured.update(url=url, body=body, headers=headers, timeout=timeout)
        return {
            "resourceType": "Parameters",
            "parameter": [
                {"name": "result", "valueBoolean": True},
                {"name": "display", "valueString": "Glucose"},
            ],
        }

    service = TerminologyService(runtime, "tenant-a", transport=transport)
    result = await service.execute(
        "validate-code",
        {"system": "http://loinc.org", "code": "2345-7", "display": "Glucose"},
    )

    assert captured["url"] == "https://tx.example/fhir/CodeSystem/$validate-code"
    assert captured["headers"] == {"Authorization": "Bearer secret-value"}
    assert {tuple(item.items()) for item in captured["body"]["parameter"]} >= {
        (("name", "system"), ("valueUri", "http://loinc.org")),
        (("name", "code"), ("valueCode", "2345-7")),
    }
    assert result["valid"] is True
    assert result["provider"]["id"] == "loinc"
    assert result["evidence"]["providerId"] == "loinc"
    assert result["evidence"]["correlationId"]
    assert result["evidence"]["latencyMs"] >= 0


@pytest.mark.asyncio
async def test_external_fhir_provider_capabilities_are_discovered_and_compared():
    runtime = _Runtime(
        terminology={
            "providers": [
                {
                    "id": "external",
                    "type": "fhir-terminology",
                    "base_url": "https://tx.example/fhir",
                    "systems": ["http://loinc.org"],
                    "operations": ["lookup", "validate-code", "expand"],
                }
            ]
        }
    )
    captured = {}

    async def metadata_transport(url, headers, timeout):
        captured.update(url=url, headers=headers, timeout=timeout)
        return {
            "resourceType": "CapabilityStatement",
            "status": "active",
            "fhirVersion": "5.0.0",
            "format": ["json"],
            "software": {"name": "Test terminology server", "version": "1.0"},
            "rest": [
                {
                    "mode": "server",
                    "resource": [
                        {
                            "type": "CodeSystem",
                            "operation": [
                                {"name": "lookup"},
                                {"name": "validate-code"},
                            ],
                        },
                        {"type": "ValueSet", "operation": [{"name": "expand"}]},
                    ],
                }
            ],
        }

    result = await TerminologyService(
        runtime, "tenant-a", metadata_transport=metadata_transport
    ).provider_capabilities("external")

    assert captured["url"] == "https://tx.example/fhir/metadata"
    assert result["healthy"] is True
    assert result["capabilities"]["fhirVersion"] == "5.0.0"
    assert result["advertisedOperations"] == ["expand", "lookup", "validate-code"]
    assert result["missingDeclaredOperations"] == []
    assert result["evidence"]["capabilityDigest"]


@pytest.mark.asyncio
async def test_value_set_validation_uses_value_set_operation(monkeypatch):
    runtime = _Runtime(
        terminology={
            "providers": [
                {
                    "id": "valuesets",
                    "type": "fhir-terminology",
                    "base_url": "https://tx.example/fhir",
                    "systems": ["http://loinc.org"],
                }
            ],
            "routes": [
                {
                    "value_set": "http://example.org/ValueSet/labs",
                    "provider_id": "valuesets",
                }
            ],
        }
    )
    captured = {}

    async def transport(url, body, headers, timeout):
        captured.update(url=url, body=body)
        return {
            "resourceType": "Parameters",
            "parameter": [{"name": "result", "valueBoolean": True}],
        }

    result = await TerminologyService(runtime, "tenant-a", transport=transport).execute(
        "validate-code",
        {
            "url": "http://example.org/ValueSet/labs",
            "value_set_version": "2026.1",
            "system": "http://loinc.org",
            "version": "2.78",
            "code": "2345-7",
        },
    )

    assert captured["url"] == "https://tx.example/fhir/ValueSet/$validate-code"
    assert {tuple(item.items()) for item in captured["body"]["parameter"]} >= {
        (("name", "valueSetVersion"), ("valueString", "2026.1")),
        (("name", "systemVersion"), ("valueString", "2.78")),
    }
    assert result["valid"] is True


@pytest.mark.asyncio
async def test_external_concept_map_translation_preserves_fhir_evidence():
    runtime = _Runtime(
        terminology={
            "providers": [
                {
                    "id": "maps",
                    "type": "fhir-terminology",
                    "base_url": "https://tx.example/fhir",
                    "systems": ["http://example.org/source"],
                }
            ]
        }
    )
    captured = {}

    async def transport(url, body, headers, timeout):
        captured.update(url=url, body=body)
        return {
            "resourceType": "Parameters",
            "parameter": [
                {"name": "result", "valueBoolean": True},
                {
                    "name": "match",
                    "part": [
                        {
                            "name": "concept",
                            "valueCoding": {
                                "system": "http://example.org/target",
                                "code": "B",
                            },
                        }
                    ],
                },
            ],
        }

    result = await TerminologyService(runtime, "tenant-a", transport=transport).execute(
        "translate",
        {
            "provider_id": "maps",
            "url": "http://example.org/ConceptMap/a-to-b",
            "system": "http://example.org/source",
            "code": "A",
            "target_system": "http://example.org/target",
        },
    )

    assert captured["url"] == "https://tx.example/fhir/ConceptMap/$translate"
    assert result["matched"] is True
    assert result["response"]["parameter"][1]["name"] == "match"


@pytest.mark.asyncio
async def test_routing_fails_closed_when_two_providers_claim_same_system():
    runtime = _Runtime(
        terminology={
            "providers": [
                {"id": "one", "type": "fhir-terminology", "base_url": "https://one.example/fhir", "systems": ["http://loinc.org"]},
                {"id": "two", "type": "fhir-terminology", "base_url": "https://two.example/fhir", "systems": ["http://loinc.org"]},
            ]
        }
    )

    with pytest.raises(KehrnelError) as exc_info:
        await TerminologyService(runtime, "tenant-a").execute(
            "lookup", {"system": "http://loinc.org", "code": "2345-7"}
        )

    assert exc_info.value.code == "TERMINOLOGY_ROUTE_AMBIGUOUS"


@pytest.mark.asyncio
async def test_explicit_provider_cannot_bypass_its_declared_system_scope():
    runtime = _Runtime(
        terminology={
            "providers": [
                {
                    "id": "loinc",
                    "type": "fhir-terminology",
                    "base_url": "https://tx.example/fhir",
                    "systems": ["http://loinc.org"],
                }
            ]
        }
    )

    with pytest.raises(KehrnelError) as exc_info:
        await TerminologyService(runtime, "tenant-a").execute(
            "validate-code",
            {"provider_id": "loinc", "system": SNOMED_SYSTEM_URI, "code": "73211009"},
        )

    assert exc_info.value.code == "TERMINOLOGY_SYSTEM_UNSUPPORTED"


def test_openapi_exposes_native_gateway_and_fhir_terminology_operations(tmp_path, monkeypatch):
    from kehrnel.api.app import create_app

    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "false")
    monkeypatch.setenv("KEHRNEL_RATE_LIMIT", "0")
    paths = create_app(str(tmp_path / "registry.json")).openapi()["paths"]

    assert "/api/terminology/validate-code" in paths
    assert "/api/terminology/validate-codes" in paths
    assert "/api/terminology/translate" in paths
    assert "/api/terminology/configuration" in paths
    assert "/api/terminology/providers/{provider_id}/test" in paths
    assert "/api/terminology/providers/{provider_id}/capabilities" in paths
    assert "/api/domains/snomedct/subsumes" in paths
    assert "/api/domains/snomedct/value-sets" in paths
    assert "/api/domains/snomedct/value-sets/$validate-code" in paths
    assert "/api/domains/snomedct/suggest" in paths
    assert "/api/domains/snomedct/concepts/{concept_id}/history" in paths
    assert "/api/domains/snomedct/ground/reviews" in paths
    assert "/api/domains/snomedct/ground/corpus/search" in paths
    assert "/api/domains/snomedct/benchmarks/retrieval" in paths
    assert "/api/domains/snomedct/releases" in paths
    assert "/api/domains/snomedct/releases/inspect" in paths
    assert "/api/domains/snomedct/releases/diff" in paths
    assert "/api/domains/snomedct/releases/ingest" in paths
    assert "/api/domains/snomedct/sidecar/rebuild" in paths
    assert "/api/domains/snomedct/indexes/ensure" in paths
    assert "/api/domains/fhir/CodeSystem/$validate-code" in paths
    assert "/api/domains/fhir/CodeSystem/$subsumes" in paths
    assert "/api/domains/fhir/ValueSet/$expand" in paths
    assert "/api/domains/fhir/ValueSet/$validate-code" in paths
    assert "/api/domains/fhir/ConceptMap/$translate" in paths
