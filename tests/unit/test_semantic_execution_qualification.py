from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Mapping

import pytest
from pydantic import ValidationError

from kehrnel.contracts.semantic_qualification import (
    QualificationHttpResponse,
    SemanticExecutionQualificationResult,
    SemanticExecutionQualificationSpec,
    content_digest,
    run_semantic_execution_qualification,
)


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _spec() -> SemanticExecutionQualificationSpec:
    return SemanticExecutionQualificationSpec.model_validate(
        {
            "contract": "semantic-execution-qualification-spec/v1",
            "qualificationId": "pilot-2026-10-01",
            "environmentId": "tenant-a",
            "baseUrl": "https://kehrnel.example.test/",
            "probes": [
                {
                    "probeId": "openehr-heart-failure",
                    "kind": "openehr-terminology-query",
                    "templateId": "sample_laboratory_v0.4",
                    "request": {
                        "contract": "openehr-terminology-query-plan-request/v1",
                        "query": {"limit": 50},
                    },
                },
                {
                    "probeId": "fhir-heart-failure",
                    "kind": "fhir-terminology-cohort",
                    "request": {
                        "contract": "fhir-terminology-cohort-query-request/v1",
                        "limit": 50,
                    },
                },
            ],
        }
    )


def _openehr_response() -> dict[str, Any]:
    manifest = {
        "contract": "openehr-semantic-projection-manifest/v1",
        "environmentId": "tenant-a",
        "strategyId": "openehr.rps_dual",
        "templateId": "sample_laboratory_v0.4",
        "templateSourceDigest": _digest("1"),
        "catalogDigest": _digest("2"),
        "planDigest": _digest("3"),
        "mappingDigest": _digest("4"),
        "projectionRevision": "revision-1",
        "requiredPaths": ["/content[at0001]/data[at0002]"],
        "coveredPaths": ["/content[at0001]/data[at0002]"],
        "missingPaths": [],
        "atlasIndexName": "openehr-search",
        "atlasIndexDefinitionDigest": _digest("5"),
    }
    manifest["manifestDigest"] = content_digest(manifest)
    return {
        "contract": "openehr-terminology-query-execution-result/v1",
        "environmentId": "tenant-a",
        "strategyId": "openehr.rps_dual",
        "plan": {"planDigest": _digest("3")},
        "terminologyResolution": [
            {"codes": ["84114007"], "expansionDigest": _digest("9")}
        ],
        "result": {
            "resultKind": "composition_match_rows",
            "engineUsed": "text_search_dual",
            "rows": [{"patientSecret": "must-not-enter-artifact"}],
            "requestedLimit": 50,
            "offset": 0,
            "hasMore": False,
        },
        "executionEvidence": {
            "environmentId": "tenant-a",
            "strategyId": "openehr.rps_dual",
            "activationId": "activation-openehr-1",
            "strategyVersion": "0.4.0",
            "manifestDigest": _digest("a"),
            "configHash": _digest("b"),
            "bindingsHash": _digest("c"),
            "dataTargetDigest": _digest("d"),
            "planDigest": _digest("3"),
            "semanticQueryDigest": _digest("6"),
            "catalogDigest": _digest("2"),
            "executionInputDigest": _digest("7"),
            "compiledPipelineDigest": _digest("8"),
            "terminologyExpansionDigests": [_digest("9")],
            "projectionReadiness": {
                "contract": "openehr-semantic-projection-readiness/v1",
                "ready": True,
                "manifest": manifest,
                "rebuildState": "ready",
                "rebuildComplete": True,
                "rebuildInProgress": False,
                "reconciliation": {},
                "consistency": {},
                "atlasIndexState": "verified_ready",
                "atlasIndexLiveVerified": True,
                "reasons": [],
            },
            "requireExactPathCorrelation": True,
            "engineUsed": "text_search_dual",
            "rowCount": 1,
            "requestedLimit": 50,
            "offset": 0,
            "hasMore": False,
            "projectionMutationSequence": 4,
        },
    }


def _fhir_response() -> dict[str, Any]:
    response = {
        "contract": "fhir-terminology-cohort-query-result/v1",
        "environmentId": "tenant-a",
        "strategyId": "fhir.resource_store",
        "resourceType": "Condition",
        "terminologyResolution": {
            "codes": ["84114007"],
            "expansionDigest": _digest("4"),
        },
        "result": {
            "resultKind": "condition_resource_search",
            "engineUsed": "fhir_mql",
            "resources": [{"patientSecret": "also-must-not-enter-artifact"}],
            "totalMatchingResources": 1,
            "returnedResourceCount": 1,
            "returnedSubjectReferences": ["Patient/private"],
        },
        "executionEvidence": {
            "environmentId": "tenant-a",
            "strategyId": "fhir.resource_store",
            "activationId": "activation-fhir-1",
            "strategyVersion": "0.4.0",
            "manifestDigest": _digest("a"),
            "configHash": _digest("b"),
            "bindingsHash": _digest("c"),
            "dataTargetDigest": _digest("e"),
            "queryDigest": _digest("1"),
            "compiledQueryDigest": _digest("2"),
            "compiledFilterDigest": _digest("3"),
            "terminologyExpansionDigest": _digest("4"),
            "exactTokenProjection": "_search.code_systemCode",
            "projectionReadiness": {
                "resourceType": "Condition",
                "dataTargetDigest": _digest("e"),
                "database": "private_database_name",
                "collection": "private_collection_name",
                "collectionExists": True,
                "documentCount": 100,
                "contractCurrentCount": 100,
                "canonicalProjectionMismatchCount": 0,
                "storageSchemaVersion": "v1",
                "projectionContractVersion": "v1",
                "resourceProjectionVersion": "v1",
                "searchIndexPresent": True,
                "exactCodeIndexPresent": True,
                "readinessReceiptContract": "fhir-resource-store-projection-readiness/v1",
                "readinessReceiptDigest": "placeholder",
            },
            "expansionVersionPinned": True,
            "storedCodingVersionEnforced": False,
            "rowLimit": 50,
            "rowCount": 1,
        },
    }
    readiness = response["executionEvidence"]["projectionReadiness"]
    receipt = {
        "contract": "fhir-resource-store-projection-readiness/v1",
        "state": "ready",
        "resourceType": readiness["resourceType"],
        "database": readiness["database"],
        "collection": readiness["collection"],
        "collectionExists": True,
        "documentCount": readiness["documentCount"],
        "contractCurrentCount": readiness["contractCurrentCount"],
        "canonicalProjectionMismatchCount": 0,
        "storageSchemaVersion": readiness["storageSchemaVersion"],
        "projectionContractVersion": readiness["projectionContractVersion"],
        "resourceProjectionVersion": readiness["resourceProjectionVersion"],
        "searchConfigured": True,
        "searchIndexPresent": True,
        "exactCodeIndexPresent": True,
    }
    readiness["readinessReceiptDigest"] = content_digest(receipt)
    return response


def test_qualification_writes_only_redacted_execution_evidence():
    calls: list[tuple[str, Mapping[str, str]]] = []

    def transport(url, _body, headers, _timeout):
        calls.append((url, headers))
        payload = _openehr_response() if "/openehr/" in url else _fhir_response()
        return QualificationHttpResponse(status=200, payload=payload)

    moments = iter(
        [
            datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 10, 1, 10, 1, tzinfo=timezone.utc),
        ]
    )
    result = run_semantic_execution_qualification(
        _spec(),
        transport,
        api_key="super-secret-api-key",
        clock=lambda: next(moments),
    )

    assert result.status == "qualified"
    assert result.verify_artifact_digest()
    assert [probe.status for probe in result.probes] == ["qualified", "qualified"]
    assert calls[0][1]["X-Active-Env"] == "tenant-a"
    assert calls[0][1]["X-API-Key"] == "super-secret-api-key"
    serialized = json.dumps(result.to_wire(), sort_keys=True)
    assert "must-not-enter-artifact" not in serialized
    assert "super-secret-api-key" not in serialized
    assert "private_database_name" not in serialized
    assert "private_collection_name" not in serialized
    assert "kehrnel.example.test" not in serialized


def test_http_failure_is_recorded_without_leaking_error_payload():
    def transport(_url, _body, _headers, _timeout):
        return QualificationHttpResponse(
            status=503,
            payload={"detail": "mongodb://username:password@private-host/patient-db"},
        )

    spec = _spec().model_copy(update={"probes": [_spec().probes[1]]})
    result = run_semantic_execution_qualification(spec, transport)

    assert result.status == "failed"
    assert result.probes[0].failure.code == "ENDPOINT_REJECTED_PROBE"
    assert "password" not in json.dumps(result.to_wire())
    assert result.probes[0].response_digest is not None
    assert result.verify_artifact_digest()


def test_success_response_digest_accepts_finite_runtime_timings():
    response = _openehr_response()
    response["result"]["explain"] = {
        "timings": {"kehrnel_db_ms": 12.375}
    }

    def transport(_url, _body, _headers, _timeout):
        return QualificationHttpResponse(status=200, payload=response)

    spec = _spec().model_copy(update={"probes": [_spec().probes[0]]})
    result = run_semantic_execution_qualification(spec, transport)

    assert result.status == "qualified"
    assert result.probes[0].response_digest.startswith("sha256:")
    assert result.verify_artifact_digest()


def test_qualification_normalizes_runtime_activation_identity_hashes():
    response = _openehr_response()
    evidence = response["executionEvidence"]
    for key in ("manifestDigest", "configHash", "bindingsHash"):
        evidence[key] = evidence[key].removeprefix("sha256:")

    def transport(_url, _body, _headers, _timeout):
        return QualificationHttpResponse(status=200, payload=response)

    spec = _spec().model_copy(update={"probes": [_spec().probes[0]]})
    result = run_semantic_execution_qualification(spec, transport)

    assert result.status == "qualified"
    assert result.probes[0].evidence.manifest_digest == _digest("a")
    assert result.probes[0].evidence.config_hash == _digest("b")
    assert result.probes[0].evidence.bindings_hash == _digest("c")


@pytest.mark.parametrize(
    ("payload", "expected_code"),
    [
        (
            {
                "error": {
                    "code": "OPENEHR_SEMANTIC_PROJECTION_NOT_READY",
                    "details": "mongodb://username:password@private-host/patient-db",
                }
            },
            "OPENEHR_SEMANTIC_PROJECTION_NOT_READY",
        ),
        (
            {
                "resourceType": "OperationOutcome",
                "issue": [
                    {
                        "details": {"text": "FHIR_SEARCH_PROJECTION_NOT_READY"},
                        "diagnostics": "private_collection_name",
                    }
                ],
            },
            "FHIR_SEARCH_PROJECTION_NOT_READY",
        ),
    ],
)
def test_http_failure_retains_only_safe_machine_error_code(payload, expected_code):
    def transport(_url, _body, _headers, _timeout):
        return QualificationHttpResponse(status=409, payload=payload)

    spec = _spec().model_copy(update={"probes": [_spec().probes[1]]})
    result = run_semantic_execution_qualification(spec, transport)

    serialized = json.dumps(result.to_wire())
    assert result.probes[0].failure.code == expected_code
    assert "password" not in serialized
    assert "private_collection_name" not in serialized


def test_http_failure_rejects_untrusted_error_code_text():
    def transport(_url, _body, _headers, _timeout):
        return QualificationHttpResponse(
            status=409,
            payload={"error": {"code": "NOT SAFE: private-host"}},
        )

    spec = _spec().model_copy(update={"probes": [_spec().probes[1]]})
    result = run_semantic_execution_qualification(spec, transport)

    assert result.probes[0].failure.code == "ENDPOINT_REJECTED_PROBE"
    assert "private-host" not in json.dumps(result.to_wire())


def test_environment_mismatch_fails_closed():
    response = _fhir_response()
    response["environmentId"] = "tenant-b"

    def transport(_url, _body, _headers, _timeout):
        return QualificationHttpResponse(status=200, payload=response)

    spec = _spec().model_copy(update={"probes": [_spec().probes[1]]})
    result = run_semantic_execution_qualification(spec, transport)

    assert result.status == "failed"
    assert result.probes[0].failure.code == "EVIDENCE_IDENTITY_MISMATCH"


def test_unverified_openehr_atlas_index_is_not_qualified():
    response = _openehr_response()
    readiness = response["executionEvidence"]["projectionReadiness"]
    readiness["atlasIndexState"] = "configured_unverified"
    readiness["atlasIndexLiveVerified"] = False

    def transport(_url, _body, _headers, _timeout):
        return QualificationHttpResponse(status=200, payload=response)

    spec = _spec().model_copy(update={"probes": [_spec().probes[0]]})
    result = run_semantic_execution_qualification(spec, transport)

    assert result.status == "failed"
    assert result.probes[0].failure.code == "PROJECTION_NOT_LIVE_VERIFIED"


def test_artifact_digest_detects_tampering():
    def transport(url, _body, _headers, _timeout):
        return QualificationHttpResponse(
            status=200,
            payload=_openehr_response() if "/openehr/" in url else _fhir_response(),
        )

    result = run_semantic_execution_qualification(_spec(), transport)
    tampered = SemanticExecutionQualificationResult.model_validate(
        {
            **result.to_wire(),
            "environmentId": "tenant-b",
        }
    )
    assert not tampered.verify_artifact_digest()


@pytest.mark.parametrize(
    "base_url",
    [
        "https://user:password@example.test",
        "file:///tmp/kehrnel",
        "https://example.test?apiKey=secret",
        "https://example.test/#secret",
    ],
)
def test_spec_rejects_credential_bearing_or_non_http_base_urls(base_url: str):
    payload = _spec().to_wire()
    payload["baseUrl"] = base_url
    with pytest.raises(ValidationError):
        SemanticExecutionQualificationSpec.model_validate(payload)
