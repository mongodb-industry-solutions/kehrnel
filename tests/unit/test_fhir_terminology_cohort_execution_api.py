from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.requests import Request

from kehrnel.api.app import create_app
from kehrnel.api.domains.fhir import routes as fhir_routes
from kehrnel.api.domains.fhir.terminology_cohort import (
    FhirTerminologyCohortExecutionService,
    FhirTerminologyCohortQueryRequest,
    MAX_FHIR_COHORT_EXPANSION_CODES,
    MAX_FHIR_COHORT_ROWS,
)
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.strategies.fhir.resource_store.scripts.query import (
    _bson_digest,
    _digest,
)


SNOMED = "http://snomed.info/sct"
VERSION = f"{SNOMED}/version/20260131"
CODES = ["195111005", "84114007"]
DATA_TARGET_DIGEST = "sha256:" + "d" * 64
OTHER_DATA_TARGET_DIGEST = "sha256:" + "e" * 64


def _request() -> dict[str, Any]:
    return {
        "contract": "fhir-terminology-cohort-query-request/v1",
        "resourceType": "Condition",
        "codeSearchParameter": "code",
        "criteria": {"clinical-status": "active"},
        "sort": "_id",
        "limit": 50,
        "offset": 0,
        "expansionRequest": {
            "expansionId": "snomed-heart-failure-20260131",
            "providerId": "snomed-local",
            "systemUri": SNOMED,
            "version": VERSION,
            "expression": "<< 84114007",
            "maxCodes": 32,
        },
    }


class _Gateway:
    def __init__(self, result: Any = None, error: Exception | None = None):
        self.result = result or {
            "ok": True,
            "system": SNOMED,
            "version": VERSION,
            "releaseId": "20260131",
            "concepts": [{"conceptId": code} for code in reversed(CODES)],
            "matches": [{"conceptId": code} for code in reversed(CODES)],
            "page": {
                "offset": 0,
                "limit": 33,
                "returned": 2,
                "hasMore": False,
                "nextOffset": None,
            },
            "provider": {"id": "snomed-local", "type": "kehrnel-strategy"},
            "evidence": {
                "correlationId": "terminology-call-1",
                "providerId": "snomed-local",
                "providerType": "kehrnel-strategy",
                "system": SNOMED,
                "version": VERSION,
                "releaseId": "20260131",
                "requestDigest": "request-digest",
                "resultDigest": "result-digest",
            },
        }
        self.error = error
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, operation: str, payload: dict[str, Any]):
        self.calls.append((operation, deepcopy(payload)))
        if self.error:
            raise self.error
        return deepcopy(self.result)


class _Registry:
    def __init__(self, strategy_id: str = "fhir.resource_store"):
        self.activation = SimpleNamespace(
            strategy_id=strategy_id,
            domain="fhir",
            activation_id="activation-fhir-1",
            version="0.4.0",
            manifest_digest="sha256:" + "a" * 64,
            config_hash="sha256:" + "b" * 64,
            bindings_hash="sha256:" + "c" * 64,
        )

    def get_activation(self, environment_id: str, domain: str):
        assert environment_id == "tenant-a"
        assert domain == "fhir"
        return self.activation


class _Runtime:
    def __init__(
        self,
        *,
        strategy_id: str = "fhir.resource_store",
        error: Exception | None = None,
        projection_document_count: int = 2,
        projection_current_count: int = 2,
        projection_collection_exists: bool = True,
        projection_mismatch_count: int = 0,
        projection_data_target_digest: str | None = DATA_TARGET_DIGEST,
        execution_data_target_digest: str | None = DATA_TARGET_DIGEST,
    ):
        self.registry = _Registry(strategy_id)
        self.error = error
        self.projection_document_count = projection_document_count
        self.projection_current_count = projection_current_count
        self.projection_collection_exists = projection_collection_exists
        self.projection_mismatch_count = projection_mismatch_count
        self.projection_data_target_digest = projection_data_target_digest
        self.execution_data_target_digest = execution_data_target_digest
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def dispatch(
        self,
        environment_id: str,
        operation: str,
        payload: dict[str, Any],
    ):
        self.calls.append((environment_id, operation, deepcopy(payload)))
        if operation == "op":
            assert payload["domain"] == "fhir"
            assert payload["op"] == "fhir_stats"
            assert payload["payload"] == {"resource_types": ["Condition"]}
            receipt = {
                "contract": "fhir-resource-store-projection-readiness/v1",
                "state": "ready",
                "resourceType": "Condition",
                "database": "fhir_test",
                "collection": "Condition",
                "collectionExists": self.projection_collection_exists,
                "documentCount": self.projection_document_count,
                "contractCurrentCount": self.projection_current_count,
                "canonicalProjectionMismatchCount": self.projection_mismatch_count,
                "storageSchemaVersion": "3",
                "projectionContractVersion": "v2:global-current",
                "resourceProjectionVersion": "v2:condition-current",
                "searchConfigured": True,
                "searchIndexPresent": True,
                "exactCodeIndexPresent": True,
            }
            if (
                not self.projection_collection_exists
                or self.projection_mismatch_count
                or self.projection_document_count != self.projection_current_count
            ):
                receipt["state"] = "not_ready"
            receipt["receiptDigest"] = _digest(receipt)
            return {
                "ok": True,
                "database": "fhir_test",
                "data_target_digest": self.projection_data_target_digest,
                "projection_contract_version": "v2:global-current",
                "collections": [
                    {
                        "resource_type": "Condition",
                        "collection": "Condition",
                        "collection_exists": self.projection_collection_exists,
                        "document_count": self.projection_document_count,
                        "contract_current_count": self.projection_current_count,
                        "canonical_projection_mismatch_count": (
                            self.projection_mismatch_count
                        ),
                        "storage_schema_version": "3",
                        "resource_projection_version": "v2:condition-current",
                        "search_configured": True,
                        "search_index_present": True,
                        "exact_code_index_present": True,
                        "projection_readiness_receipt": receipt,
                    }
                ],
            }
        if self.error:
            raise self.error
        query = payload["query"]
        tokens = sorted(
            f"{query['terminology_code_filter']['system']}|{code}"
            for code in query["terminology_code_filter"]["codes"]
        )
        compiled_filter = {
            "$and": [
                {"_search.clinicalStatus_codes": "active"},
                {"_search.code_systemCode": {"$in": tokens}},
            ]
        }
        if query.get("criteria", {}).get("onset-date"):
            compiled_filter["$and"].append(
                {
                    "_search._dates.onset-date": {
                        "$elemMatch": {
                            "end": {"$gt": datetime(2025, 1, 1, tzinfo=UTC)}
                        }
                    }
                }
            )
        compiled_sort = [["id", 1]]
        source_query_digest = _bson_digest(query)
        compiled_query_contract = {
            "contract": "fhir-compiled-query-evidence/v1",
            "resourceType": "Condition",
            "database": "fhir_test",
            "collection": "Condition",
            "filter": compiled_filter,
            "sort": compiled_sort,
            "limit": query["_count"],
            "offset": query["_offset"],
        }
        compiled_query_digest = _bson_digest(compiled_query_contract)
        compiler_evidence = {
            "contract": "fhir-compiled-query-evidence/v1",
            "source_query_digest": source_query_digest,
            "compiled_query_digest": compiled_query_digest,
            "resource_type": "Condition",
            "database": "fhir_test",
            "collection": "Condition",
            "sort": compiled_sort,
            "limit": query["_count"],
            "offset": query["_offset"],
        }
        activation = self.registry.activation
        return {
            "engine_used": "fhir_mql",
            "rows": [
                {
                    "resourceType": "Condition",
                    "id": "condition-1",
                    "subject": {"reference": "Patient/patient-1"},
                    "code": {
                        "coding": [
                            {"system": SNOMED, "code": "84114007"}
                        ]
                    },
                    "_search": {"code_systemCode": [f"{SNOMED}|84114007"]},
                }
            ],
            "explain": {
                "activation_id": activation.activation_id,
                "strategy_id": activation.strategy_id,
                "config_hash": activation.config_hash,
                "manifest_digest": activation.manifest_digest,
                "data_target_digest": self.execution_data_target_digest,
                "builder": {
                    "chosen": "fhir_mql",
                    "terminology_code_filter": {
                        "parameter": "code",
                        "system": SNOMED,
                        "projection_field": "_search.code_systemCode",
                        "code_count": len(tokens),
                        "token_digest": _digest(tokens),
                    },
                    "compiler_evidence": compiler_evidence,
                },
                "filter": compiled_filter,
                "total": 1,
                "returned": 1,
                "execution": {
                    "database": "fhir_test",
                    "collection": "Condition",
                    "sort": compiled_sort,
                    "limit": query["_count"],
                    "skip": query["_offset"],
                    "source_query_digest": source_query_digest,
                    "compiled_query_digest": compiled_query_digest,
                },
            },
        }


def _service(
    runtime: _Runtime,
    gateway: _Gateway | None = None,
) -> FhirTerminologyCohortExecutionService:
    return FhirTerminologyCohortExecutionService(
        terminology_gateway=gateway or _Gateway(),
        runtime=runtime,
        environment_id="tenant-a",
    )


@pytest.mark.asyncio
async def test_server_resolves_and_executes_bounded_fhir_cohort():
    runtime = _Runtime()
    gateway = _Gateway()
    request = FhirTerminologyCohortQueryRequest.model_validate(_request())

    response = await _service(runtime, gateway).execute_query(request)

    assert response.contract == "fhir-terminology-cohort-query-result/v1"
    assert response.result.result_kind == "condition_resource_search"
    assert response.result.returned_resource_count == 1
    assert response.result.total_matching_resources == 1
    assert response.result.returned_subject_references == ["Patient/patient-1"]
    assert response.result.resources[0]["id"] == "condition-1"
    assert "_search" not in response.result.resources[0]
    assert response.terminology_resolution.codes == CODES
    assert response.execution_evidence.expansion_version_pinned is True
    assert response.execution_evidence.stored_coding_version_enforced is False
    assert response.execution_evidence.compiled_filter_digest.startswith("sha256:")
    assert response.execution_evidence.compiled_query_digest.startswith("sha256:")
    assert response.execution_evidence.data_target_digest == DATA_TARGET_DIGEST
    assert (
        response.execution_evidence.projection_readiness.data_target_digest
        == DATA_TARGET_DIGEST
    )
    assert response.execution_evidence.projection_readiness.collection_exists is True
    assert (
        response.execution_evidence.projection_readiness.canonical_projection_mismatch_count
        == 0
    )
    assert response.execution_evidence.projection_readiness.document_count == 2
    assert (
        response.execution_evidence.projection_readiness.contract_current_count
        == 2
    )

    assert gateway.calls == [
        (
            "expand",
            {
                "provider_id": "snomed-local",
                "system": SNOMED,
                "version": VERSION,
                "offset": 0,
                "count": 33,
                "expression": "<< 84114007",
                "release_id": "20260131",
            },
        )
    ]
    assert len(runtime.calls) == 2
    preflight_env, preflight_operation, preflight_payload = runtime.calls[0]
    assert preflight_env == "tenant-a"
    assert preflight_operation == "op"
    assert preflight_payload["__activation_precondition"]["activationId"] == (
        "activation-fhir-1"
    )
    environment_id, operation, payload = runtime.calls[1]
    assert environment_id == "tenant-a"
    assert operation == "query"
    assert payload["domain"] == "fhir"
    assert payload["query"]["terminology_code_filter"] == {
        "parameter": "code",
        "system": SNOMED,
        "codes": CODES,
    }
    assert payload["query"]["_count"] == 50
    assert payload["__activation_precondition"] == {
        "activationId": "activation-fhir-1",
        "strategyId": "fhir.resource_store",
        "configHash": "sha256:" + "b" * 64,
        "manifestDigest": "sha256:" + "a" * 64,
        "bindingsHash": "sha256:" + "c" * 64,
        "dataTargetDigest": DATA_TARGET_DIGEST,
    }


@pytest.mark.asyncio
async def test_execution_rejects_unsupported_activation_before_terminology_call():
    runtime = _Runtime(strategy_id="fhir.other")
    gateway = _Gateway()

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime, gateway).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_TERMINOLOGY_COHORT_STRATEGY_UNSUPPORTED"
    assert caught.value.status == 409
    assert gateway.calls == []
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_rejects_incomplete_expansion_before_fhir_query():
    runtime = _Runtime()
    result = deepcopy(_Gateway().result)
    result["page"]["hasMore"] = True
    result["page"]["nextOffset"] = 2

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime, _Gateway(result=result)).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_TERMINOLOGY_EXPANSION_INCOMPLETE"
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_rejects_boundary_sized_expansion_without_completion_evidence():
    runtime = _Runtime()
    result = deepcopy(_Gateway().result)
    result.pop("page")
    body = _request()
    body["expansionRequest"]["maxCodes"] = 2

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime, _Gateway(result=result)).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(body)
        )

    assert caught.value.code == "FHIR_TERMINOLOGY_EXPANSION_INCOMPLETE"
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_accepts_sub_boundary_expansion_without_pagination_metadata():
    runtime = _Runtime()
    result = deepcopy(_Gateway().result)
    result.pop("page")

    response = await _service(runtime, _Gateway(result=result)).execute_query(
        FhirTerminologyCohortQueryRequest.model_validate(_request())
    )

    assert response.terminology_resolution.codes == CODES


@pytest.mark.asyncio
async def test_execution_accepts_boundary_sized_expansion_with_terminal_page_evidence():
    runtime = _Runtime()
    body = _request()
    body["expansionRequest"]["maxCodes"] = 2

    response = await _service(runtime).execute_query(
        FhirTerminologyCohortQueryRequest.model_validate(body)
    )

    assert response.terminology_resolution.codes == CODES
    assert [call[1] for call in runtime.calls] == ["op", "query"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        lambda result: result["page"].update(hasMore=False, nextOffset=2),
        lambda result: result["page"].update(total=3, hasMore=False),
    ],
)
async def test_execution_rejects_contradictory_expansion_completion_evidence(
    mutate,
):
    runtime = _Runtime()
    result = deepcopy(_Gateway().result)
    mutate(result)

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime, _Gateway(result=result)).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_TERMINOLOGY_RESULT_INVALID"
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_rejects_provider_identity_mismatch_before_fhir_query():
    runtime = _Runtime()
    result = deepcopy(_Gateway().result)
    result["provider"]["id"] = "other-provider"

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime, _Gateway(result=result)).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_TERMINOLOGY_PROVIDER_MISMATCH"
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_rejects_code_system_mismatch_before_fhir_query():
    runtime = _Runtime()
    result = deepcopy(_Gateway().result)
    result["system"] = "http://loinc.org"

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime, _Gateway(result=result)).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_TERMINOLOGY_SYSTEM_MISMATCH"
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_propagates_runtime_failure_unchanged():
    expected = KehrnelError(
        code="FHIR_SEARCH_COMPILE_FAILED",
        status=400,
        message="unsupported criterion",
    )
    runtime = _Runtime(error=expected)

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value is expected


@pytest.mark.asyncio
async def test_execution_rejects_result_from_switched_activation():
    class SwitchedRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            result = await super().dispatch(environment_id, operation, payload)
            if operation == "query":
                result["explain"]["activation_id"] = "activation-fhir-2"
            return result

    with pytest.raises(KehrnelError) as caught:
        await _service(SwitchedRuntime()).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_EXECUTION_ACTIVATION_MISMATCH"
    assert caught.value.status == 409


@pytest.mark.asyncio
async def test_execution_rejects_missing_projection_data_target_before_query():
    runtime = _Runtime(projection_data_target_digest=None)

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_SEARCH_PROJECTION_NOT_READY"
    assert caught.value.status == 409
    assert [call[1] for call in runtime.calls] == ["op"]


@pytest.mark.asyncio
async def test_execution_rejects_result_from_switched_data_target():
    runtime = _Runtime(
        execution_data_target_digest=OTHER_DATA_TARGET_DIGEST,
    )

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_EXECUTION_ACTIVATION_MISMATCH"
    assert caught.value.status == 409
    assert caught.value.details["mismatches"]["dataTargetDigest"] == {
        "expected": DATA_TARGET_DIGEST,
        "actual": OTHER_DATA_TARGET_DIGEST,
    }


@pytest.mark.asyncio
async def test_execution_propagates_data_target_change_at_dispatch_boundary():
    expected = KehrnelError(
        code="DATA_TARGET_PRECONDITION_FAILED",
        status=409,
        message="resolved target changed",
        details={
            "expectedDataTargetDigest": DATA_TARGET_DIGEST,
            "actualDataTargetDigest": OTHER_DATA_TARGET_DIGEST,
        },
    )

    class ChangedTargetRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            if operation == "query":
                assert payload["__activation_precondition"][
                    "dataTargetDigest"
                ] == DATA_TARGET_DIGEST
                raise expected
            return await super().dispatch(environment_id, operation, payload)

    with pytest.raises(KehrnelError) as caught:
        await _service(ChangedTargetRuntime()).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value is expected


@pytest.mark.asyncio
async def test_execution_rejects_stale_projection_coverage_before_query():
    runtime = _Runtime(
        projection_document_count=7,
        projection_current_count=6,
    )
    gateway = _Gateway()

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime, gateway).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_SEARCH_PROJECTION_NOT_READY"
    assert caught.value.status == 409
    assert caught.value.details["missingOrStaleCount"] == 1
    assert len(gateway.calls) == 1
    assert [call[1] for call in runtime.calls] == ["op"]


@pytest.mark.asyncio
async def test_execution_rejects_missing_condition_collection_before_query():
    runtime = _Runtime(
        projection_document_count=0,
        projection_current_count=0,
        projection_collection_exists=False,
    )

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_SEARCH_PROJECTION_NOT_READY"
    assert [call[1] for call in runtime.calls] == ["op"]


@pytest.mark.asyncio
async def test_execution_rejects_canonical_condition_projection_mismatch():
    runtime = _Runtime(projection_mismatch_count=1)

    with pytest.raises(KehrnelError) as caught:
        await _service(runtime).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_SEARCH_PROJECTION_NOT_READY"
    assert caught.value.details["canonicalProjectionMismatchCount"] == 1
    assert [call[1] for call in runtime.calls] == ["op"]


@pytest.mark.asyncio
async def test_execution_rejects_invalid_projection_readiness_receipt():
    class TamperedReceiptRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            result = await super().dispatch(environment_id, operation, payload)
            if operation == "op":
                result["collections"][0]["projection_readiness_receipt"][
                    "receiptDigest"
                ] = "sha256:" + "0" * 64
            return result

    runtime = TamperedReceiptRuntime()
    with pytest.raises(KehrnelError) as caught:
        await _service(runtime).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_SEARCH_PROJECTION_NOT_READY"
    assert "receipt" in caught.value.details["reason"]


@pytest.mark.asyncio
async def test_execution_rejects_compiler_that_drops_an_ordinary_criterion():
    class DroppedCriterionRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            result = await super().dispatch(environment_id, operation, payload)
            if operation == "query":
                result["explain"]["filter"] = result["explain"]["filter"]["$and"][1]
            return result

    with pytest.raises(KehrnelError) as caught:
        await _service(DroppedCriterionRuntime()).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_COHORT_QUERY_RESULT_INVALID"
    assert "complete typed query" in caught.value.details["reason"]


@pytest.mark.parametrize("operator", ["$or", "$nor"])
@pytest.mark.asyncio
async def test_execution_rejects_terminology_clause_below_non_conjunctive_operator(
    operator: str,
):
    class NonConjunctiveRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            result = await super().dispatch(environment_id, operation, payload)
            if operation == "query":
                exact = result["explain"]["filter"]["$and"][1]
                result["explain"]["filter"] = {
                    operator: [exact, {"_search.clinicalStatus_codes": "active"}]
                }
            return result

    with pytest.raises(KehrnelError) as caught:
        await _service(NonConjunctiveRuntime()).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_COHORT_QUERY_RESULT_INVALID"
    assert "exact system|code expansion" in caught.value.details["reason"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("collection", "OtherCondition"),
        ("database", "other_database"),
        ("sort", [["recordedDate", -1]]),
        ("limit", 49),
        ("skip", 1),
    ],
)
@pytest.mark.asyncio
async def test_execution_rejects_runtime_control_drift(field: str, value: Any):
    class DriftedRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            result = await super().dispatch(environment_id, operation, payload)
            if operation == "query":
                result["explain"]["execution"][field] = value
            return result

    with pytest.raises(KehrnelError) as caught:
        await _service(DriftedRuntime()).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value.code == "FHIR_COHORT_QUERY_RESULT_INVALID"
    assert "complete typed query" in caught.value.details["reason"]


@pytest.mark.asyncio
async def test_execution_propagates_activation_change_during_projection_preflight():
    expected = KehrnelError(
        code="ACTIVATION_PRECONDITION_FAILED",
        status=409,
        message="activation changed",
    )

    class ChangedRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            if operation == "op":
                raise expected
            return await super().dispatch(environment_id, operation, payload)

    with pytest.raises(KehrnelError) as caught:
        await _service(ChangedRuntime()).execute_query(
            FhirTerminologyCohortQueryRequest.model_validate(_request())
        )

    assert caught.value is expected


def test_request_contract_enforces_interactive_bounds():
    body = _request()
    body["limit"] = MAX_FHIR_COHORT_ROWS + 1
    with pytest.raises(ValidationError):
        FhirTerminologyCohortQueryRequest.model_validate(body)

    body = _request()
    body["expansionRequest"]["maxCodes"] = MAX_FHIR_COHORT_EXPANSION_CODES + 1
    with pytest.raises(ValidationError):
        FhirTerminologyCohortQueryRequest.model_validate(body)


def test_request_contract_rejects_conflicting_snomed_release_pins():
    body = _request()
    body["expansionRequest"]["releaseId"] = "20250731"

    with pytest.raises(ValidationError):
        FhirTerminologyCohortQueryRequest.model_validate(body)


@pytest.mark.asyncio
async def test_execution_evidence_digests_compiled_bson_date_filters():
    body = _request()
    body["criteria"] = {"onset-date": "ge2025-01-01"}
    response = await FhirTerminologyCohortExecutionService(
        terminology_gateway=_Gateway(),
        runtime=_Runtime(),
        environment_id="tenant-a",
    ).execute_query(FhirTerminologyCohortQueryRequest.model_validate(body))

    assert response.execution_evidence.compiled_filter_digest.startswith("sha256:")


def test_fhir_terminology_cohort_http_contract(tmp_path, monkeypatch):
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "false")
    monkeypatch.setenv("KEHRNEL_DEFAULT_ENV_ID", "tenant-a")
    runtime = _Runtime()
    gateway = _Gateway()
    monkeypatch.setattr(
        fhir_routes,
        "_require_fhir_activation",
        lambda _request, _env_id: (runtime, runtime.registry.activation),
    )
    monkeypatch.setattr(
        fhir_routes,
        "TerminologyService",
        lambda _runtime, _env_id: gateway,
    )
    app = create_app(str(tmp_path / "registry.json"))

    response = TestClient(app).post(
        "/api/domains/fhir/semantic/terminology-query-executions",
        json=_request(),
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["contract"] == "fhir-terminology-cohort-query-result/v1"
    assert body["environmentId"] == "tenant-a"
    assert body["strategyId"] == "fhir.resource_store"
    assert body["result"]["engineUsed"] == "fhir_mql"
    assert body["executionEvidence"]["dataTargetDigest"] == DATA_TARGET_DIGEST
    assert (
        body["executionEvidence"]["projectionReadiness"]["dataTargetDigest"]
        == DATA_TARGET_DIGEST
    )
    assert body["result"]["returnedSubjectReferences"] == [
        "Patient/patient-1"
    ]


def test_fhir_terminology_cohort_endpoint_requires_api_authentication(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "true")
    monkeypatch.setenv("KEHRNEL_API_KEYS", "fhir-cohort-test-key")
    monkeypatch.setenv("KEHRNEL_DEFAULT_ENV_ID", "tenant-a")
    runtime = _Runtime()
    monkeypatch.setattr(
        fhir_routes,
        "_require_fhir_activation",
        lambda _request, _env_id: (runtime, runtime.registry.activation),
    )
    app = create_app(str(tmp_path / "registry.json"))

    response = TestClient(app).post(
        "/api/domains/fhir/semantic/terminology-query-executions",
        json=_request(),
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHORIZED"
    assert runtime.calls == []


def test_fhir_semantic_cohort_route_reuses_environment_authorization(monkeypatch):
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "true")
    monkeypatch.setenv("KEHRNEL_DEFAULT_ENV_ID", "tenant-a")
    monkeypatch.setattr(fhir_routes, "_is_env_access_allowed", lambda *_args: False)
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/domains/fhir/semantic/terminology-query-executions",
            "headers": [],
            "query_string": b"",
            "server": ("testserver", 80),
            "client": ("127.0.0.1", 1),
            "scheme": "http",
            "app": SimpleNamespace(state=SimpleNamespace()),
        }
    )

    with pytest.raises(HTTPException) as caught:
        fhir_routes.resolve_active_env_id(request)

    assert caught.value.status_code == 403
