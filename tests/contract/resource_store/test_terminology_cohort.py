"""Contract tests for the governed terminology-driven cohort query (G17).

These pin the behaviours that make the governed lane trustworthy:
  * it refuses anything but its declared request contract,
  * it refuses to execute unless the projection readiness receipt says ready,
  * it resolves the value set from the ACTIVATED release and queries per code
    (comma-OR is not supported by the store and returns zero rows silently),
  * it de-duplicates resources and subjects across the per-code union.
"""

from __future__ import annotations

import pytest

from kehrnel.api.domains.fhir.terminology_cohort import (
    REQUEST_CONTRACT,
    RESULT_CONTRACT,
    execute_terminology_cohort_query,
    validate_request,
)
from kehrnel.engine.core.errors import KehrnelError


def _request(**overrides):
    body = {
        "contract": REQUEST_CONTRACT,
        "resourceType": "Condition",
        "codeSearchParameter": "code",
        "criteria": {"onset-date": "ge2014-01-01"},
        "limit": 50,
        "offset": 0,
        "expansionRequest": {
            "expansionId": "snomed-heart-failure",
            "providerId": "snomed-local",
            "systemUri": "http://snomed.info/sct",
            "expression": "<< 84114007",
            "maxCodes": 128,
        },
    }
    body.update(overrides)
    return body


READY_RECEIPT = {
    "contract": "fhir-projection-readiness-receipt/v1",
    "state": "ready",
    "conditions": {"search_index_present": True, "search_configured": True},
    "unmet": [],
    "receiptDigest": "sha256:deadbeef",
}


class FakeRuntime:
    """Minimal stand-in for the strategy runtime dispatch surface."""

    def __init__(self, *, receipt=READY_RECEIPT, concepts=None, rows_by_code=None):
        self.receipt = receipt
        self.concepts = concepts if concepts is not None else [
            {"conceptId": "84114007", "descriptions": [{"term": "Heart failure"}]},
            {"conceptId": "42343007", "descriptions": [{"term": "CCF"}]},
        ]
        self.rows_by_code = rows_by_code or {}
        self.searched_codes: list[str] = []

    async def dispatch(self, env_id, kind, payload):
        if kind == "op" and payload.get("op") == "fhir_stats":
            row = {"resource_type": "Condition", "collection": "Condition", "document_count": 950}
            if self.receipt is not None:
                row["projection_readiness_receipt"] = self.receipt
            return {"result": {"database": "fhir_test", "collections": [row]}}

        if kind == "op" and payload.get("op") == "snomed_expand_value_set":
            offset = int(payload["payload"].get("offset") or 0)
            limit = int(payload["payload"].get("limit") or 100)
            page = self.concepts[offset : offset + limit]
            has_more = offset + len(page) < len(self.concepts)
            return {
                "concepts": page,
                "releaseId": "test-release",
                "page": {
                    "offset": offset,
                    "limit": limit,
                    "returned": len(page),
                    "hasMore": has_more,
                    "nextOffset": offset + len(page) if has_more else None,
                },
            }

        if kind == "query":
            criteria = payload["query"]["criteria"]
            token = str(criteria.get("code") or "")
            code = token.split("|")[-1]
            self.searched_codes.append(code)
            rows = self.rows_by_code.get(code, [])
            return {"rows": rows, "total": len(rows)}

        raise AssertionError(f"unexpected dispatch: {kind} {payload}")


def test_validate_request_rejects_foreign_contract():
    with pytest.raises(KehrnelError) as excinfo:
        validate_request({"contract": "something-else/v1", "resourceType": "Condition"})
    assert excinfo.value.status == 400


def test_validate_request_requires_an_expression():
    body = _request()
    body["expansionRequest"] = {"providerId": "snomed-local"}
    with pytest.raises(KehrnelError) as excinfo:
        validate_request(body)
    assert excinfo.value.status == 400


@pytest.mark.asyncio
async def test_refuses_when_readiness_receipt_is_absent():
    """Without the governed receipt the lane must not execute at all."""
    runtime = FakeRuntime(receipt=None)
    with pytest.raises(KehrnelError) as excinfo:
        await execute_terminology_cohort_query(
            runtime, "env-1", _request(), strategy_id="fhir.resource_store"
        )
    assert excinfo.value.status == 409
    assert runtime.searched_codes == []


@pytest.mark.asyncio
async def test_refuses_when_projection_is_not_ready():
    not_ready = {**READY_RECEIPT, "state": "not_ready", "unmet": ["projection_complete"]}
    runtime = FakeRuntime(receipt=not_ready)
    with pytest.raises(KehrnelError) as excinfo:
        await execute_terminology_cohort_query(
            runtime, "env-1", _request(), strategy_id="fhir.resource_store"
        )
    assert excinfo.value.status == 409
    assert "projection_complete" in str(excinfo.value)
    assert runtime.searched_codes == []


@pytest.mark.asyncio
async def test_refuses_an_expansion_that_resolves_to_nothing():
    runtime = FakeRuntime(concepts=[])
    with pytest.raises(KehrnelError) as excinfo:
        await execute_terminology_cohort_query(
            runtime, "env-1", _request(), strategy_id="fhir.resource_store"
        )
    assert excinfo.value.status == 422


@pytest.mark.asyncio
async def test_queries_every_resolved_code_and_unions_the_result():
    """Per-code execution, because the store's `code` param has no comma-OR."""
    rows_by_code = {
        "84114007": [
            {"resourceType": "Condition", "id": "c1", "subject": {"reference": "Patient/p1"}},
            {"resourceType": "Condition", "id": "c2", "subject": {"reference": "Patient/p2"}},
        ],
        # Same patient as c2, and a duplicate resource id, to exercise de-duplication.
        "42343007": [
            {"resourceType": "Condition", "id": "c3", "subject": {"reference": "Patient/p2"}},
            {"resourceType": "Condition", "id": "c1", "subject": {"reference": "Patient/p1"}},
        ],
    }
    runtime = FakeRuntime(rows_by_code=rows_by_code)
    result = await execute_terminology_cohort_query(
        runtime, "env-1", _request(), strategy_id="fhir.resource_store"
    )

    assert result["contract"] == RESULT_CONTRACT
    assert sorted(runtime.searched_codes) == ["42343007", "84114007"]
    # c1 appeared twice but is counted once.
    assert [r["id"] for r in result["result"]["resources"]] == ["c1", "c2", "c3"]
    assert result["result"]["returnedSubjectReferences"] == ["Patient/p1", "Patient/p2"]
    assert result["executionEvidence"]["projectionReadiness"]["state"] == "ready"
    assert result["terminologyResolution"]["conceptCount"] == 2
    assert result["terminologyResolution"]["releaseId"] == "test-release"


@pytest.mark.asyncio
async def test_follows_expansion_pagination():
    """The expansion paginates; a single page would silently truncate the cohort."""
    concepts = [{"conceptId": str(900000 + i)} for i in range(150)]
    runtime = FakeRuntime(concepts=concepts)
    result = await execute_terminology_cohort_query(
        runtime, "env-1", _request(), strategy_id="fhir.resource_store"
    )
    # maxCodes=128 bounds the fan-out even though 150 concepts exist.
    assert result["terminologyResolution"]["conceptCount"] == 128
    assert len(runtime.searched_codes) == 128
