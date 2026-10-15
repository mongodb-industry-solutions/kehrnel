from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.api.domains.openehr.semantics.routes import (
    get_open_ehr_terminology_planning_service,
)
from kehrnel.api.domains.openehr.semantics.service import (
    OpenEhrSemanticService,
    StoredAdl14Opt,
    get_stored_adl14_opt_loader,
)
from kehrnel.api.domains.openehr.semantics.terminology_planning import (
    OpenEhrTerminologyPlanningService,
)
from kehrnel.engine.domains.openehr.templates import (
    compile_opt_semantic_catalog_xml,
)


SAMPLE_LAB_OPT = Path(
    "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/"
    "templates/sample_laboratory_v0_4.opt"
)


def _stored_opt() -> StoredAdl14Opt:
    return StoredAdl14Opt(
        template_id="sample_laboratory_v0.4",
        content=SAMPLE_LAB_OPT.read_text(encoding="utf-8"),
    )


def _request(stored: StoredAdl14Opt) -> dict[str, Any]:
    catalog = compile_opt_semantic_catalog_xml(stored.content)
    diagnosis = next(field for field in catalog.fields if field.label == "Diagnosis")
    onset = next(
        field
        for field in catalog.fields
        if field.label == "Date/time clinically recognised"
    )
    expansion_id = "snomed-heart-failure-20260131"
    return {
        "contract": "openehr-terminology-query-plan-request/v1",
        "query": {
            "contract": "openehr-semantic-query/v1",
            "scope": "cross_patient",
            "templateRef": {
                "templateId": catalog.template.template_id,
                "sourceDigest": catalog.template.source_digest,
                "catalogDigest": catalog.catalog_digest,
            },
            "select": ["ehr_id", "composition_uid"],
            "predicates": [
                {
                    "semanticUnitId": diagnosis.field_id,
                    "facet": "code",
                    "operator": "matches",
                    "terminologyExpansionRef": expansion_id,
                    "correlationGroup": "problem-instance",
                },
                {
                    "semanticUnitId": onset.field_id,
                    "facet": "value",
                    "operator": "gte",
                    "value": "2025-01-01",
                    "correlationGroup": "problem-instance",
                },
            ],
            "terminologyExpansions": [],
            "limit": 50,
        },
        "expansionRequests": [
            {
                "expansionId": expansion_id,
                "providerId": "snomed-local",
                "systemUri": "http://snomed.info/sct",
                "terminologyId": "SNOMED-CT",
                "version": "http://snomed.info/sct/version/20260131",
                "expression": "<< 84114007",
                "maxCodes": 32,
            }
        ],
    }


class _Gateway:
    def __init__(self, result: dict[str, Any]):
        self.result = result
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, operation: str, payload: dict[str, Any]):
        self.calls.append((operation, payload))
        return deepcopy(self.result)


def _gateway_result() -> dict[str, Any]:
    version = "http://snomed.info/sct/version/20260131"
    return {
        "ok": True,
        "system": "http://snomed.info/sct",
        "version": version,
        "releaseId": "20260131",
        "concepts": [
            {"conceptId": "84114007"},
            {"conceptId": "195111005"},
        ],
        "matches": [
            {"conceptId": "84114007"},
            {"conceptId": "195111005"},
        ],
        "page": {
            "offset": 0,
            "limit": 33,
            "returned": 2,
            "hasMore": False,
            "nextOffset": None,
        },
        "provider": {
            "id": "snomed-local",
            "type": "kehrnel-strategy",
            "label": "Kehrnel SNOMED CT",
        },
        "evidence": {
            "correlationId": "tx-123",
            "operation": "expand",
            "providerId": "snomed-local",
            "providerType": "kehrnel-strategy",
            "system": "http://snomed.info/sct",
            "version": version,
            "releaseId": "20260131",
            "requestDigest": "request-digest",
            "resultDigest": "result-digest",
        },
    }


def _client(
    tmp_path: Path,
    gateway: _Gateway,
    *,
    terminology_id_bindings: dict[str, str] | None = None,
) -> tuple[TestClient, StoredAdl14Opt]:
    stored = _stored_opt()
    app = create_app(str(tmp_path / "registry.json"))

    class Loader:
        async def load(self, template_id: str) -> StoredAdl14Opt:
            assert template_id == stored.template_id
            return stored

    app.dependency_overrides[get_stored_adl14_opt_loader] = Loader
    app.dependency_overrides[get_open_ehr_terminology_planning_service] = lambda: (
        OpenEhrTerminologyPlanningService(
            semantic_service=OpenEhrSemanticService(),
            terminology_gateway=gateway,
            environment_id="tenant-a",
            terminology_id_bindings=terminology_id_bindings,
        )
    )
    return TestClient(app), stored


def test_server_resolves_expansion_and_returns_a_real_aql_plan(tmp_path):
    gateway = _Gateway(_gateway_result())
    client, stored = _client(tmp_path, gateway)

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=_request(stored),
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["contract"] == "openehr-terminology-query-plan-result/v1"
    assert body["environmentId"] == "tenant-a"
    assert "195111005" in body["plan"]["aql"]
    assert "84114007" in body["plan"]["aql"]
    assert body["plan"]["parameters"]["terminology0"] == "SNOMED-CT"
    receipt = body["plan"]["terminologyExpansionReceipts"][0]
    assert receipt["providerId"] == "snomed-local"
    assert receipt["version"] == "http://snomed.info/sct/version/20260131"
    resolution = body["terminologyResolution"][0]
    assert resolution["releaseId"] == "20260131"
    assert resolution["correlationId"] == "tx-123"
    assert resolution["expansionDigest"] == receipt["expansionDigest"]
    assert gateway.calls == [
        (
            "expand",
            {
                "provider_id": "snomed-local",
                "system": "http://snomed.info/sct",
                "version": "http://snomed.info/sct/version/20260131",
                "expression": "<< 84114007",
                "offset": 0,
                "count": 33,
                "release_id": "20260131",
            },
        )
    ]


def test_external_value_set_requires_matching_definition_and_system_version(tmp_path):
    version = "http://snomed.info/sct/version/20260131"
    result = {
        "ok": True,
        "valueSet": {
            "resourceType": "ValueSet",
            "url": "https://example.test/ValueSet/heart-failure",
            "version": "1.0.0",
            "expansion": {
                "offset": 0,
                "total": 2,
                "contains": [
                    {
                        "system": "http://snomed.info/sct",
                        "version": version,
                        "code": "84114007",
                    },
                    {
                        "system": "http://snomed.info/sct",
                        "version": version,
                        "code": "195111005",
                    },
                ],
            },
        },
        "provider": {"id": "external-snomed", "type": "fhir-terminology"},
        "evidence": {
            "correlationId": "tx-fhir",
            "providerId": "external-snomed",
            "providerType": "fhir-terminology",
            "system": "http://snomed.info/sct",
            "version": version,
            "requestDigest": "request-digest",
            "resultDigest": "result-digest",
        },
    }
    gateway = _Gateway(result)
    client, stored = _client(tmp_path, gateway)
    body = _request(stored)
    expansion = body["expansionRequests"][0]
    expansion.pop("expression")
    expansion["providerId"] = "external-snomed"
    expansion["valueSetUri"] = "https://example.test/ValueSet/heart-failure"
    expansion["valueSetVersion"] = "1.0.0"

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=body,
    )

    assert response.status_code == 200, response.text
    receipt = response.json()["plan"]["terminologyExpansionReceipts"][0]
    assert receipt["valueSetUri"] == "https://example.test/ValueSet/heart-failure"
    assert receipt["expression"] == (
        "value-set:https://example.test/ValueSet/heart-failure|1.0.0"
    )
    assert gateway.calls[0][1]["url"] == (
        "https://example.test/ValueSet/heart-failure"
    )
    assert "expression" not in gateway.calls[0][1]


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (
            lambda result: result["provider"].update(id="other"),
            "OPENEHR_TERMINOLOGY_PROVIDER_MISMATCH",
        ),
        (
            lambda result: result.update(system="http://loinc.org"),
            "OPENEHR_TERMINOLOGY_SYSTEM_MISMATCH",
        ),
        (
            lambda result: result.update(releaseId="20250731"),
            "OPENEHR_TERMINOLOGY_VERSION_MISMATCH",
        ),
        (
            lambda result: (
                result["provider"].update(type="fhir-terminology"),
                result["evidence"].update(providerType="fhir-terminology"),
            ),
            "OPENEHR_TERMINOLOGY_SELECTOR_UNSUPPORTED",
        ),
        (
            lambda result: result["page"].update(
                hasMore=True,
                nextOffset=2,
            ),
            "OPENEHR_TERMINOLOGY_EXPANSION_INCOMPLETE",
        ),
        (
            lambda result: result.pop("evidence"),
            "OPENEHR_TERMINOLOGY_RESULT_INVALID",
        ),
    ],
)
def test_server_fails_closed_on_untrusted_expansion_results(
    tmp_path,
    mutate,
    code,
):
    result = _gateway_result()
    mutate(result)
    client, stored = _client(tmp_path, _Gateway(result))

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=_request(stored),
    )

    assert response.status_code in {422, 502}
    assert response.json()["error"]["code"] == code


def test_server_rejects_an_expansion_larger_than_the_requested_bound(tmp_path):
    result = _gateway_result()
    result["concepts"] = [{"conceptId": str(index)} for index in range(33)]
    result["matches"] = deepcopy(result["concepts"])
    client, stored = _client(tmp_path, _Gateway(result))

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=_request(stored),
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == (
        "OPENEHR_TERMINOLOGY_EXPANSION_TOO_LARGE"
    )


def test_server_rejects_caller_terminology_id_that_conflicts_with_system_binding(
    tmp_path,
):
    gateway = _Gateway(_gateway_result())
    client, stored = _client(tmp_path, gateway)
    body = _request(stored)
    body["expansionRequests"][0]["terminologyId"] = "SNOMED"

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=body,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "OPENEHR_TERMINOLOGY_ID_MISMATCH"
    assert gateway.calls == []


def test_server_owned_terminology_bindings_can_be_extended(tmp_path):
    system_uri = "http://loinc.org"
    result = _gateway_result()
    result.update(system=system_uri, version="2.78", releaseId="2.78")
    result["evidence"].update(system=system_uri, version="2.78", releaseId="2.78")
    result["concepts"] = [{"conceptId": "1234-5"}]
    result["matches"] = deepcopy(result["concepts"])
    result["page"].update(returned=1)
    gateway = _Gateway(result)
    client, stored = _client(
        tmp_path,
        gateway,
        terminology_id_bindings={system_uri: "LOINC"},
    )
    body = _request(stored)
    expansion = body["expansionRequests"][0]
    expansion.update(
        systemUri=system_uri,
        terminologyId="LOINC",
        version="2.78",
        releaseId="2.78",
        expression="1234-5",
    )

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=body,
    )

    assert response.status_code == 200, response.text
    receipt = response.json()["plan"]["terminologyExpansionReceipts"][0]
    assert receipt["systemUri"] == system_uri
    assert receipt["terminologyId"] == "LOINC"


def test_server_rejects_an_unregistered_terminology_binding_before_gateway_call(
    tmp_path,
):
    gateway = _Gateway(_gateway_result())
    client, stored = _client(tmp_path, gateway)
    body = _request(stored)
    body["expansionRequests"][0].update(
        systemUri="http://loinc.org",
        terminologyId="LOINC",
        version="2.78",
        releaseId="2.78",
        expression="1234-5",
    )

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=body,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == (
        "OPENEHR_TERMINOLOGY_BINDING_REQUIRED"
    )
    assert gateway.calls == []


def test_server_rejects_boundary_sized_expansion_without_completeness_evidence(
    tmp_path,
):
    result = _gateway_result()
    result.pop("page")
    gateway = _Gateway(result)
    client, stored = _client(tmp_path, gateway)
    body = _request(stored)
    body["expansionRequests"][0]["maxCodes"] = 2

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=body,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == (
        "OPENEHR_TERMINOLOGY_EXPANSION_INCOMPLETE"
    )


def test_server_accepts_sub_boundary_expansion_without_pagination_metadata(
    tmp_path,
):
    result = _gateway_result()
    result.pop("page")
    client, stored = _client(tmp_path, _Gateway(result))

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=_request(stored),
    )

    assert response.status_code == 200, response.text


def test_server_accepts_boundary_sized_expansion_with_terminal_page_evidence(
    tmp_path,
):
    gateway = _Gateway(_gateway_result())
    client, stored = _client(tmp_path, gateway)
    body = _request(stored)
    body["expansionRequests"][0]["maxCodes"] = 2

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=body,
    )

    assert response.status_code == 200, response.text


@pytest.mark.parametrize(
    "mutate",
    [
        lambda result: result["page"].update(hasMore=False, nextOffset=2),
        lambda result: result["page"].update(total=3, hasMore=False),
    ],
)
def test_server_rejects_contradictory_expansion_completion_evidence(
    tmp_path,
    mutate,
):
    result = _gateway_result()
    mutate(result)
    client, stored = _client(tmp_path, _Gateway(result))

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=_request(stored),
    )

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "OPENEHR_TERMINOLOGY_RESULT_INVALID"


@pytest.mark.parametrize(
    "mode",
    ["missing", "unused", "duplicate", "receipt", "unpinned"],
)
def test_request_expansions_must_exactly_match_typed_query_refs(
    tmp_path,
    mode,
):
    client, stored = _client(tmp_path, _Gateway(_gateway_result()))
    body = _request(stored)
    if mode == "missing":
        body["expansionRequests"][0]["expansionId"] = "different"
    elif mode == "unused":
        body["expansionRequests"].append(
            {
                **body["expansionRequests"][0],
                "expansionId": "unused",
            }
        )
    elif mode == "duplicate":
        body["expansionRequests"].append(deepcopy(body["expansionRequests"][0]))
    elif mode == "receipt":
        body["query"]["terminologyExpansions"] = [
            {
                "expansionId": "caller-receipt",
                "providerId": "caller",
                "systemUri": "http://snomed.info/sct",
                "terminologyId": "SNOMED-CT",
                "version": "20260131",
                "expression": "<< 84114007",
                "codes": ["84114007"],
                "truncated": False,
                "expansionDigest": "sha256:" + "0" * 64,
            }
        ]
    else:
        body["expansionRequests"][0].pop("version")

    response = client.post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-plans",
        json=body,
    )

    assert response.status_code == 422
    assert response.json()["detail"]


def test_openapi_keeps_compile_only_and_server_orchestrated_routes_distinct(tmp_path):
    paths = create_app(str(tmp_path / "registry.json")).openapi()["paths"]

    assert (
        "/api/domains/openehr/semantics/templates/{template_id}/query-plans"
        in paths
    )
    assert (
        "/api/domains/openehr/semantics/templates/{template_id}/"
        "terminology-query-plans"
        in paths
    )
    description = paths[
        "/api/domains/openehr/semantics/templates/{template_id}/query-plans"
    ]["post"]["description"]
    assert "performs no NLP, terminology expansion, or query execution" in description
