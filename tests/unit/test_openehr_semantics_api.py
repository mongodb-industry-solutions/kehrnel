from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.api.domains.openehr.semantics.service import (
    get_open_ehr_semantic_service,
    StoredAdl14Opt,
    get_stored_adl14_opt_loader,
    load_stored_adl14_opt,
)
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.domains.openehr.semantic_query import (
    create_terminology_expansion_receipt,
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


def _client(tmp_path: Path, stored: StoredAdl14Opt) -> TestClient:
    app = create_app(str(tmp_path / "registry.json"))

    class FakeLoader:
        async def load(self, template_id: str) -> StoredAdl14Opt:
            assert template_id == stored.template_id
            return stored

    app.dependency_overrides[get_stored_adl14_opt_loader] = FakeLoader
    return TestClient(app)


def _field(catalog, label: str):
    matches = [field for field in catalog.fields if field.label == label]
    assert len(matches) == 1
    return matches[0]


def _semantic_query(stored: StoredAdl14Opt) -> dict:
    catalog = compile_opt_semantic_catalog_xml(stored.content)
    diagnosis = _field(catalog, "Diagnosis")
    onset = _field(catalog, "Date/time clinically recognised")
    expansion = create_terminology_expansion_receipt(
        expansion_id="snomed-heart-failure-20260131",
        provider_id="tenant-snomed",
        system_uri="http://snomed.info/sct",
        terminology_id="SNOMED-CT",
        version="20260131",
        expression="<< 84114007",
        codes=["84114007", "195111005"],
    )
    return {
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
                "terminologyExpansionRef": expansion.expansion_id,
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
        "terminologyExpansions": [expansion.to_wire()],
        "limit": 50,
    }


@pytest.mark.parametrize(
    "route_prefix",
    ["", "/v1"],
    ids=["canonical", "versioned-alias"],
)
def test_catalog_endpoint_compiles_the_stored_opt_with_camel_case_contract(
    tmp_path,
    route_prefix,
):
    stored = _stored_opt()
    response = _client(tmp_path, stored).get(
        f"{route_prefix}/api/domains/openehr/semantics/templates/"
        f"{stored.template_id}/catalog"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["contract"] == "openehr-opt-semantic-catalog/v1"
    assert body["template"]["templateId"] == stored.template_id
    assert body["template"]["sourceDigest"].startswith("sha256:")
    assert body["catalogDigest"].startswith("sha256:")
    assert body["fields"]
    assert "semanticUnitId" not in body["fields"][0]
    assert "fieldId" in body["fields"][0]
    assert "containingArchetype" in body["fields"][0]
    assert "catalog_digest" not in body


def test_query_plan_endpoint_renders_aql_without_executing_it(tmp_path):
    stored = _stored_opt()
    response = _client(tmp_path, stored).post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/query-plans",
        json=_semantic_query(stored),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["contract"] == "openehr-semantic-query-plan/v1"
    assert body["catalogRef"]["templateId"] == stored.template_id
    assert body["planDigest"].startswith("sha256:")
    assert "semanticQueryDigest" in body
    assert "resolvedFields" in body
    assert "terminologyExpansionRefs" in body
    assert body["terminologyExpansionReceipts"][0]["providerId"] == "tenant-snomed"
    assert body["terminologyExpansionReceipts"][0]["version"] == "20260131"
    assert body["executionRequest"]["rawAql"] == body["aql"]
    assert body["executionRequest"]["requireExactPathCorrelation"] is True
    assert "raw_aql" not in body["executionRequest"]
    assert "195111005" in body["aql"]
    assert "84114007" in body["aql"]
    assert ">= $p1" in body["aql"]
    assert body["parameters"]["p1"] == "2025-01-01"


def test_catalog_compilation_runs_off_the_async_request_thread(tmp_path):
    import threading

    stored = _stored_opt()
    app = create_app(str(tmp_path / "registry.json"))
    request_thread: list[int] = []
    compiler_thread: list[int] = []

    class RecordingLoader:
        async def load(self, template_id: str) -> StoredAdl14Opt:
            assert template_id == stored.template_id
            request_thread.append(threading.get_ident())
            return stored

    class RecordingService:
        def compile_catalog(self, loaded: StoredAdl14Opt):
            compiler_thread.append(threading.get_ident())
            return compile_opt_semantic_catalog_xml(loaded.content)

    app.dependency_overrides[get_stored_adl14_opt_loader] = RecordingLoader
    app.dependency_overrides[get_open_ehr_semantic_service] = RecordingService

    response = TestClient(app).get(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/catalog"
    )

    assert response.status_code == 200
    assert request_thread and compiler_thread
    assert request_thread[0] != compiler_thread[0]


def test_invalid_stored_opt_is_a_stable_422(tmp_path):
    stored = StoredAdl14Opt(template_id="broken-template", content="<not-an-opt>")
    response = _client(tmp_path, stored).get(
        "/api/domains/openehr/semantics/templates/broken-template/catalog"
    )

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "OPENEHR_OPT_SEMANTIC_CATALOG_INVALID",
            "message": "The stored OPT cannot be compiled into a semantic catalog.",
            "details": {
                "templateId": "broken-template",
                "reason": "Invalid OPT XML: no element found: line 1, column 12",
            },
        }
    }


def test_invalid_opt_cardinality_is_also_a_stable_422(tmp_path):
    content = SAMPLE_LAB_OPT.read_text(encoding="utf-8").replace(
        "<lower>1</lower>",
        "<lower>2</lower>",
        1,
    )
    stored = StoredAdl14Opt(
        template_id="sample_laboratory_v0.4",
        content=content,
    )
    response = _client(tmp_path, stored).get(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/catalog"
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "OPENEHR_OPT_SEMANTIC_CATALOG_INVALID"


def test_query_plan_rejects_catalog_drift_with_a_stable_409(tmp_path):
    stored = _stored_opt()
    query = _semantic_query(stored)
    query["templateRef"]["catalogDigest"] = "sha256:" + "f" * 64

    response = _client(tmp_path, stored).post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/query-plans",
        json=query,
    )

    assert response.status_code == 409
    body = response.json()["error"]
    assert body["code"] == "TEMPLATE_REF_MISMATCH"
    assert body["details"]["mismatches"]["catalogDigest"]["actual"] == (
        "sha256:" + "f" * 64
    )


def test_missing_stored_template_is_a_stable_404(tmp_path):
    app = create_app(str(tmp_path / "registry.json"))

    class FakeMissingLoader:
        async def load(self, template_id: str) -> StoredAdl14Opt:
            raise KehrnelError(
                code="OPENEHR_TEMPLATE_NOT_FOUND",
                status=404,
                message=f"Stored ADL 1.4 OPT '{template_id}' was not found.",
                details={"templateId": template_id, "templateFormat": "adl1.4"},
            )

    app.dependency_overrides[get_stored_adl14_opt_loader] = FakeMissingLoader
    response = TestClient(app).get(
        "/api/domains/openehr/semantics/templates/unknown/catalog"
    )

    assert response.status_code == 404
    assert response.json()["error"] == {
        "code": "OPENEHR_TEMPLATE_NOT_FOUND",
        "message": "Stored ADL 1.4 OPT 'unknown' was not found.",
        "details": {"templateId": "unknown", "templateFormat": "adl1.4"},
    }


@pytest.mark.asyncio
async def test_loader_maps_the_existing_template_service_not_found(monkeypatch):
    import kehrnel.api.domains.openehr.semantics.service as semantics_service

    async def fake_retrieve_template_by_id_and_format(**_kwargs):
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="not found")

    monkeypatch.setattr(
        semantics_service,
        "retrieve_template_by_id_and_format",
        fake_retrieve_template_by_id_and_format,
    )

    with pytest.raises(KehrnelError) as caught:
        await load_stored_adl14_opt("unknown", object())

    assert caught.value.code == "OPENEHR_TEMPLATE_NOT_FOUND"
    assert caught.value.status == 404
