from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

import kehrnel.api.domains.openehr.composition.routes as composition_routes
from kehrnel.api.domains.openehr.composition.models import BulkCompositionCreateResult
from kehrnel.api.domains.openehr.composition.models import BulkCompositionDeleteResult
from kehrnel.api.domains.openehr.composition.routes import (
    get_flattener,
    get_composition_config,
    get_mongodb_ehr_db,
    router,
)


def test_delete_composition_returns_empty_204_with_headers(monkeypatch):
    app = FastAPI()
    app.include_router(router, prefix="/api/domains/openehr")
    app.dependency_overrides[get_mongodb_ehr_db] = lambda: object()
    app.dependency_overrides[get_composition_config] = lambda: SimpleNamespace()

    async def fake_delete_composition_by_preceding_uid(**_kwargs):
        return {
            "new_audit_uid": "comp-1::my-openehr-server::2",
            "time_committed": datetime(2026, 4, 14, 20, 20, tzinfo=timezone.utc),
            "versioned_object_locator": "comp-1::my-openehr-server",
        }

    monkeypatch.setattr(
        composition_routes,
        "delete_composition_by_preceding_uid",
        fake_delete_composition_by_preceding_uid,
    )

    client = TestClient(app)
    response = client.delete(
        "/api/domains/openehr/ehr/ehr-1/composition/comp-1::my-openehr-server::1",
        headers={"If-Match": '"comp-1::my-openehr-server::1"'},
    )

    assert response.status_code == 204
    assert response.text == ""
    assert response.headers["etag"] == '"comp-1::my-openehr-server::2"'
    assert response.headers["location"] == "/v1/ehr/ehr-1/composition/comp-1::my-openehr-server"
    assert response.headers["last-modified"] == "Tue, 14 Apr 2026 20:20:00 GMT"


def test_bulk_delete_compositions_returns_summary(monkeypatch):
    app = FastAPI()
    app.include_router(router, prefix="/api/domains/openehr")
    app.dependency_overrides[get_mongodb_ehr_db] = lambda: object()
    app.dependency_overrides[get_composition_config] = lambda: SimpleNamespace()

    async def fake_bulk_delete_compositions(**_kwargs):
        return BulkCompositionDeleteResult(
            ehr_id="ehr-1",
            deletedCount=2,
            deletedUids=[
                "comp-1::my-openehr-server::1",
                "comp-2::my-openehr-server::1",
            ],
            auditUids=[
                "comp-1::my-openehr-server::2",
                "comp-2::my-openehr-server::2",
            ],
            failed=[],
            committedAt=datetime(2026, 4, 15, 10, 0, tzinfo=timezone.utc),
        )

    monkeypatch.setattr(
        composition_routes,
        "bulk_delete_compositions",
        fake_bulk_delete_compositions,
    )

    client = TestClient(app)
    response = client.post(
        "/api/domains/openehr/ehr/ehr-1/composition/$bulk-delete",
        json={"uids": ["comp-1::my-openehr-server::1", "comp-2::my-openehr-server::1"]},
    )

    assert response.status_code == 200
    assert response.json()["deletedCount"] == 2
    assert response.json()["deletedUids"] == [
        "comp-1::my-openehr-server::1",
        "comp-2::my-openehr-server::1",
    ]


def test_bulk_create_compositions_returns_summary(monkeypatch):
    app = FastAPI()
    app.include_router(router, prefix="/api/domains/openehr")
    app.dependency_overrides[get_mongodb_ehr_db] = lambda: object()
    app.dependency_overrides[get_composition_config] = lambda: SimpleNamespace(merge_search_docs=False)
    app.dependency_overrides[get_flattener] = lambda: object()

    async def fake_bulk_add_compositions(**_kwargs):
        return BulkCompositionCreateResult(
            ehr_id="ehr-1",
            createdCount=2,
            created=[
                {"index": 0, "uid": "comp-1::my-openehr-server::1", "name": "One", "templateId": "template-a"},
                {"index": 1, "uid": "comp-2::my-openehr-server::1", "name": "Two", "templateId": "template-b"},
            ],
            failed=[],
            committedAt=datetime(2026, 4, 15, 11, 0, tzinfo=timezone.utc),
        )

    monkeypatch.setattr(
        composition_routes,
        "bulk_add_compositions",
        fake_bulk_add_compositions,
    )

    client = TestClient(app)
    response = client.post(
        "/api/domains/openehr/ehr/ehr-1/composition/$bulk-create",
        json={
            "items": [
                {"composition": {"_type": "COMPOSITION", "archetype_details": {"template_id": {"value": "template-a"}}}},
                {"composition": {"_type": "COMPOSITION", "archetype_details": {"template_id": {"value": "template-b"}}}},
            ]
        },
    )

    assert response.status_code == 200
    assert response.json()["createdCount"] == 2
    assert [item["uid"] for item in response.json()["created"]] == [
        "comp-1::my-openehr-server::1",
        "comp-2::my-openehr-server::1",
    ]
