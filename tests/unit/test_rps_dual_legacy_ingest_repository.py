from __future__ import annotations

from copy import deepcopy

import pytest

from kehrnel.api.strategies.openehr.rps_dual.ingest.repository import (
    IngestionRepository,
)


class _FakeCollection:
    def __init__(self):
        self.inserted: list[dict] = []
        self.updated_many: list[dict] = []

    async def insert_one(self, document, session=None):
        self.inserted.append({"document": deepcopy(document), "session": session})

    async def update_many(self, filter_query, update_operation, session=None):
        self.updated_many.append(
            {
                "filter": deepcopy(filter_query),
                "update": deepcopy(update_operation),
                "session": session,
            }
        )


class _FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def start_transaction(self):
        return self


class _FakeClient:
    def __init__(self):
        self.session = _FakeSession()

    async def start_session(self):
        return self.session


class _FakeDb:
    def __init__(self):
        self.client = _FakeClient()
        self.collections: dict[str, _FakeCollection] = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, _FakeCollection())


@pytest.mark.asyncio
async def test_legacy_flattened_ingest_invalidates_all_matching_receipts_atomically():
    target_db = _FakeDb()
    source_db = _FakeDb()
    repository = IngestionRepository(
        target_db,
        source_db,
        {
            "source": {"canonical_compositions_collection": "canonical"},
            "target": {
                "compositions_collection": "compositions_rps",
                "search_collection": "compositions_search",
            },
        },
    )

    await repository.insert_flattened_composition_in_transaction(
        {"ehr_id": "ehr-1", "cn": [{"p": "1"}]},
        {"ehr_id": "ehr-1", "sn": [{"p": "1"}]},
    )

    session = target_db.client.session
    assert target_db["compositions_rps"].inserted[0]["session"] is session
    assert target_db["compositions_search"].inserted[0]["session"] is session

    updates = target_db["_kehrnel_projection_state"].updated_many
    assert len(updates) == 1
    operation = updates[0]
    assert operation["session"] is session
    assert operation["filter"] == {
        "strategyId": "openehr.rps_dual",
        "sourceCollection": "compositions_rps",
        "targetCollection": "compositions_search",
    }
    assert "environmentId" not in operation["filter"]
    assert operation["update"]["$inc"] == {"mutationSequence": 1}
    invalidation = operation["update"]["$set"]
    assert invalidation["projection.state"] == "stale"
    assert invalidation["projection.complete"] is False
    assert invalidation["reconciliation.safe"] is False
    assert invalidation["reconciliation.complete"] is False
    assert invalidation["invalidation"]["reason"] == "legacy_http_flattened_insert"
    assert invalidation["invalidation"]["writer"] == "legacy-http"
