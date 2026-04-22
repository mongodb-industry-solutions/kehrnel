from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from kehrnel.api.domains.openehr.ehr import repository, service


class _FakeDeleteResult:
    def __init__(self, deleted_count: int = 1):
        self.deleted_count = deleted_count


class _FakeCollection:
    def __init__(self):
        self.deleted_many = []
        self.deleted_one = []

    async def delete_many(self, filter_query, session=None):
        self.deleted_many.append(deepcopy(filter_query))
        return _FakeDeleteResult()

    async def delete_one(self, filter_query, session=None):
        self.deleted_one.append(deepcopy(filter_query))
        return _FakeDeleteResult()


class _FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def start_transaction(self):
        return self


class _FakeClient:
    async def start_session(self):
        return _FakeSession()


class _FakeDb:
    def __init__(self):
        self.client = _FakeClient()
        self._collections = {}

    def __getitem__(self, name):
        if name not in self._collections:
            self._collections[name] = _FakeCollection()
        return self._collections[name]


@pytest.mark.asyncio
async def test_delete_ehr_service_collects_related_ids(monkeypatch):
    captured = {}

    async def fake_retrieve_ehr_by_id(ehr_id, db):
        return SimpleNamespace(
            compositions=[
                SimpleNamespace(id=SimpleNamespace(value="comp-1")),
                SimpleNamespace(id=SimpleNamespace(value="comp-2")),
            ],
            contributions=[
                SimpleNamespace(id=SimpleNamespace(value="contrib-1")),
                SimpleNamespace(id=SimpleNamespace(value="contrib-2")),
            ],
        )

    async def fake_delete_ehr_and_related_documents(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(service, "retrieve_ehr_by_id", fake_retrieve_ehr_by_id)
    monkeypatch.setattr(service, "delete_ehr_and_related_documents", fake_delete_ehr_and_related_documents)

    await service.delete_ehr("ehr-1", object())

    assert captured["ehr_id"] == "ehr-1"
    assert captured["composition_ids"] == ["comp-1", "comp-2"]
    assert captured["contribution_ids"] == ["contrib-1", "contrib-2"]


@pytest.mark.asyncio
async def test_delete_ehr_repository_removes_all_related_records(monkeypatch):
    db = _FakeDb()

    monkeypatch.setattr(repository.settings, "COMPOSITIONS_COLL_NAME", "compositions")
    monkeypatch.setattr(repository.settings, "FLAT_COMPOSITIONS_COLL_NAME", "compositions_rps")
    monkeypatch.setattr(repository.settings, "SEARCH_COMPOSITIONS_COLL_NAME", "compositions_search")
    monkeypatch.setattr(repository.settings, "EHR_CONTRIBUTIONS_COLL", "contributions")
    monkeypatch.setattr(repository.settings, "EHR_COLL_NAME", "ehr")

    await repository.delete_ehr_and_related_documents(
        ehr_id="ehr-1",
        composition_ids=["comp-1", "comp-2"],
        contribution_ids=["contrib-1", "contrib-2"],
        db=db,
    )

    assert db["compositions"].deleted_many == [
        {"$or": [{"ehr_id": "ehr-1"}, {"_id": {"$in": ["comp-1", "comp-2"]}}]}
    ]
    assert db["compositions_rps"].deleted_many == [
        {"$or": [{"ehr_id": "ehr-1"}, {"_id": {"$in": ["comp-1", "comp-2"]}}]}
    ]
    assert db["compositions_search"].deleted_many == [
        {
            "$or": [
                {"ehr_id": "ehr-1"},
                {"_id": "ehr-1"},
                {"_id": {"$in": ["comp-1", "comp-2"]}},
                {"comp_id": {"$in": ["comp-1", "comp-2"]}},
                {"comps.comp_id": {"$in": ["comp-1", "comp-2"]}},
            ]
        }
    ]
    assert db["contributions"].deleted_many == [
        {"$or": [{"ehr_id": "ehr-1"}, {"_id": {"$in": ["contrib-1", "contrib-2"]}}]}
    ]
    assert db["ehr"].deleted_one == [{"_id.value": "ehr-1"}]
