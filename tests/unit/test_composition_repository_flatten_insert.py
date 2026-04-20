from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from kehrnel.api.domains.openehr.composition.repository import (
    add_bulk_deletion_contributions_and_update_ehr,
    insert_composition_contribution_and_update_ehr,
    insert_compositions_contributions_and_update_ehr,
)


class _FakeInsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class _FakeUpdateResult:
    def __init__(self, matched_count: int = 1):
        self.matched_count = matched_count


class _FakeCollection:
    def __init__(self):
        self.inserted = []
        self.inserted_many = []
        self.updated = []
        self.deleted = []

    async def insert_one(self, document, session=None):
        self.inserted.append(deepcopy(document))
        return _FakeInsertResult(document.get("_id"))

    async def insert_many(self, documents, ordered=True, session=None):
        self.inserted_many.append(
            {
                "documents": deepcopy(documents),
                "ordered": ordered,
            }
        )
        return _FakeInsertResult(None)

    async def update_one(self, filter_query, update_operation, upsert=False, session=None):
        self.updated.append(
            {
                "filter": deepcopy(filter_query),
                "update": deepcopy(update_operation),
                "upsert": upsert,
            }
        )
        return _FakeUpdateResult()

    async def delete_many(self, filter_query, session=None):
        self.deleted.append(deepcopy(filter_query))
        return _FakeUpdateResult()


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


def _config():
    return SimpleNamespace(
        contributions="contributions",
        compositions="compositions",
        flatten_compositions="compositions_rps",
        search_compositions="compositions_search",
        ehr="ehr",
        search_fields=SimpleNamespace(nodes="sn", template_id="tid"),
    )


@pytest.mark.asyncio
async def test_inserts_flattened_base_even_without_search_nodes():
    db = _FakeDb()
    config = _config()

    await insert_composition_contribution_and_update_ehr(
        ehr_id="ehr-1",
        composition_doc={"_id": "comp-1::server::1"},
        contribution_doc={"_id": "contrib-1"},
        db=db,
        config=config,
        flattened_base_doc={"cn": [{"p": "1", "data": {"name": {"value": "Example"}}}]},
        flattened_search_doc={},
        merge_search_docs=False,
    )

    assert len(db["compositions_rps"].inserted) == 1
    assert db["compositions_rps"].inserted[0]["_id"] == "comp-1::server::1"
    assert db["compositions_search"].inserted == []


@pytest.mark.asyncio
async def test_inserts_search_document_when_search_nodes_are_present():
    db = _FakeDb()
    config = _config()

    await insert_composition_contribution_and_update_ehr(
        ehr_id="ehr-1",
        composition_doc={"_id": "comp-2::server::1"},
        contribution_doc={"_id": "contrib-2"},
        db=db,
        config=config,
        flattened_base_doc={"cn": [{"p": "1"}]},
        flattened_search_doc={"sn": [{"p": "1"}], "tid": "template-a"},
        merge_search_docs=False,
    )

    assert len(db["compositions_rps"].inserted) == 1
    assert len(db["compositions_search"].inserted) == 1
    assert db["compositions_search"].inserted[0]["_id"] == "comp-2::server::1"


@pytest.mark.asyncio
async def test_bulk_insert_updates_ehr_once_and_inserts_flattened_documents():
    db = _FakeDb()
    config = _config()

    await insert_compositions_contributions_and_update_ehr(
        ehr_id="ehr-1",
        composition_docs=[
            {"_id": "comp-1::server::1"},
            {"_id": "comp-2::server::1"},
        ],
        contribution_docs=[
            {"_id": "contrib-1"},
            {"_id": "contrib-2"},
        ],
        db=db,
        config=config,
        flattened_base_docs=[
            {"cn": [{"p": "1"}]},
            {"cn": [{"p": "2"}]},
        ],
        flattened_search_docs=[
            {"sn": [{"p": "1"}], "tid": "template-a"},
            {"sn": [{"p": "2"}], "tid": "template-b"},
        ],
        merge_search_docs=False,
    )

    assert db["contributions"].inserted_many == [
        {
            "documents": [{"_id": "contrib-1"}, {"_id": "contrib-2"}],
            "ordered": True,
        }
    ]
    assert db["compositions"].inserted_many == [
        {
            "documents": [
                {"_id": "comp-1::server::1", "ehr_id": "ehr-1"},
                {"_id": "comp-2::server::1", "ehr_id": "ehr-1"},
            ],
            "ordered": True,
        }
    ]
    assert db["compositions_rps"].inserted_many == [
        {
            "documents": [
                {"_id": "comp-1::server::1", "cn": [{"p": "1"}]},
                {"_id": "comp-2::server::1", "cn": [{"p": "2"}]},
            ],
            "ordered": True,
        }
    ]
    assert db["compositions_search"].inserted_many == [
        {
            "documents": [
                {"_id": "comp-1::server::1", "sn": [{"p": "1"}], "tid": "template-a"},
                {"_id": "comp-2::server::1", "sn": [{"p": "2"}], "tid": "template-b"},
            ],
            "ordered": True,
        }
    ]
    assert db["ehr"].updated == [
        {
            "filter": {"_id.value": "ehr-1"},
            "update": {
                "$push": {
                    "contributions": {
                        "$each": [
                            {"id": {"value": "contrib-1"}, "namespace": "local", "type": "CONTRIBUTION"},
                            {"id": {"value": "contrib-2"}, "namespace": "local", "type": "CONTRIBUTION"},
                        ]
                    },
                    "compositions": {
                        "$each": [
                            {"id": {"value": "comp-1::server::1"}, "namespace": "local", "type": "COMPOSITION"},
                            {"id": {"value": "comp-2::server::1"}, "namespace": "local", "type": "COMPOSITION"},
                        ]
                    }
                }
            },
            "upsert": False,
        }
    ]


@pytest.mark.asyncio
async def test_bulk_delete_updates_ehr_once_and_deletes_flattened_documents():
    db = _FakeDb()
    config = _config()

    await add_bulk_deletion_contributions_and_update_ehr(
        ehr_id="ehr-1",
        preceding_version_uids=["comp-1::server::1", "comp-2::server::1"],
        contribution_docs=[{"_id": "audit-1"}, {"_id": "audit-2"}],
        db=db,
        config=config,
    )

    assert db["contributions"].inserted_many == [
        {
            "documents": [{"_id": "audit-1"}, {"_id": "audit-2"}],
            "ordered": True,
        }
    ]
    assert db["compositions_rps"].deleted == [
        {"_id": {"$in": ["comp-1::server::1", "comp-2::server::1"]}}
    ]
    assert db["compositions_search"].deleted == [
        {"_id": {"$in": ["comp-1::server::1", "comp-2::server::1"]}}
    ]
    assert db["ehr"].updated == [
        {
            "filter": {"_id.value": "ehr-1"},
            "update": {
                "$push": {
                    "contributions": {
                        "$each": [
                            {"id": {"value": "audit-1"}, "namespace": "local", "type": "CONTRIBUTION"},
                            {"id": {"value": "audit-2"}, "namespace": "local", "type": "CONTRIBUTION"},
                        ]
                    }
                },
                "$pull": {
                    "compositions": {
                        "id.value": {"$in": ["comp-1::server::1", "comp-2::server::1"]}
                    }
                }
            },
            "upsert": False,
        }
    ]
