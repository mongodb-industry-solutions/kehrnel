from __future__ import annotations

from kehrnel.persistence import MemoryPersister
from kehrnel.persistence.mongo import MongoStore


class _FakeCollection:
    def __init__(self):
        self.inserted_many = []

    def insert_many(self, documents, ordered=False):
        self.inserted_many.append(
            {
                "documents": list(documents),
                "ordered": ordered,
            }
        )


def test_memory_persister_skips_empty_search_sidecars():
    persister = MemoryPersister()
    persister.insert_many(
        [
            {"_id": "base-1", "cn": [{"p": "1"}]},
            {"_id": "search-empty", "sn": []},
            {"_id": "search-1", "sn": [{"p": "1"}]},
        ]
    )

    assert [doc["_id"] for doc in persister.base] == ["base-1"]
    assert [doc["_id"] for doc in persister.search] == ["search-1"]


def test_mongo_store_flush_partitions_base_and_search_docs():
    store = MongoStore({})
    store.col_base = _FakeCollection()
    store.col_search = _FakeCollection()

    store._flush(
        [
            {"_id": "base-1", "cn": [{"p": "1"}]},
            {"_id": "search-empty", "sn": []},
            {"_id": "search-1", "sn": [{"p": "1"}]},
        ]
    )

    assert store.col_base.inserted_many == [
        {
            "documents": [{"_id": "base-1", "cn": [{"p": "1"}]}],
            "ordered": False,
        }
    ]
    assert store.col_search.inserted_many == [
        {
            "documents": [{"_id": "search-1", "sn": [{"p": "1"}]}],
            "ordered": False,
        }
    ]
