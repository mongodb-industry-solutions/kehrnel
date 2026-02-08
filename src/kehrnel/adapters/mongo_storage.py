from __future__ import annotations

from typing import Iterable, Optional

from kehrnel.persistence.mongo import MongoStore


class MongoStorageAdapter:
    """
    Thin adapter over MongoStore to satisfy the legacy StrategyBindings contract.
    """

    def __init__(self, store: MongoStore, search_collection_name: Optional[str] = None):
        self.store = store
        self.search_collection_name = search_collection_name

    @classmethod
    def from_config(cls, cfg: dict) -> "MongoStorageAdapter":
        store = MongoStore(cfg)
        store.connect()
        return cls(store)

    def insert_one(self, doc: dict, *, search: bool | None = None):
        return self.store.insert_one(doc, search=bool(search))

    def insert_many(self, docs: Iterable[dict], *, search: bool | None = None):
        return self.store.insert_many(docs, workers=4)
