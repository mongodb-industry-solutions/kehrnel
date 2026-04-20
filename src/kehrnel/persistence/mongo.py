# persistence/mongo.py
from __future__ import annotations
import certifi, json, logging
from pymongo import MongoClient
from pathlib import Path
from typing import Iterable, Iterator, Optional

log = logging.getLogger(__name__)


def _search_doc_key(doc: dict) -> str | None:
    if "sn" in doc:
        return "sn"
    if "search_nodes" in doc:
        return "search_nodes"
    return None


def _is_non_empty_search_doc(doc: dict) -> bool:
    key = _search_doc_key(doc)
    if key is None:
        return False
    return bool(doc.get(key))

class MongoStore:
    def __init__(self, cfg: dict):
        self.cfg  = cfg
        self.stats= type("S", (), {"inserted": 0})()

    def connect(self):
        uri = self.cfg["connection_string"]
        cli = MongoClient(uri, tlsCAFile=certifi.where())
        db  = cli[self.cfg["database_name"]]
        self.col_base   = db[self.cfg["compositions_collection"]]
        self.col_search = db[self.cfg["search_collection"]]

    # --- public API expected by ingest.bulk --------------------------------
    def insert_one(self, doc: dict, *, search=False):
        self._target(search).insert_one(doc)
        self.stats.inserted += 1

    def insert_many(self, docs, workers: int = 4):
        # naive implementation – one thread, chunked
        BATCH = 1000
        buf = []
        for d in docs:
            buf.append(d)
            if len(buf) == BATCH:
                self._flush(buf)
                buf.clear()
        self._flush(buf)

    # -----------------------------------------------------------------------
    def _target(self, search=False):
        return self.col_search if search else self.col_base

    def _flush(self, buf):
        if not buf:
            return
        base_docs = [doc for doc in buf if _search_doc_key(doc) is None]
        search_docs = [doc for doc in buf if _is_non_empty_search_doc(doc)]
        try:
            if base_docs:
                self.col_base.insert_many(base_docs, ordered=False)
                self.stats.inserted += len(base_docs)
            if search_docs:
                self.col_search.insert_many(search_docs, ordered=False)
                self.stats.inserted += len(search_docs)
        except Exception as exc:
            log.error("Bulk insert failed: %s", exc)


class MongoSource:
    """
    Thin reader wrapper to iterate compositions from a Mongo collection.
    """

    def __init__(self, cfg: dict, limit: Optional[int] = None):
        self.cfg = cfg
        self.limit = limit
        uri = cfg["connection_string"]
        cli = MongoClient(uri, tlsCAFile=certifi.where())
        db = cli[cfg["database_name"]]
        col_name = (
            cfg.get("source_collection")
            or cfg.get("compositions_collection")
            or cfg.get("collection")
        )
        if not col_name:
            raise ValueError("MongoSource requires a collection name")
        self.collection = db[col_name]

    def iter_compositions(self) -> Iterator[dict]:
        cursor = self.collection.find({})
        if self.limit:
            cursor = cursor.limit(self.limit)
        yield from cursor
