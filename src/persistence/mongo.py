# persistence/mongo.py
from __future__ import annotations
import certifi, json, logging
from pymongo import MongoClient
from pathlib import Path

log = logging.getLogger(__name__)

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
        if not buf: return
        try:
            self._target(buf[0].get("sn") is not None).insert_many(buf, ordered=False)
            self.stats.inserted += len(buf)
        except Exception as exc:
            log.error("Bulk insert failed: %s", exc)