# kehrnel/persistence/mongo.py

import threading, time, certifi
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

class MongoPersister:
    def __init__(self, src_cfg: dict, tgt_cfg: dict):
        ca = certifi.where()
        self.src = MongoClient(src_cfg["connection_string"], tlsCAFile=ca)[
            src_cfg["database_name"]][src_cfg["collection_name"]]
        tgt_cli = MongoClient(tgt_cfg["connection_string"], tlsCAFile=ca)
        db = tgt_cli[tgt_cfg["database_name"]]
        self.coll_full   = db[tgt_cfg["compositions_collection"]]
        self.coll_search = db[tgt_cfg["search_collection"]]
        self.codes_col   = db[tgt_cfg["codes_collection"]]
        self.cfg = {"source":src_cfg, "target":tgt_cfg, "role":tgt_cfg.get("role","primary")}

    def bootstrap_codes(self):
        self.codes_col.update_one({"_id":"ar_code"},
                                  {"$setOnInsert":{"_id":"ar_code"}}, upsert=True)
        self.codes_col.update_one({"_id":"sequence"},
                                  {"$setOnInsert":{"_id":"sequence","seq":{}}}, upsert=True)

    def start_refresh_loops(self, role: str, interval: int):
        if role=="primary":
            threading.Thread(target=self._flush_loop, args=(interval,), daemon=True).start()
        else:
            threading.Thread(target=self._reload_loop, args=(interval,), daemon=True).start()

    def _flush_loop(self, interval:int):
        from ..transform.at_code_codec import CODE_BOOK, SEQ
        while True:
            time.sleep(interval)
            nested = {"_id":"ar_code", **{"at":CODE_BOOK["at"], "_min":SEQ["at"], **{}}}
            self.codes_col.replace_one({"_id":"ar_code"}, nested, upsert=True)
    def _reload_loop(self, interval:int):
        from ..transform.at_code_codec import load_codes_from_db
        while True:
            time.sleep(interval)
            load_codes_from_db(self.codes_col)

    def claim_patient_id(self) -> str:
        # copy your claim_patient_id() here, using self.src
        raise NotImplementedError

    def next_patient_ids(self, limit:int):
        # generator wrapping claim_patient_id
        raise NotImplementedError

    def fetch_raw(self, ehr_id:str):
        return list(self.src.find({"ehr_id":ehr_id}))

    def insert_compositions(self, docs: List[dict]) -> int:
        if not docs: return 0
        try:
            return len(self.coll_full.insert_many(docs, ordered=False).inserted_ids)
        except BulkWriteError as bwe:
            dup = sum(1 for e in bwe.details["writeErrors"] if e["code"]==11000)
            return len(docs)-dup

    def insert_search(self, docs: List[dict]) -> int:
        if not docs: return 0
        try:
            return len(self.coll_search.insert_many(docs, ordered=False).inserted_ids)
        except BulkWriteError as bwe:
            dup = sum(1 for e in bwe.details["writeErrors"] if e["code"]==11000)
            return len(docs)-dup

    def flush_codes(self):
        # same as bootstrap_codes + replace_one on ar_code and sequence
        raise NotImplementedError

    def clean_target_collections(self):
        self.coll_full.drop(); self.coll_search.drop()

    def reset_used_flags(self, batch:int):
        # copy your reset_used() here against self.src
        raise NotImplementedError

    def close(self):
        # close clients if held
        pass