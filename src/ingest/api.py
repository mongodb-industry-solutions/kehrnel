# kehrnel/ingest/api.py

from bson import ObjectId
from typing import Dict, Any, List
from ..transform.core import Transformer

class IngestAPI:
    def __init__(self, persister):
        self.p = persister
        self.role = persister.cfg["role"]

    def claim_patient(self) -> str:
        return self.p.claim_patient_id()

    def ingest_one(self, raw: Dict[str,Any], replicate: int=1) -> List[ObjectId]:
        t = Transformer(
            mappings_path=self.p.cfg["transform"]["mappings_yaml"],
            shortcuts_path=self.p.cfg["transform"].get("shortcuts_jsonc"),
            role=self.role
        )
        base, slim = t.flatten(raw)
        out = []
        for i in range(replicate):
            new_id = ObjectId()
            d = base.copy(); d["_id"]=new_id
            self.p.insert_compositions([d])
            if slim.get("sn"):
                s = slim.copy(); s["_id"]=new_id
                self.p.insert_search([s])
            out.append(new_id)
        return out

    def close(self):
        self.p.close()