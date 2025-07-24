# kehrnel/persistence/memory.py

from typing import List, Dict, Any, Iterator

class MemoryPersister:
    def __init__(self):
        self._comps = []
        self._search = []
        self._used = []

    def claim_patient_id(self) -> str:
        if not self._used:
            return None
        eid = self._used.pop(0)
        return eid

    def next_patient_ids(self, limit:int) -> Iterator[str]:
        for _ in range(limit):
            eid = self.claim_patient_id()
            if eid is None: break
            yield eid

    def fetch_raw(self, ehr_id:str) -> List[Dict[str,Any]]:
        return [d for d in self._comps if d["ehr_id"]==ehr_id]

    def insert_compositions(self, docs: List[Dict[str,Any]])->int:
        self._comps.extend(docs); return len(docs)

    def insert_search(self, docs: List[Dict[str,Any]])->int:
        self._search.extend(docs); return len(docs)

    def reset_used_flags(self, batch:int):
        # no-op
        pass

    def bootstrap_codes(self): pass
    def start_refresh_loops(self, role, interval): pass
    def flush_codes(self): pass
    def clean_target_collections(self): pass
    def close(self): pass