# kehrnel/ingest/bulk.py

import threading, time, logging, traceback
from multiprocessing import freeze_support
from bson import ObjectId

from ..transform.core import Transformer
from ..persistence.mongo import MongoPersister

log = logging.getLogger(__name__)

def run(cfg: dict):
    pers = MongoPersister(cfg["source"], cfg["target"])
    t    = Transformer(
        mappings_path=cfg["transform"]["mappings_yaml"],
        shortcuts_path=cfg["transform"].get("shortcuts_jsonc"),
        role=cfg["role"],
        codes_refresh_interval=cfg["codes_refresh_interval"]
    )

    pers.bootstrap_codes()
    pers.start_refresh_loops(cfg["role"], cfg["codes_refresh_interval"])

    if cfg.get("clean_collections"):
        pers.clean_target_collections()
    if cfg.get("reset_used_flags"):
        pers.reset_used_flags(cfg.get("batch_size_reset",2500))

    done = fail = total_read = total_ins = 0
    for idx, ehr_id in enumerate(pers.next_patient_ids(cfg["patient_limit"]), start=1):
        try:
            raws = pers.fetch_raw(ehr_id)
            docs_full, docs_search = [], []
            for r in raws:
                try:
                    base, slim = t.flatten(r)
                except Exception:
                    log.exception("transform failed for %s", ehr_id)
                    continue
                rf = cfg.get("replication_factor",1)
                for i in range(rf):
                    new_id = ObjectId()
                    d = base.copy(); d["_id"]=new_id; d["ehr_id"]=f"{ehr_id}~r{i+1}"
                    docs_full.append(d)
                    if slim.get("sn"):
                        s = slim.copy(); s["_id"]=new_id; s["ehr_id"]=d["ehr_id"]
                        docs_search.append(s)
            insf = pers.insert_compositions(docs_full)
            inss = pers.insert_search(docs_search)
            total_read += len(raws)
            total_ins  += insf
            done += 1
        except Exception:
            fail += 1
            log.error("✖ %s failed\n%s", ehr_id, traceback.format_exc())

        if idx % 10 == 0:
            log.info("… processed %d", idx)

    log.info("✓ done %d ✖ failed %d read %d ins %d",
             done, fail, total_read, total_ins)
    pers.flush_codes()