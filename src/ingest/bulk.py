# ingest/bulk.py  – trimmed down from the old ingestion.py
from __future__ import annotations
import json, logging, threading
from pathlib import Path
from typing import Iterator, Dict, Any
import certifi

from persistence import get_driver        # entry-point loader
from transform.core import Transformer

log = logging.getLogger(__name__)

def run(transformer: Transformer,
        jsonl: Path,
        driver_cfg: Path,
        workers: int = 4):
    """Ingest an NDJSON file already in **flattened** shape."""

    drv = get_driver(driver_cfg)          # e.g. MongoStore
    drv.connect()

    def producer() -> Iterator[Dict[str, Any]]:
        with jsonl.open("r", encoding="utf-8") as fh:
            for line in fh:
                yield json.loads(line)

    drv.insert_many(producer(), workers=workers)
    log.info("✓ done – inserted %d docs", drv.stats.inserted)

# optional helper to mimic the old “pull-from-source Mongo” pipeline
def from_mongo(src_cfg: Path, driver_cfg: Path, limit: int | None):
    from persistence.mongo import MongoSource   # thin wrapper around PyMongo
    src = MongoSource(json.loads(src_cfg.read_text()), limit=limit)

    tf  = Transformer(json.loads(Path("transform/config/default_config.json")
                                 .read_text(encoding="utf-8")))
    drv = get_driver(driver_cfg)
    drv.connect()

    for raw in src.iter_compositions():
        try:
            docs = tf.flatten(raw)
            drv.insert_one(docs["base"])
            if "search" in docs:
                drv.insert_one(docs["search"], search=True)
        except ValueError as e:
            log.warning("skip %s – %s", raw["_id"], e)