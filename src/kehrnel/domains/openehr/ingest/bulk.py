# ingest/bulk.py  – trimmed down from the old ingestion.py
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterator, Dict, Any

from kehrnel.persistence import get_driver        # entry-point loader
from kehrnel.legacy.transform.core import Transformer, load_default_cfg

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
    from kehrnel.persistence.mongo import MongoSource   # thin wrapper around PyMongo
    from kehrnel.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
    source = MongoSource(json.loads(src_cfg.read_text()), limit=limit)
    drv = get_driver(driver_cfg)
    drv.connect()
    flattener = CompositionFlattener(
        db=None,
        config={
            "role": "primary",
            "apply_shortcuts": False,
            "paths": {"separator": "."},
            "collections": {},
        },
        mappings_path=str(
            Path(__file__).resolve().parents[3]
            / "strategies"
            / "openehr"
            / "rps_dual"
            / "ingest"
            / "config"
            / "flattener_mappings_f.jsonc"
        ),
        mappings_content=None,
        coding_opts={"arcodes": {"strategy": "literal"}, "atcodes": {"strategy": "literal"}},
    )

    for raw in source.iter_compositions():
        try:
            base, search = flattener.transform_composition(raw)
            drv.insert_one(base)
            if search and search.get("sn"):
                drv.insert_one(search, search=True)
        except ValueError as e:
            log.warning("skip %s – %s", raw["_id"], e)
