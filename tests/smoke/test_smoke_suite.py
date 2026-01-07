from pathlib import Path
import json

import pytest

from kehrnel.domains.openehr.templates.parser import TemplateParser
from kehrnel.strategies.openehr.rps_dual.ingest.core import Transformer
from kehrnel.common.mapping.mapping_engine import apply_mapping
from kehrnel.persistence import get_driver, MemoryPersister, MongoStore


SAMPLES = Path("samples")


def test_template_parser_loads_opt():
    opt_path = SAMPLES / "templates" / "whoqol_v0.opt"
    parser = TemplateParser(opt_path)
    assert parser.template_id
    assert parser.term_definitions  # should expose term map


def test_transformer_flattens_minimal(tmp_path):
    mappings = tmp_path / "mappings.yaml"
    shortcuts = tmp_path / "shortcuts.json"
    mappings.write_text('{"templates": {}}', encoding="utf-8")
    shortcuts.write_text("{}", encoding="utf-8")

    cfg = {"mappings": str(mappings), "shortcuts": str(shortcuts)}
    tf = Transformer(cfg=cfg, role="primary")

    raw = {
        "_id": "c1",
        "ehr_id": "e1",
        "canonicalJSON": {
            "_type": "COMPOSITION",
            "archetype_node_id": "openEHR-EHR-COMPOSITION.test.v1",
            "content": [
                {
                    "_type": "OBSERVATION",
                    "archetype_node_id": "openEHR-EHR-OBSERVATION.test.v1",
                    "data": {
                        "_type": "HISTORY",
                        "events": [
                            {
                                "_type": "POINT_EVENT",
                                "data": {
                                    "_type": "ITEM_TREE",
                                    "items": [
                                        {
                                            "_type": "ELEMENT",
                                            "archetype_node_id": "at0001",
                                            "value": {"value": "foo"},
                                        }
                                    ],
                                },
                            }
                        ],
                    },
                }
            ],
        },
    }

    docs = tf.flatten(raw)
    assert "base" in docs
    assert docs["base"]["ehr_id"] == "e1"
    assert docs["base"]["cn"]  # nodes collected


def test_mapping_engine_applies_rules():
    class DummyGen:
        def __init__(self):
            self.doc = {}

        def set_at_path(self, target, path, value):
            target[path] = value

    gen = DummyGen()
    flat_map = {"/context/language/code_string": {"literal": "es"}}
    composition = {}
    result = apply_mapping(gen, flat_map, composition)
    assert result["/context/language/code_string"] == "es"


def test_persistence_driver_resolution(tmp_path):
    mem = get_driver({"driver": "memory"})
    assert isinstance(mem, MemoryPersister)

    mongo_cfg = {
        "driver": "mongo",
        "connection_string": "mongodb://localhost:27017",
        "database_name": "testdb",
        "compositions_collection": "compositions",
        "search_collection": "search",
    }
    # YAML input should also work
    mongo_yaml = tmp_path / "mongo.yaml"
    mongo_yaml.write_text(
        json.dumps(mongo_cfg), encoding="utf-8"
    )
    drv = get_driver(mongo_yaml)
    assert isinstance(drv, MongoStore)
