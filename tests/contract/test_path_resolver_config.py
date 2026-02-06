import pytest

from kehrnel.strategies.openehr.rps_dual.query.path_resolver import PathResolver


def test_path_resolver_respects_custom_fields():
    cfg = {
        "fields": {
            "composition": {"nodes": "custom_cn", "path": "pp", "ehr_id": "ehr_custom", "comp_id": "cid_custom"},
            "search": {"nodes": "custom_sn", "path": "sp", "ehr_id": "sehr", "comp_id": "scid"},
        }
    }
    resolver = PathResolver(cfg)
    assert resolver.resolve("ehr_id", scope="patient") == "ehr_custom"
    assert resolver.resolve("comp_id", scope="patient") == "cid_custom"
    assert resolver.resolve("any", scope="patient") == "custom_cn"
    assert resolver.resolve("ehr_id", scope="cross_patient") == "sehr"
    assert resolver.resolve("comp_id", scope="cross_patient") == "scid"
    assert resolver.resolve("any", scope="cross_patient") == "custom_sn"
