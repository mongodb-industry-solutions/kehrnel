from kehrnel.strategies.openehr.rps_dual.query.path_resolver import PathResolver


def test_path_resolver_uses_path_codec_for_tokens():
    cfg = {
        "fields": {"composition": {"nodes": "cn"}, "search": {"nodes": "sn"}},
        "paths": {"separator": "/"},
    }
    shortcuts = {"data": "d"}
    resolver = PathResolver(cfg, shortcuts=shortcuts)
    rp = resolver.resolve_full("/content[openEHR-EHR-COMPOSITION.v1]/items[at0001]/value", scope="patient")
    assert rp.cn_regex.startswith("^")
    # token joiner should respect separator
    assert "/" in resolver.token_joiner


def test_path_resolver_data_paths():
    cfg = {
        "fields": {
            "composition": {"nodes": "cn", "ehr_id": "ehr_id"},
            "search": {"nodes": "sn", "ehr_id": "ehr_id"},
        },
        "paths": {"separator": "."},
    }
    resolver = PathResolver(cfg, shortcuts={"value": "v"})
    rp = resolver.resolve_full("/data[at0001]/value", scope="cross_patient")
    assert rp.sn_data_path.startswith("data.")
