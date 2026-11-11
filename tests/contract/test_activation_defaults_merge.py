from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_activation_merges_defaults_and_overrides_and_updates_hash(tmp_path):
    client = TestClient(create_app(str(tmp_path / "reg.json")))
    # initial activation with defaults
    res = client.post(
        "/v1/environments/env-merge/activate",
        json={
            "strategy_id": "openehr.rps_dual",
            "version": "0.2.0",
            "domain": "openEHR",
            "config": {"database": "kehrnel_contract_test"},
            "bindings": {},
            "allow_plaintext_bindings": True,
        },
    )
    assert res.status_code == 200
    act1 = res.json()["activation"]
    cfg1 = act1["config"]
    assert cfg1.get("collections", {}).get("compositions", {}).get("name") == "compositions_rps"
    hash1 = act1["config_hash"]

    # re-activate with override and force replace
    res = client.post(
        "/v1/environments/env-merge/activate",
        json={
            "strategy_id": "openehr.rps_dual",
            "version": "0.2.0",
            "domain": "openEHR",
            "config": {
                "database": "kehrnel_contract_test",
                "collections": {"search": {"atlas_index_name": "custom_idx"}},
            },
            "bindings": {},
            "allow_plaintext_bindings": True,
            "force": True,
            "reason": "override",
        },
    )
    assert res.status_code == 200
    act2 = res.json()["activation"]
    cfg2 = act2["config"]
    assert cfg2["collections"]["search"]["atlas_index_name"] == "custom_idx"
    hash2 = act2["config_hash"]
    assert hash2 and hash2 != hash1

    # compile uses activation config (custom index)
    res = client.post(
        "/v1/environments/env-merge/compile_query",
        json={
            "domain": "openEHR",
            "query": {
                "scope": "cross_patient",
                "predicates": [{"path": "text", "op": "eq", "value": "hello"}],
                "select": [{"path": "ehr_id", "alias": "ehr_id"}],
            },
        },
        params={"debug": "true"},
    )
    assert res.status_code == 200
    plan = res.json()["result"]["plan"]
    search_stage = plan["pipeline"][0]["$search"]
    assert search_stage["index"] == "custom_idx"

    # The release-blocking IBM profile also merges its exact-model defaults.
    res = client.post(
        "/v1/environments/env-single/activate",
        json={
            "strategy_id": "openehr.rps_dual_ibm",
            "version": "0.2.0",
            "domain": "openEHR",
            "config": {"database": "kehrnel_contract_test"},
            "bindings": {},
            "allow_plaintext_bindings": True,
        },
    )
    assert res.status_code == 200
    act_ibm = res.json()["activation"]
    assert act_ibm["config"]["collections"]["compositions"]["name"] == "compositions_rps"
    assert act_ibm["config"]["fields"]["node"]["pi"] == "li"
    assert act_ibm["config_hash"]
