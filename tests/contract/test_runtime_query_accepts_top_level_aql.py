import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    return TestClient(app)


def test_compile_query_accepts_top_level_aql_payload(client):
    client.post(
        "/v1/environments/envAql/activate",
        json={
            "strategy_id": "openehr.rps_dual",
            "version": "0.1.0",
            "config": {},
            "bindings": {},
            "allow_plaintext_bindings": True,
            "domain": "openEHR",
        },
    )
    res = client.post(
        "/v1/environments/envAql/compile_query",
        json={
            "domain": "openEHR",
            "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c WHERE e/ehr_id/value = 'p1'",
            "debug": True,
        },
        params={"debug": "true"},
    )
    assert res.status_code == 200
    pipeline = res.json()["result"]["plan"]["pipeline"]
    assert pipeline and "$match" in pipeline[0]
