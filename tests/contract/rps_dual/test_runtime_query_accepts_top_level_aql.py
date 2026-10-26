import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    return TestClient(app)


def test_compile_query_accepts_top_level_and_nested_aql_payloads(client):
    activation = client.post(
        "/v1/environments/envAql/activate",
        json={
            "strategy_id": "openehr.rps_dual",
            "version": "0.2.0",
            "config": {"database": "kehrnel_contract_test"},
            "bindings": {},
            "allow_plaintext_bindings": True,
            "domain": "openEHR",
        },
    )
    assert activation.status_code == 200, activation.text
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

    nested = client.post(
        "/v1/environments/envAql/compile_query",
        json={
            "domain": "openEHR",
            "query": {
                "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c WHERE e/ehr_id/value = 'p1'",
            },
        },
    )
    assert nested.status_code == 200, nested.text
    assert nested.json()["result"]["plan"]["pipeline"] == pipeline

    nested = client.post(
        "/v1/environments/envAql/compile_query",
        json={
            "domain": "openEHR",
            "query": {
                "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c WHERE e/ehr_id/value = 'p1'",
            },
        },
    )
    assert nested.status_code == 200, nested.text
    assert nested.json()["result"]["plan"]["pipeline"] == pipeline

    nested = client.post(
        "/v1/environments/envAql/compile_query",
        json={
            "domain": "openEHR",
            "query": {
                "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c WHERE e/ehr_id/value = 'p1'",
            },
        },
    )
    assert nested.status_code == 200, nested.text
    assert nested.json()["result"]["plan"]["pipeline"] == pipeline

    nested = client.post(
        "/v1/environments/envAql/compile_query",
        json={
            "domain": "openEHR",
            "query": {
                "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c WHERE e/ehr_id/value = 'p1'",
            },
        },
    )
    assert nested.status_code == 200, nested.text
    assert nested.json()["result"]["plan"]["pipeline"] == pipeline

    nested = client.post(
        "/v1/environments/envAql/compile_query",
        json={
            "domain": "openEHR",
            "query": {
                "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c WHERE e/ehr_id/value = 'p1'",
            },
        },
    )
    assert nested.status_code == 200, nested.text
    assert nested.json()["result"]["plan"]["pipeline"] == pipeline
