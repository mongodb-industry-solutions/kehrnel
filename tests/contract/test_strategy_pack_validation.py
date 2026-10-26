import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import validate_strategy_pack, create_app


def test_all_discovered_strategies_validate(tmp_path):
    app = create_app(str(tmp_path / "registry.json"))
    runtime = app.state.strategy_runtime
    strategies = runtime.list_strategies()
    assert {item.id for item in strategies} == {
        "cdisc.sdr",
        "contextobjects.rps",
        "fhir.resource_store",
        "genomics.variant_first",
        "openehr.rps_dual",
        "openehr.rps_dual_ibm",
        "snomedct.mongodb",
        "x12.co_single",
    }
    assert not [item for item in app.state.strategy_diagnostics if not item["is_valid"]]
    assert all(item.package_digest for item in strategies)
    assert all(item.contract_digest for item in strategies)

    response = TestClient(app).get("/strategies")
    assert response.status_code == 200
    wire_by_id = {item["id"]: item for item in response.json()["strategies"]}
    for manifest in strategies:
        wire = wire_by_id[manifest.id]
        assert wire["packageDigest"] == manifest.package_digest
        assert wire["contractDigest"] == manifest.contract_digest


def test_invalid_strategy_pack_reports_errors(tmp_path, monkeypatch):
    # build a fake strategies path with an incomplete manifest
    base = tmp_path / "strategies" / "fake"
    base.mkdir(parents=True, exist_ok=True)
    manifest = {"id": "fake.invalid", "version": "0.0.1", "domain": "fake", "ops": [{"name": "op1"}], "entrypoint": "does.not.exist:Strategy"}
    (base / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    errors = validate_strategy_pack(manifest, base)
    assert errors
    assert any("input_schema" in e for e in errors)
    assert any("entrypoint module not found" in e for e in errors)
