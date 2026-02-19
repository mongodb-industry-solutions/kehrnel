import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


@pytest.fixture
def client(tmp_path, monkeypatch):
    invalid_path = Path("tests/fixtures/invalid_strategy").resolve()
    existing = os.getenv("KEHRNEL_STRATEGY_PATHS", "")
    extra = f"{existing}:{invalid_path}" if existing else str(invalid_path)
    monkeypatch.setenv("KEHRNEL_STRATEGY_PATHS", extra)
    app = create_app(str(tmp_path / "reg.json"))
    return TestClient(app)


def test_diagnostics_includes_valid_and_invalid(client):
    res = client.get("/strategies/diagnostics")
    assert res.status_code == 200
    entries = res.json()["strategies"]
    assert entries
    # shipped strategies should be valid
    valid_entries = [e for e in entries if e.get("id") and (e["id"].startswith("openehr") or e["id"].startswith("fhir"))]
    assert all(e["is_valid"] for e in valid_entries)
    # invalid fixture should be present with errors
    invalid = next((e for e in entries if e.get("id") == "invalid.strategy"), None)
    assert invalid is not None
    assert invalid["is_valid"] is False
    assert invalid["validation_errors"]
    assert "manifest_path" in invalid["paths"]
    assert "entrypoint" in invalid
