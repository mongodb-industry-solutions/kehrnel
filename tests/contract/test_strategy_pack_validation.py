import json
from pathlib import Path

import pytest

from kehrnel.api.app import validate_strategy_pack, create_app


def test_all_discovered_strategies_validate():
    app = create_app()
    runtime = app.state.strategy_runtime
    assert runtime.list_strategies(), "expected at least one strategy loaded"


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
