import os
from pathlib import Path

import pytest

from kehrnel.api.app import create_app


def test_manifest_discovery_failfast(tmp_path: Path, monkeypatch):
    bad_dir = tmp_path / "bad"
    bad_dir.mkdir()
    (bad_dir / "manifest.json").write_text("{bad json}", encoding="utf-8")
    monkeypatch.setenv("KEHRNEL_STRATEGY_PATHS", str(bad_dir))
    app = create_app(str(tmp_path / "reg.json"))
    diags = app.state.strategy_diagnostics
    assert diags
    bad_entry = next((d for d in diags if str(bad_dir) in d["paths"]["base_dir"]), None)
    assert bad_entry is not None
    assert bad_entry["is_valid"] is False
    assert bad_entry["validation_errors"]
