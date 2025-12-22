import os
from pathlib import Path

import pytest

from kehrnel.api.app import create_app


def test_manifest_discovery_failfast(tmp_path: Path, monkeypatch):
    bad_dir = tmp_path / "bad"
    bad_dir.mkdir()
    (bad_dir / "manifest.json").write_text("{bad json}", encoding="utf-8")
    monkeypatch.setenv("KEHRNEL_STRATEGY_PATHS", str(bad_dir))
    with pytest.raises(Exception):
        create_app(str(tmp_path / "reg.json"))
