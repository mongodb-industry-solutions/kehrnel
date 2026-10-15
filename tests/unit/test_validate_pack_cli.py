from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
FHIR_PACK = ROOT / "src" / "kehrnel" / "engine" / "strategies" / "fhir" / "resource_store"


def _isolated_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment["KEHRNEL_SKIP_DOTENV"] = "true"
    environment.pop("FHIR_TEST_MONGODB_URI", None)
    environment.pop("FHIR_TEST_DB", None)
    return environment


def test_installed_cli_validates_specification_layout_with_runtime_identity():
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kehrnel.cli.unified",
            "common",
            "validate-pack",
            "--",
            str(FHIR_PACK),
            "--json",
        ],
        cwd=ROOT,
        env=_isolated_environment(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["valid"] is True
    assert payload["strategyId"] == "fhir.resource_store"
    assert payload["manifestPath"].endswith("specification/manifest.json")
    assert payload["packageDigest"].startswith("sha256:")
    assert payload["contractDigest"].startswith("sha256:")


def test_validate_pack_cli_emits_structured_root_diagnostic(tmp_path):
    (tmp_path / "manifest.json").write_text("[]", encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kehrnel.cli.validate_pack",
            str(tmp_path),
            "--json",
        ],
        cwd=ROOT,
        env=_isolated_environment(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["valid"] is False
    assert payload["diagnostics"][0]["code"] == "MANIFEST_SCHEMA_INVALID"
    assert payload["diagnostics"][0]["path"] == "/"
