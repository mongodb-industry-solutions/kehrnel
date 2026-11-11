"""FHIR search must work after a non-editable install on another machine."""
import shutil
import pytest
import subprocess
import sys
import zipfile
from pathlib import Path


def test_fhir_search_wheel_contains_release_configs_and_compartments(tmp_path):
    pytest.importorskip("setuptools", reason="Wheel qualification requires optional build tools")
    pytest.importorskip("wheel", reason="Wheel qualification requires optional build tools")
    library = Path(__file__).resolve().parents[2] / "src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql"
    build_source = tmp_path / "library"
    shutil.copytree(library, build_source, ignore=shutil.ignore_patterns("build", "dist", "*.egg-info", "__pycache__", ".venv"))
    wheels = tmp_path / "wheels"
    subprocess.run([sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation", "--wheel-dir", str(wheels), str(build_source)], check=True, capture_output=True, text=True, timeout=60)
    wheel = next(wheels.glob("fhir_search_to_mql-*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    for release in ("r4", "r5"):
        assert f"fhir_search_to_mql/configs/{release}/Patient.yaml" in names
        assert f"fhir_search_to_mql/configs/{release}/Observation.yaml" in names
    assert "fhir_search_to_mql/configs/r5/Condition.yaml" in names
    assert "fhir_search_to_mql/compartments/definitions/patient.json" in names
