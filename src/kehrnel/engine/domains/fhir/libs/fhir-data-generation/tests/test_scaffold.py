"""Prompt 1 scaffold verification."""

from pathlib import Path

from fhir_gen import __version__, settings


def test_version():
    assert __version__ == "1.0.0"


def test_schema_path_exists():
    path = settings.resolved_schema_path
    assert path.exists()
    assert path.name == "fhir.schema.v5.json"


def test_codes_path_exists():
    assert settings.codes_path.exists()
    assert settings.codes_path.name == "healthcare_codes.yaml"


def test_package_layout():
    root = Path(__file__).resolve().parent.parent / "fhir_gen"
    for part in (
        "schema/parser.py",
        "generators/base.py",
        "resolvers/reference.py",
        "persistence/mongo.py",
        "codes/loader.py",
        "cli/main.py",
    ):
        assert (root / part).exists(), part
