from pathlib import Path


def test_no_duplicate_mongo_modules():
    src_root = Path("src")
    vendored_fhir_libs = Path("src/kehrnel/engine/domains/fhir/libs")
    offenders = []
    for path in src_root.rglob("*.py"):
        if "mongo" in path.name.lower():
            # Vendored FHIR libraries are independently packaged dependencies;
            # their persistence adapters are not duplicate Kehrnel core seams.
            if path.is_relative_to(vendored_fhir_libs) or ".venv" in path.parts:
                continue
            # allow canonical persistence location
            if "kehrnel/persistence" in path.as_posix():
                continue
            offenders.append(path)
    assert not offenders, f"Mongo modules outside kehrnel/persistence: {offenders}"
