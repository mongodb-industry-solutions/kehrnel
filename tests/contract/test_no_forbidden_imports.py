from pathlib import Path


FORBIDDEN = ("src.api.v1.", "src.app.core.config", "src.persistence")
ALLOW_DIR_SUBSTR = "legacy_aql"


def test_no_forbidden_imports():
    root = Path("src/kehrnel")
    offenders = []
    for path in root.rglob("*.py"):
        if ALLOW_DIR_SUBSTR in str(path):
            continue
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN:
            if token in text:
                offenders.append((path, token))
    assert not offenders, f"Forbidden imports found: {offenders}"
