from pathlib import Path


def test_no_duplicate_mongo_modules():
    src_root = Path("src")
    offenders = []
    for path in src_root.rglob("*.py"):
        if "mongo" in path.name.lower():
            # allow canonical persistence location
            if "kehrnel/persistence" in path.as_posix():
                continue
            offenders.append(path)
    assert not offenders, f"Mongo modules outside kehrnel/persistence: {offenders}"
