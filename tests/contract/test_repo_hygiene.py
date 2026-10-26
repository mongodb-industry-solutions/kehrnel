from pathlib import Path


def test_no_copy_suffix_files():
    offenders = []
    for path in Path(".").rglob("*"):
        if path.is_file() and " copy" in path.name.lower():
            offenders.append(path)
    assert not offenders, f"Remove leftover duplicate files: {offenders}"


def test_no_protocols_dirs_in_strategies():
    offenders = []
    for path in Path("src/kehrnel/engine/strategies").rglob("protocols"):
        if path.is_dir():
            offenders.append(path)
    assert not offenders, f"Unexpected protocols directories remain: {offenders}"
