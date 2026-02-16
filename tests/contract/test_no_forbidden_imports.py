from pathlib import Path
import re

SRC_ROOT = Path("src")
CODE_ROOTS = [SRC_ROOT / "kehrnel"]
ALLOWED_PACKAGES = {"kehrnel", "strategy_sdk", "cli"}
# Only flag actual imports, not variable names like "src.get(...)".
SRC_IMPORT_PATTERN = re.compile(r"^\s*(from|import)\s+src\.", re.MULTILINE)
PERSISTENCE_PATTERN = re.compile(r"^\s*(from|import)\s+(?<!kehrnel\.)persistence\.", re.MULTILINE)
ADAPTERS_PATTERN = re.compile(r"^\s*(from|import)\s+adapters\.mongo", re.MULTILINE | re.IGNORECASE)
LEGACY_ENGINE_IMPORT_PATTERN = re.compile(
    r"^\s*(from|import)\s+kehrnel\.(core|common|domains|strategies)\b",
    re.MULTILINE,
)
THIS_FILE = Path(__file__).resolve()
FORBIDDEN_PATH_TOKENS = (
    "src.core.",
    "src.transform.",
    "src.mapper.",
    "src.ingest.",
    "src.persistence",
    "src.adapters",
    "libs.",
)
FORBIDDEN_RPS_DUAL_COMPILERS = (
    "kehrnel.strategies.openehr.rps_dual.query.compiler_match",
    "kehrnel.strategies.openehr.rps_dual.query.compiler_atlas_search",
)


def test_no_src_style_imports():
    offenders = []
    for root in CODE_ROOTS:
        for path in root.rglob("*.py"):
            if path.resolve() == THIS_FILE:
                continue
            text = path.read_text(encoding="utf-8")
            if SRC_IMPORT_PATTERN.search(text):
                offenders.append(path)
            if PERSISTENCE_PATTERN.search(text):
                offenders.append(path)
            if ADAPTERS_PATTERN.search(text):
                offenders.append(path)
            if any(tok in text for tok in FORBIDDEN_PATH_TOKENS):
                offenders.append(path)
    assert not offenders, f"Forbidden imports are present: {offenders}"


def test_no_extra_top_level_packages():
    unexpected = []
    for child in SRC_ROOT.iterdir():
        if child.name.startswith(".") or child.name.endswith(".egg-info"):
            continue
        if child.is_dir() and (child / "__init__.py").exists() and child.name not in ALLOWED_PACKAGES:
            unexpected.append(child.name)
    assert not unexpected, f"Unexpected top-level packages under src/: {sorted(unexpected)}"


def test_no_new_rps_dual_compiler_imports():
    offenders = []
    strategies_root = SRC_ROOT / "kehrnel" / "engine" / "strategies"
    for path in strategies_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if any(token in text for token in FORBIDDEN_RPS_DUAL_COMPILERS):
            offenders.append(path)
    assert not offenders, f"Forbidden rps_dual compiler imports detected: {sorted(offenders)}"


def test_cli_and_api_import_engine_paths_only():
    offenders = []
    for root in (SRC_ROOT / "kehrnel" / "cli", SRC_ROOT / "kehrnel" / "api"):
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if LEGACY_ENGINE_IMPORT_PATTERN.search(text):
                offenders.append(path)
    assert not offenders, f"CLI/API must import engine modules only: {sorted(offenders)}"
