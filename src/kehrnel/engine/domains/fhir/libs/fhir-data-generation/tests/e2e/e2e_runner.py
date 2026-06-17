"""Helpers to invoke fhir-gen / fhir-mql CLIs for E2E runs."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
MQL_REPO_ROOT = REPO_ROOT.parent / "fhir-search-to-mql"
DEFAULT_MONGODB_URI = "mongodb://localhost:27017/"


def _parse_json_summary(stdout: str) -> dict[str, Any]:
    """Parse the trailing ``generate-many`` summary object from CLI stdout."""
    text = stdout.strip()
    if not text:
        return {}
    decoder = json.JSONDecoder()
    last: dict[str, Any] = {}
    idx = 0
    while idx < len(text):
        chunk = text[idx:].lstrip()
        if chunk.startswith("{"):
            try:
                obj, end = decoder.raw_decode(chunk)
                if isinstance(obj, dict):
                    last = obj
                idx += len(text[idx:]) - len(chunk) + end
                continue
            except json.JSONDecodeError:
                pass
        idx += 1
    return last


def venv_python(repo_root: Path) -> str:
    win = repo_root / ".venv" / "Scripts" / "python.exe"
    if win.is_file():
        return str(win)
    unix = repo_root / ".venv" / "bin" / "python"
    if unix.is_file():
        return str(unix)
    return sys.executable


def run_fhir_gen(
    args: Sequence[str],
    *,
    mongo_uri: str = DEFAULT_MONGODB_URI,
    cwd: Path | None = None,
    python: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    py = python or venv_python(REPO_ROOT)
    cmd = [py, "-m", "fhir_gen.cli.main", *args]
    env = {**dict(**__import__("os").environ), "FHIR_GEN_MONGODB_URI": mongo_uri}
    return subprocess.run(
        cmd,
        cwd=str(cwd or REPO_ROOT),
        capture_output=True,
        text=True,
        check=check,
        env=env,
    )


def run_fhir_mql(
    args: Sequence[str],
    *,
    mongo_uri: str = DEFAULT_MONGODB_URI,
    db: str,
    python: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    py = python or venv_python(MQL_REPO_ROOT)
    cmd = [py, "-m", "fhir_search_to_mql.cli", *args, "--uri", mongo_uri, "--db", db]
    return subprocess.run(
        cmd,
        cwd=str(MQL_REPO_ROOT),
        capture_output=True,
        text=True,
        check=check,
    )


def load_gen_data(
    *,
    seed: int,
    resources: Sequence[str],
    counts: Mapping[str, int],
    db: str,
    mongo_uri: str = DEFAULT_MONGODB_URI,
    python: str | None = None,
) -> dict[str, Any]:
    """Run ``fhir-gen generate-many --save`` and return parsed summary JSON."""
    args = [
        f"--seed={seed}",
        f"--db={db}",
        "--mongo-uri",
        mongo_uri,
        "generate-many",
        *resources,
    ]
    for rtype, count in counts.items():
        args.extend(["--count", f"{rtype}={count}"])
    args.append("--save")
    result = run_fhir_gen(args, mongo_uri=mongo_uri, python=python)
    return _parse_json_summary(result.stdout)


def generate_many_scenario(
    scenario_id: str,
    resources: Sequence[str],
    counts: Mapping[str, int],
    *,
    seed: int,
    db: str,
    mongo_uri: str = DEFAULT_MONGODB_URI,
) -> dict[str, Any]:
    """Alias for load_gen_data (kept for tests)."""
    return load_gen_data(
        seed=seed,
        resources=resources,
        counts=counts,
        db=db,
        mongo_uri=mongo_uri,
    )


def mql_pipeline_for_db(
    resources: Sequence[str],
    db: str,
    *,
    mongo_uri: str = DEFAULT_MONGODB_URI,
    denormalize_all: bool = False,
) -> None:
    """indexes + denormalize (+ stats) for a populated database."""
    if denormalize_all:
        run_fhir_mql(["indexes", "--all"], mongo_uri=mongo_uri, db=db)
        run_fhir_mql(
            ["denormalize", "--all", "--batch-size", "100"],
            mongo_uri=mongo_uri,
            db=db,
        )
    else:
        run_fhir_mql(["indexes", *resources], mongo_uri=mongo_uri, db=db)
        run_fhir_mql(
            ["denormalize", *resources, "--batch-size", "100"],
            mongo_uri=mongo_uri,
            db=db,
        )
    run_fhir_mql(["stats", *resources, "--format", "json"], mongo_uri=mongo_uri, db=db)
