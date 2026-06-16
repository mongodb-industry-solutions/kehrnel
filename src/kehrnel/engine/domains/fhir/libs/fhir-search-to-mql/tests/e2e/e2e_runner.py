"""Subprocess helpers for cross-repo E2E (fhir-gen + fhir-mql)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
GEN_REPO_ROOT = REPO_ROOT.parent / "fhir-data-generation"
DEFAULT_MONGODB_URI = "mongodb://localhost:27017/"


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
    python: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    py = python or venv_python(GEN_REPO_ROOT)
    cmd = [py, "-m", "fhir_gen.cli.main", *args]
    return subprocess.run(
        cmd,
        cwd=str(GEN_REPO_ROOT),
        capture_output=True,
        text=True,
        check=check,
        env={**dict(**__import__("os").environ), "FHIR_GEN_MONGODB_URI": mongo_uri},
    )


def run_fhir_mql(
    args: Sequence[str],
    *,
    mongo_uri: str = DEFAULT_MONGODB_URI,
    db: str,
    python: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    py = python or venv_python(REPO_ROOT)
    full = [py, "-m", "fhir_search_to_mql.cli", *args, "--uri", mongo_uri, "--db", db]
    return subprocess.run(
        full,
        cwd=str(REPO_ROOT),
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
    gen_python: str | None = None,
) -> None:
    args = [
        f"--seed={seed}",
        f"--db={db}",
        "--mongo-uri",
        mongo_uri,
        "generate-many",
        *resources,
    ]
    for rtype, n in counts.items():
        args.extend(["--count", f"{rtype}={n}"])
    args.append("--save")
    run_fhir_gen(args, mongo_uri=mongo_uri, python=gen_python)


def run_search(
    resource: str,
    query: str,
    db: str,
    *,
    extra_args: Sequence[str] = (),
    mongo_uri: str = DEFAULT_MONGODB_URI,
    mql_python: str | None = None,
) -> dict:
    args = ["search", resource, query, "--limit", "5", "--format", "json", *extra_args]
    proc = run_fhir_mql(args, mongo_uri=mongo_uri, db=db, python=mql_python)
    return json.loads(proc.stdout)
