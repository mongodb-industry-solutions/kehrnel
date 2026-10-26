"""E2E fixtures for fhir-mql CLI_COMMANDS tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
GEN_REPO_ROOT = REPO_ROOT.parent / "fhir-data-generation"
E2E_MONGODB_URI = os.environ.get("E2E_MONGODB_URI", "mongodb://localhost:27017/")
E2E_DB_PREFIX = os.environ.get("E2E_DB_PREFIX", "fhir_e2e_gen_")


def _venv_python(repo: Path) -> str:
    for rel in (("Scripts", "python.exe"), ("bin", "python")):
        candidate = repo / ".venv" / rel[0] / rel[1]
        if candidate.is_file():
            return str(candidate)
    return sys.executable


@pytest.fixture(scope="session")
def mongo_uri() -> str:
    return E2E_MONGODB_URI


@pytest.fixture(scope="session")
def mql_python() -> str:
    return _venv_python(REPO_ROOT)


@pytest.fixture(scope="session")
def gen_python() -> str:
    return _venv_python(GEN_REPO_ROOT)


@pytest.fixture(scope="session")
def mongo_available(mongo_uri: str):
    try:
        from pymongo import MongoClient

        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        client.server_info()
        yield client
        client.close()
    except Exception as exc:
        pytest.skip(f"MongoDB not available at {mongo_uri}: {exc}")


@pytest.fixture
def drop_e2e_db(mongo_available, mongo_uri: str):
    from pymongo import MongoClient

    dropped: list[str] = []

    def _drop(db_name: str) -> None:
        mongo_available.drop_database(db_name)
        dropped.append(db_name)

    yield _drop
    client = MongoClient(mongo_uri)
    for name in dropped:
        client.drop_database(name)
    client.close()
