"""Shared fixtures for CLI_COMMANDS E2E tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
E2E_MONGODB_URI = os.environ.get("E2E_MONGODB_URI", "mongodb://localhost:27017/")
E2E_DB_PREFIX = os.environ.get("E2E_DB_PREFIX", "fhir_e2e_gen_")


def _venv_python() -> str:
    venv_py = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
    if venv_py.is_file():
        return str(venv_py)
    venv_py = REPO_ROOT / ".venv" / "bin" / "python"
    if venv_py.is_file():
        return str(venv_py)
    return sys.executable


@pytest.fixture(scope="session")
def mongo_uri() -> str:
    return E2E_MONGODB_URI


@pytest.fixture(scope="session")
def venv_python() -> str:
    return _venv_python()


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
    """Drop the E2E database after each test."""
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
