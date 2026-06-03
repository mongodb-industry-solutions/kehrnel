"""Shared helpers for fhir.rps_canonical contract tests (prompt 12)."""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.rps_canonical.strategy import MANIFEST
from kehrnel.strategy_sdk import StrategyBindings

FIXTURES_DIR = Path(__file__).parent / "fixtures"
GOLDEN_QUERIES_PATH = FIXTURES_DIR / "fhir_golden_queries.json"

FHIR_MQL = pytest.importorskip("fhir_search_to_mql")


def mongo_available() -> bool:
    if os.getenv("FHIR_CONTRACT_MONGO", "").lower() in ("0", "false", "no"):
        return False
    if os.getenv("FHIR_CONTRACT_MONGO", "").lower() in ("1", "true", "yes"):
        return True
    try:
        from pymongo import MongoClient

        uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        client = MongoClient(uri, serverSelectionTimeoutMS=2500)
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


requires_mongo = pytest.mark.skipif(
    not mongo_available(),
    reason="MongoDB required (set FHIR_CONTRACT_MONGO=1 or run local mongod)",
)


def unique_db_name(prefix: str = "fhir_contract") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def activation_payload(*, database: str) -> dict[str, Any]:
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    return {
        "strategy_id": "fhir.rps_canonical",
        "version": "0.1.0",
        "domain": "fhir",
        "config": {
            "database": database,
            "schema_version": "R5",
            "collections": {"mode": "per_resource_type"},
            "search": {"enabled": True, "auto_index": False},
        },
        "bindings": {
            "db": {
                "provider": "mongodb",
                "uri": uri,
                "database": database,
            }
        },
        "allow_plaintext_bindings": True,
    }


def strategy_context(*, database: str, environment_id: str = "contract-env") -> StrategyContext:
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    return StrategyContext(
        environment_id=environment_id,
        config=activation_payload(database=database)["config"],
        bindings=activation_payload(database=database)["bindings"],
        manifest=MANIFEST,
    )


async def activate_runtime(runtime, *, env_id: str, database: str) -> None:
    payload = activation_payload(database=database)
    bindings = StrategyBindings(**payload["bindings"])
    await runtime.activate(
        env_id,
        payload["strategy_id"],
        payload["version"],
        payload["config"],
        bindings,
        allow_plaintext_bindings=True,
        domain="fhir",
    )


def make_test_client(tmp_path, monkeypatch: pytest.MonkeyPatch | None = None) -> TestClient:
    os.environ.setdefault("KEHRNEL_AUTH_ENABLED", "false")
    if monkeypatch is not None:
        monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "false")
        monkeypatch.setenv("KEHRNEL_RATE_LIMIT", "0")
        monkeypatch.delenv("CORE_MONGODB_URL", raising=False)
    app = create_app(str(tmp_path / "reg.json"))
    # Avoid per-progress Mongo writes during contract tests (slow / flaky).
    from kehrnel.engine.core.synthetic_jobs import SyntheticJobManager

    app.state.synthetic_job_manager = SyntheticJobManager(
        app.state.strategy_runtime,
        store=None,
    )
    return TestClient(app)


def load_golden_cases() -> list[dict[str, Any]]:
    data = json.loads(GOLDEN_QUERIES_PATH.read_text(encoding="utf-8"))
    cases = data.get("cases") or []
    if not isinstance(cases, list) or not cases:
        raise ValueError(f"No golden cases in {GOLDEN_QUERIES_PATH}")
    return cases


def mql_contains_keys(mql: Any, expected_keys: list[str]) -> bool:
    if not expected_keys:
        return True
    blob = json.dumps(mql, default=str)
    return all(key in blob for key in expected_keys)


def drop_database(database: str) -> None:
    from pymongo import MongoClient

    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    client = MongoClient(uri)
    try:
        client.drop_database(database)
    finally:
        client.close()
