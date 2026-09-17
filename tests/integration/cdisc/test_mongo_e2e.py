"""Real-Mongo CDISC vertical smoke test with strictly isolated collections.

Uses CDISC_TEST_MONGODB_URI/CDISC_TEST_DB when provided, otherwise the
authorized CORE_MONGODB_URL/CORE_DATABASE_NAME from .env.local.  The test never
drops a database and only removes collections carrying its random prefix.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

motor = pytest.importorskip("motor.motor_asyncio")

from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.cdisc.sdr.strategy import CDISCSDRStrategy
from kehrnel.persistence.artifacts import FileSystemArtifactStore
from kehrnel.persistence.mongodb.index_admin import MongoIndexAdminAdapter
from kehrnel.persistence.mongodb.storage import MongoStorageAdapter


ROOT = Path(__file__).resolve().parents[3]
PACK = ROOT / "src" / "kehrnel" / "engine" / "strategies" / "cdisc" / "sdr"
FIXTURE = ROOT / "tests" / "fixtures" / "cdisc" / "dm.dataset.json"


def _mongo_config() -> tuple[str | None, str | None]:
    uri = os.getenv("CDISC_TEST_MONGODB_URI")
    database = os.getenv("CDISC_TEST_DB")
    if uri and database:
        return uri, database
    try:
        from dotenv import dotenv_values

        values = dotenv_values(ROOT / ".env.local")
        return uri or values.get("CORE_MONGODB_URL"), database or values.get("CORE_DATABASE_NAME")
    except Exception:
        return uri, database


MONGO_URI, MONGO_DB = _mongo_config()
EXPLICIT_TEST_MONGO = bool(os.getenv("CDISC_TEST_MONGODB_URI") and os.getenv("CDISC_TEST_DB"))
pytestmark = pytest.mark.skipif(
    not (MONGO_URI and MONGO_DB),
    reason="No authorized CDISC test MongoDB configuration is available.",
)


@pytest.mark.asyncio
async def test_cdisc_plan_ingest_validate_publish_query_export_on_real_mongo(tmp_path):
    prefix = f"cdisc_smoke_{uuid.uuid4().hex}_"
    client = motor.AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    database = client[MONGO_DB]
    try:
        await client.admin.command("ping")
    except Exception as exc:
        client.close()
        if EXPLICIT_TEST_MONGO:
            raise
        pytest.skip(f"Fallback tenant MongoDB is not reachable: {type(exc).__name__}")
    manifest = load_strategy("cdisc.sdr", PACK)
    configured = {
        key: f"{prefix}{key}"
        for key in manifest.default_config["collections"]
    }
    config = {
        **manifest.default_config,
        "tenant_id": f"mongo-smoke-{uuid.uuid4().hex}",
        "collections": configured,
        "semantic": {**manifest.default_config["semantic"], "enabled": False},
        "validation": {
            **manifest.default_config["validation"],
            "require_before_publish": True,
        },
    }
    ctx = StrategyContext(
        environment_id="cdisc-mongo-smoke",
        config=config,
        adapters={
            "storage": MongoStorageAdapter(database),
            "index_admin": MongoIndexAdminAdapter(database),
            "artifact_store": FileSystemArtifactStore(tmp_path / "artifacts"),
        },
        manifest=manifest,
        meta={},
    )
    strategy = CDISCSDRStrategy()
    try:
        await strategy.validate_config(ctx)
        plan = await strategy.plan(ctx)
        applied = await strategy.apply(ctx, plan)
        assert not applied.warnings

        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        ingested = await strategy.ingest(ctx, {
            "datasetJSON": document,
            "packageId": "mongo-smoke-package",
            "snapshotId": "v1",
            "standardsPackageId": "mongo-smoke-standard",
            "profile": "sdtm",
            "standard": {"family": "SDTM", "implementationGuide": "SDTMIG"},
            "publicationState": "staged",
        })
        validated = await strategy.run_op(ctx, "cdisc_validate_snapshot", {
            "studyId": ingested["studyId"], "snapshotId": ingested["snapshotId"],
        })
        published = await strategy.run_op(ctx, "cdisc_publish_snapshot", {
            "studyId": ingested["studyId"], "snapshotId": ingested["snapshotId"],
        })
        query = await strategy.compile_query(ctx, "cdisc", {
            "scope": {"studies": [ingested["studyId"]], "snapshots": "published"},
            "from": {"profile": "sdtm", "domains": ["DM"]},
            "where": {"and": [{"path": "data.SEX", "op": "eq", "value": "F"}]},
            "select": ["modelSchemaVersion", "data.USUBJID", "data.SEX"],
        })
        queried = await strategy.execute_query(ctx, query)
        exported = await strategy.run_op(ctx, "cdisc_export_dataset_json", {
            "datasetId": ingested["datasetId"], "persistArtifact": True,
        })

        assert validated["ok"] is True
        assert published["state"] == "published"
        assert queried.rows and all(row["modelSchemaVersion"] == "1.1.0" for row in queried.rows)
        assert exported["equivalence"]["equivalent"] is True
        assert exported["artifact"]["digest"]["value"]
    finally:
        for name in await database.list_collection_names():
            if name.startswith(prefix):
                await database.drop_collection(name)
        client.close()
