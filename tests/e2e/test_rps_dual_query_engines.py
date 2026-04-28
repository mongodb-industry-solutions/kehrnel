import os
import pytest
import asyncio
from pathlib import Path
from pymongo.errors import ConfigurationError

from kehrnel.core.registry import FileActivationRegistry
from kehrnel.core.runtime import StrategyRuntime
from kehrnel.core.manifest import StrategyManifest
from kehrnel.engine.strategies.openehr.rps_dual.strategy import MANIFEST as RPS_MANIFEST, RPSDualStrategy
from kehrnel.strategy_sdk import StrategyBindings


pytestmark = pytest.mark.asyncio


def mongo_bindings_from_env():
    uri = os.getenv("MONGODB_URI")
    db = os.getenv("MONGODB_DB")
    if not uri or not db:
        return None
    return {
        "db": {
            "provider": "mongodb",
            "uri": uri,
            "database": db,
        }
    }


@pytest.fixture
def runtime(tmp_path):
    reg = FileActivationRegistry(tmp_path / "reg.json")
    rt = StrategyRuntime(registry=reg)
    rt.register_manifest(RPS_MANIFEST)
    return rt


@pytest.fixture
async def activated_env(runtime):
    bindings = mongo_bindings_from_env()
    if not bindings:
        pytest.skip("MONGODB_URI and MONGODB_DB required for e2e query test")
    await runtime.activate("env-e2e", RPS_MANIFEST.id, RPS_MANIFEST.version, RPS_MANIFEST.default_config, StrategyBindings(**bindings), allow_plaintext_bindings=True)
    return "env-e2e"


async def test_query_engines(runtime, activated_env):
    env_id = activated_env
    try:
        # apply plan (best-effort)
        await runtime.dispatch(env_id, "apply", {})
        # ingest two docs
        doc1 = {"ehr_id": "p1", "_id": "1", "search_nodes": [{"p": "node", "text": "hello"}]}
        doc2 = {"ehr_id": "p2", "_id": "2", "search_nodes": [{"p": "node", "text": "world"}]}
        await runtime.dispatch(env_id, "ingest", doc1)
        await runtime.dispatch(env_id, "ingest", doc2)

        # patient query
        ir_patient = {"scope": "patient", "predicates": [{"path": "ehr_id", "op": "eq", "value": "p1"}]}
        res_patient = await runtime.dispatch(env_id, "query", {"domain": "openehr", "query": ir_patient})
        assert res_patient.get("engine_used") == "mongo_pipeline"
        assert res_patient.get("explain", {}).get("pipeline", [{}])[0].get("$match") is not None
        assert "plan" not in (res_patient.get("explain") or {})

        # cross-patient query (forces search)
        ir_cross = {"scope": "cross_patient", "predicates": [{"path": "text", "op": "eq", "value": "hello"}]}
        res_cross = await runtime.dispatch(env_id, "query", {"domain": "openehr", "query": ir_cross})
        assert res_cross.get("engine_used") in ("atlas_search_dual", "text_search_dual", "mongo_pipeline")
        pipeline = res_cross.get("explain", {}).get("pipeline", [])
        assert pipeline, "Pipeline should be present"
        assert "plan" not in (res_cross.get("explain") or {})
        assert list(pipeline[0].keys())[0] == "$search"
    except ConfigurationError as exc:
        pytest.skip(f"MongoDB not reachable: {exc}")
