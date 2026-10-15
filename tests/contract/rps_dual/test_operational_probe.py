from unittest.mock import AsyncMock, patch

import pytest

from kehrnel.engine.core.types import ApplyPlan, StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.config import normalize_config
from kehrnel.engine.strategies.openehr.rps_dual.strategy import (
    DEFAULTS_PATH,
    MANIFEST,
    RPSDualStrategy,
    load_json,
)


class _Collection:
    async def index_information(self):
        return {
            "_id_": {"key": [("_id", 1)]},
            "required_index": {"key": [("tid", 1)]},
        }


class _Database:
    async def command(self, _name):
        return {"ok": 1}

    async def list_collection_names(self):
        return ["compositions_rps", "compositions_search", "_codes", "_shortcuts"]

    def __getitem__(self, _name):
        return _Collection()


class _Storage:
    db = _Database()

    async def find_one(self, _collection, _filter):
        return {
            "projection": {
                "state": "ready",
                "complete": True,
                "revision": "sha256:" + "1" * 64,
                "mappingDigest": "sha256:" + "2" * 64,
                "indexDefinitionDigest": "sha256:" + "3" * 64,
            },
            "reconciliation": {
                "safe": True,
                "complete": True,
                "sourceToTargetCoverage": "verified",
                "targetToSourceCoverage": "verified",
                "mappedEmptyCount": 0,
            },
            "checkpoint": {"mutationSequence": 4, "updatedAt": "2026-10-05T09:00:00Z"},
            "consistency": {"activeMutations": 0, "mutationSequence": 4},
        }


class _AtlasSearch:
    async def list_search_indexes(self, _collection):
        return [{"name": "default", "status": "READY", "queryable": True}]


@pytest.mark.asyncio
async def test_rps_dual_operational_probe_reports_in_sync_runtime_contract():
    strategy = RPSDualStrategy(MANIFEST)
    config = load_json(DEFAULTS_PATH)
    strategy_cfg = normalize_config(config)
    required_collections = [
        strategy_cfg.collections.compositions.name,
        strategy_cfg.collections.search.name,
        strategy_cfg.collections.codes.name,
        strategy_cfg.collections.shortcuts.name,
    ]
    strategy.plan = AsyncMock(
        return_value=ApplyPlan(
            artifacts={
                "collections": required_collections,
                "indexes": [
                    {
                        "collection": strategy_cfg.collections.compositions.name,
                        "options": {"name": "required_index"},
                    }
                ],
            }
        )
    )
    strategy._build_active_search_projection = AsyncMock(
        return_value={
            "projectionRevision": "sha256:" + "1" * 64,
            "mappingDigest": "sha256:" + "2" * 64,
            "indexDefinitionDigest": "sha256:" + "3" * 64,
        }
    )
    ctx = StrategyContext(
        environment_id="sandbox-a",
        config=config,
        adapters={"storage": _Storage(), "atlas_search": _AtlasSearch()},
        manifest=MANIFEST,
    )

    with patch(
        "kehrnel.engine.strategies.openehr.rps_dual.strategy.atlas_index_readiness",
        return_value=("ready", True),
    ):
        result = await strategy._operational_probe(
            ctx,
            strategy_cfg,
            {"freshness_target_seconds": 30},
        )

    assert result["state"] == "healthy"
    assert result["schemaState"] == "in_sync"
    assert result["indexState"] == "in_sync"
    assert result["freshnessTargetSeconds"] == 30
    assert result["observedFreshnessSeconds"] == 0
    assert result["reconciliationDivergenceCount"] == 0
    assert all(check["state"] == "passed" for check in result["checks"])
    assert {check["checkId"] for check in result["checks"]} >= {
        "data_freshness",
        "reconciliation_integrity",
    }


@pytest.mark.asyncio
async def test_rps_dual_operational_probe_is_unavailable_without_runtime_adapter():
    strategy = RPSDualStrategy(MANIFEST)
    config = load_json(DEFAULTS_PATH)
    result = await strategy._operational_probe(
        StrategyContext(
            environment_id="sandbox-a",
            config=config,
            adapters={},
            manifest=MANIFEST,
        ),
        normalize_config(config),
    )

    assert result["state"] == "unavailable"
    assert all(check["state"] == "unavailable" for check in result["checks"])
