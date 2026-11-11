from __future__ import annotations

from copy import deepcopy

import pytest

from kehrnel.api.domains.openehr.semantics.execution import (
    OpenEhrSemanticProjectionReadiness,
)
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import (
    MANIFEST,
    RPSDualStrategy,
)
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import (
    CompositionFlattener,
)


_DIGEST_A = "sha256:" + "a" * 64
_DIGEST_B = "sha256:" + "b" * 64
_PATH = "/content[openEHR-EHR-EVALUATION.problem.v1]/data[at0001]/items[at0002]/value/value"
_INDEX_DEFINITION = {"mappings": {"dynamic": False}}


class _Storage:
    db = None

    def __init__(self, state):
        self.state = state

    async def find_one(self, collection, flt, projection=None):
        assert collection == "_kehrnel_projection_state"
        return deepcopy(self.state) if self.state else None


class _Atlas:
    def __init__(self, indexes):
        self.indexes = indexes

    async def list_search_indexes(self, collection):
        assert collection == "compositions_search"
        return deepcopy(self.indexes)


class _PreparationStorage:
    db = object()


class _PreparationFlattener:
    def __init__(self):
        self.code_book = {"ar_code": {"existing": 1}, "at": {"at0001": -1}}
        self.template_fields = {
            "template-a": [
                {"path": _PATH, "rmType": "DV_TEXT"},
            ]
        }
        self.persisted = False

    def _compiled_rules_for_template(self, template_id):
        assert template_id == "template-a"
        self.code_book["ar_code"]["openEHR-EHR-EVALUATION.problem.v1"] = 2
        self.code_book["at"]["at0002"] = -2
        return [{"path": _PATH}]

    async def flush_codes_to_db(self):
        self.persisted = True


class _PreparationStrategy(RPSDualStrategy):
    def __init__(self):
        super().__init__(MANIFEST.model_copy(deep=True))
        self.flattener = _PreparationFlattener()

    async def _build_flattener_for_context(self, ctx):
        assert ctx.meta["bulk_config"]["role"] == "primary"
        return self.flattener


class _Strategy(RPSDualStrategy):
    async def _build_active_search_projection(self, ctx, strategy_cfg):
        return {
            "activeMappings": {
                "templates": [
                    {
                        "templateId": "template-a",
                        "fields": [{"path": _PATH, "rmType": "DV_TEXT"}],
                    }
                ]
            },
            "mappingDigest": "1" * 64,
            "indexDefinition": deepcopy(_INDEX_DEFINITION),
            "indexDefinitionDigest": "2" * 64,
            "projectionRevision": "projection-revision-1",
            "projectionAlgorithmVersion": "rps-dual-projector/v2",
            "templatePaths": {"template-a": [_PATH]},
            "templateProvenance": {
                "template-a": {
                    "templateSourceDigest": _DIGEST_A,
                    "catalogDigest": _DIGEST_B,
                }
            },
        }


class _MissingProvenanceStrategy(_Strategy):
    async def _build_active_search_projection(self, ctx, strategy_cfg):
        projection = await super()._build_active_search_projection(ctx, strategy_cfg)
        projection["templateProvenance"] = {}
        return projection


class _MismatchedProvenanceStrategy(_Strategy):
    async def _build_active_search_projection(self, ctx, strategy_cfg):
        projection = await super()._build_active_search_projection(ctx, strategy_cfg)
        projection["templateProvenance"] = {
            "template-a": {
                "templateSourceDigest": "sha256:" + "d" * 64,
                "catalogDigest": "sha256:" + "e" * 64,
            }
        }
        return projection


def _state(*, revision="projection-revision-1", state="ready", complete=True):
    return {
        "_id": "openehr.rps_dual:tenant-a:compositions_search",
        "contract": "openehr.rps-dual.search-projection-readiness/v1",
        "environmentId": "tenant-a",
        "strategyId": "openehr.rps_dual",
        "sourceCollection": "compositions_rps",
        "targetCollection": "compositions_search",
        "mutationSequence": 3,
        "activeMutations": 0,
        "projection": {
            "state": state,
            "revision": revision,
            "projectionAlgorithmVersion": "rps-dual-projector/v2",
            "mappingDigest": "1" * 64,
            "indexDefinitionDigest": "2" * 64,
            "complete": complete,
        },
        "checkpoint": {
            "processedCount": 12,
            "nextCursor": None,
            "mutationSequence": 3,
        },
        "reconciliation": {
            "complete": complete,
            "safe": state == "ready" and complete,
            "sourceToTargetCoverage": "verified",
            "targetToSourceCoverage": "verified",
            "mappedEmptyCount": 0,
        },
        "consistency": {
            "model": "governed-writer-mutation-fence/v1",
            "mutationSequence": 3,
            "activeMutations": 0,
            "governedWritersOnly": True,
            "publication": "compare-and-set",
        },
        "rebuildLease": {"active": False},
    }


def _context(state, *, indexes):
    return StrategyContext(
        environment_id="tenant-a",
        config=deepcopy(MANIFEST.default_config),
        adapters={"storage": _Storage(state), "atlas_search": _Atlas(indexes)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )


def _request(*, required_paths=None):
    return {
        "contract": "openehr-semantic-projection-preflight/v1",
        "templateId": "template-a",
        "templateSourceDigest": _DIGEST_A,
        "catalogDigest": _DIGEST_B,
        "planDigest": "sha256:" + "c" * 64,
        "requiredPaths": required_paths or [_PATH],
    }


def _ready_index():
    return [
        {
            "name": "search_nodes_index",
            "status": "READY",
            "queryable": True,
            "latestDefinition": deepcopy(_INDEX_DEFINITION),
        }
    ]


def test_negative_integer_path_encoding_accepts_valid_at0000_nodes():
    flattener = CompositionFlattener(
        None,
        {"role": "primary"},
        "unused.json",
        mappings_content={"templates": []},
    )

    assert flattener._at_code_to_int("at0000") == 0


@pytest.mark.asyncio
async def test_prepare_search_projection_dictionary_allocates_then_persists_codes():
    strategy = _PreparationStrategy()
    ctx = StrategyContext(
        environment_id="tenant-a",
        config=deepcopy(MANIFEST.default_config),
        adapters={"storage": _PreparationStorage()},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )

    result = await strategy.run_op(
        ctx,
        "prepare_search_projection_dictionary",
        {},
    )

    assert result == {
        "ok": True,
        "dryRun": False,
        "templateCount": 1,
        "fieldCount": 1,
        "dictionary": {
            "archetypeCodesBefore": 1,
            "archetypeCodesAfter": 2,
            "atCodesBefore": 1,
            "atCodesAfter": 2,
            "persisted": True,
        },
    }
    assert strategy.flattener.persisted is True


@pytest.mark.asyncio
async def test_semantic_projection_preflight_proves_ready_revision_and_index():
    result = await _Strategy().run_op(
        _context(_state(), indexes=_ready_index()),
        "semantic_projection_preflight",
        _request(),
    )

    assert result["ready"] is True
    assert result["manifest"]["requiredPaths"] == [_PATH]
    assert result["manifest"]["coveredPaths"] == [_PATH]
    assert result["manifest"]["missingPaths"] == []
    assert result["manifest"]["mappingDigest"].startswith("sha256:")
    assert result["manifest"]["manifestDigest"].startswith("sha256:")
    assert result["rebuildState"] == "ready"
    assert result["rebuildComplete"] is True
    assert result["rebuildInProgress"] is False
    assert result["rebuildWatermark"]["processedCount"] == 12
    assert result["atlasIndexState"] == "verified_ready"
    assert result["atlasIndexLiveVerified"] is True
    # Keep the strategy's real wire response synchronized with the strict API
    # receipt consumed by semantic execution.
    validated = OpenEhrSemanticProjectionReadiness.model_validate(result)
    assert validated.ready is True
    assert validated.rebuild_in_progress is False


@pytest.mark.asyncio
async def test_semantic_projection_preflight_reports_missing_required_path():
    missing = "/content[openEHR-EHR-OBSERVATION.other.v1]/data[at0001]/value/value"
    result = await _Strategy().run_op(
        _context(_state(), indexes=_ready_index()),
        "semantic_projection_preflight",
        _request(required_paths=[missing]),
    )

    assert result["ready"] is False
    assert result["manifest"]["missingPaths"] == [missing]
    assert "required_projection_paths_missing" in result["reasons"]


@pytest.mark.asyncio
async def test_semantic_projection_preflight_fails_closed_without_mapping_provenance():
    result = await _MissingProvenanceStrategy().run_op(
        _context(_state(), indexes=_ready_index()),
        "semantic_projection_preflight",
        _request(),
    )

    assert result["ready"] is False
    assert "projection_provenance_missing" in result["reasons"]


@pytest.mark.asyncio
async def test_semantic_projection_preflight_rejects_mapping_provenance_mismatch():
    result = await _MismatchedProvenanceStrategy().run_op(
        _context(_state(), indexes=_ready_index()),
        "semantic_projection_preflight",
        _request(),
    )

    assert result["ready"] is False
    assert "projection_template_source_digest_mismatch" in result["reasons"]
    assert "projection_catalog_digest_mismatch" in result["reasons"]


@pytest.mark.asyncio
async def test_semantic_projection_preflight_rejects_stale_rebuild_revision():
    result = await _Strategy().run_op(
        _context(_state(revision="old-projection"), indexes=_ready_index()),
        "semantic_projection_preflight",
        _request(),
    )

    assert result["ready"] is False
    assert result["rebuildState"] == "stale"
    assert result["rebuildComplete"] is False
    assert "projection_rebuild_receipt_stale" in result["reasons"]


@pytest.mark.asyncio
async def test_semantic_projection_preflight_rejects_legacy_ready_without_integrity_proof():
    legacy = _state()
    legacy.pop("mutationSequence")
    legacy.pop("reconciliation")
    legacy.pop("consistency")

    result = await _Strategy().run_op(
        _context(legacy, indexes=_ready_index()),
        "semantic_projection_preflight",
        _request(),
    )

    assert result["ready"] is False
    assert result["rebuildComplete"] is False
    assert "projection_rebuild_incomplete" in result["reasons"]
    assert "projection_integrity_unproven" in result["reasons"]


@pytest.mark.asyncio
async def test_semantic_projection_preflight_rejects_legacy_algorithm_receipt():
    legacy = _state()
    legacy["projection"].pop("projectionAlgorithmVersion")

    result = await _Strategy().run_op(
        _context(legacy, indexes=_ready_index()),
        "semantic_projection_preflight",
        _request(),
    )

    assert result["ready"] is False
    assert result["rebuildState"] == "stale"
    assert "projection_rebuild_receipt_stale" in result["reasons"]


@pytest.mark.asyncio
async def test_semantic_projection_preflight_does_not_infer_live_index_readiness():
    result = await _Strategy().run_op(
        _context(
            _state(),
            indexes=[
                {
                    "name": "search_nodes_index",
                    "status": "BUILDING",
                    "queryable": False,
                    "latestDefinition": deepcopy(_INDEX_DEFINITION),
                }
            ],
        ),
        "semantic_projection_preflight",
        _request(),
    )

    assert result["ready"] is False
    assert result["atlasIndexState"] == "configured_unverified"
    assert result["atlasIndexLiveVerified"] is False
    assert "atlas_search_index_not_verified_ready" in result["reasons"]
