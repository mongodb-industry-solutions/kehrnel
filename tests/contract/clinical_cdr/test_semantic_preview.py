"""FHIR semantic configuration and text-preview contracts."""

from __future__ import annotations

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.semantic import (
    describe_semantic_config,
    fhir_semantic_materialize,
    fhir_semantic_preview,
    fhir_semantic_search,
    validate_semantic_config,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import MANIFEST


def _config() -> dict:
    return {
        "database": "fhir_semantic_test",
        "schema_version": "R5",
        "collections": {"mode": "per_resource_type"},
        "semantic": {
            "enabled": True,
            "pipelines": [
                {
                    "id": "clinical-notes-v1",
                    "resource_types": ["DiagnosticReport", "Composition"],
                    "fields": [
                        {"path": "DiagnosticReport.conclusion", "label": "Conclusion"},
                        {"path": "Composition.section.text.div", "label": "Narrative"},
                    ],
                    "chunking": {"max_chars": 128, "overlap_chars": 16},
                    "embedding": {
                        "binding": "embedding",
                        "model": "clinical-model",
                        "dimensions": 768,
                    },
                    "storage": {
                        "mode": "sidecar",
                        "collection": "fhir_semantic_chunks",
                    },
                    "trigger": "manual",
                }
            ],
        },
    }


def _ctx(config: dict | None = None, adapters: dict | None = None) -> StrategyContext:
    return StrategyContext(
        environment_id="semantic-test",
        config=config or _config(),
        bindings={},
        adapters=adapters or {},
        manifest=MANIFEST,
    )


def test_semantic_capabilities_separate_preview_from_execution():
    result = describe_semantic_config(_config(), {})
    assert result["enabled"] is True
    assert result["active_pipeline_count"] == 1
    assert result["execution"] == {
        "preview": True,
        "embedding_generation": False,
        "sidecar_persistence": False,
        "vector_search": False,
        "rebuild_jobs": False,
    }
    assert result["canonical_resources_mutated"] is False
    assert result["pipelines"][0]["projection_version"].startswith("sha256:")


def test_semantic_preview_combines_fields_and_strips_xhtml():
    result = fhir_semantic_preview(
        _ctx(),
        {
            "pipeline_id": "clinical-notes-v1",
            "resource": {
                "resourceType": "Composition",
                "id": "note-1",
                "section": [
                    {
                        "text": {
                            "status": "generated",
                            "div": "<div>Stable &amp; improving</div>",
                        }
                    },
                    {
                        "text": {
                            "status": "generated",
                            "div": "<div>Review in two weeks</div>",
                        }
                    },
                ],
                "_search": {"client": "removed"},
            },
        },
    )
    assert "Stable & improving" in result["rendered_text"]
    assert "Review in two weeks" in result["rendered_text"]
    assert result["chunks"]
    assert result["embedding_generated"] is False
    assert result["canonical_resource_mutated"] is False


def test_semantic_configuration_is_optional_and_fail_closed():
    disabled = _config()
    disabled["semantic"]["enabled"] = False
    assert describe_semantic_config(disabled)["active_pipeline_count"] == 0
    with pytest.raises(KehrnelError) as exc_info:
        fhir_semantic_preview(
            _ctx(disabled), {"pipeline_id": "clinical-notes-v1", "resource": {}}
        )
    assert exc_info.value.code == "FHIR_SEMANTIC_DISABLED"

    invalid = _config()
    invalid["semantic"]["pipelines"][0]["chunking"] = {
        "max_chars": 100,
        "overlap_chars": 100,
    }
    with pytest.raises(ValueError):
        validate_semantic_config(invalid)


class _Embedding:
    async def embed(self, texts):
        return [[float(len(text)), 0.5, 1.0] for text in texts]


class _Storage:
    def __init__(self):
        self.documents = []
        self.deleted = []
        self.pipeline = None

    async def replace_many(self, collection, docs):
        self.collection = collection
        self.documents = list(docs)

    async def delete_many(self, collection, flt):
        self.deleted.append((collection, flt))

    async def find_one(self, collection, flt, projection=None):
        return {
            "resourceType": "DiagnosticReport",
            "id": flt["id"],
            "conclusion": "Stored clinical conclusion",
            "_search": {},
        }

    async def aggregate(self, collection, pipeline, allow_disk_use=True):
        self.pipeline = pipeline
        return [
            {
                "score": 0.91,
                "source": {"resource_type": "DiagnosticReport", "id": "report-1"},
                "text": "Conclusion: Stable",
            }
        ]


class _IndexAdmin:
    async def ensure_collection(self, name):
        self.collection = name

    async def ensure_indexes(self, collection, specs):
        self.specs = specs
        return {
            "created": ["semantic_source_chunk", "semantic_projection_version"],
            "warnings": [],
        }


class _Atlas:
    async def ensure_vector_index(self, collection, index_name, definition):
        self.request = (collection, index_name, definition)
        return {"created": [index_name], "updated": [], "warnings": []}


def _semantic_adapters():
    return {
        "embedding": _Embedding(),
        "storage": _Storage(),
        "index_admin": _IndexAdmin(),
        "atlas_search": _Atlas(),
    }


@pytest.mark.asyncio
async def test_semantic_materialization_writes_rebuildable_sidecars():
    config = _config()
    config["semantic"]["pipelines"][0]["embedding"]["dimensions"] = 3
    adapters = _semantic_adapters()
    result = await fhir_semantic_materialize(
        _ctx(config, adapters),
        {
            "pipeline_id": "clinical-notes-v1",
            "resource": {
                "resourceType": "DiagnosticReport",
                "id": "report-1",
                "conclusion": "Stable and improving",
            },
        },
    )

    assert result["ok"] is True
    assert result["resources"] == 1
    assert result["chunks"] == 1
    assert result["dimensions"] == 3
    stored = adapters["storage"].documents[0]
    assert stored["source"]["id"] == "report-1"
    assert stored["embedding"] == [
        float(len("Conclusion: Stable and improving")),
        0.5,
        1.0,
    ]
    assert "_search" not in stored
    assert result["canonical_resources_mutated"] is False


@pytest.mark.asyncio
async def test_semantic_materialization_rejects_sources_without_logical_ids():
    config = _config()
    config["semantic"]["pipelines"][0]["embedding"]["dimensions"] = 3

    with pytest.raises(KehrnelError) as exc_info:
        await fhir_semantic_materialize(
            _ctx(config, _semantic_adapters()),
            {
                "pipeline_id": "clinical-notes-v1",
                "resource": {
                    "resourceType": "DiagnosticReport",
                    "conclusion": "No logical id",
                },
            },
        )

    assert exc_info.value.code == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_semantic_materialization_can_read_canonical_source_by_target():
    config = _config()
    config["semantic"]["pipelines"][0]["embedding"]["dimensions"] = 3
    adapters = _semantic_adapters()

    result = await fhir_semantic_materialize(
        _ctx(config, adapters),
        {
            "pipeline_id": "clinical-notes-v1",
            "targets": [{"resource_type": "DiagnosticReport", "id": "report-1"}],
        },
    )

    assert result["resources"] == 1
    assert adapters["storage"].documents[0]["source"]["id"] == "report-1"


@pytest.mark.asyncio
async def test_semantic_search_executes_vector_pipeline_without_returning_vectors():
    config = _config()
    config["semantic"]["pipelines"][0]["embedding"]["dimensions"] = 3
    adapters = _semantic_adapters()

    result = await fhir_semantic_search(
        _ctx(config, adapters),
        {
            "pipeline_id": "clinical-notes-v1",
            "query": "stable patient",
            "limit": 5,
            "resource_types": ["DiagnosticReport"],
        },
    )

    assert result["count"] == 1
    assert result["results"][0]["score"] == 0.91
    vector_stage = adapters["storage"].pipeline[0]["$vectorSearch"]
    assert vector_stage["limit"] == 5
    assert vector_stage["queryVector"] == [14.0, 0.5, 1.0]
    assert "embedding" not in result["results"][0]
