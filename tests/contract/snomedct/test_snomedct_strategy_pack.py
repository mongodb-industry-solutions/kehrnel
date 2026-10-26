import base64
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.snomedct.mongodb.strategy import SNOMEDCTMongoDBStrategy


PACK_DIR = Path("src/kehrnel/engine/strategies/snomedct/mongodb")


def test_snomedct_mongodb_pack_loads_with_defaults_schema_and_ops():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)

    assert manifest.domain == "snomedct"
    assert manifest.default_config["collections"]["sidecar_enabled"] is True
    assert "collections" in manifest.config_schema["properties"]

    op_names = {op.name for op in manifest.ops}
    assert {
        "snomed_list_releases",
        "snomed_diff_release",
        "snomed_ingest_release",
        "snomed_rebuild_sidecar",
        "snomed_capabilities",
        "snomed_validate_code",
        "snomed_subsumes",
        "snomed_put_value_set",
        "snomed_list_value_sets",
        "snomed_validate_value_set",
        "snomed_search",
        "snomed_suggest",
        "snomed_concept_history",
        "snomed_ground_note",
        "snomed_save_grounding_review",
        "snomed_query_grounded_corpus",
        "snomed_benchmark_retrieval",
    }.issubset(op_names)


@pytest.mark.asyncio
async def test_snomedct_transform_builds_canonical_and_sidecar_docs():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(environment_id="test", config=manifest.default_config, manifest=manifest)

    result = await strategy.transform(
        ctx,
        {
            "concept": {
                "conceptId": "73211009",
                "active": "1",
                "moduleId": "900000000000207008",
                "effectiveTime": "20260601",
                "definitionStatusId": "900000000000074008",
                "inferredParentIds": ["44054006"],
                "inferredAncestorIds": ["404684003"],
                "inferredDescendantIds": ["1", "2"],
                "descriptions": [
                    {
                        "descriptionId": "100",
                        "active": "1",
                        "languageCode": "en",
                        "typeId": "900000000000003001",
                        "term": "Diabetes mellitus (disorder)",
                        "acceptabilityMap": {"900000000000509007": "900000000000548007"},
                    },
                    {
                        "descriptionId": "101",
                        "active": "1",
                        "languageCode": "en",
                        "typeId": "900000000000013009",
                        "term": "Diabetes mellitus",
                        "acceptabilityMap": {"900000000000509007": "900000000000548007"},
                    },
                ],
            }
        },
    )

    assert result.base["conceptId"] == "73211009"
    assert "inferredDescendantIds" not in result.base
    assert result.search
    assert result.search["terms"][0]["conceptId"] == "73211009"


@pytest.mark.asyncio
async def test_snomedct_local_release_staging_list_and_inspect(tmp_path):
    release_dir = tmp_path / "snomed-releases"
    release_dir.mkdir()
    release_file = release_dir / "edicion_20260601.json"
    release_file.write_text(
        """
        [
          {
            "conceptId": "73211009",
            "active": "1",
            "effectiveTime": "20260601",
            "descriptions": [
              {
                "descriptionId": "101",
                "active": "1",
                "languageCode": "en",
                "typeId": "900000000000013009",
                "term": "Diabetes mellitus"
              }
            ]
          }
        ]
        """,
        encoding="utf-8",
    )

    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    cfg = dict(manifest.default_config)
    cfg["source"] = {
        "local_dir": str(release_dir),
        "file_name": release_file.name,
        "file_pattern": "*.json",
    }
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(environment_id="test", config=cfg, manifest=manifest)

    listed = await strategy.snomed_list_releases(ctx, {})
    inspected = await strategy.snomed_inspect_release(ctx, {})

    assert listed["count"] == 1
    assert listed["files"][0]["name"] == release_file.name
    assert inspected["path"] == str(release_file)
    assert inspected["concepts"] == 1
    assert inspected["active"] == 1


@pytest.mark.asyncio
async def test_snomedct_release_operations_cannot_escape_configured_staging_directory(tmp_path):
    release_dir = tmp_path / "snomed-releases"
    release_dir.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("[]", encoding="utf-8")
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    cfg = dict(manifest.default_config)
    cfg["source"] = {**manifest.default_config["source"], "local_dir": str(release_dir)}
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(environment_id="test", config=cfg, manifest=manifest)

    with pytest.raises(KehrnelError) as exc_info:
        await strategy.snomed_inspect_release(ctx, {"path": str(outside)})

    assert exc_info.value.code == "RELEASE_PATH_OUTSIDE_STAGING"


@pytest.mark.asyncio
async def test_snomedct_release_ingest_requires_license_acknowledgement_before_persistence(tmp_path):
    release_dir = tmp_path / "snomed-releases"
    release_dir.mkdir()
    release_file = release_dir / "release.json"
    release_file.write_text("[]", encoding="utf-8")
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    cfg = dict(manifest.default_config)
    cfg["source"] = {
        **manifest.default_config["source"],
        "local_dir": str(release_dir),
        "file_name": release_file.name,
    }
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(environment_id="test", config=cfg, manifest=manifest)

    with pytest.raises(KehrnelError) as exc_info:
        await strategy.snomed_ingest_release(
            ctx,
            {"release_id": "20260601", "dry_run": False},
        )

    assert exc_info.value.code == "SNOMED_LICENSE_ACKNOWLEDGEMENT_REQUIRED"


@pytest.mark.asyncio
async def test_snomedct_inline_release_upload_stages_inside_the_configured_directory(tmp_path):
    release_dir = tmp_path / "snomed-releases"
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    cfg = dict(manifest.default_config)
    cfg["source"] = {**manifest.default_config["source"], "local_dir": str(release_dir)}
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(environment_id="test", config=cfg, manifest=manifest)
    content = b'[{"conceptId": "73211009"}]'
    digest = "sha256:" + hashlib.sha256(content).hexdigest()

    result = await strategy.snomed_upload_release(
        ctx,
        {
            "file_name": "irbd-20260601.json",
            "content_base64": base64.b64encode(content).decode("ascii"),
            "sha256": digest,
        },
    )

    assert result["ok"] is True
    assert result["sha256"] == digest
    staged = release_dir / "irbd-20260601.json"
    assert staged.read_bytes() == content

    # The staged file immediately flows through the existing list/inspect chain.
    listed = await strategy.snomed_list_releases(ctx, {})
    assert listed["count"] == 1
    inspected = await strategy.snomed_inspect_release(ctx, {"file_name": staged.name})
    assert inspected["concepts"] == 1


@pytest.mark.asyncio
async def test_snomedct_inline_release_upload_enforces_size_and_checksum(tmp_path):
    release_dir = tmp_path / "snomed-releases"
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    cfg = dict(manifest.default_config)
    cfg["source"] = {**manifest.default_config["source"], "local_dir": str(release_dir)}
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(environment_id="test", config=cfg, manifest=manifest)
    content = b"[]"

    with pytest.raises(KehrnelError) as exc_info:
        await strategy.snomed_upload_release(
            ctx,
            {
                "file_name": "too-big.json",
                "content_base64": base64.b64encode(content).decode("ascii"),
                "max_bytes": 1,
            },
        )
    assert exc_info.value.code == "RELEASE_UPLOAD_TOO_LARGE"

    with pytest.raises(KehrnelError) as exc_info:
        await strategy.snomed_upload_release(
            ctx,
            {
                "file_name": "mismatch.json",
                "content_base64": base64.b64encode(content).decode("ascii"),
                "sha256": "sha256:" + "0" * 64,
            },
        )
    assert exc_info.value.code == "RELEASE_UPLOAD_CHECKSUM_MISMATCH"

    with pytest.raises(KehrnelError) as exc_info:
        await strategy.snomed_upload_release(
            ctx,
            {
                "file_name": "../escape.json",
                "content_base64": base64.b64encode(content).decode("ascii"),
            },
        )
    assert exc_info.value.code == "INVALID_RELEASE_FILE_NAME"

    assert not release_dir.exists() or not any(release_dir.iterdir())


def test_snomedct_irbd_json_quirks_are_normalized(tmp_path):
    """Pin the official IRBD JSON quirks documented by the Spanish release format."""
    from kehrnel.engine.domains.snomedct import iter_concepts_from_json, normalize_concept

    release = tmp_path / "irbd.json"
    release.write_text(
        json.dumps(
            [
                {
                    "conceptId": 73211009,  # number, not string
                    "active": "1",  # string flag
                    "effectiveTime": "20260601",
                    "moduleId": "900000000000207008",
                    "definitionStatusId": "900000000000074008",
                    "inferredParentIds": [44054006],  # numbers
                    "inferredAncestorIds": [404684003],
                    "inferredDescendantIds": [1, 2, 3],  # dropped by the projection
                    "descriptions": [
                        {
                            "descriptionId": 100,
                            "active": "1",
                            "languageCode": "es",
                            "typeId": 900000000000013009,
                            "term": "Diabetes mellitus",
                            "acceptabilityMap": {},  # empty map in the Spanish release
                        }
                    ],
                    "relationships": [
                        {
                            "relationshipId": 1,
                            "active": "1",
                            "typeId": 116680003,
                            "destinationId": 404684003,
                            "groupId": 0,
                        }
                    ],
                }
            ]
        ),
        encoding="utf-8",
    )

    concepts = list(iter_concepts_from_json(release))
    assert len(concepts) == 1
    doc = normalize_concept(concepts[0], release_id="20260601")

    assert doc["conceptId"] == "73211009"
    assert doc["active"] is True
    assert doc["inferredParentIds"] == ["44054006"]
    assert doc["inferredAncestorIds"] == ["404684003"]
    assert "inferredDescendantIds" not in doc
    description = doc["descriptions"][0]
    assert description["descriptionId"] == "100"
    assert description["active"] is True
    assert description["acceptabilityMap"] == {}
    relationship = doc["relationships"][0]
    assert relationship["active"] is True
    assert relationship["destinationId"] == "404684003"


def test_snomedct_example_catalog_matches_the_manifest_checksum():
    """The manifest's ui.example_data pointer must match the published catalog."""
    import hashlib

    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    pointer = manifest.ui.example_data
    assert pointer["catalog"] == "examples/catalog.json"
    assert pointer["delivery"] == "fetch-and-checksum-verify"

    catalog_path = PACK_DIR / pointer["catalog"]
    payload = catalog_path.read_bytes()
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    assert digest == pointer["sha256"], "regenerate ui.example_data.sha256 after editing the catalog"

    import json

    catalog = json.loads(payload)
    assert catalog["contract"] == "strategy-example-catalog/v1"
    for section in pointer["sections"]:
        assert catalog.get(section) is not None, f"catalog section '{section}' is missing"


class _ConceptCollection:
    def __init__(self, concepts):
        self.concepts = concepts

    async def find_one(self, query, projection=None):
        for concept in self.concepts:
            if all(concept.get(key) == value for key, value in query.items()):
                if not projection:
                    return dict(concept)
                return {
                    key: value
                    for key, value in concept.items()
                    if key != "_id" and projection.get(key)
                }
        return None


class _ValueSetCollection:
    def __init__(self):
        self.documents = []

    async def find_one(self, query, projection=None, sort=None):
        matches = [
            item
            for item in self.documents
            if all(item.get(key) == value for key, value in query.items())
        ]
        if sort:
            for key, direction in reversed(sort):
                matches.sort(key=lambda item: item.get(key) or "", reverse=direction < 0)
        if not matches:
            return None
        item = dict(matches[0])
        item.pop("_id", None)
        return item

    async def insert_one(self, document):
        self.documents.append(dict(document))
        return SimpleNamespace(inserted_id=len(self.documents))


class _TerminologyDatabase:
    def __init__(self, concepts):
        self.collection = _ConceptCollection(concepts)

    def __getitem__(self, name):
        assert name == "snomed_concepts"
        return self.collection


class _Storage:
    def __init__(self, concepts):
        self.db = _TerminologyDatabase(concepts)


class _ValueSetDatabase:
    def __init__(self, concepts):
        self.collections = {
            "snomed_concepts": _ConceptCollection(concepts),
            "snomed_value_sets": _ValueSetCollection(),
        }

    def __getitem__(self, name):
        return self.collections[name]


class _ValueSetStorage:
    def __init__(self, concepts):
        self.db = _ValueSetDatabase(concepts)


def _terminology_context(manifest, concepts):
    return StrategyContext(
        environment_id="test",
        config=manifest.default_config,
        manifest=manifest,
        adapters={"storage": _Storage(concepts)},
    )


@pytest.mark.asyncio
async def test_snomedct_validate_code_checks_release_activity_and_display():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = _terminology_context(
        manifest,
        [
            {
                "conceptId": "73211009",
                "releaseId": "20260601",
                "active": True,
                "descriptions": [
                    {"active": True, "languageCode": "en", "term": "Diabetes mellitus"},
                    {"active": True, "languageCode": "es", "term": "Diabetes mellitus"},
                ],
            }
        ],
    )

    valid = await strategy.snomed_validate_code(
        ctx,
        {"code": "73211009", "language": "en", "display": "DIABETES mellitus"},
    )
    mismatch = await strategy.snomed_validate_code(
        ctx,
        {"code": "73211009", "language": "en", "display": "Hypertension"},
    )
    missing = await strategy.snomed_validate_code(ctx, {"code": "999999"})

    assert valid["valid"] is True
    assert valid["display"] == "Diabetes mellitus"
    assert mismatch["valid"] is False
    assert mismatch["issues"][0]["code"] == "display-mismatch"
    assert missing["valid"] is False
    assert missing["issues"][0]["code"] == "not-found"


@pytest.mark.asyncio
async def test_snomedct_subsumes_uses_materialized_ancestor_paths():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = _terminology_context(
        manifest,
        [
            {"conceptId": "404684003", "releaseId": "20260601", "active": True, "inferredAncestorIds": []},
            {
                "conceptId": "73211009",
                "releaseId": "20260601",
                "active": True,
                "inferredAncestorIds": ["404684003"],
            },
        ],
    )

    result = await strategy.snomed_subsumes(ctx, {"code_a": "404684003", "code_b": "73211009"})
    reverse = await strategy.snomed_subsumes(ctx, {"code_a": "73211009", "code_b": "404684003"})
    same = await strategy.snomed_subsumes(ctx, {"code_a": "73211009", "code_b": "73211009"})

    assert result["outcome"] == "subsumes"
    assert reverse["outcome"] == "subsumed-by"
    assert same["outcome"] == "equivalent"


@pytest.mark.asyncio
async def test_snomedct_registered_value_set_is_versioned_and_validates_membership():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    storage = _ValueSetStorage(
        [
            {
                "conceptId": "73211009",
                "releaseId": "20260601",
                "active": True,
                "descriptions": [
                    {"active": True, "languageCode": "en", "term": "Diabetes mellitus"}
                ],
            }
        ]
    )
    ctx = StrategyContext(
        environment_id="test",
        config=manifest.default_config,
        manifest=manifest,
        adapters={"storage": storage},
    )

    created = await strategy.snomed_put_value_set(
        ctx,
        {
            "url": "https://example.org/ValueSet/diabetes",
            "version": "1.0.0",
            "concepts": ["73211009"],
        },
    )
    replay = await strategy.snomed_put_value_set(
        ctx,
        {
            "url": "https://example.org/ValueSet/diabetes",
            "version": "1.0.0",
            "concepts": ["73211009"],
        },
    )
    member = await strategy.snomed_validate_value_set(
        ctx,
        {
            "url": "https://example.org/ValueSet/diabetes",
            "value_set_version": "1.0.0",
            "code": "73211009",
            "display": "Diabetes mellitus",
            "language": "en",
        },
    )

    assert created["created"] is True
    assert replay["created"] is False
    assert member["valid"] is True
    assert member["inValueSet"] is True
    assert member["definitionDigest"]


class _PagingStorage:
    def __init__(self, rows):
        self.rows = rows
        self.collection = None
        self.pipeline = None

    async def aggregate(self, collection, pipeline):
        self.collection = collection
        self.pipeline = pipeline
        return list(self.rows)


@pytest.mark.asyncio
async def test_snomedct_search_pagination_fetches_lookahead_and_returns_cursor_metadata():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(
        environment_id="test",
        config=manifest.default_config,
        manifest=manifest,
        adapters={"storage": _PagingStorage([{"conceptId": "1"}, {"conceptId": "2"}, {"conceptId": "3"}])},
    )

    plan = await strategy.compile_query(
        ctx,
        "snomedct",
        {"mode": "search", "q": "diabetes", "offset": 20, "limit": 2},
    )
    result = await strategy.execute_query(ctx, plan)

    assert {"$skip": 20} in plan.plan["pipeline"]
    assert {"$limit": 3} in plan.plan["pipeline"]
    assert [row["conceptId"] for row in result.rows] == ["1", "2"]
    assert result.explain["page"] == {
        "offset": 20,
        "limit": 2,
        "returned": 2,
        "hasMore": True,
        "nextOffset": 22,
    }


@pytest.mark.asyncio
async def test_snomedct_grounded_corpus_uses_materialized_ancestor_path_and_pagination():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    storage = _PagingStorage([{"reviewId": "r1"}, {"reviewId": "r2"}, {"reviewId": "r3"}])
    ctx = StrategyContext(
        environment_id="test",
        config=manifest.default_config,
        manifest=manifest,
        adapters={"storage": storage},
    )

    result = await strategy.snomed_query_grounded_corpus(
        ctx,
        {"concept_id": "404684003", "offset": 10, "limit": 2},
    )

    assert storage.collection == "snomed_grounding_reviews"
    assert storage.pipeline[0] == {"$match": {"acceptedAncestorIds": "404684003"}}
    assert {"$skip": 10} in storage.pipeline
    assert {"$limit": 3} in storage.pipeline
    assert [row["reviewId"] for row in result["reviews"]] == ["r1", "r2"]
    assert result["page"]["nextOffset"] == 12


class _GroundingReviewCollection(_ValueSetCollection):
    pass


class _GroundingDatabase:
    def __init__(self, concepts):
        self.collections = {
            "snomed_concepts": _ConceptCollection(concepts),
            "snomed_grounding_reviews": _GroundingReviewCollection(),
        }

    def __getitem__(self, name):
        return self.collections[name]


@pytest.mark.asyncio
async def test_snomedct_grounding_review_is_idempotent_and_does_not_store_source_text_by_default():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    db = _GroundingDatabase(
        [
            {
                "conceptId": "73211009",
                "releaseId": "20260601",
                "active": True,
                "inferredAncestorIds": ["404684003"],
                "descriptions": [{"active": True, "languageCode": "en", "term": "Diabetes mellitus"}],
            }
        ]
    )
    ctx = StrategyContext(
        environment_id="test",
        config=manifest.default_config,
        manifest=manifest,
        adapters={"storage": SimpleNamespace(db=db)},
    )
    payload = {
        "source_ref": "note-123",
        "text": "Patient has diabetes mellitus",
        "language": "en",
        "codings": [{"concept_id": "73211009", "status": "accepted"}],
    }

    created = await strategy.snomed_save_grounding_review(ctx, payload)
    replay = await strategy.snomed_save_grounding_review(ctx, payload)

    assert created["created"] is True
    assert replay["created"] is False
    assert created["review"]["sourceTextHash"]
    assert "sourceText" not in created["review"]
    assert created["review"]["codings"][0]["ancestorIds"] == ["73211009", "404684003"]
    assert created["review"]["acceptedAncestorIds"] == ["404684003", "73211009"]


@pytest.mark.asyncio
async def test_snomedct_retrieval_benchmark_reports_hit_rate_and_rank(monkeypatch):
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(environment_id="test", config=manifest.default_config, manifest=manifest)

    async def fake_search(_ctx, payload):
        return {"matches": [{"conceptId": "111111"}, {"conceptId": "73211009"}]}

    monkeypatch.setattr(strategy, "snomed_search", fake_search)
    result = await strategy.snomed_benchmark_retrieval(
        ctx,
        {"top_k": 5, "cases": [{"id": "diabetes", "query": "diabetes", "expected_concept_ids": ["73211009"]}]},
    )

    assert result["summary"]["hitRate"] == 1.0
    assert result["summary"]["meanReciprocalRank"] == 0.5
    assert result["cases"][0]["rank"] == 2
