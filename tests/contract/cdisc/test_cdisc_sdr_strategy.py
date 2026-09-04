import json
from pathlib import Path

import jsonschema
import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.cdisc.sdr.strategy import CDISCSDRStrategy
from kehrnel.engine.strategies.cdisc.sdr.common import MODEL_SCHEMA_VERSION


ROOT = Path(__file__).resolve().parents[3]
PACK = ROOT / "src" / "kehrnel" / "engine" / "strategies" / "cdisc" / "sdr"
FIXTURE = ROOT / "tests" / "fixtures" / "cdisc" / "dm.dataset.json"


class MemoryStorage:
    def __init__(self):
        self.data = {}

    async def replace_many(self, collection, docs):
        target = self.data.setdefault(collection, {})
        for doc in docs:
            target[doc["_id"]] = doc

    async def find_one(self, collection, flt, projection=None):
        if "_id" in flt:
            candidate = self.data.get(collection, {}).get(flt["_id"])
            if candidate and all(candidate.get(key) == value for key, value in flt.items() if key != "_id"):
                return candidate
            return None
        return next(
            (
                document
                for document in self.data.get(collection, {}).values()
                if all(document.get(key) == value for key, value in flt.items())
            ),
            None,
        )

    async def aggregate(self, collection, pipeline, allow_disk_use=True):
        def value_at(document, path):
            value = document
            for part in path.split("."):
                if isinstance(value, list):
                    value = [item.get(part) for item in value if isinstance(item, dict)]
                elif isinstance(value, dict):
                    value = value.get(part)
                else:
                    return None
            return value

        def matches(document, expression):
            if "$and" in expression:
                return all(matches(document, item) for item in expression["$and"])
            for path, expected in expression.items():
                actual = value_at(document, path)
                if isinstance(expected, dict):
                    if "$in" in expected and actual not in expected["$in"]:
                        return False
                elif isinstance(actual, list):
                    if expected not in actual:
                        return False
                elif actual != expected:
                    return False
            return True

        documents = list(self.data.get(collection, {}).values())
        for stage in pipeline:
            if "$match" in stage:
                documents = [document for document in documents if matches(document, stage["$match"])]
            elif "$lookup" in stage:
                lookup = stage["$lookup"]
                foreign = self.data.get(lookup["from"], {}).values()
                for document in documents:
                    document[lookup["as"]] = [
                        item for item in foreign
                        if item.get(lookup["foreignField"]) == document.get(lookup["localField"])
                    ]
            elif "$sort" in stage:
                for key, direction in reversed(list(stage["$sort"].items())):
                    documents.sort(key=lambda document: value_at(document, key), reverse=direction < 0)
            elif "$project" in stage:
                selected = [key for key, include in stage["$project"].items() if include and key != "_id"]
                documents = [{key: value_at(document, key) for key in selected} for document in documents]
            elif "$skip" in stage:
                documents = documents[stage["$skip"]:]
            elif "$limit" in stage:
                documents = documents[:stage["$limit"]]
        return documents


class MemoryArtifactStore:
    provider = "memory"

    def __init__(self):
        self.objects = {}

    async def put(self, key, content, *, media_type, metadata=None):
        from kehrnel.persistence.artifacts import ArtifactLocation

        existing = self.objects.get(key)
        if existing is not None and existing != content:
            raise ValueError("immutable key conflict")
        self.objects[key] = content
        return ArtifactLocation(key=key, uri=f"memory://{key}", provider=self.provider, size=len(content))

    async def get(self, key):
        return self.objects[key]

    async def stat(self, key):
        import hashlib
        from kehrnel.persistence.artifacts import ArtifactLocation

        content = self.objects[key]
        return ArtifactLocation(
            key=key,
            uri=f"memory://{key}",
            provider=self.provider,
            size=len(content),
            metadata={"sha256": hashlib.sha256(content).hexdigest()},
        )

    async def create_upload(self, key, *, media_type, size, sha256, expires_in):
        return {
            "url": f"memory://upload/{key}", "method": "PUT",
            "headers": {"x-memory-sha256": sha256}, "contentLength": size,
        }

    async def create_download(self, key, *, expires_in):
        return {"url": f"memory://download/{key}", "method": "GET", "headers": {}}


def _payload():
    return {
        "datasetJSON": json.loads(FIXTURE.read_text(encoding="utf-8")),
        "packageId": "package-1",
        "snapshotId": "snapshot-1",
        "standardsPackageId": "standards-1",
        "profile": "sdtm",
        "publicationState": "published",
        "sourceArtifactId": "artifact-1",
        "standard": {
            "family": "SDTM",
            "implementationGuide": "SDTMIG",
            "implementationGuideVersion": "pinned-test-version",
            "exchangeStandard": "Dataset-JSON",
            "exchangeVersion": "1.1.0"
        }
    }


def _context(storage=None, artifact_store=None, tenant_id="tenant-a", require_validation=False):
    manifest = load_strategy("cdisc.sdr", PACK)
    adapters = {}
    if storage:
        adapters["storage"] = storage
    if artifact_store:
        adapters["artifact_store"] = artifact_store
    return StrategyContext(
        environment_id="test",
        config={
            **manifest.default_config,
            "tenant_id": tenant_id,
            "validation": {"require_before_publish": require_validation},
        },
        adapters=adapters,
        manifest=manifest,
        meta={},
    )


def test_cdisc_pack_loads_and_declares_profiles():
    manifest = load_strategy("cdisc.sdr", PACK)
    assert manifest.domain == "cdisc"
    assert manifest.pack_spec["profiles"] == ["send", "sdtm", "adam", "tig"]
    assert "query" in manifest.capabilities


def test_cdisc_manifest_spec_config_and_output_contracts_are_release_aligned():
    manifest_payload = json.loads((PACK / "manifest.json").read_text(encoding="utf-8"))
    spec = json.loads((PACK / "spec.json").read_text(encoding="utf-8"))
    schema = json.loads((PACK / "schema.json").read_text(encoding="utf-8"))
    defaults = json.loads((PACK / "defaults.json").read_text(encoding="utf-8"))

    assert manifest_payload["spec"]["version"] == spec["meta"]["specVersion"]
    assert spec["meta"]["modelSchemaVersion"] == MODEL_SCHEMA_VERSION
    assert manifest_payload["config"]["strategy"] == {
        "schema": "schema.json",
        "defaults": "defaults.json",
        "description": "CDISC study repository, governance, query, projection, and artifact configuration",
    }
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(defaults, schema)
    assert all(op["output_schema"].get("properties") for op in manifest_payload["ops"])
    assert all(op["output_schema"].get("required") for op in manifest_payload["ops"])
    for op in manifest_payload["ops"]:
        jsonschema.Draft7Validator.check_schema(op["input_schema"])
        jsonschema.Draft7Validator.check_schema(op["output_schema"])


def test_cdisc_example_catalog_is_small_external_and_checksum_pinned():
    catalog = json.loads((PACK / "examples" / "catalog.json").read_text(encoding="utf-8"))

    assert {item["profile"] for item in catalog["examples"]} == {"sdtm", "send"}
    assert {item["id"] for item in catalog["examples"]} == {
        "cdisc-pilot-sdtm-dm", "phuse-ffu-send",
    }
    for example in catalog["examples"]:
        assert example["source"]["distribution"] == "fetch-only"
        assert len(example["source"]["revision"]) == 40
        assert example["files"]
        for item in example["files"]:
            assert item["url"].startswith("https://raw.githubusercontent.com/")
            assert example["source"]["revision"] in item["url"]
            assert len(item["sha256"]) == 64


def test_cdisc_pack_matches_the_rps_visualization_contract():
    """CDISC uses the same strategy-pack sections and model tabs as RPS."""

    manifest = load_strategy("cdisc.sdr", PACK)
    spec = manifest.pack_spec

    assert spec["meta"]["specVersion"] == "1.0"
    assert spec["logicalModel"]["concepts"]
    assert spec["logicalModel"]["destinations"]["types"]
    assert spec["storageModel"]["stores"]
    assert spec["transformModel"]["pipeline"]
    assert spec["queryModel"]["modes"]
    assert spec["visualization"]["collectionModel"]["entities"]
    assert spec["visualization"]["transformGraph"]["nodes"]
    assert [view["id"] for view in spec["visualization"]["canvas"]["views"]] == [
        "overview", "collections", "dictionaries", "transform"
    ]

    configured_collections = {
        f"collections.{name}" for name in manifest.default_config["collections"]
    }
    modeled_collections = {
        store["collectionNameFromConfig"] for store in spec["storageModel"]["stores"]
    }
    assert modeled_collections == configured_collections

    record = next(
        concept for concept in spec["logicalModel"]["concepts"]
        if concept["id"] == "cdisc.record.v1"
    )
    assert record["polymorphism"] == {
        "discriminator": ["profile", "domain"],
        "schemaSource": "cdisc.dataset.metamodel.v1.variables",
        "dynamicObject": "data",
        "stableProjection": "facets",
    }

    assert manifest.ui.story
    assert manifest.ui.how_it_works
    assert manifest.ui.ideal_for
    assert manifest.ui.consider_alternatives


@pytest.mark.asyncio
async def test_spec_indexes_match_the_runtime_plan():
    manifest = load_strategy("cdisc.sdr", PACK)
    strategy = CDISCSDRStrategy()
    ctx = _context(MemoryStorage())
    plan = await strategy.plan(ctx)

    runtime = {
        item["options"]["name"]: {
            "collection": item["collection"],
            "fields": [field for field, _direction in item["keys"]],
        }
        for item in plan.artifacts["indexes"]
    }
    modeled = {}
    for store in manifest.pack_spec["storageModel"]["stores"]:
        config_key = store["collectionNameFromConfig"].split(".", 1)[1]
        for index in store.get("indexes") or []:
            modeled[index["id"]] = {
                "collection": manifest.default_config["collections"][config_key],
                "fields": index["fields"],
            }
    assert runtime == modeled


@pytest.mark.asyncio
async def test_config_rejects_ambiguous_collections_and_inconsistent_semantic_settings():
    strategy = CDISCSDRStrategy()
    defaults = load_strategy("cdisc.sdr", PACK).default_config

    duplicate = json.loads(json.dumps(defaults))
    duplicate["collections"]["records"] = duplicate["collections"]["datasets"]
    with pytest.raises(KehrnelError) as exc:
        await strategy.validate_config(duplicate)
    assert exc.value.code == "CONFIG_INVALID"

    inconsistent = json.loads(json.dumps(defaults))
    inconsistent["semantic"].update(enabled=False, embed_on_rebuild=True)
    with pytest.raises(KehrnelError) as exc:
        await strategy.validate_config(inconsistent)
    assert exc.value.code == "CONFIG_INVALID"


@pytest.mark.asyncio
async def test_cdisc_ingest_is_idempotent_and_writes_snapshot_marker_last():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)

    first = await strategy.ingest(ctx, _payload())
    second = await strategy.ingest(ctx, _payload())

    assert first["contentHash"] == second["contentHash"]
    assert len(storage.data["cdisc_records"]) == 2
    assert len(storage.data["cdisc_datasets"]) == 1
    snapshot = storage.data["cdisc_snapshots"]["tenant-a:STUDY-001:snapshot-1"]
    assert snapshot["state"] == "published"
    assert first["idempotent"] is True
    for collection in ("cdisc_records", "cdisc_datasets", "cdisc_studies", "cdisc_snapshots", "cdisc_transformations"):
        assert storage.data[collection]
        assert all(
            document["modelSchemaVersion"] == MODEL_SCHEMA_VERSION
            for document in storage.data[collection].values()
        )


@pytest.mark.asyncio
async def test_staged_snapshot_is_published_by_atomic_marker():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)
    payload = _payload()
    payload["publicationState"] = "staged"

    await strategy.ingest(ctx, payload)
    snapshot_ref = "tenant-a:STUDY-001:snapshot-1"
    assert storage.data["cdisc_snapshots"][snapshot_ref]["state"] == "canonicalized"

    result = await strategy.run_op(
        ctx,
        "cdisc_publish_snapshot",
        {"studyId": "STUDY-001", "snapshotId": "snapshot-1"},
    )
    repeated = await strategy.run_op(
        ctx,
        "cdisc_publish_snapshot",
        {"studyId": "STUDY-001", "snapshotId": "snapshot-1"},
    )

    assert result["alreadyPublished"] is False
    assert storage.data["cdisc_snapshots"][snapshot_ref]["state"] == "published"
    assert repeated["alreadyPublished"] is True


@pytest.mark.asyncio
async def test_published_dataset_identity_rejects_changed_content():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)
    await strategy.ingest(ctx, _payload())
    changed = _payload()
    changed["datasetJSON"]["rows"][0][-1] = "X"

    with pytest.raises(KehrnelError) as exc:
        await strategy.ingest(ctx, changed)

    assert exc.value.code == "CDISC_SNAPSHOT_IMMUTABLE"


@pytest.mark.asyncio
async def test_cdisc_query_explain_reports_governance():
    strategy = CDISCSDRStrategy()
    ctx = _context(MemoryStorage())
    plan = await strategy.compile_query(
        ctx,
        "cdisc",
        {
            "scope": {"studies": ["STUDY-001"], "snapshots": "published"},
            "from": {"profile": "sdtm", "domains": ["DM"]},
            "where": {"and": [{"path": "data.SEX", "op": "eq", "value": "F"}]},
            "select": ["studyId", "data.USUBJID", "data.SEX"]
        },
    )

    assert plan.engine == "mongo_pipeline"
    assert plan.explain["governance"]["tenantInjected"] is True
    assert plan.explain["governance"]["modelSchemaVersionInjected"] == MODEL_SCHEMA_VERSION
    assert plan.plan["pipeline"][1]["$lookup"]["from"] == "cdisc_snapshots"


@pytest.mark.asyncio
async def test_published_records_execute_through_governed_query():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)
    await strategy.ingest(ctx, _payload())
    plan = await strategy.compile_query(ctx, "cdisc", {
        "scope": {"studies": ["STUDY-001"], "snapshots": "published"},
        "from": {"profile": "sdtm", "domains": ["DM"]},
        "where": {"and": [{"path": "data.SEX", "op": "eq", "value": "F"}]},
        "select": ["studyId", "data.USUBJID", "data.SEX"],
    })

    result = await strategy.execute_query(ctx, plan)

    assert result.rows == [{
        "studyId": "STUDY-001", "data.USUBJID": "STUDY-001-001", "data.SEX": "F",
    }]


@pytest.mark.asyncio
async def test_governed_query_denies_cross_tenant_records():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    await strategy.ingest(_context(storage, tenant_id="tenant-a"), _payload())
    await strategy.ingest(_context(storage, tenant_id="tenant-b"), _payload())

    ctx = _context(storage, tenant_id="tenant-a")
    plan = await strategy.compile_query(ctx, "cdisc", {
        "scope": {"studies": ["STUDY-001"], "snapshots": "published"},
        "from": {"profile": "sdtm", "domains": ["DM"]},
        "where": {"and": []},
        "select": ["studyId", "data.USUBJID"],
    })
    result = await strategy.execute_query(ctx, plan)

    assert len(result.rows) == 2
    assert plan.plan["pipeline"][0]["$match"]["$and"][0] == {"tenantId": "tenant-a"}


@pytest.mark.asyncio
async def test_repository_discovery_is_tenant_scoped_and_paginated():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)
    await strategy.ingest(ctx, _payload())
    storage.data["cdisc_studies"]["tenant-a:STUDY-002"] = {
        "_id": "tenant-a:STUDY-002", "tenantId": "tenant-a", "studyId": "STUDY-002",
        "profiles": ["send"], "updatedAt": "2026-01-02T00:00:00+00:00",
    }
    storage.data["cdisc_studies"]["tenant-b:HIDDEN"] = {
        "_id": "tenant-b:HIDDEN", "tenantId": "tenant-b", "studyId": "HIDDEN",
        "profiles": ["sdtm"], "updatedAt": "2026-01-03T00:00:00+00:00",
    }

    first = await strategy.run_op(ctx, "cdisc_list_studies", {"pageSize": 1})
    second = await strategy.run_op(
        ctx, "cdisc_list_studies", {"pageSize": 1, "cursor": first["page"]["nextCursor"]}
    )

    assert first["page"] == {
        "size": 1, "hasMore": True, "nextCursor": first["page"]["nextCursor"]
    }
    assert {first["items"][0]["studyId"], second["items"][0]["studyId"]} == {
        "STUDY-001", "STUDY-002"
    }
    assert second["page"]["hasMore"] is False
    assert all(item["tenantId"] == "tenant-a" for item in first["items"] + second["items"])


@pytest.mark.asyncio
async def test_repository_snapshot_summary_and_dataset_listing():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)
    ingested = await strategy.ingest(ctx, _payload())

    summary = await strategy.run_op(
        ctx, "cdisc_snapshot_summary", {"studyId": "STUDY-001", "snapshotId": "snapshot-1"}
    )
    datasets = await strategy.run_op(
        ctx,
        "cdisc_list_datasets",
        {"studyId": "STUDY-001", "snapshotId": "snapshot-1", "domain": "dm"},
    )
    snapshots = await strategy.run_op(
        ctx, "cdisc_list_snapshots", {"studyId": "STUDY-001", "state": "published"}
    )

    assert summary["snapshot"]["_id"] == ingested["snapshotRef"]
    assert summary["summary"]["datasetCount"] == 1
    assert summary["summary"]["recordCount"] == 2
    assert summary["summary"]["domains"] == ["DM"]
    assert [item["domain"] for item in datasets["items"]] == ["DM"]
    assert [item["snapshotId"] for item in snapshots["items"]] == ["snapshot-1"]


@pytest.mark.asyncio
async def test_repository_lists_validation_standards_and_snapshot_artifacts():
    import base64

    storage = MemoryStorage()
    artifact_store = MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifact_store, require_validation=True)
    stored = await strategy.run_op(
        ctx,
        "cdisc_store_artifact",
        {
            "contentBase64": base64.b64encode(b"source").decode("ascii"),
            "mediaType": "application/octet-stream",
            "kind": "source",
        },
    )
    payload = _payload()
    payload["publicationState"] = "staged"
    payload["sourceArtifactId"] = stored["artifact"]["artifactId"]
    await strategy.ingest(ctx, payload)
    await strategy.run_op(
        ctx, "cdisc_validate_snapshot", {"studyId": "STUDY-001", "snapshotId": "snapshot-1"}
    )
    await strategy.run_op(
        ctx,
        "cdisc_register_standards",
        {"package": {"packageId": "standards-1", "profile": "sdtm", "standard": {"family": "SDTM"}}},
    )

    runs = await strategy.run_op(
        ctx, "cdisc_list_validation_runs", {"studyId": "STUDY-001", "snapshotId": "snapshot-1"}
    )
    standards = await strategy.run_op(ctx, "cdisc_list_standards", {"profile": "sdtm"})
    artifacts = await strategy.run_op(
        ctx, "cdisc_list_artifacts", {"studyId": "STUDY-001", "snapshotId": "snapshot-1", "kind": "source"}
    )

    assert len(runs["items"]) == 1
    assert [item["packageId"] for item in standards["items"]] == ["standards-1"]
    assert standards["items"][0]["profile"] == "sdtm"
    assert standards["items"][0]["standard"] == {"family": "SDTM", "terminologyPackages": []}
    assert [item["artifactId"] for item in artifacts["items"]] == [stored["artifact"]["artifactId"]]


@pytest.mark.asyncio
async def test_repository_rejects_cursor_for_another_resource():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)
    await strategy.ingest(ctx, _payload())
    page = await strategy.run_op(ctx, "cdisc_list_studies", {"pageSize": 1})
    storage.data["cdisc_studies"]["tenant-a:STUDY-002"] = {
        "_id": "tenant-a:STUDY-002", "tenantId": "tenant-a", "studyId": "STUDY-002",
        "profiles": ["send"], "updatedAt": "2026-01-02T00:00:00+00:00",
    }
    page = await strategy.run_op(ctx, "cdisc_list_studies", {"pageSize": 1})

    with pytest.raises(KehrnelError) as exc:
        await strategy.run_op(
            ctx, "cdisc_list_datasets", {"cursor": page["page"]["nextCursor"]}
        )

    assert exc.value.code == "INVALID_CURSOR"


@pytest.mark.asyncio
async def test_cdisc_query_execution_propagates_storage_failure():
    class BrokenStorage(MemoryStorage):
        async def aggregate(self, collection, pipeline, allow_disk_use=True):
            raise RuntimeError("database unavailable")

    strategy = CDISCSDRStrategy()
    ctx = _context(BrokenStorage())
    plan = await strategy.compile_query(ctx, "cdisc", {"scope": {}, "from": {}, "where": {"and": []}})

    with pytest.raises(KehrnelError) as exc:
        await strategy.execute_query(ctx, plan)
    assert exc.value.code == "CDISC_QUERY_EXECUTION_FAILED"


@pytest.mark.asyncio
async def test_artifact_store_replay_and_checksum_enforcement():
    storage = MemoryStorage()
    artifact_store = MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifact_store)
    raw = b"original Dataset-JSON bytes\n"
    import base64
    import hashlib

    stored = await strategy.run_op(
        ctx,
        "cdisc_store_artifact",
        {
            "contentBase64": base64.b64encode(raw).decode("ascii"),
            "mediaType": "application/json",
            "sourceName": "dm.json",
            "expectedSha256": hashlib.sha256(raw).hexdigest(),
        },
    )
    artifact_id = stored["artifact"]["artifactId"]
    repeated = await strategy.run_op(
        ctx,
        "cdisc_store_artifact",
        {
            "contentBase64": base64.b64encode(raw).decode("ascii"),
            "mediaType": "application/json",
            "sourceName": "dm.json",
            "expectedSha256": hashlib.sha256(raw).hexdigest(),
        },
    )
    replayed = await strategy.run_op(ctx, "cdisc_replay_artifact", {"artifactId": artifact_id})

    assert repeated["created"] is False
    assert base64.b64decode(replayed["contentBase64"]) == raw
    assert replayed["integrityVerified"] is True
    with pytest.raises(KehrnelError) as exc:
        await strategy.run_op(
            ctx,
            "cdisc_store_artifact",
            {
                "contentBase64": base64.b64encode(raw).decode("ascii"),
                "mediaType": "application/json",
                "expectedSha256": "0" * 64,
            },
        )
    assert exc.value.code == "ARTIFACT_CHECKSUM_MISMATCH"


@pytest.mark.asyncio
async def test_direct_upload_finalize_and_download_are_checksum_governed():
    import hashlib

    storage = MemoryStorage()
    artifact_store = MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifact_store)
    content = b"large xpt placeholder"
    digest = hashlib.sha256(content).hexdigest()

    initiated = await strategy.run_op(
        ctx,
        "cdisc_initiate_upload",
        {"sha256": digest, "size": len(content), "mediaType": "application/x-sas-xport"},
    )
    artifact_store.objects[initiated["objectKey"]] = content
    finalized = await strategy.run_op(
        ctx,
        "cdisc_finalize_upload",
        {
            "uploadId": initiated["uploadId"], "sha256": digest, "size": len(content),
            "mediaType": "application/x-sas-xport", "sourceName": "dm.xpt",
        },
    )
    download = await strategy.run_op(
        ctx, "cdisc_prepare_download", {"artifactId": finalized["artifact"]["artifactId"]}
    )

    assert initiated["target"]["method"] == "PUT"
    assert finalized["artifact"]["digest"]["value"] == digest
    assert download["target"]["method"] == "GET"
    assert finalized["artifact"]["tenantId"] == "tenant-a"


@pytest.mark.asyncio
async def test_dataset_json_export_persists_equivalent_artifact_and_audit():
    storage = MemoryStorage()
    artifact_store = MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifact_store)
    ingested = await strategy.ingest(ctx, _payload())

    exported = await strategy.run_op(
        ctx,
        "cdisc_export_dataset_json",
        {"datasetId": ingested["datasetId"], "persistArtifact": True},
    )

    assert exported["datasetJSON"]["rows"] == _payload()["datasetJSON"]["rows"]
    assert exported["equivalence"]["equivalent"] is True
    assert exported["equivalence"]["guarantee"] == "semantic"
    assert exported["artifact"]["metadata"]["kind"] == "generated-dataset-json"
    assert exported["executionId"] in storage.data["cdisc_transformations"]


@pytest.mark.asyncio
async def test_replay_detects_corrupted_artifact_bytes():
    storage = MemoryStorage()
    artifact_store = MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifact_store)
    import base64

    stored = await strategy.run_op(
        ctx,
        "cdisc_store_artifact",
        {"contentBase64": base64.b64encode(b"trusted").decode("ascii"), "mediaType": "application/octet-stream"},
    )
    artifact = stored["artifact"]
    artifact_store.objects[artifact["objectKey"]] = b"corrupt"

    with pytest.raises(KehrnelError) as exc:
        await strategy.run_op(ctx, "cdisc_replay_artifact", {"artifactId": artifact["artifactId"]})

    assert exc.value.code == "ARTIFACT_INTEGRITY_FAILED"


@pytest.mark.asyncio
async def test_export_rejects_an_incomplete_stored_dataset():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)
    ingested = await strategy.ingest(ctx, _payload())
    storage.data["cdisc_records"].pop(next(iter(storage.data["cdisc_records"])))

    with pytest.raises(KehrnelError) as exc:
        await strategy.reverse_transform(ctx, {"datasetId": ingested["datasetId"]})

    assert exc.value.code == "CDISC_DATASET_INCOMPLETE"


@pytest.mark.asyncio
async def test_validation_gate_blocks_then_allows_publication():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, require_validation=True)
    payload = _payload()
    payload["publicationState"] = "staged"
    ingested = await strategy.ingest(ctx, payload)

    with pytest.raises(KehrnelError) as exc:
        await strategy.run_op(ctx, "cdisc_publish_snapshot", {"studyId": "STUDY-001", "snapshotId": "snapshot-1"})
    assert exc.value.code == "CDISC_SNAPSHOT_NOT_PUBLISHABLE"

    validated = await strategy.run_op(ctx, "cdisc_validate_snapshot", {"studyId": "STUDY-001", "snapshotId": "snapshot-1"})
    published = await strategy.run_op(ctx, "cdisc_publish_snapshot", {"studyId": "STUDY-001", "snapshotId": "snapshot-1"})
    assert validated["ok"] is True
    assert published["state"] == "published"
    assert ingested["snapshotRef"] in storage.data["cdisc_snapshots"]


@pytest.mark.asyncio
async def test_synthetic_study_runs_through_ingest_validate_and_publish():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, require_validation=True)

    result = await strategy.run_op(ctx, "cdisc_generate_synthetic_study", {
        "recipe": {"studyId": "SYNTH-E2E", "subjects": 10, "seed": 123},
        "persist": True,
        "validate": True,
        "publish": True,
    })

    assert result["ok"] is True
    assert len(result["ingested"]) == 4
    assert result["validation"]["snapshotState"] == "validated"
    assert result["publication"]["state"] == "published"
    assert len(storage.data["cdisc_records"]) >= 30


@pytest.mark.asyncio
async def test_synthetic_publication_cannot_bypass_validation():
    strategy = CDISCSDRStrategy()
    ctx = _context(MemoryStorage(), require_validation=True)

    with pytest.raises(KehrnelError) as exc:
        await strategy.run_op(ctx, "cdisc_generate_synthetic_study", {
            "recipe": {"studyId": "SYNTH-NO-VALIDATION", "subjects": 2},
            "persist": True,
            "validate": False,
            "publish": True,
        })

    assert exc.value.code == "CDISC_SYNTHETIC_PUBLICATION_REQUIRES_VALIDATION"


@pytest.mark.asyncio
async def test_curated_example_publication_cannot_bypass_validation():
    strategy = CDISCSDRStrategy()
    ctx = _context(MemoryStorage(), require_validation=True)

    with pytest.raises(KehrnelError) as exc:
        await strategy.run_op(ctx, "cdisc_ingest_example", {
            "exampleId": "cdisc-pilot-sdtm-dm",
            "acknowledgeTerms": True,
            "validate": False,
            "publish": True,
        })

    assert exc.value.code == "CDISC_EXAMPLE_PUBLICATION_REQUIRES_VALIDATION"


@pytest.mark.asyncio
async def test_declared_synthetic_anomaly_is_normalized_and_blocks_publish():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, require_validation=True)

    result = await strategy.run_op(ctx, "cdisc_generate_synthetic_study", {
        "recipe": {"studyId": "SYNTH-FAIL", "subjects": 20, "seed": 7, "anomalyRate": 0.2},
        "persist": True,
        "validate": True,
        "publish": True,
    })

    assert result["ok"] is False
    assert result["validation"]["snapshotState"] == "quarantined"
    assert result["publication"] == {"state": "blocked", "reason": "validation_failed"}
    assert any(
        finding["ruleId"] == "SDR.AE.AEDECOD.REQUIRED"
        for finding in result["validation"]["findings"]
    )
