import base64
import io
import hashlib
import zipfile

import pytest

from kehrnel.engine.strategies.cdisc.sdr.strategy import CDISCSDRStrategy
from kehrnel.persistence.artifacts import FileSystemArtifactStore
from tests.contract.cdisc.test_cdisc_sdr_strategy import MemoryArtifactStore, MemoryStorage, _context


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["send", "adam", "tig"])
async def test_each_extended_profile_runs_generate_ingest_validate_publish(profile):
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, require_validation=True)

    result = await strategy.run_op(ctx, "cdisc_generate_synthetic_study", {
        "recipe": {"studyId": f"E2E-{profile.upper()}", "profile": profile, "subjects": 8, "seed": 11},
        "persist": True, "validate": True, "publish": True,
    })

    assert result["ok"] is True
    assert result["modelSource"]["kind"] == "builtin-cdisc-profile-metamodel"
    assert result["modelSource"]["profile"] == profile
    assert result["modelSource"]["modelDigest"].startswith("sha256:")
    assert result["watermark"]["modelDigest"] == result["modelSource"]["modelDigest"]
    assert result["publication"]["state"] == "published"
    assert result["publication"]["projections"]["materializationCount"] > 0


@pytest.mark.asyncio
async def test_package_ingest_publish_projection_search_export_and_lineage():
    storage, artifacts = MemoryStorage(), MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifacts, require_validation=True)
    generated = strategy.synthetic.generate({"studyId": "PKG-001", "subjects": 6, "seed": 13})

    imported = await strategy.run_op(ctx, "cdisc_ingest_package", {
        "datasets": list(generated["datasets"].values()),
        "packageId": "package-1", "snapshotId": "v1", "standardsPackageId": "standards-1",
        "profile": "sdtm", "standard": {"family": "SDTM"}, "validate": True, "publish": True,
    })
    searched = await strategy.run_op(ctx, "cdisc_semantic_search", {
        "q": "headache", "mode": "lexical", "studyIds": ["PKG-001"],
        "snapshotIds": ["v1"], "domains": ["AE"],
    })
    exported = await strategy.run_op(ctx, "cdisc_export_package", {"studyId": "PKG-001", "snapshotId": "v1"})
    lineage = await strategy.run_op(ctx, "cdisc_get_lineage", {"studyId": "PKG-001", "snapshotId": "v1"})
    views = await strategy.run_op(ctx, "cdisc_browse_projections", {
        "studyId": "PKG-001", "snapshotId": "v1", "resource": "materializations",
        "kind": "subject-timeline",
    })
    validation_run = await strategy.run_op(ctx, "cdisc_get_validation_run", {
        "runId": imported["validation"]["run"]["runId"],
    })

    assert imported["ok"] is True
    assert imported["publication"]["projections"]["entityCount"] > 0
    assert searched["modeUsed"] == "lexical"
    assert all(row["domain"] == "AE" for row in searched["rows"])
    package_bytes = artifacts.objects[exported["artifact"]["objectKey"]]
    with zipfile.ZipFile(io.BytesIO(package_bytes)) as archive:
        assert "manifest.json" in archive.namelist()
        assert "datasets/dm.dataset.json" in archive.namelist()
    assert len(lineage["datasets"]) == 4
    assert views["count"] == 6
    assert validation_run["run"]["status"] == "passed"
    snapshot = storage.data["cdisc_snapshots"]["tenant-a:PKG-001:v1"]
    assert exported["artifact"]["artifactId"] in snapshot["artifactIds"]


@pytest.mark.asyncio
async def test_scoped_waiver_changes_a_blocking_finding_to_audited_info():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, require_validation=True)
    generated = strategy.synthetic.generate({"studyId": "WAIVER-1", "subjects": 20, "seed": 7, "anomalyRate": 0.2})
    for document in generated["datasets"].values():
        await strategy.ingest(ctx, {
            "datasetJSON": document, "packageId": "p", "snapshotId": "v1",
            "standardsPackageId": "s", "profile": "sdtm", "standard": {"family": "SDTM"},
            "publicationState": "staged",
        })
    await strategy.run_op(ctx, "cdisc_register_validation_waiver", {
        "waiverId": "approved-aedecod", "ruleId": "SDR.AE.AEDECOD.REQUIRED",
        "studyId": "WAIVER-1", "justification": "Known synthetic negative-control case", "approvedBy": "quality@example.test",
    })

    validated = await strategy.run_op(ctx, "cdisc_validate_snapshot", {"studyId": "WAIVER-1", "snapshotId": "v1"})

    assert validated["ok"] is True
    assert validated["run"]["summary"]["waived"] > 0
    assert all(item["severity"] == "info" for item in validated["findings"] if item.get("waived"))


@pytest.mark.asyncio
async def test_xpt_export_is_verified_by_readback():
    pytest.importorskip("pyreadstat")
    storage, artifacts = MemoryStorage(), MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifacts)
    document = strategy.synthetic.generate({"studyId": "XPT-1", "subjects": 3})["datasets"]["DM"]
    ingested = await strategy.ingest(ctx, {
        "datasetJSON": document, "packageId": "p", "snapshotId": "v1",
        "profile": "sdtm", "standard": {"family": "SDTM"}, "publicationState": "staged",
    })

    exported = await strategy.run_op(ctx, "cdisc_export_xpt", {"datasetId": ingested["datasetId"], "version": 5})

    assert exported["equivalence"]["equivalent"] is True
    assert exported["recordCount"] == 3
    assert artifacts.objects[exported["artifact"]["objectKey"]].startswith(b"HEADER RECORD")
    snapshot = storage.data["cdisc_snapshots"]["tenant-a:XPT-1:v1"]
    assert exported["artifact"]["artifactId"] in snapshot["artifactIds"]


@pytest.mark.asyncio
async def test_preuploaded_object_can_be_verified_and_registered(tmp_path):
    storage = MemoryStorage()
    artifact_store = FileSystemArtifactStore(tmp_path)
    content = b"large-object-uploaded-outside-the-api"
    key = "uploads/study/source.xpt"
    await artifact_store.put(key, content, media_type="application/x-sas-xport")
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifact_store)

    registered = await strategy.run_op(ctx, "cdisc_register_external_artifact", {
        "objectKey": key, "sha256": hashlib.sha256(content).hexdigest(), "size": len(content),
        "mediaType": "application/x-sas-xport", "sourceName": "source.xpt",
    })

    assert registered["created"] is True
    assert registered["artifact"]["objectKey"] == key


@pytest.mark.asyncio
async def test_governed_query_cursor_returns_non_overlapping_pages():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)
    document = strategy.synthetic.generate({"studyId": "PAGE-1", "subjects": 7})["datasets"]["DM"]
    await strategy.ingest(ctx, {
        "datasetJSON": document, "packageId": "p", "snapshotId": "v1",
        "profile": "sdtm", "standard": {"family": "SDTM"}, "publicationState": "published",
    })
    query = {
        "scope": {"studies": ["PAGE-1"], "snapshots": "published"},
        "from": {"domains": ["DM"]}, "select": ["data.USUBJID"],
        "orderBy": [{"path": "data.USUBJID", "direction": "asc"}], "page": {"limit": 3},
    }
    first_plan = await strategy.compile_query(ctx, "cdisc", query)
    first = await strategy.execute_query(ctx, first_plan)
    query["page"]["token"] = first.explain["pagination"]["nextToken"]
    second = await strategy.execute_query(ctx, await strategy.compile_query(ctx, "cdisc", query))

    assert len(first.rows) == len(second.rows) == 3
    assert {row["data.USUBJID"] for row in first.rows}.isdisjoint(row["data.USUBJID"] for row in second.rows)


@pytest.mark.asyncio
async def test_standards_assets_are_checksum_linked_and_registration_is_idempotent():
    storage, artifacts = MemoryStorage(), MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifacts)
    content = b"licensed-rule-package-manifest"
    stored = await strategy.run_op(ctx, "cdisc_store_artifact", {
        "contentBase64": base64.b64encode(content).decode(),
        "mediaType": "application/json", "kind": "standards-manifest",
    })
    package = {
        "packageId": "sdtmig-test", "profile": "sdtm", "standard": {"family": "SDTM"},
        "assets": [{
            "assetId": "rules", "artifactId": stored["artifact"]["artifactId"], "kind": "rule-manifest",
            "version": "1", "digest": {"algorithm": "sha256", "value": hashlib.sha256(content).hexdigest()},
            "redistribution": "customer_supplied",
        }],
        "validationEngines": {"core": "configured-externally"}, "rulePackages": ["rules-v1"],
    }

    first = await strategy.run_op(ctx, "cdisc_register_standards", {"package": package})
    second = await strategy.run_op(ctx, "cdisc_register_standards", {"package": package})
    fetched = await strategy.run_op(ctx, "cdisc_get_standards_package", {"packageId": "sdtmig-test"})

    assert first["created"] is True
    assert second["created"] is False
    assert fetched["package"]["assets"][0]["artifactId"] == stored["artifact"]["artifactId"]


@pytest.mark.asyncio
async def test_external_validator_failure_is_persisted_as_blocking_finding():
    class BrokenValidator:
        async def validate(self, **kwargs):
            raise RuntimeError("engine unavailable")

    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, require_validation=True)
    ctx.adapters["validation_engine"] = BrokenValidator()
    document = strategy.synthetic.generate({"studyId": "VALIDATOR-1", "subjects": 2})["datasets"]["DM"]
    await strategy.ingest(ctx, {
        "datasetJSON": document, "packageId": "p", "snapshotId": "v1",
        "profile": "sdtm", "standard": {"family": "SDTM"}, "publicationState": "staged",
    })

    result = await strategy.run_op(ctx, "cdisc_validate_snapshot", {"studyId": "VALIDATOR-1", "snapshotId": "v1"})

    assert result["ok"] is False
    assert result["findings"][0]["ruleId"] == "SDR.VALIDATION_ENGINE.FAILURE"
    assert result["snapshotState"] == "quarantined"


@pytest.mark.asyncio
async def test_projection_can_embed_semantic_text_with_configured_dimensions():
    class Embeddings:
        async def embed(self, texts):
            return [[float(len(text)), 1.0, 0.0] for text in texts]

    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)
    ctx.adapters["embedding"] = Embeddings()
    ctx.config["semantic"] = {"enabled": True, "embed_on_rebuild": True, "embedding_dimensions": 3}
    document = strategy.synthetic.generate({"studyId": "EMBED-1", "subjects": 2})["datasets"]["DM"]
    await strategy.ingest(ctx, {
        "datasetJSON": document, "packageId": "p", "snapshotId": "v1",
        "profile": "sdtm", "standard": {"family": "SDTM"}, "publicationState": "staged",
    })

    rebuilt = await strategy.run_op(ctx, "cdisc_rebuild_projections", {"studyId": "EMBED-1", "snapshotId": "v1"})

    records = list(storage.data["cdisc_records"].values())
    assert rebuilt["recordCount"] == 2
    assert all(len(record["semantic"]["vector"]) == 3 for record in records)
    assert all("EMBED-1-" not in record["semantic"]["text"] for record in records)


@pytest.mark.asyncio
async def test_published_snapshot_can_be_superseded_without_deletion():
    storage = MemoryStorage()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage)
    for snapshot_id in ("v1", "v2"):
        generated = strategy.synthetic.generate({"studyId": "SUPER-1", "subjects": 2, "seed": 4})
        for document in generated["datasets"].values():
            await strategy.ingest(ctx, {
                "datasetJSON": document, "packageId": snapshot_id, "snapshotId": snapshot_id,
                "profile": "sdtm", "standard": {"family": "SDTM"}, "publicationState": "staged",
            })
        await strategy.run_op(ctx, "cdisc_publish_snapshot", {"studyId": "SUPER-1", "snapshotId": snapshot_id})

    result = await strategy.run_op(ctx, "cdisc_supersede_snapshot", {
        "studyId": "SUPER-1", "snapshotId": "v1", "replacementSnapshotId": "v2", "reason": "Corrected data",
    })

    assert result["supersededBy"].endswith(":v2")
    assert storage.data["cdisc_snapshots"]["tenant-a:SUPER-1:v1"]["state"] == "superseded"
