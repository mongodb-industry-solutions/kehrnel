"""Opt-in end-to-end check against unmodified official CDISC Pilot files.

Fetch with ``python scripts/fetch_cdisc_pilot.py /tmp/kehrnel-cdisc-pilot`` and
set ``CDISC_PILOT_FIXTURE_DIR`` to that directory.
"""

import base64
import json
import os
from pathlib import Path

import pytest

from kehrnel.engine.strategies.cdisc.sdr.strategy import CDISCSDRStrategy
from tests.contract.cdisc.test_cdisc_sdr_strategy import MemoryArtifactStore, MemoryStorage, _context


@pytest.mark.asyncio
async def test_official_pilot_dm_end_to_end():
    fixture_dir = os.getenv("CDISC_PILOT_FIXTURE_DIR")
    if not fixture_dir:
        pytest.skip("set CDISC_PILOT_FIXTURE_DIR after running scripts/fetch_cdisc_pilot.py")
    root = Path(fixture_dir)
    storage, artifacts = MemoryStorage(), MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifacts, tenant_id="official-pilot", require_validation=True)

    artifact_ids = {}
    for name, media_type in (("dm.xpt", "application/x-sas-xport"), ("define.xml", "application/xml")):
        stored = await strategy.run_op(ctx, "cdisc_store_artifact", {
            "contentBase64": base64.b64encode((root / name).read_bytes()).decode("ascii"),
            "mediaType": media_type,
            "sourceName": name,
        })
        artifact_ids[name] = stored["artifact"]["artifactId"]
    ingested = await strategy.run_op(ctx, "cdisc_ingest_xpt", {
        "xptArtifactId": artifact_ids["dm.xpt"], "defineArtifactId": artifact_ids["define.xml"],
        "packageId": "official-cdisc-pilot", "snapshotId": "master", "profile": "sdtm",
        "standard": {"family": "SDTM", "implementationGuide": "SDTMIG", "implementationGuideVersion": "3.1.2"},
    })
    validated = await strategy.run_op(ctx, "cdisc_validate_snapshot", {"studyId": "CDISCPILOT01", "snapshotId": "master"})
    published = await strategy.run_op(ctx, "cdisc_publish_snapshot", {"studyId": "CDISCPILOT01", "snapshotId": "master"})
    plan = await strategy.compile_query(ctx, "cdisc", {
        "scope": {"studies": ["CDISCPILOT01"], "snapshots": "published"},
        "from": {"profile": "sdtm", "domains": ["DM"]},
        "where": {"and": [{"path": "data.SEX", "op": "eq", "value": "F"}]},
        "select": ["data.USUBJID", "data.SEX"],
    })
    queried = await strategy.execute_query(ctx, plan)
    exported = await strategy.run_op(ctx, "cdisc_export_dataset_json", {"datasetId": ingested["datasetId"]})
    exported_xpt = await strategy.run_op(ctx, "cdisc_export_xpt", {"datasetId": ingested["datasetId"], "version": 5})
    lineage = await strategy.run_op(
        ctx,
        "cdisc_get_lineage",
        {"studyId": "CDISCPILOT01", "snapshotId": "master"},
    )

    assert ingested["recordCount"] == 306
    assert validated["ok"] is True
    assert published["state"] == "published"
    assert queried.rows
    assert all(row["data.SEX"] == "F" for row in queried.rows)
    assert exported["equivalence"]["equivalent"] is True
    assert exported_xpt["equivalence"]["equivalent"] is True
    assert exported_xpt["recordCount"] == 306
    assert any(item["operation"] == "cdisc_ingest_xpt" for item in lineage["transformations"])


@pytest.mark.asyncio
async def test_official_pilot_dataset_json_end_to_end():
    fixture_dir = os.getenv("CDISC_PILOT_FIXTURE_DIR")
    if not fixture_dir:
        pytest.skip("set CDISC_PILOT_FIXTURE_DIR after running scripts/fetch_cdisc_pilot.py")
    root = Path(fixture_dir)
    document = json.loads((root / "dm.json").read_text(encoding="utf-8"))
    storage, artifacts = MemoryStorage(), MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifacts, tenant_id="official-pilot-json", require_validation=True)

    stored = await strategy.run_op(ctx, "cdisc_store_artifact", {
        "contentBase64": base64.b64encode((root / "dm.json").read_bytes()).decode("ascii"),
        "mediaType": "application/json",
        "sourceName": "dm.json",
        "kind": "dataset-json",
    })
    inspected = await strategy.run_op(
        ctx, "cdisc_inspect_dataset_json", {"datasetJSON": document}
    )
    ingested = await strategy.run_op(ctx, "cdisc_ingest_dataset_json", {
        "datasetJSON": document,
        "packageId": "official-cdisc-pilot-json",
        "snapshotId": "master-json",
        "standardsPackageId": "sdtmig-3.1.2",
        "profile": "sdtm",
        "standard": {
            "family": "SDTM",
            "implementationGuide": "SDTMIG",
            "implementationGuideVersion": "3.1.2",
            "exchangeStandard": "Dataset-JSON",
            "exchangeVersion": "1.1.0",
        },
        "sourceArtifactId": stored["artifact"]["artifactId"],
        "publicationState": "staged",
    })
    validated = await strategy.run_op(
        ctx,
        "cdisc_validate_snapshot",
        {"studyId": "CDISCPILOT01", "snapshotId": "master-json"},
    )
    published = await strategy.run_op(
        ctx,
        "cdisc_publish_snapshot",
        {"studyId": "CDISCPILOT01", "snapshotId": "master-json"},
    )
    summary = await strategy.run_op(
        ctx,
        "cdisc_snapshot_summary",
        {"studyId": "CDISCPILOT01", "snapshotId": "master-json"},
    )

    assert inspected["records"] == 306
    assert ingested["recordCount"] == 306
    assert validated["ok"] is True
    assert published["state"] == "published"
    assert summary["summary"]["recordCount"] == 306
    assert summary["summary"]["domains"] == ["DM"]


@pytest.mark.asyncio
async def test_public_phuse_send_example_end_to_end():
    fixture_dir = os.getenv("CDISC_SEND_FIXTURE_DIR")
    if not fixture_dir:
        pytest.skip("set CDISC_SEND_FIXTURE_DIR after fetching the phuse-ffu-send example")
    root = Path(fixture_dir)
    storage, artifacts = MemoryStorage(), MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifacts, tenant_id="public-send", require_validation=True)

    define = await strategy.run_op(ctx, "cdisc_store_artifact", {
        "contentBase64": base64.b64encode((root / "define.xml").read_bytes()).decode("ascii"),
        "mediaType": "application/xml",
        "sourceName": "define.xml",
        "kind": "define-xml",
    })
    ingested = []
    for domain in ("dm", "tx", "mi", "lb"):
        stored = await strategy.run_op(ctx, "cdisc_store_artifact", {
            "contentBase64": base64.b64encode((root / f"{domain}.xpt").read_bytes()).decode("ascii"),
            "mediaType": "application/x-sas-xport",
            "sourceName": f"{domain}.xpt",
            "kind": "source",
        })
        ingested.append(await strategy.run_op(ctx, "cdisc_ingest_xpt", {
            "xptArtifactId": stored["artifact"]["artifactId"],
            "defineArtifactId": define["artifact"]["artifactId"],
            "studyOID": "Study ID",
            "packageId": "phuse-ffu-send",
            "snapshotId": "public-example",
            "profile": "send",
            "standard": {
                "family": "SEND",
                "implementationGuide": "SENDIG",
                "implementationGuideVersion": "3.0",
            },
        }))

    validated = await strategy.run_op(
        ctx, "cdisc_validate_snapshot", {"studyId": "Study ID", "snapshotId": "public-example"}
    )
    published = await strategy.run_op(
        ctx, "cdisc_publish_snapshot", {"studyId": "Study ID", "snapshotId": "public-example"}
    )
    summary = await strategy.run_op(
        ctx, "cdisc_snapshot_summary", {"studyId": "Study ID", "snapshotId": "public-example"}
    )
    plan = await strategy.compile_query(ctx, "cdisc", {
        "scope": {"studies": ["Study ID"], "snapshots": "published"},
        "from": {"profile": "send", "domains": ["MI"]},
        "where": {"and": []},
        "select": ["data.USUBJID", "data.MISPEC", "data.MISTRESC"],
        "page": {"limit": 20},
    })
    queried = await strategy.execute_query(ctx, plan)

    assert {item["sourceFormat"] for item in ingested} == {"XPT"}
    assert validated["ok"] is True
    assert published["state"] == "published"
    assert summary["summary"]["domains"] == ["DM", "LB", "MI", "TX"]
    assert summary["summary"]["recordCount"] == 2319
    assert queried.rows


@pytest.mark.asyncio
async def test_curated_example_operation_is_one_step_and_checksum_governed():
    fixture_dir = os.getenv("CDISC_PILOT_FIXTURE_DIR")
    if not fixture_dir:
        pytest.skip("set CDISC_PILOT_FIXTURE_DIR after fetching the clinical example")
    root = Path(fixture_dir)

    class LocalExampleFetcher:
        async def fetch(self, url):
            return (root / url.rsplit("/", 1)[-1]).read_bytes()

    storage, artifacts = MemoryStorage(), MemoryArtifactStore()
    strategy = CDISCSDRStrategy()
    ctx = _context(storage, artifacts, tenant_id="curated-example", require_validation=True)
    ctx.adapters["example_fetcher"] = LocalExampleFetcher()

    catalog = await strategy.run_op(ctx, "cdisc_list_examples", {"profile": "sdtm"})
    result = await strategy.run_op(ctx, "cdisc_ingest_example", {
        "exampleId": "cdisc-pilot-sdtm-dm",
        "snapshotId": "guided-example",
        "acknowledgeTerms": True,
        "validate": True,
        "publish": True,
    })

    assert [item["id"] for item in catalog["items"]] == ["cdisc-pilot-sdtm-dm"]
    assert result["ok"] is True
    assert result["publication"]["state"] == "published"
    assert result["ingested"][0]["recordCount"] == 306
    assert all(item["modelSchemaVersion"] == "1.0.0" for item in result["artifacts"])
