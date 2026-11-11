#!/usr/bin/env python3
"""Aggregate-only exported-sample validation against disposable local MongoDB.

Reads exactly MedObjects/studies.json and instances.json. Never reads customer
configuration, user exports, credential files, a live URI, or binary DICOMs.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import copy
import hashlib
import json
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

from bson import BSON, json_util
from motor.motor_asyncio import AsyncIOMotorClient

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.dicom.tiered_store import headers, model
from kehrnel.engine.strategies.dicom.tiered_store.common import config
from kehrnel.engine.strategies.dicom.tiered_store.strategy import DICOMTieredStoreStrategy
from kehrnel.persistence.artifacts.filesystem import FileSystemArtifactStore
from kehrnel.persistence.mongodb.index_admin import MongoIndexAdminAdapter
from kehrnel.persistence.mongodb.storage import MongoStorageAdapter


def require(condition, message):
    if not condition:
        raise RuntimeError(message)  # Never include source values in failures.


def profile(studies, instances):
    series = {child["si"]: (study, child) for study in studies for child in study["se"]}
    return {
        "studies": len(studies), "series": len(series), "instances": len(instances),
        "centers": len({x["dm"] for x in studies} | {x["ci"] for _, x in series.values()}),
        "modalities": dict(collections.Counter(child["mo"] for _, child in series.values())),
        "series_center_differs_from_study": sum(study["dm"] != child["ci"] for study, child in series.values()),
        "scalar_series_uid_values": sum(isinstance(x["header"]["0020000E"].get("Value"), str) for x in instances),
        "orphan_series": sum(model.scalar(x["header"], "0020000E") not in series for x in instances),
        "distinct_header_tags": len({tag for x in instances for tag in x["header"]}),
        "missing_content_index": sum("ContentIndex" not in x for x in instances),
        "source_instance_logical_bson_bytes": sum(len(BSON.encode(x)) for x in instances),
        "source_study_logical_bson_bytes": sum(len(BSON.encode(x)) for x in studies),
    }


async def collection_sizes(db, names):
    # WiredTiger's file sizes immediately after inserts can still be one page.
    # Force a checkpoint on this explicitly disposable local server.
    await db.client.admin.command("fsync")
    output = {}
    for kind, name in names.items():
        if name not in await db.list_collection_names():
            continue
        stats = await db.command("collStats", name)
        output[kind] = {key: stats.get(key, 0) for key in ("count", "size", "storageSize", "totalIndexSize", "nindexes")}
    return output


def safe_explain(explanation):
    found = collections.defaultdict(list)
    def walk(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key in ("rejectedPlans", "allPlansExecution"):
                    continue
                if key in ("indexName", "stage", "totalDocsExamined", "totalKeysExamined", "nReturned"):
                    if isinstance(child, (str, int)):
                        found[key].append(child)
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    walk(explanation)
    return {"indexes": sorted(set(found["indexName"])),
            "collectionScan": "COLLSCAN" in found["stage"],
            "blockingSort": "SORT" in found["stage"],
            **{key: max(found[key], default=0) for key in ("totalDocsExamined", "totalKeysExamined", "nReturned")}}


async def import_batches(strategy, ctx, field, docs):
    for begin in range(0, len(docs), 100):
        await strategy.ingest(ctx, {field: docs[begin:begin + 100]})


async def query_all(strategy, ctx, request):
    rows, cursor = [], None
    while True:
        plan = await strategy.compile_query(ctx, "dicom", {
            "version": "dicom-query/v1", **request, **({"cursor": cursor} if cursor else {})})
        page = await strategy.execute_query(ctx, plan)
        rows.extend(page.rows)
        cursor = page.explain["page"]["nextCursor"]
        if not cursor:
            return rows


async def validate(args):
    require(urlparse(args.mongo_uri).hostname in {"localhost", "127.0.0.1", "::1"}, "Use a disposable localhost MongoDB.")
    sample = args.samples / "MedObjects"
    sources = {}
    for name in ("studies", "instances"):
        path = sample / (name + ".json")
        content = path.read_bytes()
        sources[name] = json_util.loads(content)
        require(isinstance(sources[name], list), "Samples must be JSON arrays.")
    studies, instances = sources["studies"], sources["instances"]
    summary = profile(studies, instances)
    require(summary["orphan_series"] == 0, "The sample has unresolved instance/series references.")
    cfg_centers = sorted({x["dm"] for x in studies} | {x["ci"] for s in studies for x in s["se"]})
    client = AsyncIOMotorClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    await client.admin.command("ping")
    database_name = "kehrnel_dicom_validation_" + uuid.uuid4().hex
    db = client[database_name]
    report = {"contract": "dicom-validation/v1", "serverVersion": (await client.server_info())["version"],
              "sampleProfile": summary, "containsSourceIdentifiers": False, "scaleBenchmark": False,
              "physicalStatisticsCheckpointed": True, "modes": {}}
    strategy = DICOMTieredStoreStrategy()
    class RecordingStorage(MongoStorageAdapter):
        def __init__(self, database):
            super().__init__(database)
            self.writes = collections.Counter()
        async def replace_one(self, collection, flt, doc, upsert=False):
            self.writes[collection] += 1
            return await super().replace_one(collection, flt, doc, upsert=upsert)
        async def update_one(self, collection, flt, update, upsert=False):
            self.writes[collection] += 1
            return await super().update_one(collection, flt, update, upsert=upsert)
    started = time.perf_counter()
    try:
        await db.baseline_instances.insert_many(copy.deepcopy(instances))
        for keys, name in (
            ([("header.0020000D.Value", 1)], "baseline_study"),
            ([("header.0020000E.Value", 1)], "baseline_series"),
            ([("header.0020000D.Value", 1), ("_id", 1)], "baseline_study_uid"),
            ([("header.0020000E.Value", 1), ("_id", 1)], "baseline_series_uid"),
        ):
            await db.baseline_instances.create_index(keys, name=name)
        report["baselineInstanceCollection"] = (await collection_sizes(db, {"instances": "baseline_instances"}))["instances"]
        by_series = collections.defaultdict(list)
        for raw in instances:
            by_series[model.scalar(raw["header"], "0020000E")].append(raw)
        with tempfile.TemporaryDirectory(prefix="kehrnel-dicom-metadata-") as temporary:
            for mode in ("inline", "bundle"):
                names = {x: mode + "_" + x for x in ("studies", "series", "instances", "research")}
                ctx = StrategyContext(environment_id="sample-validation",
                    config={"database": database_name, "tenant_id": "sample-validation",
                            "allowed_centers": cfg_centers, "collections": names, "headers": {"mode": mode}},
                    adapters={"storage": RecordingStorage(db), "index_admin": MongoIndexAdminAdapter(db),
                              "artifact": FileSystemArtifactStore(Path(temporary) / mode)})
                await strategy.apply(ctx, await strategy.plan(ctx))
                await import_batches(strategy, ctx, "studies", studies)
                ctx.adapters["storage"].writes.clear()
                missing = await strategy.run_op(ctx, "dicom_get_header", {"sopUid": instances[0]["_id"]})
                require(missing["status"] == "not_available", "Progressive import did not report missing metadata.")
                await import_batches(strategy, ctx, "instances", instances)
                catalog_writes = sum(ctx.adapters["storage"].writes[names[x]] for x in ("studies", "series"))
                require(catalog_writes == 0, "Instance-only ingestion rewrote a catalog document.")
                cache, roundtrips = {}, 0
                stored = {x["_id"]: x async for x in db[names["instances"]].find({})}
                for raw in instances:
                    replay = await headers.original(ctx, config(ctx), stored[raw["_id"]], cache)
                    require(BSON.encode(replay) == BSON.encode(raw), "A metadata roundtrip differs from its source.")
                    roundtrips += 1
                checked = 0
                for series_uid, expected in by_series.items():
                    rows = await query_all(strategy, ctx, {"resource": "instances", "filter": {"seriesUid": series_uid}, "limit": 100})
                    expected_ids = [x["_id"] for x in sorted(expected, key=lambda x: (int(str(model.scalar(x["header"], "00200013")).strip()), x["_id"]))]
                    require([x["_id"] for x in rows] == expected_ids, "Series browse differs from the independent source oracle.")
                    checked += 1
                all_studies = await query_all(strategy, ctx, {"resource": "studies", "limit": 100})
                require({x["_id"] for x in all_studies} == {x["_id"] for x in studies}, "Study browsing differs from its source.")
                first = studies[0]
                source_day = model.instant(first["dt"]).date().isoformat()
                patient_request = {"resource": "studies", "centers": [first["dm"]],
                                   "filter": {"patientId": first["pi"], "day": source_day}, "limit": 100}
                patient_rows = await query_all(strategy, ctx, patient_request)
                expected_patient = {x["_id"] for x in studies if x["dm"] == first["dm"] and x["pi"] == first["pi"]
                                    and model.instant(x["dt"]).date().isoformat() == source_day}
                require({x["_id"] for x in patient_rows} == expected_patient, "Patient/day browsing differs from its source.")
                modality_request = {"resource": "studies", "filter": {"modality": "MG"}, "limit": 100}
                modality_rows = await query_all(strategy, ctx, modality_request)
                require({x["_id"] for x in modality_rows} == {x["_id"] for x in studies if any(y["mo"] == "MG" for y in x["se"])},
                        "Modality browsing differs from its source.")
                series_request = {"resource": "series", "filter": {"studyUid": first["_id"]}, "limit": 100}
                series_rows = await query_all(strategy, ctx, series_request)
                require({x["_id"] for x in series_rows} == {x["si"] for x in first["se"]}, "Study/series browsing differs from its source.")
                # Real plan evidence; remove all patient/UID/filter values.
                largest = max(by_series, key=lambda key: len(by_series[key]))
                plan = await strategy.compile_query(ctx, "dicom", {"version": "dicom-query/v1", "resource": "instances",
                    "filter": {"seriesUid": largest}, "limit": 100})
                explanation = await db.command({"explain": {"aggregate": names["instances"], "pipeline": plan.plan["pipeline"], "cursor": {}},
                                               "verbosity": "executionStats"})
                evidence = safe_explain(explanation)
                require(not evidence["collectionScan"] and "idx_dicom_instance_series_order" in evidence["indexes"],
                        "The viewer query did not use its intended regular index.")
                result = {"metadataRoundtripsVerified": roundtrips, "seriesBrowseQueriesVerified": checked,
                          "studyBrowseVerified": True, "patientDayBrowseVerified": True,
                          "modalityBrowseVerified": True, "studySeriesBrowseVerified": True, "progressiveImportVerified": True,
                          "collectionsBeforeResearch": await collection_sizes(db, names),
                          "viewerPlan": evidence}
                result["instanceIngestCatalogWrites"] = catalog_writes
                summary_result = await strategy.run_op(ctx, "dicom_refresh_series_summary", {"seriesUid": largest})
                require(summary_result["summary"]["importedInstanceDocuments"] == len(by_series[largest]),
                        "The asynchronous metadata count differs from imported source documents.")
                result["asynchronousSeriesSummaryVerified"] = True
                described = next((child for s in studies for child in s["se"] if isinstance(child.get("de"), str) and len(child["de"].strip()) >= 3), None)
                if described:
                    description_rows = await query_all(strategy, ctx, {"resource": "series",
                        "filter": {"description": described["de"], "descriptionMode": "exact"}, "limit": 100})
                    expected_description = {child["si"] for s in studies for child in s["se"]
                        if " ".join(child.get("de", "").casefold().split()) == " ".join(described["de"].casefold().split())}
                    require({x["_id"] for x in description_rows} == expected_description,
                            "Description browsing differs from the source description oracle.")
                    result["descriptionBrowseVerified"] = True
                result["catalogPlans"] = {}
                for name, request in (("patientDay", patient_request), ("modality", modality_request), ("studySeries", series_request)):
                    compiled = await strategy.compile_query(ctx, "dicom", {"version": "dicom-query/v1", **request})
                    explained = await db.command({"explain": {"aggregate": compiled.plan["collection"], "pipeline": compiled.plan["pipeline"], "cursor": {}},
                                                  "verbosity": "executionStats"})
                    result["catalogPlans"][name] = safe_explain(explained)
                    require(not result["catalogPlans"][name]["collectionScan"], "A catalog query did not use a regular index.")
                ctx.config["projections"] = {"enabled": True}
                await strategy.apply(ctx, await strategy.plan(ctx))
                # The workbench can activate fields after migration, then resume
                # bounded series pages. No corpus publication is implied.
                for series_uid in by_series:
                    cursor = None
                    while True:
                        response = await strategy.run_op(ctx, "dicom_rebuild_projections", {
                            "seriesUid": series_uid, "limit": 100, **({"cursor": cursor} if cursor else {})})
                        cursor = response["page"]["nextCursor"]
                        if not cursor:
                            break
                projected = await query_all(strategy, ctx, {"resource": "research", "allowPartial": True, "limit": 100})
                result["selectiveResearchDocuments"] = len(projected)
                result["researchCoverage"] = "partial"
                result["collectionsAfterResearch"] = await collection_sizes(db, names)
                result["retainedBundleObjects"] = len(list((Path(temporary) / mode).rglob("*.bson")))
                result["retainedBundleBytes"] = sum(p.stat().st_size for p in (Path(temporary) / mode).rglob("*.bson"))
                report["modes"][mode] = result
        report["elapsedSecondsDiagnosticOnly"] = round(time.perf_counter() - started, 3)
        report["limitations"] = [
            "Mostly synthetic OT; no CT/MR sizing conclusion.",
            "Logical BSON bytes and local index/storage sizes are not Atlas prices.",
            "Local unsharded correctness validation is not a 15-billion-instance benchmark.",
            "Azure transfers, Search builds, archive scans, pixel rendering, and condition cohorts were not exercised.",
        ]
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    finally:
        await client.drop_database(database_name)
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--mongo-uri", required=True)
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(validate(parser.parse_args()))


if __name__ == "__main__":
    main()
