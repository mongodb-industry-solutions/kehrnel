"""Versioned domain samples use the normal native persistence/query boundary.

Samples are optional starter data, never an activation prerequisite. HTTP
callers select catalogued identities, not filesystem paths or databases.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone

from kehrnel.engine.domains.dicom.sample_data import (
    DICOMSampleError, describe_sample, load_sample, sample_catalog,
)

from . import playground, rules
from .common import config, centers, digest, fail, storage


def catalog(ctx, _):
    cfg = config(ctx)
    datasets = []
    try:
        for item in sample_catalog()["datasets"]:
            description = describe_sample(item["id"], item["version"], expected_digest=item["digest"])
            datasets.append(description)
    except DICOMSampleError:
        fail("DICOM_SAMPLE_INVALID", "The domain sample catalog cannot be verified.", 503)
    return {"ok": True, "contract": "dicom-sample-runtime-catalog/v1",
            "datasets": datasets, "authorizedCenters": centers(ctx, cfg),
            "importOperation": "dicom_sample_import", "statusOperation": "dicom_sample_status",
            "queryOperation": "dicom_sample_query", "optionalStarterData": True}


def default_profile(dataset_id):
    profile = copy.deepcopy(playground.PROFILES["profiles"][0])
    if dataset_id == "dicom.public-imaging":
        profile["id"] = "public-imaging-core"
        profile["name"] = "Public imaging acquisition and rendering"
        next(f for f in profile["fields"] if f["name"] == "frames")["aggregate"] = "distinct"
        profile["fields"].append({"name": "sliceThickness", "source": "header", "path": ["00180050"],
                                 "type": "number", "aggregate": "distinct", "target": "research",
                                 "maxValues": 32, "unit": "mm"})
    return profile


async def import_sample(ctx, payload):
    dataset_id, version, variant = payload["datasetId"], payload["version"], payload["variant"]
    try:
        info = describe_sample(dataset_id, version, expected_digest=payload.get("digest"))
        source = load_sample(dataset_id, version, variant=variant, expected_digest=info["reference"]["digest"])
    except DICOMSampleError:
        fail("DICOM_SAMPLE_INVALID", "Select a verified domain dataset, version and variant.")
    profile = payload.get("profile") or default_profile(dataset_id)
    profile_version = rules.validate(profile)
    # Preparation checks all centers before any viewer catalog or intake write.
    cfg, _, parents, _, _, _ = rules.prepare(ctx, {**source, "profile": profile})
    from .strategy import DICOMTieredStoreStrategy
    await DICOMTieredStoreStrategy().ingest(ctx, source)
    await playground.apply_indexes(ctx, {"profile": profile})
    received = await playground.receive(ctx, {**source, "profile": profile})
    publication = await playground.process(ctx, {"batchId": received["batchId"]})
    receipt_id = digest([cfg["tenant_id"], info["reference"], variant, profile_version, publication["generationId"]])
    receipt = {"_id": receipt_id, "tenantId": cfg["tenant_id"],
               "centerIds": sorted({p["_kehrnel"]["centerId"] for p in parents.values()}),
               "dataset": info["reference"], "variant": variant, "profile": profile,
               "profileVersion": profile_version, "generationId": publication["generationId"],
               "sourceCounts": info["variants"][variant]["counts"],
               "state": "ready", "publishedAt": datetime.now(timezone.utc)}
    await storage(ctx).update_one(rules.collection_names(cfg)["samples"],
                                 {"_id": receipt_id, "tenantId": cfg["tenant_id"]},
                                 {"$setOnInsert": receipt}, upsert=True)
    return {"ok": True, "contract": "dicom-sample-import-result/v1", "receipt": receipt,
            "sourcePreserved": True, "queryEngine": "MongoDB", "boundedSample": True}


async def status(ctx, payload):
    cfg = config(ctx)
    match = {"tenantId": cfg["tenant_id"], "state": "ready",
             "dataset.id": payload["datasetId"], "dataset.version": payload["version"]}
    rows = await storage(ctx).aggregate(rules.collection_names(cfg)["samples"],
                                       [{"$match": match}, {"$sort": {"_id": 1}}, {"$limit": 26}],
                                       allow_disk_use=False)
    allowed = set(centers(ctx, cfg))
    authorized = [row for row in rows if set(row["centerIds"]) <= allowed]
    return {"ok": True, "contract": "dicom-sample-status/v1", "receipts": authorized[:25],
            "truncated": len(rows) > 25, "ready": bool(authorized), "queryEngine": "MongoDB"}


async def query(ctx, payload):
    cfg = config(ctx)
    receipt_ids = payload["receiptIds"]
    receipts = []
    for receipt_id in receipt_ids:
        receipt = await storage(ctx).find_one(rules.collection_names(cfg)["samples"],
                                             {"_id": receipt_id, "tenantId": cfg["tenant_id"], "state": "ready"})
        if not receipt or not set(receipt["centerIds"]) <= set(centers(ctx, cfg)):
            fail("DICOM_SAMPLE_RECEIPT_NOT_FOUND", "Select an authorized ready sample receipt.", 404)
        receipts.append(receipt)
    # Never merge generations from different datasets or projection meanings.
    if len({digest([r["dataset"], r["profileVersion"]]) for r in receipts}) != 1:
        fail("DICOM_SAMPLE_SCOPE_MISMATCH", "Query one dataset version and one projection profile.")
    request = {k: v for k, v in payload.items() if k != "receiptIds"}
    request["allowPartial"] = True  # This contract explicitly targets bounded starter data.
    if not request.get("text"):
        request.setdefault("where", [{"field": "rows", "op": ">", "value": 0}])
    request.setdefault("limit", 50)
    evidence, by_series = [], {}
    for receipt in receipts:
        result = await playground.execute(ctx, {**request, "profile": receipt["profile"],
                                                "generationId": receipt["generationId"]})
        evidence.append({"generationId": receipt["generationId"], "pipeline": result["pipeline"],
                         "semantics": result["semantics"], "matchedSeriesUids": [r["seriesUid"] for r in result["rows"]]})
        for row in result["rows"]:
            key = (row["_kehrnel"]["centerId"], row["seriesUid"])
            by_series[key] = row
    rows = [by_series[k] for k in sorted(by_series)]
    return {"ok": True, "contract": "dicom-sample-query-result/v1", "dataset": receipts[0]["dataset"],
            "rows": rows[:request["limit"]], "evidence": evidence, "executed": True, "engine": "MongoDB",
            "resultGrain": "series", "boundedSample": True,
            "limited": len(rows) > request["limit"] or any(len(e["matchedSeriesUids"]) >= request["limit"] for e in evidence),
            "seriesCount": min(len(rows), request["limit"])}


async def dispatch(ctx, op, payload):
    if op == "dicom_sample_catalog":
        return catalog(ctx, payload)
    return await {"dicom_sample_import": import_sample, "dicom_sample_status": status,
                  "dicom_sample_query": query}[op](ctx, payload)
