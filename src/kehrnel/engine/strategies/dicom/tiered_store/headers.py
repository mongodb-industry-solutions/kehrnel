from __future__ import annotations

import copy
import hashlib

from bson import BSON
from bson.codec_options import CodecOptions

from .common import artifacts, bounded_bson, config, fail, storage
from .query import scoped


def routing_metadata(document):
    # Mutable shared fields belong to the catalog and selected research
    # materializations, not every live instance locator.
    return {key: copy.deepcopy(value) for key, value in document["_kehrnel"].items()
            if key not in {"patientId", "patientKey", "day", "studyDate", "modality"}}


async def bundle(ctx, cfg, documents):
    """Retain bounded metadata bundles before writing their MongoDB locators."""
    if cfg["headers"]["mode"] == "inline":
        return [{**doc, "_kehrnel": routing_metadata(doc)} for doc in documents]
    maximum = cfg["headers"]["bundle_max_bytes"]
    groups, current = [], []
    for doc in documents:
        raw = {k: copy.deepcopy(v) for k, v in doc.items() if k != "_kehrnel"}
        if len(BSON.encode({"documents": [raw]})) > maximum:
            fail("DICOM_BUNDLE_BOUND_EXCEEDED", "One instance metadata record exceeds the bundle byte bound.")
        if current and len(BSON.encode({"documents": [x[1] for x in current] + [raw]})) > maximum:
            groups.append(current)
            current = []
        current.append((doc, raw))
    if current:
        groups.append(current)
    output = []
    for group in groups:
        content = BSON.encode({"documents": [x[1] for x in group]})
        checksum = hashlib.sha256(content).hexdigest()
        # Stable object references, never expiring SAS URLs. The configured
        # artifact adapter can place these bytes in Azure/S3/filesystem.
        key = "dicom/" + cfg["tenant_id"] + "/metadata/" + checksum + ".bson"
        location = await artifacts(ctx).put(key, content, media_type="application/bson",
                                            metadata={"sha256": checksum})
        if location.key != key or location.size != len(content):
            fail("DICOM_ARTIFACT_WRITE_INVALID", "The artifact adapter returned an inconsistent metadata object.", 502)
        for ordinal, (doc, raw) in enumerate(group):
            locator = {"_id": doc["_id"], "sz": raw.get("sz"), "_kehrnel": routing_metadata(doc)}
            locator["_kehrnel"]["headerRef"] = {"key": key, "sha256": checksum,
                                                  "bytes": len(content), "ordinal": ordinal,
                                                  "format": "dicom-metadata-bundle/v1"}
            bounded_bson(locator)
            output.append(locator)
    return output


async def original(ctx, cfg, doc, cache=None):
    reference = doc["_kehrnel"].get("headerRef")
    if not reference:
        return {k: copy.deepcopy(v) for k, v in doc.items() if k != "_kehrnel"}
    if reference.get("format") != "dicom-metadata-bundle/v1" or reference["bytes"] > cfg["headers"]["bundle_max_bytes"]:
        fail("DICOM_METADATA_REFERENCE_INVALID", "The metadata reference exceeds the configured bound.", 409)
    key = reference["key"]
    cache = cache if cache is not None else {}
    if key not in cache:
        # Keep one bounded bundle, never one bundle per instance in a page.
        cache.clear()
        try:
            content = await artifacts(ctx).get(key)
        except FileNotFoundError:
            fail("DICOM_METADATA_OBJECT_MISSING", "The retained metadata object is unavailable.", 503)
        if len(content) != reference["bytes"] or hashlib.sha256(content).hexdigest() != reference["sha256"]:
            fail("DICOM_METADATA_INTEGRITY_FAILED", "The retained metadata object failed integrity verification.", 502)
        cache[key] = BSON(content).decode(codec_options=CodecOptions(tz_aware=True))
    try:
        raw = cache[key]["documents"][reference["ordinal"]]
    except (IndexError, KeyError, TypeError):
        fail("DICOM_METADATA_REFERENCE_INVALID", "The metadata reference does not identify an instance.", 409)
    if raw.get("_id") != doc["_id"]:
        fail("DICOM_METADATA_INTEGRITY_FAILED", "The retained metadata instance identity differs.", 502)
    return copy.deepcopy(raw)


async def get_header(ctx, payload):
    cfg = config(ctx)
    doc = await storage(ctx).find_one(cfg["collections"]["instances"], {
        **scoped(ctx, cfg), "_id": payload["sopUid"]})
    if not doc:
        return {"ok": True, "status": "not_available", "document": None,
                "reason": "Instance metadata has not been imported into this authorized catalog."}
    raw = await original(ctx, cfg, doc)
    return {"ok": True, "status": "available", "document": raw,
            "integrityVerified": bool(doc["_kehrnel"].get("headerRef"))}


async def get_headers(ctx, payload):
    """Hydrate one bounded series selection without an archive-wide read."""
    from .common import check_cancel
    cfg = config(ctx)
    scope = {**scoped(ctx, cfg), "_kehrnel.seriesUid": payload["seriesUid"]}
    parent = await storage(ctx).find_one(cfg["collections"]["series"], {
        **scoped(ctx, cfg), "_id": payload["seriesUid"]})
    if parent is None:
        fail("DICOM_SERIES_NOT_FOUND", "The series is absent from the authorized catalog.", 404)
    requested = payload["sopUids"]
    # Fetch only routing envelopes together; inline headers are hydrated one
    # at a time, keeping memory independent of the sum of source sizes.
    locators = await storage(ctx).aggregate(cfg["collections"]["instances"], [
        {"$match": {**scope, "_id": {"$in": requested}}},
        {"$limit": len(requested)}, {"$project": {"_id": 1, "_kehrnel": 1}},
    ], allow_disk_use=False)
    by_uid = {row["_id"]: row for row in locators}
    tags = payload.get("tags")
    include_index = payload.get("includeContentIndex", False)
    result = {"ok": True, "seriesUid": payload["seriesUid"], "rows": [
        {"sopUid": sop, "status": "deferred", "reason": "response_byte_bound"}
        if sop in by_uid else {"sopUid": sop, "status": "not_available"}
        for sop in requested],
        "selection": {"tags": tags, "includeContentIndex": include_index, "preservesVR": True},
        "page": {"requested": len(requested), "available": 0,
                 "deferredSopUids": [sop for sop in requested if sop in by_uid]},
        "consistency": "per-record reads; no snapshot guarantee", "responseBytes": 0,
        "byteAccounting": "BSON; JSON transport encoding can be larger"}
    maximum = cfg["headers"]["max_response_bytes"]
    cache = {}
    for index, sop in enumerate(requested):
        if sop not in by_uid:
            continue
        check_cancel(ctx)
        doc = by_uid[sop]
        if not doc["_kehrnel"].get("headerRef"):
            doc = await storage(ctx).find_one(cfg["collections"]["instances"], {**scope, "_id": sop})
        if doc is None:
            result["rows"][index] = {"sopUid": sop, "status": "not_available"}
            result["page"]["deferredSopUids"].remove(sop)
            continue
        raw = await original(ctx, cfg, doc, cache)
        source_header = raw["header"]
        row = {"sopUid": sop, "status": "available",
               "header": {tag: copy.deepcopy(source_header[tag]) for tag in tags if tag in source_header}
                         if tags else copy.deepcopy(source_header),
               "missingTags": [tag for tag in tags if tag not in source_header] if tags else [],
               "integrityVerified": bool(doc["_kehrnel"].get("headerRef"))}
        if include_index:
            row["ContentIndex"] = copy.deepcopy(raw.get("ContentIndex"))
        previous = result["rows"][index]
        result["rows"][index] = row
        # Remaining deferred entries/identities reserve their response budget.
        if len(BSON.encode(result)) > maximum:
            result["rows"][index] = previous
            break
        result["page"]["deferredSopUids"].remove(sop)
        result["page"]["available"] += 1
    result["responseBytes"] = len(BSON.encode(result))
    if result["responseBytes"] > maximum:
        fail("DICOM_HEADER_RESPONSE_BOUND_EXCEEDED", "The header response exceeds its configured bound.")
    return result
