from __future__ import annotations

import copy
import re
import unicodedata
from datetime import datetime, timezone

from dateutil.parser import isoparse

from .common import bounded_bson, centers, digest, fail, version

NUMERIC_VRS = {"DS", "IS", "US", "SS", "UL", "SL", "UV", "SV", "FL", "FD"}
UID_RE = re.compile(r"[0-9]+(?:\.[0-9]+)+")


def description(value):
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split()) if isinstance(value, str) else ""


def uid(value):
    if not isinstance(value, str) or len(value) > 64 or not UID_RE.fullmatch(value):
        fail("DICOM_UID_INVALID", "A valid DICOM UID is required.")
    return value


def values(header, tag):
    node = header.get(tag, {})
    if not isinstance(node, dict):
        fail("DICOM_ATTRIBUTE_INVALID", "A DICOM attribute must contain a VR and values.")
    result = node.get("Value", [])
    # Customer export compatibility: preserve the original scalar representation.
    return result if isinstance(result, list) else [result]


def scalar(header, tag):
    result = values(header, tag)
    return result[0] if result else None


def source(document):
    if not isinstance(document, dict) or "_kehrnel" in document:
        fail("DICOM_SOURCE_INVALID", "Source documents must be objects without the reserved _kehrnel namespace.")
    bounded_bson(document)
    return copy.deepcopy(document)


def instant(value):
    if isinstance(value, dict) and "$date" in value:
        value = value["$date"]
        if isinstance(value, dict) and "$numberLong" in value:
            value = datetime.fromtimestamp(int(value["$numberLong"]) / 1000, timezone.utc)
    if isinstance(value, str):
        value = isoparse(value)
    if not isinstance(value, datetime):
        fail("DICOM_DATE_INVALID", "Study dt must be a valid source date/time.")
    # Legacy SQL/Extended JSON dt is interpreted as UTC; raw DICOM DA/TM remains
    # untouched. This is not an inferred acquisition timezone.
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def envelope(cfg, raw, center):
    return {"tenantId": cfg["tenant_id"], "centerId": center,
            "storage_schema_version": "1.0.0",
            "projection_contract_version": version(cfg),
            "source_revision": digest(raw)}


def study(ctx, cfg, document):
    raw = source(document)
    study_uid = uid(raw.get("_id"))
    center = raw.get("dm")
    if center not in centers(ctx, cfg):
        fail("DICOM_CENTER_FORBIDDEN", "The study center is outside the authorized scope.", 403)
    if not isinstance(raw.get("pi"), str) or not raw["pi"]:
        fail("DICOM_PATIENT_SCOPE_INVALID", "Study pi is required; patient identity is center-scoped.")
    date = instant(raw.get("dt"))
    children = raw.get("se", [])
    if not isinstance(children, list) or len(children) > cfg["ingest"]["max_series_per_study"]:
        fail("DICOM_SERIES_BOUND_EXCEEDED", "Embedded series exceed the study catalog bound; split the source catalog first.")
    series_docs, seen = [], set()
    common = {"studyUid": study_uid, "patientId": raw["pi"],
              "studyDate": date, "day": date.date().isoformat()}
    for child in children:
        if not isinstance(child, dict):
            fail("DICOM_SOURCE_INVALID", "Study se must contain series objects.")
        series_uid = uid(child.get("si"))
        if series_uid in seen:
            fail("DICOM_SERIES_DUPLICATE", "A study cannot contain duplicate series UIDs.")
        seen.add(series_uid)
        child_center = child.get("ci")
        if child_center not in centers(ctx, cfg):
            fail("DICOM_CENTER_FORBIDDEN", "The series center is absent or outside the authorized scope.", 403)
        modality = child.get("mo")
        if modality is not None and not isinstance(modality, str):
            fail("DICOM_MODALITY_INVALID", "Series mo must be a source modality string or absent.")
        metadata = {**envelope(cfg, child, child_center), **common,
                    "seriesUid": series_uid, "modality": modality or None,
                    "sourceReportedCount": child.get("ni"),
                    "description": description(child.get("de")),
                    "patientKey": digest([cfg["tenant_id"], child_center, raw["pi"]]),
                    "sourceStudyRevision": digest(raw)}
        series_doc = {"_id": series_uid, "source": copy.deepcopy(child), "_kehrnel": metadata}
        bounded_bson(series_doc)
        series_docs.append(series_doc)
    metadata = {**envelope(cfg, raw, center), **common,
                "seriesCount": len(series_docs),
                "description": description(raw.get("de")),
                "patientKey": digest([cfg["tenant_id"], center, raw["pi"]]),
                "modalities": sorted({x["_kehrnel"]["modality"] for x in series_docs if x["_kehrnel"]["modality"]}),
                "seriesFacets": [{"centerId": x["_kehrnel"]["centerId"], "modality": x["_kehrnel"]["modality"],
                                  "description": x["_kehrnel"]["description"]}
                                 for x in series_docs]}
    raw["_kehrnel"] = metadata
    bounded_bson(raw)
    return raw, series_docs


def instance(ctx, cfg, document, series_doc):
    raw = source(document)
    header = raw.get("header")
    if not isinstance(header, dict):
        fail("DICOM_HEADER_REQUIRED", "An instance source must contain a DICOM JSON header.")
    sop_uid = uid(raw.get("_id"))
    if uid(scalar(header, "00080018")) != sop_uid:
        fail("DICOM_HIERARCHY_CONFLICT", "The SOP UID differs from the source document identity.")
    series_uid = uid(scalar(header, "0020000E"))
    study_uid = uid(scalar(header, "0020000D"))
    if not series_doc or series_doc["_id"] != series_uid:
        fail("DICOM_SERIES_NOT_FOUND", "Import the authoritative study/series catalog before its instances.", 409)
    owner = series_doc["_kehrnel"]
    if owner["studyUid"] != study_uid or owner["tenantId"] != cfg["tenant_id"]:
        fail("DICOM_HIERARCHY_CONFLICT", "The instance hierarchy conflicts with the authoritative catalog.", 409)
    if owner["centerId"] not in centers(ctx, cfg):
        fail("DICOM_CENTER_FORBIDDEN", "The instance series is outside the authorized scope.", 403)
    number = scalar(header, "00200013")
    valid_number = isinstance(number, (str, int)) and not isinstance(number, bool) and re.fullmatch(r"[+-]?[0-9]+", str(number).strip())
    number = int(str(number).strip()) if valid_number else None
    if number is not None and not -(2**31) <= number < 2**31:
        number = None
    metadata = {**envelope(cfg, raw, owner["centerId"]),
                **{k: owner[k] for k in ("studyUid", "seriesUid", "patientId", "patientKey", "day", "studyDate", "modality")},
                "sopUid": sop_uid, "instanceNumber": number,
                "orderMissing": 1 if number is None else 0,
                "sourceSeriesRevision": owner["source_revision"],
                "normalizationWarnings": (["scalar_uid_value"] if any(not isinstance(header[t].get("Value", []), list) for t in ("0020000D", "0020000E", "00080018")) else [])}
    raw["_kehrnel"] = metadata
    bounded_bson(raw)
    return raw


def project(cfg, document, raw, series_doc=None):
    policy = cfg["projections"]
    metadata = copy.deepcopy(document["_kehrnel"])
    if series_doc:
        parent = series_doc["_kehrnel"]
        if (metadata["studyUid"], metadata["seriesUid"], metadata["centerId"]) != (parent["studyUid"], parent["seriesUid"], parent["centerId"]):
            fail("DICOM_HIERARCHY_CONFLICT", "Projection source and series catalog ownership differ.", 409)
        metadata.update({key: parent[key] for key in ("patientId", "patientKey", "day", "studyDate", "modality")})
        metadata["sourceSeriesRevision"] = parent["source_revision"]
        metadata["sourceStudyRevision"] = parent["sourceStudyRevision"]
    if not policy["enabled"] or (policy["modalities"] and metadata["modality"] not in policy["modalities"]):
        return None
    attrs = []
    header = raw.get("header", {})
    for rule in policy["tags"]:
        node = header.get(rule["tag"])
        if not node:
            continue
        if node.get("vr") == "SQ":
            fail("DICOM_PROJECTION_UNSUPPORTED", "Sequence projections require a future occurrence-aware contract.")
        for value in values(header, rule["tag"]):
            if rule["type"] == "number":
                if node.get("vr") not in NUMERIC_VRS:
                    continue
                try:
                    import math
                    number = float(value)
                    if not math.isfinite(number):
                        continue
                except (ValueError, TypeError):
                    continue
                attrs.append({"p": rule["tag"], "n": number})
            elif isinstance(value, str):
                attrs.append({"p": rule["tag"], "s": value})
    if len(attrs) > policy["max_values_per_instance"]:
        fail("DICOM_PROJECTION_BOUND_EXCEEDED", "Promoted values exceed the configured projection bound.")
    if not attrs:
        return None
    result = {"_id": document["_id"], "_kehrnel": copy.deepcopy(metadata),
              "attrs": attrs, "projectionVersion": version(cfg)}
    result["_kehrnel"].pop("headerRef", None)
    result["_kehrnel"]["projection_contract_version"] = version(cfg)
    bounded_bson(result)
    return result
