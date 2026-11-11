from __future__ import annotations

import base64
import copy
import json
import math
import re
from datetime import date
from functools import lru_cache

import jsonschema

from kehrnel.engine.core.types import QueryPlan, QueryResult

from .common import ROOT, STRATEGY_VERSION, centers, config, digest, fail, storage, version
from .model import description

QUERY_SCHEMA = json.loads((ROOT / "query.schema.json").read_text())


@lru_cache(maxsize=1)
def package_digest():
    from kehrnel.engine.core.manifest_digest import compute_package_digest
    return compute_package_digest(ROOT)


def scoped(ctx, cfg, requested=None):
    allowed = centers(ctx, cfg)
    if requested is not None:
        if not set(requested) <= set(allowed):
            fail("DICOM_CENTER_FORBIDDEN", "Requested centers are outside the authorized scope.", 403)
        allowed = sorted(set(requested))
    return {"_kehrnel.tenantId": cfg["tenant_id"], "_kehrnel.centerId": {"$in": allowed}}


def compile_query(ctx, query):
    cfg = config(ctx)
    if "condition" in (query.get("filter") or {}):
        fail("DICOM_CONDITION_EVIDENCE_REQUIRED", "Condition cohorts require separately linked clinical evidence; these headers contain no diagnosis.")
    try:
        jsonschema.Draft7Validator(QUERY_SCHEMA).validate(query)
    except jsonschema.ValidationError:
        fail("DICOM_QUERY_INVALID", "The query does not satisfy dicom-query/v1.")
    resource, filters = query["resource"], query.get("filter", {})
    match = scoped(ctx, cfg, query.get("centers"))
    if resource == "instances" and not filters.get("seriesUid"):
        fail("DICOM_SERIES_SCOPE_REQUIRED", "Instance browsing requires one series UID.")
    if resource == "instances" and set(filters) - {"studyUid", "seriesUid"}:
        fail("DICOM_CATALOG_FILTER_REQUIRED", "Select patients, dates and modalities in the study/series catalog before browsing a series' instances.")
    if resource == "research":
        if not cfg["projections"]["enabled"]:
            fail("DICOM_PROJECTION_DISABLED", "Enable a selective research projection before querying it.", 409)
        if not query.get("allowPartial", False):
            fail("DICOM_PROJECTION_COVERAGE_UNKNOWN", "Preview research projections have partial coverage. Explicitly allow partial results.", 409)
        match["projectionVersion"] = version(cfg)
    for field in ("patientId", "studyUid", "seriesUid", "modality"):
        if field in filters:
            if resource == "studies" and field == "seriesUid":
                fail("DICOM_QUERY_INVALID", "Use the series catalog for a series UID filter.")
            if resource == "studies" and field == "modality":
                match["_kehrnel.seriesFacets"] = {"$elemMatch": {
                    "centerId": match["_kehrnel.centerId"], "modality": filters[field]}}
            else:
                match["_kehrnel." + field] = filters[field]
    for field in ("day", "fromDay", "toDay"):
        if field in filters:
            try:
                date.fromisoformat(filters[field])
            except ValueError:
                fail("DICOM_DATE_INVALID", "Day filters must contain valid ISO calendar dates.")
    if "day" in filters and ("fromDay" in filters or "toDay" in filters):
        fail("DICOM_QUERY_INVALID", "Use day or a half-open fromDay/toDay range.")
    if filters.get("fromDay") and filters.get("toDay") and filters["fromDay"] >= filters["toDay"]:
        fail("DICOM_QUERY_INVALID", "toDay must be later than fromDay.")
    if "day" in filters:
        match["_kehrnel.day"] = filters["day"]
    elif "fromDay" in filters or "toDay" in filters:
        match["_kehrnel.day"] = {op: filters[key] for key, op in (("fromDay", "$gte"), ("toDay", "$lt")) if key in filters}
    if filters.get("descriptionMode") and not (filters.get("description") or filters.get("seriesDescription")):
        fail("DICOM_QUERY_INVALID", "descriptionMode requires a description filter.")
    for field in ("description", "seriesDescription"):
        if field not in filters:
            continue
        if resource not in {"studies", "series"} or (field == "seriesDescription" and resource != "studies"):
            fail("DICOM_CATALOG_FILTER_REQUIRED", "Description filters target study/series catalogs.")
        mode = filters.get("descriptionMode", "exact")
        text = description(filters[field])
        if len(text) < 3:
            fail("DICOM_QUERY_INVALID", "Description filters require at least three characters.")
        scoped_day = "day" in filters or ("fromDay" in filters and "toDay" in filters
            and (date.fromisoformat(filters["toDay"]) - date.fromisoformat(filters["fromDay"])).days == 1)
        if (mode != "exact" or field == "seriesDescription") and not scoped_day:
            fail("DICOM_DESCRIPTION_DATE_SCOPE_REQUIRED", "Prefix and series-description filters require one day in this preview.")
        target = "_kehrnel.description"
        element = None
        if field == "seriesDescription":
            element = match.setdefault("_kehrnel.seriesFacets", {"$elemMatch": {
                "centerId": match["_kehrnel.centerId"]}})["$elemMatch"]
            target = "description"
        owner = element if element is not None else match
        if mode == "exact":
            owner[target] = text
        else:
            owner[target] = {"$regex": "^" + re.escape(text)}
    predicates = filters.get("tags", [filters["tag"]] if "tag" in filters else [])
    elements = []
    paths = set()
    for tag in predicates:
        if resource != "research":
            fail("DICOM_TAG_QUERY_REQUIRES_PROJECTION", "Instance tag filtering requires a selective research projection.")
        if tag["path"] in paths:
            fail("DICOM_TAG_PREDICATE_DUPLICATE", "Combine a tag's numeric bounds in one predicate; each tag may occur once.")
        paths.add(tag["path"])
        elements.append(tag_element(cfg, tag))
    if elements:
        # Every predicate belongs to one SOP row. Within each predicate the
        # tag and all value bounds must match the same attribute occurrence.
        match["attrs"] = ({"$elemMatch": elements[0]} if len(elements) == 1
                          else {"$all": [{"$elemMatch": element} for element in elements]})
    sort = ({"_kehrnel.orderMissing": 1, "_kehrnel.instanceNumber": 1, "_id": 1}
            if resource == "instances" else {"_kehrnel.day": 1, "_id": 1})
    keys = list(sort)
    clean = {k: copy.deepcopy(v) for k, v in query.items() if k != "cursor"}
    if filters.get("description") or filters.get("seriesDescription"):
        # Bind continuation tokens to resolved semantics, including the default.
        clean["filter"]["descriptionMode"] = filters.get("descriptionMode", "exact")
    binding = digest({"query": clean, "scope": scoped(ctx, cfg, query.get("centers")), "projection": version(cfg)})
    if query.get("cursor"):
        try:
            decoded = json.loads(base64.b64decode(query["cursor"], altchars=b"-_", validate=True))
            last = decoded["last"]
            if set(decoded) != {"binding", "last"} or decoded["binding"] != binding or len(last) != len(keys):
                raise ValueError()
            if resource == "instances":
                if type(last[0]) is not int or last[0] not in (0, 1) or (last[1] is not None and type(last[1]) is not int) or not isinstance(last[2], str):
                    raise ValueError()
            elif not all(isinstance(x, str) for x in last):
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            fail("DICOM_CURSOR_INVALID", "The continuation cursor does not belong to this query and scope.")
        branches = []
        for index, key in enumerate(keys):
            if last[index] is not None:
                branches.append({**{keys[j]: last[j] for j in range(index)}, key: {"$gt": last[index]}})
        match = {"$and": [match, {"$or": branches}]}
    size = query.get("limit", cfg["query"]["default_limit"])
    if size > cfg["query"]["max_limit"]:
        fail("DICOM_QUERY_LIMIT_EXCEEDED", "The requested page exceeds the configured query bound.")
    pipeline = [{"$match": match}, {"$sort": sort}, {"$limit": size + 1}]
    # Avoid sending raw headers, patient names, full frame indexes, or mixed
    # center embedded series through a discovery/list query.
    projected = {"_id": 1, "_kehrnel": 1, "attrs": 1, "projectionVersion": 1, "sz": 1}
    if resource == "studies":
        projected = {"_id": 1, "pn": 1, "pb": 1, "ps": 1, "an": 1, "de": 1, "dt": 1,
                     **{"_kehrnel." + key: 1 for key in (
                         "tenantId", "centerId", "patientId", "patientKey", "studyUid", "day", "studyDate",
                         "source_revision", "storage_schema_version", "projection_contract_version", "seriesCount")}}
    pipeline.append({"$project": projected})
    return QueryPlan(engine="mongo_pipeline", plan={
        "collection": cfg["collections"][resource], "pipeline": pipeline,
        "query": copy.deepcopy(query), "limit": size, "sortKeys": keys, "binding": binding,
    }, explain={"contract": "dicom-query/v1", "resource": resource,
                "engine": "mongo_pipeline", "domain": "dicom",
                "strategy_id": "dicom.tiered_store", "strategy_version": STRATEGY_VERSION,
                "activation_id": (ctx.meta or {}).get("activation_id"),
                "config_hash": digest(cfg),
                "manifest_digest": ctx.manifest.package_digest if ctx.manifest else package_digest(),
                "scope": "series" if filters.get("seriesUid") else "patient" if filters.get("patientId") else "cross_patient",
                "scopeInjected": True, "sourceHeadersReturned": False,
                "coverage": "partial" if resource == "research" else "catalog",
                "resultGrain": "instance" if resource in {"instances", "research"} else "study" if resource == "studies" else "series",
                "tagPredicateCount": len(elements),
                "tagCorrelation": "same_instance; same_attribute_value_per_tag" if elements else None,
                "projectionFreshness": "explicit_last_build" if resource == "research" else None,
                "ordering": "numeric InstanceNumber, missing last, SOP UID tie-break" if resource == "instances" else "UTC source day, UID",
                "geometricOrdering": False,
                "datePolicy": "legacy dt in UTC; toDay is exclusive"})


def tag_element(cfg, tag):
    rule = next((x for x in cfg["projections"]["tags"] if x["tag"] == tag["path"]), None)
    if not rule:
        fail("DICOM_TAG_NOT_PROMOTED", "This tag is not included in the active research projection.")
    element = {"p": tag["path"]}
    if rule["type"] == "string":
        if set(tag) != {"path", "equals"} or not isinstance(tag["equals"], str):
            fail("DICOM_QUERY_INVALID", "A promoted string tag requires an exact string value.")
        element["s"] = tag["equals"]
    else:
        predicates = {op: tag[key] for key, op in (("equals", "$eq"), ("gte", "$gte"), ("lt", "$lt")) if key in tag}
        if not predicates or any(not isinstance(v, (int, float)) or isinstance(v, bool) or not math.isfinite(v) for v in predicates.values()):
            fail("DICOM_QUERY_INVALID", "A promoted numeric tag requires numeric predicates.")
        if "gte" in tag and "lt" in tag and tag["gte"] >= tag["lt"]:
            fail("DICOM_QUERY_INVALID", "A numeric lower bound must be less than its exclusive upper bound.")
        element["n"] = predicates
    return element


def value_at(row, path):
    value = row
    for part in path.split("."):
        value = value.get(part) if isinstance(value, dict) else None
    return value


async def execute_query(ctx, plan):
    if not isinstance(plan, QueryPlan) or plan.engine != "mongo_pipeline" or "query" not in plan.plan:
        fail("DICOM_PLAN_INVALID", "Execute a compiled DICOM query.")
    # Do not accept an injected executable pipeline or stale authorization.
    fresh = compile_query(ctx, plan.plan["query"])
    data = fresh.plan
    rows = await storage(ctx).aggregate(data["collection"], data["pipeline"], allow_disk_use=False)
    more = len(rows) > data["limit"]
    rows = rows[:data["limit"]]
    cursor = None
    if more and rows:
        cursor = base64.urlsafe_b64encode(json.dumps({
            "binding": data["binding"], "last": [value_at(rows[-1], k) for k in data["sortKeys"]]
        }, separators=(",", ":")).encode()).decode()
    explain = {**fresh.explain, "page": {"size": len(rows), "hasMore": more, "nextCursor": cursor}}
    return QueryResult(engine_used="mongo_pipeline", rows=rows, explain=explain)
