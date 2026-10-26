"""Governed CDISC Study Query IR and MongoDB compiler."""

from __future__ import annotations

import base64
import json
import re
from typing import Any, Dict, List, Literal, Optional, Set

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


_DATA_PATH = re.compile(r"^data\.[A-Z][A-Z0-9_]{0,31}$")
_EXTENSION_PATH = re.compile(r"^(facets|semantic)(\.[A-Za-z_][A-Za-z0-9_]*)+$")
_SAFE_FACET_PATHS = {
    "facets.subjectId",
    "facets.subjectType",
    "facets.sex",
    "facets.species",
    "facets.strain",
    "facets.armCode",
    "facets.arm",
    "facets.treatmentGroup",
    "facets.visitNumber",
    "facets.visit",
    "facets.epoch",
    "facets.sequence",
    "facets.studyDay",
    "facets.testCode",
    "facets.test",
    "facets.category",
    "facets.specimen",
    "facets.severity",
    "facets.resultCharacter",
    "facets.resultNumeric",
    "facets.resultUnit",
    "facets.projectionVersion",
    "facets.testArticle",
    "facets.doseLevel",
    "facets.doseUnit",
    "facets.route",
    "facets.organ",
    "facets.finding",
    "facets.laterality",
    "facets.specimenId",
    "facets.eventTerm",
    "facets.intervention",
    "facets.startDateTime",
    "facets.endDateTime",
    "facets.referenceId",
    "facets.parameterCode",
    "facets.parameter",
    "facets.analysisValue",
    "facets.analysisValueCharacter",
    "facets.baselineType",
    "facets.analysisRecordFlag",
    "facets.sourceDomain",
    "facets.sourceVariable",
    "facets.sourceSequence",
    "facets.productId",
    "facets.productName",
    "facets.batchId",
    "facets.constituent",
    "facets.evidenceType",
    "facets.evidenceId",
}
_SAFE_TOP_LEVEL_PATHS = {
    "modelSchemaVersion",
    "studyId",
    "packageId",
    "snapshotId",
    "datasetId",
    "profile",
    "domain",
    "publicationState",
    "rowOrdinal",
    "recordKey",
    "entityRefs.type",
    "entityRefs.id",
}


class QueryPredicate(BaseModel):
    path: str
    op: Literal["eq", "ne", "in", "nin", "gt", "gte", "lt", "lte", "exists", "contains"]
    value: Any = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_value(self):
        if self.op in {"in", "nin"} and not isinstance(self.value, list):
            raise ValueError(f"operator {self.op} requires an array value")
        if self.op == "exists" and not isinstance(self.value, bool):
            raise ValueError("operator exists requires a boolean value")
        return self


class QueryWhere(BaseModel):
    and_: List[QueryPredicate] = Field(default_factory=list, alias="and")

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class QuerySource(BaseModel):
    profile: Optional[Literal["send", "sdtm", "adam", "tig"]] = None
    domains: List[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")

    @field_validator("domains")
    @classmethod
    def normalize_domains(cls, value: List[str]) -> List[str]:
        return [domain.strip().upper() for domain in value if domain.strip()]


class QueryScope(BaseModel):
    studies: List[str] = Field(default_factory=list)
    snapshots: Literal["published"] | List[str] = "published"

    model_config = ConfigDict(extra="forbid")


class QueryOrder(BaseModel):
    path: str
    direction: Literal["asc", "desc"] = "asc"

    model_config = ConfigDict(extra="forbid")


class QueryPage(BaseModel):
    limit: int = Field(default=100, ge=1, le=1000)
    token: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class CdiscStudyQuery(BaseModel):
    version: Literal["cdisc-query/v1"] = "cdisc-query/v1"
    scope: QueryScope = Field(default_factory=QueryScope)
    source: QuerySource = Field(default_factory=QuerySource, alias="from")
    where: QueryWhere = Field(default_factory=QueryWhere)
    select: List[str] = Field(default_factory=lambda: ["studyId", "datasetId", "rowOrdinal", "data"])
    order_by: List[QueryOrder] = Field(default_factory=list, alias="orderBy")
    page: QueryPage = Field(default_factory=QueryPage)

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


def _path_allowed(path: str, extra_paths: Set[str]) -> bool:
    configured_extension = path in extra_paths and bool(_EXTENSION_PATH.fullmatch(path))
    return (
        path in _SAFE_TOP_LEVEL_PATHS
        or path in _SAFE_FACET_PATHS
        or configured_extension
        or bool(_DATA_PATH.fullmatch(path))
    )


def _compile_predicate(predicate: QueryPredicate) -> Dict[str, Any]:
    def has_operator_key(value: Any) -> bool:
        if isinstance(value, dict):
            return any(str(key).startswith("$") or has_operator_key(item) for key, item in value.items())
        if isinstance(value, list):
            return any(has_operator_key(item) for item in value)
        return False

    if has_operator_key(predicate.value):
        raise ValueError("predicate values cannot contain MongoDB operator keys")
    operator_map = {
        "eq": None,
        "ne": "$ne",
        "in": "$in",
        "nin": "$nin",
        "gt": "$gt",
        "gte": "$gte",
        "lt": "$lt",
        "lte": "$lte",
        "exists": "$exists",
        "contains": "$elemMatch",
    }
    mongo_operator = operator_map[predicate.op]
    if mongo_operator is None:
        return {predicate.path: predicate.value}
    if predicate.op == "contains":
        if not isinstance(predicate.value, dict):
            raise ValueError("contains requires an object value")
    return {predicate.path: {mongo_operator: predicate.value}}


def compile_study_query(
    query: CdiscStudyQuery | Dict[str, Any],
    *,
    tenant_id: str,
    collection: str,
    snapshot_collection: str,
    model_schema_version: Optional[str] = None,
    extra_allowed_paths: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    ir = query if isinstance(query, CdiscStudyQuery) else CdiscStudyQuery.model_validate(query)
    offset = 0
    if ir.page.token is not None:
        try:
            padding = "=" * (-len(ir.page.token) % 4)
            cursor = json.loads(base64.urlsafe_b64decode(ir.page.token + padding).decode("utf-8"))
            if cursor.get("version") != 1 or not isinstance(cursor.get("offset"), int) or cursor["offset"] < 0:
                raise ValueError
            offset = cursor["offset"]
        except Exception as exc:
            raise ValueError("page.token is not a valid cdisc-query/v1 cursor") from exc
    extra_paths = extra_allowed_paths or set()
    referenced_paths = {
        *(predicate.path for predicate in ir.where.and_),
        *ir.select,
        *(order.path for order in ir.order_by),
    }
    invalid_paths = sorted(path for path in referenced_paths if path != "data" and not _path_allowed(path, extra_paths))
    if invalid_paths:
        raise ValueError(f"query references paths that are not allowed: {invalid_paths}")

    mandatory: List[Dict[str, Any]] = [{"tenantId": tenant_id}]
    if model_schema_version:
        mandatory.append({"modelSchemaVersion": model_schema_version})
    if ir.scope.snapshots != "published":
        mandatory.append({"snapshotId": {"$in": ir.scope.snapshots}})
    if ir.scope.studies:
        mandatory.append({"studyId": {"$in": ir.scope.studies}})
    if ir.source.profile:
        mandatory.append({"profile": ir.source.profile})
    if ir.source.domains:
        mandatory.append({"domain": {"$in": ir.source.domains}})
    mandatory.extend(_compile_predicate(predicate) for predicate in ir.where.and_)

    pipeline: List[Dict[str, Any]] = [{"$match": {"$and": mandatory}}]
    if ir.scope.snapshots == "published":
        pipeline.extend(
            [
                {
                    "$lookup": {
                        "from": snapshot_collection,
                        "localField": "snapshotRef",
                        "foreignField": "_id",
                        "as": "__snapshot",
                    }
                },
                {"$match": {"__snapshot.state": "published"}},
            ]
        )
    if ir.order_by:
        ordering = {
            order.path: 1 if order.direction == "asc" else -1
            for order in ir.order_by
        }
        ordering.setdefault("_id", 1)
        pipeline.append(
            {
                "$sort": ordering
            }
        )
    else:
        pipeline.append({"$sort": {"_id": 1}})
    if offset:
        pipeline.append({"$skip": offset})
    projection = {path: 1 for path in ir.select}
    projection.setdefault("_id", 0)
    pipeline.append({"$project": projection})
    pipeline.append({"$limit": ir.page.limit + 1})

    scope = "portfolio"
    if len(ir.scope.studies) == 1 and any(
        predicate.path in {"facets.subjectId", "data.USUBJID", "data.SUBJID"}
        for predicate in ir.where.and_
    ):
        scope = "study_subject"
    elif len(ir.scope.studies) == 1:
        scope = "study"
    return {
        "engine": "mongo_pipeline",
        "collection": collection,
        "pipeline": pipeline,
        "scope": scope,
        "queryVersion": ir.version,
        "pagination": {"offset": offset, "limit": ir.page.limit},
        "governance": {
            "tenantInjected": True,
            "modelSchemaVersionInjected": model_schema_version,
            "publicationConstraintInjected": ir.scope.snapshots == "published",
            "publicationMarkerCollection": snapshot_collection if ir.scope.snapshots == "published" else None,
            "clientMongoOperatorsAccepted": False,
        },
    }


def encode_page_token(offset: int) -> str:
    payload = json.dumps({"version": 1, "offset": offset}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")
