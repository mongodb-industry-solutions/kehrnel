"""Safe grouped aggregation over published CDISC records."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.cdisc.query import (
    QueryScope,
    QuerySource,
    QueryWhere,
    _compile_predicate,
    _path_allowed,
)

from ..common import MODEL_SCHEMA_VERSION, collections, config, storage_adapter


_METRIC_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


class AnalysisMetric(BaseModel):
    name: str
    op: Literal["count", "countDistinct", "sum", "avg", "min", "max"]
    path: Optional[str] = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_metric(self):
        if not _METRIC_NAME.fullmatch(self.name):
            raise ValueError("metric names must start with a letter and contain only letters, digits, or underscore")
        if self.op != "count" and not self.path:
            raise ValueError(f"metric {self.op} requires path")
        return self


class AnalysisOrder(BaseModel):
    field: str
    direction: Literal["asc", "desc"] = "desc"

    model_config = ConfigDict(extra="forbid")


class CdiscAnalysis(BaseModel):
    version: Literal["cdisc-analysis/v1"] = "cdisc-analysis/v1"
    scope: QueryScope = Field(default_factory=QueryScope)
    source: QuerySource = Field(default_factory=QuerySource, alias="from")
    where: QueryWhere = Field(default_factory=QueryWhere)
    group_by: List[str] = Field(default_factory=list, alias="groupBy", max_length=5)
    metrics: List[AnalysisMetric] = Field(min_length=1, max_length=20)
    order_by: List[AnalysisOrder] = Field(default_factory=list, alias="orderBy", max_length=5)
    limit: int = Field(default=100, ge=1, le=500)

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("group_by")
    @classmethod
    def unique_groups(cls, value: List[str]) -> List[str]:
        if len(value) != len(set(value)):
            raise ValueError("groupBy paths must be unique")
        return value


def compile_analysis(
    value: Dict[str, Any],
    *,
    tenant_id: str,
    collection: str,
    snapshot_collection: str,
    model_schema_version: str | None = None,
    extra_allowed_paths: set[str] | None = None,
) -> Dict[str, Any]:
    ir = CdiscAnalysis.model_validate(value or {})
    extra_paths = extra_allowed_paths or set()
    paths = set(ir.group_by)
    paths.update(metric.path for metric in ir.metrics if metric.path)
    paths.update(predicate.path for predicate in ir.where.and_)
    invalid = sorted(path for path in paths if not _path_allowed(path, extra_paths))
    if invalid:
        raise ValueError(f"analysis references paths that are not allowed: {invalid}")

    metric_names = {metric.name for metric in ir.metrics}
    invalid_order = sorted(order.field for order in ir.order_by if order.field not in metric_names)
    if invalid_order:
        raise ValueError(f"analysis orderBy fields must name declared metrics: {invalid_order}")

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
        pipeline.extend([
            {"$lookup": {
                "from": snapshot_collection,
                "localField": "snapshotRef",
                "foreignField": "_id",
                "as": "__snapshot",
            }},
            {"$match": {"__snapshot.state": "published"}},
        ])

    group: Dict[str, Any] = {
        "_id": {f"g{index}": f"${path}" for index, path in enumerate(ir.group_by)}
        if ir.group_by else None
    }
    distinct_fields: Dict[str, str] = {}
    for metric in ir.metrics:
        if metric.op == "count":
            group[metric.name] = {"$sum": 1}
        elif metric.op == "countDistinct":
            internal = f"__distinct_{metric.name}"
            distinct_fields[metric.name] = internal
            group[internal] = {"$addToSet": f"${metric.path}"}
        else:
            group[metric.name] = {f"${metric.op}": f"${metric.path}"}
    pipeline.append({"$group": group})

    project: Dict[str, Any] = {
        "_id": 0,
        "groupValues": [f"$_id.g{index}" for index in range(len(ir.group_by))],
    }
    for metric in ir.metrics:
        project[metric.name] = (
            {"$size": {"$filter": {
                "input": f"${distinct_fields[metric.name]}",
                "as": "value",
                "cond": {"$ne": ["$$value", None]},
            }}}
            if metric.op == "countDistinct" else 1
        )
    pipeline.append({"$project": project})
    if ir.order_by:
        pipeline.append({"$sort": {
            **{order.field: 1 if order.direction == "asc" else -1 for order in ir.order_by},
            "groupValues": 1,
        }})
    else:
        pipeline.append({"$sort": {"groupValues": 1}})
    pipeline.append({"$limit": ir.limit})

    return {
        "version": ir.version,
        "collection": collection,
        "pipeline": pipeline,
        "columns": {
            "groupBy": ir.group_by,
            "metrics": [metric.model_dump(exclude_none=True) for metric in ir.metrics],
        },
        "governance": {
            "tenantInjected": True,
            "modelSchemaVersionInjected": model_schema_version,
            "publicationConstraintInjected": ir.scope.snapshots == "published",
            "publicationMarkerCollection": snapshot_collection if ir.scope.snapshots == "published" else None,
            "clientMongoOperatorsAccepted": False,
        },
        "evidence": {
            "studyIds": ir.scope.studies,
            "snapshotScope": ir.scope.snapshots,
            "profile": ir.source.profile,
            "domains": ir.source.domains,
        },
    }


class AnalysisService:
    async def run(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg = config(ctx)
        coll = collections(cfg)
        try:
            plan = compile_analysis(
                payload,
                tenant_id=str(cfg["tenant_id"]),
                collection=coll["records"],
                snapshot_collection=coll["snapshots"],
                model_schema_version=MODEL_SCHEMA_VERSION,
                extra_allowed_paths=set((cfg.get("query") or {}).get("extra_allowed_paths") or []),
            )
        except Exception as exc:
            raise KehrnelError(code="INVALID_CDISC_ANALYSIS", status=400, message=str(exc)) from exc
        try:
            rows = await storage_adapter(ctx).aggregate(plan["collection"], plan["pipeline"])
        except Exception as exc:
            raise KehrnelError(code="CDISC_ANALYSIS_EXECUTION_FAILED", status=502, message=str(exc)) from exc
        return {
            "ok": True,
            "version": plan["version"],
            "rows": rows,
            "columns": plan["columns"],
            "explain": {"governance": plan["governance"], "evidence": plan["evidence"]},
        }
