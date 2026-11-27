"""Immutable rule revisions and explicit activation, independent of wall-clock retries."""

from bisect import bisect_right
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from .projection import Catalog, utc
from .source import digest, segments
from .model_config import validate_stored
from .metadata_compatibility import IdentityPolicy
from .source_coverage import CoveragePlan


@dataclass(frozen=True)
class RuleRevision:
    revision: str
    epoch: int
    effective_at: datetime
    template: str
    catalog: Catalog
    retention: dict
    schema_version: int = 1
    source_metadata_policy: IdentityPolicy | None = None
    source_coverage: CoveragePlan | None = None

    @classmethod
    def compile(cls, value):
        if set(value) - {
            "catalog",
            "epoch",
            "effective_at",
            "retention",
            "schema_version",
            "revision",
            "_id",
            "template",
        }:
            raise ValueError("Unknown rule revision setting")
        epoch = value.get("epoch")
        schema = value.get("schema_version", 1)
        if (
            type(epoch) is not int
            or epoch < 1
            or type(schema) is not int
            or schema != 1
        ):
            raise ValueError(
                "Positive rule epoch and supported schema_version=1 required"
            )
        c = Catalog(value["catalog"])
        if len(c.templates) != 1:
            raise ValueError(
                "A rule revision binds exactly one template/version identifier"
            )
        t = next(iter(c.templates.values()))
        if "source_template_ids" in t:
            ids = t["source_template_ids"]
            if (
                not isinstance(ids, list)
                or not ids
                or any(not isinstance(x, str) or not x for x in ids)
                or len(set(ids)) != len(ids)
            ):
                raise ValueError(
                    "Approved source template IDs must be a non-empty list of unique strings"
                )
        policy = IdentityPolicy.compile(t.get("source_metadata_policy"), t["template_name"])
        validate_stored(t, c.variables)
        coverage = CoveragePlan.compile(t.get("source_coverage"), t)
        # A worker compiles only variables used by this template, not the global catalog.
        used = {b["key"] for g in t["groups"] for b in g["fields"]} | {
            b["key"] for b in t.get("activity_fields", [])
        }
        c = Catalog(
            {
                "variables": [deepcopy(c.variables[k]) for k in sorted(used)],
                "templates": [t],
            }
        )
        for g in t["groups"]:
            segments(g["anchor"])
            for b in g["fields"]:
                segments(b["path"])
        retention = deepcopy(value.get("retention", {"mode": "retain"}))
        if retention.get("mode") == "retain":
            if set(retention) != {"mode"}:
                raise ValueError("Retain policy has no expiry settings")
        elif retention.get("mode") == "delete_facts":
            if (
                set(retention) != {"mode", "clock", "days"}
                or retention["clock"] not in ("clinical_time", "committed_time")
                or type(retention["days"]) is not int
                or not 1 <= retention["days"] <= 36500
            ):
                raise ValueError(
                    "Fact TTL needs an explicit clock and 1–36500 elapsed days"
                )
        else:
            raise ValueError(
                "Supported retention: retain or delete_facts; TTL does not archive"
            )
        at = utc(value["effective_at"])
        normalized = {
            "catalog": c.value,
            "epoch": epoch,
            "effective_at": at.isoformat(),
            "retention": retention,
            "schema_version": schema,
        }
        revision = digest(normalized)
        if value.get("_id") not in (None, revision) or value.get("template") not in (
            None,
            t["template_name"],
        ):
            raise ValueError("Stored rule identity does not match contents")
        if value.get("revision") not in (None, revision):
            raise ValueError("Rule digest differs from its immutable contents")
        return cls(revision, epoch, at, t["template_name"], c, retention, schema, policy, coverage)

    def document(self):
        return {
            "_id": self.revision,
            "revision": self.revision,
            "catalog": self.catalog.value,
            "epoch": self.epoch,
            "effective_at": self.effective_at.isoformat(),
            "retention": self.retention,
            "schema_version": self.schema_version,
        }


class RuleSet:
    def __init__(self, revisions):
        self.by_template = {}
        self.by_revision = {}
        self.times = {}
        semantics = {}
        for r in revisions:
            for key, var in r.catalog.variables.items():
                identity = (var["type"], var.get("unit"), var.get("semantic_key"))
                if key in semantics and semantics[key] != identity:
                    raise ValueError(
                        "Cross-template business variables must retain their type, canonical unit and declared semantic identity: "
                        + key
                    )
                semantics[key] = identity
            if r.revision in self.by_revision:
                raise ValueError("Duplicate rule revision")
            self.by_revision[r.revision] = r
            self.by_template.setdefault(r.template, []).append(r)
        for template, rs in self.by_template.items():
            rs.sort(key=lambda r: r.effective_at)
            if len({r.effective_at for r in rs}) != len(rs) or any(
                a.epoch >= b.epoch for a, b in zip(rs, rs[1:])
            ):
                raise ValueError(
                    "Template revisions need unique activation times and increasing epochs"
                )
            self.times[template] = [r.effective_at for r in rs]
        self.revision = digest(sorted(self.by_revision))

    @classmethod
    def compile(cls, values):
        return cls([RuleRevision.compile(v) for v in values])

    def select(self, template, accepted_at, forced_revision=None):
        if forced_revision:
            r = self.by_revision.get(forced_revision)
            if not r or r.template != template:
                raise ValueError(
                    "Backfill pins an existing revision of the same template"
                )
            return r
        i = bisect_right(self.times.get(template, []), utc(accepted_at)) - 1
        return self.by_template[template][i] if i >= 0 else None
