"""Native MongoDB row inspection, not an unrestricted aggregation endpoint."""
from .model_config import require
from .projection import utc
from .rules import RuleSet
from .inspection import _indexed_read, _limit, MAX_BYTES


def workbench_plan(rules, *, dataset, scope, start, end, variables=None, limit=25, max_time_ms=3000):
    require(dataset in {"facts", "activity"}, "Choose facts or activity")
    require(isinstance(scope, dict) and not set(scope) - {"kind", "provider_id", "patient_id", "template_name"},
            "Unsupported workbench scope")
    require(all(isinstance(v, str) and 0 < len(v) <= 512 for v in scope.values()),
            "Scope values must be exact identifiers")
    require(dataset != "activity" or "kind" not in scope, "Activity has no clinical fact kind")
    start, end = utc(start), utc(end)
    require(start < end, "Choose a non-empty time window")
    limit, timeout = _limit(limit, 100), _limit(max_time_ms, 5000)
    # These prefixes correspond to the native indexes declared by the strategy.
    candidates = ("kind", "provider_id") if dataset == "facts" else ("patient_id", "template_name")
    leading = next((key for key in candidates if key in scope), None)
    require(leading is not None,
            "Facts need a record type or provider; activity needs a patient or template")
    query = {"context." + key: value for key, value in scope.items()}
    query["context.event_at"] = {"$gte": start, "$lt": end}
    compiled = RuleSet.compile(rules)
    projection = None
    if variables is not None:
        require(isinstance(variables, list) and 1 <= len(variables) <= 50
                and all(isinstance(k, str) for k in variables)
                and len(set(variables)) == len(variables), "Choose 1–50 unique business variables")
        known = set()
        for rule in compiled.by_revision.values():
            template = rule.catalog.templates[rule.template]
            if scope.get("template_name") and scope["template_name"] != rule.template:
                continue
            if dataset == "facts":
                for group in template["groups"]:
                    if not scope.get("kind") or group["kind"] == scope["kind"]:
                        known.update(b["key"] for b in group["fields"])
            else:
                known.update(b["key"] for b in template.get("activity_fields", []))
        require(set(variables) <= known, "Variable does not belong to the selected collection and record type")
        root = "clinical." if dataset == "facts" else "business."
        projection = {"_id": 1, "context": 1, **{root + key: 1 for key in variables}}
    return {
        "ok": True, "contract": "openehr-workbench-rows/v1", "dataset": dataset,
        "filter": query, "projection": projection, "sort": [("context.event_at", 1)],
        "required_index_prefix": [("context." + leading, 1), ("context.event_at", 1)],
        "limit": limit, "max_time_ms": timeout, "max_response_bytes": MAX_BYTES,
        "rules_revision": compiled.revision, "executed": False, "read_only": True,
        "notes": [
            "Row and time limits bound the response and execution time, not a proven number of examined documents.",
            "has_more means more matching rows exist; this response is not the complete population.",
            "For broad composition counts use activity rollups at their declared dimensions.",
            "An index hint is not executionStats or evidence of Atlas Search use.",
        ],
    }


def read_workbench(collection, rules, **options):
    """Caller resolves the dataset to the activated environment's collection."""
    return _indexed_read(collection, workbench_plan(rules, **options))
