"""Bounded, indexed reads shared by source inspection and the workbench.

Collection authorization belongs to the caller. Never accept a browser-supplied
MongoDB URI, collection object, query or plan in place of these validated builders.
"""
from dataclasses import asdict
from time import monotonic
from pymongo import timeout
from bson import BSON
from bson.codec_options import CodecOptions
from bson.binary import UuidRepresentation
from .converter import SourceFields, source_id
from .model_config import require
from .projection import utc
from .rules import RuleSet

MAX_BYTES = 8 * 1024 * 1024


def _limit(value, maximum):
    require(type(value) is int and 1 <= value <= maximum, f"Limit must be 1–{maximum}")
    return value


def _fields(fields):
    values = asdict(fields)
    require(all(isinstance(v, str) and v and "." not in v and not v.startswith("$")
                for v in values.values()), "Source mappings require top-level field names")
    require(len(set(values.values())) == len(values), "Source envelope fields must be distinct")
    return values


def inspection_plan(fields=SourceFields(), **options):
    require(not set(options) - {"template", "committed_from", "committed_until", "limit", "max_time_ms"},
            "Unsupported source inspection setting")
    _fields(fields)
    limit = _limit(options.get("limit", 100), 1000)
    timeout = _limit(options.get("max_time_ms", 3000), 5000)
    query, keys = {}, []
    template = options.get("template")
    if template is not None:
        require(isinstance(template, str) and 0 < len(template) <= 512, "Select an exact template")
        query[fields.template] = template
        keys.append((fields.template, 1))
    start, end = options.get("committed_from"), options.get("committed_until")
    require((start is None) == (end is None), "Supply both source commit time bounds")
    if start is not None:
        start, end = utc(start), utc(end)
        require(start < end, "Source time window must be non-empty")
        query[fields.committed_at] = {"$gte": start, "$lt": end}
        keys.append((fields.committed_at, 1))
    keys.append(("_id", 1))
    # Do not read canonicalJSON/cn, patients or full clinical payloads for coverage.
    projection = {name: 1 for name in (
        "_id", fields.composition_id, fields.template, fields.template_id,
        fields.version, fields.committed_at,
    )}
    sort = keys[1:] if template is not None else keys
    return {
        "ok": True, "contract": "openehr-source-inspection/v1",
        "filter": query, "projection": projection, "sort": sort,
        "required_index_prefix": keys, "limit": limit, "max_time_ms": timeout,
        "max_response_bytes": MAX_BYTES, "source_writes": False,
        "coverage_scope": "inspected_sample_only", "representative_sample": False,
        "executed": False,
    }


def _indexed_read(collection, plan):
    """Internal executor: only call with a freshly built, validated plan."""
    with timeout(plan["max_time_ms"] / 1000):
        return _execute_indexed_read(collection, plan)


def _execute_indexed_read(collection, plan):
    started = monotonic()
    required = [tuple(k) for k in plan["required_index_prefix"]]
    indexes = collection.list_indexes()
    try:
        available = list(indexes)
    finally:
        indexes.close()
    compatible = [
        i["name"] for i in available
        if list(i["key"].items())[:len(required)] == required
        and not any(i.get(k) for k in ("partialFilterExpression", "sparse", "hidden"))
        and (not i.get("collation") or i["collation"].get("locale") == "simple")
    ]
    require(bool(compatible), "No compatible existing index; inspection/query was not run")
    index = sorted(compatible)[0]
    cursor = collection.find(plan["filter"], plan.get("projection"),
                             collation={"locale": "simple"})
    cursor = cursor.hint(index).sort(plan["sort"]).limit(plan["limit"] + 1)
    cursor = cursor.max_time_ms(plan["max_time_ms"]).batch_size(min(101, plan["limit"] + 1))
    documents, size, more = [], 0, False
    codec = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
    try:
        for document in cursor:
            if len(documents) == plan["limit"]:
                more = True
                break
            size += len(BSON.encode(document, codec_options=codec))
            require(size <= plan["max_response_bytes"], "Response byte limit exceeded; narrow the scope")
            documents.append(document)
    finally:
        cursor.close()
    return {
        "ok": True, "documents": documents, "has_more": more, "executed": True,
        "query": plan,
        "executed_mql": {
            "find": collection.name, "filter": plan["filter"],
            **({"projection": plan["projection"]} if plan.get("projection") is not None else {}),
            "sort": dict(plan["sort"]), "hint": index, "collation": {"locale": "simple"},
            "limit": plan["limit"] + 1, "maxTimeMS": plan["max_time_ms"],
            "batchSize": min(101, plan["limit"] + 1),
        },
        "telemetry": {
            "requested_index": index, "elapsed_ms": round((monotonic() - started) * 1000, 2),
            "returned": len(documents), "response_bson_bytes": size,
            "documents_examined": None, "keys_examined": None,
            "execution_stats": "not_collected",
        },
    }


def inspect_source(collection, fields=SourceFields(), **options):
    """Read only metadata from a server-authorized source collection."""
    return _indexed_read(collection, inspection_plan(fields, **options))


def source_coverage(headers, opts, rules, *, as_of, fields=SourceFields()):
    """Exact template/version coverage within supplied, bounded metadata only."""
    _fields(fields)
    require(isinstance(headers, list) and len(headers) <= 1000, "Inspect at most 1,000 headers")
    require(isinstance(opts, list) and len(opts) <= 10000, "Invalid OPT inventory")
    inventory = {}
    for opt in opts:
        require(isinstance(opt, dict) and not set(opt) - {"template", "source_digest"},
                "OPT inventory requires exact template and source_digest")
        require(all(isinstance(opt.get(k), str) and opt[k] for k in ("template", "source_digest")),
                "OPT identity must be explicit")
        inventory.setdefault(opt["template"], set()).add(opt["source_digest"])
    compiled = RuleSet.compile(rules)
    at, result, invalid = utc(as_of), {}, 0
    for header in headers:
        require(isinstance(header, dict), "A source header must be an object")
        template = header.get(fields.template)
        if not isinstance(template, str) or not template:
            invalid += 1
            continue
        if template not in result:
            digests = sorted(inventory.get(template, set()))
            rule = compiled.select(template, at)
            result[template] = {
                "template": template, "sample_compositions": 0,
                "opt_status": "missing" if not digests else "present" if len(digests) == 1 else "ambiguous",
                "opt_digests": digests, "rule_revision": rule.revision if rule else None,
                "rule_status": "applicable" if rule else "scheduled" if template in compiled.by_template else "missing",
                "source_template_id_conflicts": 0,
                "approval": "not_reported", "opt_rule_alignment": "requires_exact_preview",
            }
        row = result[template]
        row["sample_compositions"] += 1
        rule = compiled.select(template, at)
        ids = rule.catalog.templates[template].get("source_template_ids") if rule else None
        if ids and (header.get(fields.template_id) is None or source_id(header[fields.template_id]) not in ids):
            row["source_template_id_conflicts"] += 1
    return {
        "ok": True, "contract": "openehr-source-coverage/v1",
        "scope": "inspected_sample_only", "as_of": at, "rules_revision": compiled.revision,
        "inspected_headers": len(headers), "invalid_template_headers": invalid,
        "templates": [result[t] for t in sorted(result)],
        "complete_source_inventory": False, "source_writes": False,
    }
