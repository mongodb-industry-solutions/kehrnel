"""Business variables across retained rules. No clinical document reads or counts."""

from collections import defaultdict
from copy import deepcopy
from .model_config import require
from .projection import utc
from .rules import RuleSet
from .business_reuse import source_definition, variable_contract
from .source import digest

TYPE_LABELS = {
    "raw": "Typed JSON value", "measurement": "Measurement with type and unit",
    "quantity": "Number with unit", "number": "Number", "coded": "Code and label",
    "boolean": "Yes or no", "date": "Date or time", "text": "Text", "identifier": "Identifier",
}


def catalog_page(rules, *, as_of, key=None, search="", offset=0, limit=50):
    """Without key return variables; with key return a page of their bindings.

    Rule applicability is not evidence of approval or data presence. HDL supplies
    release/draft state from its own store, keyed by the immutable revision.
    """
    require(type(limit) is int and 1 <= limit <= 100, "Catalog page limit is 1–100")
    require(type(offset) is int and offset >= 0, "Catalog offset must be nonnegative")
    require(isinstance(search, str) and len(search) <= 256, "Invalid catalog search")
    compiled = RuleSet.compile(rules)
    at = utc(as_of)
    variables, bindings, label_sets = {}, defaultdict(list), defaultdict(set)
    for rule in sorted(compiled.by_revision.values(), key=lambda r: (r.effective_at, r.revision)):
        selected = compiled.select(rule.template, at)
        state = ("scheduled" if rule.effective_at > at else
                 "applicable" if selected.revision == rule.revision else "historical")
        template = rule.catalog.templates[rule.template]
        for variable_key, variable in rule.catalog.variables.items():
            variables[variable_key] = deepcopy(variable)
            label_sets[variable_key].add(variable.get("label", variable_key))
        groups = [("facts", g, g["fields"]) for g in template["groups"]]
        groups.append(("activity", {}, template.get("activity_fields", [])))
        for collection, group, fields in groups:
            for field in fields:
                bindings[field["key"]].append({
                    "template": rule.template, "rule_revision": rule.revision,
                    "epoch": rule.epoch, "effective_at": rule.effective_at,
                    "applicability": state, "approval": "not_reported",
                    "collection": collection, "kind": group.get("kind"),
                    "business_path": ("clinical." if collection == "facts" else "business.") + field["key"],
                    "anchor": group.get("anchor"), "field_id": field.get("field_id"),
                    "source_path": field["path"],
                    "source_definition": source_definition(field["path"]),
                    "value_shape": "array" if field.get("multiple") else "value",
                    "source_semantic_key": field.get("business_reuse", {}).get(
                        "source_semantic_key", rule.catalog.variables[field["key"]].get("semantic_key")),
                    "source_rm_types": field.get("business_reuse", {}).get(
                        "source_rm_types", rule.catalog.variables[field["key"]].get("rm_types", [])),
                    "reuse": deepcopy(field.get("business_reuse")),
                })
    if key is not None:
        require(key in variables, "Unknown business variable")
        rows = bindings[key]
    else:
        rows = []
        for variable_key, variable in sorted(variables.items()):
            sources = bindings[variable_key]
            labels = sorted(label_sets[variable_key])
            if search.casefold() not in " ".join([variable_key, *labels]).casefold():
                continue
            rows.append({
                **variable, "labels": labels,
                "display_type": TYPE_LABELS[variable["type"]],
                "template_count": len({b["template"] for b in sources}),
                "binding_count": len(sources),
                "rule_revisions": sorted({b["rule_revision"] for b in sources}),
                "value_shapes": sorted({b["value_shape"] for b in sources}),
                "business_contract_digest": digest(variable_contract(variable)),
                "collections": sorted({b["collection"] for b in sources}),
                "fact_count": None, "data_presence": "not_measured",
            })
    return {
        "ok": True, "contract": "openehr-business-catalog-page/v1",
        "rules_revision": compiled.revision, "as_of": at,
        "mode": "bindings" if key is not None else "variables", "key": key,
        "items": rows[offset:offset + limit], "total": len(rows),
        "next_offset": offset + limit if offset + limit < len(rows) else None,
        "clinical_reads": False,
        "equivalence_policy": "Exact-source aliases or explicit reviewed business reuse; never merge by label.",
    }
