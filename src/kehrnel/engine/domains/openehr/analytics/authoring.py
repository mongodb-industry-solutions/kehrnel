"""Compile GUI choices against an exact OPT. No database reads or writes.

The browser selects from a server-derived inventory. It cannot invent source
paths or fact boundaries. The same compiler serves HDL and direct SDK callers.
"""

from copy import deepcopy

from .default_mapping import propose_from_opt, finalize_proposal
from .rules import RuleRevision, RuleSet
from .model_config import require
from .metadata_compatibility import POLICY
from .business_reuse import apply_reuse


def business_aliases(rules, extra=None):
    aliases = dict(extra or {})
    require(
        all(isinstance(k, str) and isinstance(v, str) for k, v in aliases.items()),
        "Business aliases must map semantic identities to business keys",
    )
    for rule in rules:
        for variable in RuleRevision.compile(rule).catalog.variables.values():
            semantic = variable.get("semantic_key")
            if semantic:
                require(
                    semantic not in aliases or aliases[semantic] == variable["key"],
                    "An existing semantic identity cannot change its business key",
                )
                aliases[semantic] = variable["key"]
    require(
        len(set(aliases.values())) == len(aliases),
        "Different semantic identities cannot share a business key",
    )
    return aliases


def proposal_for(payload, prior_rules=()):
    return propose_from_opt(
        payload["opt_xml"],
        epoch=payload.get("epoch", 1),
        effective_at=payload["effective_at"],
        aliases=business_aliases(prior_rules, payload.get("aliases")),
    )


def compile_draft(payload, prior_rules=()):
    proposal = proposal_for(payload, prior_rules)
    require(
        payload.get("proposal_revision") in (None, proposal["revision"]),
        "Template or business registry changed; regenerate the proposal",
    )
    edits = deepcopy(payload.get("edits", {}))
    require(
        isinstance(edits, dict)
        and not set(edits)
        - {"exclude_field_ids", "disabled_anchors", "variables", "groups", "retention", "source_metadata_policy", "reuse_variables"},
        "Unsupported draft setting",
    )
    groups = proposal["candidate_rule"]["catalog"]["templates"][0]["groups"]
    anchors = {g["anchor"] for g in groups}
    disabled = edits.get("disabled_anchors", [])
    require(
        isinstance(disabled, list)
        and all(isinstance(a, str) for a in disabled)
        and len(disabled) == len(set(disabled))
        and set(disabled) <= anchors,
        "Disable only unique proposed fact blocks",
    )
    excluded = edits.get("exclude_field_ids", [])
    require(
        isinstance(excluded, list)
        and all(isinstance(a, str) for a in excluded)
        and len(excluded) == len(set(excluded)),
        "Select unique excluded fields",
    )
    excluded = set(excluded)
    excluded.update(
        f["field_id"]
        for f in proposal["fields"]
        if f["group_anchors"] and set(f["group_anchors"]) <= set(disabled)
    )
    value = finalize_proposal(proposal, exclude_field_ids=excluded)
    value.pop("_id", None)
    value.pop("revision", None)
    template = value["catalog"]["templates"][0]
    source_policy = edits.get("source_metadata_policy", POLICY)
    require(source_policy in ("strict", POLICY), "Choose a supported source metadata policy")
    if source_policy == "strict":
        template.pop("source_metadata_policy", None)
    template["groups"] = [g for g in template["groups"] if g["anchor"] not in disabled]
    changes = edits.get("variables", {})
    known = {v["key"] for v in proposal["candidate_rule"]["catalog"]["variables"]}
    require(
        isinstance(changes, dict) and set(changes) <= known, "Unknown business variable"
    )
    for variable in value["catalog"]["variables"]:
        patch = changes.get(variable["key"], {})
        require(
            isinstance(patch, dict)
            and not set(patch) - {"label", "type", "unit", "conversion_rules"},
            "Only labels, types and supported conversions can be edited",
        )
        if "label" in patch:
            require(
                isinstance(patch["label"], str) and 0 < len(patch["label"]) <= 256,
                "Business labels must contain 1–256 characters",
            )
        variable.update({k: v for k, v in patch.items() if k != "conversion_rules"})
        if variable["type"] == "quantity":
            require(
                "DV_QUANTITY" in variable.get("rm_types", [])
                or (
                    "DV_TEXT" in variable.get("rm_types", [])
                    and patch.get("conversion_rules", {}).get("text_numeric_conversion")
                ),
                "Quantities need a quantity source or an explicit text conversion",
            )
        if variable["type"] == "date":
            require(
                set(variable.get("rm_types", [])) <= {"DV_DATE", "DV_DATE_TIME"},
                "Date bindings need a date source",
            )
        for g in template["groups"]:
            for b in g["fields"]:
                if b["key"] == variable["key"] and "conversion_rules" in patch:
                    b["conversions"] = deepcopy(patch["conversion_rules"])
    settings = edits.get("groups", {})
    require(
        isinstance(settings, dict) and set(settings) <= anchors,
        "Configure only fact blocks derived from this exact OPT",
    )
    for g in template["groups"]:
        patch = settings.get(g["anchor"], {})
        require(
            isinstance(patch, dict)
            and not set(patch)
            - {
                "subject",
                "subject_mode",
                "subject_key",
                "time",
                "time_key",
                "required_fields",
                "allow_composition_time",
            },
            "Fact boundaries and source paths cannot be edited",
        )
        d = g["definition"]
        d.update({k: v for k, v in patch.items() if k != "subject_key"})
        g["subject"] = d["subject"]
        require(
            isinstance(g["subject"], str) and 0 < len(g["subject"]) <= 80,
            "Declare the clinical subject",
        )
        keys = {b["key"] for b in g["fields"]}
        protected = set(d["required_fields"])
        subject_key = patch.get("subject_key")
        if d["subject_mode"] == "bound_identity":
            require(
                subject_key in keys, "Select a subject identity inside the fact block"
            )
            for b in g["fields"]:
                if b["key"] == subject_key:
                    require(
                        not b.get("multiple") and b.get("scope") == "block",
                        "Subject identity must be a single value owned by this block",
                    )
                    b["subject_identity"] = True
            protected.add(subject_key)
        else:
            require(
                subject_key is None, "EHR owner does not use a separate subject key"
            )
        if d["time"] == "mapped_field":
            require(
                d.get("time_key") in keys, "Select clinical time inside the fact block"
            )
            require(
                not next(b for b in g["fields"] if b["key"] == d["time_key"]).get(
                    "multiple"
                ),
                "Clinical time must be a single value",
            )
            g["time_key"] = d["time_key"]
            d["allow_composition_time"] = False
            protected.add(d["time_key"])
        d["required_fields"] = sorted(protected)
    value["retention"] = edits.get("retention", {"mode": "retain"})
    reuse = apply_reuse(value, edits.get("reuse_variables", {}), prior_rules)
    rule = RuleRevision.compile(value).document()
    # Existing immutable revisions remain in the release. No silent replacement.
    retained = [r for r in prior_rules if r.get("revision") != rule["revision"]]
    rules = RuleSet.compile([*retained, rule])
    rule.pop("_id", None)
    return {
        "ok": True,
        "proposal": proposal,
        "rule": rule,
        "rules_revision": rules.revision,
        "review": {
            "status": "validated_draft",
            "approved": False,
            "source_writes": False,
            "excluded_field_ids": sorted(excluded),
            "disabled_anchors": disabled,
            "sample_validation_required": True,
            "business_reuse": reuse,
        },
    }
