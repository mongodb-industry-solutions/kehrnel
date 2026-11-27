"""Clinical contract validation shared with the independent analytical model."""

from copy import deepcopy
import math
from .source import SourceError

CONTRACT = "openehr-fact-model/v1"
POLICY = {
    "representation": "individual_facts",
    "activation": "explicit_publication",
    "history": "rebuild_loaded_cohort",
    "retention": "retain_until_explicit_cleanup",
}
ENTRY_TYPES = {"OBSERVATION", "EVALUATION", "ACTION", "INSTRUCTION", "ADMIN_ENTRY"}


def under(path, parent):
    return not parent or path == parent or path.startswith(parent + "/")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def conversion_rules(rules, variable):
    require(
        isinstance(rules, dict)
        and not set(rules) - {"unit_conversions", "text_numeric_conversion"},
        "Only explicit unit and strict decimal conversions are supported",
    )
    require(
        not rules or variable["type"] == "quantity",
        "Conversions require a quantity with a canonical unit",
    )
    units = rules.get("unit_conversions", {})
    require(
        isinstance(units, dict) and len(units) <= 40,
        "Declare at most 40 source-unit conversions",
    )
    for unit, rule in units.items():
        require(
            isinstance(unit, str) and 0 < len(unit) <= 32,
            "A conversion needs its exact source unit",
        )
        r = rule if isinstance(rule, dict) else {"scale": rule}
        require(
            not set(r) - {"scale", "offset"} and "scale" in r,
            "A unit rule uses scale and optional offset",
        )
        require(
            all(type(v) in (int, float) and math.isfinite(v) for v in r.values())
            and r["scale"] > 0,
            "Conversion scale must be positive and all coefficients finite",
        )
        require(
            unit != variable.get("unit")
            or (r["scale"] == 1 and r.get("offset", 0) == 0),
            "The canonical unit cannot be converted to a different magnitude",
        )
    parser = rules.get("text_numeric_conversion")
    if parser is not None:
        require(
            isinstance(parser, dict)
            and not set(parser)
            - {"kind", "decimal_separator", "source_unit", "on_non_numeric"},
            "Invalid numeric parser",
        )
        require(
            parser.get("kind") == "strict_decimal"
            and parser.get("decimal_separator") in (".", ","),
            "Choose the strict decimal parser and decimal separator",
        )
        require(
            parser.get("source_unit") == variable.get("unit")
            or parser.get("source_unit") in units,
            "The text source unit needs an approved conversion to the canonical unit",
        )
        require(
            parser.get("on_non_numeric", "preserve") in ("preserve", "reject"),
            "Nonnumeric text must be preserved as unavailable or rejected",
        )
        require(
            "DV_TEXT" in variable.get("rm_types", []),
            "Text parsing requires a DV_TEXT source variant",
        )
    return deepcopy(rules)


def validate_stored(template, variables):
    """Protect persisted contracts as well as the interactive API."""
    require(
        template.get("model_contract") == CONTRACT,
        "Require the reviewed v1 clinical model contract",
    )
    from .business_reuse import validate_reuse
    validate_reuse(template, variables)
    seen = set()
    for g in template["groups"]:
        d = g.get("definition")
        require(isinstance(d, dict), "Every configured fact needs its block definition")
        require(
            d.get("entry_type") in ENTRY_TYPES | {"COMPOSITION"},
            "Unknown openEHR ENTRY profile",
        )
        require(
            d.get("subject_mode") in ("ehr_owner", "bound_identity"),
            "Choose an explicit clinical-subject policy",
        )
        require(
            g.get("subject") == d.get("subject"),
            "The fact subject type must match its definition",
        )
        require(
            d.get("time") in ("nearest_recorded_time", "mapped_field")
            and type(d.get("allow_composition_time")) is bool,
            "Invalid clinical-time policy",
        )
        require(
            d.get("missing_required") == "reject_composition",
            "Unsupported missing-value policy",
        )
        require(
            isinstance(d.get("required_fields"), list),
            "Declare required interpretation fields",
        )
        require(
            under(g["anchor"], d["entry_path"]),
            "Fact boundary must belong to its declared ENTRY",
        )
        keys = {b["key"] for b in g["fields"]}
        require(
            set(d["required_fields"]) <= keys,
            "Required interpretation field is not mapped",
        )
        require(
            len(keys) == len(g["fields"]) and (g["anchor"], g["kind"]) not in seen,
            "Duplicate block or field",
        )
        seen.add((g["anchor"], g["kind"]))
        subjects = [b["key"] for b in g["fields"] if b.get("subject_identity")]
        require(
            (
                d["subject_mode"] == "ehr_owner"
                and not subjects
                and d["subject"] == "patient"
            )
            or (
                d["subject_mode"] == "bound_identity"
                and len(subjects) == 1
                and subjects[0] in d["required_fields"]
            ),
            "Subject identity must be protected",
        )
        if d["time"] == "mapped_field":
            require(
                g.get("time_key") == d.get("time_key")
                and d["time_key"] in d["required_fields"]
                and variables[d["time_key"]]["type"] == "date"
                and not d["allow_composition_time"],
                "Clinical-time binding must be protected",
            )
        else:
            require(
                not g.get("time_key") and d.get("time_key") is None,
                "Unexpected mapped time",
            )
        require(
            type(g.get("retain_null_facts", False)) is bool, "Invalid null-fact policy"
        )
        for b in g["fields"]:
            require(
                type(b.get("multiple", False)) is bool, "Invalid repeated-field policy"
            )
            require(
                not b.get("multiple")
                or (b.get("scope") == "block" and not b.get("subject_identity")),
                "Repeated fields must stay inside their block and cannot identify a subject",
            )
            require(
                b.get("missing_policy", "record") in ("record", "omit_optional"),
                "Invalid absent-field policy",
            )
            require(
                b.get("scope") in ("block", "same_entry"), "Declare field ownership"
            )
            require(
                (
                    under(b["path"], g["anchor"])
                    if b["scope"] == "block"
                    else bool(d["entry_path"])
                    and under(b["path"], d["entry_path"])
                    and b.get("inherit") == "ancestor"
                ),
                "Field escapes its clinical scope",
            )
            require(
                "/state/" not in b["path"]
                or under(g["anchor"], d["entry_path"] + "/state"),
                "Parallel state HISTORY requires a separate alignment rule",
            )
            conversion_rules(b.get("conversions", {}), variables[b["key"]])


def retain_interpretation(
    canonical, group, identity_chain, context, quality, resolver=None
):
    """Keep RM meaning independent of optional display fields, within the same instance."""
    from .source import resolve

    d = group["definition"]
    entry_path = d["entry_path"]
    from .source import segments

    depth = len(segments(entry_path))
    matches = (
        [resolver.instance(entry_path, identity_chain[:depth])]
        if resolver
        else [
            node
            for node, chain in resolve(canonical, entry_path)
            if identity_chain[: len(chain)] == chain
        ]
    )
    matches = [n for n in matches if n is not None]

    if len(matches) != 1:
        raise SourceError("The fact must have one owning ENTRY instance")
    entry = matches[0]
    if entry.get("_type") != d["entry_type"]:
        raise SourceError("Declared ENTRY type does not match the source instance")
    anchor = (
        resolver.instance(group["anchor"], identity_chain)
        if resolver
        else next(
            n for n, i in resolve(canonical, group["anchor"]) if i == identity_chain
        )
    )
    allowed = {
        "OBSERVATION": {"POINT_EVENT", "INTERVAL_EVENT", "EVENT", "CLUSTER"},
        "INSTRUCTION": {"INSTRUCTION", "ACTIVITY", "CLUSTER"},
        "ACTION": {"ACTION", "CLUSTER"},
        "EVALUATION": {"EVALUATION", "CLUSTER"},
        "ADMIN_ENTRY": {"ADMIN_ENTRY", "CLUSTER"},
        "COMPOSITION": {"COMPOSITION", "CLUSTER"},
    }
    if (
        not isinstance(anchor, dict)
        or anchor.get("_type") not in allowed[d["entry_type"]]
    ):
        raise SourceError(
            "Fact boundary must be a clinical event, ENTRY or reviewed CLUSTER; scalar nodes are not facts"
        )
    subject = entry.get("subject")
    if (
        d["subject_mode"] == "ehr_owner"
        and isinstance(subject, dict)
        and subject.get("_type") not in (None, "PARTY_SELF")
    ):
        raise SourceError(
            "A related clinical subject requires an explicit identity binding"
        )
    if not d["allow_composition_time"] and context["time_basis"] in (
        "composition_start",
        "source_committed",
    ):
        raise SourceError(
            "Clinical time is missing; composition-time fallback is disabled"
        )
    if d["entry_type"] == "ACTION":
        transition = entry.get("ism_transition", {})
        if not transition.get("current_state"):
            quality["action.current_state"] = {"state": "missing"}
        else:
            context["action_state"] = deepcopy(transition)
    if d["entry_type"] == "INSTRUCTION":
        context["care_intent"] = "planned"
        for node, chain in resolve(canonical, group["anchor"]):
            if chain == identity_chain and node.get("timing"):
                context["planned_timing"] = deepcopy(node["timing"])
    for depth in range(len(identity_chain), 0, -1):
        path = "/" + "/".join(
            a + ("[" + s + "]" if s else "") for a, s, i in identity_chain[:depth]
        )
        for node, chain in (
            [(resolver.instance(path, identity_chain[:depth]), identity_chain[:depth])]
            if resolver
            else resolve(canonical, path)
        ):
            if (
                chain == identity_chain[:depth]
                and isinstance(node, dict)
                and node.get("_type") == "INTERVAL_EVENT"
            ):
                if not node.get("width") or not node.get("math_function"):
                    raise SourceError(
                        "INTERVAL_EVENT requires width and statistical meaning"
                    )
                context["observation_interval"] = {
                    k: deepcopy(node[k])
                    for k in ("width", "math_function", "sample_count")
                    if k in node
                }
                return
