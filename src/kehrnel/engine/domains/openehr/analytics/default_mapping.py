"""Generate inspectable, maximal clinical mapping proposals from exact OPTs.

Compilation happens once per template, never once per composition. Proposals are
local configuration artifacts; generation neither approves nor publishes rules.
"""

from copy import deepcopy
import re
import unicodedata
import xml.etree.ElementTree as ET

from kehrnel.engine.domains.openehr.templates.semantic_catalog import (
    OptSemanticCatalogCompiler,
)
from .model_config import CONTRACT, ENTRY_TYPES, under
from .rules import RuleRevision
from .source import digest, segments
from .metadata_compatibility import policy_from_semantic
from .business_reuse import source_definition
from .source_coverage import inventory_from_compiler


class _StructureCompiler(OptSemanticCatalogCompiler):
    """Reuse the catalog compiler's exact paths, including fixed-name selectors."""

    def __init__(self, xml):
        super().__init__(xml)
        self.frames = {}
        self.slots = []

    def _walk_object(self, node, **kwargs):
        frame = kwargs["frame"]
        scope = kwargs["current_scope"]
        term = scope.terms.get(frame.node_id) if scope else None
        slot = node.get("{http://www.w3.org/2001/XMLSchema-instance}type", "").endswith("ARCHETYPE_SLOT")
        if slot:
            closed = node.findtext("{*}is_closed")
            if closed is not None and closed.strip().lower() not in {"true", "false", "1", "0"}:
                raise ValueError("Invalid slot closure flag in OPT")
            def expressions(tag):
                return [e.findtext("{*}string_expression") or ET.tostring(e, encoding="unicode")
                        for e in node.findall("{*}" + tag)]
            self.slots.append({
                "path": frame.composition_aql_path, "rm_type": frame.rm_type,
                "occurrences": frame.occurrences.model_dump(mode="json"),
                "closed": None if closed is None else closed.strip().lower() in {"true", "1"},
                "include_expressions": expressions("includes"),
                "exclude_expressions": expressions("excludes"),
            })
        self.frames[frame.composition_aql_path] = {
            "slot": slot,
            "path": frame.composition_aql_path,
            "rm_type": frame.rm_type,
            "repeated": (
                frame.occurrences.repeated
                if frame.rm_type == "ELEMENT"
                else frame.repeated
            ),
            "label": term.text if term else None,
        }
        return super()._walk_object(node, **kwargs)


def _slug(text):
    text = (
        unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    )
    text = re.sub("[^a-z0-9]+", "_", text).strip("_")
    if not text or not text[0].isalpha():
        text = "value_" + text
    return text[:55]


def propose_from_opt(xml, *, epoch, effective_at, aliases=None):
    """All clinical ELEMENTs start selected. Unresolved decisions remain explicit.

    `aliases` can be shared by a batch of templates: semantic key -> business key.
    It preserves exact semantic identities; labels alone never merge two fields.
    """
    compiler = _StructureCompiler(xml.encode() if isinstance(xml, str) else xml)
    semantic = compiler.compile()
    frames = compiler.frames
    aliases = aliases if aliases is not None else {}
    reserved = {key: identity for identity, key in aliases.items()}
    archetypes = {a.composition_aql_path: a for a in semantic.archetypes}
    variables, fields, groups, issues = {}, [], {}, []
    pending_protocol = []

    def ancestry(path):
        nodes = [frames[""]] if "" in frames else []
        prefix = ""
        for attribute, selector in segments(path):
            prefix += "/" + attribute + ("[" + selector + "]" if selector else "")
            if prefix in frames:
                nodes.append(frames[prefix])
        return nodes

    def key_for(field):
        identity = field.semantic_key
        if identity in aliases:
            return aliases[identity]
        owner = field.containing_archetype.archetype_id.split(".")[1]
        key = _slug(owner) + "." + _slug(field.label)
        if key in reserved and reserved[key] != identity:
            key += "__" + identity.rsplit(":", 1)[-1][:8]
        aliases[identity] = key
        reserved[key] = identity
        return key

    def group_for(entry, anchor):
        address = anchor["path"]
        if address in groups:
            return groups[address]
        entry_ref = archetypes[entry["path"]]
        label = entry_ref.label or entry_ref.archetype_id.split(".")[1]
        role = "clinical"
        if "/state" in address[len(entry["path"]) :]:
            role = "state"
        if anchor["rm_type"] == "CLUSTER":
            label += " · " + (
                archetypes[address].label
                if address in archetypes
                else anchor.get("label") or "Repeated clinical block"
            )
        identity = entry_ref.archetype_id + ":" + address[len(entry["path"]) :]
        kind = _slug(label)[:55] + "_" + digest(identity)[:8]
        g = {
            "anchor": address,
            "kind": kind,
            "label": label,
            "subject": "patient",
            "role": role,
            "retain_null_facts": True,
            "fields": [],
            "definition": {
                "entry_type": entry["rm_type"],
                "entry_path": entry["path"],
                "subject": "patient",
                "subject_mode": "ehr_owner",
                "time": "nearest_recorded_time",
                "time_key": None,
                "allow_composition_time": True,
                "required_fields": [],
                "missing_required": "reject_composition",
            },
        }
        groups[address] = g
        return g

    for field in semantic.fields:
        path = field.composition_aql_path
        chain = ancestry(path)
        entries = [n for n in chain if n["rm_type"] in ENTRY_TYPES]
        types = sorted({v.rm_type for v in field.value_variants})
        row = {
            "field_id": field.field_id,
            "semantic_key": field.semantic_key,
            "label": field.label,
            "path": path,
            "source_definition": source_definition(path),
            "value_types": types,
            "selected": bool(entries),
            "disposition": "composition_context" if not entries else "projected",
            "group_anchors": [],
        }
        fields.append(row)
        if not entries:
            continue
        key = key_for(field)
        row["business_key"] = key
        variables[key] = {
            "key": key,
            "label": field.label,
            "type": "raw",
            "rm_types": types,
            "semantic_key": field.semantic_key,
            "query_facets": {
                v.rm_type: [q.model_dump(mode="json") for q in v.query_paths]
                for v in field.value_variants
            },
        }
        entry = entries[-1]
        descendants = [
            n
            for n in chain
            if n["path"] != entry["path"] and under(n["path"], entry["path"])
        ]
        binding = {
            "key": key,
            "path": path,
            "field_id": field.field_id,
            "scope": "block",
            "missing_policy": "omit_optional",
        }
        if field.occurrences.repeated:
            binding["multiple"] = True
        # Large binary payloads need an explicit reference/metadata policy, not blind copying.
        if not types or any(t in ("DV_MULTIMEDIA", "DV_ENCAPSULATED") for t in types):
            row["disposition"] = "requires_review"
            issues.append(
                {"code": "VALUE_REPRESENTATION_REVIEW", "field_id": field.field_id}
            )
            continue
        if entry["rm_type"] == "OBSERVATION":
            events = [
                n
                for n in descendants
                if n["rm_type"] in ("EVENT", "POINT_EVENT", "INTERVAL_EVENT")
            ]
            if not events:
                if any(
                    "/protocol" == n["path"][len(entry["path"]) :].split("[")[0]
                    for n in descendants
                ):
                    pending_protocol.append((entry, row, binding, descendants))
                else:
                    row["disposition"] = "requires_review"
                    issues.append(
                        {
                            "code": "OBSERVATION_VALUE_OUTSIDE_EVENT",
                            "field_id": field.field_id,
                        }
                    )
                continue
            anchor = events[-1]
        elif entry["rm_type"] == "INSTRUCTION":
            activities = [n for n in descendants if n["rm_type"] == "ACTIVITY"]
            anchor = activities[-1] if activities else entry
        else:
            anchor = entry
        # Split repeated CLUSTERs as a whole; never flatten their child fields into parallel arrays.
        nested = [
            n
            for n in descendants
            if n["rm_type"] == "CLUSTER"
            and n["repeated"]
            and under(n["path"], anchor["path"])
        ]
        if nested:
            anchor = nested[-1]
        unresolved = [
            n
            for n in descendants
            if n["repeated"]
            and n["path"] not in (path, anchor["path"])
            and under(n["path"], anchor["path"])
        ]
        if unresolved:
            row["disposition"] = "requires_review"
            issues.append(
                {"code": "REPEATED_STRUCTURE_REVIEW", "field_id": field.field_id}
            )
            continue
        group_for(entry, anchor)["fields"].append(binding)
        row["group_anchors"] = [anchor["path"]]

    for entry, row, binding, chain in pending_protocol:
        targets = [
            g
            for g in groups.values()
            if g["definition"]["entry_path"] == entry["path"]
            and g["role"] == "clinical"
        ]
        if not targets or any(n["repeated"] for n in chain):
            row["disposition"] = "requires_review"
            issues.append(
                {"code": "PROTOCOL_CORRELATION_REVIEW", "field_id": row["field_id"]}
            )
            continue
        for g in targets:
            g["fields"].append(
                {**binding, "scope": "same_entry", "inherit": "ancestor"}
            )
            row["group_anchors"].append(g["anchor"])

    template = {
        "template_name": semantic.template.template_id,
        "family": "clinical",
        "phase": "recorded",
        "model_contract": CONTRACT,
        "source_metadata_policy": policy_from_semantic(semantic),
        "source_coverage": inventory_from_compiler(compiler, semantic),
        "groups": list(groups.values()),
        "activity_fields": [],
    }
    rule = RuleRevision.compile(
        {
            "epoch": epoch,
            "effective_at": effective_at,
            "retention": {"mode": "retain"},
            "catalog": {"variables": list(variables.values()), "templates": [template]},
        }
    ).document()
    proposal = {
        "contract": "openehr-default-proposal/v1",
        "status": "draft",
        "default": "all_present_clinical_values",
        "template": semantic.template.model_dump(mode="json"),
        "catalog_digest": semantic.catalog_digest,
        "fields": fields,
        "slots": deepcopy(compiler.slots),
        "issues": issues,
        "candidate_rule": rule,
        "review_required": [
            "business_aliases_and_cross_template_equivalence",
            "subject_identity",
            "clinical_time_and_qualifiers",
            "source_sample_coverage",
        ],
        "notes": [
            "Every clinical ELEMENT is selected in the proposal; unresolved selected fields block finalization.",
            "Candidate rule is a review artifact, not an approved activation.",
            "Copied COMPOSITION metadata on an OBSERVATION is tolerated only at an exact OPT root; the original is preserved and the exception is traced once per composition.",
            "Original typed values are preserved. No guessed units, terminology mappings, or clinical calculations.",
            "OPTIONAL absent fields produce no placeholder or missing-quality document.",
            "ENTRY protocol fields are inherited only from the same instance and only where correlation is unambiguous.",
            "The EHR owner is accepted only when the runtime subject guard permits it; related subjects need an identity binding.",
            "RM interpretation metadata is preserved by the projector; this ELEMENT inventory is not every RM attribute.",
        ],
    }
    proposal["revision"] = digest(proposal)
    return proposal


def finalize_proposal(proposal, *, exclude_field_ids=()):
    """Materialize a reviewed selection; never silently omit selected unresolved fields."""
    p = deepcopy(proposal)
    if p.get("revision") != digest({k: v for k, v in p.items() if k != "revision"}):
        raise ValueError("Default proposal changed; regenerate before finalization")
    known = {f["field_id"] for f in p["fields"]}
    excluded = set(exclude_field_ids)
    if excluded - known:
        raise ValueError("Unknown excluded fields")
    selected = {
        f["field_id"]
        for f in p["fields"]
        if f["selected"] and f["field_id"] not in excluded
    }
    if any(i["field_id"] in selected for i in p["issues"]):
        raise ValueError(
            "Resolve or explicitly exclude the selected fields requiring review"
        )
    value = p["candidate_rule"]
    value.pop("_id", None)
    value.pop("revision", None)
    template = value["catalog"]["templates"][0]
    for group in template["groups"]:
        group["fields"] = [b for b in group["fields"] if b["field_id"] in selected]
    template["groups"] = [g for g in template["groups"] if g["fields"]]
    return RuleRevision.compile(value).document()
