from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import json
import math
import re

from .source import SourceError, digest, resolve, segments


def utc(value):
    if isinstance(value, datetime):
        return (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return (
            parsed.replace(tzinfo=timezone.utc)
            if parsed.tzinfo is None
            else parsed.astimezone(timezone.utc)
        )
    except (ValueError, TypeError):
        raise SourceError("A valid event or committed timestamp is required")


def set_business(out, key, value):
    parts = key.split(".")
    for part in parts[:-1]:
        out = out.setdefault(part, {})
    out[parts[-1]] = value


class Catalog:
    def __init__(self, value=None):
        if not isinstance(value, dict):
            raise ValueError("Supply an explicit immutable business catalog")
        self.value = deepcopy(value)
        self.variables = {v["key"]: v for v in self.value["variables"]}
        if len(self.variables) != len(self.value["variables"]):
            raise ValueError("Duplicate business variable key")
        keys = sorted(self.variables)
        if any(b.startswith(a + ".") for a, b in zip(keys, keys[1:])):
            raise ValueError(
                "Business variables cannot overlap scalar and object paths"
            )
        for key, variable in self.variables.items():
            if variable.get("type") not in {
                "raw",
                "measurement",
                "quantity",
                "number",
                "coded",
                "boolean",
                "date",
                "text",
                "identifier",
            }:
                raise ValueError("Unsupported business variable type")
            if not re.fullmatch(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+", key):
                raise ValueError("Invalid business variable key")
            if variable["type"] == "quantity" and not variable.get("unit"):
                raise ValueError("Quantity variables need a canonical unit")
        self.templates = {}
        for template in self.value["templates"]:
            name = template["template_name"]
            if name in self.templates:
                raise ValueError("Duplicate template binding: " + name)
            self.templates[name] = template
            for group in template["groups"]:
                if not isinstance(group.get("kind"), str) or not re.fullmatch(
                    r"[a-z][a-z0-9_]{0,79}", group["kind"]
                ):
                    raise ValueError("Fact types need stable business identifiers")
                segments(group["anchor"])
                if group.get("time_key") and (
                    group["time_key"] not in self.variables
                    or self.variables[group["time_key"]]["type"] != "date"
                    or group["time_key"] not in {b["key"] for b in group["fields"]}
                ):
                    raise ValueError(
                        "Clinical time requires a registered date binding in the same fact group"
                    )
                for binding in group["fields"]:
                    if binding["key"] not in self.variables:
                        raise ValueError("Unregistered business variable")
                    segments(binding["path"])
        for template in self.templates.values():
            for binding in template.get("activity_fields", []):
                if binding["key"] not in self.variables:
                    raise ValueError("Unregistered activity variable")
                segments(binding["path"])
        for template in self.templates.values():
            if template.get("model_contract"):
                from .model_config import validate_stored

                validate_stored(template, self.variables)
        self.revision = digest(self.value)

    def public(self):
        return {
            "revision": self.revision,
            "variables": list(self.variables.values()),
            "templates": list(self.templates.values()),
            "contract": "openehr-business-analytics/v1",
        }

    def selection(self, keys=None):
        keys = list(self.variables) if keys is None else keys
        if (
            not isinstance(keys, list)
            or len(keys) != len(set(keys))
            or any(k not in self.variables for k in keys)
        ):
            raise ValueError("Select unique registered business variables")
        # Subject identity is compulsory even when it was not selected in HDL.
        selected = set(keys) | {
            k for k, v in self.variables.items() if v.get("required")
        }
        for template in self.templates.values():
            for group in template["groups"]:
                if any(
                    b["key"] in selected
                    and not self.variables[b["key"]].get("required")
                    for b in group["fields"]
                ):
                    selected.update(
                        group.get("definition", {}).get("required_fields", [])
                    )
        return selected


def typed(element, variable, scope=None, source_types=None):
    if not isinstance(element, dict):
        raise SourceError("Expected an openEHR ELEMENT")
    if "null_flavour" in element:
        return None, {"state": "null_flavour", "reason": element["null_flavour"]}
    value = element.get("value")
    if not isinstance(value, dict):
        return None, {"state": "missing"}
    rm, kind = value.get("_type"), variable["type"]
    if source_types is not None and rm not in source_types:
        raise SourceError("Source value type is outside the reviewed business reuse binding")
    if kind == "raw":
        return deepcopy(value), {"state": "present", "rm_type": rm}
    if rm not in variable["rm_types"]:
        raise SourceError("Unexpected value type for " + variable["key"])
    if kind == "measurement":
        if rm == "DV_TEXT":
            return {"kind": "text", "text": value.get("value")}, {
                "state": "present",
                "rm_type": rm,
            }
        n = value.get("magnitude", value.get("value"))
        if (
            isinstance(n, bool)
            or not isinstance(n, (int, float))
            or not math.isfinite(n)
        ):
            raise SourceError("Invalid typed measurement")
        unit = value.get("units") if rm == "DV_QUANTITY" else "1"
        if not isinstance(unit, str) or not unit:
            raise SourceError("A typed quantity requires its reported units")
        return {
            "kind": {
                "DV_QUANTITY": "quantity",
                "DV_COUNT": "count",
                "DV_ORDINAL": "ordinal",
            }[rm],
            "value": n,
            "unit": unit,
        }, {"state": "present", "rm_type": rm}
    if kind == "quantity":
        n, unit = value.get("magnitude"), value.get("units")
        conversion = variable.get("text_numeric_conversion")
        if rm == "DV_TEXT" and conversion:
            if (
                conversion.get("kind") != "strict_decimal"
                or conversion.get("decimal_separator") not in (".", ",")
                or not conversion.get("source_unit")
            ):
                raise SourceError(
                    "Text conversion requires a declared parser, decimal separator and source unit"
                )
            text = value.get("value")
            if not isinstance(text, str):
                raise SourceError("Text conversion requires text input")
            text = text.strip()
            separator = re.escape(conversion["decimal_separator"])
            if not re.fullmatch(
                r"[+-]?(?:\d+(?:"
                + separator
                + r"\d*)?|"
                + separator
                + r"\d+)(?:[eE][+-]?\d+)?",
                text,
            ):
                if conversion.get("on_non_numeric", "preserve") == "preserve":
                    return None, {
                        "state": "not_numeric",
                        "source_rm_type": rm,
                        "conversion": "strict_decimal",
                        "source_preserved": True,
                    }
                raise SourceError("Text is not a strict numeric value")
            n = float(text.replace(conversion["decimal_separator"], "."))
            unit = conversion["source_unit"]
        if (
            isinstance(n, bool)
            or not isinstance(n, (int, float))
            or not math.isfinite(n)
        ):
            raise SourceError("Invalid numeric magnitude")
        conversions = variable.get("unit_conversions", {})
        if unit != variable["unit"]:
            if unit not in conversions:
                raise SourceError(
                    "Unapproved unit for " + variable["key"] + ": " + str(unit)
                )
            rule = conversions[unit]
            if isinstance(rule, dict):
                n = n * rule["scale"] + rule.get("offset", 0)
            else:
                n *= rule
        if not math.isfinite(n):
            raise SourceError("Conversion produced a non-finite magnitude")
        return n, {
            "state": "present",
            "unit": variable["unit"],
            "source_unit": unit,
            "rm_type": rm,
            **(
                {"conversion": "strict_decimal"}
                if rm == "DV_TEXT" and conversion
                else {}
            ),
        }
    if kind == "number":
        n = value.get("magnitude", value.get("value"))
        if (
            isinstance(n, bool)
            or not isinstance(n, (int, float))
            or not math.isfinite(n)
        ):
            raise SourceError("Invalid count")
        return n, {"state": "present", "rm_type": rm}
    if kind == "coded":
        from .terminology import project_coding

        try:
            recorded = project_coding(value, scope)
        except ValueError as exc:
            raise SourceError(str(exc)) from None
        return recorded, {"state": "present", "rm_type": rm}
    if kind == "boolean":
        if type(value.get("value")) is not bool:
            raise SourceError("Invalid boolean")
        return value["value"], {"state": "present", "rm_type": rm}
    if kind == "date":
        return utc(value.get("value")), {"state": "present", "rm_type": rm}
    return value.get("id") if rm == "DV_IDENTIFIER" else value.get("value"), {
        "state": "present",
        "rm_type": rm,
    }


def project(
    source,
    canonical,
    catalog,
    decoder_revision,
    selected=None,
    allow_activity_only=False,
    source_fingerprint=None,
    suppress_facts=False,
):
    resolver = DocumentResolver(canonical)
    resolve = resolver.resolve
    name = source.get("template")
    template = catalog.templates.get(name)
    if template and template.get("example_only") and not source.get("synthetic"):
        raise SourceError(
            "Synthetic example bindings cannot project customer clinical records"
        )
    unmapped = template is None
    if unmapped and not allow_activity_only:
        raise SourceError("Template has no approved analytics binding: " + str(name))
    if unmapped:
        template = {"family": "unmapped", "phase": "unknown", "groups": []}
    selected = catalog.selection(selected)
    composition_id, patient_id = str(source["_id"]), str(source["ehr_id"])
    committed = utc(source["creation_date"])
    context = {
        "composition_id": composition_id,
        "patient_id": patient_id,
        "provider_id": source.get("provider_id", "unknown"),
        "provider_role": "source_header",
        "template_name": name,
        "family": template["family"],
        "phase": template.get("phase", "unknown"),
        "event_at": utc(
            canonical.get("context", {}).get("start_time", {}).get("value", committed)
        ),
    }
    context["time_basis"] = (
        "composition_start"
        if canonical.get("context", {}).get("start_time", {}).get("value")
        else "source_committed"
    )
    provider = template.get("provider_binding")
    if provider:
        matches = resolve(canonical, provider["path"])
        if len(matches) == 1:
            code = matches[0][0].get("value", {}).get("defining_code", {})
            system = code.get("terminology_id", {}).get("value")
            if system and code.get("code_string"):
                context["provider_id"] = system + ":" + code["code_string"]
                context["provider_role"] = provider["role"]
    lineage = {
        "template_id": str(source["template_id"]),
        "template_name": name,
        "source_version": source["version"],
        "source_revision": source["revision"],
        "source_digest": source_fingerprint
        or digest({"envelope": source, "canonical": canonical}),
        "binding_revision": catalog.revision,
        "dictionary_revision": decoder_revision,
        "committed_at": committed,
        "dataset": "synthetic" if source.get("synthetic") else "customer",
    }
    facts = []
    for group in (() if suppress_facts else template["groups"]):
        if not any(
            b["key"] in selected and not catalog.variables[b["key"]].get("required")
            for b in group["fields"]
        ):
            continue
        group_selected = selected | set(
            group.get("definition", {}).get("required_fields", [])
        )
        for anchor, identity in resolve(canonical, group["anchor"]):
            clinical, quality, origins = {}, {}, {}
            subject_id = patient_id
            for binding in group["fields"]:
                if binding["key"] not in group_selected:
                    continue
                path = binding["path"]
                relative = (
                    path[len(group["anchor"]) :]
                    if path.startswith(group["anchor"] + "/")
                    else None
                )
                if path == group["anchor"]:
                    found = [(anchor, ())]
                elif relative is not None:
                    found = resolve(anchor, relative)
                elif binding.get("inherit") == "ancestor":
                    # Select only a value in the same structural ancestor instance.
                    field_parts = segments(path)
                    common = 0
                    for (a, s), (ia, isel, idx) in zip(field_parts, identity):
                        if (a, s) != (ia, isel):
                            break
                        common += 1
                    found = resolver.inherited(path, common, identity[:common])
                else:
                    found = resolve(canonical, path)
                if len(found) > 1 and not binding.get("multiple"):
                    raise SourceError(
                        "Binding crosses a repeat: define a finer fact grain for "
                        + binding["key"]
                    )
                key = binding["key"]
                if not found:
                    if binding.get("missing_policy") != "omit_optional":
                        quality[key] = {"state": "missing"}
                    continue
                from .terminology import binding_scope

                variable = catalog.variables[key]
                if "conversions" in binding:
                    variable = {
                        k: v
                        for k, v in variable.items()
                        if k not in ("unit_conversions", "text_numeric_conversion")
                    }
                    variable.update(binding["conversions"])
                scope = binding_scope(path, source["template_id"])

                if binding.get("multiple"):
                    if len({chain[:-1] for _, chain in found}) > 1:
                        raise SourceError(
                            "Repeated field crosses container instances; define a finer fact block"
                        )
                    # A repeated ELEMENT becomes an array of complete values. Paired
                    # fields beneath repeated CLUSTERs require separate block facts.
                    values = [
                        typed(
                            {"value": node} if binding.get("direct_value") else node,
                            variable,
                            scope,
                            binding.get("business_reuse", {}).get("source_rm_types"),
                        )
                        for node, _ in found
                    ]
                    val = [v for v, _ in values]
                    states = [status for _, status in values]
                    status = (
                        {"state": "present"}
                        if all(
                            q.get("state") == "present"
                            and not q.get("conversion")
                            and (
                                not q.get("source_unit")
                                or q.get("source_unit") == q.get("unit")
                            )
                            for q in states
                        )
                        else {"state": "repeated", "items": states}
                    )
                else:
                    val, status = typed(
                        (
                            {"value": found[0][0]}
                            if binding.get("direct_value")
                            else found[0][0]
                        ),
                        variable,
                        scope,
                        binding.get("business_reuse", {}).get("source_rm_types"),
                    )
                quality[key] = status
                origins[key] = {"field_id": binding.get("field_id"), "path": path}
                if val is not None:
                    set_business(clinical, key, val)
                    if binding.get("subject_identity"):
                        if group.get("definition") and (
                            not isinstance(val, str) or not val.strip()
                        ):
                            raise SourceError(
                                "Clinical subject identity must be a non-empty identifier"
                            )
                        subject_id = str(val)
            for key in group.get("definition", {}).get("required_fields", []):
                required_value = clinical
                for part in key.split("."):
                    required_value = (
                        required_value.get(part)
                        if isinstance(required_value, dict)
                        else None
                    )
                if (
                    required_value is None
                    or quality.get(key, {}).get("state") != "present"
                ):
                    raise SourceError(
                        "Required interpretation field is unavailable: " + key
                    )
            if not clinical and not (
                group.get("retain_null_facts")
                and any(q.get("state") == "null_flavour" for q in quality.values())
            ):
                continue
            if group.get("subject") == "newborn" and subject_id == patient_id:
                raise SourceError("Newborn grain requires its own subject identity")
            fact_context = {
                **context,
                "subject_id": subject_id,
                "subject_type": group.get("subject", "patient"),
                "kind": group["kind"],
            }
            # An event timestamp in the anchor is more precise than composition time.
            for ancestor_length in range(len(identity), -1, -1):
                ancestor_path = "/" + "/".join(
                    a + ("[" + s + "]" if s else "")
                    for a, s, i in identity[:ancestor_length]
                )
                event = resolver.instance(ancestor_path, identity[:ancestor_length])
                if not isinstance(event, dict) or not isinstance(
                    event.get("time"), dict
                ):
                    event = None
                if event:
                    fact_context["event_at"] = utc(event["time"]["value"])
                    fact_context["time_basis"] = (
                        "observation_event"
                        if any(a == "events" for a, s, i in identity[:ancestor_length])
                        else "recorded_entry_time"
                    )
                    break
            if group.get("time_key"):
                parts = group["time_key"].split(".")
                event_value = clinical
                for part in parts:
                    event_value = (
                        event_value.get(part) if isinstance(event_value, dict) else None
                    )
                if event_value is None:
                    raise SourceError(
                        "The configured clinical event timestamp is missing"
                    )
                fact_context.update(
                    event_at=utc(event_value),
                    time_basis="mapped_clinical_time",
                    time_key=group["time_key"],
                )
            if group.get("definition"):
                from .model_config import retain_interpretation

                retain_interpretation(
                    canonical, group, identity, fact_context, quality, resolver
                )
            occurrence = digest({"identity": identity, "kind": group["kind"]})[:24]
            facts.append(
                {
                    "_id": composition_id + ":" + occurrence,
                    "context": fact_context,
                    "clinical": clinical,
                    "quality": quality,
                    "lineage": {
                        **lineage,
                        "occurrence_id": occurrence,
                        "anchor_path": group["anchor"],
                        "instance_chain": [i for a, s, i in identity],
                        "fields": origins,
                    },
                }
            )
    business, activity_quality = {}, {}
    for binding in template.get("activity_fields", []):
        if binding["key"] not in selected:
            continue
        matches = resolve(canonical, binding["path"])
        if len(matches) > 1:
            raise SourceError("Composition metadata crosses a repeat")
        if not matches:
            activity_quality[binding["key"]] = {"state": "missing"}
            continue
        from .terminology import binding_scope

        val, status = typed(
            {"value": matches[0][0]} if binding.get("direct_value") else matches[0][0],
            catalog.variables[binding["key"]],
            binding_scope(binding["path"], source["template_id"]),
            binding.get("business_reuse", {}).get("source_rm_types"),
        )
        activity_quality[binding["key"]] = status
        if val is not None:
            set_business(business, binding["key"], val)
    return {
        "_id": composition_id,
        "context": context,
        "business": business,
        "quality": activity_quality,
        "lineage": {
            **lineage,
            "clinical_mapping": (
                "activity_only"
                if unmapped
                else template.get("binding_status", "opt_bound")
            ),
        },
        "facts": facts,
    }


class DocumentResolver:
    """Per-document cache: bounded by this composition and its compiled bindings."""

    def __init__(self, root):
        self.root = root
        self.cache = {}
        self.instances = {}
        self.prefixes = {}

    def resolve(self, root, path):
        key = (id(root), path)
        if key not in self.cache:
            self.cache[key] = resolve(root, path)
        return self.cache[key]

    def instance(self, path, chain):
        if path not in self.instances:
            self.instances[path] = {i: v for v, i in self.resolve(self.root, path)}
        return self.instances[path].get(chain)

    def inherited(self, path, depth, chain):
        key = (path, depth)
        if key not in self.prefixes:
            index = {}
            for value, identity in self.resolve(self.root, path):
                index.setdefault(identity[:depth], []).append((value, identity))
            self.prefixes[key] = index
        return self.prefixes[key].get(chain, [])
