"""Parse FHIR R5 JSON Schema (fhir.schema.v5.json) into structured definitions."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

# Complex datatype suffixes on polymorphic FHIR elements (valueQuantity, etc.)
_COMPLEX_TYPE_SUFFIXES: tuple[str, ...] = (
    "CodeableConcept",
    "Coding",
    "Quantity",
    "SimpleQuantity",
    "Period",
    "Range",
    "Ratio",
    "SampledData",
    "Attachment",
    "Reference",
    "Identifier",
    "HumanName",
    "Address",
    "ContactPoint",
)


@dataclass
class FieldDef:
    name: str
    ref: str | None
    is_array: bool
    is_required: bool
    description: str
    is_primitive: bool
    const_value: str | None = None


@dataclass
class ResourceDef:
    name: str
    description: str
    fields: dict[str, FieldDef]
    required: list[str]
    is_resource: bool
    poly_groups: dict[str, list[str]] = field(default_factory=dict)


class FHIRSchemaParser:
    PRIMITIVES = frozenset({
        "base64Binary", "boolean", "canonical", "code", "date", "dateTime",
        "decimal", "id", "instant", "integer", "integer64", "markdown", "oid",
        "positiveInt", "string", "time", "unsignedInt", "uri", "url", "uuid",
        "xhtml",
    })

    def __init__(self, schema_path: Path):
        self._schema_path = schema_path
        with open(schema_path, encoding="utf-8") as f:
            self._raw = json.load(f)
        self._defs: dict = self._raw["definitions"]

    @property
    def raw_schema(self) -> dict:
        """Loaded JSON Schema used by generation and conformance checks."""
        return self._raw

    def parse_all(self) -> dict[str, ResourceDef]:
        return {name: self.parse_definition(name) for name in self._defs}

    def parse_definition(self, name: str) -> ResourceDef:
        if name not in self._defs:
            raise KeyError(f"Unknown definition: {name}")

        raw = self._defs[name]
        properties: dict = raw.get("properties", {})
        required_list: list[str] = list(raw.get("required", []))
        description: str = raw.get("description", "")

        fields: dict[str, FieldDef] = {}
        for fname, prop in properties.items():
            ref = self._extract_ref(prop, field_name=fname)
            is_array = prop.get("type") == "array" or "items" in prop
            const_value = prop.get("const")
            if const_value is not None:
                const_value = str(const_value)

            fields[fname] = FieldDef(
                name=fname,
                ref=ref,
                is_array=is_array,
                is_required=fname in required_list,
                description=prop.get("description", ""),
                is_primitive=ref in self.PRIMITIVES if ref else False,
                const_value=const_value,
            )

        resource_type_prop = properties.get("resourceType", {})
        is_resource = resource_type_prop.get("const") is not None

        return ResourceDef(
            name=name,
            description=description,
            fields=fields,
            required=required_list,
            is_resource=is_resource,
            poly_groups=self._find_poly_groups(fields),
        )

    def _ref_name(self, ref: str) -> str | None:
        if ref.startswith("#/definitions/"):
            return ref.rsplit("/", 1)[-1]
        return None

    def _infer_inline_primitive(
        self, prop: dict, field_name: str | None
    ) -> str | None:
        """Recover FHIR primitive semantics from inlined JSON Schema fields.

        The HL7 schemas inline many primitive constraints instead of retaining a
        ``$ref`` (for example ``effectiveInstant`` becomes ``type: string`` plus
        the instant regex). Treating those values as ordinary strings generated
        clinical prose that could never validate against the same schema.
        """
        inline_type = prop.get("type")
        if inline_type == "integer":
            minimum = prop.get("minimum")
            if minimum == 1:
                return "positiveInt"
            if minimum == 0:
                return "unsignedInt"
            return "integer"
        if inline_type == "number":
            return "decimal"
        if inline_type != "string":
            return inline_type if inline_type in self.PRIMITIVES else None

        name = field_name or ""
        exact_names = {
            "id": "id",
            "birthDate": "date",
            "lastUpdated": "instant",
        }
        if name in exact_names:
            return exact_names[name]
        suffixes = (
            ("Base64Binary", "base64Binary"),
            ("DateTime", "dateTime"),
            ("Canonical", "canonical"),
            ("PositiveInt", "positiveInt"),
            ("UnsignedInt", "unsignedInt"),
            ("Integer64", "integer64"),
            ("Instant", "instant"),
            ("Markdown", "markdown"),
            ("Boolean", "boolean"),
            ("Decimal", "decimal"),
            ("Integer", "integer"),
            ("Date", "date"),
            ("Time", "time"),
            ("Canonical", "canonical"),
            ("Uri", "uri"),
            ("Url", "url"),
            ("Uuid", "uuid"),
            ("Oid", "oid"),
            ("Code", "code"),
            ("Id", "id"),
        )
        for suffix, primitive in suffixes:
            if name.endswith(suffix):
                return primitive

        pattern = prop.get("pattern")
        if pattern:
            for primitive in ("instant", "dateTime", "date", "time", "id", "oid", "uuid"):
                if pattern == (self._defs.get(primitive) or {}).get("pattern"):
                    return primitive
            # FHIR code/uri/canonical fields commonly share the no-whitespace
            # regex. ``code`` is the safest conforming scalar when the field name
            # does not preserve the original primitive type.
            if pattern == r"^\S*$":
                return "code"
        return "string"

    def _extract_ref(
        self, prop: dict, field_name: str | None = None
    ) -> str | None:
        if not isinstance(prop, dict):
            return None
        if "$ref" in prop:
            return self._ref_name(prop["$ref"])
        inline_type = prop.get("type")
        if isinstance(inline_type, str):
            inferred = self._infer_inline_primitive(prop, field_name)
            if inferred:
                return inferred
        if inline_type == "array" and isinstance(prop.get("items"), dict):
            return self._extract_ref(prop["items"], field_name=field_name)
        if "allOf" in prop:
            for item in prop["allOf"]:
                ref = self._extract_ref(item, field_name=field_name)
                if ref:
                    return ref
        return None

    def _pascal_type_suffixes(self) -> tuple[str, ...]:
        """PascalCase suffix tokens for FHIR choice elements (longest first)."""
        names: set[str] = set(_COMPLEX_TYPE_SUFFIXES)
        for primitive in self.PRIMITIVES:
            if not primitive:
                continue
            names.add(primitive[0].upper() + primitive[1:])
        return tuple(sorted(names, key=len, reverse=True))

    def _split_type_suffix_field(self, fname: str) -> tuple[str, str] | None:
        """Split ``versionAlgorithmString`` -> (``versionAlgorithm``, ``String``)."""
        for suffix in self._pascal_type_suffixes():
            if fname.endswith(suffix) and len(fname) > len(suffix):
                base = fname[: -len(suffix)]
                if base and base[0].islower():
                    return base, suffix
        return None

    def _find_poly_groups(self, fields: dict[str, FieldDef]) -> dict[str, list[str]]:
        """Group polymorphic fields (e.g. valueQuantity, valueString -> value)."""
        buckets: dict[str, list[str]] = {}

        for fname in fields:
            if fname.startswith("_"):
                continue
            split = self._split_type_suffix_field(fname)
            if not split:
                continue
            prefix, _suffix = split
            buckets.setdefault(prefix, []).append(fname)

        return {k: sorted(v) for k, v in buckets.items() if len(v) > 1}

    @lru_cache(maxsize=1)
    def get_all_resources(self) -> tuple[str, ...]:
        resources: list[str] = []
        for name, raw in self._defs.items():
            rt = raw.get("properties", {}).get("resourceType", {})
            if isinstance(rt, dict) and rt.get("const") is not None:
                resources.append(name)
        return tuple(sorted(resources))

    def get_references_for(self, resource_name: str) -> list[str]:
        """Resource types referenced via Reference fields (from descriptions + names)."""
        res_def = self.parse_definition(resource_name)
        all_resources = set(self.get_all_resources())
        found: set[str] = set()

        field_targets = {
            "subject": {"Patient", "Group", "Device", "Location"},
            "patient": {"Patient"},
            "encounter": {"Encounter"},
            "performer": {"Practitioner", "PractitionerRole", "Organization", "CareTeam"},
            "requester": {"Practitioner", "PractitionerRole", "Organization"},
            "recorder": {"Practitioner", "PractitionerRole"},
            "organization": {"Organization"},
            "location": {"Location"},
            "medication": {"Medication"},
            "specimen": {"Specimen"},
            "device": {"Device"},
            "basedOn": {"CarePlan", "ServiceRequest"},
        }

        for fname, fdef in res_def.fields.items():
            if fdef.ref != "Reference":
                continue
            if fname in field_targets:
                found.update(field_targets[fname] & all_resources)
                continue
            desc = fdef.description or ""
            for rname in all_resources:
                if rname in desc:
                    found.add(rname)

        return sorted(found)
