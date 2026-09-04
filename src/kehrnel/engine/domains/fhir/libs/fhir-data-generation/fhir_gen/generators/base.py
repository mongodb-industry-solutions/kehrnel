"""Schema-driven FHIR R5 resource generator engine."""

from __future__ import annotations

import random
from collections import Counter
from pathlib import Path
from typing import Any

from ..resolvers.dependency import CORE_DEPENDENCIES, resolve_order
from ..resolvers.reference import ReferenceStore
from ..schema.field_validation import is_empty
from ..schema.parser import FieldDef, ResourceDef
from ..schema.registry import SchemaRegistry
from .primitives import PrimitiveGenerator
from .scenarios import prepare_scenario_deps, scenario_by_id, scenario_catalog, scenario_for_index
from .special_types import SpecialTypeGenerator

_REFERENCE_TARGETS: dict[str, list[str]] = {
    "subject": ["Patient", "Group"],
    "patient": ["Patient"],
    "performer": ["Practitioner", "PractitionerRole", "Organization"],
    "author": ["Practitioner", "PractitionerRole"],
    "requester": ["Practitioner", "PractitionerRole"],
    "recorder": ["Practitioner", "PractitionerRole"],
    "asserter": ["Practitioner", "PractitionerRole"],
    "encounter": ["Encounter"],
    "organization": ["Organization"],
    "managingOrganization": ["Organization"],
    "location": ["Location"],
    "medication": ["Medication"],
    "careTeam": ["CareTeam"],
    "coverage": ["Coverage"],
    "insurer": ["Organization"],
    "provider": ["Practitioner", "PractitionerRole", "Organization"],
    "basedOn": ["ServiceRequest", "CarePlan"],
    "partOf": ["Procedure", "Observation"],
    "hasMember": ["Observation"],
    "derivedFrom": ["Observation", "QuestionnaireResponse"],
    "specimen": ["Specimen"],
    "device": ["Device"],
    "goal": ["Goal"],
    "condition": ["Condition"],
    "focus": ["Condition", "Observation"],
    "reasonReference": ["Condition", "Observation"],
    "valueReference": ["Patient", "Practitioner", "Organization", "Observation", "Encounter"],
    "target": ["Patient", "Organization", "Location", "Practitioner"],
    "source": ["Patient", "Practitioner", "Organization", "Device"],
    "appointment": ["Appointment"],
    "subscription": ["Subscription"],
    "request": ["CoverageEligibilityRequest", "ServiceRequest", "CommunicationRequest"],
    "insurer": ["Organization"],
    "recipient": ["Organization", "Practitioner", "Patient"],
    "prescriber": ["Practitioner", "PractitionerRole"],
    "requestor": ["Practitioner", "PractitionerRole", "Patient"],
    "immunizationEvent": ["Immunization"],
    "immunization": ["Immunization"],
    "product": ["Medication", "Device", "BiologicallyDerivedProduct", "Substance"],
    "currentLocation": ["Location"],
    "requestedLocation": ["Location"],
    "controller": ["Organization", "Patient", "Practitioner"],
    "observation": ["Observation"],
    "manipulated": ["Device"],
    "link": ["DocumentReference", "DiagnosticReport"],
    "reference": ["Observation", "DocumentReference", "DiagnosticReport", "Patient"],
}


class ResourceGenerator:
    """
    Generic FHIR R5 resource generator.
    Generates any resource from the schema, handling field types,
    polymorphism, and inter-resource references.
    """

    SKIP_FIELDS = frozenset({
        "resourceType", "id", "meta", "implicitRules", "language",
        "text", "contained", "extension", "modifierExtension",
    })

    OPTIONAL_FIELD_PROB = 0.88
    BACKBONE_FIELD_PROB = 0.85
    MAX_BACKBONE_DEPTH = 8

    def __init__(
        self,
        seed: int | None = None,
        store: ReferenceStore | None = None,
        *,
        schema_path: str | Path | None = None,
        schema_version: str | None = None,
    ) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        self._prim = PrimitiveGenerator(seed=seed)
        self._types = SpecialTypeGenerator(self._prim)
        self._store = store or ReferenceStore()
        if schema_path or schema_version:
            from ..schema.versions import resolve_schema_path

            resolved = resolve_schema_path(
                schema_version=schema_version,
                schema_path=Path(schema_path) if schema_path else None,
            )
            self._registry = SchemaRegistry(resolved)
        else:
            self._registry = SchemaRegistry.get()
        self._types.schema_registry = self._registry
        self._conformance_resources = 0
        self._conformance_removals: Counter[str] = Counter()

    @property
    def store(self) -> ReferenceStore:
        return self._store

    @property
    def schema_registry(self) -> SchemaRegistry:
        return self._registry

    def conformance_report(self) -> dict[str, Any]:
        """Evidence from the schema guard applied to every generated resource."""
        return {
            "passed": True,
            "resources_checked": self._conformance_resources,
            "optional_values_removed": sum(self._conformance_removals.values()),
            "removals_by_path": dict(self._conformance_removals.most_common(50)),
            "policy": "remove-invalid-optional-content;never-invent-required-content",
        }

    def generate(
        self,
        resource_type: str,
        count: int = 1,
        overrides: dict[str, Any] | None = None,
        schema_path: str | None = None,
        schema_version: str | None = None,
    ) -> list[dict[str, Any]]:
        """Generate `count` instances of `resource_type` with dependencies pre-generated."""
        if schema_path or schema_version:
            from ..schema.versions import resolve_schema_path

            self._registry = SchemaRegistry(
                resolve_schema_path(
                    schema_version=schema_version,
                    schema_path=Path(schema_path) if schema_path else None,
                )
            )
            self._types.schema_registry = self._registry

        known = set(self._registry.all_resources())
        for dep in resolve_order([resource_type], self._registry):
            if dep != resource_type and dep in known and not self._store.has(dep):
                self._generate_one(dep)

        results: list[dict[str, Any]] = []
        for i in range(count):
            scenario_entry = scenario_for_index(
                resource_type, i, schema_registry=self._registry
            )
            results.append(
                self._generate_one(
                    resource_type,
                    overrides=overrides,
                    forced_poly=scenario_entry.forced_poly if scenario_entry else None,
                    scenario=scenario_entry.id if scenario_entry else None,
                )
            )
        return results

    def generate_scenario(
        self,
        resource_type: str,
        scenario_id: str,
        *,
        register: bool = True,
        overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Generate a single resource for an explicit named scenario.

        Use this when you need a specific variant (e.g. Patient ``deceased_datetime``)
        rather than the first catalog entry used by ``generate(..., count=1)``.
        """
        entry = scenario_by_id(
            resource_type,
            scenario_id,
            include_poly_variants=True,
            schema_registry=self._registry,
        )
        if entry is None:
            known = [
                s.id
                for s in scenario_catalog(
                    resource_type,
                    include_poly_variants=True,
                    schema_registry=self._registry,
                )
            ]
            raise ValueError(
                f"Unknown scenario {scenario_id!r} for {resource_type}. "
                f"Known scenarios: {known}"
            )

        known = set(self._registry.all_resources())
        for dep in resolve_order([resource_type], self._registry):
            if dep != resource_type and dep in known and not self._store.has(dep):
                self._generate_one(dep)

        prepare_scenario_deps(self, resource_type, entry)
        return self._generate_one(
            resource_type,
            overrides=overrides,
            forced_poly=entry.forced_poly or None,
            scenario=entry.id,
            register=register,
        )

    def generate_many(
        self,
        resource_types: list[str],
        counts: dict[str, int] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Generate multiple resource types in dependency order."""
        counts = counts or {}
        requested = set(resource_types)
        known = set(self._registry.all_resources())
        all_needed: set[str] = {rt for rt in resource_types if rt in known}
        for rt in resource_types:
            for dep in CORE_DEPENDENCIES.get(rt, []):
                if dep in known:
                    all_needed.add(dep)

        results: dict[str, list[dict[str, Any]]] = {}
        for rt in resolve_order(list(all_needed), self._registry):
            n = counts.get(rt, 1)
            generated = [self._generate_one(rt) for _ in range(n)]
            if rt in requested:
                results[rt] = generated
        return results

    def generate_variants(
        self,
        resource_type: str,
        variant_fields: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Emit one document per polymorphic choice variant (INSTRUCTIONS #7).
        Shared dependencies are generated once.
        """
        resource_def = self._registry.definition(resource_type)
        groups = resource_def.poly_groups
        if variant_fields:
            groups = {k: v for k, v in groups.items() if k in variant_fields}
        if not groups:
            return self.generate(resource_type, count=1)

        known = set(self._registry.all_resources())
        for dep in resolve_order([resource_type], self._registry):
            if dep != resource_type and dep in known and not self._store.has(dep):
                self._generate_one(dep)

        variants: list[dict[str, Any]] = []
        for _base, keys in groups.items():
            for key in keys:
                resource = self._generate_one(
                    resource_type,
                    forced_poly={_base: key},
                    register=False,
                    enrich=False,
                    enforce_conformance=False,
                )
                for sib in keys:
                    if sib != key:
                        resource.pop(sib, None)
                field = resource_def.fields.get(key)
                if field:
                    if self._is_effectively_empty(resource.get(key)):
                        resource.pop(key, None)
                    if key not in resource:
                        value = self._generate_field(
                            field, resource_type, stack=(), force=True,
                        )
                        if value is not None and not self._is_effectively_empty(value):
                            resource[key] = value
                if not self._is_effectively_empty(resource.get(key)):
                    resource = self._conform_generated_resource(
                        resource, resource_type
                    )
                    if self._is_effectively_empty(resource.get(key)):
                        continue
                    self._store.register(resource)
                    variants.append(resource)
        return variants

    def generate_scenarios(
        self,
        resource_type: str,
        *,
        register: bool = True,
        include_poly_variants: bool = True,
        named_only: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Generate one resource per scenario (named lifecycle + schema choice variants).

        For resources without any scenarios, falls back to ``generate_variants``.
        """
        catalog = scenario_catalog(
            resource_type,
            include_poly_variants=include_poly_variants and not named_only,
            schema_registry=self._registry,
        )
        if not catalog:
            variants = self.generate_variants(resource_type)
            return variants if variants else self.generate(resource_type, count=1)

        known = set(self._registry.all_resources())
        for dep in resolve_order([resource_type], self._registry):
            if dep != resource_type and dep in known and not self._store.has(dep):
                self._generate_one(dep)

        results: list[dict[str, Any]] = []
        for entry in catalog:
            prepare_scenario_deps(self, resource_type, entry)
            results.append(
                self._generate_one(
                    resource_type,
                    forced_poly=entry.forced_poly or None,
                    register=register,
                    scenario=entry.id,
                )
            )
        return results

    def _new_id(self) -> str:
        return self._prim.gen_id()

    def _prune_empty_array_elements(self, node: Any) -> Any:
        if isinstance(node, dict):
            return {
                key: self._prune_empty_array_elements(value)
                for key, value in node.items()
            }
        if isinstance(node, list):
            cleaned = [self._prune_empty_array_elements(item) for item in node]
            return [
                item for item in cleaned
                if not self._is_effectively_empty(item)
            ]
        return node

    @staticmethod
    def _is_effectively_empty(value: Any) -> bool:
        if is_empty(value):
            return True
        if isinstance(value, list):
            return not value or all(ResourceGenerator._is_effectively_empty(item) for item in value)
        return False

    def _poly_variant_fields(self, resource_def: ResourceDef) -> set[str]:
        fields: set[str] = set()
        for keys in resource_def.poly_groups.values():
            fields.update(keys)
        return fields

    @staticmethod
    def _enforce_poly_exclusivity(
        resource: dict[str, Any],
        resource_def: ResourceDef,
        forced_poly: dict[str, str] | None,
    ) -> None:
        """Keep one concrete property for every FHIR choice element."""
        for base, variants in resource_def.poly_groups.items():
            present = [key for key in resource if key in variants]
            if len(present) <= 1:
                continue
            forced = (forced_poly or {}).get(base)
            chosen = forced if forced in present else present[-1]
            for key in present:
                if key != chosen:
                    resource.pop(key, None)

    def _generate_one(
        self,
        resource_type: str,
        overrides: dict[str, Any] | None = None,
        forced_poly: dict[str, str] | None = None,
        register: bool = True,
        enrich: bool = True,
        scenario: str | None = None,
        enforce_conformance: bool = True,
    ) -> dict[str, Any]:
        resource_def = self._registry.definition(resource_type)
        poly_variants = self._poly_variant_fields(resource_def)

        resource: dict[str, Any] = {
            "resourceType": resource_type,
            "id": self._new_id(),
            "meta": self._types.gen_Meta(),
        }

        for field_name in resource_def.required:
            if field_name in self.SKIP_FIELDS or field_name in poly_variants:
                continue
            field = resource_def.fields.get(field_name)
            if field:
                value = self._generate_field(field, resource_type, stack=(), force=True)
                if value is not None and not self._is_effectively_empty(value):
                    resource[field_name] = value

        for field_name in resource_def.required:
            if field_name in self.SKIP_FIELDS or field_name in poly_variants:
                continue
            if not self._is_effectively_empty(resource.get(field_name)):
                continue
            field = resource_def.fields.get(field_name)
            if field:
                value = self._generate_field(field, resource_type, stack=(), force=True)
                if value is not None and not self._is_effectively_empty(value):
                    resource[field_name] = value

        for field_name, field in resource_def.fields.items():
            if (
                field_name in self.SKIP_FIELDS
                or field_name in resource
                or field_name in poly_variants
                or field_name.startswith("_")
            ):
                continue
            if self.rng.random() > self.OPTIONAL_FIELD_PROB:
                continue
            value = self._generate_field(field, resource_type, stack=())
            if value is not None:
                resource[field_name] = value

        for base, variants in resource_def.poly_groups.items():
            if forced_poly and base in forced_poly:
                chosen = forced_poly[base]
            else:
                existing = [v for v in variants if v in resource]
                if existing:
                    chosen = existing[0]
                    for v in variants:
                        if v != chosen:
                            resource.pop(v, None)
                    continue
                chosen = self.rng.choice(variants)

            for v in variants:
                if v != chosen:
                    resource.pop(v, None)
            field = resource_def.fields.get(chosen)
            if field and chosen not in resource:
                val = self._generate_field(field, resource_type, stack=())
                if val is not None:
                    resource[chosen] = val

        if enrich:
            resource = self._enrich(resource, resource_type)
            from .field_fill import fill_schema_gaps

            resource = fill_schema_gaps(
                resource,
                resource_def,
                self._types,
                self._store,
                self.rng,
            )
            from .scenarios import apply_scenario

            resource = apply_scenario(
                resource,
                resource_type,
                scenario,
                self._types,
                self._store,
                self.rng,
            )
        self._enforce_poly_exclusivity(resource, resource_def, forced_poly)
        resource = self._store.fill_missing_references(resource, self.rng)
        resource = self._store.repair_resource(resource, self.rng)
        resource = self._ensure_required_fields(resource, resource_def, resource_type)
        resource = self._prune_empty_array_elements(resource)

        from .canonical_resource import normalize_canonical_resource

        resource = normalize_canonical_resource(
            resource, self._types, self._store, self.rng
        )

        if overrides:
            resource.update(overrides)

        if enforce_conformance:
            resource = self._conform_generated_resource(resource, resource_type)

        if register:
            self._store.register(resource)
        return resource

    def _conform_generated_resource(
        self, resource: dict[str, Any], resource_type: str
    ) -> dict[str, Any]:
        from ..schema.conformance import conform_resource_to_schema

        conformance = conform_resource_to_schema(resource, self._registry)
        if not conformance["passed"]:
            raise ValueError(
                f"Generated {resource_type} does not conform to the active base schema: "
                f"{conformance['unresolved'][:3]}"
            )
        self._conformance_resources += 1
        self._conformance_removals.update(
            {
                f"{resource_type}.{path}": count
                for path, count in conformance["removals"].items()
            }
        )
        return resource

    def _ensure_required_fields(
        self,
        resource: dict[str, Any],
        resource_def: ResourceDef,
        resource_type: str,
    ) -> dict[str, Any]:
        """Fill any still-missing top-level required fields after reference repair."""
        poly_variants = self._poly_variant_fields(resource_def)
        for field_name in resource_def.required:
            if field_name in self.SKIP_FIELDS or field_name in poly_variants:
                continue
            if not self._is_effectively_empty(resource.get(field_name)):
                continue
            field = resource_def.fields.get(field_name)
            if not field:
                continue
            value = self._generate_field(field, resource_type, stack=(), force=True)
            if value is not None and not self._is_effectively_empty(value):
                resource[field_name] = value
        if (
            "subject" in resource_def.required
            and self._is_effectively_empty(resource.get("subject"))
            and not self._is_effectively_empty(resource.get("patient"))
        ):
            resource["subject"] = resource["patient"]
        return resource

    def _generate_field(
        self,
        field: FieldDef,
        context_resource: str,
        stack: tuple[str, ...] = (),
        depth: int = 0,
        force: bool = False,
    ) -> Any:
        if field.const_value is not None:
            return field.const_value

        ref = field.ref
        if ref is None:
            return None

        if field.is_primitive:
            ctx = {
                "resource_type": context_resource,
                "field_name": field.name,
            }
            if ref in ("string", "markdown", "xhtml"):
                value = self._prim.generate(ref, **ctx)
            else:
                value = self._prim.generate(ref)
            return self._wrap_array(field, value)

        if ref == "Reference":
            return self._generate_reference_field(field)

        if hasattr(self._types, f"gen_{ref}"):
            value = getattr(self._types, f"gen_{ref}")()
            return self._wrap_array(field, value)

        if depth >= self.MAX_BACKBONE_DEPTH or ref in stack:
            return self._wrap_array(field, {}) if field.is_array else {}

        try:
            nested_def = self._registry.definition(ref)
        except KeyError:
            return None

        from .field_fill import backbone_filler_for

        filler = backbone_filler_for(ref)
        if filler:
            nested = filler(self._types, self._store, self.rng)
            if nested is None:
                return None
        else:
            nested = self._generate_backbone(
                nested_def,
                context_resource,
                stack=stack,
                depth=depth + 1,
                force=force,
            )
        if self._is_effectively_empty(nested) and force and hasattr(self._types, f"gen_{ref}"):
            nested = getattr(self._types, f"gen_{ref}")()
        return self._wrap_array(field, nested)

    @staticmethod
    def _wrap_array(field: FieldDef, value: Any) -> Any:
        if field.is_array:
            return [value]
        return value

    def _generate_reference_field(self, field: FieldDef) -> Any:
        candidates = _REFERENCE_TARGETS.get(field.name, [])
        for candidate in candidates:
            if self._store.has(candidate):
                ref = self._store.get_reference(candidate, self.rng)
                if ref:
                    return self._wrap_array(field, ref)
        return None

    def _generate_backbone(
        self,
        nested_def: ResourceDef,
        context: str,
        stack: tuple[str, ...] = (),
        depth: int = 0,
        force: bool = False,
    ) -> dict[str, Any]:
        if depth >= self.MAX_BACKBONE_DEPTH or nested_def.name in stack:
            return {}

        poly_variants = self._poly_variant_fields(nested_def)
        result: dict[str, Any] = {}
        child_stack = stack + (nested_def.name,)

        for fname in nested_def.required:
            if fname.startswith("_") or fname in poly_variants or fname in result:
                continue
            field = nested_def.fields.get(fname)
            if field:
                val = self._generate_field(
                    field, context, stack=child_stack, depth=depth, force=True,
                )
                if val is not None and not self._is_effectively_empty(val):
                    result[fname] = val

        for fname, field in nested_def.fields.items():
            if fname.startswith("_") or fname in poly_variants or fname in result:
                continue
            is_req = fname in nested_def.required
            if not is_req and not force and self.rng.random() > self.BACKBONE_FIELD_PROB:
                continue
            val = self._generate_field(
                field, context, stack=child_stack, depth=depth, force=is_req or force,
            )
            if val is not None and not self._is_effectively_empty(val):
                result[fname] = val

        for _base, variants in nested_def.poly_groups.items():
            chosen = self.rng.choice(variants)
            field = nested_def.fields.get(chosen)
            if field and chosen not in result:
                val = self._generate_field(
                    field, context, stack=child_stack, depth=depth, force=force,
                )
                if val is not None and not self._is_effectively_empty(val):
                    result[chosen] = val

        if any(
            self._is_effectively_empty(result.get(field_name))
            for field_name in nested_def.required
            if not field_name.startswith("_")
        ):
            return {}

        return result

    def _enrich(self, resource: dict[str, Any], resource_type: str) -> dict[str, Any]:
        from .resources import clinical, financial, medication, specialized, workflow

        enrichers = {
            **getattr(clinical, "ENRICHERS", {}),
            **getattr(medication, "ENRICHERS", {}),
            **getattr(workflow, "ENRICHERS", {}),
            **getattr(financial, "ENRICHERS", {}),
            **getattr(specialized, "ENRICHERS", {}),
        }
        enricher = enrichers.get(resource_type)
        if enricher:
            return enricher(resource, self._types, self._store, self.rng)
        return resource
