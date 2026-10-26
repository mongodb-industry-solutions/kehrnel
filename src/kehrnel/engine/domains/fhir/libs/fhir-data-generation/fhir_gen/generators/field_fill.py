"""Fill clinically relevant schema fields after core generation and enrichers."""

from __future__ import annotations

import random
from typing import Any

from ..codes.loader import get_system, random_code
from ..resolvers.reference import ReferenceStore
from ..schema.parser import ResourceDef
from .special_types import SpecialTypeGenerator

# Backbone element generators keyed by schema definition name suffix / name.
_BACKBONE_FILLERS: dict[str, Any] = {}


def backbone_filler_for(ref: str) -> Any | None:
    """Return a backbone filler callable for schema type ``ref``, if registered."""
    return _BACKBONE_FILLERS.get(ref)


def _register_backbone(name: str):
    def decorator(fn):
        _BACKBONE_FILLERS[name] = fn
        return fn
    return decorator


@_register_backbone("Patient_Contact")
def _fill_patient_contact(
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    rel = random_code("contact_relationship", rng) or random_code("related_person_relationship", rng)
    contact: dict[str, Any] = {
        "relationship": [t.gen_CodeableConcept(
            system=get_system("contact_relationship") if rel else "http://terminology.hl7.org/CodeSystem/v2-0131",
            code=rel["code"] if rel else "C",
            display=rel["display"] if rel else "Emergency Contact",
        )],
        "name": t.gen_HumanName(use="usual"),
        "telecom": [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")],
        "gender": rng.choice(["male", "female", "other", "unknown"]),
    }
    if rng.random() < 0.6:
        contact["address"] = t.gen_Address()
    if store.has("Organization") and rng.random() < 0.4:
        contact["organization"] = store.get_reference("Organization", rng)
    return contact


@_register_backbone("Patient_Communication")
def _fill_patient_communication(
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    lang = random_code("languages", rng)
    return {
        "language": t.gen_CodeableConcept(
            system=lang.get("system") if lang else get_system("languages"),
            code=lang["code"] if lang else "en",
            display=lang.get("display") if lang else "English",
        ),
        "preferred": True,
    }


@_register_backbone("Patient_Link")
def _fill_patient_link(
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any] | None:
    if not store.has("Patient"):
        return None
    return {
        "type": rng.choice(["replaced-by", "replaces", "refer", "seealso"]),
        "other": store.get_reference("Patient", rng),
    }


@_register_backbone("Encounter_Participant")
def _fill_encounter_participant(
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    part: dict[str, Any] = {
        "type": [t.gen_CodeableConcept(
            system="http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
            code=rng.choice(["ATND", "ADM", "PART", "REF"]),
            display=rng.choice(["attender", "admitter", "Participation", "referrer"]),
        )],
    }
    if store.has("Practitioner"):
        part["actor"] = store.get_reference("Practitioner", rng)
    elif store.has("Patient"):
        part["actor"] = store.get_reference("Patient", rng)
    return part


# Field-name based fillers (resource-level), applied when field absent.
_FIELD_FILLERS: dict[str, Any] = {}


def _register_field(*names: str):
    def decorator(fn):
        for n in names:
            _FIELD_FILLERS[n] = fn
        return fn
    return decorator


@_register_field("contact")
def _fill_contact_field(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    resource_def: ResourceDef,
) -> Any:
    if resource.get("resourceType") == "Patient":
        return [_fill_patient_contact(t, store, rng)]
    if resource.get("resourceType") in ("HealthcareService", "Location"):
        return [t.gen_ExtendedContactDetail()]
    if resource.get("resourceType") == "Device":
        return [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    return [t.gen_ContactDetail()]


@_register_field("telecom")
def _fill_telecom(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    resource_def: ResourceDef,
) -> Any:
    return [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]


@_register_field("address")
def _fill_address(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    resource_def: ResourceDef,
) -> Any:
    return [t.gen_Address()]


@_register_field("identifier")
def _fill_identifier(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    resource_def: ResourceDef,
) -> Any:
    return [t.gen_Identifier()]


@_register_field("name")
def _fill_name(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    resource_def: ResourceDef,
) -> Any:
    rtype = resource.get("resourceType", "")
    if rtype in ("Patient", "Practitioner", "RelatedPerson", "Person"):
        return [t.gen_HumanName(use="official")]
    if rtype == "Organization":
        return t.p.faker.company()
    if rtype == "Location":
        return t.p.faker.company() + " Clinic"
    return t.p.gen_string(
        max_length=80, resource_type=rtype, field_name="name"
    )


@_register_field("active")
def _fill_active(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    resource_def: ResourceDef,
) -> Any:
    return True


@_register_field("gender")
def _fill_gender(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    resource_def: ResourceDef,
) -> Any:
    return rng.choice(["male", "female", "other", "unknown"])


@_register_field("birthDate")
def _fill_birth_date(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    resource_def: ResourceDef,
) -> Any:
    return t.p.gen_date(min_year=1940, max_year=2010)


@_register_field("status")
def _fill_status(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    resource_def: ResourceDef,
) -> Any:
    rtype = resource.get("resourceType", "")
    status_maps = {
        "Encounter": "encounter_status",
        "Observation": "observation_status",
        "Condition": "condition_clinical_status",
        "MedicationRequest": "medication_request_status",
        "Appointment": "appointment_status",
        "Task": "task_status",
        "Claim": "claim_status",
    }
    section = status_maps.get(rtype)
    if section:
        entry = random_code(section, rng)
        if entry:
            return entry["code"]
    return rng.choice(["active", "completed", "final", "draft", "unknown"])


def _generate_for_ref(
    ref: str | None,
    field_name: str,
    is_array: bool,
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    resource: dict[str, Any],
) -> Any:
    rtype = resource.get("resourceType")
    if ref is None:
        # An unresolved schema union is not safely representable as arbitrary
        # text. Leave optional content absent instead of emitting invalid JSON.
        return None

    if ref in t.p.PRIMITIVE_TYPES:
        ctx = {"resource_type": rtype, "field_name": field_name}
        if ref in ("string", "markdown", "xhtml"):
            val = t.p.generate(ref, **ctx)
        else:
            val = t.p.generate(ref)
        return [val] if is_array else val

    if ref == "Reference":
        from .base import _REFERENCE_TARGETS  # noqa: PLC0415

        for candidate in _REFERENCE_TARGETS.get(field_name, []):
            if store.has(candidate):
                got = store.get_reference(candidate, rng)
                if got:
                    return [got] if is_array else got
        return None

    filler = backbone_filler_for(ref)
    if filler:
        val = filler(t, store, rng)
        return [val] if is_array else val

    if hasattr(t, f"gen_{ref}"):
        val = getattr(t, f"gen_{ref}")()
        return [val] if is_array else val

    return None


def fill_schema_gaps(
    resource: dict[str, Any],
    resource_def: ResourceDef,
    types: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    *,
    fill_probability: float = 0.92,
) -> dict[str, Any]:
    """
    Populate high-value optional fields that are often skipped by probability-based generation.
    """
    poly_variants: set[str] = set()
    for keys in resource_def.poly_groups.values():
        poly_variants.update(keys)

    for field_name, field in resource_def.fields.items():
        if field_name in resource or field_name.startswith("_"):
            continue
        if field_name in poly_variants:
            continue
        if field_name in ("extension", "modifierExtension", "text", "contained"):
            continue
        if rng.random() > fill_probability:
            continue

        filler = _FIELD_FILLERS.get(field_name)
        if filler:
            value = filler(resource, types, store, rng, resource_def)
            if value is not None:
                resource[field_name] = value
            continue

        value = _generate_for_ref(
            field.ref,
            field_name,
            field.is_array,
            types,
            store,
            rng,
            resource,
        )
        if value is not None and not (isinstance(value, dict) and not value):
            resource[field_name] = value

    return resource
