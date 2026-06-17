"""Terminology validation — YAML CodeSystems, HL7 URLs, and generated resource codings."""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any

import pytest

from fhir_gen import ResourceGenerator
from fhir_gen.schema.registry import SchemaRegistry
from fhir_gen.codes.loader import get_codes, get_system, list_sections, reload_codes
from fhir_gen.codes.validation import (
    CONDITION_CLINICAL_STATUS_SYSTEM,
    CONDITION_VERIFICATION_STATUS_SYSTEM,
    canonical_systems_from_builder,
    clear_terminology_cache,
    terminology_index,
    validate_all_yaml_sections,
    validate_coding,
    validate_resource_codings,
    validate_system_url,
    validate_yaml_section,
)
from fhir_gen.generators.resources import ENRICHERS

CLINICAL_STATUS_CODES = frozenset({
    "active", "recurrence", "relapse", "inactive", "remission", "resolved",
})
VERIFICATION_STATUS_CODES = frozenset({
    "unconfirmed", "provisional", "differential", "confirmed", "refuted", "entered-in-error",
})

_V5_SCHEMA = Path(__file__).resolve().parent.parent / "fhir_gen" / "schema" / "fhir.schema.v5.json"


@pytest.fixture(autouse=True)
def fresh_terminology():
    clear_terminology_cache()
    yield
    clear_terminology_cache()


@pytest.fixture(autouse=True)
def _v5_schema_registry():
    """Terminology tests target R5 enrichers; reset after v6 field-validation tests."""
    SchemaRegistry.reload(_V5_SCHEMA)
    yield
    SchemaRegistry.reload(_V5_SCHEMA)


@pytest.fixture
def gen() -> ResourceGenerator:
    SchemaRegistry.reload(_V5_SCHEMA)
    return ResourceGenerator(seed=42)


def _coding_systems(resource: dict[str, Any]) -> set[str]:
    systems: set[str] = set()
    for _, coding in _iter_codings_flat(resource):
        if coding.get("system"):
            systems.add(coding["system"])
    return systems


def _iter_codings_flat(resource: dict[str, Any]):
    from fhir_gen.codes.validation import iter_codings

    yield from iter_codings(resource)


# --- YAML catalog -------------------------------------------------------------


@pytest.mark.parametrize("section", list_sections())
def test_yaml_section_valid(section: str):
    errors = validate_yaml_section(section)
    assert not errors, f"{section}: {errors}"


def test_all_yaml_sections_pass_validation():
    failures = {
        section: errs
        for section, errs in validate_all_yaml_sections().items()
        if errs
    }
    assert not failures, f"YAML sections with errors: {list(failures.keys())[:5]} ..."


@pytest.mark.parametrize("section", sorted(canonical_systems_from_builder().keys()))
def test_yaml_section_matches_builder_canonical_system(section: str):
    expected = canonical_systems_from_builder()[section]
    actual = get_system(section)
    if actual is None:
        pytest.skip(f"section {section} not in loaded YAML")
    assert actual == expected


def test_condition_verification_status_canonical_url():
    assert get_system("condition_verification_status") == CONDITION_VERIFICATION_STATUS_SYSTEM


def test_condition_clinical_status_canonical_url():
    assert get_system("condition_clinical_status") == CONDITION_CLINICAL_STATUS_SYSTEM


def test_condition_verification_codes_match_hl7():
    codes = {c["code"] for c in get_codes("condition_verification_status")}
    assert codes == VERIFICATION_STATUS_CODES


def test_condition_clinical_codes_match_hl7():
    codes = {c["code"] for c in get_codes("condition_clinical_status")}
    assert codes == CLINICAL_STATUS_CODES


def test_terminology_index_includes_condition_systems():
    index = terminology_index()
    assert CONDITION_VERIFICATION_STATUS_SYSTEM in index
    assert VERIFICATION_STATUS_CODES <= index[CONDITION_VERIFICATION_STATUS_SYSTEM]
    assert CONDITION_CLINICAL_STATUS_SYSTEM in index
    assert CLINICAL_STATUS_CODES <= index[CONDITION_CLINICAL_STATUS_SYSTEM]


def test_rejects_valueset_url_as_system():
    errors = validate_system_url("http://hl7.org/fhir/ValueSet/condition-ver-status")
    assert any("ValueSet" in e for e in errors)


def test_rejects_relative_codesystem_path():
    errors = validate_system_url("CodeSystem/condition-ver-status")
    assert any("absolute" in e for e in errors)


# --- Generated enriched resources ---------------------------------------------


@pytest.mark.parametrize("resource_type", sorted(ENRICHERS.keys()))
def test_enriched_resource_codings_valid(resource_type: str, gen: ResourceGenerator):
    for dep in ("Patient", "Practitioner", "Organization", "Location", "Encounter"):
        if dep != resource_type:
            try:
                gen.generate(dep, count=1)
            except Exception:
                pass
    resources = gen.generate(resource_type, count=3)
    for resource in resources:
        errors = validate_resource_codings(resource, strict_registered=True)
        assert not errors, f"{resource_type}: {errors[:5]}"


def test_condition_verification_status_on_generated_condition(gen: ResourceGenerator):
    gen.generate("Patient", count=1)
    condition = gen.generate("Condition", count=1)[0]
    vs = condition.get("verificationStatus", {})
    codings = vs.get("coding", [])
    assert codings, "verificationStatus.coding required"
    coding = codings[0]
    assert coding["system"] == CONDITION_VERIFICATION_STATUS_SYSTEM
    assert coding["code"] in VERIFICATION_STATUS_CODES
    assert not validate_coding(coding)


def test_condition_clinical_status_on_generated_condition(gen: ResourceGenerator):
    gen.generate("Patient", count=1)
    condition = gen.generate("Condition", count=1)[0]
    cs = condition.get("clinicalStatus", {})
    coding = cs["coding"][0]
    assert coding["system"] == CONDITION_CLINICAL_STATUS_SYSTEM
    assert coding["code"] in CLINICAL_STATUS_CODES


def test_condition_snomed_code_is_numeric(gen: ResourceGenerator):
    gen.generate("Patient", count=1)
    condition = gen.generate("Condition", count=1)[0]
    code_cc = condition.get("code", {})
    coding = code_cc["coding"][0]
    assert coding["system"] == "http://snomed.info/sct"
    assert coding["code"].isdigit()


@pytest.mark.parametrize(
    "section,expected_system",
    [
        ("allergy_clinical_status", "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical"),
        ("allergy_verification_status", "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification"),
        ("observation_categories", "http://terminology.hl7.org/CodeSystem/observation-category"),
        ("encounter_status", "http://hl7.org/fhir/encounter-status"),
        ("observation_status", "http://hl7.org/fhir/observation-status"),
        ("appointment_status", "http://hl7.org/fhir/appointmentstatus"),
        ("gender", "http://hl7.org/fhir/administrative-gender"),
        ("marital_status", "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus"),
    ],
)
def test_key_sections_have_expected_system(section: str, expected_system: str):
    assert get_system(section) == expected_system
    for entry in get_codes(section):
        errors = validate_coding(
            {"system": expected_system, "code": entry["code"]},
            strict_registered=True,
        )
        assert not errors, f"{section}/{entry['code']}: {errors}"


@pytest.mark.parametrize("section", list_sections())
def test_every_yaml_code_validates_against_its_system(section: str):
    system = get_system(section)
    if not system:
        return
    for entry in get_codes(section):
        errors = validate_coding(
            {"system": system, "code": entry["code"]},
            strict_registered=True,
        )
        assert not errors, f"{section}: {errors}"


def test_flag_resource_uses_registered_flag_codes(gen: ResourceGenerator):
    gen.generate("Patient", count=1)
    flag = gen.generate("Flag", count=1)[0]
    code_coding = flag["code"]["coding"][0]
    assert code_coding["system"] == get_system("flag_codes")
    assert code_coding["code"].isdigit()
    assert code_coding["code"] in terminology_index()[get_system("flag_codes")]


# Resources whose status/category codings must come from YAML-managed FHIR code systems.
_STRICT_BINDING_RESOURCES = frozenset({
    "Condition",
    "Observation",
    "AllergyIntolerance",
    "Encounter",
    "Appointment",
    "MedicationRequest",
    "Immunization",
    "Procedure",
    "DiagnosticReport",
    "Claim",
    "Task",
    "Goal",
    "CarePlan",
    # MQL gap resources using YAML-managed FHIR code systems
    "Composition",
    "AdverseEvent",
    "Questionnaire",
    "DeviceRequest",
    "SupplyRequest",
    "SupplyDelivery",
    "ExplanationOfBenefit",
    "CoverageEligibilityRequest",
    "CoverageEligibilityResponse",
    "EnrollmentRequest",
    "EnrollmentResponse",
    "InsurancePlan",
    "PaymentNotice",
    "PaymentReconciliation",
    "Measure",
    "MeasureReport",
    "GenomicStudy",
    "BiologicallyDerivedProduct",
    "DeviceDispense",
    "DeviceUsage",
    "NutritionIntake",
    "MedicationStatement",
    "Contract",
    "Basic",
})


@pytest.mark.parametrize("resource_type", sorted(_STRICT_BINDING_RESOURCES))
def test_managed_bindings_use_yaml_codes(resource_type: str, gen: ResourceGenerator):
    gen.generate("Patient", count=1)
    resource = gen.generate(resource_type, count=1)[0]
    index = terminology_index()
    for path, coding in _iter_codings_flat(resource):
        system = coding.get("system")
        code = coding.get("code")
        if not system or system in (
            "http://snomed.info/sct",
            "http://loinc.org",
            "http://www.nlm.nih.gov/research/umls/rxnorm",
        ):
            continue
        if system in index and code and str(code) not in index[system]:
            # Only assert for systems with a closed code list in YAML
            if len(index[system]) <= 30:
                pytest.fail(f"{resource_type} {path}: {code!r} not in {system}")


def test_no_invalid_snomed_faker_codes_in_condition(gen: ResourceGenerator):
    """Regression: schema fill must not emit random SNOMED codes on Condition."""
    gen.generate("Patient", count=1)
    for _ in range(5):
        condition = gen.generate("Condition", count=1)[0]
        for path, coding in _iter_codings_flat(condition):
            if coding.get("system") == "http://snomed.info/sct":
                assert coding["code"].isdigit(), f"{path}: non-numeric SNOMED {coding['code']!r}"
