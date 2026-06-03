"""Prompt 3 — healthcare codes loader tests."""

import random

import pytest

from fhir_gen.codes.loader import (
    get_codes,
    get_system,
    list_sections,
    load_codes,
    random_code,
    reload_codes,
)

REQUIRED_SECTIONS = [
    "languages",
    "mime_types",
    "gender",
    "marital_status",
    "contact_relationship",
    "identifier_types",
    "name_use",
    "address_use",
    "address_type",
    "telecom_system",
    "telecom_use",
    "appointment_status",
    "slot_status",
    "participation_status",
    "encounter_status",
    "encounter_class",
    "observation_status",
    "condition_clinical_status",
    "condition_verification_status",
    "allergy_clinical_status",
    "allergy_verification_status",
    "procedure_status",
    "immunization_status",
    "medication_request_status",
    "medication_admin_status",
    "medication_dispense_status",
    "claim_status",
    "coverage_status",
    "care_plan_status",
    "care_plan_intent",
    "goal_status",
    "task_status",
    "document_reference_status",
    "composition_status",
    "dosage_routes",
    "dosage_timing",
    "service_category",
    "service_type",
    "loinc_observations",
    "snomed_conditions",
    "snomed_procedures",
    "snomed_allergies",
    "snomed_medications",
    "body_sites",
    "countries",
    "us_states",
]


@pytest.fixture(autouse=True)
def fresh_codes_cache():
    reload_codes()
    yield
    reload_codes()


def test_load_codes_non_empty():
    data = load_codes()
    assert len(data) >= 90


def test_all_required_sections_present():
    sections = set(list_sections())
    missing = [s for s in REQUIRED_SECTIONS if s not in sections]
    assert not missing, f"Missing sections: {missing}"


def test_random_code_gender_reproducible():
  rng = random.Random(1)
  code = random_code("gender", rng)
  assert code is not None
  assert code["code"] in ("male", "female", "other", "unknown")
  assert get_system("gender") == "http://hl7.org/fhir/administrative-gender"


def test_get_codes_loinc_has_clinical_fields():
    codes = get_codes("loinc_observations")
    assert len(codes) >= 10
    sample = codes[0]
    assert "code" in sample
    assert "value" in sample
    assert "unit" in sample
    assert "low" in sample
    assert "high" in sample


def test_get_system_marital_status():
    assert get_system("marital_status") == "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus"


def test_identifier_types_alias():
    codes = get_codes("identifier_types")
    assert any(c.get("code") == "MR" for c in codes)


def test_snomed_allergies_section():
    codes = get_codes("snomed_allergies")
    assert len(codes) >= 5
    assert all("code" in c and "display" in c for c in codes)


def test_reload_clears_cache():
    first = load_codes()
    reload_codes()
    second = load_codes()
    assert first is not second
