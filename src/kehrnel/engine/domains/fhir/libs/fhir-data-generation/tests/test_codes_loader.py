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

# Sections referenced by MQL-shipped / gap enrichers (aliases resolve to targets).
MQL_ENRICHER_SECTIONS = [
    "composition_status",
    "loinc_composition_types",
    "adverse_event_status",
    "adverse_event_actuality",
    "body_sites",
    "immunization_forecast_status",
    "vaccines",
    "questionnaire_status",
    "loinc_questionnaire_panels",
    "countries",
    "device_request_status",
    "request_intent",
    "request_priority",
    "snomed_devices",
    "supply_request_status",
    "supply_categories",
    "supply_delivery_status",
    "request_orchestration_status",
    "vision_prescription_status",
    "eye_laterality",
    "nutrition_intake_status",
    "nutrition_foods",
    "basic_resource_codes",
    "provenance_activity",
    "provenance_participant_type",
    "coverage_eligibility_request_status",
    "eligibility_purpose",
    "explanation_of_benefit_status",
    "claim_type",
    "claim_use",
    "coverage_eligibility_response_status",
    "enrollment_status",
    "insurance_plan_status",
    "insurance_plan_types",
    "charge_item_definition_status",
    "payment_notice_status",
    "payment_status",
    "payment_reconciliation_status",
    "payment_reconciliation_outcome",
    "device_usage_status",
    "device_dispense_status",
    "biologically_derived_product_category",
    "biologically_derived_product_status",
    "biologically_derived_product_codes",
    "endpoint_connection_type",
    "mime_types",
    "genomic_study_status",
    "measure_status",
    "measure_report_status",
    "measure_report_type",
    "medication_statement_status",
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


def test_mql_enricher_sections_present():
    sections = set(list_sections())
    missing = [s for s in MQL_ENRICHER_SECTIONS if s not in sections]
    assert not missing, f"Missing MQL enricher sections: {missing}"


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
