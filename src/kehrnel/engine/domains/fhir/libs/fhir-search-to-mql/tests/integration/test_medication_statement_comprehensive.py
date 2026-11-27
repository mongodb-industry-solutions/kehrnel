"""
Comprehensive integration tests for ALL MedicationStatement search parameters (FHIR R5).

Local spec: schema/indexes/search-parameters.r5.json, configs/MedicationStatement.yaml.
"""
from __future__ import annotations

import copy
from typing import Any, Dict

import pytest

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def converter() -> FHIRSearchConverter:
    return FHIRSearchConverter()


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


@pytest.fixture
def rich_medication_statement() -> Dict[str, Any]:
    return {
        "resourceType": "MedicationStatement",
        "id": "ms-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "recorded",
        "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/medication-statement-category", "code": "inpatient"}]}],
        "medication": {
            "concept": {"coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "313782"}]},
            "reference": {"reference": "Medication/med-1"},
        },
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "effectiveDateTime": "2024-06-15T08:00:00Z",
        "identifier": [{"system": "http://hospital.org/ms", "value": "MS-001"}],
        "informationSource": [{"reference": "Practitioner/prac-1"}],
        "adherence": {
            "code": {"coding": [{"code": "taking"}]},
        },
    }


@pytest.fixture
def minimal_medication_statement() -> Dict[str, Any]:
    return {
        "resourceType": "MedicationStatement",
        "id": "ms-min",
        "status": "recorded",
        "medication": {
            "concept": {"coding": [{"code": "313782"}]},
        },
        "subject": {"reference": "Patient/pat-min"},
    }


class TestMedicationStatementReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("MedicationStatement", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("MedicationStatement", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("MedicationStatement", "encounter=enc-1")
        assert "enc-1" in str(q)

    def test_medication(self, converter):
        q = converter.convert("MedicationStatement", "medication=med-1")
        assert "med-1" in str(q)

    def test_source(self, converter):
        q = converter.convert("MedicationStatement", "source=prac-1")
        assert "sourceIds" in str(q)


class TestMedicationStatementTokenParameters:
    def test_code(self, converter):
        q = converter.convert("MedicationStatement", "code=313782")
        assert "medicationConcept_codes" in str(q)

    def test_category(self, converter):
        q = converter.convert("MedicationStatement", "category=inpatient")
        assert "category_codes" in str(q)

    def test_adherence(self, converter):
        q = converter.convert("MedicationStatement", "adherence=taking")
        assert "adherence_codes" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("MedicationStatement", "identifier=MS-001")
        assert "MS-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("MedicationStatement", "status=recorded")
        assert "recorded" in str(q)

    def test_id(self, converter):
        q = converter.convert("MedicationStatement", "_id=ms-rich")
        assert "ms-rich" in str(q)


class TestMedicationStatementDateParameters:
    def test_effective(self, converter):
        q = converter.convert("MedicationStatement", "effective=ge2024-06-01")
        assert "effectiveDateTime" in str(q) or "effectivePeriod" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("MedicationStatement", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestMedicationStatementDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_medication_statement):
        doc = denormalizer.denormalize(copy.deepcopy(rich_medication_statement))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert search["encounterId"] == "enc-1"
        assert "313782" in search["medicationConcept_codes"]
        assert "med-1" in search["medicationReferenceIds"]
        assert "prac-1" in search["sourceIds"]
        assert "taking" in search["adherence_codes"]
        assert "Patient" in doc["_compartments"]
        assert "Practitioner" in doc["_compartments"]

    def test_minimal_sparse(self, denormalizer, minimal_medication_statement):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_medication_statement))
        assert doc["_search"]["patientId"] == "pat-min"
        assert "313782" in doc["_search"]["medicationConcept_codes"]
