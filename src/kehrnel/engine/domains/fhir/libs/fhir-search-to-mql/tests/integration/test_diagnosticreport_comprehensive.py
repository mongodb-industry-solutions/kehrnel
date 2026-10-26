"""
Comprehensive integration tests for ALL DiagnosticReport search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "DiagnosticReport")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/DiagnosticReport.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json, encounter.json

Exercises 19 search parameters in ``configs/DiagnosticReport.yaml``.

Compartments (precomputed): Patient, Practitioner, Device, Encounter.
"""
from __future__ import annotations

import copy
from datetime import datetime
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
def rich_report() -> Dict[str, Any]:
    return {
        "resourceType": "DiagnosticReport",
        "id": "dr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "final",
        "identifier": [{"system": "http://hospital.org/dr", "value": "DR-001"}],
        "category": [
            {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0074", "code": "LAB"}]}
        ],
        "code": {
            "coding": [{"system": "http://loinc.org", "code": "11502-2"}]
        },
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "effectiveDateTime": "2024-07-10T08:00:00Z",
        "effectivePeriod": {
            "start": "2024-07-10T07:00:00Z",
            "end": "2024-07-10T09:00:00Z",
        },
        "issued": "2024-07-10T10:00:00Z",
        "performer": [
            {"reference": "Practitioner/prac-1"},
            {"reference": "Device/dev-1"},
        ],
        "resultsInterpreter": [{"reference": "Practitioner/prac-2"}],
        "basedOn": [{"reference": "ServiceRequest/sr-1"}],
        "specimen": [{"reference": "Specimen/spec-1"}],
        "result": [{"reference": "Observation/obs-1"}],
        "study": [{"reference": "ImagingStudy/img-1"}],
        "media": [{"link": {"reference": "DocumentReference/doc-1"}}],
        "conclusionCode": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "10828004"}]}
        ],
    }


@pytest.fixture
def minimal_report() -> Dict[str, Any]:
    return {
        "resourceType": "DiagnosticReport",
        "id": "dr-min",
        "status": "preliminary",
        "code": {"coding": [{"code": "11502-2"}]},
        "subject": {"reference": "Patient/pat-min"},
    }


class TestDiagnosticReportReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("DiagnosticReport", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("DiagnosticReport", "subject=Patient/pat-1")
        assert "_search.subjectId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("DiagnosticReport", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_performer(self, converter):
        q = converter.convert("DiagnosticReport", "performer=prac-1")
        assert "_search.performerIds" in str(q)

    def test_results_interpreter(self, converter):
        q = converter.convert("DiagnosticReport", "results-interpreter=prac-2")
        assert "_search.resultsInterpreterIds" in str(q)

    def test_based_on(self, converter):
        q = converter.convert("DiagnosticReport", "based-on=sr-1")
        assert "_search.basedOnIds" in str(q)

    def test_result(self, converter):
        q = converter.convert("DiagnosticReport", "result=obs-1")
        assert "_search.resultIds" in str(q)

    def test_specimen(self, converter):
        q = converter.convert("DiagnosticReport", "specimen=spec-1")
        assert "_search.specimenIds" in str(q)

    def test_study(self, converter):
        q = converter.convert("DiagnosticReport", "study=img-1")
        assert "_search.studyIds" in str(q)

    def test_media(self, converter):
        q = converter.convert("DiagnosticReport", "media=doc-1")
        assert "_search.mediaIds" in str(q)


class TestDiagnosticReportTokenParameters:
    def test_status(self, converter):
        assert converter.convert("DiagnosticReport", "status=final") == {"status": "final"}

    def test_code(self, converter):
        q = converter.convert("DiagnosticReport", "code=11502-2")
        assert "11502-2" in str(q)

    def test_category(self, converter):
        q = converter.convert("DiagnosticReport", "category=LAB")
        assert "LAB" in str(q)

    def test_conclusion(self, converter):
        q = converter.convert("DiagnosticReport", "conclusion=10828004")
        assert "10828004" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("DiagnosticReport", "identifier=DR-001")
        assert "DR-001" in str(q)


class TestDiagnosticReportDateParameters:
    def test_date_effective(self, converter):
        q = converter.convert("DiagnosticReport", "date=ge2024-07-10")
        assert "effectiveDateTime" in str(q) or "effectivePeriod" in str(q)

    def test_issued(self, converter):
        q = converter.convert("DiagnosticReport", "issued=ge2024-07-10")
        assert "issued" in str(q)


class TestDiagnosticReportDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_report):
        out = denormalizer.denormalize(minimal_report)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert "11502-2" in s["code_codes"]

    def test_rich_fields(self, denormalizer, rich_report):
        out = denormalizer.denormalize(rich_report)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert "LAB" in s["category_codes"]
        assert "11502-2" in s["code_codes"]
        assert "10828004" in s["conclusionCode_codes"]
        assert "prac-1" in s["performerIds"]
        assert "prac-2" in s["resultsInterpreterIds"]
        assert "sr-1" in s["basedOnIds"]
        assert "spec-1" in s["specimenIds"]
        assert "obs-1" in s["resultIds"]
        assert "img-1" in s["studyIds"]
        assert "doc-1" in s["mediaIds"]
        assert "effectivePeriod" in s

    def test_input_not_mutated(self, denormalizer, rich_report):
        original = copy.deepcopy(rich_report)
        denormalizer.denormalize(rich_report)
        assert rich_report == original


class TestDiagnosticReportPrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_report):
        out = denormalizer.denormalize(rich_report)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment_performer_only(self, denormalizer, rich_report):
        out = denormalizer.denormalize(rich_report)
        prac = out["_compartments"]["Practitioner"]
        assert "prac-1" in prac
        assert "prac-2" not in prac

    def test_device_compartment(self, denormalizer, rich_report):
        out = denormalizer.denormalize(rich_report)
        assert "dev-1" in out["_compartments"]["Device"]

    def test_encounter_compartment(self, denormalizer, rich_report):
        out = denormalizer.denormalize(rich_report)
        assert "enc-1" in out["_compartments"]["Encounter"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "DiagnosticReport", "status=final"
        )
        assert "_compartments.Patient" in str(q)


def _mongo_available() -> bool:
    try:
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.server_info()
        client.close()
        return True
    except Exception:
        return False


@pytest.mark.mongodb
@pytest.mark.skipif(not _mongo_available(), reason="MongoDB not running")
class TestDiagnosticReportMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["diagnosticreport_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "DiagnosticReport",
            "id": "e2e-dr-1",
            "status": "final",
            "code": {"coding": [{"code": "11502-2"}]},
            "subject": {"reference": "Patient/p1"},
            "issued": datetime(2024, 7, 10, 10, 0, 0),
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(seeded.find(conv.convert("DiagnosticReport", "status=final")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-dr-1"
