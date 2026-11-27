"""
Comprehensive integration tests for ALL Specimen search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Specimen")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Specimen.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 14 search parameters in ``configs/Specimen.yaml``.

Compartments (precomputed): Patient, Device.
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
def rich_specimen() -> Dict[str, Any]:
    return {
        "resourceType": "Specimen",
        "id": "specimen-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "available",
        "type": {"coding": [{"system": "http://snomed.info/sct", "code": "119297000"}]},
        "subject": {"reference": "Patient/pat-1"},
        "accessionIdentifier": {
            "system": "http://hospital.org/accession",
            "value": "ACC-001",
        },
        "identifier": [{"system": "http://hospital.org/specimen", "value": "SP-001"}],
        "parent": [{"reference": "Specimen/spec-parent"}],
        "collection": {
            "collectedDateTime": "2024-07-15T08:00:00Z",
            "collector": {"reference": "Practitioner/prac-1"},
            "procedure": {"reference": "Procedure/proc-1"},
            "bodySite": {
                "reference": {"reference": "BodyStructure/bs-1"},
            },
        },
        "container": [{"device": {"reference": "Device/dev-1"}}],
    }


@pytest.fixture
def minimal_specimen() -> Dict[str, Any]:
    return {
        "resourceType": "Specimen",
        "id": "specimen-min",
        "status": "available",
        "subject": {"reference": "Patient/pat-min"},
    }


@pytest.fixture
def device_subject_specimen() -> Dict[str, Any]:
    return {
        "resourceType": "Specimen",
        "id": "specimen-device-subject",
        "status": "available",
        "subject": {"reference": "Device/dev-subject"},
    }


class TestSpecimenReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Specimen", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("Specimen", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_collector(self, converter):
        q = converter.convert("Specimen", "collector=prac-1")
        assert "_search.collectorId" in str(q)

    def test_procedure(self, converter):
        q = converter.convert("Specimen", "procedure=proc-1")
        assert "_search.procedureId" in str(q)

    def test_bodysite(self, converter):
        q = converter.convert("Specimen", "bodysite=bs-1")
        assert "_search.bodySiteReferenceIds" in str(q)

    def test_parent(self, converter):
        q = converter.convert("Specimen", "parent=spec-parent")
        assert "_search.parentIds" in str(q)

    def test_container_device(self, converter):
        q = converter.convert("Specimen", "container-device=dev-1")
        assert "_search.containerDeviceIds" in str(q)


class TestSpecimenTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("Specimen", "identifier=SP-001")
        assert "SP-001" in str(q)

    def test_accession(self, converter):
        q = converter.convert("Specimen", "accession=ACC-001")
        assert "ACC-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("Specimen", "status=available")
        assert "available" in str(q)

    def test_type(self, converter):
        q = converter.convert("Specimen", "type=119297000")
        assert "type_codes" in str(q)


class TestSpecimenDateParameters:
    def test_collected(self, converter):
        q = converter.convert("Specimen", "collected=ge2024-07-01")
        assert "collectedDateTime" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Specimen", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestSpecimenCommonParameters:
    def test_id(self, converter):
        q = converter.convert("Specimen", "_id=specimen-rich")
        assert "specimen-rich" in str(q)


class TestSpecimenDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_specimen):
        out = denormalizer.denormalize(minimal_specimen)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_specimen):
        out = denormalizer.denormalize(rich_specimen)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["subjectId"] == "pat-1"
        assert s["collectorId"] == "prac-1"
        assert s["procedureId"] == "proc-1"
        assert "bs-1" in s["bodySiteReferenceIds"]
        assert "spec-parent" in s["parentIds"]
        assert "dev-1" in s["containerDeviceIds"]
        assert "ACC-001" in s["accession_values"]
        assert "SP-001" in s["identifier_values"]
        assert "119297000" in s["type_codes"]
        assert out["_compartments"]["Patient"] == ["pat-1"]

    def test_device_subject_compartment(self, denormalizer, device_subject_specimen):
        out = denormalizer.denormalize(device_subject_specimen)
        assert out["_compartments"]["Device"] == ["dev-subject"]

    def test_input_not_mutated(self, denormalizer, rich_specimen):
        original = copy.deepcopy(rich_specimen)
        denormalizer.denormalize(rich_specimen)
        assert rich_specimen == original
