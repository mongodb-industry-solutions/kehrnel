"""
Comprehensive integration tests for ALL ImagingStudy search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "ImagingStudy")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/ImagingStudy.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 19 search parameters in ``configs/ImagingStudy.yaml``.

Compartments (precomputed): Patient, Encounter, Device.
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
def rich_imaging_study() -> Dict[str, Any]:
    return {
        "resourceType": "ImagingStudy",
        "id": "imaging-study-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "available",
        "started": "2024-07-15T10:00:00Z",
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "referrer": {"reference": "Practitioner/prac-ref"},
        "identifier": [{"system": "urn:dicom:uid", "value": "1.2.3.4"}],
        "basedOn": [{"reference": "ServiceRequest/sr-1"}],
        "endpoint": [{"reference": "Endpoint/ep-1"}],
        "reason": [{"concept": {"coding": [{"code": "reason-1"}]}}],
        "series": [
            {
                "uid": "1.2.3.4.5",
                "modality": {
                    "coding": [
                        {
                            "system": "http://dicom.nema.org/resources/ontology/DCM",
                            "code": "CT",
                        }
                    ]
                },
                "bodySite": {
                    "concept": {"coding": [{"code": "body-site-1"}]},
                    "reference": {"reference": "BodyStructure/bs-1"},
                },
                "endpoint": [{"reference": "Endpoint/ep-series"}],
                "performer": [{"actor": {"reference": "Device/dev-perf"}}],
                "instance": [
                    {
                        "uid": "1.2.3.4.5.6",
                        "sopClass": {
                            "system": "urn:ietf:rfc:3986",
                            "code": "1.2.840.10008.5.1.4.1.1.2",
                        },
                    }
                ],
            }
        ],
    }


@pytest.fixture
def minimal_imaging_study() -> Dict[str, Any]:
    return {
        "resourceType": "ImagingStudy",
        "id": "imaging-study-min",
        "status": "available",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestImagingStudyReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("ImagingStudy", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("ImagingStudy", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("ImagingStudy", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_based_on(self, converter):
        q = converter.convert("ImagingStudy", "based-on=sr-1")
        assert "_search.basedOnIds" in str(q)

    def test_referrer(self, converter):
        q = converter.convert("ImagingStudy", "referrer=prac-ref")
        assert "_search.referrerId" in str(q)

    def test_performer(self, converter):
        q = converter.convert("ImagingStudy", "performer=dev-perf")
        assert "_search.performerIds" in str(q)

    def test_body_structure(self, converter):
        q = converter.convert("ImagingStudy", "body-structure=bs-1")
        assert "_search.bodyStructureReferenceIds" in str(q)

    def test_endpoint(self, converter):
        q = converter.convert("ImagingStudy", "endpoint=ep-1")
        assert "_search.endpointIds" in str(q)


class TestImagingStudyTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("ImagingStudy", "identifier=1.2.3.4")
        assert "1.2.3.4" in str(q)

    def test_modality(self, converter):
        q = converter.convert("ImagingStudy", "modality=CT")
        assert "seriesModality_codes" in str(q)

    def test_body_site(self, converter):
        q = converter.convert("ImagingStudy", "body-site=body-site-1")
        assert "bodySite_codes" in str(q)

    def test_series(self, converter):
        q = converter.convert("ImagingStudy", "series=1.2.3.4.5")
        assert "seriesUid_values" in str(q)

    def test_instance(self, converter):
        q = converter.convert("ImagingStudy", "instance=1.2.3.4.5.6")
        assert "instanceUid_values" in str(q)

    def test_dicom_class(self, converter):
        q = converter.convert(
            "ImagingStudy",
            "dicom-class=1.2.840.10008.5.1.4.1.1.2",
        )
        assert "dicomClass_codes" in str(q)

    def test_reason(self, converter):
        q = converter.convert("ImagingStudy", "reason=reason-1")
        assert "reasonConcept_codes" in str(q)

    def test_status(self, converter):
        q = converter.convert("ImagingStudy", "status=available")
        assert "available" in str(q)


class TestImagingStudyDateParameters:
    def test_started(self, converter):
        q = converter.convert("ImagingStudy", "started=ge2024-07-01")
        assert "started" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("ImagingStudy", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestImagingStudyCommonParameters:
    def test_id(self, converter):
        q = converter.convert("ImagingStudy", "_id=imaging-study-rich")
        assert "imaging-study-rich" in str(q)


class TestImagingStudyDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_imaging_study):
        out = denormalizer.denormalize(minimal_imaging_study)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_imaging_study):
        out = denormalizer.denormalize(rich_imaging_study)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert s["referrerId"] == "prac-ref"
        assert "sr-1" in s["basedOnIds"]
        assert "ep-1" in s["endpointIds"]
        assert "ep-series" in s["endpointIds"]
        assert "CT" in s["seriesModality_codes"]
        assert "body-site-1" in s["bodySite_codes"]
        assert "bs-1" in s["bodyStructureReferenceIds"]
        assert "dev-perf" in s["performerIds"]
        assert "1.2.3.4.5" in s["seriesUid_values"]
        assert "1.2.3.4.5.6" in s["instanceUid_values"]
        assert "1.2.840.10008.5.1.4.1.1.2" in s["dicomClass_codes"]
        assert "reason-1" in s["reasonConcept_codes"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
        assert out["_compartments"]["Encounter"] == ["enc-1"]
        assert out["_compartments"]["Device"] == ["dev-perf"]

    def test_input_not_mutated(self, denormalizer, rich_imaging_study):
        original = copy.deepcopy(rich_imaging_study)
        denormalizer.denormalize(rich_imaging_study)
        assert rich_imaging_study == original
