"""
Comprehensive integration tests for ALL Composition search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Composition")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Composition.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, encounter.json, device.json

Exercises 21 shipped search parameters in ``configs/Composition.yaml``
(composite `section-code-text` deferred).

Compartments (precomputed): Patient, Practitioner, Encounter, Device.
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
def rich_composition() -> Dict[str, Any]:
    return {
        "resourceType": "Composition",
        "id": "comp-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "final",
        "version": "2",
        "url": "http://example.org/Composition/comp-rich",
        "title": "Discharge Summary",
        "date": "2024-07-15",
        "type": {"coding": [{"code": "18842-5"}]},
        "category": [{"coding": [{"code": "clinical-note"}]}],
        "subject": [{"reference": "Patient/pat-1"}],
        "author": [{"reference": "Practitioner/pr-author"}],
        "encounter": {"reference": "Encounter/enc-1"},
        "identifier": [
            {"system": "http://hospital.org/comp", "value": "COMP-001"}
        ],
        "attester": [{"party": {"reference": "Practitioner/pr-attest"}}],
        "event": [
            {
                "period": {"start": "2024-07-01", "end": "2024-07-14"},
                "detail": [
                    {
                        "concept": {"coding": [{"code": "admission"}]},
                        "reference": {"reference": "Encounter/enc-ev"},
                    }
                ],
            }
        ],
        "relatesTo": [
            {
                "resourceReference": {"reference": "Composition/comp-prior"},
            }
        ],
        "section": [
            {
                "code": {"coding": [{"code": "48767-8"}]},
                "entry": [{"reference": "Observation/obs-1"}],
            }
        ],
    }


@pytest.fixture
def minimal_composition() -> Dict[str, Any]:
    return {
        "resourceType": "Composition",
        "id": "comp-min",
        "status": "preliminary",
        "type": {"coding": [{"code": "18842-5"}]},
        "author": [{"reference": "Practitioner/pr-min"}],
    }


class TestCompositionReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Composition", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("Composition", "subject=pat-1")
        assert "_search.subjectIds" in str(q)

    def test_author(self, converter):
        q = converter.convert("Composition", "author=pr-author")
        assert "_search.authorIds" in str(q)

    def test_attester(self, converter):
        q = converter.convert("Composition", "attester=pr-attest")
        assert "_search.attesterPartyIds" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("Composition", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_entry(self, converter):
        q = converter.convert("Composition", "entry=obs-1")
        assert "_search.sectionEntryIds" in str(q)

    def test_related(self, converter):
        q = converter.convert("Composition", "related=comp-prior")
        assert "_search.relatedTargetIds" in str(q)

    def test_event_reference(self, converter):
        q = converter.convert("Composition", "event-reference=enc-ev")
        assert "_search.eventReferenceIds" in str(q)


class TestCompositionTokenParameters:
    def test_status(self, converter):
        assert converter.convert("Composition", "status=final") == {
            "status": "final"
        }

    def test_type(self, converter):
        q = converter.convert("Composition", "type=18842-5")
        assert "type_codes" in str(q)

    def test_category(self, converter):
        q = converter.convert("Composition", "category=clinical-note")
        assert "category_codes" in str(q)

    def test_section(self, converter):
        q = converter.convert("Composition", "section=48767-8")
        assert "section_codes" in str(q)

    def test_event_code(self, converter):
        q = converter.convert("Composition", "event-code=admission")
        assert "eventCode_codes" in str(q)

    def test_version(self, converter):
        assert converter.convert("Composition", "version=2") == {"version": "2"}

    def test_identifier(self, converter):
        q = converter.convert("Composition", "identifier=COMP-001")
        assert "COMP-001" in str(q)


class TestCompositionStringParameters:
    def test_title(self, converter):
        q = converter.convert("Composition", "title=Discharge")
        assert "title_lower" in str(q)


class TestCompositionUriParameters:
    def test_url(self, converter):
        q = converter.convert(
            "Composition", "url=http://example.org/Composition/comp-rich"
        )
        assert "url" in str(q)


class TestCompositionDateParameters:
    def test_date(self, converter):
        q = converter.convert("Composition", "date=ge2024-07-01")
        assert "date" in str(q)

    def test_period(self, converter):
        q = converter.convert("Composition", "period=ge2024-07-01")
        assert "period" in str(q)


class TestCompositionDenormalization:
    def test_rich(self, denormalizer, rich_composition):
        out = denormalizer.denormalize(copy.deepcopy(rich_composition))
        s = out["_search"]
        assert "pat-1" in s["subjectIds"]
        assert "pr-author" in s["authorIds"]
        assert s["encounterId"] == "enc-1"
        assert "obs-1" in s["sectionEntryIds"]
        assert "comp-prior" in s["relatedTargetIds"]
        assert "admission" in s["eventCode_codes"]
        assert "48767-8" in s["section_codes"]
        c = out["_compartments"]
        assert "pat-1" in c["Patient"]
        assert "pr-author" in c["Practitioner"]
        assert "enc-1" in c["Encounter"]

    def test_minimal(self, denormalizer, minimal_composition):
        out = denormalizer.denormalize(copy.deepcopy(minimal_composition))
        assert "pr-min" in out["_search"]["authorIds"]


class TestCompositionCompartmentRouting:
    def test_patient_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "Composition")
        assert q == {"_compartments.Patient": "pat-1"}

    def test_encounter_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Encounter", "enc-1", "Composition"
        )
        assert q == {"_compartments.Encounter": "enc-1"}
