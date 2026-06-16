"""
Comprehensive integration tests for ALL DocumentReference search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "DocumentReference")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/DocumentReference.yaml
- src/fhir_search_to_mql/compartments/definitions/

Exercises 35 search parameters in ``configs/DocumentReference.yaml``
(36 R5 index rows; composite ``relationship`` deferred).

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
def rich_document_reference() -> Dict[str, Any]:
    return {
        "resourceType": "DocumentReference",
        "id": "doc-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "current",
        "docStatus": "final",
        "type": {
            "coding": [{"system": "http://loinc.org", "code": "34117-2"}],
            "text": "History and physical note",
        },
        "category": [
            {"coding": [{"system": "http://hl7.org/fhir/us/core/CodeSystem/us-core-documentreference-category", "code": "clinical-note"}]}
        ],
        "subject": {"reference": "Patient/pat-1"},
        "date": "2024-07-15T10:00:00Z",
        "author": [{"reference": "Practitioner/pr-author"}],
        "attester": [{"party": {"reference": "Practitioner/pr-attest"}}],
        "custodian": {"reference": "Organization/org-cust"},
        "context": [{"reference": "Encounter/enc-1"}],
        "identifier": [{"system": "http://hospital.org/docs", "value": "DOC-001"}],
        "description": "Admission H&P",
        "version": "v1",
        "modality": [{"coding": [{"code": "DOC"}]}],
        "facilityType": {"coding": [{"code": "inpatient"}]},
        "practiceSetting": {"coding": [{"code": "cardiology"}]},
        "securityLabel": [{"coding": [{"code": "R"}]}],
        "bodySite": [
            {
                "concept": {"coding": [{"code": "368208006"}]},
                "reference": {"reference": "BodyStructure/bs-1"},
            }
        ],
        "event": [
            {
                "concept": {"coding": [{"code": "admission"}]},
                "reference": {"reference": "Encounter/enc-1"},
            }
        ],
        "relatesTo": [
            {
                "code": {"coding": [{"code": "replaces"}]},
                "target": {"reference": "DocumentReference/doc-old"},
            }
        ],
        "basedOn": [{"reference": "ServiceRequest/sr-1"}],
        "content": [
            {
                "attachment": {
                    "contentType": "application/pdf",
                    "language": "en",
                    "url": "https://example.org/docs/doc-rich.pdf",
                    "creation": "2024-07-14",
                },
                "profile": [
                    {"valueCoding": {"system": "http://terminology.hl7.org/CodeSystem/formatcodes", "code": "urn:hl7-org:sdwg:ccda-structuredBody:1.1"}},
                    {"valueUri": "http://example.org/profile/doc"},
                    {"valueCanonical": "http://hl7.org/fhir/StructureDefinition/DocumentReference"},
                ],
            }
        ],
        "period": {"start": "2024-07-01", "end": "2024-07-31"},
    }


@pytest.fixture
def minimal_document_reference() -> Dict[str, Any]:
    return {
        "resourceType": "DocumentReference",
        "id": "doc-min",
        "status": "current",
        "content": [{"attachment": {"contentType": "text/plain"}}],
        "subject": {"reference": "Patient/pat-min"},
    }


class TestDocumentReferenceReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("DocumentReference", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("DocumentReference", "subject=Patient/pat-1")
        assert "_search.subjectId" in str(q)

    def test_author(self, converter):
        q = converter.convert("DocumentReference", "author=pr-author")
        assert "_search.authorIds" in str(q)

    def test_attester(self, converter):
        q = converter.convert("DocumentReference", "attester=pr-attest")
        assert "_search.attesterPartyIds" in str(q)

    def test_context(self, converter):
        q = converter.convert("DocumentReference", "context=enc-1")
        assert "_search.contextIds" in str(q)

    def test_custodian(self, converter):
        q = converter.convert("DocumentReference", "custodian=org-cust")
        assert "_search.custodianId" in str(q)

    def test_relatesto(self, converter):
        q = converter.convert("DocumentReference", "relatesto=doc-old")
        assert "_search.relatesToTargetIds" in str(q)

    def test_bodysite_reference(self, converter):
        q = converter.convert("DocumentReference", "bodysite-reference=bs-1")
        assert "_search.bodySiteReferenceIds" in str(q)

    def test_event_reference(self, converter):
        q = converter.convert("DocumentReference", "event-reference=enc-1")
        assert "_search.eventReferenceIds" in str(q)

    def test_based_on(self, converter):
        q = converter.convert("DocumentReference", "based-on=sr-1")
        assert "_search.basedOnIds" in str(q)

    def test_format_canonical(self, converter):
        q = converter.convert(
            "DocumentReference",
            "format-canonical=http://hl7.org/fhir/StructureDefinition/DocumentReference",
        )
        assert "formatCanonical" in str(q)


class TestDocumentReferenceTokenParameters:
    def test_status(self, converter):
        assert converter.convert("DocumentReference", "status=current") == {
            "status": "current"
        }

    def test_doc_status(self, converter):
        assert converter.convert("DocumentReference", "doc-status=final") == {
            "docStatus": "final"
        }

    def test_type(self, converter):
        q = converter.convert("DocumentReference", "type=34117-2")
        assert "34117-2" in str(q)

    def test_category(self, converter):
        q = converter.convert("DocumentReference", "category=clinical-note")
        assert "clinical-note" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("DocumentReference", "identifier=DOC-001")
        assert "DOC-001" in str(q)

    def test_contenttype(self, converter):
        q = converter.convert("DocumentReference", "contenttype=application/pdf")
        assert "application/pdf" in str(q)

    def test_language(self, converter):
        q = converter.convert("DocumentReference", "language=en")
        assert "en" in str(q)

    def test_modality(self, converter):
        q = converter.convert("DocumentReference", "modality=DOC")
        assert "DOC" in str(q)

    def test_bodysite(self, converter):
        q = converter.convert("DocumentReference", "bodysite=368208006")
        assert "368208006" in str(q)

    def test_event_code(self, converter):
        q = converter.convert("DocumentReference", "event-code=admission")
        assert "admission" in str(q)

    def test_relation(self, converter):
        q = converter.convert("DocumentReference", "relation=replaces")
        assert "replaces" in str(q)

    def test_setting(self, converter):
        q = converter.convert("DocumentReference", "setting=cardiology")
        assert "cardiology" in str(q)

    def test_facility(self, converter):
        q = converter.convert("DocumentReference", "facility=inpatient")
        assert "inpatient" in str(q)

    def test_security_label(self, converter):
        q = converter.convert("DocumentReference", "security-label=R")
        assert "R" in str(q)

    def test_format_code(self, converter):
        q = converter.convert(
            "DocumentReference",
            "format-code=urn:hl7-org:sdwg:ccda-structuredBody:1.1",
        )
        assert "ccda-structuredBody" in str(q)


class TestDocumentReferenceStringParameters:
    def test_description(self, converter):
        q = converter.convert("DocumentReference", "description=admission")
        assert "description" in str(q).lower()

    def test_version(self, converter):
        q = converter.convert("DocumentReference", "version=v1")
        assert "version" in str(q).lower()


class TestDocumentReferenceUriParameters:
    def test_location(self, converter):
        q = converter.convert("DocumentReference", "location=example.org/docs")
        assert "attachmentUrl" in str(q) or "example.org" in str(q)

    def test_format_uri(self, converter):
        q = converter.convert("DocumentReference", "format-uri=example.org/profile")
        assert "formatUri" in str(q) or "example.org" in str(q)


class TestDocumentReferenceDateParameters:
    def test_date(self, converter):
        q = converter.convert("DocumentReference", "date=ge2024-07-15")
        assert "date" in str(q)

    def test_creation(self, converter):
        q = converter.convert("DocumentReference", "creation=ge2024-07-14")
        assert "attachmentCreation" in str(q)

    def test_period(self, converter):
        q = converter.convert("DocumentReference", "period=ge2024-07-01")
        assert "period" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("DocumentReference", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestDocumentReferenceCommonParameters:
    def test_id(self, converter):
        q = converter.convert("DocumentReference", "_id=doc-rich")
        assert "doc-rich" in str(q)


class TestDocumentReferenceDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_document_reference):
        out = denormalizer.denormalize(minimal_document_reference)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert "text/plain" in s["contentType_values"]

    def test_rich_fields(self, denormalizer, rich_document_reference):
        out = denormalizer.denormalize(rich_document_reference)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "pr-author" in s["authorIds"]
        assert "pr-attest" in s["attesterPartyIds"]
        assert s["custodianId"] == "org-cust"
        assert "enc-1" in s["contextIds"]
        assert "34117-2" in s["type_codes"]
        assert "DOC-001" in s["identifier_values"]
        assert s["description_lower"] == "admission h&p"
        assert s["version"] == "v1"
        assert "application/pdf" in s["contentType_values"]
        assert "replaces" in s["relation_codes"]
        assert "doc-old" in s["relatesToTargetIds"]
        assert "https://example.org/docs/doc-rich.pdf" in s["attachmentUrl_values"]

    def test_input_not_mutated(self, denormalizer, rich_document_reference):
        original = copy.deepcopy(rich_document_reference)
        denormalizer.denormalize(rich_document_reference)
        assert rich_document_reference == original


class TestDocumentReferencePrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_document_reference):
        out = denormalizer.denormalize(rich_document_reference)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment(self, denormalizer, rich_document_reference):
        out = denormalizer.denormalize(rich_document_reference)
        prac = out["_compartments"]["Practitioner"]
        assert "pr-author" in prac
        assert "pr-attest" in prac

    def test_encounter_compartment(self, denormalizer, rich_document_reference):
        out = denormalizer.denormalize(rich_document_reference)
        assert "enc-1" in out["_compartments"]["Encounter"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "DocumentReference", "status=current"
        )
        assert "_compartments.Patient" in str(q)
