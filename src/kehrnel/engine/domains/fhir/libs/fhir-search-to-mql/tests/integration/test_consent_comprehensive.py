"""
Comprehensive integration tests for ALL Consent search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Consent")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Consent.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 20 search parameters in ``configs/Consent.yaml``.

Compartments (precomputed): Patient.
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
def rich_consent() -> Dict[str, Any]:
    return {
        "resourceType": "Consent",
        "id": "consent-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "date": "2024-07-15",
        "subject": {"reference": "Patient/pat-1"},
        "grantee": [{"reference": "Practitioner/prac-1"}],
        "controller": [{"reference": "Organization/org-1"}],
        "manager": [{"reference": "Practitioner/prac-2"}],
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/consentcategorycodes",
                        "code": "idscl",
                    }
                ]
            }
        ],
        "identifier": [{"system": "http://hospital.org/consent", "value": "CONSENT-001"}],
        "sourceReference": [{"reference": "DocumentReference/doc-1"}],
        "provision": [
            {
                "period": {
                    "start": "2024-07-01T00:00:00Z",
                    "end": "2025-06-30T23:59:59Z",
                },
                "action": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/consentaction",
                                "code": "access",
                            }
                        ]
                    }
                ],
                "purpose": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ActReason",
                        "code": "PATRQT",
                    }
                ],
                "securityLabel": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-Confidentiality",
                        "code": "R",
                    }
                ],
                "actor": [
                    {
                        "role": {"coding": [{"code": "PRCP"}]},
                        "reference": {"reference": "Organization/org-2"},
                    }
                ],
                "data": [
                    {
                        "meaning": "related",
                        "reference": {"reference": "Observation/obs-1"},
                    }
                ],
            }
        ],
        "verification": [
            {
                "verified": True,
                "verificationDate": ["2024-07-16T10:00:00Z"],
            }
        ],
    }


@pytest.fixture
def minimal_consent() -> Dict[str, Any]:
    return {
        "resourceType": "Consent",
        "id": "consent-min",
        "status": "active",
        "subject": {"reference": "Patient/pat-min"},
        "provision": [{"purpose": [{"code": "PATRQT"}]}],
    }


class TestConsentReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Consent", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("Consent", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_grantee(self, converter):
        q = converter.convert("Consent", "grantee=prac-1")
        assert "_search.granteeIds" in str(q)

    def test_controller(self, converter):
        q = converter.convert("Consent", "controller=org-1")
        assert "_search.controllerIds" in str(q)

    def test_manager(self, converter):
        q = converter.convert("Consent", "manager=prac-2")
        assert "_search.managerIds" in str(q)

    def test_actor(self, converter):
        q = converter.convert("Consent", "actor=org-2")
        assert "_search.provisionActorReferenceIds" in str(q)

    def test_data(self, converter):
        q = converter.convert("Consent", "data=obs-1")
        assert "_search.provisionDataReferenceIds" in str(q)

    def test_source_reference(self, converter):
        q = converter.convert("Consent", "source-reference=doc-1")
        assert "_search.sourceReferenceIds" in str(q)


class TestConsentTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("Consent", "identifier=CONSENT-001")
        assert "CONSENT-001" in str(q)

    def test_category(self, converter):
        q = converter.convert("Consent", "category=idscl")
        assert "idscl" in str(q)

    def test_status(self, converter):
        q = converter.convert("Consent", "status=active")
        assert "active" in str(q)

    def test_action(self, converter):
        q = converter.convert("Consent", "action=access")
        assert "access" in str(q)

    def test_purpose(self, converter):
        q = converter.convert("Consent", "purpose=PATRQT")
        assert "PATRQT" in str(q)

    def test_security_label(self, converter):
        q = converter.convert("Consent", "security-label=R")
        assert "provisionSecurityLabel_codes" in str(q)

    def test_verified(self, converter):
        q = converter.convert("Consent", "verified=true")
        assert "verificationVerified_values" in str(q)


class TestConsentDateParameters:
    def test_date(self, converter):
        q = converter.convert("Consent", "date=ge2024-07-01")
        assert "date" in str(q)

    def test_period(self, converter):
        q = converter.convert("Consent", "period=ge2024-07-01")
        assert "provisionPeriod" in str(q)

    def test_verified_date(self, converter):
        q = converter.convert("Consent", "verified-date=ge2024-07-01")
        assert "verificationDate_values" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Consent", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestConsentCommonParameters:
    def test_id(self, converter):
        q = converter.convert("Consent", "_id=consent-rich")
        assert "consent-rich" in str(q)


class TestConsentDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_consent):
        out = denormalizer.denormalize(minimal_consent)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert "PATRQT" in s["provisionPurpose_codes"]
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_consent):
        out = denormalizer.denormalize(rich_consent)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "prac-1" in s["granteeIds"]
        assert "org-1" in s["controllerIds"]
        assert "prac-2" in s["managerIds"]
        assert "org-2" in s["provisionActorReferenceIds"]
        assert "obs-1" in s["provisionDataReferenceIds"]
        assert "doc-1" in s["sourceReferenceIds"]
        assert "idscl" in s["category_codes"]
        assert "access" in s["provisionAction_codes"]
        assert "PATRQT" in s["provisionPurpose_codes"]
        assert "R" in s["provisionSecurityLabel_codes"]
        assert "CONSENT-001" in s["identifier_values"]
        assert True in s["verificationVerified_values"]
        assert out["_compartments"]["Patient"] == ["pat-1"]

    def test_input_not_mutated(self, denormalizer, rich_consent):
        original = copy.deepcopy(rich_consent)
        denormalizer.denormalize(rich_consent)
        assert rich_consent == original
