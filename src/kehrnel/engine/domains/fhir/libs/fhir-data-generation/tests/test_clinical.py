"""Prompt 9 — clinical resource enricher tests."""

import pytest

from fhir_gen import ResourceGenerator
from fhir_gen.generators.resources.clinical import ENRICHERS

SEED = 42


def make_gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


class TestClinicalEnrichers:
    def test_enrichers_registered(self):
        assert "Patient" in ENRICHERS
        assert len(ENRICHERS) == 15

    def test_patient_structure(self):
        patients = make_gen().generate("Patient", count=3)
        assert len(patients) == 3
        for p in patients:
            assert p["resourceType"] == "Patient"
            assert p["gender"] in ["male", "female", "other", "unknown"]
            assert "birthDate" in p
            assert p["name"]
            assert p["identifier"]

    def test_practitioner_structure(self):
        pr = make_gen().generate("Practitioner")[0]
        assert pr["resourceType"] == "Practitioner"
        assert pr["name"]
        assert pr["identifier"]
        assert pr["active"] is True

    def test_organization_structure(self):
        org = make_gen().generate("Organization")[0]
        assert org["resourceType"] == "Organization"
        assert org["name"]

    def test_encounter_references_patient(self):
        gen = make_gen()
        gen.generate("Patient", count=2)
        enc = gen.generate("Encounter")[0]
        assert enc["subject"]["reference"].startswith("Patient/")

    def test_observation_has_value(self):
        gen = make_gen()
        gen.generate("Patient")
        obs = gen.generate("Observation")[0]
        value_fields = [k for k in obs if k.startswith("value")]
        assert value_fields
        if "valueQuantity" in obs:
            assert "value" in obs["valueQuantity"]

    def test_condition_clinical_status(self):
        gen = make_gen()
        gen.generate("Patient")
        cond = gen.generate("Condition")[0]
        assert "clinicalStatus" in cond
        cs_code = cond["clinicalStatus"]["coding"][0]["code"]
        assert cs_code in [
            "active", "recurrence", "relapse", "inactive", "remission", "resolved",
        ]

    def test_allergy_intolerance(self):
        gen = make_gen()
        gen.generate("Patient")
        allergy = gen.generate("AllergyIntolerance")[0]
        assert "clinicalStatus" in allergy
        assert "code" in allergy
        assert "reaction" in allergy
        assert allergy["criticality"] in ["low", "high", "unable-to-assess"]

    def test_procedure(self):
        gen = make_gen()
        gen.generate("Patient")
        proc = gen.generate("Procedure")[0]
        assert "code" in proc
        assert "subject" in proc

    def test_diagnostic_report(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Observation", count=3)
        dr = gen.generate("DiagnosticReport")[0]
        assert dr["status"]
        assert dr["code"]

    def test_immunization(self):
        gen = make_gen()
        gen.generate("Patient")
        imm = gen.generate("Immunization")[0]
        assert "vaccineCode" in imm
        assert imm["patient"]["reference"].startswith("Patient/")

    def test_risk_assessment_prediction(self):
        gen = make_gen()
        gen.generate("Patient")
        ra = gen.generate("RiskAssessment")[0]
        assert ra["prediction"]
        assert "probabilityDecimal" in ra["prediction"][0]
