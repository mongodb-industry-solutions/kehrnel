"""Prompt 18 — resource enricher integration tests across clinical, workflow, financial, and specialized modules."""

from fhir_gen.generators.base import ResourceGenerator

from .conftest import SEED


def make_gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


class TestClinicalResources:
    def test_patient_structure(self):
        gen = make_gen()
        patients = gen.generate("Patient", count=3)
        assert len(patients) == 3
        for p in patients:
            assert p["resourceType"] == "Patient"
            assert "id" in p and "meta" in p
            assert p["gender"] in ["male", "female", "other", "unknown"]
            assert "birthDate" in p
            assert "name" in p and len(p["name"]) > 0
            assert "identifier" in p and len(p["identifier"]) > 0

    def test_practitioner_structure(self):
        gen = make_gen()
        pr = gen.generate("Practitioner")[0]
        assert pr["resourceType"] == "Practitioner"
        assert "name" in pr and "identifier" in pr
        assert pr["active"] is True

    def test_organization_structure(self):
        gen = make_gen()
        org = gen.generate("Organization")[0]
        assert org["resourceType"] == "Organization"
        assert "name" in org and len(org["name"]) > 0

    def test_encounter_references_patient(self):
        gen = make_gen()
        gen.generate("Patient", count=2)
        enc = gen.generate("Encounter")[0]
        assert "subject" in enc
        assert enc["subject"]["reference"].startswith("Patient/")

    def test_observation_has_value(self):
        gen = make_gen()
        gen.generate("Patient")
        obs = gen.generate("Observation")[0]
        assert obs["resourceType"] == "Observation"
        assert "code" in obs
        value_fields = [k for k in obs if k.startswith("value")]
        assert len(value_fields) > 0

    def test_condition_has_clinical_status(self):
        gen = make_gen()
        gen.generate("Patient")
        cond = gen.generate("Condition")[0]
        assert "clinicalStatus" in cond
        assert "coding" in cond["clinicalStatus"]
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

    def test_procedure_has_body_site(self):
        gen = make_gen()
        gen.generate("Patient")
        proc = gen.generate("Procedure")[0]
        assert "code" in proc and "status" in proc
        assert "subject" in proc

    def test_diagnostic_report(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Observation", count=3)
        dr = gen.generate("DiagnosticReport")[0]
        assert "status" in dr and "code" in dr

    def test_immunization(self):
        gen = make_gen()
        gen.generate("Patient")
        imm = gen.generate("Immunization")[0]
        assert "vaccineCode" in imm
        assert "patient" in imm
        assert imm["status"] in ["completed", "not-done"]


class TestMedicationResources:
    def test_medication_has_rxnorm_code(self):
        gen = make_gen()
        med = gen.generate("Medication")[0]
        assert "code" in med
        coding = med["code"]["coding"][0]
        assert coding["system"] == "http://www.nlm.nih.gov/research/umls/rxnorm"

    def test_medication_request_chain(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Practitioner")
        mr = gen.generate("MedicationRequest")[0]
        assert "subject" in mr
        assert mr["subject"]["reference"].startswith("Patient/")
        assert "medication" in mr
        assert "dosageInstruction" in mr

    def test_medication_administration(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("MedicationRequest")
        ma = gen.generate("MedicationAdministration")[0]
        assert "medication" in ma and "subject" in ma
        assert "dosage" in ma

    def test_medication_dispense(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("MedicationRequest")
        md = gen.generate("MedicationDispense")[0]
        assert "medication" in md and "subject" in md
        assert "quantity" in md


class TestWorkflowResources:
    def test_appointment_has_participants(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Practitioner")
        appt = gen.generate("Appointment")[0]
        assert "participant" in appt
        assert len(appt["participant"]) >= 1
        assert "start" in appt and "end" in appt

    def test_care_plan_has_goal(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Goal")
        cp = gen.generate("CarePlan")[0]
        assert "status" in cp
        assert "subject" in cp

    def test_task_has_status(self):
        gen = make_gen()
        gen.generate("Patient")
        task = gen.generate("Task")[0]
        assert task["status"] in [
            "draft", "requested", "received", "accepted",
            "in-progress", "completed", "cancelled",
        ]

    def test_service_request(self):
        gen = make_gen()
        gen.generate("Patient")
        sr = gen.generate("ServiceRequest")[0]
        assert "code" in sr and "status" in sr and "subject" in sr


class TestFinancialResources:
    def test_coverage_structure(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        cov = gen.generate("Coverage")[0]
        assert "beneficiary" in cov and "insurer" in cov
        assert "period" in cov

    def test_claim_with_coverage(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        gen.generate("Coverage")
        gen.generate("Practitioner")
        claim = gen.generate("Claim")[0]
        assert "patient" in claim and "insurer" in claim
        assert "insurance" in claim


class TestSpecializedResources:
    def test_specimen_collection(self):
        gen = make_gen()
        gen.generate("Patient")
        spec = gen.generate("Specimen")[0]
        assert "type" in spec and "collection" in spec

    def test_imaging_study(self):
        gen = make_gen()
        gen.generate("Patient")
        img = gen.generate("ImagingStudy")[0]
        assert "modality" in img
        assert "series" in img and len(img["series"]) > 0

    def test_device(self):
        gen = make_gen()
        dev = gen.generate("Device")[0]
        assert "name" in dev and len(dev["name"]) > 0

    def test_research_study(self):
        gen = make_gen()
        gen.generate("Organization")
        rs = gen.generate("ResearchStudy")[0]
        assert "title" in rs and "status" in rs

    def test_group(self):
        gen = make_gen()
        gen.generate("Patient", count=3)
        grp = gen.generate("Group")[0]
        assert "type" in grp and "quantity" in grp
