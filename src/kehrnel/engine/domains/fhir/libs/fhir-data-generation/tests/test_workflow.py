"""Prompt 11 — workflow resource enricher tests."""

from fhir_gen import ResourceGenerator
from fhir_gen.generators.resources.workflow import ENRICHERS

SEED = 42


def make_gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


class TestWorkflowEnrichers:
    def test_enrichers_registered(self):
        assert len(ENRICHERS) == 23
        assert "Appointment" in ENRICHERS
        assert "Questionnaire" in ENRICHERS
        assert "Provenance" in ENRICHERS

    def test_appointment_has_participants(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Practitioner")
        appt = gen.generate("Appointment")[0]
        assert appt["participant"]
        assert len(appt["participant"]) >= 1
        assert "start" in appt and "end" in appt

    def test_care_plan_has_subject(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Goal")
        cp = gen.generate("CarePlan")[0]
        assert cp["status"]
        assert cp["subject"]["reference"].startswith("Patient/")

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
        assert sr["code"] and sr["status"] and sr["subject"]

    def test_slot_links_schedule(self):
        gen = make_gen()
        gen.generate("Practitioner")
        gen.generate("Location")
        gen.generate("Schedule")
        slot = gen.generate("Slot")[0]
        assert slot["schedule"]["reference"].startswith("Schedule/")
        assert "start" in slot and "end" in slot

    def test_document_reference_content(self):
        gen = make_gen()
        gen.generate("Patient")
        doc = gen.generate("DocumentReference")[0]
        assert doc["content"]
        assert "attachment" in doc["content"][0]

    def test_care_team_participants(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Practitioner", count=2)
        team = gen.generate("CareTeam")[0]
        assert team["participant"]

    def test_questionnaire(self):
        gen = make_gen()
        q = gen.generate("Questionnaire")[0]
        assert q["status"] in ["draft", "active", "retired", "unknown"]
        assert q["code"][0]["coding"][0]["system"] == "http://loinc.org"

    def test_device_request(self):
        gen = make_gen()
        dr = gen.generate("DeviceRequest")[0]
        assert dr["code"]["concept"]["coding"][0]["system"] == "http://snomed.info/sct"
        assert dr["subject"]["reference"].startswith("Patient/")

    def test_supply_request_and_delivery(self):
        gen = make_gen()
        sr = gen.generate("SupplyRequest")[0]
        assert sr["category"]["coding"]
        sd = gen.generate("SupplyDelivery")[0]
        assert sd["status"]

    def test_request_orchestration(self):
        gen = make_gen()
        ro = gen.generate("RequestOrchestration")[0]
        assert ro["action"]
        assert ro["action"][0]["resource"]["reference"].startswith("ServiceRequest/")

    def test_nutrition_intake(self):
        gen = make_gen()
        ni = gen.generate("NutritionIntake")[0]
        assert ni["code"]["coding"][0]["system"] == "http://snomed.info/sct"

    def test_provenance(self):
        gen = make_gen()
        prov = gen.generate("Provenance")[0]
        assert prov["activity"]["coding"]
        assert prov["agent"]
