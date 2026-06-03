"""Prompt 11 — workflow resource enricher tests."""

from fhir_gen import ResourceGenerator
from fhir_gen.generators.resources.workflow import ENRICHERS

SEED = 42


def make_gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


class TestWorkflowEnrichers:
    def test_enrichers_registered(self):
        assert len(ENRICHERS) == 13
        assert "Appointment" in ENRICHERS
        assert "Slot" in ENRICHERS

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
