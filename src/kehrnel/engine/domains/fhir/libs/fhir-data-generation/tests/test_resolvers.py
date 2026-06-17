"""Prompt 7 — dependency graph and reference store tests."""

import random

import pytest

from fhir_gen.resolvers import CORE_DEPENDENCIES, ReferenceStore, resolve_order


class TestResolveOrder:
    def test_encounter_patient_before_encounter(self):
        order = resolve_order(["Encounter"])
        assert order.index("Patient") < order.index("Encounter")

    def test_observation_includes_encounter_chain(self):
        order = resolve_order(["Observation"])
        assert "Patient" in order
        assert "Encounter" in order
        assert order.index("Patient") < order.index("Encounter")
        assert order.index("Encounter") < order.index("Observation")

    def test_medication_request_includes_medication(self):
        order = resolve_order(["MedicationRequest"])
        assert "Medication" in order
        assert order.index("Medication") < order.index("MedicationRequest")

    def test_multiple_resources_shared_deps_once(self):
        order = resolve_order(["Observation", "Condition"])
        assert order.count("Patient") == 1
        assert order.index("Patient") < order.index("Observation")
        assert order.index("Patient") < order.index("Condition")

    def test_slot_includes_schedule(self):
        order = resolve_order(["Slot"])
        assert "Schedule" in order
        assert order.index("Schedule") < order.index("Slot")

    def test_care_plan_transitive_deps(self):
        order = resolve_order(["CarePlan"])
        for dep in ("Patient", "Practitioner", "CareTeam", "Condition", "Goal"):
            assert dep in order
        assert order.index("Patient") < order.index("CarePlan")

    def test_research_subject_chain(self):
        order = resolve_order(["ResearchSubject"])
        assert "ResearchStudy" in order
        assert "Patient" in order
        assert order.index("Patient") < order.index("ResearchSubject")

    def test_empty_list(self):
        assert resolve_order([]) == []

    def test_patient_only(self):
        assert resolve_order(["Patient"]) == ["Patient"]


class TestReferenceStore:
    @pytest.fixture
    def store(self) -> ReferenceStore:
        return ReferenceStore()

    @pytest.fixture
    def rng(self):
        return random.Random(42)

    def test_register_and_get_reference(self, store: ReferenceStore, rng):
        store.register({
            "resourceType": "Patient",
            "id": "p1",
            "name": [{"family": "Smith", "given": ["John"]}],
        })
        ref = store.get_reference("Patient", rng)
        assert ref is not None
        assert ref["reference"] == "Patient/p1"
        assert ref["type"] == "Patient"
        assert "Smith" in ref["display"]

    def test_has_and_count(self, store: ReferenceStore):
        store.register({"resourceType": "Patient", "id": "a"})
        store.register({"resourceType": "Patient", "id": "b"})
        assert store.has("Patient")
        assert store.count("Patient") == 2
        assert not store.has("Practitioner")

    def test_get_id(self, store: ReferenceStore, rng):
        store.register({"resourceType": "Organization", "id": "org-1", "name": "Acme"})
        rid = store.get_id("Organization", rng)
        assert rid == "org-1"

    def test_organization_display(self, store: ReferenceStore, rng):
        store.register({"resourceType": "Organization", "id": "o1", "name": "General Hospital"})
        ref = store.get_reference("Organization", rng)
        assert ref["display"] == "General Hospital"

    def test_medication_display(self, store: ReferenceStore, rng):
        store.register({
            "resourceType": "Medication",
            "id": "m1",
            "code": {"coding": [{"display": "Aspirin 81mg"}]},
        })
        ref = store.get_reference("Medication", rng)
        assert ref["display"] == "Aspirin 81mg"

    def test_register_skips_invalid(self, store: ReferenceStore):
        store.register({"resourceType": "Patient"})
        store.register({"id": "x"})
        assert store.count("Patient") == 0

    def test_clear(self, store: ReferenceStore):
        store.register({"resourceType": "Patient", "id": "p1"})
        store.clear("Patient")
        assert not store.has("Patient")
        store.register({"resourceType": "Patient", "id": "p2"})
        store.clear()
        assert store.count("Patient") == 0

    def test_get_reference_missing_type(self, store: ReferenceStore, rng):
        assert store.get_reference("Patient", rng) is None


class TestCoreDependencies:
    def test_core_map_has_clinical_resources(self):
        assert "Patient" in CORE_DEPENDENCIES
        assert "MedicationRequest" in CORE_DEPENDENCIES
        assert "Appointment" in CORE_DEPENDENCIES
