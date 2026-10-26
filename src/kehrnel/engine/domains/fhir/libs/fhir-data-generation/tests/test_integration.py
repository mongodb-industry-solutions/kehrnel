"""Prompt 18 — end-to-end integration tests for dependency order, references, bundles, and data quality."""

from datetime import datetime

import pytest

from fhir_gen.generators.base import ResourceGenerator
from fhir_gen.resolvers.dependency import resolve_order

pytestmark = pytest.mark.integration

INTEGRATION_SEED = 99


class TestDependencyResolution:
    def test_patient_has_no_deps(self):
        order = resolve_order(["Patient"])
        assert "Patient" in order

    def test_observation_after_patient(self):
        order = resolve_order(["Observation", "Patient"])
        assert order.index("Patient") < order.index("Observation")

    def test_encounter_after_patient(self):
        order = resolve_order(["Encounter", "Patient"])
        assert order.index("Patient") < order.index("Encounter")

    def test_claim_after_coverage(self):
        order = resolve_order(["Claim", "Coverage", "Patient", "Organization"])
        assert order.index("Coverage") < order.index("Claim")
        assert order.index("Patient") < order.index("Claim")

    def test_medication_request_chain(self):
        order = resolve_order([
            "MedicationRequest", "Patient", "Practitioner", "Encounter",
        ])
        assert order.index("Patient") < order.index("MedicationRequest")
        assert order.index("Practitioner") < order.index("MedicationRequest")


class TestCrossResourceReferences:
    def test_encounter_references_registered_patient(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        patients = gen.generate("Patient", count=3)
        patient_ids = {f"Patient/{p['id']}" for p in patients}
        encounters = gen.generate("Encounter", count=5)
        for enc in encounters:
            if "subject" in enc:
                assert enc["subject"]["reference"] in patient_ids

    def test_observation_references_registered_encounter(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        gen.generate("Patient", count=2)
        gen.generate("Encounter", count=2)
        observations = gen.generate("Observation", count=5)
        for obs in observations:
            if "encounter" in obs:
                ref = obs["encounter"]["reference"]
                assert gen.store.reference_is_valid(ref), f"Broken encounter ref: {ref}"

    def test_medication_request_references_patient(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        patients = gen.generate("Patient", count=2)
        gen.generate("Practitioner", count=1)
        patient_ids = {f"Patient/{p['id']}" for p in patients}
        mrs = gen.generate("MedicationRequest", count=3)
        for mr in mrs:
            if "subject" in mr:
                assert mr["subject"]["reference"] in patient_ids


class TestPolymorphicVariants:
    def test_generate_variants_observation(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        gen.generate("Patient", count=1)
        variants = gen.generate_variants("Observation")
        assert len(variants) >= 2
        value_keys = {k for v in variants for k in v if k.startswith("value")}
        assert len(value_keys) >= 2


class TestGenerateMany:
    def test_generate_many_basic(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        results = gen.generate_many(
            ["Patient", "Practitioner", "Organization", "Encounter"],
            counts={
                "Patient": 3,
                "Practitioner": 2,
                "Organization": 2,
                "Encounter": 5,
            },
        )
        assert len(results["Patient"]) == 3
        assert len(results["Practitioner"]) == 2
        assert len(results["Encounter"]) == 5

    def test_generate_many_clinical_bundle(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        resources = [
            "Patient", "Practitioner", "Organization", "Location",
            "Encounter", "Condition", "Observation", "MedicationRequest",
            "AllergyIntolerance", "Procedure",
        ]
        counts = {r: 2 for r in resources}
        counts["Patient"] = 5
        results = gen.generate_many(resources, counts=counts)
        for rtype in resources:
            assert rtype in results
            assert len(results[rtype]) > 0

    def test_all_resources_have_required_fields(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        results = gen.generate_many(
            ["Patient", "Condition", "Observation", "MedicationRequest"],
            counts={
                "Patient": 2,
                "Condition": 2,
                "Observation": 2,
                "MedicationRequest": 2,
            },
        )
        for rtype, resources in results.items():
            for resource in resources:
                assert "resourceType" in resource, f"{rtype} missing resourceType"
                assert "id" in resource, f"{rtype} missing id"
                assert resource["resourceType"] == rtype


class TestDataCorrectness:
    def test_observation_values_in_range(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        gen.generate("Patient", count=2)
        observations = gen.generate("Observation", count=20)
        for obs in observations:
            if "valueQuantity" in obs and "referenceRange" in obs:
                val = obs["valueQuantity"]["value"]
                low = obs["referenceRange"][0]["low"]["value"]
                high = obs["referenceRange"][0]["high"]["value"]
                assert low <= val <= high, f"Value {val} outside range [{low}, {high}]"

    def test_patient_age_reasonable(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        patients = gen.generate("Patient", count=20)
        for p in patients:
            if "birthDate" in p and len(p["birthDate"]) == 10:
                bd = datetime.strptime(p["birthDate"], "%Y-%m-%d")
                age_days = (datetime.now() - bd).days
                assert 0 <= age_days <= 365 * 120, f"Unreasonable age: {age_days} days"

    def test_medication_request_status_valid(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        gen.generate("Patient")
        gen.generate("Practitioner")
        mrs = gen.generate("MedicationRequest", count=10)
        valid = [
            "active", "on-hold", "cancelled", "completed",
            "entered-in-error", "stopped", "draft", "unknown",
        ]
        for mr in mrs:
            assert mr["status"] in valid

    def test_encounter_period_order(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        gen.generate("Patient")
        encounters = gen.generate("Encounter", count=5)
        for enc in encounters:
            if "actualPeriod" in enc:
                start = enc["actualPeriod"].get("start", "")
                end = enc["actualPeriod"].get("end", "")
                if start and end:
                    assert start <= end


class TestReferenceIntegrity:
    def test_store_registers_generated_resources(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        patients = gen.generate("Patient", count=3)
        assert gen.store.count("Patient") == 3
        for p in patients:
            ref = gen.store.get_reference("Patient", gen.rng)
            assert ref is not None
            assert ref["reference"].startswith("Patient/")

    def test_find_resource_by_reference(self):
        gen = ResourceGenerator(seed=INTEGRATION_SEED)
        patients = gen.generate("Patient", count=2)
        p = patients[0]
        entry = gen.store._store["Patient"][0]
        assert entry["id"] == p["id"]
        assert entry["reference"] == f"Patient/{p['id']}"
