"""Prompt 8 / 17 — ResourceGenerator engine tests."""

from fhir_gen import ResourceGenerator
from fhir_gen.schema.registry import registry

from .conftest import SEED


class TestResourceGenerator:
    def test_generate_patient(self, gen: ResourceGenerator):
        patients = gen.generate("Patient", count=2)
        assert len(patients) == 2
        for patient in patients:
            assert patient["resourceType"] == "Patient"
            assert "id" in patient
            assert "meta" in patient

    def test_seeded_reproducibility(self):
        a = ResourceGenerator(seed=99).generate("Organization", count=1)[0]
        b = ResourceGenerator(seed=99).generate("Organization", count=1)[0]
        assert a["id"] == b["id"]

    def test_generate_auto_dependencies(self, gen: ResourceGenerator):
        enc = gen.generate("Encounter", count=1)[0]
        assert enc["resourceType"] == "Encounter"
        assert gen.store.has("Patient")

    def test_generate_many(self, gen: ResourceGenerator):
        results = gen.generate_many(
            ["Patient", "Practitioner", "Encounter"],
            counts={"Patient": 2, "Practitioner": 1, "Encounter": 2},
        )
        assert len(results["Patient"]) == 2
        assert len(results["Encounter"]) == 2
        assert gen.store.count("Patient") >= 2

    def test_generate_variants_observation(self, gen: ResourceGenerator):
        gen.generate("Patient", count=1)
        variants = gen.generate_variants("Observation", variant_fields=["value"])
        assert len(variants) >= 2
        value_keys = {k for v in variants for k in v if k.startswith("value")}
        assert len(value_keys) >= 2
        for variant in variants:
            present = [k for k in variant if k.startswith("value")]
            assert len(present) == 1

    def test_polymorphic_single_choice(self, gen: ResourceGenerator):
        obs = gen.generate("Observation", count=1)[0]
        value_keys = [k for k in obs if k.startswith("value")]
        assert len(value_keys) <= 1

    def test_all_resources_generatable(self):
        for name in registry.all_resources():
            r = ResourceGenerator(seed=SEED).generate(name, count=1)[0]
            assert r["resourceType"] == name
            assert r["id"]

    def test_overrides(self, gen: ResourceGenerator):
        r = gen.generate("Patient", count=1, overrides={"active": False})[0]
        assert r["active"] is False


class TestEngineWithCoreFixtures:
    def test_gen_with_core_observation_references_patient(self, gen_with_core: ResourceGenerator):
        obs = gen_with_core.generate("Observation", count=1)[0]
        assert obs["subject"]["reference"].startswith("Patient/")
        assert gen_with_core.store.count("Patient") >= 3

    def test_reference_store_fixture(self, store, gen: ResourceGenerator):
        patient = gen.generate("Patient", count=1)[0]
        store.register(patient)
        ref = store.get_reference("Patient", gen.rng)
        assert ref["reference"] == f"Patient/{patient['id']}"
