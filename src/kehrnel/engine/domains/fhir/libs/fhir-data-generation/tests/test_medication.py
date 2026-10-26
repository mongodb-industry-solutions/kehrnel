"""Prompt 10 — medication resource enricher tests."""

from fhir_gen import ResourceGenerator
from fhir_gen.generators.resources.medication import ENRICHERS

SEED = 42


def make_gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


class TestMedicationEnrichers:
    def test_enrichers_registered(self):
        assert len(ENRICHERS) == 6
        assert "MedicationRequest" in ENRICHERS

    def test_medication_has_rxnorm_code(self):
        med = make_gen().generate("Medication")[0]
        assert "code" in med
        coding = med["code"]["coding"][0]
        assert coding["system"] == "http://www.nlm.nih.gov/research/umls/rxnorm"

    def test_medication_request_chain(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Practitioner")
        mr = gen.generate("MedicationRequest")[0]
        assert mr["subject"]["reference"].startswith("Patient/")
        assert "medication" in mr
        assert "dosageInstruction" in mr
        assert "concept" in mr["medication"] or "reference" in mr["medication"]

    def test_medication_administration(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("MedicationRequest")
        ma = gen.generate("MedicationAdministration")[0]
        assert "medication" in ma
        assert "subject" in ma
        assert "dosage" in ma

    def test_medication_dispense(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("MedicationRequest")
        md = gen.generate("MedicationDispense")[0]
        assert "medication" in md
        assert "subject" in md
        assert "quantity" in md

    def test_medication_statement(self):
        gen = make_gen()
        gen.generate("Patient")
        stmt = gen.generate("MedicationStatement")[0]
        assert stmt["resourceType"] == "MedicationStatement"
        assert "medication" in stmt
        assert "dosage" in stmt
