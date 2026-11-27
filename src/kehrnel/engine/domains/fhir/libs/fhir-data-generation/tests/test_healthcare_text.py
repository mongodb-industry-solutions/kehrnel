"""Healthcare narrative text helpers."""

import random

from fhir_gen.generators.healthcare_text import clinical_text


class TestHealthcareText:
    def test_clinical_not_lorem_like(self):
        rng = random.Random(42)
        text = clinical_text(
            rng, resource_type="Encounter", field_name="description"
        )
        lowered = text.lower()
        assert any(
            w in lowered
            for w in (
                "visit", "encounter", "clinical", "admission", "outpatient",
                "emergency", "surgical", "hospital",
            )
        )

    def test_field_specific_comment(self):
        rng = random.Random(1)
        text = clinical_text(rng, resource_type="Observation", field_name="comment")
        lowered = text.lower()
        assert any(
            w in lowered
            for w in (
                "specimen", "patient", "laboratory", "protocol", "bedside",
                "glucose", "population", "hemolyzed",
            )
        )

    def test_seeded_reproducibility(self):
        a = clinical_text(
            random.Random(99), resource_type="Task", field_name="description"
        )
        b = clinical_text(
            random.Random(99), resource_type="Task", field_name="description"
        )
        assert a == b

    def test_industrial_payer_claim(self):
        rng = random.Random(7)
        text = clinical_text(rng, resource_type="Claim", field_name="description")
        assert any(
            w in text.lower()
            for w in ("claim", "charge", "facility", "professional", "pharmacy")
        )

    def test_industrial_supply_blood_bank(self):
        rng = random.Random(11)
        text = clinical_text(
            rng, resource_type="BiologicallyDerivedProduct", field_name="description"
        )
        assert any(
            w in text.lower() for w in ("blood", "platelet", "red", "rh")
        )

    def test_population_health_group(self):
        rng = random.Random(13)
        text = clinical_text(rng, resource_type="Group", field_name="description")
        assert any(
            w in text.lower()
            for w in ("cohort", "registry", "population", "risk", "panel")
        )
