"""Generation scenario coverage — lifecycle and polymorphic choice variants."""

from __future__ import annotations

from datetime import datetime

import pytest

from fhir_gen import ResourceGenerator
from fhir_gen.generators.scenarios import PATIENT_SCENARIOS, scenario_catalog

from .conftest import SEED

pytestmark = pytest.mark.integration


def _gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


class TestPatientScenarioCatalog:
    def test_catalog_includes_deceased_variants(self):
        ids = {s.id for s in PATIENT_SCENARIOS}
        assert "deceased_boolean" in ids
        assert "deceased_datetime" in ids
        assert "alive_active" in ids

    def test_generate_scenarios_count(self):
        gen = _gen()
        gen.generate("Organization", count=1)
        patients = gen.generate_scenarios("Patient")
        assert len(patients) == len(PATIENT_SCENARIOS)


class TestPatientDeceasedBooleanScenario:
    @pytest.fixture
    def deceased_patient(self):
        gen = _gen()
        gen.generate("Organization", count=1)
        for p in gen.generate_scenarios("Patient"):
            if p.get("deceasedBoolean") is True:
                return p
        pytest.fail("no Patient with deceasedBoolean true in scenario batch")

    def test_deceased_boolean_fields(self, deceased_patient):
        p = deceased_patient
        assert p["deceasedBoolean"] is True
        assert "deceasedDateTime" not in p
        assert p["active"] is False
        assert "birthDate" in p

    def test_batch_generation_includes_deceased_boolean(self):
        gen = ResourceGenerator(seed=99)
        gen.generate("Organization", count=1)
        batch = gen.generate("Patient", count=50)
        deceased = [p for p in batch if p.get("deceasedBoolean") is True]
        assert len(deceased) >= 1


class TestPatientDeceasedDateTimeScenario:
    """Patient.deceasedDateTime (death date) — not produced by generate(Patient, count=1)."""

    @pytest.fixture
    def deceased_dt_patient(self):
        gen = _gen()
        gen.generate("Organization", count=1)
        return gen.generate_scenario("Patient", "deceased_datetime")

    def test_generate_scenario_produces_deceased_datetime(self, deceased_dt_patient):
        """Explicit scenario API must set deceasedDateTime (not only deceasedBoolean)."""
        p = deceased_dt_patient
        assert isinstance(p.get("deceasedDateTime"), str)
        assert len(p["deceasedDateTime"]) >= 10
        assert "deceasedBoolean" not in p

    def test_deceased_datetime_fields(self, deceased_dt_patient):
        p = deceased_dt_patient
        assert "deceasedDateTime" in p
        assert "deceasedBoolean" not in p
        assert p["active"] is False
        assert "birthDate" in p
        birth = datetime.strptime(p["birthDate"][:10], "%Y-%m-%d")
        death = datetime.fromisoformat(p["deceasedDateTime"].replace("Z", "+00:00"))
        assert death.date() > birth.date()

    def test_single_generate_count_one_does_not_use_deceased_datetime(self):
        """Default generate(count=1) rotates to catalog[0] (alive_active), not deceased."""
        gen = ResourceGenerator(seed=SEED)
        gen.generate("Organization", count=1)
        p = gen.generate("Patient", count=1)[0]
        assert "deceasedDateTime" not in p

    def test_generate_scenarios_includes_deceased_datetime_record(self):
        gen = _gen()
        gen.generate("Organization", count=1)
        with_dt = [
            p for p in gen.generate_scenarios("Patient")
            if isinstance(p.get("deceasedDateTime"), str)
        ]
        assert len(with_dt) == 1
        assert with_dt[0]["active"] is False

    def test_batch_generation_includes_deceased_datetime(self):
        gen = ResourceGenerator(seed=123)
        gen.generate("Organization", count=1)
        catalog = scenario_catalog("Patient")
        batch = gen.generate("Patient", count=len(catalog) * 6)
        with_dt = [p for p in batch if p.get("deceasedDateTime")]
        assert len(with_dt) >= 1


class TestPatientOtherScenarios:
    def test_alive_active_scenario(self):
        gen = _gen()
        gen.generate("Organization", count=1)
        patients = gen.generate_scenarios("Patient")
        alive = next(p for p in patients if p.get("active") is True and "deceasedBoolean" not in p and "deceasedDateTime" not in p)
        assert alive["active"] is True

    def test_multiple_birth_scenarios(self):
        gen = _gen()
        gen.generate("Organization", count=1)
        patients = gen.generate_scenarios("Patient")
        assert any(p.get("multipleBirthBoolean") is True for p in patients)
        assert any(isinstance(p.get("multipleBirthInteger"), int) for p in patients)

    def test_forced_poly_exclusive_deceased(self):
        gen = _gen()
        gen.generate("Organization", count=1)
        for p in gen.generate_scenarios("Patient"):
            assert not (p.get("deceasedBoolean") is not None and "deceasedDateTime" in p)


class TestPractitionerScenarios:
    def test_practitioner_deceased_boolean(self):
        gen = _gen()
        practitioners = gen.generate_scenarios("Practitioner")
        deceased = next(p for p in practitioners if p.get("deceasedBoolean") is True)
        assert deceased["active"] is False
        assert "deceasedDateTime" not in deceased


class TestPolyScenarioCatalog:
    def test_observation_has_all_value_variants(self):
        from fhir_gen.generators.scenarios import scenario_catalog

        catalog = scenario_catalog("Observation")
        value_ids = {e.id for e in catalog if e.id.startswith("poly_value_")}
        assert len(value_ids) == 13
        assert "poly_value_valueQuantity" in value_ids
        assert "poly_value_valueString" in value_ids

    def test_generate_poly_scenario_by_id(self):
        gen = ResourceGenerator(seed=SEED)
        gen.generate("Patient", count=1)
        obs = gen.generate_scenario("Observation", "poly_value_valueQuantity")
        assert obs["resourceType"] == "Observation"
        assert "valueQuantity" in obs
        assert "valueString" not in obs


class TestScenarioRotation:
    def test_generate_cycles_scenarios(self):
        catalog = scenario_catalog("Patient")
        gen = ResourceGenerator(seed=7)
        gen.generate("Organization", count=1)
        batch = gen.generate("Patient", count=len(catalog) * 2)
        assert len(batch) == len(catalog) * 2
