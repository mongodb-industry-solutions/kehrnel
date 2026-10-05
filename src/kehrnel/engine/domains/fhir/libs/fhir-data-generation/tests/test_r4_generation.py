"""R4 FHIR schema version and US Core profile generation tests."""

from pathlib import Path

import pytest

from fhir_gen.config import V4_SCHEMA_PATH, settings
from fhir_gen.schema.versions import normalize_schema_version, resolve_schema_path, supported_schema_versions


# ── Version routing ──────────────────────────────────────────────────────────

def test_r4_in_supported_versions():
    assert "R4" in supported_schema_versions()


def test_r4_aliases():
    for label in ("R4", "r4", "4", "v4", "V4", "FHIR4"):
        assert resolve_schema_path(schema_version=label) == V4_SCHEMA_PATH, f"Failed for alias: {label}"


def test_r4_schema_file_exists():
    assert V4_SCHEMA_PATH.exists(), f"R4 schema not found at {V4_SCHEMA_PATH}"
    assert V4_SCHEMA_PATH.stat().st_size > 1_000_000, "R4 schema file seems too small"


def test_r4_schema_is_valid_json():
    import json
    with open(V4_SCHEMA_PATH, encoding="utf-8") as f:
        data = json.load(f)
    assert "definitions" in data, "R4 schema missing 'definitions' key"
    assert "Patient" in data["definitions"], "R4 schema missing Patient definition"


def test_normalize_r4_version():
    assert normalize_schema_version("r4") == "R4"
    assert normalize_schema_version("4") == "R4"


def test_unknown_version_still_raises():
    with pytest.raises(ValueError, match="Unknown"):
        resolve_schema_path(schema_version="R3")


def test_r4_schema_path_override_wins():
    """Explicit schema_path overrides schema_version even for R4."""
    custom = V4_SCHEMA_PATH
    assert resolve_schema_path(schema_version="R5", schema_path=custom) == custom


# ── R4 generation smoke tests ────────────────────────────────────────────────

@pytest.fixture(autouse=False)
def r4_registry():
    """Reload SchemaRegistry with R4 schema for the test, restore R5 after."""
    from fhir_gen.schema.registry import SchemaRegistry
    from fhir_gen.schema.versions import resolve_schema_path
    from fhir_gen.config import settings
    original_version = settings.schema_version
    settings.schema_version = "R4"
    SchemaRegistry.reload(resolve_schema_path(schema_version="R4"))
    yield
    settings.schema_version = original_version
    SchemaRegistry.reload(resolve_schema_path(schema_version="R5"))


def test_r4_patient_generates(r4_registry):
    from fhir_gen import ResourceGenerator
    gen = ResourceGenerator(seed=42)
    patients = gen.generate("Patient", count=3)
    assert len(patients) == 3
    for p in patients:
        assert p["resourceType"] == "Patient"
        assert "id" in p
        assert "meta" in p


def test_r4_encounter_class_is_coding(r4_registry):
    """R4 Encounter.class must be a single Coding dict, not an array."""
    from fhir_gen import ResourceGenerator
    gen = ResourceGenerator(seed=42)
    gen.generate("Encounter", count=2)
    encounters = [r for r in gen.store.all_resources() if r["resourceType"] == "Encounter"]
    assert encounters, "No Encounter resources generated"
    for enc in encounters:
        if "class" in enc:
            assert isinstance(enc["class"], dict), f"R4 Encounter.class should be dict, got {type(enc['class'])}"
            assert "coding" not in enc["class"], "R4 Encounter.class should be Coding, not CodeableConcept"


def test_r4_encounter_uses_period_not_actual_period(r4_registry):
    """R4 Encounter uses 'period', not 'actualPeriod'."""
    from fhir_gen import ResourceGenerator
    gen = ResourceGenerator(seed=42)
    gen.generate("Encounter", count=2)
    encounters = [r for r in gen.store.all_resources() if r["resourceType"] == "Encounter"]
    for enc in encounters:
        assert "actualPeriod" not in enc, "R4 Encounter must not have 'actualPeriod'"


def test_r4_medication_request_uses_r4_fields(r4_registry):
    """R4 MedicationRequest uses medicationCodeableConcept or medicationReference, not medication CodeableReference."""
    from fhir_gen import ResourceGenerator
    gen = ResourceGenerator(seed=42)
    gen.generate("MedicationRequest", count=3)
    resources = gen.store.all_resources()
    mrs = [r for r in resources if r["resourceType"] == "MedicationRequest"]
    for mr in mrs:
        assert "medication" not in mr or not isinstance(mr.get("medication"), dict) or \
               "concept" not in mr.get("medication", {}), \
               "R4 MedicationRequest must not have R5 CodeableReference 'medication.concept'"
        has_r4_field = (
            "medicationCodeableConcept" in mr or
            "medicationReference" in mr
        )
        assert has_r4_field, f"R4 MedicationRequest missing medicationCodeableConcept or medicationReference"


def test_r4_no_version_algorithm_on_canonical(r4_registry):
    """R4 canonical resources must not have versionAlgorithmCoding (R5+ only)."""
    from fhir_gen import ResourceGenerator
    from fhir_gen.config import settings
    # Ensure settings.schema_version is R4 for the duration of this test
    # (the r4_registry fixture sets it, but we re-assert here for robustness)
    assert settings.schema_version == "R4", f"Expected R4, got {settings.schema_version}"
    assert settings.fhir_version == "R4", f"Expected fhir_version R4, got {settings.fhir_version}"
    gen = ResourceGenerator(seed=42)
    gen.generate("Questionnaire", count=2)
    resources = gen.store.all_resources()
    for r in resources:
        assert "versionAlgorithmCoding" not in r, \
            f"{r['resourceType']} has R5-only field 'versionAlgorithmCoding' (fhir_version={settings.fhir_version})"
        assert "versionAlgorithmString" not in r, \
            f"{r['resourceType']} has R5-only field 'versionAlgorithmString'"


# ── US Core profile pack tests ───────────────────────────────────────────────

def test_us_core_profile_pack_requires_r4():
    """apply_profile_pack must raise ValueError when schema_version != R4."""
    import random
    from fhir_gen.profiles.applicator import apply_profile_pack
    rng = random.Random(42)
    resource = {"resourceType": "Patient", "id": "test-1"}
    with pytest.raises(ValueError, match="R4"):
        apply_profile_pack(resource, "us_core", "R5", rng)


def test_us_core_patient_gets_meta_profile(r4_registry):
    """US Core Patient must have meta.profile stamped with us-core-patient URL."""
    import random
    from fhir_gen.profiles.applicator import apply_profile_pack
    rng = random.Random(42)
    resource = {"resourceType": "Patient", "id": "test-1", "meta": {}}
    result = apply_profile_pack(resource, "us_core", "R4", rng)
    assert "meta" in result
    assert "profile" in result["meta"]
    assert "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient" in result["meta"]["profile"]


def test_us_core_patient_gets_race_extension(r4_registry):
    """US Core Patient should have race extension added."""
    import random
    from fhir_gen.profiles.applicator import apply_profile_pack
    from fhir_gen.profiles.us_core.terminology import EXT_RACE
    # Run many times to account for probabilistic generation
    found_race = False
    for seed in range(20):
        rng = random.Random(seed)
        resource = {"resourceType": "Patient", "id": f"test-{seed}"}
        result = apply_profile_pack(resource, "us_core", "R4", rng)
        extensions = result.get("extension", [])
        if any(ext.get("url") == EXT_RACE for ext in extensions):
            found_race = True
            break
    assert found_race, "US Core Patient should have race extension in at least some cases"


def test_us_core_unknown_pack_raises():
    """Unknown profile pack name must raise ValueError."""
    import random
    from fhir_gen.profiles.applicator import apply_profile_pack
    rng = random.Random(42)
    resource = {"resourceType": "Patient", "id": "test-1"}
    with pytest.raises(ValueError, match="Unknown profile_pack"):
        apply_profile_pack(resource, "unknown_pack", "R4", rng)


def test_profile_registry_lists_us_core():
    from fhir_gen.profiles.registry import list_packs, get_pack
    packs = list_packs()
    assert "us_core" in packs
    spec = get_pack("us_core")
    assert spec.fhir_version == "R4"
    assert "us.core" in spec.ig_name
