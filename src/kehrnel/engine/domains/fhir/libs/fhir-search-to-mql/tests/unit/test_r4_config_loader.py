"""R4 FHIR version config loader tests."""

import pytest
from pathlib import Path

from fhir_search_to_mql.core.config_loader import ConfigLoader
from fhir_search_to_mql.core.exceptions import MissingConfigurationError
from fhir_search_to_mql.core.constants import FHIR_VERSIONS


def test_fhir_versions_includes_r4():
    assert "R4" in FHIR_VERSIONS


def test_r4_schema_file_exists():
    """The R4 schema file must be present in the schema/ directory."""
    schema_path = Path(__file__).parent.parent.parent / "schema" / "fhir.schema.v4.json"
    assert schema_path.exists(), f"R4 schema not found at {schema_path}"
    assert schema_path.stat().st_size > 1_000_000, "R4 schema file seems too small"


def test_r4_patient_yaml_exists():
    """The R4 Patient YAML config must exist."""
    configs_r4 = Path(__file__).parent.parent.parent / "src" / "fhir_search_to_mql" / "configs" / "r4"
    patient_yaml = configs_r4 / "Patient.yaml"
    assert patient_yaml.exists(), f"R4 Patient config not found at {patient_yaml}"


def test_r4_config_loader_loads_r4_patient():
    """ConfigLoader must load R4 Patient config from configs/r4/ directory."""
    configs_r4 = Path(__file__).parent.parent.parent / "src" / "fhir_search_to_mql" / "configs" / "r4"
    loader = ConfigLoader(config_dir=str(configs_r4))
    config = loader.get_config("Patient")
    assert config["resource"] == "Patient"
    assert config["fhir_version"] == "R4"


def test_r4_plain_config_has_no_profile_search_param():
    """Plain R4 Patient config must NOT have _profile — that is US Core only."""
    configs_r4 = Path(__file__).parent.parent.parent / "src" / "fhir_search_to_mql" / "configs" / "r4"
    loader = ConfigLoader(config_dir=str(configs_r4))
    config = loader.get_config("Patient")
    search_params = config.get("search_parameters", {})
    assert "_profile" not in search_params, (
        "Plain R4 Patient config must NOT have _profile. "
        "Use configs/r4-us-core/ for US Core profile search support."
    )


def test_r4_us_core_config_has_profile_search_param():
    """R4 + US Core Patient config must have _profile search parameter."""
    configs_r4 = Path(__file__).parent.parent.parent / "src" / "fhir_search_to_mql" / "configs" / "r4"
    configs_r4_us_core = Path(__file__).parent.parent.parent / "src" / "fhir_search_to_mql" / "configs" / "r4-us-core"
    if not configs_r4_us_core.exists():
        pytest.skip("configs/r4-us-core/ not yet created")
    # Layer: plain R4 first, US Core overlay wins for same resource
    loader = ConfigLoader(config_dir=[str(configs_r4), str(configs_r4_us_core)])
    config = loader.get_config("Patient", fhir_version="R4", profile="us-core")
    search_params = config.get("search_parameters", {})
    assert "_profile" in search_params, (
        "R4 + US Core Patient config must have _profile search parameter"
    )
    assert search_params["_profile"]["type"] == "uri"


def test_versioned_config_cache_populated():
    """ConfigLoader must populate _versioned_config_cache for R4 configs."""
    configs_r4 = Path(__file__).parent.parent.parent / "src" / "fhir_search_to_mql" / "configs" / "r4"
    loader = ConfigLoader(config_dir=str(configs_r4))
    assert hasattr(loader, "_versioned_config_cache"), "ConfigLoader must have _versioned_config_cache"
    assert ("Patient", "R4") in loader._versioned_config_cache


def test_get_config_with_fhir_version_r4():
    """get_config(resource, fhir_version='R4') must return R4 config."""
    configs_r4 = Path(__file__).parent.parent.parent / "src" / "fhir_search_to_mql" / "configs" / "r4"
    loader = ConfigLoader(config_dir=str(configs_r4))
    config = loader.get_config("Patient", fhir_version="R4")
    assert config["fhir_version"] == "R4"


def test_r4_encounter_config_has_r4_field_paths():
    """R4 Encounter config must use 'period' (not 'actualPeriod') and 'individual' (not 'actor')."""
    configs_r4 = Path(__file__).parent.parent.parent / "src" / "fhir_search_to_mql" / "configs" / "r4"
    loader = ConfigLoader(config_dir=str(configs_r4))
    config = loader.get_config("Encounter")
    denorm = config.get("denormalization", {})
    # Check period (not actualPeriod)
    assert "period" in denorm, "R4 Encounter denorm must use 'period' not 'actualPeriod'"
    assert "actualPeriod" not in denorm, "R4 Encounter must not have 'actualPeriod' in denorm"
    # Check participant uses individual path
    if "participant" in denorm:
        mappings = denorm["participant"].get("field_mappings", [])
        paths = [m.get("source_path", "") for m in mappings]
        assert any("individual" in p for p in paths), \
            "R4 Encounter participant must use 'individual' path (not 'actor')"


def test_r4_configs_all_have_correct_fhir_version():
    """All configs in configs/r4/ must declare fhir_version: R4."""
    configs_r4 = Path(__file__).parent.parent.parent / "src" / "fhir_search_to_mql" / "configs" / "r4"
    if not configs_r4.exists():
        pytest.skip("configs/r4/ directory not yet created")
    import yaml
    for yaml_file in configs_r4.glob("*.yaml"):
        with open(yaml_file, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        assert config.get("fhir_version") == "R4", \
            f"{yaml_file.name} must declare fhir_version: R4"
