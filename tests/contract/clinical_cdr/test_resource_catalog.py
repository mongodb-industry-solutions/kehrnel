"""Contract tests for the package-backed FHIR resource catalog."""

from __future__ import annotations

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.resource_catalog import (
    CATALOG_CONTRACT_VERSION,
    fhir_resource_catalog,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import MANIFEST

pytest.importorskip("fhir_search_to_mql")
pytest.importorskip("fhir_gen")


def _ctx(release: str = "R5") -> StrategyContext:
    return StrategyContext(
        environment_id="catalog-test",
        config={
            "database": "fhir_catalog_test",
            "schema_version": release,
            "collection_prefix": "fhir_",
        },
        bindings={},
        manifest=MANIFEST,
    )


def test_catalog_lists_only_the_active_strategy_scope_without_database_access():
    catalog = fhir_resource_catalog(_ctx(), {})

    assert catalog["ok"] is True
    assert catalog["contract_version"] == CATALOG_CONTRACT_VERSION
    assert catalog["source"] == "kehrnel.fhir_packages"
    assert catalog["database_backed"] is False
    assert catalog["storage_schema_version"] == "2"
    assert catalog["projection_contract_version"].startswith("v1:")
    assert catalog["resource_count"] == len(catalog["resources"])
    assert catalog["resource_count"] == catalog["schema_resource_count"]
    assert catalog["resource_count"] > catalog["storable_resource_count"]
    assert catalog["storable_resource_count"] == catalog["searchable_resource_count"]
    assert catalog["configured_resource_count"] < catalog["resource_count"]
    assert {recipe["name"] for recipe in catalog["generation_recipes"]} == {
        "clinical_dev",
        "clinical_full84",
    }
    assert {item["resource_type"] for item in catalog["resources"]} >= {
        "Patient",
        "Observation",
        "Condition",
    }
    assert all(item["capabilities"]["schema_supported"] for item in catalog["resources"])


@pytest.mark.parametrize("release", ["R5", "R6"])
def test_patient_detail_joins_schema_search_projection_and_index_metadata(release):
    result = fhir_resource_catalog(_ctx(release), {"resource_type": "Patient"})
    patient = result["resource"]

    assert result["fhir_version"] == release
    assert patient["collection"] == "fhir_Patient"
    assert patient["storage"]["database"] == "fhir_catalog_test"
    assert patient["storage"]["canonical_location"] == "document root"
    assert result["resource_projection_version"].startswith("v1:")
    assert "_kehrnel" in patient["storage"]["operational_fields"]
    assert patient["structure"]["root"] == "Patient"
    assert any(field["name"] == "name" and field["type"] == "HumanName" for field in patient["structure"]["fields"])
    assert any(group["name"] == "deceased[x]" for group in patient["structure"]["polymorphic"])
    assert any(parameter["name"] == "identifier" for parameter in patient["search"]["parameters"])
    assert patient["storage"]["indexes"]
    assert any(index["name"] == "idx_family_name_lower" for index in patient["storage"]["indexes"])


def test_catalog_describes_schema_resource_without_search_mapping():
    result = fhir_resource_catalog(_ctx(), {"resource_type": "CapabilityStatement"})

    assert result["resource"]["capabilities"]["schema_supported"] is True
    assert result["resource"]["capabilities"]["storable"] is False
    assert result["resource"]["search"]["parameter_count"] == 0
    assert result["resource_projection_version"] is None


def test_catalog_rejects_unknown_resource_type():
    with pytest.raises(KehrnelError) as exc_info:
        fhir_resource_catalog(_ctx(), {"resource_type": "NotARealFHIRResource"})

    assert exc_info.value.code == "FHIR_RESOURCE_DEFINITION_UNAVAILABLE"
    assert exc_info.value.status == 404
