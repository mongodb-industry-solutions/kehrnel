"""Unit/regression tests for all 84 fhir-search-to-mql shipped resources.

Registry alignment (deps, enrichers, resolve_order) plus per-resource generation,
enriched fields, reference integrity, and terminology validation.

Run integration-only batch/chain tests: ``pytest -m integration tests/test_mql_integration.py``
"""

from __future__ import annotations

import pytest

from fhir_gen import ResourceGenerator
from fhir_gen.codes.validation import validate_resource_codings
from fhir_gen.generators.resources import ENRICHERS
from fhir_gen.resolvers.dependency import (
    CORE_DEPENDENCIES,
    MQL_SHIPPED_RESOURCES,
    assert_mql_dependencies_complete,
    resolve_order,
)

from .mql_resource_checks import assert_enriched_fields, assert_references_valid

SEED = 42


def test_mql_shipped_list_has_84_resources() -> None:
    assert len(MQL_SHIPPED_RESOURCES) == 84


def test_all_mql_resources_have_core_dependencies() -> None:
    assert_mql_dependencies_complete()
    for name in MQL_SHIPPED_RESOURCES:
        assert name in CORE_DEPENDENCIES
        assert isinstance(CORE_DEPENDENCIES[name], list)


def test_resolve_order_includes_transitive_deps() -> None:
    order = resolve_order(["MeasureReport"])
    assert "Measure" in order
    assert order.index("Measure") < order.index("MeasureReport")
    assert "Patient" in order
    assert order.index("Patient") < order.index("MeasureReport")


def test_all_mql_resources_have_enrichers() -> None:
    missing = [r for r in MQL_SHIPPED_RESOURCES if r not in ENRICHERS]
    assert not missing, f"Add enrichers for: {missing}"


@pytest.fixture
def gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


@pytest.mark.parametrize("resource_type", MQL_SHIPPED_RESOURCES)
def test_generate_each_mql_resource(gen: ResourceGenerator, resource_type: str) -> None:
    docs = gen.generate(resource_type, count=1)
    assert len(docs) == 1
    doc = docs[0]
    assert doc["resourceType"] == resource_type
    assert doc.get("id")


@pytest.mark.parametrize("resource_type", MQL_SHIPPED_RESOURCES)
def test_dependencies_precreated(gen: ResourceGenerator, resource_type: str) -> None:
    """Generating one resource should materialize declared CORE_DEPENDENCIES."""
    gen.generate(resource_type, count=1)
    for dep in CORE_DEPENDENCIES[resource_type]:
        assert gen.store.has(dep), (
            f"{resource_type} depends on {dep} but store has no {dep} after generate"
        )


@pytest.mark.parametrize("resource_type", MQL_SHIPPED_RESOURCES)
def test_enriched_fields_present(gen: ResourceGenerator, resource_type: str) -> None:
    doc = gen.generate(resource_type, count=1)[0]
    assert_enriched_fields(resource_type, doc)


@pytest.mark.parametrize("resource_type", MQL_SHIPPED_RESOURCES)
def test_internal_references_valid(gen: ResourceGenerator, resource_type: str) -> None:
    doc = gen.generate(resource_type, count=1)[0]
    assert_references_valid(gen.store, doc)


@pytest.mark.parametrize("resource_type", MQL_SHIPPED_RESOURCES)
def test_codings_pass_validation(gen: ResourceGenerator, resource_type: str) -> None:
    doc = gen.generate(resource_type, count=1)[0]
    errors = validate_resource_codings(doc, strict_registered=True)
    assert not errors, f"{resource_type}: {errors[:5]}"
