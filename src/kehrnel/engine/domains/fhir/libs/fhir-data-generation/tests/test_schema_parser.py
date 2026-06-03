"""Prompt 2 — FHIR schema parser and registry tests."""

import pytest

from fhir_gen.config import settings
from fhir_gen.schema import FHIRSchemaParser, SchemaRegistry, registry


@pytest.fixture
def parser() -> FHIRSchemaParser:
    return FHIRSchemaParser(settings.resolved_schema_path)


def test_all_resources_count(parser: FHIRSchemaParser):
    resources = parser.get_all_resources()
    assert len(resources) == 158
    assert "Patient" in resources
    assert "Observation" in resources


def test_patient_resource_type_const(parser: FHIRSchemaParser):
    patient = parser.parse_definition("Patient")
    assert patient.is_resource
    assert patient.fields["resourceType"].const_value == "Patient"
    assert "name" in patient.fields
    assert patient.fields["name"].is_array


def test_observation_polymorphic_value_group(parser: FHIRSchemaParser):
    obs = parser.parse_definition("Observation")
    assert "value" in obs.poly_groups
    variants = obs.poly_groups["value"]
    assert "valueQuantity" in variants
    assert "valueString" in variants
    assert len(variants) >= 3


def test_extract_ref_array(parser: FHIRSchemaParser):
    obs = parser.parse_definition("Observation")
    assert obs.fields["category"].is_array
    assert obs.fields["category"].ref == "CodeableConcept"


def test_unknown_definition_raises(parser: FHIRSchemaParser):
    with pytest.raises(KeyError, match="Unknown definition"):
        parser.parse_definition("NotARealType")


def test_registry_definition_cached():
    reg = SchemaRegistry.get()
    a = reg.definition("Encounter")
    b = reg.definition("Encounter")
    assert a is b


def test_registry_all_resources():
    names = registry.all_resources()
    assert len(names) == 158
    assert names == sorted(names)


def test_references_for_observation(parser: FHIRSchemaParser):
    refs = parser.get_references_for("Observation")
    assert "Patient" in refs


def test_parse_all_count(parser: FHIRSchemaParser):
    all_defs = parser.parse_all()
    assert len(all_defs) == 857
    assert "Quantity" in all_defs
    assert all_defs["Quantity"].is_resource is False
