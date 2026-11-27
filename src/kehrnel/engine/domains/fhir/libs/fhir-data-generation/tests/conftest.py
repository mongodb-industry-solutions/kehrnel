"""Shared pytest fixtures for datatype, engine, and integration tests."""

import pytest

from fhir_gen.generators.base import ResourceGenerator
from fhir_gen.generators.complex_types import ComplexTypeGenerator
from fhir_gen.generators.primitives import PrimitiveGenerator
from fhir_gen.generators.special_types import SpecialTypeGenerator
from fhir_gen.resolvers.reference import ReferenceStore

SEED = 42


@pytest.fixture(scope="session")
def prim() -> PrimitiveGenerator:
    return PrimitiveGenerator(seed=SEED)


@pytest.fixture(scope="session")
def types(prim: PrimitiveGenerator) -> SpecialTypeGenerator:
    return SpecialTypeGenerator(prim)


@pytest.fixture(scope="session")
def ct(prim: PrimitiveGenerator) -> ComplexTypeGenerator:
    return ComplexTypeGenerator(prim)


@pytest.fixture
def store() -> ReferenceStore:
    return ReferenceStore()


@pytest.fixture
def gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


@pytest.fixture
def gen_with_core(gen: ResourceGenerator) -> ResourceGenerator:
    """Generator with Patient, Practitioner, Organization, Location pre-generated."""
    gen.generate("Patient", count=3)
    gen.generate("Practitioner", count=2)
    gen.generate("Organization", count=2)
    gen.generate("Location", count=2)
    return gen
