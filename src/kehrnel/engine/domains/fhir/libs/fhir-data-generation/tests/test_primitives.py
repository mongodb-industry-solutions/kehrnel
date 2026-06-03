"""Prompt 4 / 17 — primitive datatype generator tests."""

import base64
import re

import pytest

from fhir_gen.generators.primitives import PrimitiveGenerator

PRIMITIVE_TYPES = [
    "base64Binary", "boolean", "canonical", "code", "date", "dateTime",
    "decimal", "id", "instant", "integer", "integer64", "markdown", "oid",
    "positiveInt", "string", "time", "unsignedInt", "uri", "url", "uuid",
    "xhtml",
]


class TestPrimitiveTypes:
    def test_id_format(self, prim: PrimitiveGenerator):
        id_val = prim.gen_id()
        assert re.match(r"^[A-Za-z0-9\-\.]{1,64}$", id_val)

    def test_boolean(self, prim: PrimitiveGenerator):
        assert prim.gen_boolean() in (True, False)

    def test_integer_range(self, prim: PrimitiveGenerator):
        val = prim.gen_integer(0, 100)
        assert 0 <= val <= 100

    def test_positive_int(self, prim: PrimitiveGenerator):
        assert prim.gen_positiveInt(max_val=100) > 0

    def test_unsigned_int(self, prim: PrimitiveGenerator):
        assert prim.gen_unsignedInt(max_val=100) >= 0

    def test_decimal_precision(self, prim: PrimitiveGenerator):
        val = prim.gen_decimal(0.0, 10.0, precision=2)
        assert isinstance(val, float)
        assert 0.0 <= val <= 10.0
        assert len(str(val).split(".")[-1]) <= 2

    def test_date_formats(self, prim: PrimitiveGenerator):
        for _ in range(20):
            d = prim.gen_date()
            assert re.match(r"^\d{4}(-\d{2}(-\d{2})?)?$", d)

    def test_datetime_format(self, prim: PrimitiveGenerator):
        dt = prim.gen_dateTime()
        assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$", dt)

    def test_instant_format(self, prim: PrimitiveGenerator):
        inst = prim.gen_instant()
        assert inst.endswith("Z")

    def test_time_format(self, prim: PrimitiveGenerator):
        t = prim.gen_time()
        assert re.match(r"^\d{2}:\d{2}:\d{2}$", t)

    def test_uri_format(self, prim: PrimitiveGenerator):
        uri = prim.gen_uri()
        assert uri.startswith("urn:")

    def test_canonical_format(self, prim: PrimitiveGenerator):
        c = prim.gen_canonical()
        assert "|" in c
        assert c.startswith("http")

    def test_oid_format(self, prim: PrimitiveGenerator):
        oid = prim.gen_oid()
        assert oid.startswith("urn:oid:")

    def test_uuid_format(self, prim: PrimitiveGenerator):
        u = prim.gen_uuid()
        assert u.startswith("urn:uuid:")

    def test_base64_binary(self, prim: PrimitiveGenerator):
        b64 = prim.gen_base64Binary(32)
        base64.b64decode(b64)

    def test_xhtml_structure(self, prim: PrimitiveGenerator):
        x = prim.gen_xhtml()
        assert x.startswith('<div xmlns="http://www.w3.org/1999/xhtml">')
        assert "</div>" in x

    def test_seeded_reproducibility(self):
        p1 = PrimitiveGenerator(seed=123)
        p2 = PrimitiveGenerator(seed=123)
        assert p1.gen_id() == p2.gen_id()
        assert p1.gen_dateTime() == p2.gen_dateTime()

    def test_code_from_set(self, prim: PrimitiveGenerator):
        codes = ["active", "inactive", "entered-in-error"]
        c = prim.gen_code(code_set=codes)
        assert c in codes

    def test_dispatch(self, prim: PrimitiveGenerator):
        for type_name in [
            "id", "string", "boolean", "integer", "decimal",
            "date", "dateTime", "instant", "uri", "code",
        ]:
            val = prim.generate(type_name)
            assert val is not None

    def test_all_fhir_primitive_types_dispatchable(self, prim: PrimitiveGenerator):
        for type_name in PRIMITIVE_TYPES:
            assert type_name in prim.PRIMITIVE_TYPES
            val = prim.generate(type_name)
            assert val is not None
