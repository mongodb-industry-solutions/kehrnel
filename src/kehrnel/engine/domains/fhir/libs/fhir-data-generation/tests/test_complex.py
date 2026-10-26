"""Prompt 5 / 17 — complex and special datatype generator tests."""

import pytest

from fhir_gen.generators.complex_types import ComplexTypeGenerator
from fhir_gen.generators.special_types import SpecialTypeGenerator

COMPLEX_TYPES = [
    "Address", "Age", "Annotation", "Attachment", "CodeableConcept", "Coding",
    "ContactPoint", "Count", "Distance", "Duration", "HumanName", "Identifier",
    "Money", "MoneyQuantity", "Period", "Quantity", "Range", "Ratio", "RatioRange",
    "SampledData", "Signature", "SimpleQuantity", "Timing",
]


class TestComplexTypes:
    def test_identifier(self, ct: ComplexTypeGenerator):
        ident = ct.gen_Identifier()
        assert "system" in ident and "value" in ident

    def test_human_name(self, ct: ComplexTypeGenerator):
        name = ct.gen_HumanName()
        assert "family" in name and "given" in name
        assert isinstance(name["given"], list)

    def test_address(self, ct: ComplexTypeGenerator):
        addr = ct.gen_Address()
        assert all(k in addr for k in ("line", "city", "postalCode"))

    def test_contact_point_phone(self, ct: ComplexTypeGenerator):
        cp = ct.gen_ContactPoint(system="phone")
        assert cp["system"] == "phone"
        assert "value" in cp

    def test_contact_point_email(self, ct: ComplexTypeGenerator):
        cp = ct.gen_ContactPoint(system="email")
        assert "@" in cp["value"]

    def test_codeable_concept(self, ct: ComplexTypeGenerator):
        cc = ct.gen_CodeableConcept(
            system="http://snomed.info/sct", code="123", display="Test"
        )
        assert "coding" in cc and "text" in cc
        assert cc["coding"][0]["code"] == "123"

    def test_coding(self, ct: ComplexTypeGenerator):
        coding = ct.gen_Coding(
            system="http://loinc.org", code="8867-4", display="Heart rate"
        )
        assert coding["code"] == "8867-4"

    def test_period(self, ct: ComplexTypeGenerator):
        period = ct.gen_Period()
        assert "start" in period and "end" in period
        assert period["end"] > period["start"]

    def test_quantity(self, ct: ComplexTypeGenerator):
        q = ct.gen_Quantity(value=5.5, unit="mg")
        assert q["value"] == 5.5 and q["unit"] == "mg"

    def test_simple_quantity(self, ct: ComplexTypeGenerator):
        q = ct.gen_SimpleQuantity(value=1.0, unit="g")
        assert "comparator" not in q

    def test_range(self, ct: ComplexTypeGenerator):
        r = ct.gen_Range(low_val=10.0, high_val=20.0)
        assert r["low"]["value"] < r["high"]["value"]

    def test_ratio(self, ct: ComplexTypeGenerator):
        ratio = ct.gen_Ratio()
        assert "numerator" in ratio and "denominator" in ratio

    def test_timing(self, ct: ComplexTypeGenerator):
        timing = ct.gen_Timing()
        assert "repeat" in timing
        assert timing["repeat"]["frequency"] >= 1

    def test_annotation(self, ct: ComplexTypeGenerator):
        ann = ct.gen_Annotation()
        assert "text" in ann and "time" in ann

    def test_money(self, ct: ComplexTypeGenerator):
        m = ct.gen_Money()
        assert "value" in m and m["currency"] == "USD"

    def test_age(self, ct: ComplexTypeGenerator):
        age = ct.gen_Age(value=45.0)
        assert age["value"] == 45.0 and age["code"] == "a"

    def test_duration(self, ct: ComplexTypeGenerator):
        d = ct.gen_Duration(value=7.0, unit="d")
        assert d["value"] == 7.0 and d["code"] == "d"

    def test_sampled_data(self, ct: ComplexTypeGenerator):
        sd = ct.gen_SampledData()
        assert "data" in sd and "origin" in sd
        assert len(sd["data"].split(" ")) >= 10

    def test_count_distance_money_quantity(self, ct: ComplexTypeGenerator):
        assert ct.gen_Count(value=1.0)["value"] == 1.0
        assert ct.gen_Distance(unit="km")["unit"] == "km"
        assert ct.gen_MoneyQuantity(value=2.0)["value"] == 2.0

    def test_ratio_range(self, ct: ComplexTypeGenerator):
        rr = ct.gen_RatioRange()
        assert "lowRatio" in rr and "highRatio" in rr

    def test_signature(self, ct: ComplexTypeGenerator):
        sig = ct.gen_Signature()
        assert sig["who"]["reference"].startswith("Practitioner/")
        assert "when" in sig

    def test_dispatch_all_complex_types(self, ct: ComplexTypeGenerator):
        for type_name in COMPLEX_TYPES:
            assert type_name in ct.COMPLEX_TYPES
            result = ct.generate(type_name)
            assert isinstance(result, dict)

    def test_unknown_type_raises(self, ct: ComplexTypeGenerator):
        with pytest.raises(ValueError, match="No generator"):
            ct.generate("NotAComplexType")


class TestSpecialTypes:
    def test_meta(self, types: SpecialTypeGenerator):
        meta = types.gen_Meta()
        assert "versionId" in meta and "lastUpdated" in meta

    def test_narrative(self, types: SpecialTypeGenerator):
        n = types.gen_Narrative()
        assert n["status"] == "generated"
        assert n["div"].startswith("<div")

    def test_reference_with_type(self, types: SpecialTypeGenerator):
        ref = types.gen_Reference("Patient", "abc-123")
        assert ref["reference"] == "Patient/abc-123"
        assert ref["type"] == "Patient"

    def test_extension(self, types: SpecialTypeGenerator):
        ext = types.gen_Extension(url="http://example.org/ext", value_type="boolean")
        assert ext["url"] == "http://example.org/ext"
        assert "valueBoolean" in ext

    def test_dosage(self, types: SpecialTypeGenerator):
        d = types.gen_Dosage()
        assert "timing" in d and "doseAndRate" in d
        assert d["doseAndRate"][0]["doseQuantity"]["unit"] == "mg"
