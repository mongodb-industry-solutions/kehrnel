"""Prompt 6 — extended special/metadata datatype tests (beyond Prompt 17 core set)."""

from fhir_gen.generators.primitives import PrimitiveGenerator
from fhir_gen.generators.special_types import SpecialTypeGenerator

SPECIAL_TYPES = [
    "Meta", "Narrative", "Reference", "Extension", "Dosage",
    "Availability", "ContactDetail", "Contributor", "DataRequirement",
    "Expression", "ExtendedContactDetail", "MonetaryComponent",
    "ParameterDefinition", "TriggerDefinition", "UsageContext",
    "VirtualServiceDetail", "ElementDefinition",
]


class TestSpecialTypesExtended:
    def test_meta_with_profile(self, types: SpecialTypeGenerator):
        meta = types.gen_Meta(profile="http://example.org/StructureDefinition/Patient")
        assert meta["profile"] == ["http://example.org/StructureDefinition/Patient"]

    def test_reference_without_type(self, types: SpecialTypeGenerator):
        ref = types.gen_Reference()
        assert ref["reference"].startswith("urn:uuid:")

    def test_extension_rotates_value_type(self, types: SpecialTypeGenerator):
        keys = set()
        for _ in range(30):
            ext = types.gen_Extension()
            keys.update(k for k in ext if k.startswith("value"))
        assert len(keys) >= 3

    def test_metadata_types(self, types: SpecialTypeGenerator):
        assert "availableTime" in types.gen_Availability()
        assert "name" in types.gen_ContactDetail()
        assert "type" in types.gen_Contributor()
        assert "mustSupport" in types.gen_DataRequirement()
        assert "expression" in types.gen_Expression()
        assert "type" in types.gen_TriggerDefinition()
        assert "code" in types.gen_UsageContext()
        assert "channelType" in types.gen_VirtualServiceDetail()
        assert "factor" in types.gen_MonetaryComponent()
        assert "use" in types.gen_ParameterDefinition()
        assert "path" in types.gen_ElementDefinition()

    def test_dispatch_all_special_types(self, types: SpecialTypeGenerator):
        for type_name in SPECIAL_TYPES:
            result = types.generate(type_name)
            assert isinstance(result, dict)

    def test_inherits_complex_generate(self, types: SpecialTypeGenerator):
        cc = types.generate("CodeableConcept")
        assert "coding" in cc or "text" in cc
        if "coding" in cc:
            assert cc["coding"][0].get("code")

    def test_seeded_reference_reproducible(self):
        s1 = SpecialTypeGenerator(PrimitiveGenerator(seed=99))
        s2 = SpecialTypeGenerator(PrimitiveGenerator(seed=99))
        assert s1.gen_Reference("Patient") == s2.gen_Reference("Patient")
