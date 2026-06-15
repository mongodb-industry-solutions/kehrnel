"""Canonical / metadata resource field normalization."""

from fhir_gen import ResourceGenerator


class TestCanonicalResource:
    def test_charge_item_definition_publisher_and_version_algorithm(self):
        gen = ResourceGenerator(seed=42)
        cid = gen.generate("ChargeItemDefinition", count=1)[0]
        assert isinstance(cid["publisher"], str)
        vac = cid["versionAlgorithmCoding"]
        assert vac["system"] == "http://terminology.hl7.org/CodeSystem/version-algorithm"
        assert vac["code"]
        assert "versionAlgorithmString" not in cid

    def test_measure_single_version_algorithm_choice(self):
        gen = ResourceGenerator(seed=7)
        measure = gen.generate("Measure", count=1)[0]
        has_coding = "versionAlgorithmCoding" in measure
        has_string = "versionAlgorithmString" in measure
        assert has_coding ^ has_string or (has_coding and not has_string)
        if has_coding:
            assert measure["versionAlgorithmCoding"].get("code")
