from pathlib import Path
import json

from kehrnel.engine.domains.openehr.templates import (
    TemplateParser,
    kehrnelGenerator,
    kehrnelValidator,
)


SAMPLE_LAB_OPT = Path(
    "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/templates/sample_laboratory_v0_4.opt"
)


def _validate_generated(builder: str):
    tpl = TemplateParser(SAMPLE_LAB_OPT)
    gen = kehrnelGenerator(tpl)
    comp = getattr(gen, builder)()
    return kehrnelValidator(tpl).validate(comp), comp


def test_sample_laboratory_minimal_generation_validates_cleanly():
    issues, comp = _validate_generated("generate_minimal")

    assert issues == []
    assert comp["context"]["other_context"]["items"][0]["items"][5]["value"]["_type"] == "DV_CODED_TEXT"
    assert comp["content"][0]["name"]["value"] == "Laboratory test results list"
    assert comp["content"][0]["items"][0]["name"]["value"] == "Laboratory test results"


def test_sample_laboratory_random_generation_validates_cleanly():
    issues, comp = _validate_generated("generate_random")

    assert issues == []
    assert comp["content"][0]["items"][0]["data"]["events"][0]["data"]["items"][1]["items"][1]["name"]["value"] == "Test result"


def test_sample_laboratory_reference_canonical_examples_validate_cleanly():
    tpl = TemplateParser(SAMPLE_LAB_OPT)
    validator = kehrnelValidator(tpl)
    sample_dir = Path(
        "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/canonical/sample_laboratory_v0_4"
    )

    for sample in sorted(sample_dir.glob("*.json")):
        comp = json.loads(sample.read_text())
        assert validator.validate(comp) == [], sample.name
