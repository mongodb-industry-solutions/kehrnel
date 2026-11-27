"""Template name matching and conflicting exports must never pick a silent winner."""

import importlib.util
import json
from pathlib import Path
import zipfile
import pytest

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location(
    "catalog_analytics_templates", ROOT / "scripts/catalog_analytics_templates.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
AT = "2026-11-18T00:00:00Z"
SAMPLE = (
    ROOT
    / "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/templates/sample_laboratory_v0_4.opt"
)


def test_catalog_matches_internal_id_and_keeps_conflicting_definitions(tmp_path):
    xml = SAMPLE.read_bytes()
    name, _, _ = module.inspect_opt(xml)
    changed = xml.replace(b"<upper>1</upper>", b"<upper>2</upper>", 1)
    assert changed != xml
    report = module.build_catalog(
        [name, "filename-is-not-a-template"],
        [("wrong-filename.opt", xml, None), ("second.opt", changed, None)],
        tmp_path / "catalog",
        effective_at=AT,
    )
    states = {r["template"]: r for r in report["templates"]}
    assert states[name]["mapping_status"] == "ambiguous_opt_definitions"
    assert len(states[name]["variants"]) == 2
    assert (
        states["filename-is-not-a-template"]["mapping_status"]
        == "opt_not_found_in_supplied_catalog"
    )
    assert json.loads((tmp_path / "catalog/rules.json").read_text()) == []


def test_authoring_metadata_does_not_create_a_second_rule(tmp_path):
    xml = SAMPLE.read_bytes()
    tree = module.etree.fromstring(xml)
    description = tree.find(module.NS + "description")
    assert description is not None
    description.text = "different export authoring metadata"
    changed = module.etree.tostring(tree)
    name, digest, fingerprint = module.inspect_opt(xml)
    assert module.inspect_opt(changed)[1] != digest
    assert module.inspect_opt(changed)[2] == fingerprint
    report = module.build_catalog(
        [name],
        [("one.opt", xml, None), ("two.opt", changed, None)],
        tmp_path / "catalog",
        effective_at=AT,
    )
    assert report["templates"][0]["mapping_status"] == "default_draft_generated"
    assert len(json.loads((tmp_path / "catalog/rules.json").read_text())) == 1


def test_zip_inputs_never_extract_and_dtd_is_rejected(tmp_path):
    with zipfile.ZipFile(tmp_path / "templates.zip", "w") as z:
        z.writestr("../../outside.opt", SAMPLE.read_bytes())
        z.writestr("composition.json", '{"patient": "not read"}')
    inputs = list(module.opt_inputs(tmp_path))
    assert len(inputs) == 1
    assert inputs[0][0].endswith("!../../outside.opt")
    assert not (tmp_path.parent / "outside.opt").exists()
    xml = b'<!DOCTYPE template [<!ENTITY secret SYSTEM "file:///etc/passwd">]><template xmlns="http://schemas.openehr.org/v1"><template_id><value>&secret;</value></template_id></template>'
    with pytest.raises(ValueError, match="DTD"):
        module.inspect_opt(xml)
