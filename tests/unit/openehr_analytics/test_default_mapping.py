from copy import deepcopy
from pathlib import Path
import json
import pytest
from kehrnel.engine.domains.openehr.analytics import Converter, RuleSet
from kehrnel.engine.domains.openehr.analytics.default_mapping import (
    propose_from_opt,
    finalize_proposal,
)
from kehrnel.engine.domains.openehr.analytics.source import SourceError, digest

ROOT = Path(__file__).resolve().parents[3]
SAMPLE = ROOT / "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference"
AT = "2026-11-18T00:00:00Z"


def proposal():
    return propose_from_opt(
        (SAMPLE / "templates/sample_laboratory_v0_4.opt").read_bytes(),
        epoch=1,
        effective_at=AT,
    )


def test_defaults_cover_clinical_fields_without_empty_placeholders():
    p = proposal()
    assert all(
        f["selected"] for f in p["fields"] if f["disposition"] != "composition_context"
    )
    assert p == proposal()
    r = finalize_proposal(p)
    groups = r["catalog"]["templates"][0]["groups"]
    analyte = next(g for g in groups if "laboratory_test_analyte" in g["anchor"])
    assert analyte["anchor"].endswith(
        "[openEHR-EHR-CLUSTER.laboratory_test_analyte.v1]"
    )
    assert all(
        b["missing_policy"] == "omit_optional" for g in groups for b in g["fields"]
    )
    assert all(v["type"] == "raw" for v in r["catalog"]["variables"])
    excluded = analyte["fields"][0]["field_id"]
    revised = finalize_proposal(p, exclude_field_ids=[excluded])
    assert revised["revision"] != r["revision"]
    assert all(
        b["field_id"] != excluded
        for g in revised["catalog"]["templates"][0]["groups"]
        for b in g["fields"]
    )
    with pytest.raises(ValueError, match="Unknown excluded"):
        finalize_proposal(p, exclude_field_ids=["nonexistent"])


def test_unresolved_selected_fields_cannot_silently_disappear():
    p = proposal()
    f = next(f for f in p["fields"] if f["selected"])
    p["issues"].append(
        {"code": "PROTOCOL_CORRELATION_REVIEW", "field_id": f["field_id"]}
    )
    p["revision"] = digest({k: v for k, v in p.items() if k != "revision"})
    with pytest.raises(ValueError, match="Resolve or explicitly exclude"):
        finalize_proposal(p)
    finalize_proposal(p, exclude_field_ids=[f["field_id"]])


def fixture():
    folder = ROOT / "examples/openehr_analytics"
    cfg = json.loads((folder / "config.canonical.json").read_text())
    doc = json.loads((folder / "composition.canonical.json").read_text())
    rule = cfg["rules"][0]
    group = rule["catalog"]["templates"][0]["groups"][0]
    group["definition"]["required_fields"] = []
    group["retain_null_facts"] = True
    for v in rule["catalog"]["variables"]:
        v.update(type="raw")
        v.pop("unit", None)
    for b in group["fields"]:
        b.pop("conversions", None)
        b["multiple"] = True
        b["missing_policy"] = "omit_optional"
    return cfg, doc, group


def test_complete_repeated_values_and_null_reasons_are_preserved():
    cfg, doc, group = fixture()
    items = doc["canonicalJSON"]["content"][0]["data"]["events"][0]["data"]["items"]
    items.append(deepcopy(items[0]))
    items[-1]["value"]["magnitude"] = 155
    items[-1]["value"]["units"] = "mm[Hg]"
    result = Converter(RuleSet.compile(cfg["rules"]), source_namespace="test").convert(
        doc, accepted_at=AT
    )
    values = result.facts[0]["clinical"]["blood_pressure"]["systolic"]
    assert [v["magnitude"] for v in values] == [150, 155]
    assert values[-1]["units"] == "mm[Hg]"
    items[-1].pop("value")
    items[-1]["null_flavour"] = {"value": "not measured"}
    result = Converter(RuleSet.compile(cfg["rules"]), source_namespace="test").convert(
        doc, accepted_at=AT
    )
    assert result.facts[0]["clinical"]["blood_pressure"]["systolic"][-1] is None
    assert (
        result.facts[0]["quality"]["blood_pressure.systolic"]["items"][-1]["state"]
        == "null_flavour"
    )


def test_multiple_flag_cannot_flatten_across_distinct_events():
    cfg, doc, group = fixture()
    group["anchor"] = group["definition"]["entry_path"]
    group["definition"]["entry_type"] = "EVALUATION"
    # Field resolution precedes RM interpretation: ancestor-instance protection must fire.
    with pytest.raises(SourceError, match="container instances"):
        Converter(RuleSet.compile(cfg["rules"]), source_namespace="test").convert(
            doc, accepted_at=AT
        )


def test_absent_optional_fields_are_omitted_but_recorded_null_fact_is_retained():
    cfg, doc, group = fixture()
    for b in group["fields"]:
        b.pop("multiple")
    for entry in doc["canonicalJSON"]["content"]:
        for event in entry["data"]["events"]:
            event["data"]["items"] = []
    entry = doc["canonicalJSON"]["content"][0]
    entry["data"]["events"][0]["data"]["items"] = [
        {
            "_type": "ELEMENT",
            "archetype_node_id": "at0004",
            "null_flavour": {"value": "not measured"},
        }
    ]
    result = Converter(RuleSet.compile(cfg["rules"]), source_namespace="test").convert(
        doc, accepted_at=AT
    )
    assert len(result.facts) == 1
    assert result.facts[0]["clinical"] == {}
    assert set(result.facts[0]["quality"]) == {"blood_pressure.systolic"}


def test_cross_template_aliases_cannot_merge_different_semantic_fields():
    value = finalize_proposal(proposal())
    other = deepcopy(value)
    other.pop("_id")
    other.pop("revision")
    other["catalog"]["templates"][0]["template_name"] = "Another template"
    # This catalog identity test uses a synthetic binding, not an OPT-derived rule.
    other["catalog"]["templates"][0].pop("source_coverage")
    other["catalog"]["variables"][0]["semantic_key"] = "another-semantic-identity"
    with pytest.raises(ValueError, match="semantic identity"):
        RuleSet.compile([value, other])


def test_strategy_exposes_offline_default_mapping():
    import asyncio
    from kehrnel.engine.core.types import StrategyContext
    from kehrnel.engine.strategies.openehr.analytics.strategy import (
        OpenEHRAnalyticsStrategy,
    )

    output = asyncio.run(
        OpenEHRAnalyticsStrategy().run_op(
            StrategyContext("defaults", {}),
            "analytics_default_mapping",
            {
                "opt_xml": (
                    SAMPLE / "templates/sample_laboratory_v0_4.opt"
                ).read_text(),
                "effective_at": AT,
            },
        )
    )
    assert output["ok"] and output["proposal"]["status"] == "draft"
