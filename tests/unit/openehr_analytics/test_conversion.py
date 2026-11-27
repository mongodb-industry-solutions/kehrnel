from copy import deepcopy
from pathlib import Path
import json
import pytest
from bson.binary import Binary
from uuid import uuid4
from kehrnel.engine.domains.openehr.analytics import (
    Converter,
    RuleSet,
    SourceFields,
    IBMDecoder,
    IBMFields,
)
from kehrnel.engine.domains.openehr.analytics.source import SourceError
from kehrnel.engine.domains.openehr.analytics.store import decide

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = ROOT / "examples/openehr_analytics"


@pytest.fixture
def config():
    return json.loads((EXAMPLES / "config.canonical.json").read_text())


@pytest.fixture
def document():
    return json.loads((EXAMPLES / "composition.canonical.json").read_text())


def convert(config, document, **kwargs):
    return Converter(
        RuleSet.compile(config["rules"]), source_namespace="reference"
    ).convert(document, accepted_at="2026-11-01T12:00:00Z", **kwargs)


def ibm_document(document):
    from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import (
        CompositionFlattener,
    )

    shortcuts = json.loads(
        (
            ROOT
            / "src/kehrnel/engine/strategies/openehr/rps_dual/bundles/shortcuts/shortcuts.json"
        ).read_text()
    )
    flattener = CompositionFlattener(
        None,
        {
            "paths": {"separator": ":"},
            "composition_fields": {"path_instance": "li"},
            "collections": {"search": {"enabled": False}},
        },
        "",
        mappings_content={"templates": []},
    )
    flattener.shortcut_keys = shortcuts["keys"]
    flattener.shortcut_vals = shortcuts["values"]
    # Use the real writer's structural encoder, including retained wrapper skeletons.
    nodes = []
    flattener._walk(
        deepcopy(document["canonicalJSON"]), (), (), nodes, kp_chain=[], list_index=None
    )
    for node in nodes:
        node.pop("_anc", None)
    source = {k: deepcopy(v) for k, v in document.items() if k != "canonicalJSON"}
    source["cn"] = flattener._apply_sc_deep(nodes)
    return source, IBMDecoder(
        flattener.code_book, shortcuts, IBMFields(value_prefix="")
    )


def test_bp_pairs_and_repeated_observations(config, document):
    result = convert(config, document)
    assert len(result.facts) == 4
    pairs = [
        (
            f["clinical"]["blood_pressure"]["systolic"],
            f["clinical"]["blood_pressure"]["diastolic"],
        )
        for f in result.facts
    ]
    assert pairs == [(150, 65), (127, 92), (135, 80), (120, 75)]
    assert not any(s >= 140 and d >= 90 for s, d in pairs)
    assert len({f["_id"] for f in result.facts}) == 4
    assert all("quality" not in f and "patient" not in f for f in result.facts)
    assert all("anchor_path" not in f["lineage"] for f in result.facts)


def test_real_rps_writer_roundtrip_preserves_parent_instances(config, document):
    raw, decoder = ibm_document(document)
    converter = Converter(
        RuleSet.compile(config["rules"]), source_namespace="reference", decoder=decoder
    )
    actual = converter.convert(raw, accepted_at="2026-11-01T12:00:00Z")
    expected = convert(config, document)
    assert [f["clinical"] for f in actual.facts] == [
        f["clinical"] for f in expected.facts
    ]
    assert [f["_id"] for f in actual.facts] == [f["_id"] for f in expected.facts]
    assert [f["context"] for f in actual.facts] == [
        f["context"] for f in expected.facts
    ]
    # Full chains permit node reordering; source array order is not a join key.
    raw["cn"].reverse()
    assert [
        f["clinical"]
        for f in converter.convert(raw, accepted_at="2026-11-01T12:00:00Z").facts
    ] == [f["clinical"] for f in expected.facts]


def test_missing_required_rejects_composition(config, document):
    document["canonicalJSON"]["content"][0]["data"]["events"][0]["data"]["items"][
        0
    ].pop("value")
    with pytest.raises(SourceError, match="Required interpretation"):
        convert(config, document)


def test_repeat_is_never_zipped_by_position(config, document):
    items = document["canonicalJSON"]["content"][0]["data"]["events"][0]["data"][
        "items"
    ]
    items.append(deepcopy(items[0]))
    with pytest.raises(SourceError, match="crosses a repeat"):
        convert(config, document)


def test_unit_conversion_and_ttl(config, document):
    rule = config["rules"][0]
    rule["retention"] = {"mode": "delete_facts", "clock": "clinical_time", "days": 10}
    document["canonicalJSON"]["content"][0]["data"]["events"][0]["data"]["items"][0][
        "value"
    ].update(magnitude=20, units="kPa")
    result = convert(config, document)
    fact = result.facts[0]
    assert fact["clinical"]["blood_pressure"]["systolic"] == pytest.approx(150.0123366)
    assert fact["quality"]["blood_pressure.systolic"]["state"] == "converted"
    assert (fact["expires_at"] - fact["context"]["event_at"]).days == 10
    assert "expires_at" not in result.activity


def test_unknown_template_preserves_activity_not_guessed_facts(config, document):
    document["template_name"] = "New Template.v2"
    result = convert(config, document)
    assert result.status == "needs_rules" and result.activity and not result.facts
    assert result.source_ref["clinical_mapping"] == "activity_only"


def test_subject_identity_is_protected(config, document):
    document["canonicalJSON"]["content"][0]["subject"] = {"_type": "PARTY_RELATED"}
    with pytest.raises(SourceError, match="related clinical subject"):
        convert(config, document)


def test_scalar_anchor_is_not_a_fact(config, document):
    group = config["rules"][0]["catalog"]["templates"][0]["groups"][0]
    group["anchor"] = group["fields"][0]["path"]
    group["fields"] = group["fields"][:1]
    group["definition"]["required_fields"] = ["blood_pressure.systolic"]
    with pytest.raises(SourceError, match="scalar nodes"):
        convert(config, document)


def test_rule_revision_roundtrip_and_activation(config):
    rules = RuleSet.compile(config["rules"])
    r = next(iter(rules.by_revision.values()))
    assert RuleSet.compile([r.document()]).revision == rules.revision
    assert rules.select(r.template, "2019-12-31T23:59:59Z") is None
    assert rules.select(r.template, "2020-01-01T00:00:00Z") == r
    second = deepcopy(config["rules"][0])
    second["epoch"] = 2
    second["effective_at"] = "2027-01-01T00:00:00Z"
    changed = RuleSet.compile([config["rules"][0], second])
    new = changed.by_template[r.template][-1]
    assert changed.select(r.template, "2026-01-01T00:00:00Z").revision == r.revision
    assert changed.select(r.template, "2026-01-01T00:00:00Z", new.revision) == new


def test_cross_template_units_must_agree(config):
    other = deepcopy(config["rules"][0])
    other["catalog"]["templates"][0]["template_name"] = "another"
    other["catalog"]["variables"][0]["unit"] = "kPa"
    # Remove conversion of the newly canonical unit so only cross-template checks fail.
    other["catalog"]["templates"][0]["groups"][0]["fields"][0]["conversions"] = {}
    with pytest.raises(ValueError, match="Cross-template"):
        RuleSet.compile([config["rules"][0], other])


def test_uuid_binary_and_native_use_same_identity(config, document):
    cid, pid = uuid4(), uuid4()
    document.update(_id=cid, ehr_id=pid)
    a = convert(config, document)
    document.update(_id=Binary.from_uuid(cid), ehr_id=Binary.from_uuid(pid))
    b = convert(config, document)
    assert (
        a.composition_id == b.composition_id
        and a.facts[0]["context"] == b.facts[0]["context"]
    )


def test_stale_and_conflicting_replays(config, document):
    state = convert(config, document).state()
    assert decide(state, state) == "unchanged"
    with pytest.raises(ValueError, match="content changed"):
        decide(state, {**state, "source_fingerprint": "different"})
    assert decide({**state, "source_version": 2}, state) == "stale"
    with pytest.raises(ValueError, match="Concurrent"):
        decide({**state, "rule_epoch": 2}, {**state, "source_version": 2})
    assert (
        decide(
            {**state, "rule_epoch": 5},
            {**state, "source_version": 2, "template": "new.v2"},
        )
        == "write"
    )


def test_decoder_rejects_incomplete_instance_chain(document):
    source, decoder = ibm_document(document)
    node = next(n for n in source["cn"] if "li" in n)
    node["li"] = node["li"][0]
    with pytest.raises(SourceError, match="Instance chain"):
        decoder.decode(source)


def test_local_preorder_indexes_reconstruct_only_contiguous_parents(config, document):
    raw, full = ibm_document(document)
    for node in raw["cn"]:
        if "li" in node:
            node["li"] = node["li"][0]
    local = deepcopy(full)
    local.fields = IBMFields(value_prefix="", instance_mode="local_preorder")
    assert (
        local.decode(raw)["content"][1]["data"]["events"][0]["data"]["items"][0][
            "value"
        ]["magnitude"]
        == 135
    )
    # Move a grandchild ahead of its actual parent; fail rather than guess ancestry.
    raw["cn"][1], raw["cn"][-1] = raw["cn"][-1], raw["cn"][1]
    with pytest.raises(SourceError, match="preorder"):
        local.decode(raw)


@pytest.mark.parametrize("rm", ["EVALUATION", "ACTION", "INSTRUCTION", "ADMIN_ENTRY"])
def test_entry_profiles_keep_interpretation(config, document, rm):
    group = config["rules"][0]["catalog"]["templates"][0]["groups"][0]
    entry = "/content[openEHR-EHR-" + rm + ".reference.v1]"
    field = deepcopy(
        document["canonicalJSON"]["content"][0]["data"]["events"][0]["data"]["items"][0]
    )
    node = {
        "_type": rm,
        "archetype_node_id": "openEHR-EHR-" + rm + ".reference.v1",
        "data": {"_type": "ITEM_TREE", "archetype_node_id": "at0001", "items": [field]},
        "time": {"_type": "DV_DATE_TIME", "value": "2026-11-01T10:00:00Z"},
    }
    if rm == "ACTION":
        node["ism_transition"] = {
            "current_state": {
                "value": "completed",
                "defining_code": {"code_string": "532"},
            }
        }
    group.update(
        anchor=entry,
        fields=[{**group["fields"][0], "path": entry + "/data[at0001]/items[at0004]"}],
    )
    group["definition"].update(
        entry_type=rm, entry_path=entry, required_fields=["blood_pressure.systolic"]
    )
    document["canonicalJSON"]["content"] = [node]
    context = convert(config, document).facts[0]["context"]
    assert context["time_basis"] == "recorded_entry_time"
    if rm == "ACTION":
        assert context["action_state"]["current_state"]["value"] == "completed"
    if rm == "INSTRUCTION":
        assert context["care_intent"] == "planned"


def test_interval_meaning_is_kept(config, document):
    event = document["canonicalJSON"]["content"][0]["data"]["events"][0]
    event.update(
        _type="INTERVAL_EVENT",
        width={"_type": "DV_DURATION", "value": "PT30M"},
        math_function={"_type": "DV_CODED_TEXT", "value": "mean"},
    )
    interval = convert(config, document).facts[0]["context"]["observation_interval"]
    assert (
        interval["width"]["value"] == "PT30M"
        and interval["math_function"]["value"] == "mean"
    )
    event.pop("math_function")
    with pytest.raises(SourceError, match="statistical meaning"):
        convert(config, document)


def test_same_entry_qualifier_does_not_cross_observation_instances(config, document):
    rule = config["rules"][0]
    group = rule["catalog"]["templates"][0]["groups"][0]
    rule["catalog"]["variables"].append(
        {
            "key": "blood_pressure.method",
            "label": "Method",
            "type": "text",
            "rm_types": ["DV_TEXT"],
        }
    )
    group["fields"].append(
        {
            "key": "blood_pressure.method",
            "path": group["definition"]["entry_path"]
            + "/protocol[at0011]/items[at0012]",
            "scope": "same_entry",
            "inherit": "ancestor",
        }
    )
    for entry, text in zip(
        document["canonicalJSON"]["content"], ["manual", "automatic"]
    ):
        entry["protocol"] = {
            "_type": "ITEM_TREE",
            "archetype_node_id": "at0011",
            "items": [
                {
                    "_type": "ELEMENT",
                    "archetype_node_id": "at0012",
                    "value": {"_type": "DV_TEXT", "value": text},
                }
            ],
        }
    assert [
        f["clinical"]["blood_pressure"]["method"]
        for f in convert(config, document).facts
    ] == ["manual", "manual", "automatic", "automatic"]


def test_strategy_sdk_preview_and_plan(config, document):
    import asyncio
    from kehrnel.engine.strategies.openehr.analytics.strategy import (
        OpenEHRAnalyticsStrategy,
    )
    from kehrnel.engine.core.types import StrategyContext

    strategy = OpenEHRAnalyticsStrategy()
    ctx = StrategyContext("analytics", config)
    result = asyncio.run(
        strategy.run_op(
            ctx,
            "analytics_preview",
            {"document": document, "accepted_at": "2026-11-01T12:00:00Z"},
        )
    )
    assert result["ok"] and len(result["facts"]) == 4
    plan = asyncio.run(strategy.plan(ctx))
    assert plan.artifacts["source_writes"] is False
    assert any(
        i.get("expireAfterSeconds") == 0 for i in plan.artifacts["indexes"]["facts"]
    )


def test_pinned_decoder_has_identical_semantic_revision(config, document):
    from kehrnel.engine.domains.openehr.analytics.profiles import pin_source_profile

    raw, decoder = ibm_document(document)
    converter = Converter(
        RuleSet.compile(config["rules"]), source_namespace="reference", decoder=decoder
    )
    config["source"]["format"] = "ibm"
    pinned = pin_source_profile(config, converter)["source"]
    restored = IBMDecoder(
        pinned["codes"], pinned["shortcuts"], IBMFields(**pinned["nodes"])
    )
    assert restored.revision == decoder.revision
    assert restored.decode(raw) == decoder.decode(raw)


@pytest.mark.parametrize(
    "code, expected",
    [
        ("D4", "at0004"),
        ("C23", "at0023"),
        ("B5", "at05"),
        ("A13", "at13"),
        ("B.0.2", "at0.0.2"),
        ("D0.1", "at0000.1"),
    ],
)
def test_legacy_specialized_node_codes(code, expected):
    from kehrnel.engine.domains.openehr.analytics.profiles import LEGACY_CTTI

    decoder = IBMDecoder({}, {}, IBMFields(**LEGACY_CTTI))
    assert decoder.selector(code) == expected
    for invalid in ("B..2", "B.2.", "Bword", "A.2"):
        with pytest.raises(SourceError, match="Unresolved"):
            decoder.selector(invalid)


def test_legacy_payload_identity_is_not_replaced_by_path_token():
    from kehrnel.engine.domains.openehr.analytics.profiles import LEGACY_CTTI

    root = "openEHR-EHR-COMPOSITION.self_reported_data.v1"
    entry = "openEHR-EHR-OBSERVATION.reference.v1"
    decoder = IBMDecoder(
        {"selectors": {"34": root, "98701": entry}},
        {"keys": {"archetype_node_id": "ani", "content": "ct", "_type": "T"}},
        IBMFields(**LEGACY_CTTI),
    )
    raw = {
        "cn": [
            {"p": "34", "data": {"T": "COMPOSITION", "ani": "34"}},
            {
                "p": "34/34",
                "li": 0,
                "kp": ["ct"],
                "data": {"T": "OBSERVATION", "ani": "98701"},
            },
            {
                "p": "34/34",
                "li": 1,
                "kp": ["ct"],
                "data": {"T": "OBSERVATION", "ani": "98701"},
            },
        ]
    }
    before = deepcopy(raw)
    canonical = decoder.decode(raw)
    assert canonical["archetype_node_id"] == root
    assert [node["archetype_node_id"] for node in canonical["content"]] == [
        entry,
        entry,
    ]
    assert raw == before


@pytest.mark.parametrize("ibm", [False, True])
def test_conflicting_source_archetype_metadata_cannot_publish(config, document, ibm):
    if ibm:
        raw, decoder = ibm_document(document)
        entry = next(
            n
            for n in raw["cn"]
            if decoder.expand(n["data"]).get("_type") == "OBSERVATION"
        )
        entry["data"]["archetype_details"] = {
            "archetype_id": {"value": "openEHR-EHR-COMPOSITION.inconsistent.v1"}
        }
    else:
        raw, decoder = document, None
        raw["canonicalJSON"]["content"][0]["archetype_details"] = {
            "archetype_id": {"value": "openEHR-EHR-COMPOSITION.inconsistent.v1"}
        }
    converter = Converter(
        RuleSet.compile(config["rules"]), source_namespace="reference", decoder=decoder
    )
    before = deepcopy(raw)
    with pytest.raises(SourceError, match="Conflicting source archetype identities"):
        converter.convert(raw, accepted_at="2026-11-01T12:00:00Z")
    assert raw == before
