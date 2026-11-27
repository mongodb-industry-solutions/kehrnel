from copy import deepcopy
from pathlib import Path
import asyncio
import json
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.openehr.analytics.authoring import (
    compile_draft,
    proposal_for,
)
from kehrnel.engine.strategies.openehr.analytics.strategy import (
    OpenEHRAnalyticsStrategy,
)

ROOT = Path(__file__).resolve().parents[3]
REFERENCE = ROOT / "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference"
AT = "2026-11-18T00:00:00Z"


def payload():
    return {
        "opt_xml": (REFERENCE / "templates/sample_laboratory_v0_4.opt").read_text(),
        "effective_at": AT,
        "epoch": 1,
    }


def test_boundary_and_unknown_fields_cannot_be_injected():
    p = payload()
    g = compile_draft(p)["rule"]["catalog"]["templates"][0]["groups"][0]
    for edits in (
        {"groups": {"/arbitrary": {}}},
        {"groups": {g["anchor"]: {"anchor": "/content"}}},
        {"exclude_field_ids": ["unknown"]},
        {"disabled_anchors": ["unknown"]},
    ):
        with pytest.raises(ValueError):
            compile_draft({**p, "edits": edits})


def test_disabled_blocks_and_stale_opt_revision():
    p = payload()
    initial = compile_draft(p)
    anchor = initial["rule"]["catalog"]["templates"][0]["groups"][0]["anchor"]
    edited = compile_draft({**p, "edits": {"disabled_anchors": [anchor]}})
    assert anchor not in {
        g["anchor"] for g in edited["rule"]["catalog"]["templates"][0]["groups"]
    }
    assert initial["rule"]["revision"] != edited["rule"]["revision"]
    with pytest.raises(ValueError, match="regenerate"):
        compile_draft({**p, "proposal_revision": "stale"})


def test_decimal_unit_conversion_and_retention_are_validated():
    p = payload()
    variables = compile_draft(p)["rule"]["catalog"]["variables"]
    key = next(v["key"] for v in variables if "DV_QUANTITY" in v["rm_types"])
    edits = {
        "variables": {
            key: {
                "type": "quantity",
                "unit": "mm[Hg]",
                "conversion_rules": {
                    "unit_conversions": {"kPa": {"scale": 7.50061683}}
                },
            }
        },
        "retention": {"mode": "delete_facts", "clock": "clinical_time", "days": 1825},
    }
    r = compile_draft({**p, "edits": edits})["rule"]
    bindings = [
        b
        for g in r["catalog"]["templates"][0]["groups"]
        for b in g["fields"]
        if b["key"] == key
    ]
    assert all(
        b["conversions"]["unit_conversions"]["kPa"]["scale"] == 7.50061683
        for b in bindings
    )
    assert r["retention"]["days"] == 1825
    edits["variables"][key]["conversion_rules"]["unit_conversions"]["kPa"]["scale"] = (
        float("nan")
    )
    with pytest.raises(ValueError, match="finite"):
        compile_draft({**p, "edits": edits})


def test_rule_epochs_and_shared_semantics_survive_editing():
    p = payload()
    first = compile_draft(p)["rule"]
    assert compile_draft(p, [first])["rule"] == first
    with pytest.raises(ValueError, match="activation times"):
        compile_draft({**p, "epoch": 2}, [first])
    second = compile_draft(
        {**p, "epoch": 2, "effective_at": "2027-01-01T00:00:00Z"}, [first]
    )
    assert second["rule"]["epoch"] == 2
    variable = first["catalog"]["variables"][0]
    with pytest.raises(ValueError, match="cannot change"):
        proposal_for(
            {**p, "aliases": {variable["semantic_key"]: "another.key"}}, [first]
        )


def test_preview_uses_candidate_even_before_activation_and_does_not_mutate_context():
    p = payload()
    cfg = json.loads(
        (
            ROOT / "src/kehrnel/engine/strategies/openehr/analytics/defaults.json"
        ).read_text()
    )
    cfg["source_namespace"] = "synthetic"
    ctx = StrategyContext("isolated-authoring", deepcopy(cfg))
    doc = json.loads(
        (REFERENCE / "envelopes/sample_laboratory_v0_4.ndjson")
        .read_text()
        .splitlines()[0]
    )
    result = asyncio.run(
        OpenEHRAnalyticsStrategy().run_op(
            ctx,
            "analytics_preview_draft",
            {**p, "document": doc, "accepted_at": "2026-01-01T00:00:00Z"},
        )
    )
    assert result["preview"]["facts"]
    assert ctx.config == cfg
    assert result["review"]["approved"] is False


def test_authoring_route_is_scoped_admin_only_and_cannot_ingest(monkeypatch):
    from kehrnel.api.core.admin.routes import router

    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "true")
    monkeypatch.setenv("KEHRNEL_API_KEYS", "fixture-key")
    monkeypatch.setenv("KEHRNEL_API_KEY_ENV_SCOPES", '{"fixture-key":["a"]}')
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    path = "/environments/a/authoring/openehr.analytics/analytics_default_mapping"
    assert client.post(path, json={"payload": payload()}).status_code == 401
    headers = {"x-api-key": "fixture-key"}
    assert (
        client.post(
            path.replace("/a/", "/b/"), headers=headers, json={"payload": payload()}
        ).status_code
        == 403
    )
    assert (
        client.post(
            path.replace("analytics_default_mapping", "analytics_ingest"),
            headers=headers,
            json={},
        ).status_code
        == 400
    )
    response = client.post(path, headers=headers, json={"payload": payload()})
    assert response.status_code == 200, response.text
    assert response.json()["proposal"]["status"] == "draft"


def test_historical_plan_pins_exact_rule_and_commit_window_without_execution():
    cfg = json.loads(
        (ROOT / "examples/openehr_analytics/config.canonical.json").read_text()
    )
    from kehrnel.engine.domains.openehr.analytics.rules import RuleRevision

    rule = RuleRevision.compile(cfg["rules"][0])
    ctx = StrategyContext("review", cfg)
    body = {
        "job_id": "review-1",
        "template": rule.template,
        "rule_revision": rule.revision,
        "accepted_at": AT,
        "committed_from": "2020-01-01T00:00:00Z",
        "committed_until": "2021-01-01T00:00:00Z",
    }
    result = asyncio.run(
        OpenEHRAnalyticsStrategy().run_op(ctx, "analytics_reprocessing_plan", body)
    )
    assert result["submitted"] is False
    assert result["source_writes"] is False
    assert result["source_filter"]["template_name"] == rule.template
    assert set(result["source_filter"]["composition_date"]) == {"$gte", "$lt"}
    assert result["job"]["rule_revision"] == rule.revision
    with pytest.raises(ValueError, match="non-empty"):
        asyncio.run(
            OpenEHRAnalyticsStrategy().run_op(
                ctx,
                "analytics_reprocessing_plan",
                {**body, "committed_until": body["committed_from"]},
            )
        )
