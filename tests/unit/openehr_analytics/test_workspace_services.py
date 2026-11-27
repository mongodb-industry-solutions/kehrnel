from copy import deepcopy
from pathlib import Path
import asyncio
import json
import pytest
from bson import ObjectId
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.openehr.analytics.authoring import compile_draft
from kehrnel.engine.domains.openehr.analytics.business_catalog import catalog_page
from kehrnel.engine.domains.openehr.analytics.converter import SourceFields
from kehrnel.engine.domains.openehr.analytics.inspection import inspection_plan, inspect_source, source_coverage
from kehrnel.engine.domains.openehr.analytics.workbench import workbench_plan, read_workbench
from kehrnel.engine.strategies.openehr.analytics.strategy import OpenEHRAnalyticsStrategy

ROOT = Path(__file__).resolve().parents[3]
REF = ROOT / "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference"
AT = "2026-11-18T00:00:00Z"


@pytest.fixture(scope="module")
def draft():
    return {"opt_xml": (REF / "templates/sample_laboratory_v0_4.opt").read_text(),
            "effective_at": AT}


@pytest.fixture(scope="module")
def rule(draft):
    return compile_draft(draft)["rule"]


def test_catalog_pages_preserve_bindings_and_do_not_claim_counts_or_approval(rule, draft):
    next_rule = compile_draft({**draft, "epoch": 2, "effective_at": "2027-01-01T00:00:00Z"}, [rule])["rule"]
    rules = [rule, next_rule]
    result = catalog_page(rules, as_of=AT, limit=1)
    item = result["items"][0]
    assert item["fact_count"] is None and item["data_presence"] == "not_measured"
    assert result["next_offset"] == 1
    assert len(catalog_page(rules, as_of=AT, offset=1, limit=1)["items"]) == 1
    page = catalog_page(rules, as_of=AT, key=item["key"])
    assert {b["applicability"] for b in page["items"]} == {"applicable", "scheduled"}
    assert {b["approval"] for b in page["items"]} == {"not_reported"}
    assert all(b["business_path"].startswith("clinical.") for b in page["items"])
    assert {b["applicability"] for b in catalog_page(rules, as_of="2028-01-01", key=item["key"])["items"]} == {"historical", "applicable"}


def test_shared_variables_have_separate_exact_template_bindings(rule):
    other = deepcopy(rule)
    other.pop("revision")
    other["catalog"]["templates"][0]["template_name"] = "another exact template"
    # This catalog identity test uses a synthetic binding, not an OPT-derived rule.
    other["catalog"]["templates"][0].pop("source_coverage")
    result = catalog_page([rule, other], as_of=AT, limit=1)
    assert result["items"][0]["template_count"] == 2
    # A readable label is not permission to change the semantic identity.
    other["catalog"]["variables"][0]["semantic_key"] = "different"
    with pytest.raises(ValueError, match="semantic identity"):
        catalog_page([rule, other], as_of=AT)


class Cursor:
    def __init__(self, rows):
        self.rows, self.closed, self.calls = rows, False, []
    def __iter__(self):
        return iter(self.rows)
    def close(self):
        self.closed = True
    def __getattr__(self, name):
        def call(*args):
            self.calls.append((name, args))
            return self
        return call


class Collection:
    name = "test_collection"
    def __init__(self, keys, rows=(), **index_options):
        self.index_cursor = Cursor([{"name": "candidate", "key": dict(keys), **index_options}])
        self.cursor, self.find_calls = Cursor(rows), []
    def list_indexes(self):
        return self.index_cursor
    def find(self, *args, **kwargs):
        self.find_calls.append((args, kwargs))
        return self.cursor


def test_source_read_is_metadata_only_indexed_bounded_and_keeps_bson_ids():
    plan = inspection_plan(template="Exact v1", limit=1)
    identifier = ObjectId()
    collection = Collection(plan["required_index_prefix"], [{"_id": identifier}, {"_id": ObjectId()}])
    result = inspect_source(collection, template="Exact v1", limit=1)
    assert result["documents"] == [{"_id": identifier}]
    assert result["has_more"] is True
    assert result["executed_mql"]["limit"] == 2
    assert result["executed_mql"]["hint"] == "candidate"
    query, projection = collection.find_calls[0][0]
    assert query == {"template_name": "Exact v1"}
    assert "canonicalJSON" not in projection and "ehr_id" not in projection
    assert ("limit", (2,)) in collection.cursor.calls
    assert ("max_time_ms", (3000,)) in collection.cursor.calls
    assert collection.cursor.closed and collection.index_cursor.closed
    assert result["telemetry"]["documents_examined"] is None


@pytest.mark.parametrize("options", [{"sparse": True}, {"partialFilterExpression": {"x": 1}}, {"hidden": True}, {"collation": {"locale": "en"}}])
def test_partial_sparse_hidden_or_collated_index_cannot_authorize_a_read(options):
    c = Collection([("_id", 1)], **options)
    with pytest.raises(ValueError, match="compatible"):
        inspect_source(c)
    assert not c.find_calls


def test_missing_index_cannot_start_unfiltered_scan():
    c = Collection([("unrelated", 1)])
    with pytest.raises(ValueError, match="compatible"):
        inspect_source(c, template="Exact")
    assert not c.find_calls


def test_inspection_bounds_and_profiles():
    fields = SourceFields(template="template", version="version", committed_at="creation_date")
    p = inspection_plan(fields, template="X", committed_from="2020-01-01", committed_until="2021-01-01")
    assert p["required_index_prefix"] == [("template", 1), ("creation_date", 1), ("_id", 1)]
    assert p["sort"] == [("creation_date", 1), ("_id", 1)]
    for opts in ({"limit": 1001}, {"limit": True}, {"template": {"$ne": ""}}, {"filter": {}},
                 {"committed_from": AT}, {"committed_from": AT, "committed_until": AT}):
        with pytest.raises(ValueError):
            inspection_plan(**opts)


def test_byte_budget_closes_cursor_without_returning_partial_results():
    c = Collection([("_id", 1)], [{"_id": "x", "template_name": "a" * (8 * 1024 * 1024)}])
    with pytest.raises(ValueError, match="byte limit"):
        inspect_source(c)
    assert c.cursor.closed


def test_coverage_distinguishes_sample_missing_opt_ambiguity_and_future_rules(rule):
    template = rule["catalog"]["templates"][0]["template_name"]
    headers = [{"template_name": template}, {"template_name": template}, {"template_name": "Missing v2"}, {}]
    opts = [{"template": template, "source_digest": "a"}, {"template": template, "source_digest": "a"},
            {"template": template, "source_digest": "b"}]
    result = source_coverage(headers, opts, [rule], as_of="2020-01-01")
    row = next(x for x in result["templates"] if x["template"] == template)
    assert row["sample_compositions"] == 2 and row["opt_status"] == "ambiguous"
    assert row["rule_status"] == "scheduled"
    assert result["complete_source_inventory"] is False
    assert result["invalid_template_headers"] == 1
    missing = next(x for x in result["templates"] if x["template"] == "Missing v2")
    assert missing["opt_status"] == missing["rule_status"] == "missing"


def test_workbench_is_scoped_to_real_storage_paths(rule):
    group = rule["catalog"]["templates"][0]["groups"][0]
    args = dict(dataset="facts", scope={"kind": group["kind"]}, start="2020-01-01", end="2027-01-01",
                variables=[group["fields"][0]["key"]])
    plan = workbench_plan([rule], **args)
    assert "clinical." + args["variables"][0] in plan["projection"]
    assert plan["required_index_prefix"] == [("context.kind", 1), ("context.event_at", 1)]
    c = Collection(plan["required_index_prefix"])
    result = read_workbench(c, [rule], **args)
    assert result["executed"] and result["query"] == plan
    for changes in ({"scope": {}}, {"dataset": "patients"}, {"scope": {"kind": {"$ne": 1}}},
                    {"variables": ["patient.age"]}, {"end": args["start"]}):
        with pytest.raises(ValueError):
            workbench_plan([rule], **{**args, **changes})


def test_review_cannot_survive_source_or_rule_changes(draft):
    cfg = json.loads((ROOT / "src/kehrnel/engine/strategies/openehr/analytics/defaults.json").read_text())
    cfg["source_namespace"] = "synthetic-test"
    document = json.loads((REF / "envelopes/sample_laboratory_v0_4.ndjson").read_text().splitlines()[0])
    payload = {**draft, "document": document, "accepted_at": AT}
    strategy = OpenEHRAnalyticsStrategy()
    ctx = StrategyContext("review-test", cfg)
    def run(op, p):
        return asyncio.run(strategy.run_op(ctx, op, p))
    preview = run("analytics_preview_draft", payload)
    req = preview["review_requirements"]
    receipt = {"revision": req["revision"], "topics": req["required_topics"]}
    result = run("analytics_review_draft", {**payload, "acknowledgements": receipt})
    assert result["review_receipt"]["complete"]
    assert result["review_receipt"]["approval"] is False and result["review_receipt"]["persisted"] is False
    for change in (
        {"document": {**document, "composition_version": "999"}},
        {"effective_at": "2027-01-01T00:00:00Z"},
        {"accepted_at": "2027-01-01T00:00:00Z"},
    ):
        with pytest.raises(ValueError, match="stale"):
            run("analytics_review_draft", {**payload, **change, "acknowledgements": receipt})
    with pytest.raises(ValueError, match="every required"):
        run("analytics_review_draft", {**payload, "acknowledgements": {**receipt, "topics": []}})


def test_new_stateless_contracts_do_not_require_adapters(rule):
    ctx = StrategyContext("authoring-only", {"rules": [rule]})
    strategy = OpenEHRAnalyticsStrategy()
    calls = [
        ("analytics_business_catalog", {"as_of": AT}),
        ("analytics_source_inspection_plan", {}),
        ("analytics_source_coverage", {"headers": [], "opts": [], "as_of": AT}),
        ("analytics_workbench_plan", {"dataset": "activity", "scope": {"patient_id": "a"}, "start": "2020-01-01", "end": AT}),
    ]
    for op, payload in calls:
        assert asyncio.run(strategy.run_op(ctx, op, payload))["ok"]

def test_authoring_http_allows_plans_but_never_source_or_workbench_execution(monkeypatch, rule):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from kehrnel.api.core.admin.routes import router
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "true")
    monkeypatch.setenv("KEHRNEL_API_KEYS", "fixture-key")
    monkeypatch.setenv("KEHRNEL_API_KEY_ENV_SCOPES", '{"fixture-key":["a"]}')
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    base = "/environments/a/authoring/openehr.analytics/"
    body = {"config": {"rules": [rule]}, "payload": {"as_of": AT}}
    assert client.post(base + "analytics_business_catalog", json=body).status_code == 401
    headers = {"x-api-key": "fixture-key"}
    assert client.post(base.replace("/a/", "/b/") + "analytics_business_catalog",
                       headers=headers, json=body).status_code == 403
    response = client.post(base + "analytics_business_catalog", headers=headers, json=body)
    assert response.status_code == 200
    assert response.json()["clinical_reads"] is False
    for op in ("analytics_workbench", "analytics_ingest", "inspect_source"):
        assert client.post(base + op, headers=headers, json={}).status_code == 400
