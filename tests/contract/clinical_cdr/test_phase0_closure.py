"""Phase 0 closure tests — the required proofs before Phase 0 can be declared done.

Covers the reviewer's blocking findings (compile/execute level; no live Mongo):
- unfiltered search succeeds (Patient?_count=5)
- URL _count overrides the route default
- repeated parameters preserved
- mixed malformed + valid → strict fails, zero Mongo
- _count=0 cannot return every document
- advertised vs planned capabilities are truthful (_sort rejected at execute)
"""

from __future__ import annotations

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import QueryPlan, StrategyContext

from kehrnel.engine.strategies.fhir.clinical_cdr import query as fhir_query
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import MANIFEST, FHIRClinicalCDRStrategy
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.api.domains.fhir.models import FhirSearchRequest
from kehrnel.api.domains.fhir.routes import to_strategy_query

pytest.importorskip("fhir_search_to_mql")


def _ctx() -> StrategyContext:
    return StrategyContext(
        environment_id="test-env",
        config={"database": "fhir_test", "schema_version": "R5"},
        bindings={"db": {"provider": "mongodb", "uri": "mongodb://localhost:27017", "database": "fhir_test"}},
        manifest=MANIFEST,
    )


# ── Finding 1: unfiltered search succeeds ─────────────────────────────────────

@pytest.mark.asyncio
async def test_unfiltered_count_only_search_compiles_to_match_all():
    plan = await fhir_query.compile_fhir_query(_ctx(), "fhir", {"fhir_search": "Patient?_count=5"})
    assert plan.plan["filter"] == {}            # match-all, no ParsingError
    assert plan.plan["collection"] == "Patient"
    assert fhir_query.pagination_from_plan(plan.plan) == (5, None)


# ── Finding 4: URL _count overrides route default (limit=20) ──────────────────

@pytest.mark.asyncio
async def test_url_count_overrides_route_default():
    q = to_strategy_query(FhirSearchRequest(fhir_search="Patient?gender=male&_count=5", limit=20))
    plan = await fhir_query.compile_fhir_query(_ctx(), "fhir", q)
    assert fhir_query.pagination_from_plan(plan.plan) == (5, None)  # 5, not the default 20


@pytest.mark.asyncio
async def test_route_default_applies_when_url_omits_count():
    q = to_strategy_query(FhirSearchRequest(fhir_search="Patient?gender=male", limit=20))
    plan = await fhir_query.compile_fhir_query(_ctx(), "fhir", q)
    assert fhir_query.pagination_from_plan(plan.plan) == (20, None)  # fallback default


# ── Finding 2: repeated parameters preserved ──────────────────────────────────

def test_repeated_params_preserved_in_normalization():
    n = fhir_query.normalize_compile_input({"fhir_search": "Patient?name=Smith&name=Jones"})
    qs = n["query_string"]
    assert "name=Smith" in qs and "name=Jones" in qs  # both survive (no dict collapse)


# ── Finding 3/T1: malformed + valid → strict fails, zero Mongo ────────────────

@pytest.mark.asyncio
async def test_malformed_plus_valid_strict_fails_before_mongo(monkeypatch):
    calls = {"n": 0}
    def _boom(*a, **k):
        calls["n"] += 1
        raise AssertionError("Mongo must not be built for a malformed search")
    monkeypatch.setattr(bridge, "build_mql_context", _boom)

    strat = FHIRClinicalCDRStrategy(MANIFEST)
    with pytest.raises(KehrnelError):
        await strat.run_op(_ctx(), "fhir_search", {"fhir_search": "Patient?=x&gender=male"})
    assert calls["n"] == 0


# ── Finding 8: _count=0 cannot return every document ──────────────────────────

def test_count_zero_returns_no_rows_not_all():
    class _Cursor:
        def sort(self, *a): return self
        def skip(self, *a): return self
        def limit(self, *a):  # pragma: no cover - must not be reached for _count=0
            raise AssertionError("limit(0) must not be sent to Mongo (means unlimited)")
        def __iter__(self): return iter([{"id": "should-not-appear"}])

    class _Coll:
        database = None
        name = "Patient"
        def find(self, *a, **k):  # pragma: no cover
            raise AssertionError("find must not run for _count=0")

    rows = fhir_query._execute_find(_Coll(), {}, limit=0, skip=None)
    assert rows == []


# ── Finding 5 / round-7: _sort is now a supported (executed) control ──────────

def test_capabilities_sort_is_supported():
    cat = fhir_query.fhir_capabilities(_ctx())
    assert "_sort" in cat["supported_result_controls"]
    assert cat["planned_result_controls"] == []
    assert cat["fhir_version"] == "R5"  # from activation config, not hardcoded path


# ── Round-3: compartment fail-closed ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_compartment_malformed_param_fails_closed(monkeypatch):
    calls = {"n": 0}
    def _boom(*a, **k):
        calls["n"] += 1
        raise AssertionError("Mongo must not be built for a malformed compartment search")
    monkeypatch.setattr(bridge, "build_mql_context", _boom)

    strat = FHIRClinicalCDRStrategy(MANIFEST)
    with pytest.raises(KehrnelError):
        await strat.run_op(
            _ctx(), "fhir_search",
            {"fhir_search": "Patient/p1/Observation?=x&status=final"},
        )
    assert calls["n"] == 0


# ── Round-3: URL _count clamped to server max ─────────────────────────────────

@pytest.mark.asyncio
async def test_url_count_clamped_to_server_max():
    plan = await fhir_query.compile_fhir_query(
        _ctx(), "fhir", {"fhir_search": "Patient?_count=1000000000"}
    )
    limit, _ = fhir_query.pagination_from_plan(plan.plan)
    assert limit == fhir_query.DEFAULT_MAX_RESULT_COUNT  # not a billion


# ── Round-3: handling flows through the compile path ──────────────────────────

@pytest.mark.asyncio
async def test_handling_lenient_records_ignored_via_compile():
    plan = await fhir_query.compile_fhir_query(
        _ctx(), "fhir",
        {"resource_type": "Patient", "criteria": {"gender": "male", "totally_unsupported": "x"},
         "handling": "lenient"},
    )
    assert any(i["name"] == "totally_unsupported" for i in plan.plan["ignored_parameters"])


def test_route_prefer_header_is_the_handling_contract():
    from kehrnel.api.domains.fhir.routes import _prefer_handling, to_strategy_query

    class _Req:
        headers = {"prefer": "return=representation, handling=lenient"}
    assert _prefer_handling(_Req()) == "lenient"

    # There is no bespoke body handling field; to_strategy_query never sets handling.
    q = to_strategy_query(FhirSearchRequest(resource_type="Patient", criteria={"gender": "male"}))
    assert "handling" not in q
    assert not hasattr(FhirSearchRequest(), "handling") or FhirSearchRequest.model_fields.get("handling") is None


# ── Round-4: T4 indexed find + count (no $facet 16MB risk) ────────────────────

class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs
        self.sorts = []
        self.skips = []
        self.limits = []
    def sort(self, spec):
        self.sorts.append(spec)
        return self
    def skip(self, n):
        self.skips.append(n)
        return self
    def limit(self, n):
        self.limits.append(n)
        return self
    def __iter__(self):
        return iter(self._docs)


class _FakeDatabase:
    client = None  # no client → snapshot session not attempted (mode "none")


class _FakeCollection:
    def __init__(self, docs, total):
        self._docs = docs
        self._total = total
        self.database = _FakeDatabase()
        self.name = "Patient"
        self.find_calls = []
        self.count_calls = []
        self.last_cursor = None
    def find(self, flt, **kwargs):
        self.find_calls.append((flt, kwargs))
        self.last_cursor = _FakeCursor(self._docs)
        return self.last_cursor
    def count_documents(self, flt, **kwargs):
        self.count_calls.append((flt, kwargs))
        return self._total


class _FakeMqlCtx:
    def __init__(self, coll):
        self.db = {"Patient": coll}


def _patch_mongo(monkeypatch, coll):
    monkeypatch.setattr(bridge, "resolve_mongo", lambda ctx: ("mongodb://x", "db", ""))
    monkeypatch.setattr(bridge, "resolve_strategy_config", lambda ctx: {"database": "db", "search": {}})
    monkeypatch.setattr(bridge, "build_mql_context", lambda *a, **k: _FakeMqlCtx(coll))
    monkeypatch.setattr(bridge, "close_mql_context", lambda ctx: None)


@pytest.mark.asyncio
async def test_find_and_count_over_same_filter(monkeypatch):
    coll = _FakeCollection([{"resourceType": "Patient", "id": "p-1"}], total=1)
    _patch_mongo(monkeypatch, coll)
    plan = QueryPlan(engine="fhir_mql",
                     plan={"filter": {}, "collection": "Patient", "resource_type": "Patient",
                           "query_input": {"_count": 5}},
                     explain={})
    result = await fhir_query.execute_fhir_query(_ctx(), plan)
    assert len(coll.find_calls) == 1 and len(coll.count_calls) == 1
    assert coll.find_calls[0][0] == coll.count_calls[0][0]  # same resolved filter
    assert result.explain["total"] == 1 and result.explain["returned"] == 1
    assert result.explain["executed"]["snapshot"] == "best_effort"  # no snapshot session available
    assert "_executed_pipeline" not in result.explain       # bounded by default


@pytest.mark.asyncio
async def test_unsorted_find_preserves_filter_index_choice_and_paging(monkeypatch):
    coll = _FakeCollection([], total=0)
    _patch_mongo(monkeypatch, coll)
    plan = QueryPlan(engine="fhir_mql",
                     plan={"filter": {}, "collection": "Patient", "resource_type": "Patient",
                           "query_input": {"_count": 5, "_offset": 10}},
                     explain={})
    await fhir_query.execute_fhir_query(_ctx(), plan)
    cur = coll.last_cursor
    assert cur.sorts == []
    assert cur.skips == [10] and cur.limits == [5]


@pytest.mark.asyncio
async def test_count_zero_does_not_call_find(monkeypatch):
    coll = _FakeCollection([{"id": "nope"}], total=42)
    _patch_mongo(monkeypatch, coll)
    plan = QueryPlan(engine="fhir_mql",
                     plan={"filter": {}, "collection": "Patient", "resource_type": "Patient",
                           "query_input": {"_count": 0}},
                     explain={})
    result = await fhir_query.execute_fhir_query(_ctx(), plan)
    assert result.rows == [] and result.explain["total"] == 42
    assert coll.find_calls == []            # count-only, no find
    assert len(coll.count_calls) == 1


@pytest.mark.asyncio
async def test_privileged_filter_only_when_ctx_privileged(monkeypatch):
    coll = _FakeCollection([], total=0)
    _patch_mongo(monkeypatch, coll)
    ctx = StrategyContext(
        environment_id="e", config={"database": "db", "schema_version": "R5"},
        bindings={"db": {"provider": "mongodb", "uri": "mongodb://x", "database": "db"}},
        manifest=MANIFEST, meta={"privileged": True},
    )
    plan = QueryPlan(engine="fhir_mql",
                     plan={"filter": {}, "collection": "Patient", "resource_type": "Patient",
                           "query_input": {"_count": 5}}, explain={})
    result = await fhir_query.execute_fhir_query(ctx, plan)
    assert "_executed_pipeline" in result.explain  # privileged → resolved filter present


# ── Round-7: faithful _sort compiles to a Mongo sort + id tie-breaker ─────────

@pytest.mark.asyncio
async def test_sort_compiles_to_field_and_applies_at_execute(monkeypatch):
    # Compile: birthdate → descending on the configured field.
    plan = await fhir_query.compile_fhir_query(
        _ctx(), "fhir", {"resource_type": "Patient", "criteria": {"gender": "female"}, "_sort": "-birthdate"}
    )
    assert plan.plan["sort"] == [["birthDate", -1]]

    # Execute: applies the sort, then an id tie-breaker.
    coll = _FakeCollection([], total=0)
    _patch_mongo(monkeypatch, coll)
    result = await fhir_query.execute_fhir_query(_ctx(), plan)
    assert coll.last_cursor.sorts == [[("birthDate", -1), ("id", 1)]]
    assert result.explain["total"] == 0


@pytest.mark.asyncio
async def test_unsortable_sort_key_fails_closed_at_compile():
    with pytest.raises(KehrnelError) as exc:
        await fhir_query.compile_fhir_query(
            _ctx(), "fhir", {"resource_type": "Patient", "criteria": {"gender": "female"},
                             "_sort": "totally_unsortable"}
        )
    assert exc.value.code == "FHIR_SEARCH_UNSUPPORTED_PARAM"
