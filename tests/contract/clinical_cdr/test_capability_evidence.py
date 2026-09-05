"""Capability claims must be backed by executable evidence (reviewer #5).

Every search parameter advertised by ``fhir_list_search_params`` for a resource type
must actually COMPILE via the fhir-mql converter (not merely be declared in YAML).
This runs offline (compile-only; no Mongo) and turns "advertised" into "proven wired".
"""

from __future__ import annotations

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr import query as fhir_query
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import MANIFEST

pytest.importorskip("fhir_search_to_mql")

# Type-appropriate sample values so compilation exercises the real converter.
_SAMPLE_BY_TYPE = {
    "date": "2020-01-01",
    "datetime": "2020-01-01",
    "number": "1",
    "quantity": "1",
    "token": "x",
    "string": "x",
    "reference": "Patient/1",
    "uri": "http://example.org/x",
    "composite": None,  # skip composite params (need structured value)
}


def _ctx() -> StrategyContext:
    return StrategyContext(
        environment_id="cap",
        config={"database": "d", "schema_version": "R5"},
        bindings={"db": {"provider": "mongodb", "uri": "mongodb://x", "database": "d"}},
        manifest=MANIFEST,
    )


@pytest.mark.parametrize("resource_type", ["Patient", "Observation", "Condition", "Encounter"])
@pytest.mark.asyncio
async def test_advertised_params_actually_compile(resource_type):
    ctx = _ctx()
    listing = fhir_query.fhir_list_search_params(ctx, {"resource_type": resource_type})
    params = listing["parameters"]
    assert params, f"{resource_type} advertises no params"

    checked = 0
    failures: list[str] = []
    empty_filter: list[str] = []
    for p in params:
        name = p["name"]
        ptype = str(p.get("type"))
        if p.get("composite"):
            continue  # composite params require structured values — out of scope here
        sample = _SAMPLE_BY_TYPE.get(ptype, "x")
        if sample is None:
            continue
        checked += 1
        try:
            plan = await fhir_query.compile_fhir_query(
                ctx, "fhir", {"resource_type": resource_type, "criteria": {name: sample}}
            )
        except KehrnelError as exc:
            # An advertised param that cannot compile a valid typed sample is a real
            # defect (unwired or misconfigured) — not swallowed.
            failures.append(f"{name} ({ptype}): {exc.code}")
            continue
        # Real evidence: the param must produce an actual (non-empty) MQL constraint.
        if plan.plan.get("filter") in ({}, None):
            empty_filter.append(f"{name} ({ptype})")

    assert checked > 0, f"{resource_type}: no non-composite params exercised"
    assert not failures, f"{resource_type}: advertised but failed to compile: {failures}"
    assert not empty_filter, (
        f"{resource_type}: advertised but produced an EMPTY (match-all) filter: {empty_filter}"
    )
