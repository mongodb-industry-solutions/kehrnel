"""
E2E tests for CLI_COMMANDS.md fhir-mql scenarios.

Pipeline: fhir-gen → index → denormalize → **convert/search/compartment** tests for
every resource in the scenario. Results saved under ``tests/e2e/results/<id>/``.

Run::

    pip install -e "..\fhir-data-generation"
    pip install -e ".[dev]"
    pytest tests/e2e/ -m "e2e and mongodb" --no-cov -q
"""

from __future__ import annotations

import json
import os
import sys

import pytest

from .cli_scenarios_mql import (
    ALL_PIPELINE_SCENARIOS,
    GO_LIVE_CLI_STEPS,
    MqlPipelineScenario,
)
from .conftest import E2E_DB_PREFIX, E2E_MONGODB_URI, GEN_REPO_ROOT
from .e2e_runner import load_gen_data, run_fhir_mql
from .search_artifacts import E2E_RESULTS_ROOT, clear_scenario_results, scenario_results_path
from .search_runner import assert_report_passed, execute_search_plan

pytest.importorskip("fhir_gen")
_GEN_E2E = GEN_REPO_ROOT / "tests" / "e2e"
if str(_GEN_E2E) not in sys.path:
    sys.path.insert(0, str(_GEN_E2E))
from cli_scenarios_gen import ALL_GENERATE_MANY_SCENARIOS  # noqa: E402

GEN_BY_ID = {s.id: s for s in ALL_GENERATE_MANY_SCENARIOS}
USE_FULL_COUNTS = os.environ.get("E2E_FULL_COUNTS", "").lower() in ("1", "true", "yes")
SAVE_RESULTS = os.environ.get("E2E_SAVE_RESULTS", "1").lower() not in ("0", "false", "no")

pytestmark = [pytest.mark.e2e, pytest.mark.mongodb]


def _prepare_database(
    scenario: MqlPipelineScenario,
    drop_e2e_db,
    gen_python: str,
    mql_python: str,
) -> str:
    gen_def = GEN_BY_ID[scenario.gen_scenario_id]
    db = gen_def.db_name(E2E_DB_PREFIX)
    drop_e2e_db(db)
    if SAVE_RESULTS:
        clear_scenario_results(scenario.id, E2E_RESULTS_ROOT)
    counts = gen_def.resolve_counts(minimal=not USE_FULL_COUNTS)
    load_gen_data(
        seed=gen_def.seed,
        resources=gen_def.resources,
        counts=counts,
        db=db,
        mongo_uri=E2E_MONGODB_URI,
        gen_python=gen_python,
    )
    resources = list(scenario.resources)
    if scenario.denormalize_all:
        run_fhir_mql(
            ["indexes", "--all"],
            mongo_uri=E2E_MONGODB_URI,
            db=db,
            python=mql_python,
        )
        run_fhir_mql(
            ["denormalize", "--all", "--batch-size", "100"],
            mongo_uri=E2E_MONGODB_URI,
            db=db,
            python=mql_python,
        )
    else:
        run_fhir_mql(
            ["indexes", *resources],
            mongo_uri=E2E_MONGODB_URI,
            db=db,
            python=mql_python,
        )
        run_fhir_mql(
            ["denormalize", *resources, "--batch-size", "100"],
            mongo_uri=E2E_MONGODB_URI,
            db=db,
            python=mql_python,
        )
    return db


@pytest.mark.mongodb
class TestPipelineScenarios:
    """Generate → denormalize → FHIR search query testing (all resources + compartments)."""

    @pytest.mark.parametrize(
        "scenario",
        ALL_PIPELINE_SCENARIOS,
        ids=lambda s: s.id,
    )
    def test_full_pipeline_with_search_queries(
        self,
        scenario: MqlPipelineScenario,
        drop_e2e_db,
        gen_python: str,
        mql_python: str,
    ) -> None:
        db = _prepare_database(scenario, drop_e2e_db, gen_python, mql_python)
        report = execute_search_plan(
            scenario,
            db,
            mongo_uri=E2E_MONGODB_URI,
            mql_python=mql_python,
            include_compartments=True,
            results_root=E2E_RESULTS_ROOT if SAVE_RESULTS else None,
        )
        assert_report_passed(report)
        assert scenario_results_path(scenario.id).is_file() or not SAVE_RESULTS
        # Every bundled resource exercised at least once via search or compartment
        searched_resources = {s.resource for s in report.searches if s.ok}
        assert searched_resources >= set(scenario.resources), (
            f"missing search coverage: {set(scenario.resources) - searched_resources}"
        )


@pytest.mark.mongodb
class TestGoLiveAudit:
    """CLI_COMMANDS healthcare §21 go-live commands on populated hc20 DB."""

    def test_go_live_steps(
        self,
        drop_e2e_db,
        gen_python: str,
        mql_python: str,
    ) -> None:
        gen_def = GEN_BY_ID["hc20"]
        db = f"{E2E_DB_PREFIX}go_live"
        drop_e2e_db(db)
        if SAVE_RESULTS:
            clear_scenario_results("go_live", E2E_RESULTS_ROOT)
        counts = gen_def.resolve_counts(minimal=True)
        load_gen_data(
            seed=gen_def.seed,
            resources=gen_def.resources,
            counts=counts,
            db=db,
            mongo_uri=E2E_MONGODB_URI,
            gen_python=gen_python,
        )
        run_fhir_mql(
            ["indexes", "--all"],
            mongo_uri=E2E_MONGODB_URI,
            db=db,
            python=mql_python,
        )
        run_fhir_mql(
            ["denormalize", "--all", "--batch-size", "100"],
            mongo_uri=E2E_MONGODB_URI,
            db=db,
            python=mql_python,
        )
        for step in GO_LIVE_CLI_STEPS:
            proc = run_fhir_mql(
                list(step),
                mongo_uri=E2E_MONGODB_URI,
                db=db,
                python=mql_python,
            )
            if step[0] == "stats":
                json.loads(proc.stdout)
