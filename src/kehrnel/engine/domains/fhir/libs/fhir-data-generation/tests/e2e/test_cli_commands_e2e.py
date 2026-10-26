"""
E2E tests for CLI_COMMANDS.md ``generate-many`` scenarios (fhir-gen).

Requires MongoDB at localhost:27017 and an editable install in .venv::

    pip install -e ".[dev]"
    pytest tests/e2e/test_cli_commands_e2e.py -m e2e --no-cov -q

Use ``E2E_FULL_COUNTS=1`` to run documented volume (slow).
"""

from __future__ import annotations

import os

import pytest

from fhir_gen import ResourceGenerator
from fhir_gen.persistence.mongo import FHIRMongoStore

from .cli_scenarios_gen import ALL_GENERATE_MANY_SCENARIOS, GenerateManyScenario
from .conftest import E2E_DB_PREFIX, E2E_MONGODB_URI, REPO_ROOT
from .e2e_runner import generate_many_scenario, venv_python

pytestmark = [pytest.mark.e2e, pytest.mark.integration]

USE_FULL_COUNTS = os.environ.get("E2E_FULL_COUNTS", "").lower() in ("1", "true", "yes")


@pytest.fixture(scope="module")
def gen_python() -> str:
    return venv_python(REPO_ROOT)


class TestGenerateManyScenariosApi:
    """In-process generate_many (fast sanity) without MongoDB."""

    @pytest.mark.parametrize(
        "scenario",
        ALL_GENERATE_MANY_SCENARIOS,
        ids=lambda s: s.id,
    )
    def test_generate_many_in_memory(self, scenario: GenerateManyScenario) -> None:
        counts = scenario.resolve_counts(minimal=not USE_FULL_COUNTS)
        gen = ResourceGenerator(seed=scenario.seed)
        results = gen.generate_many(list(scenario.resources), counts=counts)
        for rtype in scenario.resources:
            assert rtype in results
            assert len(results[rtype]) == counts[rtype]
            assert results[rtype][0]["resourceType"] == rtype


@pytest.mark.mongodb
class TestGenerateManyScenariosCli:
    """CLI ``fhir-gen generate-many`` against real MongoDB (one DB per scenario)."""

    @pytest.mark.parametrize(
        "scenario",
        ALL_GENERATE_MANY_SCENARIOS,
        ids=lambda s: s.id,
    )
    def test_cli_generate_many_persists(
        self,
        scenario: GenerateManyScenario,
        mongo_available,
        drop_e2e_db,
        gen_python: str,
    ) -> None:
        db = scenario.db_name(E2E_DB_PREFIX)
        drop_e2e_db(db)
        counts = scenario.resolve_counts(minimal=not USE_FULL_COUNTS)
        summary = generate_many_scenario(
            scenario.id,
            scenario.resources,
            counts,
            seed=scenario.seed,
            db=db,
            mongo_uri=E2E_MONGODB_URI,
        )
        for rtype, expected in counts.items():
            assert summary.get(rtype, 0) >= expected, (
                f"{scenario.id}: expected >={expected} {rtype}, got {summary}"
            )
        store = FHIRMongoStore(E2E_MONGODB_URI, db)
        try:
            for rtype in scenario.resources:
                assert store.count(rtype) >= counts[rtype]
        finally:
            store.close()
