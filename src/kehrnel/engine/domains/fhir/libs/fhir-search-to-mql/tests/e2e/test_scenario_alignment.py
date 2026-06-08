"""Ensure fhir-mql E2E scenarios match fhir-gen CLI_COMMANDS scenario definitions."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_GEN_E2E = Path(__file__).resolve().parents[3] / "fhir-data-generation" / "tests" / "e2e"
if str(_GEN_E2E) not in sys.path:
    sys.path.insert(0, str(_GEN_E2E))

from cli_scenarios_gen import (  # noqa: E402
    ALL_GENERATE_MANY_SCENARIOS,
    HEALTHCARE_SCENARIOS,
    INDUSTRIAL_SCENARIOS,
)

from .cli_scenarios_mql import (  # noqa: E402
    ALL_PIPELINE_SCENARIOS,
    HEALTHCARE_MQL,
    INDUSTRIAL_MQL,
)

pytestmark = pytest.mark.e2e

GEN_BY_ID = {s.id: s for s in ALL_GENERATE_MANY_SCENARIOS}
MQL_BY_ID = {s.id: s for s in ALL_PIPELINE_SCENARIOS}


class TestScenarioAlignment:
    def test_same_count_as_fhir_gen(self) -> None:
        assert len(HEALTHCARE_MQL) == len(HEALTHCARE_SCENARIOS)
        assert len(INDUSTRIAL_MQL) == len(INDUSTRIAL_SCENARIOS)
        assert len(ALL_PIPELINE_SCENARIOS) == len(ALL_GENERATE_MANY_SCENARIOS)

    def test_ids_and_titles_match(self) -> None:
        for gen in ALL_GENERATE_MANY_SCENARIOS:
            mql = MQL_BY_ID[gen.id]
            assert mql.id == gen.id
            assert mql.title == gen.title
            assert mql.gen_scenario_id == gen.id
            assert mql.section == gen.section

    def test_resources_match(self) -> None:
        for gen in ALL_GENERATE_MANY_SCENARIOS:
            mql = MQL_BY_ID[gen.id]
            assert mql.resources == gen.resources

    def test_every_resource_has_search_step(self) -> None:
        for mql in ALL_PIPELINE_SCENARIOS:
            searched = {step[0] for step in mql.searches}
            assert searched >= set(mql.resources), (
                f"{mql.id}: missing search for "
                f"{set(mql.resources) - searched}"
            )
