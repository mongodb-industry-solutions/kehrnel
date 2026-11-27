"""Unit tests for E2E search plan construction (no MongoDB)."""

from __future__ import annotations

import pytest

from .cli_scenarios_mql import ALL_PIPELINE_SCENARIOS, MqlPipelineScenario
from .resource_search_queries import RESOURCE_SEARCH_QUERIES, compartment_query
from .search_plan import PATIENT_COMPARTMENT_RESOURCES, build_search_plan

pytestmark = pytest.mark.e2e


class TestSearchPlan:
    @pytest.mark.parametrize("scenario", ALL_PIPELINE_SCENARIOS, ids=lambda s: s.id)
    def test_plan_covers_all_resources(self, scenario: MqlPipelineScenario) -> None:
        plan = build_search_plan(
            scenario,
            patient_id="pat-1",
            practitioner_id="pr-1",
            encounter_id="enc-1",
            device_id="dev-1",
        )
        resources_in_plan = {p.resource for p in plan}
        assert resources_in_plan >= set(scenario.resources)

    @pytest.mark.parametrize("scenario", ALL_PIPELINE_SCENARIOS, ids=lambda s: s.id)
    def test_plan_includes_convert_and_search(
        self, scenario: MqlPipelineScenario
    ) -> None:
        plan = build_search_plan(scenario, patient_id="pat-1")
        kinds_by_resource: dict[str, set[str]] = {}
        for p in plan:
            kinds_by_resource.setdefault(p.resource, set()).add(p.kind)
        for resource in scenario.resources:
            kinds = kinds_by_resource.get(resource, set())
            assert "convert" in kinds, f"{scenario.id}/{resource} missing convert"
            assert "search" in kinds or "compartment_search" in kinds, (
                f"{scenario.id}/{resource} missing search"
            )

    def test_hc20_has_compartment_searches(self) -> None:
        hc20 = next(s for s in ALL_PIPELINE_SCENARIOS if s.id == "hc20")
        plan = build_search_plan(hc20, patient_id="pat-1")
        compartment = [p for p in plan if p.kind == "compartment_search"]
        assert len(compartment) >= 3

    def test_patient_compartment_resources_have_valid_queries(self) -> None:
        """Avoid status=active on resources without a status search param (e.g. RiskAssessment)."""
        missing = sorted(PATIENT_COMPARTMENT_RESOURCES - RESOURCE_SEARCH_QUERIES.keys())
        assert not missing, f"Add compartment queries for: {missing}"
        assert compartment_query("RiskAssessment") == "method=clinical"
        assert compartment_query("AllergyIntolerance") == "clinical-status=active"
        assert compartment_query("AuditEvent") == "action=C"

    def test_ind_pophealth_risk_compartment_query(self) -> None:
        ind = next(s for s in ALL_PIPELINE_SCENARIOS if s.id == "ind_pophealth")
        plan = build_search_plan(ind, patient_id="pat-1")
        risk_comp = [
            p for p in plan
            if p.resource == "RiskAssessment" and p.kind == "compartment_search"
        ]
        assert len(risk_comp) == 1
        assert risk_comp[0].query == "method=clinical"
