"""Unit tests for the MQL resource dependency graph."""

from __future__ import annotations

import pytest

from fhir_search_to_mql.resolvers import dependency as dep


class TestMqlDependencyGraph:
    def test_all_shipped_resources_have_core_deps(self):
        dep.assert_mql_dependencies_complete()

    def test_measure_report_includes_anchors_before_dependent(self):
        order = dep.resolve_order(["MeasureReport"])
        assert order[-1] == "MeasureReport"
        for anchor in ("Measure", "Patient", "Practitioner", "Organization"):
            assert anchor in order
            assert order.index(anchor) < order.index("MeasureReport")

    def test_observation_chain_includes_encounter(self):
        order = dep.resolve_order(["Observation"])
        assert "Encounter" in order
        assert order.index("Patient") < order.index("Observation")

    def test_resolve_configured_order_filters_unknown_configs(self):
        order = dep.resolve_configured_order(
            ["MeasureReport"],
            {"MeasureReport", "Patient", "Measure"},
        )
        assert order == ["Measure", "Patient", "MeasureReport"]

    def test_resolve_configured_order_respects_no_deps(self):
        order = dep.resolve_configured_order(
            ["MeasureReport"],
            set(dep.MQL_SHIPPED_RESOURCES),
            include_dependencies=False,
        )
        assert order == ["MeasureReport"]

    def test_circular_dependency_raises(self, monkeypatch):
        monkeypatch.setitem(
            dep.CORE_DEPENDENCIES,
            "Patient",
            ["Observation"],
        )
        monkeypatch.setitem(
            dep.CORE_DEPENDENCIES,
            "Observation",
            ["Patient"],
        )
        with pytest.raises(ValueError, match="Circular"):
            dep.resolve_order(["Patient"])
