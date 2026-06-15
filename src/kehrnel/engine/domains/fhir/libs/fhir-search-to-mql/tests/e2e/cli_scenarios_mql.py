"""
CLI_COMMANDS.md fhir-mql pipeline scenarios.

Healthcare 1–21 and industrial A–K use the same **id**, **title**, and **resources**
as ``fhir-data-generation/tests/e2e/cli_scenarios_gen.py``. Search steps come from
``scenario_searches.py`` (CLI_COMMANDS healthcare / industrial sections).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

# Sibling fhir-gen scenario definitions (source of truth for names + resources)
_GEN_E2E = Path(__file__).resolve().parents[3] / "fhir-data-generation" / "tests" / "e2e"
if str(_GEN_E2E) not in sys.path:
    sys.path.insert(0, str(_GEN_E2E))

from cli_scenarios_gen import (  # noqa: E402
    HEALTHCARE_SCENARIOS,
    INDUSTRIAL_SCENARIOS,
    MQL_84,
    GenerateManyScenario,
)

try:
    from .resource_search_queries import fallback_query
    from .scenario_searches import SCENARIO_SEARCHES
except ImportError:
    from resource_search_queries import fallback_query  # noqa: F401
    from scenario_searches import SCENARIO_SEARCHES  # noqa: F401 — run_cli_e2e flat path

SearchStep = tuple[str, str, tuple[str, ...]]


@dataclass(frozen=True)
class MqlPipelineScenario:
    id: str
    title: str
    gen_scenario_id: str
    resources: tuple[str, ...]
    searches: tuple[SearchStep, ...] = ()
    denormalize_all: bool = False
    section: str = "healthcare"

    def db_name(self, prefix: str = "fhir_e2e_gen_") -> str:
        return f"{prefix}{self.gen_scenario_id}"


def _s(resource: str, query: str, *extra: str) -> SearchStep:
    return (resource, query, extra)


def _searches_for_gen(gen: GenerateManyScenario) -> tuple[SearchStep, ...]:
    """CLI search steps for this scenario, plus fallbacks for any bundled resource."""
    base = SCENARIO_SEARCHES.get(gen.id, ())
    covered = {resource for resource, _, _ in base}
    extras: list[SearchStep] = []
    for resource in gen.resources:
        if resource in covered:
            continue
        query = fallback_query(resource)
        extras.append(_s(resource, query))
        covered.add(resource)
    return base + tuple(extras)


def _mql_from_gen(gen: GenerateManyScenario) -> MqlPipelineScenario:
    return MqlPipelineScenario(
        id=gen.id,
        title=gen.title,
        gen_scenario_id=gen.id,
        resources=gen.resources,
        searches=_searches_for_gen(gen),
        denormalize_all=len(gen.resources) >= len(MQL_84),
        section=gen.section,
    )


HEALTHCARE_MQL: tuple[MqlPipelineScenario, ...] = tuple(
    _mql_from_gen(g) for g in HEALTHCARE_SCENARIOS
)

INDUSTRIAL_MQL: tuple[MqlPipelineScenario, ...] = tuple(
    _mql_from_gen(g) for g in INDUSTRIAL_SCENARIOS
)

ALL_PIPELINE_SCENARIOS: tuple[MqlPipelineScenario, ...] = (
    *HEALTHCARE_MQL,
    *INDUSTRIAL_MQL,
)

# Go-live audit (CLI_COMMANDS healthcare §21) — uses hc20 database
GO_LIVE_CLI_STEPS: tuple[tuple[str, ...], ...] = (
    ("stats", "--all", "--format", "json"),
    ("denormalize", "--all", "--dry-run"),
    ("indexes", "--all", "--dry-run"),
)

# Representative convert commands (no DB)
CONVERT_SMOKE: tuple[SearchStep, ...] = (
    _s("Patient", "name=Smith&gender=male"),
    _s("Observation", "code=8480-6&status=final"),
    _s("MeasureReport", "patient=p1&status=complete"),
    _s("Claim", "patient=p1&status=active"),
    _s("DeviceRequest", "patient=p1&status=active"),
    _s("CoverageEligibilityResponse", "patient=p1&outcome=complete"),
    _s("ResearchSubject", "patient=pat-1&status=active"),
    _s("Endpoint", "status=active&connection-type=hl7-fhir-rest"),
)
