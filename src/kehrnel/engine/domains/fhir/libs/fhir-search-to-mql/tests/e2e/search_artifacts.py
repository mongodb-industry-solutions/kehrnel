"""Save and remove E2E FHIR search query results for manual review."""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

E2E_RESULTS_ROOT = Path(__file__).resolve().parent / "results"


@dataclass
class SearchQueryResult:
    """Outcome of one convert/search invocation."""

    kind: Literal["convert", "search", "compartment_search"]
    resource: str
    query: str
    extra_args: list[str] = field(default_factory=list)
    compartment_type: str | None = None
    compartment_id: str | None = None
    exit_code: int = 0
    ok: bool = True
    error: str | None = None
    mql: dict[str, Any] | None = None
    count: int | None = None
    result_ids: list[str] = field(default_factory=list)
    results_preview: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ScenarioSearchReport:
    scenario_id: str
    title: str
    database: str
    resources: list[str]
    mongodb_uri: str
    ran_at: str
    searches: list[SearchQueryResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "title": self.title,
            "database": self.database,
            "resources": self.resources,
            "mongodb_uri": self.mongodb_uri,
            "ran_at": self.ran_at,
            "summary": {
                "total": len(self.searches),
                "passed": sum(1 for s in self.searches if s.ok),
                "failed": sum(1 for s in self.searches if not s.ok),
                "by_kind": _count_by_kind(self.searches),
            },
            "searches": [s.to_dict() for s in self.searches],
        }


def _count_by_kind(searches: list[SearchQueryResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for s in searches:
        counts[s.kind] = counts.get(s.kind, 0) + 1
    return counts


def scenario_results_dir(scenario_id: str, root: Path | None = None) -> Path:
    return (root or E2E_RESULTS_ROOT) / scenario_id


def scenario_results_path(scenario_id: str, root: Path | None = None) -> Path:
    return scenario_results_dir(scenario_id, root) / "search_results.json"


def save_scenario_report(report: ScenarioSearchReport, root: Path | None = None) -> Path:
    out_dir = scenario_results_dir(report.scenario_id, root)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "search_results.json"
    path.write_text(
        json.dumps(report.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    return path


def load_scenario_report(scenario_id: str, root: Path | None = None) -> dict[str, Any]:
    path = scenario_results_path(scenario_id, root)
    return json.loads(path.read_text(encoding="utf-8"))


def clear_all_results(root: Path | None = None) -> list[str]:
    """Remove entire E2E results tree; returns removed top-level scenario ids."""
    base = root or E2E_RESULTS_ROOT
    if not base.is_dir():
        return []
    removed = [p.name for p in base.iterdir() if p.is_dir()]
    shutil.rmtree(base)
    return removed


def clear_scenario_results(scenario_id: str, root: Path | None = None) -> None:
    path = scenario_results_dir(scenario_id, root)
    if path.is_dir():
        shutil.rmtree(path)


def new_report(
    *,
    scenario_id: str,
    title: str,
    database: str,
    resources: list[str],
    mongodb_uri: str,
) -> ScenarioSearchReport:
    return ScenarioSearchReport(
        scenario_id=scenario_id,
        title=title,
        database=database,
        resources=resources,
        mongodb_uri=mongodb_uri,
        ran_at=datetime.now(timezone.utc).isoformat(),
    )
