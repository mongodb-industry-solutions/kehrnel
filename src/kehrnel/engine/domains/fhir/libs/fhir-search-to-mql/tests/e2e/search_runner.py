"""Execute planned FHIR search queries and persist results for E2E review."""

from __future__ import annotations

import json
from typing import Sequence

from fhir_search_to_mql import cli

from .e2e_log import status_enabled, status_phase
from .e2e_runner import run_fhir_mql
from .search_artifacts import (
    E2E_RESULTS_ROOT,
    ScenarioSearchReport,
    SearchQueryResult,
    new_report,
    save_scenario_report,
)
from .search_plan import PlannedSearch, build_search_plan, sample_reference_ids
from .cli_scenarios_mql import MqlPipelineScenario


def _run_convert_inprocess(
    resource: str,
    query: str,
    extra_args: Sequence[str],
) -> tuple[int, dict | None, str | None]:
    compartment_type = None
    compartment_id = None
    if "--compartment-type" in extra_args:
        i = extra_args.index("--compartment-type")
        compartment_type = extra_args[i + 1]
    if "--compartment-id" in extra_args:
        i = extra_args.index("--compartment-id")
        compartment_id = extra_args[i + 1]
    try:
        from fhir_search_to_mql import FHIRSearchConverter

        converter = FHIRSearchConverter()
        if compartment_type and compartment_id:
            mql = converter.convert_with_compartment(
                compartment_type=compartment_type,
                compartment_id=compartment_id,
                resource_type=resource,
                query_string=query,
            )
        else:
            mql = converter.convert(
                resource_type=resource,
                query_string=query,
            )
        return cli.EXIT_OK, mql, None
    except Exception as exc:
        return cli.EXIT_CONFIG, None, str(exc)


def _run_search_cli(
    resource: str,
    query: str,
    db: str,
    extra_args: Sequence[str],
    *,
    mongo_uri: str,
    mql_python: str | None,
    limit: int = 25,
) -> tuple[int, dict | None, str | None]:
    args = [
        "search",
        resource,
        query,
        "--limit",
        str(limit),
        "--format",
        "json",
        *extra_args,
    ]
    try:
        proc = run_fhir_mql(args, mongo_uri=mongo_uri, db=db, python=mql_python)
        payload = json.loads(proc.stdout)
        return cli.EXIT_OK, payload, None
    except Exception as exc:
        err = str(exc)
        if hasattr(exc, "stderr"):
            err = f"{exc}\n{getattr(exc, 'stderr', '')}"
        return cli.EXIT_RUNTIME, None, err


def _extract_result_summary(payload: dict) -> tuple[int | None, list[str], list[dict]]:
    count = payload.get("count")
    if count is None and "results" in payload:
        count = len(payload["results"])
    ids: list[str] = []
    preview: list[dict] = []
    for doc in (payload.get("results") or [])[:5]:
        doc_id = doc.get("id") or str(doc.get("_id", ""))
        ids.append(str(doc_id))
        preview.append(
            {
                "id": doc_id,
                "resourceType": doc.get("resourceType"),
            }
        )
    return count, ids, preview


def execute_search_plan(
    scenario: MqlPipelineScenario,
    db: str,
    *,
    mongo_uri: str,
    mql_python: str | None = None,
    include_compartments: bool = True,
    results_root=None,
) -> ScenarioSearchReport:
    """Run all planned searches; save ``search_results.json`` under results_root."""
    refs = sample_reference_ids(mongo_uri, db)
    plan = build_search_plan(
        scenario,
        patient_id=refs.get("patient_id"),
        practitioner_id=refs.get("practitioner_id"),
        encounter_id=refs.get("encounter_id"),
        device_id=refs.get("device_id"),
        include_compartments=include_compartments,
    )
    report = new_report(
        scenario_id=scenario.id,
        title=scenario.title,
        database=db,
        resources=list(scenario.resources),
        mongodb_uri=mongo_uri,
    )
    total = len(plan)
    label = f"Running search tests ({total} queries)"
    if status_enabled():
        with status_phase(label) as progress:
            report.searches = []
            for i, step in enumerate(plan, 1):
                progress["detail"] = f"{i}/{total}"
                report.searches.append(
                    _execute_one(step, db, mongo_uri=mongo_uri, mql_python=mql_python)
                )
    else:
        report.searches = [
            _execute_one(step, db, mongo_uri=mongo_uri, mql_python=mql_python)
            for step in plan
        ]
    if results_root is not None:
        save_scenario_report(report, root=results_root)
    return report


def _execute_one(
    step: PlannedSearch,
    db: str,
    *,
    mongo_uri: str,
    mql_python: str | None,
) -> SearchQueryResult:
    extra = list(step.extra_args)
    compartment_type = None
    compartment_id = None
    if "--compartment-type" in extra:
        i = extra.index("--compartment-type")
        compartment_type = extra[i + 1]
    if "--compartment-id" in extra:
        i = extra.index("--compartment-id")
        compartment_id = extra[i + 1]

    rc, mql, err = _run_convert_inprocess(step.resource, step.query, extra)
    result = SearchQueryResult(
        kind=step.kind,
        resource=step.resource,
        query=step.query,
        extra_args=extra,
        compartment_type=compartment_type,
        compartment_id=compartment_id,
        exit_code=rc,
        ok=rc == cli.EXIT_OK and err is None,
        error=err,
        mql=mql,
    )

    if step.kind in ("search", "compartment_search"):
        src, payload, serr = _run_search_cli(
            step.resource,
            step.query,
            db,
            extra,
            mongo_uri=mongo_uri,
            mql_python=mql_python,
        )
        result.exit_code = src
        result.ok = result.ok and src == cli.EXIT_OK and serr is None
        if serr:
            result.error = (result.error or "") + f"; search: {serr}"
        if payload:
            count, ids, preview = _extract_result_summary(payload)
            result.count = count
            result.result_ids = ids
            result.results_preview = preview
            if mql is None and payload.get("mql"):
                result.mql = payload["mql"]

    return result


def assert_report_passed(report: ScenarioSearchReport) -> None:
    failures = [s for s in report.searches if not s.ok]
    if failures:
        lines = [
            f"{f.kind} {f.resource} {f.query!r}: {f.error}"
            for f in failures[:10]
        ]
        raise AssertionError(
            f"{report.scenario_id}: {len(failures)}/{len(report.searches)} searches failed. "
            + "; ".join(lines)
        )
