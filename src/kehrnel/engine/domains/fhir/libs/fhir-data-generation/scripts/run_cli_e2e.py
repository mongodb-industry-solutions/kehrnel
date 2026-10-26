#!/usr/bin/env python3
"""
Run all CLI_COMMANDS.md E2E scenarios (fhir-gen + fhir-mql pipeline).

Uses MongoDB at localhost:27017. Each scenario uses one database
(``fhir_e2e_gen_<id>``) for both fhir-gen and fhir-mql; the full run does not
reload data between phases. Use ``scripts/drop_e2e_databases.py`` to clean up.

Requires editable installs in each repo's .venv::

    cd fhir-data-generation && .venv\\Scripts\\pip install -e ".[dev]"
    cd ..\\fhir-search-to-mql && .venv\\Scripts\\pip install -e ".[dev]"
    cd ..\\fhir-data-generation && .venv\\Scripts\\pip install -e "..\\fhir-search-to-mql"

Usage::

    python scripts/run_cli_e2e.py
    python scripts/run_cli_e2e.py --section healthcare
    python scripts/run_cli_e2e.py --full-counts
    python scripts/run_cli_e2e.py --gen-only
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MQL_ROOT = REPO_ROOT.parent / "fhir-search-to-mql"
GEN_E2E_DIR = REPO_ROOT / "tests" / "e2e"
MQL_TESTS_DIR = MQL_ROOT / "tests"

# fhir-gen E2E modules (flat imports under tests/e2e/)
sys.path.insert(0, str(GEN_E2E_DIR))
from cli_scenarios_gen import ALL_GENERATE_MANY_SCENARIOS  # noqa: E402
from e2e_log import log_scenario, set_status_logging, status_phase  # noqa: E402
from e2e_runner import (  # noqa: E402
    DEFAULT_MONGODB_URI,
    load_gen_data,
    run_fhir_mql,
    venv_python,
)

# fhir-mql E2E package (relative imports require tests.e2e.*)
if str(MQL_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(MQL_TESTS_DIR))
from e2e.cli_scenarios_mql import ALL_PIPELINE_SCENARIOS  # noqa: E402

GEN_BY_ID = {s.id: s for s in ALL_GENERATE_MANY_SCENARIOS}
E2E_DB_PREFIX = os.environ.get("E2E_DB_PREFIX", "fhir_e2e_gen_")


def _check_mongo(uri: str) -> None:
    from pymongo import MongoClient

    MongoClient(uri, serverSelectionTimeoutMS=3000).server_info()


def run_gen_scenarios(
    *,
    uri: str,
    minimal: bool,
    section: str | None,
) -> list[tuple[str, bool, str]]:
    py = venv_python(REPO_ROOT)
    results: list[tuple[str, bool, str]] = []
    for scenario in ALL_GENERATE_MANY_SCENARIOS:
        if section and scenario.section != section:
            continue
        db = scenario.db_name(E2E_DB_PREFIX)
        t0 = time.perf_counter()
        try:
            log_scenario("GEN", scenario.id, scenario.title)
            from pymongo import MongoClient

            with status_phase("Preparing database", heartbeat=False):
                client = MongoClient(uri)
                client.drop_database(db)
                client.close()
            counts = scenario.resolve_counts(minimal=minimal)
            with status_phase("Generating FHIR data"):
                load_gen_data(
                    seed=scenario.seed,
                    resources=scenario.resources,
                    counts=counts,
                    db=db,
                    mongo_uri=uri,
                    python=py,
                )
            elapsed = time.perf_counter() - t0
            results.append((scenario.id, True, f"ok ({elapsed:.1f}s) db={db}"))
            print(f"[GEN OK] {scenario.id}: {scenario.title}")
        except Exception as exc:
            results.append((scenario.id, False, str(exc)))
            print(f"[GEN FAIL] {scenario.id}: {exc}", file=sys.stderr)
            traceback.print_exc()
    return results


def run_pipeline_scenarios(
    *,
    uri: str,
    minimal: bool,
    section: str | None,
    reload_data: bool,
) -> list[tuple[str, bool, str]]:
    gen_py = venv_python(REPO_ROOT)
    mql_py = venv_python(MQL_ROOT)
    results: list[tuple[str, bool, str]] = []
    for mql_sc in ALL_PIPELINE_SCENARIOS:
        if section and mql_sc.section != section:
            continue
        gen_sc = GEN_BY_ID[mql_sc.gen_scenario_id]
        db = gen_sc.db_name(E2E_DB_PREFIX)
        t0 = time.perf_counter()
        try:
            log_scenario("PIPELINE", mql_sc.id, mql_sc.title)
            if reload_data:
                from pymongo import MongoClient

                with status_phase("Preparing database", heartbeat=False):
                    client = MongoClient(uri)
                    client.drop_database(db)
                    client.close()
                counts = gen_sc.resolve_counts(minimal=minimal)
                with status_phase("Generating FHIR data"):
                    load_gen_data(
                        seed=gen_sc.seed,
                        resources=gen_sc.resources,
                        counts=counts,
                        db=db,
                        mongo_uri=uri,
                        python=gen_py,
                    )
            resources = list(gen_sc.resources)
            if mql_sc.denormalize_all:
                with status_phase("Building search indexes"):
                    run_fhir_mql(
                        ["indexes", "--all"],
                        mongo_uri=uri,
                        db=db,
                        python=mql_py,
                    )
                with status_phase("Denormalizing resources"):
                    run_fhir_mql(
                        ["denormalize", "--all", "--batch-size", "100"],
                        mongo_uri=uri,
                        db=db,
                        python=mql_py,
                    )
            else:
                with status_phase("Building search indexes"):
                    run_fhir_mql(
                        ["indexes", *resources],
                        mongo_uri=uri,
                        db=db,
                        python=mql_py,
                    )
                with status_phase("Denormalizing resources"):
                    run_fhir_mql(
                        ["denormalize", *resources, "--batch-size", "100"],
                        mongo_uri=uri,
                        db=db,
                        python=mql_py,
                    )
            from e2e.search_artifacts import (  # noqa: E402
                E2E_RESULTS_ROOT,
                clear_scenario_results,
            )
            from e2e.search_runner import (  # noqa: E402
                assert_report_passed,
                execute_search_plan,
            )

            clear_scenario_results(mql_sc.id, E2E_RESULTS_ROOT)
            report = execute_search_plan(
                mql_sc,
                db,
                mongo_uri=uri,
                mql_python=mql_py,
                include_compartments=True,
                results_root=E2E_RESULTS_ROOT,
            )
            assert_report_passed(report)
            elapsed = time.perf_counter() - t0
            results.append((mql_sc.id, True, f"ok ({elapsed:.1f}s) db={db}"))
            print(f"[PIPELINE OK] {mql_sc.id}: {mql_sc.title}")
        except Exception as exc:
            results.append((mql_sc.id, False, str(exc)))
            print(f"[PIPELINE FAIL] {mql_sc.id}: {exc}", file=sys.stderr)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Run CLI_COMMANDS E2E scenarios")
    parser.add_argument(
        "--uri",
        default=os.environ.get("E2E_MONGODB_URI", DEFAULT_MONGODB_URI),
    )
    parser.add_argument(
        "--section",
        choices=("healthcare", "industrial"),
        default=None,
    )
    parser.add_argument(
        "--full-counts",
        action="store_true",
        help="Use documented volumes from CLI_COMMANDS (slow)",
    )
    parser.add_argument("--gen-only", action="store_true")
    parser.add_argument(
        "--pipeline-only",
        action="store_true",
        help="Drop/reload gen data per scenario, then fhir-mql (standalone)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print scenario pass/fail lines (no status updates)",
    )
    args = parser.parse_args()
    minimal = not args.full_counts
    set_status_logging(not args.quiet)
    from e2e.e2e_log import set_status_logging as mql_set_status  # noqa: E402

    mql_set_status(not args.quiet)

    try:
        _check_mongo(args.uri)
    except Exception as exc:
        print(f"MongoDB unavailable at {args.uri}: {exc}", file=sys.stderr)
        return 1

    all_results: list[tuple[str, bool, str]] = []
    if not args.pipeline_only:
        all_results.extend(
            run_gen_scenarios(
                uri=args.uri,
                minimal=minimal,
                section=args.section,
            )
        )
    if not args.gen_only:
        all_results.extend(
            run_pipeline_scenarios(
                uri=args.uri,
                minimal=minimal,
                section=args.section,
                reload_data=args.pipeline_only,
            )
        )

    failed = [r for r in all_results if not r[1]]
    print(f"\n{'=' * 60}")
    print(f"Total: {len(all_results)}  Passed: {len(all_results) - len(failed)}  Failed: {len(failed)}")
    for sid, ok, msg in failed:
        print(f"  FAIL {sid}: {msg}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
