#!/usr/bin/env python3
"""Smoke-test executable commands from CLI_COMMANDS.md."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Callable

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@dataclass
class Case:
    name: str
    argv: list[str] | None = None
    shell: str | None = None
    skip: bool = False
    expect_exit: int = 0
    accept_exit: tuple[int, ...] = ()
    must_not_contain: tuple[str, ...] = ()
    fn: Callable[[], None] | None = None


def run_shell(cmd: str, *, cwd: str = REPO_ROOT) -> tuple[int, str]:
    r = subprocess.run(
        cmd,
        shell=True,
        cwd=cwd,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    out = (r.stdout or "") + (r.stderr or "")
    return r.returncode, out


def run_argv(argv: list[str], *, cwd: str = REPO_ROOT) -> tuple[int, str]:
    r = subprocess.run(
        argv,
        cwd=cwd,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    out = (r.stdout or "") + (r.stderr or "")
    return r.returncode, out


def hybrid_db_exists(uri: str, db: str) -> bool:
    try:
        from pymongo import MongoClient

        client = MongoClient(uri, serverSelectionTimeoutMS=2000)
        return db in client.list_database_names()
    except Exception:
        return False


def build_cases(uri: str, db: str, *, skip_bulk: bool, skip_pytest: bool) -> list[Case]:
    cases: list[Case] = []

    def c(name: str, shell: str, **kw) -> None:
        cases.append(Case(name=name, shell=shell, **kw))

    def a(name: str, argv: list[str], **kw) -> None:
        cases.append(Case(name=name, argv=argv, **kw))

    # Install & verify
    a("fhir-mql --version", ["fhir-mql", "--version"])
    a("fhir-mql resources", ["fhir-mql", "resources"])
    a("fhir-mql resources --format json", ["fhir-mql", "resources", "--format", "json"])
    cases.append(
        Case(
            name="ConfigLoader list",
            fn=lambda: __import__(
                "fhir_search_to_mql", fromlist=["ConfigLoader"]
            ).ConfigLoader().list_resources(),
        )
    )
    cases.append(
        Case(
            name="MongoDB ping",
            fn=lambda: __import__("pymongo", fromlist=["MongoClient"])
            .MongoClient(uri, serverSelectionTimeoutMS=3000)
            .server_info(),
        )
    )

    convert_shell = [
        'fhir-mql convert Patient "_id=p1"',
        'fhir-mql convert Observation "_lastUpdated=ge2024-01-01"',
        'fhir-mql convert Patient "name:exact=Smith"',
        'fhir-mql convert Patient "identifier:missing=false"',
        'fhir-mql convert Appointment "status:not=cancelled"',
        'fhir-mql convert Patient "name=Smith&gender=male"',
        'fhir-mql convert Patient "birthdate=ge1980-01-01&birthdate=le1990-12-31"',
        'fhir-mql convert Patient "identifier=http://hospital.org/mrn|MRN-1001"',
        'fhir-mql convert Patient "active=true&address-city=Springfield"',
        'fhir-mql convert Patient "telecom=555-0100"',
        'fhir-mql convert Patient "deceased=false"',
        'fhir-mql convert Patient "language=en-US"',
        'fhir-mql convert Patient "organization=org-1"',
        'fhir-mql convert Practitioner "name=Jones&active=true"',
        'fhir-mql convert Practitioner "identifier=http://npi|1234567890"',
        'fhir-mql convert PractitionerRole "practitioner=pr-1&organization=org-1"',
        'fhir-mql convert PractitionerRole "location=loc-er&service=hs-cardiology"',
        'fhir-mql convert PractitionerRole "specialty=394814009"',
        'fhir-mql convert Organization "name=General Hospital&active=true"',
        'fhir-mql convert Organization "identifier=urn:oid:2.16.840.1.113883.4.6|123"',
        'fhir-mql convert Location "name=ER&status=active"',
        'fhir-mql convert Location "address-city=Boston&organization=org-1"',
        'fhir-mql convert Observation "code=http://loinc.org|8480-6"',
        'fhir-mql convert Observation "patient=p1&status=final"',
        'fhir-mql convert Observation "date=ge2024-06-01&code=8480-6"',
        'fhir-mql convert Observation "value-quantity=120"',
        'fhir-mql convert Observation "category=vital-signs"',
        'fhir-mql convert Observation "encounter=enc-1"',
        'fhir-mql convert Appointment "status=booked&patient=p1"',
        'fhir-mql convert Appointment "date=ge2024-07-01&actor=Practitioner/pr-1"',
        'fhir-mql convert Appointment "reason-code=185345009"',
        'fhir-mql convert Appointment "reason-reference=Condition/cond-1"',
        'fhir-mql convert Schedule "active=true&actor=Practitioner/pr-1"',
        'fhir-mql convert Schedule "service-type=11429006"',
        'fhir-mql convert Schedule "service-type-reference=HealthcareService/hs-1"',
        'fhir-mql convert Schedule "date=ge2024-07-01"',
        'fhir-mql convert Slot "status=free&schedule=sched-1&start=ge2024-07-15"',
        'fhir-mql convert Encounter "status=in-progress&patient=p1"',
        'fhir-mql convert Encounter "class=AMB&type=185349003"',
        'fhir-mql convert Encounter "date=ge2024-07-01&practitioner=pr-1"',
        'fhir-mql convert Encounter "date-start=ge2024-07-01&end-date=le2024-07-31"',
        'fhir-mql convert Encounter "location=loc-1&service-provider=org-1"',
        'fhir-mql convert Encounter "diagnosis-code=44054006"',
        'fhir-mql convert Encounter "part-of=enc-parent"',
        'fhir-mql convert Condition "clinical-status=active&patient=p1"',
        'fhir-mql convert Condition "code=44054006&verification-status=confirmed"',
        'fhir-mql convert Condition "encounter=enc-1&onset-date=ge2020-01-01"',
        'fhir-mql convert Condition "category=problem-list-item"',
        'fhir-mql convert Device "status=active&organization=org-1"',
        'fhir-mql convert Device "type=182722004&manufacturer=Acme"',
        'fhir-mql convert Device "expiration-date=le2025-12-31"',
        'fhir-mql convert Group "name=Cohort-A&type=person"',
        'fhir-mql convert Group "member=Patient/p1&membership=enumerated"',
        'fhir-mql convert Group "characteristic=73211009"',
        'fhir-mql convert Patient "name=Smith&gender=male&birthdate=ge1980-01-01"',
        'fhir-mql convert Observation "patient=p1&code=8480-6&date=ge2024-01-01&status=final"',
        'fhir-mql convert Appointment "status=booked&patient=p1&date=ge2024-07-01"',
    ]
    for i, cmd in enumerate(convert_shell, 1):
        c(f"convert[{i}]", cmd, must_not_contain=("Warning: Parameter", "Traceback"))

    compartment = [
        ('fhir-mql convert Observation "code=8480-6" --compartment-type Patient --compartment-id p1', "compartment Observation/Patient"),
        ('fhir-mql convert Schedule "" --compartment-type Practitioner --compartment-id pr-1', "compartment Schedule/Practitioner"),
        ('fhir-mql convert Observation "code=8480-6" --compartment-type Device --compartment-id dev-1', "compartment Observation/Device"),
        ('fhir-mql convert Encounter "status=in-progress" --compartment-type Encounter --compartment-id enc-1', "compartment Encounter"),
        ('fhir-mql convert Schedule "" --compartment-type RelatedPerson --compartment-id rp-1', "compartment Schedule/RelatedPerson"),
    ]
    for cmd, name in compartment:
        c(name, cmd, must_not_contain=("Warning: Parameter", "Traceback"))

    a(
        "search Patient --uri --db json",
        [
            "fhir-mql",
            "search",
            "Patient",
            "name=Smith",
            "--uri",
            uri,
            "--db",
            db,
            "--format",
            "json",
        ],
    )

    search_shell = [
        'fhir-mql search Patient "name=Smith&gender=male" --limit 10',
        'fhir-mql search Observation "code=http://loinc.org|8480-6&status=final" --limit 50',
        'fhir-mql search Slot "status=free&start=ge2024-07-01" --limit 20',
        'fhir-mql search Patient "name=Smith" --explain',
        'fhir-mql search Observation "status=final" --compartment-type Patient --compartment-id p1 --limit 25',
        'fhir-mql search Schedule "active=true" --compartment-type Practitioner --compartment-id pr-1 --limit 5',
        'fhir-mql search Patient "identifier=http://hospital.org/mrn|MRN-1001" --limit 5',
        'fhir-mql search Patient "name=Smith&birthdate=1980-05-15" --limit 10',
        'fhir-mql search Patient "name:exact=Smith&gender=male" --limit 5',
        'fhir-mql search Practitioner "name=Jones&active=true" --limit 20',
        'fhir-mql search PractitionerRole "organization=org-1&active=true" --limit 50',
        'fhir-mql search PractitionerRole "practitioner=pr-1" --limit 5',
        'fhir-mql search Organization "name=Hospital&active=true" --limit 10',
        'fhir-mql search Location "name=ER&status=active" --limit 10',
        'fhir-mql search Location "organization=org-1" --limit 25',
        'fhir-mql search Schedule "active=true&actor=Practitioner/pr-1" --limit 10',
        'fhir-mql search Slot "status=free&schedule=sched-1&start=ge2024-07-15&start=le2024-07-31" --limit 100',
        'fhir-mql search Appointment "status=booked&patient=p1&date=ge2024-07-01" --limit 20',
        'fhir-mql search Encounter "status=in-progress&location=loc-er" --limit 50',
        'fhir-mql search Encounter "patient=p1&status=in-progress" --limit 5',
        'fhir-mql search Encounter "practitioner=pr-1&date=ge2024-07-01" --limit 30',
        'fhir-mql search Condition "patient=p1&clinical-status=active" --limit 50',
        'fhir-mql search Condition "code=44054006&verification-status=confirmed" --limit 10',
        'fhir-mql search Condition "clinical-status=active" --compartment-type Patient --compartment-id p1 --limit 50',
        'fhir-mql search Observation "patient=p1&category=vital-signs&date=ge2024-06-01" --limit 100',
        'fhir-mql search Device "status=active" --limit 20',
        'fhir-mql search Device "manufacturer=Acme&expiration-date=le2025-12-31" --limit 50',
        'fhir-mql search Device "identifier=DEV-001" --limit 5',
        'fhir-mql search Group "name=Diabetes-Cohort&type=person" --limit 10',
        'fhir-mql search Group "member=Patient/p1" --limit 5',
        'fhir-mql search Group "type=person&characteristic=73211009" --limit 20',
        'fhir-mql search Appointment "actor=Practitioner/pr-1&status=booked" --limit 30',
        'fhir-mql search Encounter "participant=Practitioner/pr-1&status=completed" --limit 30',
        'fhir-mql search Encounter "careteam=ct-1" --limit 20',
    ]
    for i, cmd in enumerate(search_shell, 1):
        c(f"search[{i}]", cmd, must_not_contain=("Warning: Parameter", "Traceback"))

    if not skip_bulk:
        # exit 4 = index name conflict if DB was indexed with older YAML names
        a(
            "indexes Patient",
            ["fhir-mql", "indexes", "Patient", "--uri", uri, "--db", db],
            accept_exit=(4,),
        )
        a(
            "indexes multi dry-run",
            ["fhir-mql", "indexes", "Patient", "Observation", "Encounter", "--uri", uri, "--db", db, "--dry-run"],
        )
        a("indexes --all dry-run", ["fhir-mql", "indexes", "--all", "--dry-run"])
        a("indexes Slot Schedule dry-run", ["fhir-mql", "indexes", "Slot", "Schedule", "--dry-run"])
        a(
            "denormalize Patient dry-run",
            ["fhir-mql", "denormalize", "Patient", "--uri", uri, "--db", db, "--dry-run"],
        )
        a(
            "denormalize --all dry-run",
            ["fhir-mql", "denormalize", "--all", "--uri", uri, "--db", db, "--dry-run"],
        )
        a(
            "denormalize --all limit 1",
            ["fhir-mql", "denormalize", "--all", "--uri", uri, "--db", db, "--limit", "1"],
        )
        a("stats Patient", ["fhir-mql", "stats", "Patient", "--uri", uri, "--db", db])
        a(
            "stats --all json",
            ["fhir-mql", "stats", "--all", "--uri", uri, "--db", db, "--format", "json"],
        )
        a(
            "stats multi",
            [
                "fhir-mql",
                "stats",
                "Patient",
                "Observation",
                "Encounter",
                "Appointment",
                "--uri",
                uri,
                "--db",
                db,
            ],
        )

    # Skipped destructive / doc-only
    for name in (
        "reset Patient (destructive)",
        "reset --all (destructive)",
        "disaster recovery pipeline (destructive)",
        "indexes --all live (skipped; use dry-run)",
        "denormalize --all live (skipped; use limit 1)",
        "hybrid DB full denormalize/reset (destructive)",
        "synthetic reset --all (destructive)",
        "build_indexes (slow; run manually)",
        "docker mongo (manual)",
        "venv install (manual)",
    ):
        cases.append(Case(name=name, skip=True))

    if hybrid_db_exists(uri, "fhir_schedule_appointment_hybrid"):
        a(
            "hybrid search Patient",
            [
                "fhir-mql",
                "search",
                "Patient",
                "name=Taylor",
                "--limit",
                "10",
                "--uri",
                uri,
                "--db",
                "fhir_schedule_appointment_hybrid",
            ],
        )
        a(
            "hybrid denormalize dry-run",
            [
                "fhir-mql",
                "denormalize",
                "--uri",
                uri,
                "--db",
                "fhir_schedule_appointment_hybrid",
                "Patient",
                "--dry-run",
            ],
        )
    else:
        cases.append(Case(name="hybrid DB commands", skip=True))

    cases.append(
        Case(
            name="API convert",
            fn=lambda: print(
                __import__("fhir_search_to_mql", fromlist=["FHIRSearchConverter"])
                .FHIRSearchConverter()
                .convert("Patient", "name=Smith")
            ),
        )
    )
    cases.append(
        Case(
            name="API denormalize",
            fn=lambda: print(
                __import__("fhir_search_to_mql", fromlist=["ResourceDenormalizer"])
                .ResourceDenormalizer()
                .denormalize(
                    {
                        "resourceType": "Patient",
                        "id": "x",
                        "name": [{"family": "Smith"}],
                    }
                )
                .get("_search", {})
            ),
        )
    )
    cases.append(
        Case(
            name="API compartment",
            fn=lambda: print(
                __import__("fhir_search_to_mql", fromlist=["FHIRSearchConverter"])
                .FHIRSearchConverter()
                .convert_with_compartment("Patient", "p1", "Observation", "code=8480-6")
            ),
        )
    )

    for res in ("Encounter", "Condition", "Patient"):
        a(f"resource_spec {res}", [sys.executable, "-m", "fhir_search_to_mql.schema.resource_spec", res])

    c("where fhir-mql", "where.exe fhir-mql" if sys.platform == "win32" else "which fhir-mql")
    c("pip show", "pip show fhir-search-to-mql")
    c("denormalize Bogus (expect fail)", "fhir-mql denormalize Bogus", expect_exit=1)

    if not skip_pytest:
        a(
            "pytest unit",
            [sys.executable, "-m", "pytest", "tests/unit/", "-q", "--no-cov"],
        )
        a(
            "pytest audit harness",
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/integration/test_config_audit_regressions.py",
                "-q",
                "--no-cov",
            ],
        )
    else:
        cases.append(Case(name="pytest suites", skip=True))

    return cases


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-pytest", action="store_true")
    parser.add_argument("--skip-bulk", action="store_true")
    parser.add_argument("--uri", default=os.environ.get("MONGODB_URI", "mongodb://localhost:27017/"))
    parser.add_argument("--db", default=os.environ.get("MONGODB_DB", "fhir_synthetic"))
    args = parser.parse_args()

    os.environ["MONGODB_URI"] = args.uri
    os.environ["MONGODB_DB"] = args.db

    passed = failed = skipped = 0
    failures: list[tuple[str, str]] = []

    for case in build_cases(args.uri, args.db, skip_bulk=args.skip_bulk, skip_pytest=args.skip_pytest):
        if case.skip:
            skipped += 1
            print(f"[SKIP] {case.name}")
            continue

        print(f"[RUN ] {case.name}")
        detail = ""
        try:
            if case.fn is not None:
                case.fn()
                code, out = 0, ""
            elif case.argv is not None:
                code, out = run_argv(case.argv)
            elif case.shell is not None:
                code, out = run_shell(case.shell)
            else:
                raise RuntimeError("empty case")
        except Exception as exc:
            code, out = 1, str(exc)

        ok_codes = (case.expect_exit,) + case.accept_exit
        bad = code not in ok_codes
        if not bad:
            for pat in case.must_not_contain:
                if pat in out:
                    bad = True
                    detail += f" contains {pat!r}"
        if bad:
            failed += 1
            if not detail:
                detail = f"exit={code} (expected {case.expect_exit})"
            failures.append((case.name, detail))
            print(f"[FAIL] {case.name} — {detail}")
            if out.strip():
                print(out[:600])
        else:
            passed += 1
            print(f"[PASS] {case.name}")

    print()
    print("======== SUMMARY ========")
    print(f"PASS: {passed}  FAIL: {failed}  SKIP: {skipped}")
    if failures:
        print("\nFailures:")
        for name, detail in failures:
            print(f"  - {name}: {detail}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
