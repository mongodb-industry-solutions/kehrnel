"""
Integration tests for the ``fhir-mql`` CLI against a real MongoDB.

These cover the database-touching subcommands (``denormalize``,
``search``, ``indexes``, ``reset``, ``stats``) plus the
``_compartments`` write/clear contract that protects the Patient
compartment fast-path. Each test seeds and tears down its own
collections so they run safely against any local Mongo instance.

The suite is automatically skipped if MongoDB isn't reachable, so it
behaves gracefully in CI environments without a sidecar database.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from fhir_search_to_mql import (
    MongoDBHandler,
    ResourceDenormalizer,
)
from fhir_search_to_mql import cli


pytestmark = pytest.mark.mongodb


@pytest.fixture(scope="module")
def mongo_client():
    """Module-scoped pymongo client, skipping the suite if Mongo is down."""
    try:
        from pymongo import MongoClient
        client = MongoClient(
            "mongodb://localhost:27017/", serverSelectionTimeoutMS=2000
        )
        client.server_info()
        yield client
        client.close()
    except Exception as exc:
        pytest.skip(f"MongoDB not available: {exc}")


@pytest.fixture
def cli_db(mongo_client):
    """A throwaway database that is dropped after each test."""
    db = mongo_client["fhir_cli_integration_test"]
    # Purge anything from a previous run.
    for name in db.list_collection_names():
        db.drop_collection(name)
    yield db
    mongo_client.drop_database("fhir_cli_integration_test")


def _seed_patients(db) -> List[Dict[str, Any]]:
    docs = [
        {
            "resourceType": "Patient",
            "id": "p1",
            "name": [{"family": "Smith", "given": ["John"]}],
            "gender": "male",
            "birthDate": "1980-05-15",
        },
        {
            "resourceType": "Patient",
            "id": "p2",
            "name": [{"family": "Doe", "given": ["Jane"]}],
            "gender": "female",
            "birthDate": "1992-03-21",
        },
    ]
    db.Patient.insert_many(docs)
    return docs


def _seed_observations(db) -> List[Dict[str, Any]]:
    docs = [
        {
            "resourceType": "Observation",
            "id": "o1",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8480-6"}]},
            "subject": {"reference": "Patient/p1"},
            "effectiveDateTime": "2024-05-01T10:00:00Z",
        },
        {
            "resourceType": "Observation",
            "id": "o2",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8462-4"}]},
            "subject": {"reference": "Patient/p2"},
            "effectiveDateTime": "2024-06-01T10:00:00Z",
        },
    ]
    db.Observation.insert_many(docs)
    return docs


def _cli_args_with_db(db, *extra: str) -> List[str]:
    """Build CLI argv that targets the test database explicitly."""
    return list(extra) + ["--uri", "mongodb://localhost:27017/", "--db", db.name]


# ---------------------------------------------------------------------------
# MongoDBHandler._compartments fix (Option 2)
# ---------------------------------------------------------------------------


class TestCompartmentsBucketSync:
    """
    Pin the contract that ``_compartments`` is written and cleared
    alongside ``_search``. The historical bug only updated ``_search``
    on bulk re-denormalization, leaving ``_compartments`` stale.
    """

    def test_update_search_fields_writes_compartments(self, cli_db):
        _seed_patients(cli_db)
        _seed_observations(cli_db)
        denormalizer = ResourceDenormalizer()

        MongoDBHandler.update_search_fields(
            collection=cli_db.Observation,
            processor=denormalizer.denormalize,
        )

        for obs in cli_db.Observation.find():
            assert "_search" in obs, "_search must be persisted"
            assert "_compartments" in obs, (
                "Observation must persist _compartments so the Patient "
                "compartment fast-path keeps working after re-denorm"
            )
            assert "Patient" in obs["_compartments"]

    def test_remove_search_fields_clears_compartments(self, cli_db):
        _seed_patients(cli_db)
        denormalizer = ResourceDenormalizer()
        MongoDBHandler.update_search_fields(
            collection=cli_db.Patient,
            processor=denormalizer.denormalize,
        )

        # Sanity: both buckets present before reset.
        before = cli_db.Patient.find_one({"id": "p1"})
        assert "_search" in before
        assert "_compartments" in before

        MongoDBHandler.remove_search_fields(cli_db.Patient)

        after = cli_db.Patient.find_one({"id": "p1"})
        assert "_search" not in after, "_search must be unset"
        assert "_compartments" not in after, (
            "_compartments must be unset symmetrically with _search"
        )

    def test_batch_process_in_place_writes_compartments(self, cli_db):
        _seed_patients(cli_db)
        denormalizer = ResourceDenormalizer()

        MongoDBHandler.batch_process(
            collection=cli_db.Patient,
            processor=denormalizer.denormalize,
            update_in_place=True,
        )

        for pat in cli_db.Patient.find():
            assert "_search" in pat
            # Patient denormalization includes a self-compartment entry.
            assert "_compartments" in pat
            assert pat["_compartments"].get("Patient")


# ---------------------------------------------------------------------------
# CLI subcommand: denormalize
# ---------------------------------------------------------------------------


class TestCLIDenormalize:
    def test_denormalize_multiple_resources_one_command(self, cli_db, capsys):
        _seed_patients(cli_db)
        _seed_observations(cli_db)

        rc = cli.main(_cli_args_with_db(
            cli_db, "denormalize", "Patient", "Observation", "--format", "json"
        ))
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        payload = json.loads(captured.out)
        assert {row["resource"] for row in payload} == {"Patient", "Observation"}
        for row in payload:
            assert row["processed"] == 2
            assert row["updated"] == 2
            assert row["failed"] == 0

        # And the buckets were actually persisted.
        assert all("_search" in d for d in cli_db.Patient.find())
        assert all("_compartments" in d for d in cli_db.Observation.find())

    def test_denormalize_all_uses_every_configured_resource(self, cli_db, capsys):
        _seed_patients(cli_db)
        _seed_observations(cli_db)

        rc = cli.main(_cli_args_with_db(
            cli_db, "denormalize", "--all", "--format", "json"
        ))
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        payload = json.loads(captured.out)
        names = {row["resource"] for row in payload}
        # Bundled configs include at least these resources; we assert
        # the CLI iterated each one (collections that don't exist
        # simply report 0 processed).
        assert {"Patient", "Observation", "Appointment"}.issubset(names)


# ---------------------------------------------------------------------------
# CLI subcommand: search (with compartment fast-path)
# ---------------------------------------------------------------------------


class TestCLISearch:
    def test_search_patient_by_name_and_gender(self, cli_db, capsys):
        _seed_patients(cli_db)
        # Denormalize first so _search fields are populated.
        cli.main(_cli_args_with_db(cli_db, "denormalize", "Patient"))
        capsys.readouterr()  # discard

        rc = cli.main(_cli_args_with_db(
            cli_db, "search", "Patient", "name=Smith&gender=male"
        ))
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        payload = json.loads(captured.out)
        assert payload["count"] == 1
        assert payload["results"][0]["id"] == "p1"

    def test_search_observation_via_patient_compartment(self, cli_db, capsys):
        _seed_patients(cli_db)
        _seed_observations(cli_db)
        cli.main(_cli_args_with_db(cli_db, "denormalize", "--all"))
        capsys.readouterr()

        rc = cli.main(_cli_args_with_db(
            cli_db, "search", "Observation", "code=8480-6",
            "--compartment-type", "Patient",
            "--compartment-id", "p1",
        ))
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        payload = json.loads(captured.out)
        assert "_compartments.Patient" in json.dumps(payload["mql"])
        assert payload["count"] == 1
        assert payload["results"][0]["id"] == "o1"

    def test_search_explain_skips_db_execution(self, cli_db, capsys):
        rc = cli.main(_cli_args_with_db(
            cli_db, "search", "Patient", "name=Smith", "--explain"
        ))
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        payload = json.loads(captured.out)
        assert "mql" in payload
        assert "results" not in payload


# ---------------------------------------------------------------------------
# CLI subcommand: stats / reset
# ---------------------------------------------------------------------------


class TestCLIStatsAndReset:
    def test_stats_reports_search_coverage(self, cli_db, capsys):
        _seed_patients(cli_db)
        cli.main(_cli_args_with_db(cli_db, "denormalize", "Patient"))
        capsys.readouterr()

        rc = cli.main(_cli_args_with_db(
            cli_db, "stats", "Patient", "--format", "json"
        ))
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        payload = json.loads(captured.out)
        assert payload[0]["resource"] == "Patient"
        assert payload[0]["total_count"] == 2
        assert payload[0]["with_search"] == 2

    def test_reset_clears_both_buckets(self, cli_db, capsys):
        _seed_patients(cli_db)
        cli.main(_cli_args_with_db(cli_db, "denormalize", "Patient"))
        capsys.readouterr()

        rc = cli.main(_cli_args_with_db(
            cli_db, "reset", "Patient", "--format", "json"
        ))
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        payload = json.loads(captured.out)
        assert payload[0]["modified"] == 2

        # Both buckets are gone.
        for doc in cli_db.Patient.find():
            assert "_search" not in doc
            assert "_compartments" not in doc


# ---------------------------------------------------------------------------
# Regression: per-field failures are counted and surfaced
# ---------------------------------------------------------------------------


class TestBulkFieldFailureReporting:
    """
    Pin the contract that per-FIELD denormalization failures are
    counted in the bulk stats dict and surfaced in the completion
    log line. The historical behavior reported "0 failed" even when
    every document had a silently-broken rule, masking real bugs in
    user-supplied YAML configs.
    """

    @pytest.fixture
    def broken_overrides_dir(self, tmp_path):
        """Layered config dir whose Patient.yaml references a missing extractor."""
        cfg = tmp_path / "Patient.yaml"
        cfg.write_text(
            "resource: Patient\n"
            "fhir_version: R5\n"
            "denormalization:\n"
            "  bogus:\n"
            "    source: name\n"
            "    extractor: NotARealExtractor\n"
            "    target: _search\n"
            "    field_mappings:\n"
            "      - source_path: name[*].family\n"
            "        target_field: someField\n"
            "        datatype: string\n"
            "  name:\n"
            "    source: name\n"
            "    extractor: HumanNameExtractor\n"
            "    target: _search\n"
            "    field_mappings:\n"
            "      - source_path: name[*].family\n"
            "        target_field: familyName\n"
            "        datatype: array[string]\n"
            "search_parameters:\n"
            "  name:\n"
            "    type: string\n"
            "    fields:\n"
            "      default:\n"
            "        - field: _search.familyName\n",
            encoding="utf-8",
        )
        return tmp_path

    def test_handler_counts_per_field_failures(self, cli_db, broken_overrides_dir):
        """
        ``MongoDBHandler.update_search_fields`` MUST count every
        per-field rule failure in the new ``field_failures`` /
        ``documents_with_field_failures`` stats so callers get an
        accurate summary even when no document raised outright.
        """
        cli_db.Patient.insert_many([
            {
                "resourceType": "Patient", "id": f"p{i}",
                "name": [{"family": f"Smith{i}", "given": ["John"]}],
                "gender": "male",
                "birthDate": "1980-01-01",
            }
            for i in range(5)
        ])

        denormalizer = ResourceDenormalizer(
            config_dir=str(broken_overrides_dir)
        )
        stats = MongoDBHandler.update_search_fields(
            collection=cli_db.Patient,
            processor=denormalizer.denormalize,
        )

        # Every doc was still written (the working `name` rule
        # succeeded), so per-document counts are clean.
        assert stats["processed"] == 5
        assert stats["updated"] == 5
        assert stats["failed"] == 0

        # But the broken `bogus` rule fired on every doc — five
        # field failures across five documents.
        assert stats["field_failures"] == 5
        assert stats["documents_with_field_failures"] == 5

    def test_cli_table_includes_field_warning_columns(self, cli_db, broken_overrides_dir, capsys):
        """The ``denormalize --format table`` output must show the new columns."""
        cli_db.Patient.insert_many([
            {
                "resourceType": "Patient", "id": f"p{i}",
                "name": [{"family": f"Smith{i}", "given": ["John"]}],
                "gender": "male",
                "birthDate": "1980-01-01",
            }
            for i in range(3)
        ])

        rc = cli.main(_cli_args_with_db(
            cli_db, "denormalize", "Patient",
            "--config-dir", str(broken_overrides_dir),
            "--format", "table",
        ))
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        assert "FieldWarn" in captured.out
        # Three docs, each with one broken field → "3" appears in
        # both the field-warning total and the doc-with-warnings
        # count column.
        assert "3" in captured.out

    def test_cli_json_surfaces_field_failure_counts(self, cli_db, broken_overrides_dir, capsys):
        """The ``denormalize --format json`` output must include the new keys."""
        cli_db.Patient.insert_many([
            {
                "resourceType": "Patient", "id": "p1",
                "name": [{"family": "Smith", "given": ["John"]}],
                "gender": "male",
                "birthDate": "1980-01-01",
            }
        ])

        rc = cli.main(_cli_args_with_db(
            cli_db, "denormalize", "Patient",
            "--config-dir", str(broken_overrides_dir),
            "--format", "json",
        ))
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        payload = json.loads(captured.out)
        row = payload[0]
        assert row["processed"] == 1
        assert row["updated"] == 1
        assert row["failed"] == 0
        assert row["field_failures"] == 1
        assert row["documents_with_field_failures"] == 1


# ---------------------------------------------------------------------------
# Regression: Appointment multi-participant + Period nesting
# ---------------------------------------------------------------------------


class TestAppointmentDenormalizationRegression:
    """
    Pin the fixes that addressed two latent bugs surfaced by the
    bundled ``Appointment.yaml`` config when run against realistic
    multi-participant production data:

    1. The ``period`` rule referenced a nonexistent
       ``CustomExtractor`` and silently failed on every doc, leaving
       ``_search.appointmentPeriod`` unwritten.
    2. The ``participant`` rule declared ``patientId`` /
       ``practitionerId`` / ``locationId`` as scalar ``string`` even
       though the source path always yields a list. Multi-actor
       Appointments tripped the "expected string, got list"
       validator.

    These tests assert the post-fix contract: a real run against
    real-shape data produces zero per-field warnings and writes
    queryable nested structures.
    """

    def test_multi_participant_with_period_denormalizes_cleanly(self, cli_db, capsys):
        # Multi-participant Appointment with start AND end.
        cli_db.Appointment.insert_many([
            {
                "resourceType": "Appointment", "id": f"appt-{i}",
                "status": "booked",
                "start": "2024-06-20T09:00:00Z",
                "end": "2024-06-20T10:00:00Z",
                "participant": [
                    {"actor": {"reference": f"Patient/p{i}"}, "status": "accepted"},
                    {"actor": {"reference": "Practitioner/dr1"}, "status": "accepted"},
                    {"actor": {"reference": "Location/loc1"}, "status": "accepted"},
                ],
            }
            for i in range(10)
        ])

        denormalizer = ResourceDenormalizer()
        stats = MongoDBHandler.update_search_fields(
            collection=cli_db.Appointment,
            processor=denormalizer.denormalize,
        )
        # Per-document counters: every doc was written successfully.
        assert stats["processed"] == 10
        assert stats["updated"] == 10
        assert stats["failed"] == 0
        # Per-field counters: bundled Appointment.yaml is now clean,
        # so a happy-path run reports zero field-level failures —
        # the regression that broke this previously was the
        # "0 failed but 20 000 silent rule failures" footgun.
        assert stats["field_failures"] == 0
        assert stats["documents_with_field_failures"] == 0

        sample = cli_db.Appointment.find_one({"id": "appt-0"})
        s = sample["_search"]

        # Period rule output is a properly nested object — not a flat
        # `appointmentPeriod.start` literal-key.
        assert s["appointmentPeriod"] == {
            "start": "2024-06-20T09:00:00Z",
            "end": "2024-06-20T10:00:00Z",
        }

        # `actorIds` / `actorTypes` span every participant — these
        # back the FHIR `actor` search parameter and are intentionally
        # unfiltered.
        assert s["actorIds"] == ["p0", "dr1", "loc1"]
        assert s["actorTypes"] == ["Patient", "Practitioner", "Location"]

        # Type-filtered buckets via the new `filterType:` option in
        # ReferenceExtractor. Before the fix these were all aliases
        # of `actorIds`, which produced false-positive matches when
        # filtering by `patient=` if a Practitioner happened to share
        # an ID with a real Patient elsewhere in the dataset.
        assert s["patientId"] == ["p0"]
        assert s["practitionerId"] == ["dr1"]
        assert s["locationId"] == ["loc1"]

        # `participantStatus` holds the real participation status
        # codes (accepted/declined/tentative/needs-action) — not the
        # actor IDs that ReferenceExtractor's pre-resolved fall-through
        # used to emit for any non-reference source path.
        assert s["participantStatus"] == ["accepted", "accepted", "accepted"]

        # _compartments still gets only the actual Patient actors.
        assert sample["_compartments"]["Patient"] == ["p0"]

    def test_typed_id_buckets_are_sparse_when_no_matching_actor(self, cli_db):
        """
        Pin the sparse-output contract for type-filtered buckets:
        an Appointment with no Patient participant must omit
        ``_search.patientId`` entirely (no `[]`, no `null`). Tests
        depending on `$exists: True` to find Patient-anchored
        appointments rely on this.
        """
        cli_db.Appointment.insert_one({
            "resourceType": "Appointment", "id": "appt-no-patient",
            "status": "booked",
            "start": "2024-08-01T09:00:00Z",
            "participant": [
                {"actor": {"reference": "Practitioner/dr1"}, "status": "accepted"},
                {"actor": {"reference": "Location/loc1"}, "status": "accepted"},
            ],
        })

        denormalizer = ResourceDenormalizer()
        MongoDBHandler.update_search_fields(
            collection=cli_db.Appointment,
            processor=denormalizer.denormalize,
        )

        sample = cli_db.Appointment.find_one({"id": "appt-no-patient"})
        s = sample["_search"]
        assert "patientId" not in s
        assert s["practitionerId"] == ["dr1"]
        assert s["locationId"] == ["loc1"]
        assert s["participantStatus"] == ["accepted", "accepted"]
        # And the canonical "any participant" view still spans them.
        assert s["actorIds"] == ["dr1", "loc1"]
        assert s["actorTypes"] == ["Practitioner", "Location"]

    def test_search_by_patient_id_does_not_match_practitioner_with_same_id(self, cli_db):
        """
        Regression: `_search.patientId` MUST NOT contain
        Practitioner / Location IDs even when those IDs collide
        with valid Patient IDs in the dataset. Before `filterType:`
        was introduced, `patientId` was a duplicate of `actorIds`
        and a query for `patient=p9` would match an Appointment
        that only had a Practitioner with id=p9.
        """
        cli_db.Appointment.insert_many([
            {
                # Only a Practitioner whose ID is "p9" — NO Patient.
                "resourceType": "Appointment", "id": "appt-pract-only",
                "status": "booked",
                "start": "2024-09-01T09:00:00Z",
                "participant": [
                    {"actor": {"reference": "Practitioner/p9"}, "status": "accepted"},
                ],
            },
            {
                # A real Patient appointment with id=p9.
                "resourceType": "Appointment", "id": "appt-patient",
                "status": "booked",
                "start": "2024-09-02T09:00:00Z",
                "participant": [
                    {"actor": {"reference": "Patient/p9"}, "status": "accepted"},
                ],
            },
        ])

        denormalizer = ResourceDenormalizer()
        MongoDBHandler.update_search_fields(
            collection=cli_db.Appointment,
            processor=denormalizer.denormalize,
        )

        # Filter as the FHIR `patient` search parameter would.
        matches = list(cli_db.Appointment.find({"_search.patientId": "p9"}))
        ids = sorted(m["id"] for m in matches)
        assert ids == ["appt-patient"], (
            "patientId bucket leaked a Practitioner ID — type filtering broke"
        )

    def test_appointment_without_end_writes_sparse_period(self, cli_db):
        """
        An Appointment with only ``start`` must produce a Period
        containing just ``start`` — no ``end: None`` and no
        validation warning.
        """
        cli_db.Appointment.insert_one({
            "resourceType": "Appointment", "id": "appt-noend",
            "status": "pending",
            "start": "2024-07-01T08:00:00Z",
            "participant": [
                {"actor": {"reference": "Patient/px"}, "status": "tentative"},
            ],
        })

        denormalizer = ResourceDenormalizer()
        stats = MongoDBHandler.update_search_fields(
            collection=cli_db.Appointment,
            processor=denormalizer.denormalize,
        )
        assert stats["failed"] == 0

        sample = cli_db.Appointment.find_one({"id": "appt-noend"})
        period = sample["_search"]["appointmentPeriod"]
        assert period == {"start": "2024-07-01T08:00:00Z"}
        assert "end" not in period

    def test_period_field_is_queryable_via_nested_path(self, cli_db):
        """
        The dotted ``target_field: appointmentPeriod.start`` must
        materialize as a real nested path so MongoDB can range-query
        on it. (A literal dot in the key name would produce neither.)
        """
        cli_db.Appointment.insert_many([
            {
                "resourceType": "Appointment", "id": f"appt-{i}",
                "status": "booked",
                "start": f"2024-06-{20 + i:02d}T09:00:00Z",
                "end": f"2024-06-{20 + i:02d}T10:00:00Z",
                "participant": [
                    {"actor": {"reference": f"Patient/p{i}"}, "status": "accepted"},
                ],
            }
            for i in range(5)
        ])

        denormalizer = ResourceDenormalizer()
        MongoDBHandler.update_search_fields(
            collection=cli_db.Appointment,
            processor=denormalizer.denormalize,
        )

        # Range query that only works if `appointmentPeriod.start`
        # is a real nested path under `_search`.
        n = cli_db.Appointment.count_documents({
            "_search.appointmentPeriod.start": {
                "$gte": "2024-06-21T00:00:00Z",
                "$lt": "2024-06-23T00:00:00Z",
            },
        })
        assert n == 2  # only appt-1 (06-21) and appt-2 (06-22) match.


# ---------------------------------------------------------------------------
# CLI subcommand: indexes
# ---------------------------------------------------------------------------


class TestCLIIndexes:
    def test_indexes_creates_at_least_one_per_resource(self, cli_db, capsys):
        # Need a non-empty collection for create_index to be meaningful;
        # MongoDB will create the collection on the first index.
        rc = cli.main(_cli_args_with_db(
            cli_db, "indexes", "Patient", "--format", "json"
        ))
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        payload = json.loads(captured.out)
        assert payload[0]["resource"] == "Patient"
        # Patient.yaml ships with a non-trivial number of indexes.
        assert payload[0]["indexes_created"] >= 5
