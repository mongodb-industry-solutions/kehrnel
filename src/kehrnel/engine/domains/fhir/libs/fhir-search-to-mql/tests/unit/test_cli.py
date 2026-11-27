"""
Unit tests for the ``fhir-mql`` CLI.

These exercise pure-function paths (parser construction, argument
defaults, the ``convert`` and ``resources`` subcommands) and do NOT
require a MongoDB connection. Database-touching subcommands
(``denormalize`` / ``search`` / ``indexes`` / ``reset`` / ``stats``)
are exercised in :mod:`tests.integration.test_cli_integration`,
which spins up a real local MongoDB collection.
"""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout, redirect_stderr

import pytest

from fhir_search_to_mql import cli


# ---------------------------------------------------------------------------
# Parser construction
# ---------------------------------------------------------------------------


class TestBuildParser:
    def test_parser_exposes_all_subcommands(self):
        parser = cli.build_parser()
        # Subparsers are stored on the action with dest='command'.
        sub = next(
            a for a in parser._actions if getattr(a, "dest", None) == "command"
        )
        assert set(sub.choices) == {
            "resources",
            "convert",
            "search",
            "denormalize",
            "indexes",
            "reset",
            "stats",
        }

    def test_version_flag_emits_version(self, capsys):
        with pytest.raises(SystemExit) as excinfo:
            cli.main(["--version"])
        out = capsys.readouterr().out
        assert excinfo.value.code == 0
        assert "fhir-mql" in out

    def test_no_command_prints_help_and_exits_usage(self, capsys):
        rc = cli.main([])
        captured = capsys.readouterr()
        # argparse prints help to stderr per our wiring.
        assert rc == cli.EXIT_USAGE
        assert "fhir-mql" in captured.err


# ---------------------------------------------------------------------------
# Subcommand: resources
# ---------------------------------------------------------------------------


class TestResourcesCommand:
    def test_resources_table_lists_bundled(self, capsys):
        rc = cli.main(["resources"])
        out = capsys.readouterr().out
        assert rc == cli.EXIT_OK
        # Bundled configs ship at least these five.
        for resource in ("Patient", "Observation", "Appointment", "Organization", "Location"):
            assert resource in out

    def test_resources_json_is_machine_readable(self, capsys):
        rc = cli.main(["resources", "--format", "json"])
        out = capsys.readouterr().out
        assert rc == cli.EXIT_OK
        payload = json.loads(out)
        assert isinstance(payload, list)
        names = {item["resource"] for item in payload}
        assert {"Patient", "Observation"}.issubset(names)
        # Each entry has the expected schema.
        for item in payload:
            assert "fhir_version" in item
            assert "search_parameters" in item
            assert "denormalization_rules" in item
            assert "indexes" in item


# ---------------------------------------------------------------------------
# Subcommand: convert (no DB)
# ---------------------------------------------------------------------------


class TestConvertCommand:
    def test_convert_simple_query(self, capsys):
        rc = cli.main(["convert", "Patient", "name=Smith&gender=male"])
        out = capsys.readouterr().out
        assert rc == cli.EXIT_OK
        payload = json.loads(out)
        # Compound query is wrapped in $and.
        assert "$and" in payload

    def test_convert_with_compartment(self, capsys):
        rc = cli.main([
            "convert", "Observation", "code=8480-6",
            "--compartment-type", "Patient",
            "--compartment-id", "patient-123",
        ])
        out = capsys.readouterr().out
        assert rc == cli.EXIT_OK
        payload = json.loads(out)
        # Patient compartment fast-path uses _compartments.Patient.
        assert "_compartments.Patient" in json.dumps(payload)

    def test_convert_unknown_resource_returns_config_error(self, capsys):
        rc = cli.main(["convert", "NoSuchResource", "name=x"])
        captured = capsys.readouterr()
        assert rc == cli.EXIT_CONFIG
        assert "NoSuchResource" in captured.err


# ---------------------------------------------------------------------------
# Multi-resource expansion
# ---------------------------------------------------------------------------


class TestExpandResourceList:
    def test_explicit_resources_expand_dependencies_by_default(self):
        loader = cli.ConfigLoader()
        args = cli.build_parser().parse_args(
            ["denormalize", "Observation", "--dry-run"]
        )
        expanded = cli._expand_resource_list(args, loader)
        assert "Observation" in expanded
        assert expanded.index("Patient") < expanded.index("Observation")
        assert "Encounter" in expanded
        assert "Practitioner" in expanded

    def test_no_with_deps_returns_only_requested(self):
        loader = cli.ConfigLoader()
        args = cli.build_parser().parse_args(
            [
                "denormalize",
                "Patient",
                "Observation",
                "--no-with-deps",
                "--dry-run",
            ]
        )
        assert cli._expand_resource_list(args, loader) == [
            "Patient",
            "Observation",
        ]

    def test_measure_report_expands_anchor_types(self):
        loader = cli.ConfigLoader()
        args = cli.build_parser().parse_args(
            ["denormalize", "MeasureReport", "--dry-run"]
        )
        expanded = cli._expand_resource_list(args, loader)
        assert expanded[-1] == "MeasureReport"
        for anchor in ("Measure", "Patient", "Practitioner", "Organization"):
            assert anchor in expanded

    def test_all_flag_returns_every_configured(self):
        loader = cli.ConfigLoader()
        args = cli.build_parser().parse_args(
            ["denormalize", "--all", "--dry-run"]
        )
        result = cli._expand_resource_list(args, loader)
        assert set(result) == set(loader.list_resources())

    def test_unknown_resource_raises(self, capsys):
        loader = cli.ConfigLoader()
        args = cli.build_parser().parse_args(
            ["denormalize", "Patient", "Bogus", "--dry-run"]
        )
        with pytest.raises(SystemExit) as exc:
            cli._expand_resource_list(args, loader)
        assert "Bogus" in str(exc.value)

    def test_no_resources_and_no_all_raises(self):
        loader = cli.ConfigLoader()
        args = cli.build_parser().parse_args(["denormalize", "--dry-run"])
        with pytest.raises(SystemExit):
            cli._expand_resource_list(args, loader)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


class TestResolveConnection:
    def test_uri_precedence_args_over_env(self, monkeypatch):
        monkeypatch.setenv("MONGODB_URI", "mongodb://from-env/")
        args = cli.build_parser().parse_args([
            "stats", "--all", "--uri", "mongodb://from-arg/"
        ])
        assert cli._resolve_uri(args) == "mongodb://from-arg/"

    def test_uri_falls_back_to_env(self, monkeypatch):
        monkeypatch.setenv("MONGODB_URI", "mongodb://from-env/")
        args = cli.build_parser().parse_args(["stats", "--all"])
        assert cli._resolve_uri(args) == "mongodb://from-env/"

    def test_uri_falls_back_to_default(self, monkeypatch):
        monkeypatch.delenv("MONGODB_URI", raising=False)
        args = cli.build_parser().parse_args(["stats", "--all"])
        assert cli._resolve_uri(args) == cli.DEFAULT_MONGODB_URI

    def test_db_precedence(self, monkeypatch):
        monkeypatch.setenv("MONGODB_DB", "envdb")
        args = cli.build_parser().parse_args([
            "stats", "--all", "--db", "argdb"
        ])
        assert cli._resolve_db_name(args) == "argdb"

    def test_db_falls_back_to_default(self, monkeypatch):
        monkeypatch.delenv("MONGODB_DB", raising=False)
        args = cli.build_parser().parse_args(["stats", "--all"])
        assert cli._resolve_db_name(args) == cli.DEFAULT_DB_NAME

    def test_collection_prefix_applied(self):
        args = cli.build_parser().parse_args([
            "stats", "Patient",
            "--collection-prefix", "fhir_",
        ])
        assert cli._resolve_collection_name(args, "Patient") == "fhir_Patient"


# ---------------------------------------------------------------------------
# Dry-run paths (no DB I/O)
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_denormalize_dry_run_does_not_open_db(self, capsys):
        rc = cli.main([
            "denormalize", "Patient", "Observation", "--dry-run"
        ])
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        # We never imported pymongo or opened a client, so no error
        # bubbles up even on machines without MongoDB running. The
        # output explicitly says [dry-run] for each resource.
        assert "[dry-run]" in captured.err
        assert "Patient" in captured.err
        assert "Observation" in captured.err

    def test_indexes_dry_run(self, capsys):
        rc = cli.main(["indexes", "--all", "--dry-run"])
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        assert "[dry-run]" in captured.err

    def test_reset_dry_run(self, capsys):
        rc = cli.main(["reset", "Patient", "--dry-run"])
        captured = capsys.readouterr()
        assert rc == cli.EXIT_OK
        assert "[dry-run]" in captured.err
