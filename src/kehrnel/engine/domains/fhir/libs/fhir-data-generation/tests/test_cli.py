"""Prompt 15 — CLI tests."""

import json

import pytest
from click.testing import CliRunner

mongomock = pytest.importorskip("mongomock")

from fhir_gen.cli.main import cli


def _load_json_output(text: str):
    """Parse JSON from CLI stdout (may include status lines on stderr merged)."""
    for i, ch in enumerate(text):
        if ch in "[{":
            return json.loads(text[i:])
    raise ValueError(f"No JSON in output: {text[:200]!r}")


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(autouse=True)
def mock_mongo(monkeypatch):
    monkeypatch.setattr(
        "fhir_gen.persistence.mongo.MongoClient",
        mongomock.MongoClient,
    )


class TestCLI:
    def test_version(self, runner: CliRunner):
        result = runner.invoke(cli, ["version"])
        assert result.exit_code == 0
        assert "fhir-gen" in result.output

    def test_list_resources(self, runner: CliRunner):
        result = runner.invoke(cli, ["list-resources"])
        assert result.exit_code == 0
        assert "Patient" in result.output
        assert "158" in result.output or "Available resources" in result.output

    def test_generate_no_save_stdout(self, runner: CliRunner):
        result = runner.invoke(
            cli,
            ["--seed", "1", "generate", "Patient", "-n", "2", "--no-save"],
        )
        assert result.exit_code == 0, result.output
        data = _load_json_output(result.output)
        assert len(data) == 2
        assert data[0]["resourceType"] == "Patient"

    def test_generate_unknown_resource(self, runner: CliRunner):
        result = runner.invoke(cli, ["generate", "NotAResource", "--no-save"])
        assert result.exit_code == 1
        assert "Unknown resource" in result.output

    def test_schema_info(self, runner: CliRunner):
        result = runner.invoke(cli, ["schema-info", "Patient"])
        assert result.exit_code == 0
        assert "Patient" in result.output
        assert "resourceType" in result.output or "Total fields" in result.output

    def test_generate_many_no_save(self, runner: CliRunner):
        result = runner.invoke(
            cli,
            [
                "--seed", "42",
                "generate-many",
                "Patient",
                "Organization",
                "--no-save",
            ],
        )
        assert result.exit_code == 0
        summary = _load_json_output(result.output)
        assert summary["Patient"] >= 1
        assert summary["Organization"] >= 1

    def test_generate_many_count_pairs(self, runner: CliRunner):
        result = runner.invoke(
            cli,
            [
                "--seed", "42",
                "generate-many",
                "Patient",
                "Encounter",
                "--count", "Patient=2",
                "--count", "Encounter=3",
                "--no-save",
            ],
        )
        assert result.exit_code == 0, result.output
        summary = _load_json_output(result.output)
        assert summary["Patient"] == 2
        assert summary["Encounter"] == 3

    def test_generate_many_powershell_style_counts(self, runner: CliRunner):
        """PowerShell strips JSON quotes; CLI accepts {Patient:10,Encounter:2}."""
        result = runner.invoke(
            cli,
            [
                "--seed", "1",
                "generate-many",
                "Patient",
                "Encounter",
                "--counts",
                "{Patient:10,Encounter:2}",
                "--no-save",
            ],
        )
        assert result.exit_code == 0, result.output
        summary = _load_json_output(result.output)
        assert summary["Patient"] == 10
        assert summary["Encounter"] == 2

    def test_generate_variants(self, runner: CliRunner):
        result = runner.invoke(
            cli,
            [
                "--seed", "1",
                "generate",
                "Observation",
                "--variants",
                "--no-save",
            ],
        )
        assert result.exit_code == 0
        data = _load_json_output(result.output)
        assert len(data) >= 2

    def test_db_stats_empty(self, runner: CliRunner):
        result = runner.invoke(
            cli,
            ["--db", "fhir_cli_test_empty", "db-stats"],
        )
        assert result.exit_code == 0
        assert "No data" in result.output or "TOTAL" in result.output

    def test_generate_save_mongomock(self, runner: CliRunner):
        db = "fhir_cli_test_save"
        result = runner.invoke(
            cli,
            ["--seed", "7", "--db", db, "generate", "Patient", "-n", "2"],
        )
        assert result.exit_code == 0
        assert "Saved" in result.output or "Generated" in result.output

        stats = runner.invoke(cli, ["--db", db, "db-stats"])
        assert stats.exit_code == 0
        assert "Patient" in stats.output

    def test_search_patient(self, runner: CliRunner):
        db = "fhir_cli_test_search"
        runner.invoke(cli, ["--seed", "3", "--db", db, "generate", "Patient", "-n", "1"])
        result = runner.invoke(
            cli,
            ["--db", db, "search", "Patient", "--limit", "5"],
        )
        assert result.exit_code == 0
        data = _load_json_output(result.output)
        assert isinstance(data, list)

    def test_clear_with_yes(self, runner: CliRunner):
        db = "fhir_cli_test_clear"
        runner.invoke(cli, ["--seed", "1", "--db", db, "generate", "Patient", "-n", "1"])
        result = runner.invoke(
            cli,
            ["--db", db, "clear", "Patient", "--yes"],
        )
        assert result.exit_code == 0
        assert "Deleted" in result.output
