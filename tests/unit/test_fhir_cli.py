from __future__ import annotations

import json

from typer.testing import CliRunner

import kehrnel.cli.unified as unified


runner = CliRunner()


def test_fhir_help_exposes_accelerator_workflows():
    result = runner.invoke(unified.app, ["fhir", "--help"])

    assert result.exit_code == 0
    assert "stage-ig" in result.stdout
    assert "import-data" in result.stdout
    assert "support-matrix" in result.stdout
    assert "explain" in result.stdout


def test_fhir_capabilities_scopes_request_to_environment(monkeypatch):
    captured = {}

    def fake_http_json(method, url, api_key=None, payload=None):
        captured.update(
            {"method": method, "url": url, "api_key": api_key, "payload": payload}
        )
        return 200, {"fhir_version": "R5"}

    monkeypatch.setattr(unified, "_http_json", fake_http_json)

    result = runner.invoke(
        unified.app,
        [
            "fhir",
            "capabilities",
            "--resource-type",
            "Patient",
            "--env",
            "tenant-a",
            "--runtime-url",
            "http://localhost:8080",
            "--api-key",
            "secret",
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "method": "GET",
        "url": "http://localhost:8080/api/domains/fhir/capabilities?resource_type=Patient&env_id=tenant-a",
        "api_key": "secret",
        "payload": None,
    }


def test_fhir_search_sends_the_canonical_search_expression(monkeypatch):
    captured = {}

    def fake_http_json(method, url, api_key=None, payload=None):
        captured.update({"method": method, "url": url, "payload": payload})
        return 200, {"resourceType": "Bundle", "type": "searchset"}

    monkeypatch.setattr(unified, "_http_json", fake_http_json)

    result = runner.invoke(
        unified.app,
        [
            "fhir",
            "search",
            "Observation?status=final&date=ge2025-01-01",
            "--count",
            "50",
            "--env",
            "tenant-a",
            "--runtime-url",
            "http://localhost:8080",
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "method": "POST",
        "url": "http://localhost:8080/api/domains/fhir/search?env_id=tenant-a",
        "payload": {
            "fhir_search": "Observation?status=final&date=ge2025-01-01",
            "limit": 50,
            "offset": 0,
        },
    }


def test_fhir_stage_ig_preserves_archive_bytes_and_filename(monkeypatch, tmp_path):
    package = tmp_path / "acme.fhir.r5.tgz"
    package.write_bytes(b"package-bytes")
    captured = {}

    def fake_http_raw(method, url, api_key=None, **kwargs):
        captured.update({"method": method, "url": url, "api_key": api_key, **kwargs})
        return 201, json.dumps({"staged_id": "sha256", "activated": False}).encode(), "application/json"

    monkeypatch.setattr(unified, "_http_raw", fake_http_raw)

    result = runner.invoke(
        unified.app,
        [
            "fhir",
            "stage-ig",
            str(package),
            "--env",
            "tenant-a",
            "--release",
            "R5",
            "--runtime-url",
            "http://localhost:8080",
        ],
    )

    assert result.exit_code == 0
    assert captured["body"] == b"package-bytes"
    assert captured["headers"] == {"X-FHIR-Package-Filename": package.name}
    assert captured["url"].endswith(
        "/api/domains/fhir/implementation-guides/stage?env_id=tenant-a&fhir_release=R5"
    )


def test_fhir_import_is_dry_run_until_execute_is_explicit(monkeypatch, tmp_path):
    source = tmp_path / "patients.ndjson"
    source.write_text('{"resourceType":"Patient","id":"p1"}\n', encoding="utf-8")
    captured = {}

    def fake_http_raw(method, url, api_key=None, **kwargs):
        captured.update({"method": method, "url": url, **kwargs})
        return 200, b'{"ok":true,"committed":false}', "application/json"

    monkeypatch.setattr(unified, "_http_raw", fake_http_raw)

    result = runner.invoke(
        unified.app,
        [
            "fhir",
            "import-data",
            str(source),
            "--env",
            "tenant-a",
            "--runtime-url",
            "http://localhost:8080",
        ],
    )

    assert result.exit_code == 0
    assert "dry_run=true" in captured["url"]
    assert captured["content_type"] == "application/fhir+ndjson"
    assert captured["body"] == source.read_bytes()
