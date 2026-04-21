from __future__ import annotations

import json

from typer.testing import CliRunner

import kehrnel.cli.unified as unified


runner = CliRunner()


def test_run_ingest_expands_local_ndjson_file_into_documents(monkeypatch, tmp_path):
    ndjson_path = tmp_path / "sample.ndjson"
    ndjson_path.write_text(
        "\n".join(
            [
                json.dumps({"_id": "comp-1", "ehr_id": "ehr-1", "canonicalJSON": {"_type": "COMPOSITION"}}),
                json.dumps({"_id": "comp-2", "ehr_id": "ehr-2", "canonicalJSON": {"_type": "COMPOSITION"}}),
            ]
        ),
        encoding="utf-8",
    )

    captured: dict = {}

    def fake_http_json(method, url, api_key=None, payload=None):
        captured["method"] = method
        captured["url"] = url
        captured["payload"] = payload
        return 200, {"ok": True}

    monkeypatch.setattr(unified, "_http_json", fake_http_json)
    monkeypatch.setattr(unified, "_state", lambda: {"context": {}, "auth": {}, "resources": {}})
    monkeypatch.setattr(unified, "_save", lambda state: None)

    result = runner.invoke(
        unified.app,
        [
            "run",
            "ingest",
            "--runtime-url",
            "http://localhost:8000",
            "--env",
            "dev",
            "--domain",
            "openehr",
            "--strategy",
            "openehr.rps_dual",
            "--set",
            f"file_path={ndjson_path}",
        ],
    )

    assert result.exit_code == 0
    assert captured["method"] == "POST"
    assert captured["url"] == "http://localhost:8000/environments/dev/run"
    assert "file_path" not in captured["payload"]["payload"]
    assert len(captured["payload"]["payload"]["documents"]) == 2
    assert captured["payload"]["payload"]["documents"][0]["_id"] == "comp-1"


def test_run_ingest_keeps_file_path_when_not_local(monkeypatch):
    captured: dict = {}

    def fake_http_json(method, url, api_key=None, payload=None):
        captured["payload"] = payload
        return 200, {"ok": True}

    monkeypatch.setattr(unified, "_http_json", fake_http_json)
    monkeypatch.setattr(unified, "_state", lambda: {"context": {}, "auth": {}, "resources": {}})
    monkeypatch.setattr(unified, "_save", lambda state: None)

    result = runner.invoke(
        unified.app,
        [
            "run",
            "ingest",
            "--runtime-url",
            "http://localhost:8000",
            "--env",
            "dev",
            "--domain",
            "openehr",
            "--strategy",
            "openehr.rps_dual",
            "--set",
            "file_path=/path/that/only/the/server/can/see.ndjson",
        ],
    )

    assert result.exit_code == 0
    assert captured["payload"]["payload"]["file_path"] == "/path/that/only/the/server/can/see.ndjson"
    assert "documents" not in captured["payload"]["payload"]
