from __future__ import annotations

import json

from typer.testing import CliRunner

import kehrnel.cli.unified as unified


runner = CliRunner()


def test_root_command_without_args_shows_help_and_local_guide():
    result = runner.invoke(unified.app, [])

    assert result.exit_code == 0
    assert "Unified kehrnel CLI" in result.stdout
    assert "Local guide" in result.stdout
    assert "http://localhost:8080/guide" in result.stdout
    assert "Missing command" not in result.stdout


def test_extract_database_name_prefers_activation_record():
    data = {
        "response": {
            "activations": {
                "openehr": {
                    "strategy_id": "openehr.rps_dual",
                    "bindings_meta": {
                        "db": {
                            "provider": "mongodb",
                            "database": "openEHR_demo2",
                        }
                    },
                }
            }
        }
    }

    assert unified._extract_database_name(data, domain="openehr", strategy_id="openehr.rps_dual") == "openEHR_demo2"


def test_build_summary_rows_for_query_reports_engine_scope_collection_and_rows():
    payload = {
        "ok": True,
        "result": {
            "engine_used": "text_search_dual",
            "rows": [{"ehrId": "ehr-1"}, {"ehrId": "ehr-2"}],
            "explain": {
                "engine": "text_search_dual",
                "scope": "cross_patient",
                "collection": "compositions_search",
                "warnings": [],
                "timings": {
                    "kehrnel_total_ms": 18.4,
                    "kehrnel_compile_ms": 5.25,
                    "kehrnel_execute_ms": 11.8,
                    "kehrnel_db_ms": 8.1,
                },
            },
        },
    }

    summary = dict(unified._build_summary_rows("query", payload))

    assert summary["engine"] == "text_search_dual"
    assert summary["scope"] == "cross_patient"
    assert summary["collection"] == "compositions_search"
    assert summary["rows"] == "2"
    assert summary["warnings"] == "0"
    assert summary["total time"] == "18.40 ms"
    assert summary["compile time"] == "5.25 ms"
    assert summary["execute time"] == "11.80 ms"
    assert summary["db time"] == "8.10 ms"


def test_build_summary_rows_for_compile_query_reports_compile_time():
    payload = {
        "ok": True,
        "result": {
            "engine": "mongo_pipeline",
            "plan": {
                "collection": "ibm-semiflattened-compositions",
                "scope": "cross_patient",
                "explain": {
                    "stage0": "$match",
                    "warnings": [],
                },
                "meta": {
                    "timings": {
                        "kehrnel_compile_ms": 27.0,
                    }
                },
            },
        },
    }

    summary = dict(unified._build_summary_rows("compile-query", payload))

    assert summary["engine"] == "mongo_pipeline"
    assert summary["scope"] == "cross_patient"
    assert summary["collection"] == "ibm-semiflattened-compositions"
    assert summary["stage0"] == "$match"
    assert summary["compile time"] == "27 ms"


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


def test_strategy_build_search_index_writes_nested_definition(monkeypatch, tmp_path):
    out_path = tmp_path / "search-index.json"

    def fake_http_json(method, url, api_key=None, payload=None):
        return 200, {
            "ok": True,
            "operation": "build_search_index_definition",
            "response": {
                "ok": True,
                "result": {
                    "ok": True,
                    "definition": {
                        "mappings": {
                            "dynamic": False,
                            "fields": {
                                "sn": {"type": "embeddedDocuments"},
                            },
                        }
                    },
                    "warnings": [],
                },
            },
        }

    monkeypatch.setattr(unified, "_http_json", fake_http_json)
    monkeypatch.setattr(unified, "_state", lambda: {"context": {}, "auth": {}, "resources": {}})
    monkeypatch.setattr(unified, "_save", lambda state: None)

    result = runner.invoke(
        unified.app,
        [
            "strategy",
            "build-search-index",
            "--runtime-url",
            "http://localhost:8000",
            "--env",
            "dev",
            "--domain",
            "openehr",
            "--strategy",
            "openehr.rps_dual",
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["mappings"]["fields"]["sn"]["type"] == "embeddedDocuments"


def test_strategy_build_search_index_writes_run_result_definition(monkeypatch, tmp_path):
    out_path = tmp_path / "search-index.json"

    def fake_http_json(method, url, api_key=None, payload=None):
        return 200, {
            "ok": True,
            "env_id": "dev",
            "operation": "build_search_index_definition",
            "result": {
                "ok": True,
                "definition": {
                    "mappings": {
                        "dynamic": False,
                        "fields": {
                            "sort_time": {"type": "date"},
                        },
                    }
                },
                "warnings": [],
            },
        }

    monkeypatch.setattr(unified, "_http_json", fake_http_json)
    monkeypatch.setattr(unified, "_state", lambda: {"context": {}, "auth": {}, "resources": {}})
    monkeypatch.setattr(unified, "_save", lambda state: None)

    result = runner.invoke(
        unified.app,
        [
            "strategy",
            "build-search-index",
            "--runtime-url",
            "http://localhost:8000",
            "--env",
            "dev",
            "--domain",
            "openehr",
            "--strategy",
            "openehr.rps_dual",
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["mappings"]["fields"]["sort_time"]["type"] == "date"
