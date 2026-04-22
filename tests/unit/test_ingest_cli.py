from __future__ import annotations

import json

from typer.testing import CliRunner

import kehrnel.cli.ingest as ingest_cli


runner = CliRunner()


class _FakeDriver:
    def connect(self):
        return None

    def insert_many(self, docs, workers=4):
        list(docs)


def test_common_ingest_file_rejects_canonical_envelopes(tmp_path, monkeypatch):
    source = tmp_path / "canonical.ndjson"
    source.write_text(
        json.dumps(
            {
                "_id": "comp-1",
                "ehr_id": "ehr-1",
                "canonicalJSON": {
                    "_type": "COMPOSITION",
                    "archetype_details": {
                        "archetype_id": {"value": "openEHR-EHR-COMPOSITION.example.v1"},
                        "template_id": {"value": "example"},
                        "rm_version": "1.0.4",
                    },
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(ingest_cli, "get_driver", lambda cfg: _FakeDriver())

    result = runner.invoke(ingest_cli.app, ["file", str(source), "-d", str(tmp_path / "driver.yaml")])

    assert result.exit_code != 0
    rendered = (result.stdout or "") + (getattr(result, "stderr", "") or "") + (str(result.exception) if result.exception else "")
    assert "canonical composition envelopes" in rendered
    assert "kehrnel run ingest" in rendered
