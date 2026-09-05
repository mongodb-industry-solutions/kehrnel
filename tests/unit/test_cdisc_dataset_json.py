import json
from pathlib import Path

import pytest

from kehrnel.engine.domains.cdisc.dataset_json import canonicalize_dataset_json, parse_dataset_json


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "cdisc" / "dm.dataset.json"


def _payload():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _canonicalize(payload=None):
    return canonicalize_dataset_json(
        payload or _payload(),
        tenant_id="tenant-a",
        package_id="package-1",
        snapshot_id="snapshot-1",
        profile="sdtm",
        standard={
            "family": "SDTM",
            "implementationGuide": "SDTMIG",
            "implementationGuideVersion": "pinned-test-version",
            "exchangeStandard": "Dataset-JSON",
            "exchangeVersion": "1.1.0",
        },
        source_artifact_id="artifact-1",
        publication_state="published",
    )


def test_dataset_json_canonicalization_preserves_data_and_derives_facets():
    dataset, records = _canonicalize()

    assert dataset.domain == "DM"
    assert dataset.record_count == 2
    assert dataset.key_variables == ["STUDYID", "USUBJID"]
    assert dataset.snapshot_ref == "tenant-a:STUDY-001:snapshot-1"
    assert records[0].data["USUBJID"] == "STUDY-001-001"
    assert records[0].record_key == {"STUDYID": "STUDY-001", "USUBJID": "STUDY-001-001"}
    assert records[0].facets["subjectId"] == "STUDY-001-001"
    assert records[0].facets["sex"] == "F"
    assert records[0].entity_refs[0].type == "humanSubject"
    assert records[0].lineage.source_artifact_id == "artifact-1"


def test_dataset_json_hashes_and_identities_are_deterministic():
    first_dataset, first_records = _canonicalize()
    second_dataset, second_records = _canonicalize()

    assert first_dataset.content_hash == second_dataset.content_hash
    assert [record.id for record in first_records] == [record.id for record in second_records]
    assert [record.lineage.record_hash for record in first_records] == [
        record.lineage.record_hash for record in second_records
    ]


def test_dataset_json_rejects_row_width_and_record_count_mismatches():
    payload = _payload()
    payload["rows"][0] = payload["rows"][0][:-1]
    with pytest.raises(ValueError, match="row width"):
        parse_dataset_json(payload)

    payload = _payload()
    payload["records"] = 99
    with pytest.raises(ValueError, match="does not match"):
        parse_dataset_json(payload)


def test_dataset_json_preserves_and_reports_duplicate_declared_keys():
    payload = _payload()
    payload["rows"][1][1] = payload["rows"][0][1]
    dataset, records = _canonicalize(payload)

    assert len(records) == len(payload["rows"])
    assert len({record.id for record in records}) == len(records)
    assert records[0].record_key == records[1].record_key
    assert dataset.source_metadata["recordIdentity"] == {
        "strategy": "declared-key-plus-source-row-ordinal",
        "duplicateDeclaredKeys": 1,
        "affectedRecords": 2,
    }


def test_dataset_json_rejects_a_different_major_minor_version():
    payload = _payload()
    payload["datasetJSONVersion"] = "1.10.0"

    with pytest.raises(ValueError, match="supports Dataset-JSON 1.1 only"):
        parse_dataset_json(payload)
