import copy

from kehrnel.engine.domains.cdisc.dataset_json import canonicalize_dataset_json
from kehrnel.engine.domains.cdisc.exchange import compare_export_to_canonical, export_dataset_json
from kehrnel.engine.domains.cdisc.xpt import _read_xport, _resolve_xpt_domain
from tests.unit.test_cdisc_dataset_json import _payload


def _canonical():
    return canonicalize_dataset_json(
        _payload(),
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


def test_dataset_json_export_is_semantically_equivalent():
    source = _payload()
    source["datasetJSONCreationDateTime"] = "2026-08-21T12:00:00Z"
    source["sourceSystem"] = {"name": "test-edc", "version": "1.0"}
    source["columns"][0]["targetDataType"] = "string"
    dataset, records = canonicalize_dataset_json(
        source,
        tenant_id="tenant-a",
        package_id="package-1",
        snapshot_id="snapshot-1",
        profile="sdtm",
        standard={"family": "SDTM"},
        publication_state="published",
    )

    exported = export_dataset_json(dataset, reversed(records))
    report = compare_export_to_canonical(dataset, exported)

    assert exported["rows"][0][1] == "STUDY-001-001"
    assert exported["records"] == dataset.record_count
    assert exported["datasetJSONCreationDateTime"] == "2026-08-21T12:00:00Z"
    assert exported["sourceSystem"] == {"name": "test-edc", "version": "1.0"}
    assert exported["columns"][0]["targetDataType"] == "string"
    assert report.equivalent is True
    assert report.actual_content_hash == dataset.content_hash
    assert report.guarantee == "semantic"


def test_equivalence_report_identifies_a_changed_value():
    dataset, records = _canonical()
    exported = export_dataset_json(dataset, records)
    changed = copy.deepcopy(exported)
    changed["rows"][0][-1] = "Changed treatment"

    report = compare_export_to_canonical(dataset, changed)

    assert report.equivalent is False
    assert {mismatch.path for mismatch in report.mismatches} == {"contentHash"}


def test_export_rejects_non_contiguous_record_ordinals():
    dataset, records = _canonical()
    records[1].row_ordinal = 3

    try:
        export_dataset_json(dataset, records)
    except ValueError as exc:
        assert "contiguous" in str(exc)
    else:
        raise AssertionError("non-contiguous ordinals should fail")


def test_xpt_reader_retries_legacy_windows_1252_text():
    class Reader:
        def __init__(self):
            self.calls = []

        def read_xport(self, path, **options):
            self.calls.append((path, options))
            if "encoding" not in options:
                raise UnicodeDecodeError("utf-8", b"\x92", 0, 1, "invalid start byte")
            return {"DOMAIN": ["MI"]}, object()

    reader = Reader()
    values, _ = _read_xport(reader, "/tmp/legacy.xpt")

    assert values == {"DOMAIN": ["MI"]}
    assert reader.calls == [
        ("/tmp/legacy.xpt", {"output_format": "dict"}),
        ("/tmp/legacy.xpt", {"output_format": "dict", "encoding": "WINDOWS-1252"}),
    ]


def test_xpt_domain_restores_suppqual_identity_from_rdomain():
    assert _resolve_xpt_domain("SUPP", {"RDOMAIN": ["MA", "MA"]}) == "SUPPMA"
    assert _resolve_xpt_domain("SUPP", {"RDOMAIN": ["MI", "MI"]}) == "SUPPMI"
    assert _resolve_xpt_domain("SUPP", {"RDOMAIN": ["MI", "MA"]}) == "SUPP"
    assert _resolve_xpt_domain("", {"DOMAIN": ["LB", "LB"]}) == "LB"
