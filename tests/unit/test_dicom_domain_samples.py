"""Source fidelity and immutable sample boundaries shared by DICOM consumers."""
import hashlib
import json
from datetime import datetime, timezone
import shutil

import pytest

from kehrnel.engine.domains.dicom import sample_data
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.dicom.tiered_store import playground
from kehrnel.engine.strategies.dicom.tiered_store.common import digest


DATASET = "dicom.synthetic-imaging"
VERSION = "1.0.0"
# Before moving source ownership, record each existing native demo's fingerprint.
BASELINE = {
    "mixed": "ea70e687feb1e5bdb6d1d66b56d0a2256d0604cb81e8a9318b3e6194ce06dc8a",
    "ct": "b44e545949fc15421d385176cdfc2d8bc8350d2219570ab72e586882e1c1705c",
    "mr": "0192d414540a0329f950928b18f41d51d7838c97e5a0ffb3c3fc808b26ff8094",
    "mg": "5486dde617602dd1ea892365918f69a595d7639cbda6e7a015c37271b775ced6",
    "legacy": "b509c1949858ea21e8ccf7301f1222852a5c4aef91b66c9a86a916bceaa258c5",
}


@pytest.fixture
def local_samples(tmp_path):
    root = tmp_path / "dicom-samples"
    shutil.copytree(sample_data.SAMPLE_ROOT, root)
    return root


def rewrite_manifest(root, update):
    path = root / "synthetic-imaging/1.0.0/manifest.json"
    manifest = json.loads(path.read_text())
    update(manifest)
    path.write_text(json.dumps(manifest))
    catalog_path = root / "catalog.json"
    catalog = json.loads(catalog_path.read_text())
    catalog["datasets"][0]["digest"] = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    catalog_path.write_text(json.dumps(catalog))


@pytest.mark.parametrize("variant", BASELINE)
def test_domain_sources_preserve_existing_demo_fingerprints(variant):
    source = sample_data.load_sample(DATASET, VERSION, variant=variant)
    assert digest(source) == BASELINE[variant]
    assert playground.demo_source(variant) == source


def test_bson_dates_and_dicom_numeric_string_values_are_preserved():
    source = sample_data.load_sample(DATASET, VERSION)
    assert source["studies"][0]["dt"] == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert source["instances"][0]["header"]["00180050"] == {"vr": "DS", "Value": ["0.5"]}
    assert source["instances"][0]["header"]["00200013"] == {"vr": "IS", "Value": ["1"]}
    assert "ContentIndex" not in sample_data.load_sample(DATASET, VERSION, variant="legacy")["instances"][0]


def test_independent_consumers_cannot_mutate_the_frozen_source():
    first = sample_data.load_sample(DATASET, VERSION)
    first["instances"][0]["header"]["00080070"]["Value"][0] = "changed"
    assert digest(sample_data.load_sample(DATASET, VERSION)) == BASELINE["mixed"]


def test_local_domain_directory_is_portable_without_a_strategy(local_samples):
    source = sample_data.load_sample(DATASET, VERSION, root=local_samples)
    assert digest(source) == BASELINE["mixed"]
    metadata = sample_data.describe_sample(DATASET, VERSION, root=local_samples)
    assert metadata["domain"] == "dicom" and metadata["kind"] == "synthetic"
    assert metadata["containsPixels"] is False


@pytest.mark.parametrize("kwargs", [{"dataset_id": "unknown"}, {"version": "2.0.0"}, {"variant": "unknown"}])
def test_unknown_dataset_version_or_variant_is_not_guessed(kwargs):
    with pytest.raises(sample_data.DICOMSampleError):
        sample_data.load_sample(**{"dataset_id": DATASET, "version": VERSION, **kwargs})


def test_a_consumer_pin_rejects_a_republished_sample_version(local_samples):
    rewrite_manifest(local_samples, lambda value: value.update({"new": "revision"}))
    with pytest.raises(sample_data.DICOMSampleError, match="pinned digest"):
        sample_data.load_sample(DATASET, VERSION, root=local_samples,
                                expected_digest=playground.DEMO_SAMPLE_REF["digest"])


def test_changed_header_bytes_are_rejected_before_processing(local_samples):
    path = local_samples / "synthetic-imaging/1.0.0/mixed/instances.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(sample_data.DICOMSampleError, match="digest"):
        sample_data.load_sample(DATASET, VERSION, root=local_samples)


def test_unchecked_source_assets_are_not_accepted(local_samples):
    rewrite_manifest(local_samples, lambda value: value["variants"]["mixed"]["assets"]["instances"].update({"sha256": None}))
    with pytest.raises(sample_data.DICOMSampleError, match="incomplete"):
        sample_data.load_sample(DATASET, VERSION, root=local_samples)


@pytest.mark.parametrize("path", ["../../outside.json", "/tmp/outside.json"])
def test_asset_paths_cannot_escape_the_dataset_directory(local_samples, path):
    rewrite_manifest(local_samples, lambda value: value["variants"]["mixed"]["assets"]["instances"].update({"path": path}))
    with pytest.raises(sample_data.DICOMSampleError, match="escapes"):
        sample_data.load_sample(DATASET, VERSION, root=local_samples)


def test_symlinks_cannot_escape_the_dataset_directory(local_samples, tmp_path):
    package = local_samples / "synthetic-imaging/1.0.0"
    outside = tmp_path / "outside.json"
    shutil.copyfile(package / "mixed/instances.json", outside)
    (package / "external.json").symlink_to(outside)
    rewrite_manifest(local_samples, lambda value: value["variants"]["mixed"]["assets"]["instances"].update({"path": "external.json"}))
    with pytest.raises(sample_data.DICOMSampleError, match="escapes"):
        sample_data.load_sample(DATASET, VERSION, root=local_samples)


def test_counts_and_combined_read_limits_are_enforced(local_samples, monkeypatch):
    metadata = sample_data.describe_sample(DATASET, VERSION, root=local_samples)
    size = sum(asset["bytes"] for asset in metadata["variants"]["mixed"]["assets"].values())
    monkeypatch.setattr(sample_data, "MAX_SOURCE_BYTES", size - 1)
    with pytest.raises(sample_data.DICOMSampleError, match="read limit"):
        sample_data.load_sample(DATASET, VERSION, root=local_samples)
    monkeypatch.setattr(sample_data, "MAX_SOURCE_BYTES", size)
    monkeypatch.setattr(sample_data, "MAX_DOCUMENTS", 1)
    with pytest.raises(sample_data.DICOMSampleError, match="document limit"):
        sample_data.load_sample(DATASET, VERSION, root=local_samples)


def test_source_counts_must_match_the_manifest(local_samples):
    rewrite_manifest(local_samples, lambda value: value["variants"]["mixed"]["counts"].update({"instances": 999}))
    with pytest.raises(sample_data.DICOMSampleError, match="counts"):
        sample_data.load_sample(DATASET, VERSION, root=local_samples)


def test_native_discovery_and_demo_return_the_domain_source_reference():
    ctx = StrategyContext(environment_id="sample-test", config={})
    catalog = playground.catalog(ctx, {})
    assert catalog["sampleDatasets"][0]["reference"] == {"domain": "dicom", **playground.DEMO_SAMPLE_REF}
    result = playground.demo(ctx, {"preset": "mixed"})
    assert result["sampleDataset"] == {"domain": "dicom", **playground.DEMO_SAMPLE_REF, "variant": "mixed"}
    assert result["preview"]["counts"]["uniqueSops"] == 12
