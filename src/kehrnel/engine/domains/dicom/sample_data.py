"""Read bounded, versioned DICOM source samples independently of a strategy.

Local callers may supply a domain sample directory. HTTP operations select
bundled dataset IDs; they must not pass request-controlled filesystem roots.
"""
from __future__ import annotations

import hashlib
from datetime import timezone
from pathlib import Path
import re

from bson import json_util


SAMPLE_ROOT = Path(__file__).with_name("samples")
MAX_SOURCE_BYTES = 12 * 1024 * 1024
MAX_DOCUMENTS = 500
_METADATA_BYTES = 256 * 1024
_JSON_OPTIONS = json_util.RELAXED_JSON_OPTIONS.with_options(
    tz_aware=True, tzinfo=timezone.utc,
)


class DICOMSampleError(ValueError):
    """A sample is unknown, damaged, incompatible or outside the read bounds."""


def _read(root: Path, relative: str, *, maximum: int, digest: str | None = None):
    if not isinstance(relative, str) or not relative:
        raise DICOMSampleError("Sample asset path must be a relative path.")
    rel = Path(relative)
    root = root.resolve()
    path = (root / rel).resolve()
    if rel.is_absolute() or ".." in rel.parts or not path.is_relative_to(root):
        raise DICOMSampleError("Sample asset path escapes its domain directory.")
    try:
        with path.open("rb") as source:
            data = source.read(maximum + 1)
    except OSError as exc:
        raise DICOMSampleError("Sample asset is unavailable.") from exc
    if len(data) > maximum:
        raise DICOMSampleError("Sample asset exceeds the read limit.")
    if digest is not None:
        if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise DICOMSampleError("Sample digest must be a SHA-256 reference.")
        if "sha256:" + hashlib.sha256(data).hexdigest() != digest:
            raise DICOMSampleError("Sample asset digest does not match its reference.")
    try:
        return json_util.loads(data.decode("utf-8"), json_options=_JSON_OPTIONS), len(data)
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        raise DICOMSampleError("Sample asset is not valid MongoDB Extended JSON.") from exc


def sample_catalog(*, root: Path | str = SAMPLE_ROOT) -> dict:
    """Return fresh source discovery metadata; this does not activate a strategy."""
    value, _ = _read(Path(root), "catalog.json", maximum=_METADATA_BYTES)
    if (not isinstance(value, dict)
            or value.get("schemaVersion") != "dicom-sample-catalog/v1"
            or value.get("domain") != "dicom"
            or not isinstance(value.get("datasets"), list)):
        raise DICOMSampleError("Unsupported DICOM sample catalog.")
    keys = set()
    for row in value["datasets"]:
        if not isinstance(row, dict) or not all(isinstance(row.get(k), str) for k in ("id", "version", "manifest", "digest")):
            raise DICOMSampleError("Incomplete DICOM sample reference.")
        key = (row["id"], row["version"])
        if key in keys:
            raise DICOMSampleError("Duplicate DICOM sample version.")
        keys.add(key)
    return value


def _manifest(dataset_id, version, root, expected_digest):
    root = Path(root)
    row = next((row for row in sample_catalog(root=root)["datasets"]
                if row["id"] == dataset_id and row["version"] == version), None)
    if row is None:
        raise DICOMSampleError("Unknown DICOM sample dataset/version.")
    if expected_digest is not None and row["digest"] != expected_digest:
        raise DICOMSampleError("DICOM sample version differs from the consumer's pinned digest.")
    value, _ = _read(root, row["manifest"], maximum=_METADATA_BYTES, digest=row["digest"])
    if (not isinstance(value, dict)
            or value.get("schemaVersion") != "dicom-source-samples/v1"
            or value.get("domain") != "dicom"
            or value.get("id") != dataset_id or value.get("version") != version
            or value.get("encoding") != "mongodb-extended-json/v2"
            or not isinstance(value.get("variants"), dict)):
        raise DICOMSampleError("Unsupported or inconsistent DICOM sample manifest.")
    return value, row, (root / row["manifest"]).parent


def describe_sample(dataset_id: str, version: str, *, root=SAMPLE_ROOT,
                    expected_digest: str | None = None) -> dict:
    """Discover provenance and immutable asset references without loading headers."""
    value, row, _ = _manifest(dataset_id, version, root, expected_digest)
    return {**value, "reference": {
        "domain": "dicom", "id": dataset_id, "version": version, "digest": row["digest"],
    }}


def load_sample(dataset_id: str, version: str, *, variant: str = "mixed",
                root=SAMPLE_ROOT, expected_digest: str | None = None) -> dict:
    """Load a fresh study/instance source batch, preserving BSON and DICOM VRs."""
    manifest, _, directory = _manifest(dataset_id, version, root, expected_digest)
    selected = manifest["variants"].get(variant)
    if not isinstance(selected, dict) or not isinstance(selected.get("assets"), dict):
        raise DICOMSampleError("Unknown or incomplete DICOM sample variant.")
    counts = selected.get("counts")
    if not isinstance(counts, dict):
        raise DICOMSampleError("Sample source counts are missing.")
    batch, total_bytes, total_documents = {}, 0, 0
    for name in ("studies", "instances"):
        asset = selected["assets"].get(name)
        if (not isinstance(asset, dict)
                or not isinstance(asset.get("path"), str)
                or not isinstance(asset.get("sha256"), str)
                or not isinstance(asset.get("bytes"), int) or asset["bytes"] < 0):
            raise DICOMSampleError("Sample source asset reference is incomplete.")
        values, size = _read(directory, asset["path"], maximum=MAX_SOURCE_BYTES - total_bytes,
                             digest=asset["sha256"])
        if (not isinstance(values, list) or not all(isinstance(row, dict) for row in values)
                or len(values) != counts.get(name) or size != asset["bytes"]):
            raise DICOMSampleError("Sample source shape/counts differ from its manifest.")
        total_bytes += size
        total_documents += len(values)
        if total_documents > MAX_DOCUMENTS:
            raise DICOMSampleError("Sample source exceeds the document limit.")
        batch[name] = values
    return batch
