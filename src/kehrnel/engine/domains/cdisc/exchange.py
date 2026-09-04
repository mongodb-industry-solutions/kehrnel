"""Dataset-JSON reconstruction and semantic-equivalence helpers."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Literal

from pydantic import BaseModel, ConfigDict, Field

from .dataset_json import canonicalize_dataset_json
from .models import CanonicalDataset, CanonicalRecord


class EquivalenceMismatch(BaseModel):
    path: str
    expected: Any = None
    actual: Any = None


class SemanticEquivalenceReport(BaseModel):
    source_dataset_id: str = Field(alias="sourceDatasetId")
    guarantee: Literal["semantic"] = "semantic"
    equivalent: bool
    expected_content_hash: str = Field(alias="expectedContentHash")
    actual_content_hash: str | None = Field(default=None, alias="actualContentHash")
    expected_record_count: int = Field(alias="expectedRecordCount")
    actual_record_count: int | None = Field(default=None, alias="actualRecordCount")
    mismatches: List[EquivalenceMismatch] = Field(default_factory=list)
    validation_error: str | None = Field(default=None, alias="validationError")

    model_config = ConfigDict(populate_by_name=True)


def _dataset(value: CanonicalDataset | Dict[str, Any]) -> CanonicalDataset:
    return value if isinstance(value, CanonicalDataset) else CanonicalDataset.model_validate(value)


def _records(values: Iterable[CanonicalRecord | Dict[str, Any]]) -> List[CanonicalRecord]:
    return [value if isinstance(value, CanonicalRecord) else CanonicalRecord.model_validate(value) for value in values]


def export_dataset_json(
    dataset: CanonicalDataset | Dict[str, Any],
    records: Iterable[CanonicalRecord | Dict[str, Any]],
) -> Dict[str, Any]:
    """Reconstruct Dataset-JSON 1.1 without claiming byte-for-byte replay."""

    canonical_dataset = _dataset(dataset)
    canonical_records = sorted(_records(records), key=lambda record: record.row_ordinal)
    foreign_records = sorted(
        {
            record.dataset_id
            for record in canonical_records
            if record.dataset_id != canonical_dataset.id
        }
    )
    if foreign_records:
        raise ValueError(f"records belong to other datasets: {foreign_records[:5]}")
    ordinals = [record.row_ordinal for record in canonical_records]
    expected_ordinals = list(range(1, len(canonical_records) + 1))
    if ordinals != expected_ordinals:
        raise ValueError("record rowOrdinal values must be contiguous and start at 1")

    variables = sorted(canonical_dataset.variables, key=lambda variable: variable.ordinal)
    columns = [
        variable.model_dump(
            by_alias=True,
            mode="json",
            exclude_none=True,
            exclude={"ordinal"},
        )
        for variable in variables
    ]
    column_names = [variable.name for variable in variables]
    rows = [[record.data.get(name) for name in column_names] for record in canonical_records]
    source = canonical_dataset.source_metadata
    document: Dict[str, Any] = {
        "datasetJSONVersion": str(source.get("datasetJSONVersion") or "1.1.0"),
        "studyOID": canonical_dataset.study_id,
        "records": len(rows),
        "name": canonical_dataset.name,
        "columns": columns,
        "rows": rows,
    }
    optional = {
        "datasetJSONCreationDateTime": source.get("datasetJSONCreationDateTime"),
        "fileOID": source.get("fileOID"),
        "metaDataVersionOID": source.get("metaDataVersionOID"),
        "metaDataRef": source.get("metaDataRef"),
        "dbLastModifiedDateTime": source.get("dbLastModifiedDateTime"),
        "originator": source.get("originator"),
        "sourceSystem": source.get("sourceSystem"),
        "itemGroupOID": canonical_dataset.item_group_oid,
        "label": canonical_dataset.label,
    }
    document.update({key: value for key, value in optional.items() if value is not None})
    return document


def encode_dataset_json(document: Dict[str, Any]) -> bytes:
    """Encode deterministic UTF-8 Dataset-JSON bytes for generated artifacts."""

    return (
        json.dumps(document, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def compare_export_to_canonical(
    dataset: CanonicalDataset | Dict[str, Any],
    exported: Dict[str, Any],
) -> SemanticEquivalenceReport:
    """Recanonicalize an export and compare its semantic content digest."""

    canonical_dataset = _dataset(dataset)
    try:
        reconstructed, _ = canonicalize_dataset_json(
            exported,
            tenant_id=canonical_dataset.tenant_id,
            package_id=canonical_dataset.package_id,
            snapshot_id=canonical_dataset.snapshot_id,
            profile=canonical_dataset.profile,
            standard=canonical_dataset.standard,
            source_artifact_id=canonical_dataset.source_artifact_id,
            publication_state=canonical_dataset.publication_state,
        )
    except Exception as exc:
        return SemanticEquivalenceReport(
            sourceDatasetId=canonical_dataset.id,
            equivalent=False,
            expectedContentHash=canonical_dataset.content_hash,
            expectedRecordCount=canonical_dataset.record_count,
            validationError=str(exc),
        )

    mismatches: List[EquivalenceMismatch] = []
    comparisons = {
        "contentHash": (canonical_dataset.content_hash, reconstructed.content_hash),
        "recordCount": (canonical_dataset.record_count, reconstructed.record_count),
        "studyId": (canonical_dataset.study_id, reconstructed.study_id),
        "domain": (canonical_dataset.domain, reconstructed.domain),
        "keyVariables": (canonical_dataset.key_variables, reconstructed.key_variables),
    }
    for path, (expected, actual) in comparisons.items():
        if expected != actual:
            mismatches.append(EquivalenceMismatch(path=path, expected=expected, actual=actual))
    return SemanticEquivalenceReport(
        sourceDatasetId=canonical_dataset.id,
        equivalent=not mismatches,
        expectedContentHash=canonical_dataset.content_hash,
        actualContentHash=reconstructed.content_hash,
        expectedRecordCount=canonical_dataset.record_count,
        actualRecordCount=reconstructed.record_count,
        mismatches=mismatches,
    )
