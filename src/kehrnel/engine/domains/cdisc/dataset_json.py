"""Dataset-JSON parsing and canonicalization.

The parser accepts an already-decoded Dataset-JSON document.  Artifact byte
retention belongs to the artifact adapter; this module handles the semantic
tabular representation only.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .models import (
    CanonicalDataset,
    CanonicalRecord,
    CanonicalVariable,
    CdiscProfile,
    RecordLineage,
    StandardReference,
)
from .projection import project_record


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return f"sha256:{hashlib.sha256(_canonical_json(value)).hexdigest()}"


class DatasetJsonColumn(BaseModel):
    item_oid: Optional[str] = Field(default=None, alias="itemOID")
    name: str
    label: Optional[str] = None
    data_type: str = Field(alias="dataType")
    target_data_type: Optional[str] = Field(default=None, alias="targetDataType")
    length: Optional[int] = None
    key_sequence: Optional[int] = Field(default=None, alias="keySequence")
    display_format: Optional[str] = Field(default=None, alias="displayFormat")
    codelist_oid: Optional[str] = Field(default=None, alias="codelistOID")
    origin: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        name = value.strip()
        if not name:
            raise ValueError("Dataset-JSON column name cannot be blank")
        return name


class DatasetJsonDocument(BaseModel):
    dataset_json_version: str = Field(alias="datasetJSONVersion")
    dataset_json_creation_datetime: Optional[str] = Field(default=None, alias="datasetJSONCreationDateTime")
    file_oid: Optional[str] = Field(default=None, alias="fileOID")
    study_oid: str = Field(alias="studyOID")
    metadata_version_oid: Optional[str] = Field(default=None, alias="metaDataVersionOID")
    metadata_ref: Optional[str] = Field(default=None, alias="metaDataRef")
    database_last_modified_datetime: Optional[str] = Field(default=None, alias="dbLastModifiedDateTime")
    originator: Optional[str] = None
    source_system: Optional[Dict[str, Any]] = Field(default=None, alias="sourceSystem")
    item_group_oid: Optional[str] = Field(default=None, alias="itemGroupOID")
    records: Optional[int] = Field(default=None, ge=0)
    name: str
    label: Optional[str] = None
    columns: List[DatasetJsonColumn]
    rows: List[List[Any]]

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    @field_validator("dataset_json_version")
    @classmethod
    def validate_supported_version(cls, value: str) -> str:
        if not re.fullmatch(r"1\.1(?:\.\d+)?", value.strip()):
            raise ValueError("the initial CDISC SDR implementation supports Dataset-JSON 1.1 only")
        return value.strip()

    @model_validator(mode="after")
    def validate_shape(self):
        if not self.columns:
            raise ValueError("Dataset-JSON columns cannot be empty")
        names = [column.name for column in self.columns]
        seen_names = set()
        duplicates = set()
        for name in names:
            if name in seen_names:
                duplicates.add(name)
            seen_names.add(name)
        if duplicates:
            raise ValueError(f"Dataset-JSON contains duplicate column names: {sorted(duplicates)}")
        width = len(self.columns)
        invalid_rows = [index + 1 for index, row in enumerate(self.rows) if len(row) != width]
        if invalid_rows:
            raise ValueError(
                f"Dataset-JSON row width does not match {width} columns at rows {invalid_rows[:10]}"
            )
        if self.records is not None and self.records != len(self.rows):
            raise ValueError(
                f"Dataset-JSON records={self.records} does not match rows={len(self.rows)}"
            )
        return self


def parse_dataset_json(payload: Dict[str, Any]) -> DatasetJsonDocument:
    if not isinstance(payload, dict):
        raise TypeError("Dataset-JSON payload must be an object")
    return DatasetJsonDocument.model_validate(payload)


def _dataset_source_metadata(document: DatasetJsonDocument) -> Dict[str, Any]:
    metadata = {
        "datasetJSONVersion": document.dataset_json_version,
        "datasetJSONCreationDateTime": document.dataset_json_creation_datetime,
        "fileOID": document.file_oid,
        "studyOID": document.study_oid,
        "metaDataVersionOID": document.metadata_version_oid,
        "metaDataRef": document.metadata_ref,
        "dbLastModifiedDateTime": document.database_last_modified_datetime,
        "originator": document.originator,
        "sourceSystem": document.source_system,
    }
    return {key: value for key, value in metadata.items() if value is not None}


def canonicalize_dataset_json(
    payload: Dict[str, Any],
    *,
    tenant_id: str,
    package_id: str,
    snapshot_id: str,
    profile: CdiscProfile | str,
    standard: StandardReference | Dict[str, Any],
    source_artifact_id: Optional[str] = None,
    publication_state: str = "staged",
) -> Tuple[CanonicalDataset, List[CanonicalRecord]]:
    document = parse_dataset_json(payload)
    resolved_profile = profile if isinstance(profile, CdiscProfile) else CdiscProfile(profile.lower())
    resolved_standard = (
        standard if isinstance(standard, StandardReference) else StandardReference.model_validate(standard)
    )
    study_id = document.study_oid
    domain = document.name.strip().upper()
    dataset_id = f"{tenant_id}:{study_id}:{snapshot_id}:{domain}"
    snapshot_ref = f"{tenant_id}:{study_id}:{snapshot_id}"

    ordered_key_columns = sorted(
        (column for column in document.columns if column.key_sequence is not None),
        key=lambda column: column.key_sequence or 0,
    )
    key_names = [column.name for column in ordered_key_columns]
    variables = [
        CanonicalVariable(
            ordinal=index,
            itemOID=column.item_oid,
            name=column.name,
            label=column.label,
            dataType=column.data_type,
            targetDataType=column.target_data_type,
            length=column.length,
            keySequence=column.key_sequence,
            displayFormat=column.display_format,
            codelistOID=column.codelist_oid,
            origin=column.origin,
        )
        for index, column in enumerate(document.columns, start=1)
    ]

    records: List[CanonicalRecord] = []
    record_hashes: List[str] = []
    column_names = [column.name for column in document.columns]
    for ordinal, row in enumerate(document.rows, start=1):
        data = dict(zip(column_names, row))
        facets, entity_refs = project_record(resolved_profile, domain, data)
        record_key = {name: data.get(name) for name in key_names}
        identity_key = record_key or {"__rowOrdinal": ordinal}
        record_hash = _sha256({"columns": column_names, "values": row})
        identity_hash = _sha256(
            {
                "tenantId": tenant_id,
                "studyId": study_id,
                "snapshotId": snapshot_id,
                "domain": domain,
                "key": identity_key,
            }
        )
        record_hashes.append(record_hash)
        records.append(
            CanonicalRecord(
                _id=identity_hash,
                tenantId=tenant_id,
                studyId=study_id,
                packageId=package_id,
                snapshotId=snapshot_id,
                snapshotRef=snapshot_ref,
                datasetId=dataset_id,
                profile=resolved_profile,
                standard=resolved_standard,
                domain=domain,
                publicationState=publication_state,
                rowOrdinal=ordinal,
                recordKey=record_key,
                entityRefs=entity_refs,
                facets=facets,
                data=data,
                lineage=RecordLineage(
                    sourceArtifactId=source_artifact_id,
                    sourceDataset=domain,
                    sourceRow=ordinal,
                    recordHash=record_hash,
                ),
            )
        )

    seen_ids = set()
    duplicate_ids = set()
    for record in records:
        if record.id in seen_ids:
            duplicate_ids.add(record.id)
        seen_ids.add(record.id)
    if duplicate_ids:
        raise ValueError(
            "Dataset-JSON contains duplicate declared record keys; "
            f"sample identities: {sorted(duplicate_ids)[:5]}"
        )

    content_hash = _sha256(
        {
            "variables": [variable.model_dump(by_alias=True, exclude_none=True) for variable in variables],
            "records": record_hashes,
        }
    )
    dataset = CanonicalDataset(
        _id=dataset_id,
        tenantId=tenant_id,
        studyId=study_id,
        packageId=package_id,
        snapshotId=snapshot_id,
        snapshotRef=snapshot_ref,
        profile=resolved_profile,
        standard=resolved_standard,
        domain=domain,
        name=document.name,
        label=document.label,
        itemGroupOID=document.item_group_oid,
        variables=variables,
        keyVariables=key_names,
        recordCount=len(records),
        contentHash=content_hash,
        sourceArtifactId=source_artifact_id,
        sourceMetadata=_dataset_source_metadata(document),
        publicationState=publication_state,
    )
    return dataset, records
