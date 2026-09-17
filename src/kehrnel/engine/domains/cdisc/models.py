"""Canonical contracts for CDISC study snapshots.

These models deliberately preserve CDISC variable names and values.  Semantic
facets and entity references are derived projections, not replacements for the
source record.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class CdiscProfile(str, Enum):
    SEND = "send"
    SDTM = "sdtm"
    ADAM = "adam"
    TIG = "tig"


class Digest(BaseModel):
    algorithm: Literal["sha256"] = "sha256"
    value: str

    @field_validator("value")
    @classmethod
    def validate_hex_digest(cls, value: str) -> str:
        normalized = value.removeprefix("sha256:").lower()
        if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
            raise ValueError("sha256 digest must contain 64 hexadecimal characters")
        return normalized


class ArtifactReference(BaseModel):
    artifact_id: str = Field(alias="artifactId")
    provider: str
    uri: str
    media_type: str = Field(alias="mediaType")
    size: Optional[int] = Field(default=None, ge=0)
    digest: Digest
    acquired_at: datetime = Field(default_factory=utc_now, alias="acquiredAt")
    source_name: Optional[str] = Field(default=None, alias="sourceName")
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)


class StandardReference(BaseModel):
    family: str
    model_version: Optional[str] = Field(default=None, alias="modelVersion")
    implementation_guide: Optional[str] = Field(default=None, alias="implementationGuide")
    implementation_guide_version: Optional[str] = Field(
        default=None,
        alias="implementationGuideVersion",
    )
    terminology_packages: List[str] = Field(default_factory=list, alias="terminologyPackages")
    exchange_standard: Optional[str] = Field(default=None, alias="exchangeStandard")
    exchange_version: Optional[str] = Field(default=None, alias="exchangeVersion")
    regulator_context: Optional[str] = Field(default=None, alias="regulatorContext")

    model_config = ConfigDict(populate_by_name=True)


class StandardsAsset(BaseModel):
    asset_id: str = Field(alias="assetId")
    kind: str
    version: str
    source_uri: Optional[str] = Field(default=None, alias="sourceUri")
    artifact_id: Optional[str] = Field(default=None, alias="artifactId")
    digest: Optional[Digest] = None
    license_class: Optional[str] = Field(default=None, alias="licenseClass")
    redistribution: Literal["allowed", "fetch_at_setup", "customer_supplied", "unknown"] = "unknown"

    model_config = ConfigDict(populate_by_name=True)


class StandardsPackage(BaseModel):
    package_id: str = Field(alias="packageId")
    profile: CdiscProfile
    standard: StandardReference
    assets: List[StandardsAsset] = Field(default_factory=list)
    validation_engines: Dict[str, str] = Field(default_factory=dict, alias="validationEngines")
    rule_packages: List[str] = Field(default_factory=list, alias="rulePackages")
    created_at: datetime = Field(default_factory=utc_now, alias="createdAt")

    model_config = ConfigDict(populate_by_name=True)


class CanonicalVariable(BaseModel):
    ordinal: int = Field(ge=1)
    item_oid: Optional[str] = Field(default=None, alias="itemOID")
    name: str
    label: Optional[str] = None
    data_type: str = Field(alias="dataType")
    target_data_type: Optional[str] = Field(default=None, alias="targetDataType")
    length: Optional[int] = Field(default=None, ge=0)
    key_sequence: Optional[int] = Field(default=None, ge=1, alias="keySequence")
    display_format: Optional[str] = Field(default=None, alias="displayFormat")
    codelist_oid: Optional[str] = Field(default=None, alias="codelistOID")
    origin: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("name")
    @classmethod
    def validate_variable_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("variable name cannot be blank")
        return normalized


class EntityReference(BaseModel):
    type: str
    id: str


class RecordLineage(BaseModel):
    source_artifact_id: Optional[str] = Field(default=None, alias="sourceArtifactId")
    source_dataset: str = Field(alias="sourceDataset")
    source_row: int = Field(ge=1, alias="sourceRow")
    mapping_id: Optional[str] = Field(default=None, alias="mappingId")
    mapping_version: Optional[str] = Field(default=None, alias="mappingVersion")
    record_hash: str = Field(alias="recordHash")

    model_config = ConfigDict(populate_by_name=True)


class CanonicalRecord(BaseModel):
    id: str = Field(alias="_id")
    tenant_id: str = Field(alias="tenantId")
    study_id: str = Field(alias="studyId")
    package_id: str = Field(alias="packageId")
    snapshot_id: str = Field(alias="snapshotId")
    snapshot_ref: str = Field(alias="snapshotRef")
    dataset_id: str = Field(alias="datasetId")
    profile: CdiscProfile
    standard: StandardReference
    domain: str
    publication_state: Literal["staged", "published", "quarantined", "superseded"] = Field(
        default="staged",
        alias="publicationState",
    )
    row_ordinal: int = Field(ge=1, alias="rowOrdinal")
    record_key: Dict[str, Any] = Field(default_factory=dict, alias="recordKey")
    entity_refs: List[EntityReference] = Field(default_factory=list, alias="entityRefs")
    facets: Dict[str, Any] = Field(default_factory=dict)
    data: Dict[str, Any]
    lineage: RecordLineage

    model_config = ConfigDict(populate_by_name=True)


class CanonicalDataset(BaseModel):
    id: str = Field(alias="_id")
    tenant_id: str = Field(alias="tenantId")
    study_id: str = Field(alias="studyId")
    package_id: str = Field(alias="packageId")
    snapshot_id: str = Field(alias="snapshotId")
    snapshot_ref: str = Field(alias="snapshotRef")
    profile: CdiscProfile
    standard: StandardReference
    domain: str
    name: str
    label: Optional[str] = None
    item_group_oid: Optional[str] = Field(default=None, alias="itemGroupOID")
    variables: List[CanonicalVariable]
    key_variables: List[str] = Field(default_factory=list, alias="keyVariables")
    record_count: int = Field(ge=0, alias="recordCount")
    content_hash: str = Field(alias="contentHash")
    source_artifact_id: Optional[str] = Field(default=None, alias="sourceArtifactId")
    source_metadata: Dict[str, Any] = Field(default_factory=dict, alias="sourceMetadata")
    publication_state: Literal["staged", "published", "quarantined", "superseded"] = Field(
        default="staged",
        alias="publicationState",
    )

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def key_variables_must_exist(self):
        available = {variable.name for variable in self.variables}
        missing = [name for name in self.key_variables if name not in available]
        if missing:
            raise ValueError(f"key variables are not declared as columns: {missing}")
        return self


class CanonicalSnapshot(BaseModel):
    snapshot_id: str = Field(alias="snapshotId")
    tenant_id: str = Field(alias="tenantId")
    study_id: str = Field(alias="studyId")
    package_id: str = Field(alias="packageId")
    profile: CdiscProfile
    standards_package_id: str = Field(alias="standardsPackageId")
    state: Literal["received", "parsed", "canonicalized", "validated", "published", "quarantined", "superseded"]
    dataset_ids: List[str] = Field(default_factory=list, alias="datasetIds")
    artifact_ids: List[str] = Field(default_factory=list, alias="artifactIds")
    created_at: datetime = Field(default_factory=utc_now, alias="createdAt")
    published_at: Optional[datetime] = Field(default=None, alias="publishedAt")

    model_config = ConfigDict(populate_by_name=True)


class ValidationFinding(BaseModel):
    id: str = Field(alias="_id")
    run_id: str = Field(alias="runId")
    tenant_id: str = Field(alias="tenantId")
    snapshot_ref: str = Field(alias="snapshotRef")
    severity: Literal["error", "warning", "info"]
    rule_id: str = Field(alias="ruleId")
    message: str
    dataset_id: Optional[str] = Field(default=None, alias="datasetId")
    record_id: Optional[str] = Field(default=None, alias="recordId")
    variable: Optional[str] = None
    waived: bool = False
    waiver_id: Optional[str] = Field(default=None, alias="waiverId")
    details: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)


class ValidationRun(BaseModel):
    id: str = Field(alias="_id")
    run_id: str = Field(alias="runId")
    tenant_id: str = Field(alias="tenantId")
    snapshot_ref: str = Field(alias="snapshotRef")
    engine: str
    engine_version: str = Field(alias="engineVersion")
    status: Literal["passed", "failed"]
    summary: Dict[str, int]
    input_digest: Optional[str] = Field(default=None, alias="inputDigest")
    standards_package_id: Optional[str] = Field(default=None, alias="standardsPackageId")
    rule_packages: List[str] = Field(default_factory=list, alias="rulePackages")
    coverage: Dict[str, Any] = Field(default_factory=dict)
    started_at: datetime = Field(alias="startedAt")
    completed_at: datetime = Field(alias="completedAt")

    model_config = ConfigDict(populate_by_name=True)
