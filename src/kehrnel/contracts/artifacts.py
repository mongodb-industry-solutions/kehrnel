"""Small, stable identity, evidence, and error contracts for public APIs."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


Sha256Digest = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]


class ContractModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=lambda name: name.split("_")[0]
        + "".join(part.title() for part in name.split("_")[1:]),
        populate_by_name=True,
        extra="forbid",
    )

    def to_wire(self, *, exclude_none: bool = True) -> Dict[str, Any]:
        """Serialize with the stable camelCase API field names."""

        return self.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=exclude_none,
        )


class PackageLifecycle(str, Enum):
    DRAFT = "draft"
    EXPERIMENTAL = "experimental"
    PREVIEW = "preview"
    GENERALLY_AVAILABLE = "generally_available"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class CapabilityMaturity(str, Enum):
    NOT_ASSESSED = "not_assessed"
    SPECIFIED = "specified"
    SIMULATED = "simulated"
    COMPILABLE = "compilable"
    PILOT_VALIDATED = "pilot_validated"
    PRODUCTION_VALIDATED = "production_validated"


class SupportStatus(str, Enum):
    UNSUPPORTED = "unsupported"
    EVALUATION_ONLY = "evaluation_only"
    SUPPORTED = "supported"
    DEPRECATED = "deprecated"


class RuntimeAvailability(str, Enum):
    AVAILABLE = "available"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class ExecutionState(str, Enum):
    PLANNED = "planned"
    RUNNING = "running"
    EXECUTED = "executed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ObservationKind(str, Enum):
    MEASURED = "measured"
    SIMULATED = "simulated"
    ESTIMATED = "estimated"
    ILLUSTRATIVE = "illustrative"


class Visibility(str, Enum):
    PRIVATE = "private"
    TENANT = "tenant"
    ORGANIZATION = "organization"
    PUBLIC = "public"


class PublicationStatus(str, Enum):
    DRAFT = "draft"
    IN_REVIEW = "in_review"
    PUBLISHED = "published"
    WITHDRAWN = "withdrawn"


class ArtifactProvenance(str, Enum):
    """Who owns the lifecycle of a reusable catalog artifact."""

    MONGODB_CURATED = "mongodb_curated"
    COMMUNITY = "community"
    TENANT_GENERATED = "tenant_generated"
    TENANT_EXTENDED = "tenant_extended"


class CatalogArtifactRole(str, Enum):
    """The architectural role an artifact plays, independent of its industry."""

    SOURCE_CATEGORY = "source_category"
    SOURCE_PROFILE = "source_profile"
    MAPPING = "mapping"
    TARGET_STRATEGY = "target_strategy"
    SEMANTIC_ASSET = "semantic_asset"
    DATA_ARCHITECTURE = "data_architecture"
    APPLICATION = "application"
    OUTCOME_PATTERN = "outcome_pattern"
    REFERENCE_ARCHITECTURE = "reference_architecture"
    DEMO_EXPERIENCE = "demo_experience"
    PORTFOLIO_DOMAIN = "portfolio_domain"
    DATA_PRODUCT = "data_product"
    ARCHITECTURE_CAPABILITY = "architecture_capability"
    VISUAL_PATTERN = "visual_pattern"
    SOLUTION_BLUEPRINT_TEMPLATE = "solution_blueprint_template"


class ActionAvailabilityState(str, Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    REQUEST_ACCESS = "request_access"


class Retryability(str, Enum):
    NEVER = "never"
    SAFE = "safe"
    CONDITIONAL = "conditional"


class ArtifactRef(ContractModel):
    """Immutable reference to one exact artifact version and content digest."""

    kind: str = Field(min_length=1, pattern=r"^\S(?:.*\S)?$")
    id: str = Field(min_length=1, pattern=r"^\S(?:.*\S)?$")
    version: str = Field(min_length=1, pattern=r"^\S(?:.*\S)?$")
    digest: Sha256Digest

    model_config = ConfigDict(frozen=True)

    @field_validator("kind", "id", "version")
    @classmethod
    def no_surrounding_whitespace(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("must not contain surrounding whitespace")
        return value


class CatalogPath(ContractModel):
    """One navigational placement; artifacts may appear in several industries."""

    industry: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    segments: List[str] = Field(min_length=1)

    @field_validator("segments")
    @classmethod
    def segments_are_normalized(cls, value: List[str]) -> List[str]:
        invalid = [
            segment
            for segment in value
            if not segment
            or segment != segment.strip()
            or not all(character.islower() or character.isdigit() or character == "-" for character in segment)
        ]
        if invalid:
            raise ValueError(
                "catalog path segments must be lowercase kebab-case values"
            )
        return value


class CatalogMetadata(ContractModel):
    """Faceted catalog metadata kept separate from executable behavior."""

    role: CatalogArtifactRole
    provenance: ArtifactProvenance
    paths: List[CatalogPath] = Field(min_length=1)
    tags: List[str] = Field(default_factory=list)
    extends_ref: Optional[ArtifactRef] = None

    @field_validator("paths")
    @classmethod
    def paths_are_unique(cls, value: List[CatalogPath]) -> List[CatalogPath]:
        keys = [(path.industry, tuple(path.segments)) for path in value]
        if len(keys) != len(set(keys)):
            raise ValueError("catalog paths must be unique")
        return value

    @field_validator("tags")
    @classmethod
    def tags_are_unique(cls, value: List[str]) -> List[str]:
        if len(value) != len(set(value)):
            raise ValueError("catalog tags must be unique")
        return value

    @model_validator(mode="after")
    def extended_artifacts_identify_base(self) -> "CatalogMetadata":
        if self.provenance is ArtifactProvenance.TENANT_EXTENDED and self.extends_ref is None:
            raise ValueError("tenant_extended artifacts require extendsRef")
        if self.provenance is not ArtifactProvenance.TENANT_EXTENDED and self.extends_ref is not None:
            raise ValueError("extendsRef is reserved for tenant_extended artifacts")
        return self


class CapabilityClaim(ContractModel):
    capability_id: str = Field(min_length=1)
    capability_maturity: CapabilityMaturity
    support_status: SupportStatus
    runtime_availability: RuntimeAvailability
    evidence_refs: List[ArtifactRef] = Field(default_factory=list)


class ContentDigests(ContractModel):
    """Named digest roles; these values are related but not interchangeable."""

    package_digest: Optional[Sha256Digest] = None
    contract_digest: Optional[Sha256Digest] = None
    composition_digest: Optional[Sha256Digest] = None
    candidate_digest: Optional[Sha256Digest] = None


class ActionAvailability(ContractModel):
    action_id: str = Field(min_length=1)
    state: ActionAvailabilityState
    reason_code: Optional[str] = None
    required_capability_ids: List[str] = Field(default_factory=list)
    target_ref: Optional[ArtifactRef] = None
    contributing_capability_ids: List[str] = Field(default_factory=list)
    blocking_capability_ids: List[str] = Field(default_factory=list)


class FieldError(ContractModel):
    path: str = Field(pattern=r"^(|/.*)$")
    code: str
    message: str


class ProblemDetails(ContractModel):
    """Normalized error shape based on RFC 9457 with Kehrnel extensions."""

    type: str = "about:blank"
    title: str
    status: int = Field(ge=400, le=599)
    code: str
    detail: Optional[str] = None
    correlation_id: str = Field(min_length=1)
    field_errors: List[FieldError] = Field(default_factory=list)
    retryability: Retryability = Retryability.NEVER
    extensions: Dict[str, Any] = Field(default_factory=dict)
