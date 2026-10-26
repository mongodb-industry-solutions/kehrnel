"""Canonical strategy manifest models.

Historically the execution runtime and the public strategy SDK carried two
similar, but incompatible, Pydantic models. This module is now the single
authority. The old import paths re-export these exact classes so existing
strategy packs and SDK consumers continue to work.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Dict, Iterator, List, Literal, Optional, Set

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_serializer,
    model_validator,
)

from .artifacts import Sha256Digest


StrategyStatus = Literal["stable", "preview", "example", "community"]
InteractionMode = Literal["native", "compiled", "hybrid"]
ActivationConditionKind = Literal["attestation", "notice", "reference"]
ActivationSeedKind = Literal["collection_copy"]
ActivationSeedCollectionMode = Literal["copy", "empty"]
StrictToken = Annotated[str, StringConstraints(min_length=1, pattern=r"^\S+$")]
StrictTrimmedText = Annotated[
    str,
    StringConstraints(min_length=1, pattern=r"^\S(?:.*\S)?$"),
]
StrictEntrypoint = Annotated[
    str,
    StringConstraints(
        pattern=r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*$"
    ),
]


class AdapterRequirements(BaseModel):
    """Adapter kinds a strategy can require at activation time."""

    storage: List[str] = Field(default_factory=list)
    artifact: List[str] = Field(default_factory=list)
    search: List[str] = Field(default_factory=list)
    vector: List[str] = Field(default_factory=list)
    queue: List[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow", validate_assignment=True)

    def required_kinds(self) -> Set[str]:
        return {
            kind
            for kind in ("storage", "artifact", "search", "vector", "queue")
            if getattr(self, kind)
        }


class StrategyUI(BaseModel):
    """Presentation hints; never the authority for runtime capabilities."""

    status: Optional[StrategyStatus] = Field(default="stable")
    status_message: Optional[str] = None
    docs: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    domain_badge: Optional[str] = None
    icon: Optional[str] = None
    accent_color: Optional[str] = None
    works_with: List[str] = Field(default_factory=list)
    links: Dict[str, str] = Field(default_factory=dict)
    benefits: List[Any] = Field(default_factory=list)
    constraints: List[Any] = Field(default_factory=list)
    pick_when: List[Any] = Field(default_factory=list)
    avoid_when: List[Any] = Field(default_factory=list)
    scorecard: Dict[str, Any] = Field(default_factory=dict)
    tabs: Dict[str, Any] = Field(default_factory=dict)
    activatable: Optional[bool] = None
    consider_alternatives: Any = None
    example_data: Any = None
    how_it_works: Any = None
    ideal_for: Any = None
    index_contract: Any = None
    references: Any = None
    story: Any = None

    model_config = ConfigDict(extra="allow", validate_assignment=True)

    @model_validator(mode="after")
    def _backfill_docs_from_legacy_links(self) -> "StrategyUI":
        if not self.docs:
            docs = (self.links or {}).get("docs")
            if isinstance(docs, str) and docs.strip():
                self.docs = docs
        return self


class StrategyCompatibility(BaseModel):
    """Compatibility facts from both legacy and current manifests.

    ``extra='allow'`` deliberately preserves older keys such as ``packFormat``,
    ``requires`` and ``optional`` while giving new packs typed common fields.
    """

    platforms: List[str] = Field(default_factory=list)
    storage: List[str] = Field(default_factory=list)
    search: List[str] = Field(default_factory=list)
    vector: List[str] = Field(default_factory=list)
    domains: List[str] = Field(default_factory=list)
    pack_format: Optional[str] = Field(default=None, alias="packFormat")
    requires: List[str] = Field(default_factory=list)
    optional: List[str] = Field(default_factory=list)

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        validate_assignment=True,
    )

    def _authored(self) -> Dict[str, Any]:
        result: Dict[str, Any] = dict(self.model_extra or {})
        for field_name in self.model_fields_set:
            field = type(self).model_fields.get(field_name)
            if field is None:
                continue
            key = field.alias or field_name
            result[key] = getattr(self, field_name)
        return result

    @model_serializer
    def serialize_authored_shape(self) -> Dict[str, Any]:
        """Do not inject empty SDK defaults into legacy runtime payloads."""

        return self._authored()

    def __getitem__(self, key: str) -> Any:
        if key == "packFormat":
            return self.pack_format
        if key in type(self).model_fields:
            return getattr(self, key)
        if key in (self.model_extra or {}):
            return (self.model_extra or {})[key]
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._authored())

    def __len__(self) -> int:
        return len(self._authored())

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return self._authored().keys()

    def items(self):
        return self._authored().items()

    def values(self):
        return self._authored().values()

class StrategyOp(BaseModel):
    name: str
    kind: str = "extension"
    summary: Optional[str] = None
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow", validate_assignment=True)


class InteractionContract(BaseModel):
    """Who defines an interaction and how Kehrnel executes it."""

    id: str
    name: str
    mode: InteractionMode
    kind: str = "query"
    authority: str
    standard: Optional[str] = None
    contract: str
    description: Optional[str] = None
    engines: List[str] = Field(default_factory=list)
    builders: List[str] = Field(default_factory=list)
    operations: List[str] = Field(default_factory=list)
    source_domains: List[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow", validate_assignment=True)


class ActivationConditionReference(BaseModel):
    """A "Learn more" link attached to an activation condition."""

    label: str
    url: str

    model_config = ConfigDict(extra="allow", validate_assignment=True)


class ActivationCondition(BaseModel):
    """One condition of the activation journey.

    ``kind: attestation`` is runtime authority: activation without an explicit
    acceptance of every required attestation is refused. ``notice`` renders
    read-only and ``reference`` renders as links only; neither gates activation.
    """

    id: str
    kind: ActivationConditionKind
    required: bool = False
    code: Optional[str] = None
    title: Optional[str] = None
    statement: Optional[str] = None
    statement_translations: Dict[str, str] = Field(default_factory=dict)
    statement_version: Optional[int] = None
    message: Optional[str] = None
    references: List[ActivationConditionReference] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow", validate_assignment=True)

    @model_validator(mode="after")
    def _attestations_carry_a_versioned_statement(self) -> "ActivationCondition":
        if self.kind == "attestation":
            if not (self.statement or "").strip():
                raise ValueError("attestation conditions require a statement")
            if self.statement_version is None or self.statement_version < 1:
                raise ValueError("attestation conditions require a statement_version >= 1")
        return self


class ActivationDatabaseHint(BaseModel):
    """Presentation hints for the tenant database configuration step."""

    naming_hint: Optional[str] = None
    example: Optional[str] = None

    model_config = ConfigDict(extra="allow", validate_assignment=True)


class ActivationSeedCollection(BaseModel):
    """One collection provisioned into the tenant database by the seed step.

    ``mode: copy`` copies ``{seed.source_database}.{from}`` into the configured
    collection name; ``mode: empty`` only ensures the collection exists (used
    for collections the product fills itself, such as review/telemetry stores).
    """

    name: str
    source: Optional[str] = Field(default=None, alias="from")
    expected_documents: Optional[int] = None
    mode: ActivationSeedCollectionMode = "copy"

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        validate_assignment=True,
    )

    @model_validator(mode="after")
    def _copied_collections_name_their_source(self) -> "ActivationSeedCollection":
        if self.mode == "copy" and not (self.source or "").strip():
            raise ValueError("copied seed collections require a 'from' source collection")
        return self


class ActivationSeed(BaseModel):
    """Declarative seed provisioning executed as part of activation.

    Only sample-class, redistributable content may be declared as a seed
    source; full licensed releases arrive through the licensed ingest path.
    """

    kind: ActivationSeedKind = "collection_copy"
    source_database: str
    license_class: str = "sample"
    scope: Optional[str] = None
    digest: Optional[str] = None
    collections: List[ActivationSeedCollection] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow", validate_assignment=True)


class StrategyActivation(BaseModel):
    """Manifest-borne activation contract (runtime authority, not presentation).

    Conditions are enforced by the runtime, the database hint is rendered by
    consumers, and the seed declaration drives collection provisioning into the
    activation-owned tenant database.
    """

    requires_database: bool = False
    database: Optional[ActivationDatabaseHint] = None
    conditions: List[ActivationCondition] = Field(default_factory=list)
    seed: Optional[ActivationSeed] = None
    readiness_op: Optional[str] = None

    model_config = ConfigDict(extra="allow", validate_assignment=True)

    @property
    def required_conditions(self) -> List[ActivationCondition]:
        return [condition for condition in self.conditions if condition.required]

    @property
    def required_attestation_ids(self) -> List[str]:
        return [
            condition.id
            for condition in self.required_conditions
            if condition.kind == "attestation"
        ]


class StrategyManifest(BaseModel):
    """Backward-compatible strategy package manifest.

    Capability identifiers are strings, rather than a closed enum, because
    domain packs legitimately publish qualified capabilities such as
    ``terminology.subsumes``. SDK enum values are accepted and normalized to
    their string values.
    """

    id: str
    name: str
    version: str
    summary: Optional[str] = None
    description: Optional[str] = None
    domain: str
    capabilities: List[str] = Field(default_factory=list)
    entrypoint: Optional[str] = None
    config_schema: Dict[str, Any] = Field(default_factory=dict)
    default_config: Dict[str, Any] = Field(default_factory=dict)
    adapters: AdapterRequirements = Field(default_factory=AdapterRequirements)
    ui: StrategyUI = Field(default_factory=StrategyUI)
    activation: Optional[StrategyActivation] = None
    compatibility: StrategyCompatibility = Field(default_factory=StrategyCompatibility)
    maturity: Optional[str] = None
    license: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    ops: List[StrategyOp] = Field(default_factory=list)
    interaction_contracts: List[InteractionContract] = Field(default_factory=list)
    manifest_contract: Optional[str] = Field(default=None, alias="manifestContract")
    pack_format: Optional[str] = None
    spec: Dict[str, Any] | str | None = None
    config: Dict[str, Any] = Field(default_factory=dict)
    variants: List[str] = Field(default_factory=list)
    pack_spec: Dict[str, Any] = Field(default_factory=dict)
    package_digest: Optional[Sha256Digest] = Field(default=None, alias="packageDigest")
    contract_digest: Optional[Sha256Digest] = Field(default=None, alias="contractDigest")

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        validate_assignment=True,
    )

    @field_validator("id")
    @classmethod
    def id_must_be_slug(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("id is required")
        if any(char.isspace() for char in value):
            raise ValueError("id must be a slug (no whitespace)")
        return value

    @field_validator("name", "version", "domain")
    @classmethod
    def required_text_non_empty(cls, value: str, info) -> str:
        value = value.strip()
        if not value:
            raise ValueError(f"{info.field_name} is required")
        return value

    @field_validator("capabilities", mode="before")
    @classmethod
    def normalize_capabilities(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, (list, tuple, set)):
            raise ValueError("capabilities must be a list")
        normalized: List[str] = []
        for capability in value:
            if isinstance(capability, Enum):
                capability = capability.value
            text = str(capability).strip()
            if text:
                normalized.append(text)
        return normalized

    @field_validator("adapters", "ui", "compatibility", mode="before")
    @classmethod
    def normalize_optional_objects(cls, value: Any) -> Dict[str, Any]:
        return {} if value is None else value

    @field_validator("ops", "interaction_contracts", mode="before")
    @classmethod
    def normalize_optional_lists(cls, value: Any) -> List[Any]:
        return [] if value is None else value

    @property
    def capability_set(self) -> Set[str]:
        return set(self.capabilities or [])


class _StrictAdapterRequirements(AdapterRequirements):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class _StrictStrategyUI(StrategyUI):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class _StrictStrategyCompatibility(StrategyCompatibility):
    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True,
        strict=True,
    )


class _StrictStrategyOp(StrategyOp):
    name: StrictToken
    input_schema: Dict[str, Any]

    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class _StrictInteractionContract(InteractionContract):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class _StrictActivationConditionReference(ActivationConditionReference):
    label: StrictTrimmedText
    url: StrictToken

    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class _StrictActivationCondition(ActivationCondition):
    id: StrictToken
    title: Optional[StrictTrimmedText] = None
    references: List[_StrictActivationConditionReference] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class _StrictActivationDatabaseHint(ActivationDatabaseHint):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class _StrictActivationSeedCollection(ActivationSeedCollection):
    name: StrictToken
    source: Optional[StrictToken] = Field(default=None, alias="from")

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True,
        strict=True,
    )


class _StrictActivationSeed(ActivationSeed):
    source_database: StrictToken
    license_class: StrictToken
    collections: List[_StrictActivationSeedCollection] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class _StrictStrategyActivation(StrategyActivation):
    database: Optional[_StrictActivationDatabaseHint] = None
    conditions: List[_StrictActivationCondition] = Field(default_factory=list)
    seed: Optional[_StrictActivationSeed] = None
    readiness_op: Optional[StrictToken] = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class StrategyManifestSource(StrategyManifest):
    """Strict source-file contract before runtime-computed identity is added."""

    manifest_contract: Literal["strategy-manifest/v1"] = Field(alias="manifestContract")
    id: StrictToken
    name: StrictTrimmedText
    version: StrictToken
    domain: StrictToken
    capabilities: List[StrictToken] = Field(default_factory=list)
    entrypoint: StrictEntrypoint
    adapters: _StrictAdapterRequirements = Field(default_factory=_StrictAdapterRequirements)
    ui: _StrictStrategyUI = Field(default_factory=_StrictStrategyUI)
    activation: Optional[_StrictStrategyActivation] = None
    compatibility: _StrictStrategyCompatibility = Field(
        default_factory=_StrictStrategyCompatibility
    )
    ops: List[_StrictStrategyOp]
    interaction_contracts: List[_StrictInteractionContract] = Field(default_factory=list)
    pack_spec: Dict[str, Any] = Field(default_factory=dict, exclude=True)
    package_digest: Optional[Sha256Digest] = Field(
        default=None,
        alias="packageDigest",
        exclude=True,
    )
    contract_digest: Optional[Sha256Digest] = Field(
        default=None,
        alias="contractDigest",
        exclude=True,
    )

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True,
        strict=True,
    )

    @model_validator(mode="before")
    @classmethod
    def runtime_identity_must_not_be_authored(cls, value: Any) -> Any:
        if isinstance(value, dict):
            forbidden = {
                "packageDigest",
                "package_digest",
                "contractDigest",
                "contract_digest",
                "pack_spec",
            }
            authored = sorted(forbidden.intersection(value))
            if authored:
                raise ValueError(
                    "runtime-computed fields must not be authored: " + ", ".join(authored)
                )
            for field_name in (
                "capabilities",
                "ops",
                "interaction_contracts",
            ):
                if field_name in value and not isinstance(value[field_name], list):
                    raise ValueError(f"{field_name} must be a list")
            for field_name in ("adapters", "ui", "compatibility"):
                if field_name in value and not isinstance(value[field_name], dict):
                    raise ValueError(f"{field_name} must be an object")
            if (
                "activation" in value
                and value["activation"] is not None
                and not isinstance(value["activation"], dict)
            ):
                raise ValueError("activation must be an object")
            capabilities = value.get("capabilities", [])
            if any(
                not isinstance(item, str)
                or not item
                or item != item.strip()
                or any(char.isspace() for char in item)
                for item in capabilities
            ):
                raise ValueError("capabilities must contain non-empty string identifiers")
        return value

    @classmethod
    def model_json_schema(cls, *args, **kwargs) -> Dict[str, Any]:
        """Publish only source-authored fields; runtime identity is output-only."""

        schema = super().model_json_schema(*args, **kwargs)
        properties = schema.get("properties", {})
        for field_name in ("pack_spec", "packageDigest", "contractDigest"):
            properties.pop(field_name, None)
        return schema


def read_strategy_manifest(data: Dict[str, Any]) -> StrategyManifest:
    """Compatibility reader for current and legacy manifest dictionaries."""

    return StrategyManifest.model_validate(dict(data))
