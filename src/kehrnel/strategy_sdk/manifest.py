from __future__ import annotations

from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict

from .capabilities import StrategyCapability


class AdapterRequirements(BaseModel):
    """Declare which adapter kinds a strategy expects."""

    storage: List[str] = Field(default_factory=list, description="Storage adapter ids (e.g., mongo, postgres)")
    search: List[str] = Field(default_factory=list, description="Search adapter ids (e.g., atlas_search)")
    vector: List[str] = Field(default_factory=list, description="Vector adapter ids (e.g., atlas_vector, pgvector)")
    queue: List[str] = Field(default_factory=list, description="Queue adapter ids (e.g., kafka, nats, sqs)")

    def required_kinds(self) -> Set[str]:
        kinds = set()
        if self.storage:
            kinds.add("storage")
        if self.search:
            kinds.add("search")
        if self.vector:
            kinds.add("vector")
        if self.queue:
            kinds.add("queue")
        return kinds


class StrategyUI(BaseModel):
    """Metadata to drive catalog/portal presentation."""

    docs: Optional[str] = Field(
        default=None,
        description="Canonical strategy documentation URL/path (preferred over links.docs).",
    )
    tags: List[str] = Field(default_factory=list)
    domain_badge: Optional[str] = None
    icon: Optional[str] = None
    accent_color: Optional[str] = None
    works_with: List[str] = Field(default_factory=list)
    links: Dict[str, str] = Field(default_factory=dict)  # Legacy metadata map; docs should use `docs`.

    @model_validator(mode="after")
    def _backfill_docs_from_legacy_links(self):
        # Keep SDK-compatible behavior for manifests still using ui.links.docs.
        if not self.docs:
            docs = (self.links or {}).get("docs")
            if isinstance(docs, str) and docs.strip():
                self.docs = docs
        return self


class StrategyCompatibility(BaseModel):
    """Optional compatibility matrix for portal filtering."""

    platforms: List[str] = Field(default_factory=list, description="Kernel versions supported (semver ranges)")
    storage: List[str] = Field(default_factory=list, description="Known-good storage adapters")
    search: List[str] = Field(default_factory=list, description="Known-good search adapters")
    vector: List[str] = Field(default_factory=list, description="Known-good vector adapters")
    domains: List[str] = Field(default_factory=list, description="Domain versions or variants supported")


class StrategyManifest(BaseModel):
    """
    Declarative manifest for a strategy plugin.
    This is serializable to JSON for registry/catalog use.
    """
    model_config = ConfigDict(validate_assignment=True)
    id: str
    name: str
    version: str
    summary: Optional[str] = None
    description: Optional[str] = None
    domain: str
    capabilities: List[StrategyCapability] = Field(default_factory=list)
    entrypoint: str = Field(..., description="Dotted path 'module:ClassName' for the plugin implementation")
    config_schema: Dict[str, Any] = Field(default_factory=dict)
    default_config: Dict[str, Any] = Field(default_factory=dict)
    adapters: AdapterRequirements = Field(default_factory=AdapterRequirements)
    ui: StrategyUI = Field(default_factory=StrategyUI)
    compatibility: StrategyCompatibility = Field(default_factory=StrategyCompatibility)
    maturity: Optional[str] = Field(None, description="alpha|beta|ga|experimental")
    license: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)

    @field_validator("id")
    def id_must_be_slug(cls, v: str) -> str:
        if not v:
            raise ValueError("id is required")
        if " " in v:
            raise ValueError("id must be a slug (no spaces)")
        return v

    @field_validator("version")
    def version_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("version is required")
        return v

    @property
    def capability_set(self) -> Set[StrategyCapability]:
        return set(self.capabilities or [])
