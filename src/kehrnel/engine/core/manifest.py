from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, model_validator


# Strategy status for UI display
StrategyStatus = Literal["stable", "preview", "example", "community"]


class AdapterRequirements(BaseModel):
    storage: List[str] = Field(default_factory=list)
    search: List[str] = Field(default_factory=list)
    vector: List[str] = Field(default_factory=list)
    queue: List[str] = Field(default_factory=list)


class StrategyUI(BaseModel):
    status: Optional[StrategyStatus] = Field(
        default="stable",
        description="Strategy availability status: stable (production-ready), preview (coming soon), example (showcase), community (open for contribution)"
    )
    status_message: Optional[str] = Field(
        default=None,
        description="Custom message to display when user clicks on the status indicator"
    )
    docs: Optional[str] = Field(
        default=None,
        description="Canonical strategy documentation URL/path (preferred over links.docs).",
    )
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

    model_config = {"extra": "allow"}

    @model_validator(mode="after")
    def _backfill_docs_from_legacy_links(self):
        # Backward compatibility with older manifests that used ui.links.docs.
        if not self.docs:
            docs = (self.links or {}).get("docs")
            if isinstance(docs, str) and docs.strip():
                self.docs = docs
        return self


class StrategyOp(BaseModel):
    name: str
    kind: str = "extension"
    summary: Optional[str] = None
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)


class StrategyManifest(BaseModel):
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
    compatibility: Dict[str, Any] = Field(default_factory=dict)
    maturity: Optional[str] = None
    license: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    ops: List[StrategyOp] = Field(default_factory=list)

    model_config = {"extra": "allow"}
