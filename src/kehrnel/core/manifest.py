from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class AdapterRequirements(BaseModel):
    storage: List[str] = Field(default_factory=list)
    search: List[str] = Field(default_factory=list)
    vector: List[str] = Field(default_factory=list)
    queue: List[str] = Field(default_factory=list)


class StrategyUI(BaseModel):
    tags: List[str] = Field(default_factory=list)
    protocol_badge: Optional[str] = None
    icon: Optional[str] = None
    accent_color: Optional[str] = None
    works_with: List[str] = Field(default_factory=list)
    links: Dict[str, str] = Field(default_factory=dict)


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
    protocols: List[str] = Field(default_factory=list)
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

    model_config = {"extra": "ignore"}
