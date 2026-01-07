"""Compatibility shim for legacy transformers to get strategy config."""
from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CollectionConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    enabled: Optional[bool] = True
    atlas_index_name: Optional[str] = None
    shortcuts_doc_id: Optional[str] = None
    arcodes_doc_id: Optional[str] = None


class DictionaryConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    enabled: bool = True
    collection: Optional[str] = None
    doc_id: Optional[str] = None


class CodingConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    strategy: Optional[str] = None
    zero_prefix_map: Dict[str, str] = Field(default_factory=dict)
    dotted_variants: Optional[Dict[str, Any]] = None
    case: Optional[str] = "lower"
    store_original: Optional[bool] = True
    archetype_ids: Optional[Dict[str, Any]] = None
    atcodes: Optional[Dict[str, Any]] = None


class FieldMapping(BaseModel):
    nodes: Optional[str] = None
    data: Optional[str] = None
    path: Optional[str] = None
    archetype_path: Optional[str] = None
    ancestors: Optional[str] = None
    key_path: Optional[str] = None
    list_index: Optional[str] = None
    ehr_id: Optional[str] = None
    comp_id: Optional[str] = None
    template_id: Optional[str] = None
    version: Optional[str] = None
    score: Optional[str] = None


class NodeRepresentation(BaseModel):
    model_config = ConfigDict(extra="ignore")
    path: Optional[Dict[str, Any]] = None
    archetype_path: Optional[Dict[str, Any]] = None
    ancestors: Optional[Dict[str, Any]] = None


class PersistenceStrategy(BaseModel):
    """Legacy persistence strategy config used by old AQL transformers."""

    model_config = ConfigDict(extra="ignore")
    name: str = "default"
    description: Optional[str] = None
    database: Optional[str] = None
    collections: Dict[str, CollectionConfig] = Field(default_factory=dict)
    dictionaries: Dict[str, DictionaryConfig] = Field(default_factory=dict)
    coding: Optional[CodingConfig] = None
    fields: Dict[str, FieldMapping] = Field(default_factory=dict)
    node_representation: Optional[NodeRepresentation] = None
    enrichment: Optional[Dict[str, Any]] = None

    @field_validator("collections", mode="before")
    def ensure_collection_configs(cls, value):
        if not value:
            return {}
        return {k: (CollectionConfig(**v) if not isinstance(v, CollectionConfig) else v) for k, v in value.items()}

    @field_validator("dictionaries", mode="before")
    def ensure_dictionary_configs(cls, value):
        if not value:
            return {}
        return {k: (DictionaryConfig(**v) if not isinstance(v, DictionaryConfig) else v) for k, v in value.items()}

    @field_validator("fields", mode="before")
    def ensure_field_configs(cls, value):
        if not value:
            return {}
        return {k: (FieldMapping(**v) if not isinstance(v, FieldMapping) else v) for k, v in value.items()}


DEFAULT_STRATEGY: PersistenceStrategy | None = None


def configure(cfg: Dict[str, Any]):
    global DEFAULT_STRATEGY
    try:
        # map to expected shape
        collections = cfg.get("collections", {})
        fields = cfg.get("fields", {})
        strategy_dict = {
            "collections": {
                "compositions": collections.get("compositions", {}),
                "search": collections.get("search", {}),
            },
            "fields": {
                "composition": fields.get("composition", {}),
                "search": fields.get("search", {}),
            },
        }
        DEFAULT_STRATEGY = PersistenceStrategy(**strategy_dict)
    except Exception:
        DEFAULT_STRATEGY = PersistenceStrategy()


def get_default_strategy() -> PersistenceStrategy:
    return DEFAULT_STRATEGY or PersistenceStrategy()
