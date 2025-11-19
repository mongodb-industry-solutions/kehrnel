from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field, validator
import json


class CollectionConfig(BaseModel):
    name: str
    enabled: Optional[bool] = True
    atlas_index_name: Optional[str] = None
    shortcuts_doc_id: Optional[str] = None
    arcodes_doc_id: Optional[str] = None


class DictionaryConfig(BaseModel):
    enabled: bool = True
    collection: Optional[str] = None
    doc_id: Optional[str] = None


class CodingConfig(BaseModel):
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
    path: Optional[Dict[str, Any]] = None
    archetype_path: Optional[Dict[str, Any]] = None
    ancestors: Optional[Dict[str, Any]] = None


class PersistenceStrategy(BaseModel):
    name: str = "default"
    description: Optional[str] = None
    database: Optional[str] = None
    collections: Dict[str, CollectionConfig] = Field(default_factory=dict)
    dictionaries: Dict[str, DictionaryConfig] = Field(default_factory=dict)
    coding: Optional[CodingConfig] = None
    fields: Dict[str, FieldMapping] = Field(default_factory=dict)
    node_representation: Optional[NodeRepresentation] = None
    enrichment: Optional[Dict[str, Any]] = None

    @validator("collections", pre=True)
    def ensure_collection_configs(cls, value):
        if not value:
            return {}
        return {k: (CollectionConfig(**v) if not isinstance(v, CollectionConfig) else v) for k, v in value.items()}

    @validator("dictionaries", pre=True)
    def ensure_dictionary_configs(cls, value):
        if not value:
            return {}
        return {k: (DictionaryConfig(**v) if not isinstance(v, DictionaryConfig) else v) for k, v in value.items()}

    @validator("fields", pre=True)
    def ensure_field_configs(cls, value):
        if not value:
            return {}
        return {k: (FieldMapping(**v) if not isinstance(v, FieldMapping) else v) for k, v in value.items()}


def load_strategy_from_file(path: Union[str, Path]) -> PersistenceStrategy:
    path = Path(path)
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    return PersistenceStrategy(**data)


def load_strategy_from_json(data: Union[str, Dict[str, Any]]) -> PersistenceStrategy:
    if isinstance(data, str):
        data = json.loads(data)
    return PersistenceStrategy(**data)


def get_default_strategy() -> PersistenceStrategy:
    return PersistenceStrategy(
        name="default-hybrid",
        collections={
            "compositions": CollectionConfig(name="flatten_compositions"),
            "search": CollectionConfig(name="sm_search3"),
        },
        fields={
            "composition": FieldMapping(
                nodes="cn",
                data="data",
                path="p",
                archetype_path="ap",
                ancestors="anc",
                ehr_id="ehr_id",
                comp_id="comp_id",
                template_id="tid",
                version="v",
            ),
            "search": FieldMapping(
                nodes="sn",
                data="data",
                path="p",
                archetype_path="ap",
                ancestors="anc",
                ehr_id="ehr_id",
                template_id="tid",
                score="score",
            ),
        },
    )
