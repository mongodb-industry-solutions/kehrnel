from __future__ import annotations

from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class CollectionsCfg(BaseModel):
    compositions: Dict[str, Any] = Field(default_factory=dict)
    search: Dict[str, Any] = Field(default_factory=dict)


class FieldsCfg(BaseModel):
    composition: Dict[str, Any] = Field(default_factory=dict)
    search: Dict[str, Any] = Field(default_factory=dict)


class CodingCfg(BaseModel):
    archetype_ids: Dict[str, Any] = Field(default_factory=dict)
    atcodes: Dict[str, Any] = Field(default_factory=dict)


class QueryEngineCfg(BaseModel):
    lookup_full_composition: bool = False
    mode: Optional[str] = None


class RPSDualConfig(BaseModel):
    database: Optional[str] = None
    collections: CollectionsCfg = Field(default_factory=CollectionsCfg)
    fields: FieldsCfg = Field(default_factory=FieldsCfg)
    coding: CodingCfg = Field(default_factory=CodingCfg)
    node_representation: Dict[str, Any] = Field(default_factory=dict)
    query_engine: QueryEngineCfg = Field(default_factory=QueryEngineCfg)


def normalize_config(raw: Dict[str, Any]) -> RPSDualConfig:
    return RPSDualConfig(**(raw or {}))


def build_schema_config(cfg: RPSDualConfig) -> Dict[str, Dict[str, Any]]:
    comp_coll = cfg.collections.compositions if isinstance(cfg.collections.compositions, dict) else {}
    search_coll = cfg.collections.search if isinstance(cfg.collections.search, dict) else {}
    comp_nodes = comp_coll.get("nodes_field")
    search_nodes = search_coll.get("nodes_field")
    comp_fields = cfg.fields.composition if isinstance(cfg.fields, FieldsCfg) else cfg.fields.get("composition", {})
    search_fields = cfg.fields.search if isinstance(cfg.fields, FieldsCfg) else cfg.fields.get("search", {})
    composition_schema = {
        "composition_array": comp_nodes or comp_fields.get("nodes", "cn"),
        "path_field": comp_fields.get("path", "p"),
        "data_field": comp_fields.get("data", "data"),
        "ehr_id": comp_fields.get("ehr_id", "ehr_id"),
        "comp_id": comp_fields.get("comp_id", "cid"),
        "collection": comp_coll.get("name", "compositions"),
    }
    search_schema = {
        "composition_array": search_nodes or search_fields.get("nodes", "sn"),
        "path_field": search_fields.get("path", "p"),
        "data_field": search_fields.get("data", "data"),
        "ehr_id": search_fields.get("ehr_id", "ehr_id"),
        "comp_id": search_fields.get("comp_id", "cid"),
        "index_name": search_coll.get("atlas_index_name"),
        "lookup_from": comp_coll.get("name"),
        "lookup_as": "full_composition",
        "collection": search_coll.get("name", "compositions_search"),
    }
    return {"composition": composition_schema, "search": search_schema}
